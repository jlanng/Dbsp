using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading.Channels;
using Incremental.Storage;
using Incremental.Tables;
using Microsoft.Extensions.Logging;

namespace Incremental.Service;

/// <summary>One circuit and what hangs off it: its sources, deliveries and dead-letter reader.</summary>
internal sealed class Generation
{
    public required long Number { get; init; }
    public required Circuit Circuit { get; init; }
    public required PipelineBuilder Builder { get; init; }
    public List<(string Slot, IRunningSource Source)> Sources { get; } = [];
    public List<Task> Deliveries { get; } = [];
    public Dictionary<string, SinkSlotStats> SinkStats { get; } = new(StringComparer.Ordinal);
    public List<object> OwnedSinks { get; } = [];
    public Task DeadLetters { get; set; } = Task.CompletedTask;
    public CancellationTokenSource Stop { get; } = new();
    public volatile bool Stopping;

    /// <summary>The circuit's logger factory: quiet once the generation is abandoned
    /// because its node no longer leads.</summary>
    public GenerationLogs? Logs { get; init; }

    /// <summary>The first failure of the circuit, a source or a delivery; null when the
    /// generation was stopped on purpose.</summary>
    public TaskCompletionSource<Exception?> Faulted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public void Observe(Task task) =>
        task.ContinueWith(
            (t, state) => ((Generation)state!).Faulted.TrySetResult(t.Exception!.GetBaseException()),
            this, CancellationToken.None, TaskContinuationOptions.OnlyOnFaulted, TaskScheduler.Default);
}

/// <summary>
/// What a circuit logs through. A node that has been replaced abandons its circuits'
/// outputs without draining them, and a circuit that is in the middle of a step then
/// reports that as a fault of its own, at Error and with a stack: true of the circuit, and
/// alarming to read beside a takeover that went well. From the moment a generation is
/// abandoned for that reason its circuit's warnings and errors are written at Debug.
/// A generation abandoned because it would not drain keeps its voice.
/// </summary>
internal sealed class GenerationLogs(ILoggerFactory inner) : ILoggerFactory
{
    private volatile bool _noLongerLeading;

    public void NoLongerLeading() => _noLongerLeading = true;

    public ILogger CreateLogger(string categoryName) => new Logger(inner.CreateLogger(categoryName), this);

    public void AddProvider(ILoggerProvider provider) => inner.AddProvider(provider);

    public void Dispose()
    {
        // The factory is the host's.
    }

    private sealed class Logger(ILogger logger, GenerationLogs owner) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => logger.BeginScope(state);

        public bool IsEnabled(LogLevel logLevel) => logger.IsEnabled(owner._noLongerLeading && logLevel >= LogLevel.Warning ? LogLevel.Debug : logLevel);

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (owner._noLongerLeading && logLevel >= LogLevel.Warning)
                logLevel = LogLevel.Debug;
            if (logger.IsEnabled(logLevel))
                logger.Log(logLevel, eventId, state, exception, formatter);
        }
    }
}

internal sealed class PipelineRuntime(PipelineSpec spec, ServiceOptions options)
{
    public string Id { get; } = spec.Id;
    public volatile PipelineSpec Spec = spec;

    /// <summary>False for a placeholder whose first deploy has not committed.</summary>
    public volatile bool Exists;

    /// <summary>One operation on a pipeline at a time; pipelines do not wait for each other.</summary>
    public SemaphoreSlim Queue { get; } = new(1, 1);
    public volatile PipelineState State = PipelineState.Unbuilt;
    public DateTimeOffset Since = DateTimeOffset.UtcNow;
    public volatile string? Operation;
    public volatile Generation? Current;
    public PluginLease? Lease;
    public volatile PipelineShape? Shape;

    /// <summary>The generation of this pipeline that is being started here: its circuit
    /// runs before <see cref="Current"/> names it. Its <see cref="Generation.Stop"/> is what
    /// a node that stops leading cancels to end the start.</summary>
    public volatile Generation? Starting;
    public volatile PipelineGraph? Graph;
    public Ring<PipelineTransition> History { get; } = new(32);
    public Queue<PipelineFault> Faults { get; } = new();
    public Queue<DateTimeOffset> FaultTimes { get; } = new();
    public int Restarts;
    public Ring<DeadLetterRecord> DeadLetters { get; } = new(options.DeadLetterBuffer);
    public volatile string? BlockedOn;
    public DateTimeOffset? BackoffUntil;
    public CancellationTokenSource? Retry;
    public volatile bool Wedged;
    public long Generations;
}

/// <summary>
/// Runs one circuit per pipeline and changes them one at a time while the others run:
/// deploy, start, stop, restart, replace, reset, remove; fault, backoff and <c>Failed</c>;
/// and the dead-letter output, which it always reads.
/// </summary>
internal sealed class PipelineSupervisor(ServiceContext context) : IPipelineSupervisor
{
    private readonly ConcurrentDictionary<string, PipelineRuntime> _pipelines = new(StringComparer.Ordinal);
    private readonly ILogger _logger = context.LoggerFactory.CreateLogger("Incremental.Service.Pipelines");

    /// <summary>What a secret property of a slot's config is shown as, and what a spec may
    /// send back to mean "the value that is stored".</summary>
    internal const string RedactedMark = "***";

    /// <summary>True from this node's takeover until it stops or abandons its pipelines.
    /// Read under a pipeline's queue, it is what keeps an action that was waiting for that
    /// queue from starting a generation on a node that has stopped running them.</summary>
    private volatile bool _accepting;

    private bool Leads => _accepting && context.IsLeader;

    private void RequireLeading()
    {
        context.RequireLeader();
        if (!_accepting)
            throw new ServiceException("not-leader", $"Node '{context.NodeId}' is not running pipelines: it is taking over or stopping.", 409);
    }

    /// <summary>Whether the runtime is the one the supervisor holds for its id. A runtime
    /// leaves the dictionary only under its own queue, so under the queue this is stable.</summary>
    private bool Attached(PipelineRuntime runtime) =>
        _pipelines.TryGetValue(runtime.Id, out var held) && ReferenceEquals(held, runtime);

    internal IReadOnlyList<PipelineSpec> Specs => _pipelines.Values.Where(p => p.Exists).Select(p => p.Spec).ToList();

    // ── status ─────────────────────────────────────────────────────────────────────────

    public IReadOnlyList<PipelineStatus> Pipelines =>
        _pipelines.Values.Where(p => p.Exists).OrderBy(p => p.Id, StringComparer.Ordinal).Select(Status).ToList();

    public PipelineStatus Get(string id) => Status(Require(id));

    public PipelineSpec Spec(string id) => Require(id).Spec;

    private PipelineRuntime Require(string id) =>
        _pipelines.TryGetValue(id, out var runtime) && runtime.Exists ? runtime : throw ServiceException.NotFound($"Pipeline '{id}'");

    private PipelineStatus Status(PipelineRuntime runtime)
    {
        var spec = runtime.Spec;
        var generation = runtime.Current;
        var sinks = generation?.SinkStats.Select(s => s.Value.Status(s.Key)).ToList() ?? [];
        var sources = generation?.Sources.Select(s => new SourceSlotStatus(s.Slot, s.Source.Stats)).ToList() ?? [];

        var blockedOn = runtime.State switch
        {
            PipelineState.Backoff => $"backoff until {runtime.BackoffUntil:O}",
            PipelineState.Blocked => runtime.BlockedOn,
            PipelineState.Running when sinks.FirstOrDefault(s => s.WriteInFlightSince is { } since && DateTimeOffset.UtcNow - since > context.Options.StallAfter) is { } stalled
                => $"sink-ack:{stalled.Slot}",
            PipelineState.Running when sources.FirstOrDefault(s => s.Stats.PausedSince is not null) is { } paused => $"source-paused:{paused.Slot}",
            _ => null,
        };

        IReadOnlyDictionary<string, long> committed;
        try
        {
            committed = context.Storage.Pipeline(runtime.Id).CommittedOffsets;
        }
        catch (ObjectDisposedException)
        {
            committed = new Dictionary<string, long>();
        }
        List<PipelineFault> faults;
        lock (runtime.Faults)
            faults = [.. runtime.Faults];
        return new PipelineStatus(
            runtime.Id, runtime.State, spec.Desired, spec.Generation, spec.Plugin, spec.Version, spec.Definition,
            blockedOn, runtime.Since, runtime.Operation, runtime.Restarts, runtime.DeadLetters.Total,
            generation?.Circuit.CommittedStep, committed, faults, runtime.Shape, sources, sinks, runtime.History.After());
    }

    public PipelineGraph? Graph(string id) => Require(id).Graph;

    public PipelineSpec RedactedSpec(string id)
    {
        var spec = Require(id).Spec;
        return spec with { Inputs = Redact(spec.Inputs), Sinks = Redact(spec.Sinks) };
    }

    private IReadOnlyDictionary<string, JsonElement> Redact(IReadOnlyDictionary<string, JsonElement> bindings)
    {
        Dictionary<string, JsonElement>? redacted = null;
        foreach (var (slot, config) in bindings)
        {
            IReadOnlyList<string>? secret = null;
            try
            {
                secret = TryConnector(config)?.Describe(config).SecretProperties;
            }
            catch
            {
                // A config its connector refuses is shown as it is stored, less nothing.
            }
            if (secret is not { Count: > 0 } || config.ValueKind != JsonValueKind.Object)
                continue;
            var copy = new Dictionary<string, JsonElement>(StringComparer.Ordinal);
            foreach (var property in config.EnumerateObject())
                copy[property.Name] = secret.Contains(property.Name) ? JsonSerializer.SerializeToElement(RedactedMark) : property.Value;
            (redacted ??= new Dictionary<string, JsonElement>(bindings, StringComparer.Ordinal))[slot] = JsonSerializer.SerializeToElement(copy);
        }
        return redacted ?? bindings;
    }

    /// <summary>The other direction: a spec that was read through <see cref="RedactedSpec"/>
    /// and sent back carries <c>***</c> where a secret was. Each is replaced by the value
    /// the stored spec holds for that slot and connector; with none to restore the spec is
    /// refused, so the mark is never stored as a secret.</summary>
    private PipelineSpec RestoreSecrets(PipelineSpec spec, PipelineSpec? stored) =>
        spec with
        {
            Inputs = RestoreSecrets(spec.Inputs, stored?.Inputs, "input"),
            Sinks = RestoreSecrets(spec.Sinks, stored?.Sinks, "sink"),
        };

    private IReadOnlyDictionary<string, JsonElement> RestoreSecrets(
        IReadOnlyDictionary<string, JsonElement> bindings, IReadOnlyDictionary<string, JsonElement>? stored, string what)
    {
        Dictionary<string, JsonElement>? restored = null;
        foreach (var (slot, config) in bindings)
        {
            if (config.ValueKind != JsonValueKind.Object)
                continue;
            IReadOnlyList<string>? secret = null;
            try
            {
                secret = TryConnector(config)?.Describe(config).SecretProperties;
            }
            catch
            {
                // The dry run refuses a config its connector refuses.
            }
            if (secret is not { Count: > 0 }
                || !config.EnumerateObject().Any(p => secret.Contains(p.Name) && p.Value.ValueKind == JsonValueKind.String && p.Value.GetString() == RedactedMark))
                continue;

            JsonElement old = default;
            var comparable = stored is not null && stored.TryGetValue(slot, out old) && old.ValueKind == JsonValueKind.Object
                && ConnectorName(old) == ConnectorName(config);
            var copy = new Dictionary<string, JsonElement>(StringComparer.Ordinal);
            foreach (var property in config.EnumerateObject())
            {
                var marked = secret.Contains(property.Name) && property.Value.ValueKind == JsonValueKind.String && property.Value.GetString() == RedactedMark;
                if (!marked)
                    copy[property.Name] = property.Value;
                else if (comparable && old.TryGetProperty(property.Name, out var kept))
                    copy[property.Name] = kept;
                else
                    throw ServiceException.Invalid(
                        $"The {what} slot '{slot}' gives '{property.Name}' as \"{RedactedMark}\", which is how a stored secret is shown; " +
                        "no value is stored for that slot and connector to keep, so the spec must give the value.");
            }
            (restored ??= new Dictionary<string, JsonElement>(bindings, StringComparer.Ordinal))[slot] = JsonSerializer.SerializeToElement(copy);
        }
        return restored ?? bindings;
    }

    /// <summary>Whether this node runs, or is starting, the circuit of that pipeline: whose
    /// measurements this node's metrics count.</summary>
    internal bool RunsHere(string id) =>
        _pipelines.TryGetValue(id, out var runtime) && runtime.Exists && (runtime.Starting is not null || runtime.Current is not null);

    /// <summary>What the metrics sampler reads once a second: cheap, no copies of offsets.</summary>
    internal void SampleInto(List<PipelineSampleInput> into)
    {
        foreach (var runtime in _pipelines.Values)
        {
            if (!runtime.Exists)
                continue;
            var generation = runtime.Current;
            long records = 0;
            long? lag = null;
            if (generation is not null)
            {
                foreach (var stats in generation.SinkStats.Values)
                    records += Interlocked.Read(ref stats.Upserts) + Interlocked.Read(ref stats.Deletes) + Interlocked.Read(ref stats.Appends);
                foreach (var (_, source) in generation.Sources)
                {
                    try
                    {
                        if (source.Stats.Lag is { } sourceLag)
                            lag = (lag ?? 0) + sourceLag;
                    }
                    catch
                    {
                        // A source's stats are its own business.
                    }
                }
            }
            into.Add(new PipelineSampleInput(runtime.Id, generation?.Number ?? 0, records, lag, runtime.DeadLetters.Total));
        }
    }

    public IReadOnlyList<DeadLetterRecord> DeadLetters(string id, long afterSequence = 0, int limit = 256) =>
        Require(id).DeadLetters.After(afterSequence, limit);

    public IReadOnlyList<DeadLetterRecord> DeadLettersBefore(string id, long beforeSequence = long.MaxValue, int limit = 256) =>
        Require(id).DeadLetters.Before(beforeSequence, limit);

    private void Set(PipelineRuntime runtime, PipelineState state, string? why = null)
    {
        if (runtime.State == state && why is null)
            return;
        runtime.State = state;
        runtime.Since = DateTimeOffset.UtcNow;
        runtime.History.Add(new PipelineTransition(runtime.Since, state, runtime.Spec.Generation, why));
        if (state is not PipelineState.Blocked)
            runtime.BlockedOn = null;
        _logger.LogInformation("Pipeline {Pipeline} generation {Generation} is {State}{Why}",
            runtime.Id, runtime.Spec.Generation, state, why is null ? "" : ": " + why);
        context.Events.Publish("pipeline", runtime.Id, why is null ? state.ToString() : $"{state}: {why}");
    }

    // ── the dry run ────────────────────────────────────────────────────────────────────

    public async Task<PipelineShape> ValidateAsync(PipelineSpec spec, CancellationToken cancellationToken) =>
        (await DescribeAsync(spec, cancellationToken).ConfigureAwait(false)).Shape;

    public async Task<PipelineValidation> DescribeAsync(PipelineSpec spec, CancellationToken cancellationToken)
    {
        var (resolved, lease) = await ResolveAsync(spec, cancellationToken).ConfigureAwait(false);
        using (lease)
        {
            var (shape, graph) = DryRun(resolved, lease);
            return new PipelineValidation(resolved, shape, graph);
        }
    }

    private async Task<(PipelineSpec Spec, PluginLease Lease)> ResolveAsync(PipelineSpec spec, CancellationToken cancellationToken)
    {
        ServiceNames.Validate(spec.Id, "pipeline id");
        ServiceNames.Validate(spec.Plugin, "plugin name");
        if (spec.Circuit.Workers < 1)
            throw ServiceException.Invalid("circuit.workers must be at least 1.");
        spec = spec with { Version = context.Plugins.ResolveVersion(spec.Plugin, spec.Version) };
        return (spec, await context.Plugins.AcquireAsync(spec.Plugin, spec.Version, cancellationToken).ConfigureAwait(false));
    }

    /// <summary>Runs the definition against a builder that is never built: the slots and
    /// tables it declares, with every binding checked. No side effects — a table is claimed
    /// only at <c>Circuit.Build()</c>.</summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private (PipelineShape Shape, PipelineGraph Graph) DryRun(PipelineSpec spec, PluginLease lease)
    {
        var definition = lease.Definition(spec.Definition);
        var settings = definition.BindSettings(spec.Settings, lease.Json);
        var builder = new CircuitBuilder(new CircuitOptions { Name = spec.Id + " (dry run)", Workers = spec.Circuit.Workers });
        var pipeline = new PipelineBuilder(spec, builder, context.Tables, _logger, context.Services);
        try
        {
            definition.Build(pipeline, settings);
        }
        catch (TableNotOpenException ex)
        {
            throw ServiceException.Invalid(ex.Message);
        }
        catch (Exception ex) when (ex is not ServiceException)
        {
            ex = DefinitionBinding.Cause(ex);
            throw ServiceException.Invalid($"Definition '{spec.Definition}' threw while describing its circuit: {ex.GetType().Name}: {ex.Message}");
        }
        CheckBindings(spec, pipeline);
        return (pipeline.Shape, pipeline.Graph);
    }

    private void CheckBindings(PipelineSpec spec, PipelineBuilder pipeline)
    {
        var errors = new List<string>();
        void Check(string slot, SlotKind kind, IReadOnlyDictionary<string, JsonElement> bindings, string what)
        {
            if (!bindings.TryGetValue(slot, out var config))
            {
                errors.Add($"{what} slot '{slot}' is not bound in the spec.");
                return;
            }
            IConnector connector;
            try
            {
                connector = context.Connector(config);
                connector.Describe(config);
                (connector as KafkaConnector)?.RequireKind(config, kind);
            }
            catch (Exception ex)
            {
                errors.Add($"{what} slot '{slot}': {ex.Message}");
                return;
            }
            var supported = kind switch
            {
                SlotKind.AppendInput => connector is IAppendSourceConnector,
                SlotKind.KeyedInput => connector is IKeyedSourceConnector,
                SlotKind.UpsertSink => connector is IUpsertSinkConnector,
                _ => connector is IAppendSinkConnector,
            };
            if (!supported)
                errors.Add($"{what} slot '{slot}' is {kind}; connector '{ConnectorName(config)}' ({connector.GetType().Name}) does not support that kind.");
        }

        foreach (var input in pipeline.Inputs)
            Check(input.Name, input.Kind, spec.Inputs, "input");
        foreach (var sink in pipeline.Sinks)
            Check(sink.Name, sink.Kind, spec.Sinks, "sink");
        foreach (var bound in spec.Inputs.Keys.Where(k => pipeline.Inputs.All(i => i.Name != k)))
            errors.Add($"The spec binds input slot '{bound}', which definition '{spec.Definition}' does not declare.");
        foreach (var bound in spec.Sinks.Keys.Where(k => pipeline.Sinks.All(s => s.Name != k)))
            errors.Add($"The spec binds sink slot '{bound}', which definition '{spec.Definition}' does not declare.");
        if (errors.Count > 0)
            throw ServiceException.Invalid($"Pipeline '{spec.Id}' does not bind.", errors);
    }

    private static string ConnectorName(JsonElement config) =>
        config.ValueKind == JsonValueKind.Object && config.TryGetProperty("connector", out var name) ? name.GetString() ?? "" : "";

    /// <summary>What differs between two specs that can change what the state means.</summary>
    private List<string> StateChanges(PipelineSpec old, PipelineSpec next)
    {
        var changes = new List<string>();
        if (old.Plugin != next.Plugin)
            changes.Add($"plugin: {old.Plugin} → {next.Plugin}");
        if (old.Version != next.Version)
        {
            var was = context.Plugins.Manifest(old.Plugin, old.Version)?.Fingerprint;
            var now = context.Plugins.Manifest(next.Plugin, next.Version)?.Fingerprint;
            // The fingerprint covers what the plugin's own assembly declares. A shape that
            // exists only inside a delegate — a tuple, a type of a private dependency — is
            // not in it, so "identical" is said of the declared shapes and no more.
            changes.Add($"version: {old.Version} → {next.Version} ({(was == now
                ? "declared data shapes identical; shapes used only inside delegates are not compared"
                : "declared data shapes differ")})");
        }
        if (old.Definition != next.Definition)
            changes.Add($"definition: {old.Definition} → {next.Definition}");
        if (old.Circuit.Workers != next.Circuit.Workers)
            changes.Add($"workers: {old.Circuit.Workers} → {next.Circuit.Workers}");
        foreach (var slot in old.Inputs.Keys.Union(next.Inputs.Keys).Order(StringComparer.Ordinal))
            if (Raw(old.Inputs, slot) != Raw(next.Inputs, slot))
                changes.Add($"input binding: {slot}");
        foreach (var slot in old.Tables.Keys.Union(next.Tables.Keys).Order(StringComparer.Ordinal))
            if (old.Tables.GetValueOrDefault(slot, slot) != next.Tables.GetValueOrDefault(slot, slot))
                changes.Add($"table binding: {slot}");

        var neutral = context.Plugins.Manifest(next.Plugin, next.Version)?.Definitions.FirstOrDefault(d => d.Name == next.Definition)
            ?.Settings.Where(s => s.StateNeutral).Select(s => s.Name).ToHashSet(StringComparer.OrdinalIgnoreCase) ?? [];
        var before = Properties(old.Settings);
        var after = Properties(next.Settings);
        foreach (var name in before.Keys.Union(after.Keys, StringComparer.OrdinalIgnoreCase).Order(StringComparer.Ordinal))
            if (before.GetValueOrDefault(name) != after.GetValueOrDefault(name) && !neutral.Contains(name))
                changes.Add($"setting: {name}");
        return changes;

        static string? Raw(IReadOnlyDictionary<string, JsonElement> bindings, string slot) =>
            bindings.TryGetValue(slot, out var element) ? Canonical(element) : null;

        static Dictionary<string, string> Properties(JsonElement? settings) =>
            settings is { ValueKind: JsonValueKind.Object } s
                ? s.EnumerateObject().ToDictionary(p => p.Name, p => Canonical(p.Value), StringComparer.OrdinalIgnoreCase)
                : new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>A JSON value as text that does not depend on how it was written: no
    /// insignificant whitespace, object members in name order. A spec read back from the
    /// catalog and the same spec as a caller formats it must compare equal.</summary>
    private static string Canonical(JsonElement element)
    {
        using var buffer = new MemoryStream();
        using (var writer = new Utf8JsonWriter(buffer))
            Write(element, writer);
        return System.Text.Encoding.UTF8.GetString(buffer.GetBuffer(), 0, (int)buffer.Length);

        static void Write(JsonElement value, Utf8JsonWriter writer)
        {
            switch (value.ValueKind)
            {
                case JsonValueKind.Object:
                    writer.WriteStartObject();
                    foreach (var property in value.EnumerateObject().OrderBy(p => p.Name, StringComparer.Ordinal))
                    {
                        writer.WritePropertyName(property.Name);
                        Write(property.Value, writer);
                    }
                    writer.WriteEndObject();
                    break;
                case JsonValueKind.Array:
                    writer.WriteStartArray();
                    foreach (var item in value.EnumerateArray())
                        Write(item, writer);
                    writer.WriteEndArray();
                    break;
                default:
                    value.WriteTo(writer);
                    break;
            }
        }
    }

    // ── actions ────────────────────────────────────────────────────────────────────────

    private async Task<Exit> EnterAsync(PipelineRuntime runtime, string operation, CancellationToken cancellationToken)
    {
        await runtime.Queue.WaitAsync(cancellationToken).ConfigureAwait(false);
        runtime.Operation = operation;
        return new Exit(runtime);
    }

    /// <summary>The queue of a pipeline that exists, for an action. Everything is checked
    /// again once the queue is held: while this call waited the pipeline may have been
    /// removed and the node may have stopped leading, and an action that carried on then
    /// would write a removed pipeline back into the catalog or start a circuit nothing
    /// holds.</summary>
    private async Task<(PipelineRuntime Runtime, Exit Exit)> EnterExistingAsync(string id, string operation, CancellationToken cancellationToken)
    {
        RequireLeading();
        var runtime = Require(id);
        var exit = await EnterAsync(runtime, operation, cancellationToken).ConfigureAwait(false);
        try
        {
            RequireLeading();
            if (!Attached(runtime) || !runtime.Exists)
                throw ServiceException.NotFound($"Pipeline '{id}'");
            return (runtime, exit);
        }
        catch
        {
            exit.Dispose();
            throw;
        }
    }

    private sealed class Exit(PipelineRuntime runtime) : IDisposable
    {
        private int _disposed;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
                return;
            runtime.Operation = null;
            runtime.Queue.Release();
        }
    }

    private static void RequireGeneration(PipelineRuntime runtime, long? expected)
    {
        if (expected is { } generation && generation != runtime.Spec.Generation)
            throw new ServiceException(
                "precondition-failed", $"Pipeline '{runtime.Id}' is at generation {runtime.Spec.Generation}, not {generation}.", 412);
    }

    private static void RefuseWhileRemoving(PipelineRuntime runtime)
    {
        if (runtime.Spec.Pending == "remove")
            throw ServiceException.Conflict($"Pipeline '{runtime.Id}' is being removed; remove it again, or restart it, to finish that.");
    }

    private async Task CommitAsync(
        PipelineRuntime runtime, PipelineSpec spec, string action, string? caller, IReadOnlyList<string>? plan, CancellationToken cancellationToken,
        bool delete = false)
    {
        using var write = await context.Catalog.BeginAsync(cancellationToken).ConfigureAwait(false);
        if (delete)
            write.Delete("pipeline/" + spec.Id);
        else
            write.Put("pipeline/" + spec.Id, spec);
        write.Audit(context.NodeId, context.Term, caller, action, spec.Id, runtime.Exists ? runtime.Spec.Generation : null, delete ? null : spec.Generation, plan);
        await write.CommitAsync(cancellationToken).ConfigureAwait(false);
        if (!delete)
        {
            runtime.Spec = spec;
            runtime.Exists = true;
        }
    }

    public async Task<DeployResult> DeployAsync(PipelineSpec spec, DeployOptions options, CancellationToken cancellationToken)
    {
        RequireLeading();
        ServiceNames.Validate(spec.Id, "pipeline id");
        if (spec.Desired == DesiredState.Removed)
            throw ServiceException.Invalid("A spec cannot be deployed as Removed; remove the pipeline instead.");

        // A placeholder holds the id until the first deploy commits. Another call can hold
        // the same placeholder and drop it (a dry run, a refused deploy) while this one
        // waits for its queue, so the runtime is taken again until it is the one held.
        PipelineRuntime runtime;
        Exit exit;
        while (true)
        {
            var id = spec.Id;
            runtime = _pipelines.GetOrAdd(id, _ => new PipelineRuntime(spec, context.Options));
            exit = await EnterAsync(runtime, "deploy", cancellationToken).ConfigureAwait(false);
            if (Attached(runtime))
                break;
            exit.Dispose();
        }

        PluginLease? lease = null;
        try
        {
            RequireLeading();
            var existing = runtime.Exists ? runtime.Spec : null;
            RequireGeneration(runtime, existing is null && options.ExpectedGeneration is 0 ? null : options.ExpectedGeneration);
            if (existing?.Pending is not null)
                throw ServiceException.Conflict(
                    $"Pipeline '{spec.Id}' has an unfinished {existing.Pending}; it is retried by itself, and restarting the pipeline retries it now.");

            spec = RestoreSecrets(spec, existing);
            (spec, lease) = await ResolveAsync(spec, cancellationToken).ConfigureAwait(false);
            var changes = existing is null ? [] : StateChanges(existing, spec);
            if (changes.Count > 0 && options.State == StateChoice.Unspecified)
                throw new ServiceException(
                    "state-choice-required",
                    $"This change to pipeline '{spec.Id}' can change what its state means; say whether to keep the state or reset it.", 409, changes);

            var (shape, graph) = DryRun(spec, lease);
            RequireReferences(spec, shape);

            var reset = existing is not null && options.State == StateChoice.Reset;
            var steps = new List<string>();
            if (existing is null)
                steps.Add($"deploy generation 1 of '{spec.Definition}' from {spec.Plugin}@{spec.Version}; state starts empty");
            else
            {
                steps.Add($"stop generation {existing.Generation} (drain, commit)");
                steps.AddRange(changes.Select(c => "changes " + c));
                steps.Add(reset ? "state dropped: a new incarnation; sources start again from their start position; upsert sinks keep rows the new run does not produce"
                    : changes.Count > 0 ? "state kept: the new generation reads the old one's entries as its own types"
                    : "state kept: nothing that touches it changed");
            }
            steps.Add(spec.Desired == DesiredState.Running ? "start" : "leave stopped");
            var plan = new ActionPlan(existing is null ? "pipeline.deploy" : "pipeline.replace", spec.Id, options.DryRun, steps);
            if (options.DryRun)
                return new DeployResult(spec with { Generation = existing?.Generation ?? 0 }, shape, plan);

            // Intent first: from here a successor knows everything. The tables and the
            // plugin version the spec names are checked again under the lock their removal
            // takes, so the entry is never written beside the removal of what it names.
            spec = spec with
            {
                Generation = (existing?.Generation ?? 0) + 1,
                Reads = shape.TablesRead,
                Writes = shape.TablesWritten,
                Pending = reset ? "reset" : null,
            };
            using (await context.LockReferencesAsync(cancellationToken).ConfigureAwait(false))
            {
                RequireReferences(spec, shape);
                await CommitAsync(runtime, spec, plan.Action, options.Caller, steps, cancellationToken).ConfigureAwait(false);
            }

            // The intent is recorded: what follows is carried through whatever becomes of
            // the caller, so nothing below takes its token.
            runtime.Shape = shape;
            runtime.Graph = graph;
            if (runtime.Current is not null || existing is not null)
                await StopCurrentAsync(runtime, context.Options.StopTimeout).ConfigureAwait(false);
            if (!runtime.Wedged)
            {
                if (changes.Count > 0)
                    ReleaseBinding(runtime);
                if (runtime.Lease is null)
                {
                    Bind(runtime, lease);
                    lease = null;
                }
                ClearFaults(runtime);
                await ReconcileAsync(runtime).ConfigureAwait(false);
            }
            return new DeployResult(runtime.Spec, shape, plan);
        }
        finally
        {
            lease?.Dispose();

            // Still under the queue: a placeholder is dropped where no other call can be
            // half-way through using it.
            if (!runtime.Exists)
                _pipelines.TryRemove(new KeyValuePair<string, PipelineRuntime>(runtime.Id, runtime));
            exit.Dispose();
        }
    }

    /// <summary>What a spec names outside itself must be there: its tables declared, one
    /// writer per table, its plugin version stored.</summary>
    private void RequireReferences(PipelineSpec spec, PipelineShape shape)
    {
        foreach (var table in shape.TablesRead.Concat(shape.TablesWritten))
            if (context.Tables.Find(table) is null)
                throw ServiceException.Conflict($"Table '{table}', which pipeline '{spec.Id}' names, is not declared (it was removed while the spec was being checked).");
        foreach (var table in shape.TablesWritten)
        {
            if (_pipelines.Values.FirstOrDefault(p => p.Exists && p.Id != spec.Id && p.Spec.Writes.Contains(table)) is { } other)
                throw ServiceException.Conflict($"Table '{table}' is already written by pipeline '{other.Id}'; a table has one writer.");
            if (context.Tables.Find(table)?.Spec.Loader is { } loader)
                throw ServiceException.Conflict($"Table '{table}' has a {loader.Kind} loader; a pipeline cannot also write it.");
        }
        if (context.Plugins.Manifest(spec.Plugin, spec.Version) is null)
            throw ServiceException.Conflict($"Plugin '{spec.Plugin}' version {spec.Version} is no longer stored.");
    }

    public async Task<ActionPlan> StartAsync(string id, ActionOptions options, CancellationToken cancellationToken)
    {
        var (runtime, exit) = await EnterExistingAsync(id, "start", cancellationToken).ConfigureAwait(false);
        using var _ = exit;
        RequireGeneration(runtime, options.ExpectedGeneration);
        var plan = new ActionPlan("pipeline.start", id, options.DryRun,
            ["desired state: Running", "build a generation on the committed state; sources resume from the committed offsets"]);
        if (options.DryRun)
            return plan;
        RefuseWhileRemoving(runtime);
        if (runtime.Spec.Desired != DesiredState.Running)
            await CommitAsync(runtime, runtime.Spec with { Desired = DesiredState.Running, Generation = runtime.Spec.Generation + 1 },
                plan.Action, options.Caller, plan.Steps, cancellationToken).ConfigureAwait(false);
        if (runtime.Current is null)
        {
            ClearFaults(runtime);
            await ReconcileAsync(runtime).ConfigureAwait(false);
        }
        return plan;
    }

    public async Task<ActionPlan> StopAsync(string id, ActionOptions options, CancellationToken cancellationToken)
    {
        var (runtime, exit) = await EnterExistingAsync(id, "stop", cancellationToken).ConfigureAwait(false);
        using var _ = exit;
        RequireGeneration(runtime, options.ExpectedGeneration);
        var steps = new List<string> { "desired state: Stopped", "stop sources, drain the circuit, commit, stop deliveries" };
        foreach (var table in runtime.Spec.Reads)
        {
            try
            {
                var reader = (await context.Tables.StatsAsync(table).ConfigureAwait(false)).Readers.FirstOrDefault(r => r.Pipeline == id);
                steps.Add(reader is null
                    ? $"reads '{table}': nothing committed yet, so it holds no history"
                    : $"reads '{table}': its checkpoint holds the table's history; it must restart within {reader.VersionsUntilCutLoose} versions" +
                      (reader.TimeUntilCutLoose is { } time ? $" (about {Durations.Text(time)} at the current rate)" : "") + " or it will need a reset");
            }
            catch (ServiceException)
            {
            }
        }
        var plan = new ActionPlan("pipeline.stop", id, options.DryRun, steps);
        if (options.DryRun)
            return plan;
        RefuseWhileRemoving(runtime);
        if (runtime.Spec.Desired != DesiredState.Stopped)
            await CommitAsync(runtime, runtime.Spec with { Desired = DesiredState.Stopped, Generation = runtime.Spec.Generation + 1 },
                plan.Action, options.Caller, steps, cancellationToken).ConfigureAwait(false);
        await StopCurrentAsync(runtime, context.Options.StopTimeout).ConfigureAwait(false);
        if (!runtime.Wedged)
            await ReconcileAsync(runtime).ConfigureAwait(false);
        return plan;
    }

    public async Task<ActionPlan> RestartAsync(string id, ActionOptions options, CancellationToken cancellationToken)
    {
        var (runtime, exit) = await EnterExistingAsync(id, "restart", cancellationToken).ConfigureAwait(false);
        using var _ = exit;
        RequireGeneration(runtime, options.ExpectedGeneration);
        var steps = new List<string> { "stop the generation (drain, commit)", "clear Failed and the restart count" };
        if (runtime.Spec.Pending is { } pending)
            steps.Add($"finish the pending {pending}");
        if (runtime.Spec.Pending != "remove")
            steps.Add(runtime.Spec.Desired == DesiredState.Running ? "build a new generation on the same state" : "leave stopped");
        var plan = new ActionPlan("pipeline.restart", id, options.DryRun, steps);
        if (options.DryRun)
            return plan;
        using (var write = await context.Catalog.BeginAsync(cancellationToken).ConfigureAwait(false))
        {
            write.Audit(context.NodeId, context.Term, options.Caller, plan.Action, id, runtime.Spec.Generation, runtime.Spec.Generation, plan.Steps);
            await write.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        await StopCurrentAsync(runtime, context.Options.StopTimeout).ConfigureAwait(false);
        if (!runtime.Wedged || runtime.Spec.Pending == "remove")
        {
            ClearFaults(runtime);
            await ReconcileAsync(runtime).ConfigureAwait(false);
        }
        return plan;
    }

    public async Task<ActionPlan> ResetAsync(string id, ActionOptions options, CancellationToken cancellationToken)
    {
        var (runtime, exit) = await EnterExistingAsync(id, "reset", cancellationToken).ConfigureAwait(false);
        using var _ = exit;
        RequireGeneration(runtime, options.ExpectedGeneration);
        var steps = new List<string>
        {
            "stop the generation",
            "state dropped: a new incarnation; input offsets gone, so sources start again from their start position",
        };
        foreach (var (slot, config) in runtime.Spec.Sinks)
            steps.Add(TryConnector(config) is IResettableSinkConnector
                ? $"sink '{slot}': cleared by its connector"
                : $"sink '{slot}': keeps what the old run wrote; an upsert sink keeps rows the new run does not produce, an append sink receives the history again");
        foreach (var table in runtime.Spec.Writes)
            steps.Add($"derived table '{table}': loses every row the new run does not assert in its first step, under the pipelines that read it");
        steps.Add(runtime.Spec.Desired == DesiredState.Running ? "start" : "leave stopped");
        var plan = new ActionPlan("pipeline.reset", id, options.DryRun, steps);
        if (options.DryRun)
            return plan;
        RefuseWhileRemoving(runtime);

        // A reset that is already pending is finished, not recorded a second time.
        if (runtime.Spec.Pending != "reset")
            await CommitAsync(runtime, runtime.Spec with { Pending = "reset", Generation = runtime.Spec.Generation + 1 },
                plan.Action, options.Caller, steps, cancellationToken).ConfigureAwait(false);

        // The intent is recorded; the rest does not depend on the caller staying.
        await StopCurrentAsync(runtime, context.Options.StopTimeout).ConfigureAwait(false);
        if (runtime.Wedged)
            return plan;
        ClearFaults(runtime);
        await ReconcileAsync(runtime).ConfigureAwait(false);
        return plan;
    }

    public async Task<ActionPlan> RemoveAsync(string id, ActionOptions options, CancellationToken cancellationToken)
    {
        var (runtime, exit) = await EnterExistingAsync(id, "remove", cancellationToken).ConfigureAwait(false);
        using var _ = exit;
        RequireGeneration(runtime, options.ExpectedGeneration);
        var steps = new List<string> { "stop the generation", "tombstone its state (one empty commit record stays in the changelog)", "delete the catalog entry" };
        foreach (var table in runtime.Spec.Reads)
            steps.Add($"table '{table}' loses a reader: its retention floor may rise");
        foreach (var table in runtime.Spec.Writes)
            steps.Add($"derived table '{table}' keeps its rows and has no writer until another pipeline writes it");
        if (runtime.Spec.Sinks.Count > 0)
            steps.Add("rows already delivered to sinks stay there");
        var plan = new ActionPlan("pipeline.remove", id, options.DryRun, steps);
        if (options.DryRun)
            return plan;

        if (runtime.Spec.Pending != "remove")
            await CommitAsync(runtime, runtime.Spec with { Desired = DesiredState.Removed, Pending = "remove", Generation = runtime.Spec.Generation + 1 },
                plan.Action, options.Caller, steps, cancellationToken).ConfigureAwait(false);
        ClearFaults(runtime);
        await ReconcileAsync(runtime).ConfigureAwait(false);
        return plan;
    }

    private IConnector? TryConnector(JsonElement config)
    {
        try
        {
            return context.Connector(config);
        }
        catch
        {
            return null;
        }
    }

    /// <summary>
    /// Brings a pipeline to what its spec says, the pending act first. Every path that can
    /// start a generation comes through here — an action, a backoff retry, a takeover, a
    /// reopened table — so a pipeline with a reset or a removal pending is never started
    /// around it: the act is finished, or the pipeline waits in <c>Backoff</c> and the act
    /// is tried again. The caller holds the pipeline's queue. Never throws for a failure of
    /// the pipeline.
    /// </summary>
    private async Task ReconcileAsync(PipelineRuntime runtime)
    {
        CancelRetry(runtime);
        if (!Leads)
        {
            if (runtime.Current is null && !runtime.Wedged)
                Set(runtime, PipelineState.Stopped, "this node no longer leads");
            return;
        }

        if (runtime.Spec.Pending is { } pending)
        {
            try
            {
                // A pending act never runs beside a live generation of the state it drops.
                await StopCurrentAsync(runtime, context.Options.StopTimeout).ConfigureAwait(false);
                if (pending == "remove")
                {
                    await FinishRemoveAsync(runtime).ConfigureAwait(false);
                    return;
                }
                if (runtime.Wedged)
                {
                    Set(runtime, PipelineState.Wedged);
                    return;
                }
                await FinishResetAsync(runtime).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // Tenant code and the backend both end here, cancellations of their own
                // included: the pipeline's failure, tried again, never the node's.
                HandleFault(runtime, ex, runtime.Generations, $"{pending} not finished");
                return;
            }
        }

        if (runtime.Spec.Desired == DesiredState.Running)
            await StartCoreAsync(runtime).ConfigureAwait(false);
        else if (runtime.Wedged)
            Set(runtime, PipelineState.Wedged);
        else if (runtime.Current is null)
            Set(runtime, PipelineState.Stopped);
    }

    /// <summary>The second half of a reset, also run by a leader that finds one pending.
    /// Safe to run again after a failure part-way.</summary>
    private async Task FinishResetAsync(PipelineRuntime runtime)
    {
        var spec = runtime.Spec;
        await context.Storage.Pipeline(runtime.Id).StartNewIncarnationAsync(CancellationToken.None).ConfigureAwait(false);
        foreach (var (slot, config) in spec.Sinks)
        {
            if (TryConnector(config) is not IResettableSinkConnector resettable)
                continue;

            // A connector's call is tenant code: it gets a bound, and its token is
            // cancelled at that bound.
            var timeout = context.Options.ConnectorTimeout;
            var limit = new CancellationTokenSource(timeout);
            var sink = new SinkContext(new SlotStart
            {
                Circuit = null!,
                PipelineId = runtime.Id,
                Slot = slot,
                Config = config,
                Json = runtime.Lease?.Json ?? LoadedPlugin.NewJson(),
                Logger = _logger,
                Services = context.Services,
                Leadership = context.Backend.Leadership,
                Cancellation = limit.Token,
            });
            var reset = Task.Run(() => resettable.ResetAsync(sink, limit.Token));
            if (!await reset.CompletesWithin(timeout).ConfigureAwait(false))
            {
                reset.Forget();
                throw new TimeoutException($"The connector of sink '{slot}' did not finish its reset within {Durations.Text(timeout)}.");
            }
            await reset.ConfigureAwait(false);
        }
        await CommitAsync(runtime, spec with { Pending = null, Generation = spec.Generation + 1 }, "pipeline.reset.finished", "service", null, CancellationToken.None)
            .ConfigureAwait(false);
    }

    private async Task FinishRemoveAsync(PipelineRuntime runtime)
    {
        if (!runtime.Wedged)
        {
            await context.Storage.Pipeline(runtime.Id).StartNewIncarnationAsync(CancellationToken.None).ConfigureAwait(false);
            ReleaseBinding(runtime);
        }
        await CommitAsync(runtime, runtime.Spec, "pipeline.remove.finished", "service", null, CancellationToken.None, delete: true).ConfigureAwait(false);
        runtime.Exists = false;
        _pipelines.TryRemove(new KeyValuePair<string, PipelineRuntime>(runtime.Id, runtime));
        context.Events.Publish("pipeline", runtime.Id, "removed");
    }

    private static void ClearFaults(PipelineRuntime runtime)
    {
        runtime.FaultTimes.Clear();
        runtime.Restarts = 0;
    }

    // ── state binding: the plugin version a pipeline's typed stores belong to ──────────

    private void Bind(PipelineRuntime runtime, PluginLease lease)
    {
        var provider = context.Storage.Pipeline(runtime.Id);
        provider.ReleaseStores();
        provider.UseSerializer(new JsonStateSerializer(lease.StateJson));
        runtime.Lease = lease;
    }

    /// <summary>Turns the pipeline's typed stores back into bytes and lets go of the
    /// plugin version they belonged to, which is what allows that version to unload.</summary>
    private void ReleaseBinding(PipelineRuntime runtime)
    {
        if (runtime.Wedged)
            return; // The stores still belong to a circuit that will not stop.
        var provider = context.Storage.Pipeline(runtime.Id);
        provider.ReleaseStores();
        provider.UseSerializer(JsonStateSerializer.Instance);
        runtime.Lease?.Dispose();
        runtime.Lease = null;
    }

    // ── a generation ───────────────────────────────────────────────────────────────────

    /// <summary>Builds and starts a generation. Called by <see cref="ReconcileAsync"/>
    /// only, with the pipeline's queue held. Never throws for a failure of the pipeline:
    /// that ends in Blocked, Backoff, Failed or NeedsReset.</summary>
    private async Task StartCoreAsync(PipelineRuntime runtime)
    {
        CancelRetry(runtime);
        if (runtime.Wedged)
        {
            Set(runtime, PipelineState.Wedged);
            return;
        }
        if (runtime.Current is not null)
            return;

        Set(runtime, PipelineState.Starting);
        var number = ++runtime.Generations;
        Generation? generation = null;
        try
        {
            var spec = runtime.Spec;
            if (runtime.Lease is not { } bound || bound.Name != spec.Plugin || bound.Version != spec.Version)
            {
                ReleaseBinding(runtime);
                Bind(runtime, await context.Plugins.AcquireAsync(spec.Plugin, spec.Version, CancellationToken.None).ConfigureAwait(false));
            }
            generation = Build(runtime, number);
            runtime.Starting = generation;
            generation.Circuit.Start();
            await StartEdgesAsync(runtime, generation).ConfigureAwait(false);

            // The start can take as long as a connector does, and the node may have been
            // fenced or stopped meanwhile. Whoever ended it could not see this generation
            // (Current did not name it yet), so it is ended here rather than published.
            if (!Leads)
                throw new NoLongerLeadingException();
            runtime.Current = generation;
            runtime.Starting = null;
            Set(runtime, PipelineState.Running);
            Watch(runtime, generation);
        }
        catch (NoLongerLeadingException)
        {
            runtime.Starting = null;
            generation!.Logs?.NoLongerLeading();
            Abandon(generation!);
            await TeardownAsync(runtime, generation!, TimeSpan.Zero).ConfigureAwait(false);
            if (!runtime.Wedged)
                Set(runtime, PipelineState.Stopped, "this node no longer leads");
        }
        catch (TableNotOpenException ex)
        {
            runtime.Starting = null;
            if (generation is not null)
                await TeardownAsync(runtime, generation, context.Options.StopTimeout).ConfigureAwait(false);
            if (runtime.Wedged)
                return;
            runtime.BlockedOn = "table:" + ex.Table;
            Set(runtime, PipelineState.Blocked, ex.Message);
            ScheduleRetry(runtime, context.Options.BlockedRetryInterval, PipelineState.Blocked);
        }
        catch (Exception ex)
        {
            runtime.Starting = null;
            if (generation is not null)
                await TeardownAsync(runtime, generation, context.Options.StopTimeout).ConfigureAwait(false);
            if (!runtime.Wedged)
                HandleFault(runtime, DefinitionBinding.Cause(ex), number);
        }
    }

    private sealed class NoLongerLeadingException : Exception;

    private static void Abandon(Generation generation)
    {
        foreach (var sink in generation.Builder.Sinks)
        {
            try
            {
                sink.Abandon(generation.Circuit);
            }
            catch
            {
                // Abandoning is best effort.
            }
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private Generation Build(PipelineRuntime runtime, long number)
    {
        var spec = runtime.Spec;
        var lease = runtime.Lease!;
        var definition = lease.Definition(spec.Definition);
        var settings = definition.BindSettings(spec.Settings, lease.Json);
        var logs = new GenerationLogs(context.LoggerFactory);
        var builder = new CircuitBuilder(new CircuitOptions
        {
            Name = spec.Id,
            Workers = spec.Circuit.Workers,
            Storage = context.Storage.Pipeline(spec.Id),
            CheckpointInterval = spec.Circuit.CheckpointInterval,
            CheckpointMaxDelay = spec.Circuit.CheckpointMaxDelay,
            ErrorPolicy = spec.Circuit.ErrorPolicy,
            MaxStepItemsPerInput = spec.Circuit.MaxStepItemsPerInput ?? 8192,
            LoggerFactory = logs,
        });
        var pipeline = new PipelineBuilder(spec, builder, context.Tables, _logger, context.Services);
        definition.Build(pipeline, settings);
        CheckBindings(spec, pipeline);
        runtime.Shape = pipeline.Shape;
        runtime.Graph = pipeline.Graph;
        return new Generation { Number = number, Circuit = builder.Build(), Builder = pipeline, Logs = logs };
    }

    /// <summary>Deliveries, then the dead-letter reader, then sources — after
    /// <c>Start()</c>, because sources read the committed offsets.</summary>
    private async Task StartEdgesAsync(PipelineRuntime runtime, Generation generation)
    {
        var spec = runtime.Spec;
        var circuit = generation.Circuit;
        generation.Observe(circuit.Completion);

        SlotStart For(string slot, JsonElement config) => new()
        {
            Circuit = circuit,
            PipelineId = runtime.Id,
            Slot = slot,
            Config = config,
            Json = runtime.Lease!.Json,
            Logger = _logger,
            Services = context.Services,
            Leadership = context.Backend.Leadership,
            Cancellation = generation.Stop.Token,
            Stats = generation.SinkStats.TryGetValue(slot, out var stats) ? stats : new SinkSlotStats(),
            Owned = generation.OwnedSinks,
        };

        foreach (var sink in generation.Builder.Sinks)
        {
            generation.SinkStats[sink.Name] = new SinkSlotStats();
            var config = spec.Sinks[sink.Name];
            var delivery = sink.Start(context.Connector(config), For(sink.Name, config));
            generation.Deliveries.Add(delivery);
            generation.Observe(delivery);
        }

        ChannelReader<DeadLetter>? deadLetters = null;
        try
        {
            deadLetters = circuit.DeadLetters;
        }
        catch (InvalidOperationException)
        {
            // No operator of this circuit has the DeadLetter policy.
        }
        if (deadLetters is not null)
            generation.DeadLetters = Task.Run(() => ReadDeadLettersAsync(runtime, deadLetters, runtime.Lease!.Json));

        // A connector's StartAsync is tenant code that may connect somewhere: it runs off
        // this thread and within a bound. A start that passes the bound fails the
        // generation — this pipeline's fault, retried — and holds up nothing else: not the
        // pipeline's queue, and not a takeover, which waits for every pipeline's start.
        var timeout = context.Options.ConnectorTimeout;
        foreach (var input in generation.Builder.Inputs)
        {
            var config = spec.Inputs[input.Name];
            var connector = context.Connector(config);
            var slot = For(input.Name, config);
            var starting = Task.Run(() => input.Start(connector, slot));
            if (!await starting.CompletesWithin(timeout).ConfigureAwait(false))
            {
                generation.Stop.Cancel();
                starting.ContinueWith(
                    static async late =>
                    {
                        if (late.IsCompletedSuccessfully)
                        {
                            try
                            {
                                await late.Result.DisposeAsync().ConfigureAwait(false);
                            }
                            catch
                            {
                                // A source that started after it was given up on.
                            }
                        }
                        else
                            GC.KeepAlive(late.Exception);
                    },
                    CancellationToken.None, TaskContinuationOptions.None, TaskScheduler.Default).Forget();
                throw new TimeoutException($"The source of input '{input.Name}' did not start within {Durations.Text(timeout)}.");
            }
            var source = await starting.ConfigureAwait(false);
            generation.Sources.Add((input.Name, source));
            generation.Observe(source.Completion);
        }
    }

    /// <summary>The dead-letter output must be read or the circuit stalls, so the
    /// supervisor always reads it: into a ring per pipeline, and a count.</summary>
    private async Task ReadDeadLettersAsync(PipelineRuntime runtime, ChannelReader<DeadLetter> reader, JsonSerializerOptions json)
    {
        var lastLogged = DateTimeOffset.MinValue;
        var sinceLogged = 0;
        try
        {
            await foreach (var letter in reader.ReadAllAsync().ConfigureAwait(false))
            {
                runtime.DeadLetters.Add(sequence => new DeadLetterRecord(
                    sequence, DateTimeOffset.UtcNow, letter.Operator, letter.Weight,
                    letter.Exception.GetType().Name, letter.Exception.Message, Render(letter.Record, json)));
                sinceLogged++;
                var now = DateTimeOffset.UtcNow;
                if (now - lastLogged >= TimeSpan.FromSeconds(10))
                {
                    _logger.LogWarning("Pipeline {Pipeline} dropped {Count} record(s) to its dead letters; the last at {Operator}: {Message}",
                        runtime.Id, sinceLogged, letter.Operator, letter.Exception.Message);
                    context.Events.Publish("deadletter", runtime.Id, $"{sinceLogged} record(s) dropped at {letter.Operator}");
                    (lastLogged, sinceLogged) = (now, 0);
                }
            }
        }
        catch
        {
            // The channel fails with the circuit's fault, which the watcher handles.
        }
    }

    private static string? Render(object? record, JsonSerializerOptions json)
    {
        if (record is null)
            return null;

        // A record a Kafka source could not decode: where it is, then what it holds.
        if (record is Incremental.Kafka.KafkaRecord unreadable)
        {
            var value = unreadable.Value is { } bytes ? System.Text.Encoding.UTF8.GetString(bytes, 0, Math.Min(bytes.Length, 4096)) : "(no value)";
            return $"{unreadable.Topic}[{unreadable.Partition}]@{unreadable.Offset}: {value}";
        }
        string text;
        try
        {
            text = JsonSerializer.Serialize(record, record.GetType(), json);
        }
        catch
        {
            text = record.ToString() ?? "";
        }
        return text.Length > 4096 ? text[..4096] : text;
    }

    private void Watch(PipelineRuntime runtime, Generation generation) =>
        Task.Run(async () =>
        {
            var failure = await generation.Faulted.Task.ConfigureAwait(false);
            if (failure is null || generation.Stopping)
                return;
            await runtime.Queue.WaitAsync().ConfigureAwait(false);
            try
            {
                if (!ReferenceEquals(runtime.Current, generation) || generation.Stopping)
                    return;
                runtime.Operation = "fault";
                var cause = generation.Circuit.Fault ?? failure;
                await TeardownAsync(runtime, generation, context.Options.StopTimeout).ConfigureAwait(false);
                if (!runtime.Wedged)
                    HandleFault(runtime, cause, generation.Number);
            }
            finally
            {
                runtime.Operation = null;
                runtime.Queue.Release();
            }
        }).Forget();

    /// <summary>What becomes of a pipeline after a failure: Backoff and a retry, or one of
    /// the states nothing retries. <paramref name="during"/> names a pending act that could
    /// not be finished; the retry then tries the act again. Caller holds the queue.</summary>
    private void HandleFault(PipelineRuntime runtime, Exception ex, long generation, string? during = null)
    {
        lock (runtime.Faults)
        {
            runtime.Faults.Enqueue(new PipelineFault(DateTimeOffset.UtcNow, generation, ex.GetType().Name, ex.Message, ex.StackTrace));
            while (runtime.Faults.Count > context.Options.FaultBuffer)
                runtime.Faults.Dequeue();
        }

        if (IsFence(ex) || context.Storage.IsFenced || !Leads)
        {
            // The node is ending, or no longer leads: a restart could commit nothing. The
            // failure is what being replaced looks like from inside a circuit, so it is
            // logged as that and not as a fault of the pipeline's.
            _logger.LogWarning(
                "Pipeline {Pipeline} generation {Generation} ended because this node no longer leads ({Error}: {Message})",
                runtime.Id, runtime.Spec.Generation, ex.GetType().Name, ex.Message);
            Set(runtime, PipelineState.Stopped, "this node no longer leads");
            return;
        }
        if (during is null)
            _logger.LogError(ex, "Pipeline {Pipeline} generation {Generation} faulted", runtime.Id, runtime.Spec.Generation);
        else
            _logger.LogError(ex, "Pipeline {Pipeline} generation {Generation}: {During}; it will be tried again", runtime.Id, runtime.Spec.Generation, during);
        var prefix = during is null ? "" : during + ": ";
        if (ex is ServiceException { Code: "too-many-plugin-versions" })
        {
            Set(runtime, PipelineState.Failed, prefix + ex.Message);
            return;
        }
        if (during is null && ex.Message.Contains("no longer retained", StringComparison.Ordinal))
        {
            // Cut loose: a table's history no longer reaches back to this pipeline's checkpoint.
            _logger.LogError("Pipeline {Pipeline} was cut loose from a table's history and needs a reset: {Message}", runtime.Id, ex.Message);
            Set(runtime, PipelineState.NeedsReset, ex.Message);
            return;
        }

        var policy = runtime.Spec.Restart ?? context.Options.Restart;
        var now = DateTimeOffset.UtcNow;
        runtime.FaultTimes.Enqueue(now);
        while (runtime.FaultTimes.Count > 0 && now - runtime.FaultTimes.Peek() > policy.Within)
            runtime.FaultTimes.Dequeue();
        if (runtime.FaultTimes.Count > policy.MaxRestarts)
        {
            // No colon before the exception's own text: what follows the first colon of a
            // reason is treated as a message, and hidden where row data is.
            Set(runtime, PipelineState.Failed,
                $"{prefix}{runtime.FaultTimes.Count} faults within {Durations.Text(policy.Within)}; the last: {ex.GetType().Name}: {ex.Message}");
            return;
        }

        var delay = policy.BackoffAfter(runtime.FaultTimes.Count);
        runtime.BackoffUntil = now + delay;
        runtime.Restarts++;
        Set(runtime, PipelineState.Backoff, $"{prefix}{ex.GetType().Name}: {ex.Message}");
        ScheduleRetry(runtime, delay, PipelineState.Backoff);
    }

    private static bool IsFence(Exception? ex)
    {
        for (; ex is not null; ex = ex.InnerException)
            if (ex is ChangelogFencedException or TableLogFencedException or Incremental.Kafka.KafkaLogFencedException)
                return true;
        return false;
    }

    private void ScheduleRetry(PipelineRuntime runtime, TimeSpan delay, PipelineState expected)
    {
        var retry = runtime.Retry = new CancellationTokenSource();
        Task.Run(async () =>
        {
            try
            {
                await Task.Delay(delay, retry.Token).ConfigureAwait(false);
                await runtime.Queue.WaitAsync(retry.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
            try
            {
                if (retry.IsCancellationRequested || runtime.State != expected || !runtime.Exists || !Attached(runtime) || !Leads)
                    return;
                runtime.Operation = "restart";

                // Not a plain start: a pending reset or removal is what gets retried.
                await ReconcileAsync(runtime).ConfigureAwait(false);
            }
            finally
            {
                runtime.Operation = null;
                runtime.Queue.Release();
            }
        }).Forget();
    }

    private static void CancelRetry(PipelineRuntime runtime)
    {
        runtime.Retry?.Cancel();
        runtime.Retry = null;
    }

    private async Task StopCurrentAsync(PipelineRuntime runtime, TimeSpan stopTimeout)
    {
        CancelRetry(runtime);
        if (runtime.Current is { } generation)
        {
            Set(runtime, PipelineState.Stopping);
            await TeardownAsync(runtime, generation, stopTimeout).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Stop order: sources, then the circuit — which drains while the deliveries still
    /// read, and whose final commit waits for their acknowledgements — then the deliveries.
    /// Bounded: after <paramref name="stopTimeout"/> the outputs are abandoned, so the
    /// final commit is skipped; a circuit still not disposed
    /// <see cref="ServiceOptions.AbandonTimeout"/> later leaves the pipeline <c>Wedged</c>.
    /// </summary>
    private async Task<bool> TeardownAsync(PipelineRuntime runtime, Generation generation, TimeSpan stopTimeout)
    {
        generation.Stopping = true;
        generation.Faulted.TrySetResult(null);
        if (ReferenceEquals(runtime.Current, generation))
            runtime.Current = null;

        var abandon = context.Options.AbandonTimeout;
        foreach (var (slot, source) in generation.Sources)
        {
            try
            {
                if (!await source.DisposeAsync().AsTask().CompletesWithin(abandon).ConfigureAwait(false))
                    _logger.LogWarning("Pipeline {Pipeline}: the source of slot {Slot} did not stop within {Timeout}", runtime.Id, slot, abandon);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Pipeline {Pipeline}: stopping the source of slot {Slot} failed", runtime.Id, slot);
            }
        }

        var dispose = generation.Circuit.DisposeAsync().AsTask();
        if (!await dispose.CompletesWithin(stopTimeout).ConfigureAwait(false))
        {
            // No drain was asked for when the bound is zero (a node that no longer leads
            // abandons): that is not a pipeline that failed to drain.
            if (stopTimeout > TimeSpan.Zero)
                _logger.LogWarning("Pipeline {Pipeline} did not drain within {Timeout}; abandoning its outputs", runtime.Id, stopTimeout);
            Abandon(generation);
            if (!await dispose.CompletesWithin(abandon).ConfigureAwait(false))
            {
                dispose.Forget();
                runtime.Wedged = true;
                _logger.LogError(
                    "Pipeline {Pipeline} is Wedged: its circuit is still inside a step {Timeout} after its outputs were abandoned. " +
                    "No new generation is built for it on this node; restart this node to reclaim it.", runtime.Id, abandon);
                Set(runtime, PipelineState.Wedged, "the circuit did not stop; restart this node to reclaim it");
                return false;
            }
        }
        try
        {
            await dispose.ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Pipeline {Pipeline}: disposing the circuit failed", runtime.Id);
        }

        var edges = Task.WhenAll(generation.Deliveries.Append(generation.DeadLetters));
        if (!await edges.CompletesWithin(abandon).ConfigureAwait(false))
            _logger.LogWarning("Pipeline {Pipeline}: a delivery did not end within {Timeout} of the circuit", runtime.Id, abandon);
        edges.Forget();
        generation.Stop.Cancel();
        await DisposeSinksAsync(runtime, generation).ConfigureAwait(false);
        return true;
    }

    /// <summary>Sinks a connector made for the generation and that own something (a Kafka
    /// sink's producer): disposed last, once the deliveries have ended.</summary>
    private async Task DisposeSinksAsync(PipelineRuntime runtime, Generation generation)
    {
        object[] owned;
        lock (generation.OwnedSinks)
        {
            owned = [.. generation.OwnedSinks];
            generation.OwnedSinks.Clear();
        }
        foreach (var sink in owned)
        {
            try
            {
                if (sink is IAsyncDisposable asyncDisposable)
                {
                    if (!await asyncDisposable.DisposeAsync().AsTask().CompletesWithin(context.Options.AbandonTimeout).ConfigureAwait(false))
                        _logger.LogWarning("Pipeline {Pipeline}: a sink did not close within {Timeout}", runtime.Id, context.Options.AbandonTimeout);
                }
                else if (sink is IDisposable disposable)
                    disposable.Dispose();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Pipeline {Pipeline}: closing a sink failed", runtime.Id);
            }
        }
    }

    // ── the node's calls ───────────────────────────────────────────────────────────────

    /// <summary>A standby's view: what the catalog deploys, none of it built. Its state is
    /// followed as bytes and its circuits are built at takeover.</summary>
    internal void SyncFromCatalog(IReadOnlyDictionary<string, string> catalog)
    {
        var specs = catalog.Where(e => e.Key.StartsWith("pipeline/", StringComparison.Ordinal))
            .Select(e => ServiceJson.Read<PipelineSpec>(e.Value)).ToDictionary(s => s.Id, StringComparer.Ordinal);
        foreach (var (id, runtime) in _pipelines)
        {
            // A placeholder belongs to the deploy that made it, which drops it itself.
            if (specs.ContainsKey(id) || !runtime.Exists || runtime.Current is not null || runtime.Starting is not null)
                continue;
            runtime.Exists = false;
            _pipelines.TryRemove(new KeyValuePair<string, PipelineRuntime>(id, runtime));
        }
        foreach (var spec in specs.Values)
        {
            var runtime = _pipelines.GetOrAdd(spec.Id, _ => new PipelineRuntime(spec, context.Options));
            runtime.Spec = spec;
            runtime.Exists = true;
        }
    }

    /// <summary>The supervisor's part of a takeover: finish every pending reset and
    /// removal, then start every pipeline whose desired state is Running, in parallel,
    /// each contained. Only the takeover's own cancellation leaves this method: whatever
    /// a pipeline throws, cancellations of its own included, is that pipeline's fault.</summary>
    internal async Task StartLeadingAsync(IReadOnlyDictionary<string, string> catalog, CancellationToken cancellationToken)
    {
        SyncFromCatalog(catalog);
        _accepting = true;
        await Task.WhenAll(_pipelines.Values.Select(async runtime =>
        {
            using var exit = await EnterAsync(runtime, "takeover", cancellationToken).ConfigureAwait(false);
            if (!Attached(runtime) || !runtime.Exists)
                return;
            try
            {
                await ReconcileAsync(runtime).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Pipeline {Pipeline} could not be taken over", runtime.Id);
                HandleFault(runtime, ex, runtime.Generations);
            }
        })).ConfigureAwait(false);
    }

    /// <summary>Stops every pipeline in stop order and in parallel, within the budget.
    /// From its first line no action starts a generation here: one that was waiting for a
    /// pipeline's queue finds the node no longer running pipelines when it gets it.</summary>
    internal Task StopAllAsync(TimeSpan budget)
    {
        _accepting = false;
        var stopTimeout = budget < context.Options.StopTimeout ? budget : context.Options.StopTimeout;
        return Task.WhenAll(_pipelines.Values.Select(async runtime =>
        {
            // A start in flight is ended rather than waited for.
            CancelStart(runtime);
            if (!await runtime.Queue.WaitAsync(budget).ConfigureAwait(false))
            {
                _logger.LogWarning("Pipeline {Pipeline} is busy with '{Operation}' and was not stopped", runtime.Id, runtime.Operation);
                return;
            }
            try
            {
                runtime.Operation = "stop";
                await StopCurrentAsync(runtime, stopTimeout).ConfigureAwait(false);
                if (!runtime.Wedged)
                    Set(runtime, PipelineState.Stopped);
            }
            finally
            {
                runtime.Operation = null;
                runtime.Queue.Release();
            }
        }));
    }

    private static void CancelStart(PipelineRuntime runtime)
    {
        try
        {
            runtime.Starting?.Stop.Cancel();
        }
        catch (ObjectDisposedException)
        {
        }
    }

    /// <summary>
    /// A fenced node's circuits can commit nothing: abandon them without draining. What
    /// needs no queue comes first — outputs abandoned, a start in flight cancelled — so
    /// that whatever holds a pipeline's queue ends at once; then each generation is torn
    /// down under its queue, like every other teardown. An operation that still holds the
    /// queue after <see cref="ServiceOptions.StopTimeout"/> is left to end the generation
    /// itself, which it does: it finds the node no longer leading.
    /// </summary>
    internal Task AbandonAllAsync()
    {
        _accepting = false;
        return Task.WhenAll(_pipelines.Values.Select(async runtime =>
        {
            if (runtime.Current is { } running)
            {
                running.Logs?.NoLongerLeading();
                Abandon(running);
            }
            runtime.Starting?.Logs?.NoLongerLeading();
            CancelStart(runtime);
            if (!await runtime.Queue.WaitAsync(context.Options.StopTimeout).ConfigureAwait(false))
            {
                _logger.LogWarning("Pipeline {Pipeline} is busy with '{Operation}'; that operation ends its generation", runtime.Id, runtime.Operation);
                return;
            }
            try
            {
                runtime.Operation = "abandon";
                CancelRetry(runtime);
                if (runtime.Current is { } generation)
                {
                    Abandon(generation);
                    await TeardownAsync(runtime, generation, TimeSpan.Zero).ConfigureAwait(false);
                }
                if (!runtime.Wedged && runtime.State is PipelineState.Running or PipelineState.Starting or PipelineState.Stopping
                        or PipelineState.Backoff or PipelineState.Blocked)
                    Set(runtime, PipelineState.Stopped, "this node no longer leads");
            }
            finally
            {
                runtime.Operation = null;
                runtime.Queue.Release();
            }
        }));
    }

    /// <summary>A table was opened from its log again: the generations built on the old
    /// instance hold it, so they are rebuilt.</summary>
    internal void OnTableReopened(string table)
    {
        if (!Leads)
            return;
        foreach (var runtime in _pipelines.Values.Where(p => p.Spec.Reads.Contains(table) || p.Spec.Writes.Contains(table)))
        {
            Task.Run(async () =>
            {
                using var exit = await EnterAsync(runtime, "table reopened", CancellationToken.None).ConfigureAwait(false);
                if (!Attached(runtime) || !runtime.Exists || !Leads)
                    return;
                if (runtime.Current is null && runtime.State != PipelineState.Blocked)
                    return;
                await StopCurrentAsync(runtime, context.Options.StopTimeout).ConfigureAwait(false);
                if (!runtime.Wedged)
                    await ReconcileAsync(runtime).ConfigureAwait(false);
            }).Forget();
        }
    }

    internal void ReleaseAll()
    {
        foreach (var runtime in _pipelines.Values)
        {
            CancelRetry(runtime);
            if (runtime.Current is null && !runtime.Wedged)
            {
                try
                {
                    ReleaseBinding(runtime);
                }
                catch
                {
                    // Shutdown.
                }
            }
        }
    }
}
