using System.Diagnostics;
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
    public required long Number { get; set; }
    public required Circuit Circuit { get; init; }
    public required PipelineBuilder Builder { get; init; }
    public List<(string Slot, IRunningSource Source)> Sources { get; } = [];
    public List<Task> Deliveries { get; } = [];
    public Dictionary<string, SinkSlotStats> SinkStats { get; } = new(StringComparer.Ordinal);
    public List<object> OwnedSinks { get; } = [];
    public Task DeadLetters { get; set; } = Task.CompletedTask;

    /// <summary>The dead-letter sink's counters, when the spec binds one. Apart from the
    /// slots' because what it delivers is not the pipeline's output.</summary>
    public SinkSlotStats? DeadLetterStats { get; set; }
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

    /// <summary>The dead-letter sink's last failed write, kept across generations.</summary>
    public volatile Tuple<string, DateTimeOffset>? DeadLetterSinkError;
    public long DeadLetterSinkFailures;
    public volatile string? BlockedOn;
    public DateTimeOffset? BackoffUntil;
    public CancellationTokenSource? Retry;
    public volatile bool Wedged;
    public long Generations;

    /// <summary>The last replace of this pipeline that was undone here.</summary>
    public volatile PipelineRollback? RolledBack;

    /// <summary>What the state of <see cref="Spec"/> is made of, as the last build or dry
    /// run of that spec on this node described it; null where it never was.</summary>
    public volatile StateShape? StateShape;

    /// <summary>Where the last start of a generation on this node spent its time.</summary>
    public volatile PipelineStartTimings? LastStart;

    /// <summary>A standby's circuit for this pipeline: built on the state it follows, not
    /// started. A takeover starts it instead of building one.</summary>
    public volatile WarmGeneration? Warm;

    /// <summary>Why this standby could not build the pipeline, the last time it tried.</summary>
    public volatile string? WarmFault;

    /// <summary>When a failed standby build may be tried again, and the spec generation it
    /// failed for: a new spec is tried at once.</summary>
    public DateTimeOffset WarmRetryAt;
    public long WarmFaultGeneration = -1;

    /// <summary>A table the pipeline reads was opened again since <see cref="Warm"/> was built.</summary>
    public volatile bool WarmStale;

    /// <summary>The provider's store epoch when the pipeline's typed stores were bound: a
    /// later one says they have been given up since.</summary>
    public long BoundEpoch = -1;
}

/// <summary>A circuit a standby built ahead of a takeover, and what it was built on.</summary>
internal sealed record WarmGeneration(Generation Generation, string Layout, long StoreEpoch, long SpecGeneration);

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
            PipelineState.Running when generation?.DeadLetterStats?.Status(PipelineSpec.DeadLetterSlot).WriteInFlightSince is { } since
                && DateTimeOffset.UtcNow - since > context.Options.StallAfter
                => $"sink-ack:{PipelineSpec.DeadLetterSlot}",
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
            generation?.Circuit.CommittedStep, committed, faults, runtime.Shape, sources, sinks, runtime.History.After(),
            runtime.RolledBack, spec.Previous is not null, DeadLetterSink(runtime, spec, generation), runtime.LastStart);
    }

    private DeadLetterSinkStatus? DeadLetterSink(PipelineRuntime runtime, PipelineSpec spec, Generation? generation)
    {
        if (spec.DeadLetters is not { } config)
            return null;
        var stats = generation?.DeadLetterStats?.Status(PipelineSpec.DeadLetterSlot);
        var error = runtime.DeadLetterSinkError;
        return new DeadLetterSinkStatus(
            ConnectorName(config), stats?.Appends ?? 0, stats?.Batches ?? 0, stats?.WriteInFlightSince,
            error?.Item1, error?.Item2, Interlocked.Read(ref runtime.DeadLetterSinkFailures));
    }

    public PipelineGraph? Graph(string id) => Require(id).Graph;

    public PipelineSpec RedactedSpec(string id) => Shown(Require(id).Spec);

    /// <summary>A spec as it may leave the service: secrets as <c>***</c>, and without the
    /// spec it replaced, which is the service's own note.</summary>
    private PipelineSpec Shown(PipelineSpec spec) =>
        spec with
        {
            Inputs = Redact(spec.Inputs), Sinks = Redact(spec.Sinks), Previous = null,
            DeadLetters = spec.DeadLetters is { } config ? Redact(One(config))[PipelineSpec.DeadLetterSlot] : null,
        };

    private static Dictionary<string, JsonElement> One(JsonElement deadLetters) =>
        new(StringComparer.Ordinal) { [PipelineSpec.DeadLetterSlot] = deadLetters };

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
            DeadLetters = spec.DeadLetters is { } config
                ? RestoreSecrets(One(config), stored?.DeadLetters is { } old ? One(old) : null, "sink")[PipelineSpec.DeadLetterSlot]
                : null,
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
            long inFlightSince = 0;
            if (generation is not null)
            {
                foreach (var stats in generation.SinkStats.Values)
                {
                    records += Interlocked.Read(ref stats.Upserts) + Interlocked.Read(ref stats.Deletes) + Interlocked.Read(ref stats.Appends);
                    var since = Interlocked.Read(ref stats.InFlightSinceTicks);
                    if (since != 0 && (inFlightSince == 0 || since < inFlightSince))
                        inFlightSince = since;
                }
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
            into.Add(new PipelineSampleInput(
                runtime.Id, generation?.Number ?? 0, records, lag, runtime.DeadLetters.Total,
                runtime.State, runtime.Spec.Desired == DesiredState.Running, inFlightSince));
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
            var (shape, graph, _, _) = DryRun(resolved, lease);
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
    /// only at <c>Circuit.Build()</c>. With <paramref name="againstState"/> it also asks
    /// whether the circuit could resume the pipeline's committed checkpoint
    /// (<see cref="CircuitBuilder.CheckState"/>): what <c>Circuit.Start()</c> would refuse,
    /// known before anything is stopped.</summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private (PipelineShape Shape, PipelineGraph Graph, IReadOnlyList<string> StateProblems, StateShape? State) DryRun(
        PipelineSpec spec, PluginLease lease, bool againstState = false, bool describeState = false)
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
        IReadOnlyList<string> problems = [];
        if (againstState)
        {
            try
            {
                problems = builder.CheckState(context.Storage.Pipeline(spec.Id));
            }
            catch (Exception ex)
            {
                // Not decided here, then: the start decides, and a refusal there rolls back.
                _logger.LogDebug(ex, "Pipeline {Pipeline}: the state layout of the dry run could not be described", spec.Id);
            }
        }
        return (pipeline.Shape, pipeline.Graph, problems, describeState ? TryDescribeState(pipeline) : null);
    }

    private StateShape? TryDescribeState(PipelineBuilder pipeline)
    {
        try
        {
            return StateShapes.Describe(pipeline.Circuit.DescribeState(), pipeline.TableTypes);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Pipeline {Pipeline}: its state could not be described", pipeline.PipelineId);
            return null;
        }
    }

    /// <summary>
    /// What a person choosing between keep and reset has to go on: the state of the spec
    /// that is running against the state of the one proposed — which stores are new or
    /// gone, which types they hold changed, in which members, and whether a changed type
    /// identifies entries or is only a value. Only what state depends on is compared; a
    /// sink's row type gaining a column changes no line here.
    /// </summary>
    private async Task<List<string>> StateEvidenceAsync(PipelineRuntime runtime, PipelineSpec existing, StateShape? next)
    {
        var old = runtime.StateShape;
        if (old is null)
        {
            // Never built here (Failed since this node took over, or stopped): describe it now.
            try
            {
                var lease = runtime.Lease is { } bound && bound.Name == existing.Plugin && bound.Version == existing.Version
                    ? null
                    : await context.Plugins.AcquireAsync(existing.Plugin, existing.Version, CancellationToken.None).ConfigureAwait(false);
                using (lease)
                    old = DescribeExisting(existing, lease ?? runtime.Lease!);
            }
            catch (Exception ex)
            {
                return [$"state shapes: not compared — the spec that is deployed could not be described ({ex.GetType().Name}: {ex.Message})"];
            }
        }
        if (old is null || next is null)
            return ["state shapes: not compared — the circuit's state could not be described"];
        var differences = StateShapes.Differences(old, next);
        return differences.Count == 0 ? [StateShapes.Unchanged(next)] : [.. differences.Select(d => "state shapes: " + d)];
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private StateShape? DescribeExisting(PipelineSpec spec, PluginLease lease)
    {
        var definition = lease.Definition(spec.Definition);
        var settings = definition.BindSettings(spec.Settings, lease.Json);
        var builder = new CircuitBuilder(new CircuitOptions { Name = spec.Id + " (described)", Workers = spec.Circuit.Workers });
        var pipeline = new PipelineBuilder(spec, builder, context.Tables, _logger, context.Services);
        definition.Build(pipeline, settings);
        return TryDescribeState(pipeline);
    }

    private static ServiceException StateIncompatible(string id, IReadOnlyList<string> problems) => new(
        "state-incompatible",
        $"Pipeline '{id}' cannot keep its state across this change: the new circuit could not resume the committed checkpoint. " +
        "Nothing was changed. Deploy it with state=reset to start it from empty state, or leave out what the checkpoint cannot follow " +
        "(a worker count, a renamed or removed stateful operator).",
        409, [.. problems.Select(Flat)]);

    /// <summary>A problem on one line: the core lists store names one per line.</summary>
    private static string Flat(string problem) =>
        string.Join(" ", problem.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries));

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
                if (connector is FileConnector)
                    FileConnector.RequireKind(config, kind);
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
            errors.Add(bound == PipelineSpec.DeadLetterSlot
                ? "The dead-letter sink is bound by the spec's own \"deadLetters\" member, not under \"sinks\"."
                : $"The spec binds sink slot '{bound}', which definition '{spec.Definition}' does not declare.");
        if (spec.DeadLetters is { } deadLetters)
            Check(PipelineSpec.DeadLetterSlot, SlotKind.AppendSink, One(deadLetters), "dead-letter");
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
            changes.Add($"version: {old.Version} → {next.Version}");
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
            {
                // The choice is the caller's, but one of the two may already be closed:
                // say so with the question, not after it has been answered.
                try
                {
                    var asked = DryRun(spec, lease, againstState: true, describeState: true);
                    changes = [.. changes, .. await StateEvidenceAsync(runtime, existing!, asked.State).ConfigureAwait(false)];
                    if (asked.StateProblems is { Count: > 0 } closed)
                        changes = [.. changes, .. closed.Select(p => "keep is not possible: " + Flat(p))];
                }
                catch (ServiceException)
                {
                    // A spec that does not bind is refused for that once a choice is made.
                }
                throw new ServiceException(
                    "state-choice-required",
                    $"This change to pipeline '{spec.Id}' can change what its state means; say whether to keep the state or reset it.", 409, changes);
            }

            var reset = existing is not null && options.State == StateChoice.Reset;
            var (shape, graph, stateProblems, stateShape) = DryRun(spec, lease, againstState: existing is not null && !reset, describeState: true);
            if (stateProblems.Count > 0)
                throw StateIncompatible(spec.Id, stateProblems);
            RequireReferences(spec, shape);
            var evidence = existing is not null && changes.Count > 0 && !reset
                ? await StateEvidenceAsync(runtime, existing, stateShape).ConfigureAwait(false)
                : [];

            var steps = new List<string>();
            if (existing is null)
                steps.Add($"deploy generation 1 of '{spec.Definition}' from {spec.Plugin}@{spec.Version}; state starts empty");
            else
            {
                steps.Add($"stop generation {existing.Generation} (drain, commit)");
                steps.AddRange(changes.Select(c => "changes " + c));
                steps.AddRange(evidence);
                steps.Add(reset ? "state dropped: a new incarnation; sources start again from their start position; upsert sinks keep rows the new run does not produce"
                    : changes.Count > 0 ? "state kept: the new generation reads the old one's entries as its own types"
                    : "state kept: nothing that touches it changed");
                if (!reset && changes.Count > 0)
                    steps.Add($"if the new generation cannot read the state it is given, the spec of generation {(existing.Previous ?? existing).Generation} is put back and started");
            }
            steps.Add(spec.Desired == DesiredState.Running ? "start" : "leave stopped");
            var plan = new ActionPlan(existing is null ? "pipeline.deploy" : "pipeline.replace", spec.Id, options.DryRun, steps);
            if (options.DryRun)
                return new DeployResult(Shown(spec with { Generation = existing?.Generation ?? 0 }), shape, plan);

            // Intent first: from here a successor knows everything. The tables and the
            // plugin version the spec names are checked again under the lock their removal
            // takes, so the entry is never written beside the removal of what it names.
            spec = spec with
            {
                Generation = (existing?.Generation ?? 0) + 1,
                Reads = shape.TablesRead,
                Writes = shape.TablesWritten,
                Pending = reset ? "reset" : null,

                // What to go back to if the kept state turns out not to fit. A spec that
                // itself never ran is not it: the state is still its predecessor's.
                Previous = existing is not null && !reset && changes.Count > 0 ? (existing.Previous ?? existing) with { Previous = null } : null,
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
            runtime.StateShape = stateShape;
            runtime.RolledBack = null;
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

            // The replace happened and was undone: the caller is told it did not take,
            // with what the state would not accept, rather than handed a plan that reads
            // as done.
            if (runtime.RolledBack is { } undone && undone.From == spec.Generation)
                throw new ServiceException(
                    "state-incompatible",
                    $"Pipeline '{spec.Id}' could not keep its state across this change, so the change was rolled back: generation {undone.From} " +
                    $"({undone.Plugin}@{undone.Version}, '{undone.Definition}') could not be started on the state, the spec of generation " +
                    $"{undone.Restored} was put back as generation {undone.Now}, and the pipeline is {runtime.State}. " +
                    "Deploy the change with state=reset to start it from empty state.",
                    409, undone.Problems);
            return new DeployResult(Shown(runtime.Spec), shape, plan);
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
        provider.UseLayout(LayoutOf(runtime.Spec));
        runtime.BoundEpoch = provider.StoreEpoch;
        runtime.Lease = lease;
    }

    /// <summary>
    /// The identity of everything that decides what a pipeline's typed stores are: the
    /// plugin version, the definition, its settings, the worker count, the error policy
    /// and the tables its slots are bound to. Every record the pipeline writes carries it
    /// (<see cref="ChangelogStorageProvider.UseLayout"/>), which is what lets a standby
    /// hold the state in typed stores: it gives them up at the first record of another
    /// layout. Two specs with the same layout build the same stores.
    /// </summary>
    internal static string LayoutOf(PipelineSpec spec)
    {
        var text = string.Join('\n',
            spec.Plugin, spec.Version, spec.Definition, spec.Settings is { } settings ? Canonical(settings) : "",
            spec.Circuit.Workers.ToString(System.Globalization.CultureInfo.InvariantCulture), spec.Circuit.ErrorPolicy.ToString(),
            string.Join(',', spec.Tables.OrderBy(t => t.Key, StringComparer.Ordinal).Select(t => t.Key + "=" + t.Value)));
        return Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(text)), 0, 6).ToLowerInvariant();
    }

    /// <summary>Whether the pipeline's typed stores are still the ones bound for
    /// <paramref name="spec"/>: same plugin version, same layout, not given up since.</summary>
    private bool BoundFor(PipelineRuntime runtime, PipelineSpec spec, ChangelogStorageProvider provider) =>
        runtime.Lease is { } bound && bound.Name == spec.Plugin && bound.Version == spec.Version
        && provider.Layout == LayoutOf(spec) && provider.StoreEpoch == runtime.BoundEpoch;

    /// <summary>Turns the pipeline's typed stores back into bytes and lets go of the
    /// plugin version they belonged to, which is what allows that version to unload.</summary>
    private void ReleaseBinding(PipelineRuntime runtime)
    {
        if (runtime.Wedged)
            return; // The stores still belong to a circuit that will not stop.
        DiscardWarm(runtime);
        var provider = context.Storage.Pipeline(runtime.Id);
        provider.ReleaseStores();
        provider.UseSerializer(JsonStateSerializer.Instance);
        provider.UseLayout(null);
        runtime.BoundEpoch = -1;
        runtime.Lease?.Dispose();
        runtime.Lease = null;
    }

    /// <summary>Drops a standby's circuit. It was never started, so there is nothing to
    /// drain: disposing it completes its outputs and frees what it claimed.</summary>
    private void DiscardWarm(PipelineRuntime runtime)
    {
        if (Interlocked.Exchange(ref runtime.Warm, null) is not { } warm)
            return;
        try
        {
            warm.Generation.Circuit.DisposeAsync().AsTask().Forget();
            warm.Generation.Stop.Cancel();
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Pipeline {Pipeline}: dropping the standby's circuit failed", runtime.Id);
        }
        runtime.WarmStale = false;
    }

    /// <summary>The standby's circuit, if it is still good for <paramref name="spec"/>:
    /// built for this very spec, on stores that have not been given up, over tables that
    /// have not been opened again. Otherwise it is dropped and null is returned.</summary>
    private Generation? TakeWarm(PipelineRuntime runtime, PipelineSpec spec, ChangelogStorageProvider provider)
    {
        if (runtime.Warm is not { } warm)
            return null;
        if (!runtime.WarmStale && warm.SpecGeneration == spec.Generation && warm.Layout == LayoutOf(spec)
            && warm.StoreEpoch == provider.StoreEpoch && BoundFor(runtime, spec, provider)
            && ReferenceEquals(Interlocked.CompareExchange(ref runtime.Warm, null, warm), warm))
            return warm.Generation;
        DiscardWarm(runtime);
        return null;
    }

    /// <summary>
    /// A standby's work between takeovers: for every pipeline that should run and whose
    /// state is under its spec's layout, get the plugin, build the circuit on the followed
    /// state and keep it, unstarted. From then on the follower applies the leader's
    /// commits to the circuit's own stores, and a takeover starts the circuit — its cost
    /// no longer grows with the state. A pipeline that cannot be built here stays as bytes
    /// and is built at takeover as before; nothing a pipeline does here can fail the node.
    /// Returns how many pipelines are still to be built.
    /// </summary>
    internal async Task<int> WarmAsync(CancellationToken cancellationToken)
    {
        if (!context.Options.WarmStandbyPipelines)
            return 0;
        var results = await Task.WhenAll(_pipelines.Values.Select(runtime => Task.Run(async () =>
        {
            if (!runtime.Exists || _accepting || context.Leading)
                return 0;
            if (!await runtime.Queue.WaitAsync(0, cancellationToken).ConfigureAwait(false))
                return 1;
            try
            {
                runtime.Operation = "build (standby)";
                if (!Attached(runtime) || !runtime.Exists || _accepting || context.Leading)
                    return 0;
                var spec = runtime.Spec;
                var layout = LayoutOf(spec);
                var provider = context.Storage.Pipeline(runtime.Id);

                // A pipeline that writes a table is built at takeover: its circuit would
                // claim the table's writer while the table still follows.
                var wanted = spec.Desired == DesiredState.Running && spec.Pending is null && spec.Writes.Count == 0;
                if (runtime.Warm is { } warm
                    && (!wanted || runtime.WarmStale || warm.SpecGeneration != spec.Generation || warm.Layout != layout
                        || warm.StoreEpoch != provider.StoreEpoch || !BoundFor(runtime, spec, provider)))
                {
                    DiscardWarm(runtime);
                    Set(runtime, PipelineState.Unbuilt);
                }
                if (!wanted || runtime.Warm is not null)
                    return 0;

                // Until the leader has put the state under this layout there is nothing
                // to build on: the state is another version's, or the pipeline's first
                // generation has not started.
                if (provider.CommittedLayout != layout)
                    return 0;
                if (runtime.WarmFault is not null && runtime.WarmFaultGeneration == spec.Generation && DateTimeOffset.UtcNow < runtime.WarmRetryAt)
                    return 0;
                try
                {
                    var clock = Stopwatch.StartNew();
                    if (!BoundFor(runtime, spec, provider))
                    {
                        ReleaseBinding(runtime);
                        Bind(runtime, await context.Plugins.AcquireAsync(spec.Plugin, spec.Version, cancellationToken).ConfigureAwait(false));
                    }
                    var plugin = clock.Elapsed;
                    var epoch = provider.StoreEpoch;
                    var generation = Build(runtime, 0);
                    if (provider.StoreEpoch != epoch)
                    {
                        // The state moved to another layout while this was built.
                        generation.Circuit.DisposeAsync().AsTask().Forget();
                        return 1;
                    }
                    if (!Attached(runtime) || !runtime.Exists)
                    {
                        // Removed from the catalog while this was built.
                        generation.Circuit.DisposeAsync().AsTask().Forget();
                        ReleaseBinding(runtime);
                        return 0;
                    }
                    runtime.WarmStale = false;
                    runtime.WarmFault = null;
                    runtime.Warm = new WarmGeneration(generation, layout, epoch, spec.Generation);
                    runtime.LastStart = new PipelineStartTimings(
                        DateTimeOffset.UtcNow, 0, plugin, clock.Elapsed - plugin, TimeSpan.Zero, TimeSpan.Zero, clock.Elapsed);
                    Set(runtime, PipelineState.Built);
                    return 0;
                }
                catch (TableNotOpenException)
                {
                    return 1; // the table is still opening on this node
                }
                catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
                {
                    var cause = DefinitionBinding.Cause(ex);
                    var message = $"{cause.GetType().Name}: {cause.Message}";
                    var news = runtime.WarmFault != message || runtime.State != PipelineState.Unbuilt;
                    if (news)
                        _logger.LogWarning(cause, "Pipeline {Pipeline} could not be built on this standby; a takeover builds it from the followed state", runtime.Id);
                    runtime.WarmFault = message;
                    runtime.WarmFaultGeneration = spec.Generation;
                    runtime.WarmRetryAt = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(30);
                    try
                    {
                        ReleaseBinding(runtime);
                    }
                    catch (Exception release)
                    {
                        _logger.LogWarning(release, "Pipeline {Pipeline}: giving the standby's stores back failed", runtime.Id);
                    }
                    if (news)
                        Set(runtime, PipelineState.Unbuilt, "not built on this standby: " + message);
                    return 0;
                }
            }
            finally
            {
                runtime.Operation = null;
                runtime.Queue.Release();
            }
        }, cancellationToken))).ConfigureAwait(false);
        return results.Sum();
    }

    /// <summary>Pipelines this standby should have built and has not, failures aside.</summary>
    internal int WarmPending()
    {
        if (!context.Options.WarmStandbyPipelines)
            return 0;
        var pending = 0;
        foreach (var runtime in _pipelines.Values)
        {
            var spec = runtime.Spec;
            if (!runtime.Exists || runtime.Warm is not null || runtime.WarmFault is not null
                || spec.Desired != DesiredState.Running || spec.Pending is not null || spec.Writes.Count != 0)
                continue;
            if (context.Storage.Pipeline(runtime.Id).CommittedLayout == LayoutOf(spec))
                pending++;
        }
        return pending;
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
            var clock = Stopwatch.StartNew();
            var provider = context.Storage.Pipeline(runtime.Id);

            // What this node built while it stood by, if it is still good; else the
            // pipeline's stores under this spec's layout, and a circuit built on them.
            generation = TakeWarm(runtime, spec, provider);
            var warm = generation is not null;
            if (!warm && !BoundFor(runtime, spec, provider))
            {
                ReleaseBinding(runtime);
                Bind(runtime, await context.Plugins.AcquireAsync(spec.Plugin, spec.Version, CancellationToken.None).ConfigureAwait(false));
            }
            var plugin = clock.Elapsed;
            if (generation is null)
                generation = Build(runtime, number);
            else
                generation.Number = number;
            var built = clock.Elapsed;
            runtime.Starting = generation;

            // Before anything is written under it: a standby that reads this gives up
            // typed stores of any other layout, and may build this one's.
            await provider.CommitLayoutAsync().ConfigureAwait(false);
            generation.Circuit.Start();
            var started = clock.Elapsed;
            await StartEdgesAsync(runtime, generation).ConfigureAwait(false);
            runtime.LastStart = new PipelineStartTimings(
                DateTimeOffset.UtcNow, number, plugin, built - plugin, started - built, clock.Elapsed - started, clock.Elapsed, warm);

            // The start can take as long as a connector does, and the node may have been
            // fenced or stopped meanwhile. Whoever ended it could not see this generation
            // (Current did not name it yet), so it is ended here rather than published.
            if (!Leads)
                throw new NoLongerLeadingException();
            runtime.Current = generation;
            runtime.Starting = null;
            Set(runtime, PipelineState.Running);
            Watch(runtime, generation);
            if (runtime.Spec.Previous is not null)
                await ConfirmReplaceAsync(runtime).ConfigureAwait(false);
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
        catch (Exception ex) when (StateMismatch(ex) is { } mismatch)
        {
            // The circuit cannot be built on the committed state, or refuses to resume it.
            // Nothing ran and nothing was written, and trying again would find the same.
            runtime.Starting = null;
            if (generation is not null)
                await TeardownAsync(runtime, generation, context.Options.StopTimeout).ConfigureAwait(false);
            if (!runtime.Wedged)
                await OnStateMismatchAsync(runtime, mismatch, number).ConfigureAwait(false);
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

    private static CircuitStateMismatchException? StateMismatch(Exception? ex)
    {
        for (; ex is not null; ex = ex.InnerException)
            if (ex is CircuitStateMismatchException mismatch)
                return mismatch;
        return null;
    }

    /// <summary>The new generation of a replace that kept state is running: the spec it
    /// replaced is no longer needed. The entry's generation does not move — nothing a
    /// caller sent has changed. A failure leaves the note, which is cleared at the next
    /// start; until then a state mismatch would still roll back, and there can be none,
    /// because the state is now this spec's.</summary>
    private async Task ConfirmReplaceAsync(PipelineRuntime runtime)
    {
        try
        {
            var spec = runtime.Spec;
            await CommitAsync(runtime, spec with { Previous = null }, "pipeline.replace.confirmed", "service", null, CancellationToken.None)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Pipeline {Pipeline}: the replace runs, but recording that failed; it is recorded at the next start", runtime.Id);
        }
    }

    /// <summary>
    /// A generation that cannot start on the pipeline's committed state. If the spec is a
    /// replace that kept state and has not run, the spec it replaced is put back — one
    /// catalog commit — and started: the pipeline is down for the length of two starts,
    /// not until someone repairs it. A leader that dies before that commit leaves the
    /// unconfirmed spec, and its successor comes through here and does the same.
    /// With nothing to go back to the pipeline is <c>NeedsReset</c>: retrying would find
    /// the same state. Caller holds the queue.
    /// </summary>
    private async Task OnStateMismatchAsync(PipelineRuntime runtime, CircuitStateMismatchException mismatch, long number)
    {
        var spec = runtime.Spec;
        IReadOnlyList<string> problems = [.. mismatch.Problems.Select(Flat)];
        var reason = string.Join("; ", problems);
        ServiceMeter.PipelineFaults.Add(1, ServiceMeter.Tag("node", context.NodeId), ServiceMeter.Tag("pipeline", runtime.Id));
        lock (runtime.Faults)
        {
            runtime.Faults.Enqueue(new PipelineFault(DateTimeOffset.UtcNow, spec.Generation, mismatch.GetType().Name, reason, null, number));
            while (runtime.Faults.Count > context.Options.FaultBuffer)
                runtime.Faults.Dequeue();
        }

        // The stores this attempt created go back to the bytes they were made of.
        try
        {
            ReleaseBinding(runtime);
        }
        catch (Exception ex)
        {
            HandleFault(runtime, ex, number);
            return;
        }

        if (spec.Previous is not { } previous)
        {
            _logger.LogError(
                "Pipeline {Pipeline} generation {Generation} cannot start on its committed state and is not retried: {Reason}",
                runtime.Id, spec.Generation, reason);
            Set(runtime, PipelineState.NeedsReset,
                $"the circuit cannot resume the committed state ({reason}); deploy the spec that wrote the state with state=keep, or reset the pipeline");
            return;
        }
        if (!Leads)
        {
            Set(runtime, PipelineState.Stopped, "this node no longer leads");
            return;
        }

        try
        {
            if (context.Options.BeforeRollbackCommit is { } hook)
                await hook(runtime.Id).ConfigureAwait(false);
            var restored = previous with { Generation = spec.Generation + 1, Previous = null, Pending = null };
            List<string> steps =
            [
                $"generation {spec.Generation} ('{spec.Definition}' from {spec.Plugin}@{spec.Version}) could not start on the state it was to keep",
                .. problems.Select(p => "state: " + p),
                $"the spec of generation {previous.Generation} ('{previous.Definition}' from {previous.Plugin}@{previous.Version}) is put back as generation {restored.Generation}",
                restored.Desired == DesiredState.Running ? "start" : "leave stopped",
            ];
            await CommitAsync(runtime, restored, "pipeline.replace.rolled-back", "service", steps, CancellationToken.None).ConfigureAwait(false);
            runtime.RolledBack = new PipelineRollback(
                DateTimeOffset.UtcNow, spec.Generation, previous.Generation, restored.Generation, spec.Plugin, spec.Version, spec.Definition, problems);
            ServiceMeter.PipelineRollbacks.Add(1, ServiceMeter.Tag("node", context.NodeId), ServiceMeter.Tag("pipeline", runtime.Id));
            runtime.History.Add(new PipelineTransition(DateTimeOffset.UtcNow, PipelineState.Stopped, restored.Generation,
                $"rolled back to the spec of generation {previous.Generation}: {reason}"));
            _logger.LogWarning(
                "Pipeline {Pipeline}: generation {Generation} could not start on the kept state ({Reason}); rolled back to the spec of generation {Previous} as generation {Restored}",
                runtime.Id, spec.Generation, reason, previous.Generation, restored.Generation);

            // The fact before the colon, the state's detail after it: what follows the
            // first colon of an event is hidden where messages are.
            context.Events.Publish("pipeline", runtime.Id, $"rolled back to the spec of generation {previous.Generation}: {reason}");
        }
        catch (Exception ex)
        {
            // The catalog would not take it, or the node is going: tried again like any
            // other failure, and the next attempt comes through here again.
            HandleFault(runtime, ex, number, "roll-back not finished");
            return;
        }

        ClearFaults(runtime);
        await ReconcileAsync(runtime).ConfigureAwait(false);
    }

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
        generation.Circuit.AbandonDeadLetters();
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
            // With a dead-letter sink bound, a dropped record is in the sink before the
            // checkpoint that passes it.
            AcknowledgeDeadLetters = spec.DeadLetters is not null,
            LoggerFactory = logs,
        });
        var pipeline = new PipelineBuilder(spec, builder, context.Tables, _logger, context.Services);
        definition.Build(pipeline, settings);
        CheckBindings(spec, pipeline);
        runtime.Shape = pipeline.Shape;
        runtime.Graph = pipeline.Graph;
        runtime.StateShape = TryDescribeState(pipeline);
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
            Delivery = context.Options.SinkDelivery,
        };

        foreach (var sink in generation.Builder.Sinks)
        {
            generation.SinkStats[sink.Name] = new SinkSlotStats();
            var config = spec.Sinks[sink.Name];
            var delivery = sink.Start(context.Connector(config), For(sink.Name, config));
            generation.Deliveries.Add(delivery);
            generation.Observe(delivery);
        }

        if (spec.DeadLetters is { } deadLetterConfig)
        {
            ChannelReader<DeadLetterBatch>? batches = null;
            try
            {
                batches = circuit.DeadLetterBatches;
            }
            catch (InvalidOperationException)
            {
                // No operator of this circuit has the DeadLetter policy: nothing to deliver.
            }
            if (batches is not null)
            {
                generation.DeadLetters = DeliverDeadLettersAsync(runtime, generation, For(PipelineSpec.DeadLetterSlot, deadLetterConfig));
                generation.Observe(generation.DeadLetters);
            }
        }
        else
        {
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
        }

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
        var noted = new DeadLetterNotes();
        try
        {
            await foreach (var letter in reader.ReadAllAsync().ConfigureAwait(false))
                Remember(runtime, letter, Render(letter.Record, json), noted);
        }
        catch
        {
            // The channel fails with the circuit's fault, which the watcher handles.
        }
    }

    private sealed class DeadLetterNotes
    {
        public DateTimeOffset LastLogged = DateTimeOffset.MinValue;
        public int SinceLogged;
    }

    /// <summary>The ring, the count, and a line in the log every ten seconds at most.</summary>
    private void Remember(PipelineRuntime runtime, DeadLetter letter, string? record, DeadLetterNotes noted)
    {
        runtime.DeadLetters.Add(sequence => new DeadLetterRecord(
            sequence, DateTimeOffset.UtcNow, letter.Operator, letter.Weight,
            letter.Exception.GetType().Name, letter.Exception.Message, record));
        noted.SinceLogged++;
        var now = DateTimeOffset.UtcNow;
        if (now - noted.LastLogged < TimeSpan.FromSeconds(10))
            return;
        _logger.LogWarning("Pipeline {Pipeline} dropped {Count} record(s) to its dead letters; the last at {Operator}: {Message}",
            runtime.Id, noted.SinceLogged, letter.Operator, letter.Exception.Message);
        context.Events.Publish("deadletter", runtime.Id, $"{noted.SinceLogged} record(s) dropped at {letter.Operator}");
        (noted.LastLogged, noted.SinceLogged) = (now, 0);
    }

    /// <summary>
    /// With a dead-letter sink bound: every step's dropped records go to the sink as
    /// <see cref="DeadLetterEnvelope"/>s and are acknowledged when it has them, which is
    /// what the circuit's checkpoint waits for. The ring is filled on the way, so the
    /// newest are still to be read on this node. A write that keeps failing ends here,
    /// which faults the generation: the pipeline restarts from its checkpoint and drops
    /// and delivers the records again, and after too many faults it is Failed. That is
    /// the choice made for a sink that will not take dead letters — the pipeline stops
    /// and says why, rather than losing the one kind of record that has no other trace,
    /// or waiting without a word.
    /// </summary>
    private async Task DeliverDeadLettersAsync(PipelineRuntime runtime, Generation generation, SlotStart start)
    {
        var json = runtime.Lease!.Json;
        var noted = new DeadLetterNotes();
        var pipeline = runtime.Id;
        var specGeneration = runtime.Spec.Generation;
        var stats = generation.DeadLetterStats = new SinkSlotStats();
        // What each record of the batch being written was rendered as, for the ring and the envelope alike.
        var rendered = new Dictionary<DeadLetter, string?>(ReferenceEqualityComparer.Instance);
        IReadOnlyDictionary<string, long>? after = null;
        try
        {
            var connector = context.Connector(start.Config) as IAppendSinkConnector
                ?? throw ServiceException.Invalid($"The dead-letter sink's connector '{ConnectorName(start.Config)}' is not an append sink.");
            var sink = connector.Create<DeadLetterEnvelope>(start.Sink);
            start.Own(sink);
            var observed = new ObservedAppendSink<DeadLetterEnvelope>(context.Backend.Leadership.Leased(sink), stats);
            await generation.Circuit.DeliverDeadLettersToAsync(
                observed,
                letter => new DeadLetterEnvelope(
                    pipeline, specGeneration, letter.Step, DateTimeOffset.UtcNow, letter.Operator, letter.Weight,
                    letter.Exception.GetType().Name, letter.Exception.Message, SourceOf(letter.Record), rendered.GetValueOrDefault(letter), after),
                new SinkDeliveryOptions
                {
                    MaxRetries = (context.Options.SinkDelivery ?? new SinkDeliveryOptions()).MaxRetries,
                    InitialBackoff = (context.Options.SinkDelivery ?? new SinkDeliveryOptions()).InitialBackoff,
                    MaxBackoff = (context.Options.SinkDelivery ?? new SinkDeliveryOptions()).MaxBackoff,
                    OnFailedAttempt = (ex, _) =>
                    {
                        Interlocked.Increment(ref runtime.DeadLetterSinkFailures);
                        runtime.DeadLetterSinkError = Tuple.Create($"{ex.GetType().Name}: {ex.Message}", DateTimeOffset.UtcNow);
                    },
                },
                batch =>
                {
                    rendered.Clear();
                    after = Committed(pipeline);
                    foreach (var letter in batch.Letters)
                    {
                        var text = rendered[letter] = Render(letter.Record, json);
                        Remember(runtime, letter, text, noted);
                    }
                }).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // The circuit faults too, at its commit, and says the same: whichever of the
            // two the watcher sees first names the dead-letter sink and its failure.
            generation.Circuit.AbandonDeadLetters(ex);
            var root = ex is SinkDeliveryException { InnerException: { } last } ? last : ex;
            throw new InvalidOperationException(
                $"The circuit's dead letters could not be stored ({root.GetType().Name}: {root.Message}), so the pipeline stops rather " +
                $"than commit past records it dropped with no trace. Its dead-letter sink is '{ConnectorName(start.Config)}'; the " +
                "records are dropped and delivered again when it restarts.", ex);
        }
    }

    private IReadOnlyDictionary<string, long>? Committed(string pipeline)
    {
        try
        {
            var committed = context.Storage.Pipeline(pipeline).CommittedOffsets;
            return committed.Count == 0 ? null : new Dictionary<string, long>(committed.Where(c => c.Key != "$step"), StringComparer.Ordinal);
        }
        catch (ObjectDisposedException)
        {
            return null;
        }
    }

    /// <summary>Where a record a source rejected lies in that source; null for a record
    /// an operator dropped.</summary>
    private static string? SourceOf(object? record) => record switch
    {
        Incremental.Kafka.KafkaRecord kafka => $"{kafka.Topic}[{kafka.Partition}]@{kafka.Offset}",
        FileRecord file => $"{file.Object}:{file.Line + 1}",
        _ => null,
    };

    private static string? Render(object? record, JsonSerializerOptions json)
    {
        if (record is null)
            return null;

        // A record a Kafka source could not decode: where it is, then what it holds.
        if (record is FileRecord line)
            return line.ToString();
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
            runtime.Faults.Enqueue(new PipelineFault(DateTimeOffset.UtcNow, runtime.Spec.Generation, ex.GetType().Name, ex.Message, ex.StackTrace, generation));
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
        ServiceMeter.PipelineFaults.Add(1, ServiceMeter.Tag("node", context.NodeId), ServiceMeter.Tag("pipeline", runtime.Id));
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
        ServiceMeter.PipelineRestarts.Add(1, ServiceMeter.Tag("node", context.NodeId), ServiceMeter.Tag("pipeline", runtime.Id));
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

    /// <summary>A standby's view: what the catalog deploys. <see cref="WarmAsync"/> builds
    /// what can be built ahead of a takeover; the rest is followed as bytes.</summary>
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

            // What this standby built for it goes too, and with it the plugin version it
            // held. A build that is running finds the pipeline gone and does the same.
            if (runtime.Queue.Wait(0))
            {
                try
                {
                    ReleaseBinding(runtime);
                }
                catch (Exception ex)
                {
                    _logger.LogDebug(ex, "Pipeline {Pipeline}: releasing a removed pipeline's stores failed", id);
                }
                finally
                {
                    runtime.Queue.Release();
                }
            }
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
        // Each on a thread of its own: building a circuit on followed bytes is synchronous
        // work, and one pipeline's must not wait for another's.
        await Task.WhenAll(_pipelines.Values.Select(runtime => Task.Run(async () =>
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
        }, cancellationToken))).ConfigureAwait(false);
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
        // A circuit a standby built holds the old instance too; it is built again.
        foreach (var runtime in _pipelines.Values.Where(p => p.Warm is not null && (p.Spec.Reads.Contains(table) || p.Spec.Writes.Contains(table))))
            runtime.WarmStale = true;
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
            DiscardWarm(runtime);
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
