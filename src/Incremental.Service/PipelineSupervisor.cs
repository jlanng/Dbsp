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
    public Task DeadLetters { get; set; } = Task.CompletedTask;
    public CancellationTokenSource Stop { get; } = new();
    public volatile bool Stopping;

    /// <summary>The first failure of the circuit, a source or a delivery; null when the
    /// generation was stopped on purpose.</summary>
    public TaskCompletionSource<Exception?> Faulted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public void Observe(Task task) =>
        task.ContinueWith(
            (t, state) => ((Generation)state!).Faulted.TrySetResult(t.Exception!.GetBaseException()),
            this, CancellationToken.None, TaskContinuationOptions.OnlyOnFaulted, TaskScheduler.Default);
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
            generation?.Circuit.CommittedStep, committed, faults, runtime.Shape, sources, sinks);
    }

    public IReadOnlyList<DeadLetterRecord> DeadLetters(string id, long afterSequence = 0, int limit = 256) =>
        Require(id).DeadLetters.After(afterSequence, limit);

    private void Set(PipelineRuntime runtime, PipelineState state, string? why = null)
    {
        if (runtime.State == state && why is null)
            return;
        runtime.State = state;
        runtime.Since = DateTimeOffset.UtcNow;
        if (state is not PipelineState.Blocked)
            runtime.BlockedOn = null;
        _logger.LogInformation("Pipeline {Pipeline} generation {Generation} is {State}{Why}",
            runtime.Id, runtime.Spec.Generation, state, why is null ? "" : ": " + why);
        context.Events.Publish("pipeline", runtime.Id, why is null ? state.ToString() : $"{state}: {why}");
    }

    // ── the dry run ────────────────────────────────────────────────────────────────────

    public async Task<PipelineShape> ValidateAsync(PipelineSpec spec, CancellationToken cancellationToken)
    {
        var (resolved, lease) = await ResolveAsync(spec, cancellationToken).ConfigureAwait(false);
        using (lease)
            return DryRun(resolved, lease);
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
    private PipelineShape DryRun(PipelineSpec spec, PluginLease lease)
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
            throw ServiceException.Invalid($"Definition '{spec.Definition}' threw while describing its circuit: {ex.GetType().Name}: {ex.Message}");
        }
        CheckBindings(spec, pipeline);
        return pipeline.Shape;
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
            changes.Add($"version: {old.Version} → {next.Version} (data shapes {(was == now ? "identical" : "differ")})");
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
            bindings.TryGetValue(slot, out var element) ? element.GetRawText() : null;

        static Dictionary<string, string> Properties(JsonElement? settings) =>
            settings is { ValueKind: JsonValueKind.Object } s
                ? s.EnumerateObject().ToDictionary(p => p.Name, p => p.Value.GetRawText(), StringComparer.OrdinalIgnoreCase)
                : new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
    }

    // ── actions ────────────────────────────────────────────────────────────────────────

    private async Task<IDisposable> EnterAsync(PipelineRuntime runtime, string operation, CancellationToken cancellationToken)
    {
        await runtime.Queue.WaitAsync(cancellationToken).ConfigureAwait(false);
        runtime.Operation = operation;
        return new Exit(runtime);
    }

    private sealed class Exit(PipelineRuntime runtime) : IDisposable
    {
        public void Dispose()
        {
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
        context.RequireLeader();
        ServiceNames.Validate(spec.Id, "pipeline id");
        if (spec.Desired == DesiredState.Removed)
            throw ServiceException.Invalid("A spec cannot be deployed as Removed; remove the pipeline instead.");
        var runtime = _pipelines.GetOrAdd(spec.Id, _ => new PipelineRuntime(spec, context.Options));
        PluginLease? lease = null;
        try
        {
            using var exit = await EnterAsync(runtime, "deploy", cancellationToken).ConfigureAwait(false);
            var existing = runtime.Exists ? runtime.Spec : null;
            RequireGeneration(runtime, existing is null && options.ExpectedGeneration is 0 ? null : options.ExpectedGeneration);
            if (existing?.Pending is not null)
                throw ServiceException.Conflict($"Pipeline '{spec.Id}' has an unfinished {existing.Pending}.");

            (spec, lease) = await ResolveAsync(spec, cancellationToken).ConfigureAwait(false);
            var changes = existing is null ? [] : StateChanges(existing, spec);
            if (changes.Count > 0 && options.State == StateChoice.Unspecified)
                throw new ServiceException(
                    "state-choice-required",
                    $"This change to pipeline '{spec.Id}' can change what its state means; say whether to keep the state or reset it.", 409, changes);

            var shape = DryRun(spec, lease);
            foreach (var table in shape.TablesWritten)
            {
                if (_pipelines.Values.FirstOrDefault(p => p.Exists && p.Id != spec.Id && p.Spec.Writes.Contains(table)) is { } other)
                    throw ServiceException.Conflict($"Table '{table}' is already written by pipeline '{other.Id}'; a table has one writer.");
                if (context.Tables.Find(table)?.Spec.Loader is { } loader)
                    throw ServiceException.Conflict($"Table '{table}' has a {loader.Kind} loader; a pipeline cannot also write it.");
            }

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

            // Intent first: from here a successor knows everything.
            spec = spec with
            {
                Generation = (existing?.Generation ?? 0) + 1,
                Reads = shape.TablesRead,
                Writes = shape.TablesWritten,
                Pending = reset ? "reset" : null,
            };
            await CommitAsync(runtime, spec, plan.Action, options.Caller, steps, cancellationToken).ConfigureAwait(false);
            runtime.Shape = shape;

            if (runtime.Current is not null || existing is not null)
                await StopCurrentAsync(runtime, context.Options.StopTimeout).ConfigureAwait(false);
            if (!runtime.Wedged)
            {
                if (changes.Count > 0)
                    ReleaseBinding(runtime);
                if (reset)
                    await FinishResetAsync(runtime, cancellationToken).ConfigureAwait(false);
                if (runtime.Lease is null)
                {
                    Bind(runtime, lease);
                    lease = null;
                }
                ClearFaults(runtime);
                if (runtime.Spec.Desired == DesiredState.Running)
                    await StartCoreAsync(runtime, cancellationToken).ConfigureAwait(false);
                else
                    Set(runtime, PipelineState.Stopped);
            }
            return new DeployResult(runtime.Spec, shape, plan);
        }
        finally
        {
            lease?.Dispose();
            if (!runtime.Exists)
                _pipelines.TryRemove(new KeyValuePair<string, PipelineRuntime>(spec.Id, runtime));
        }
    }

    public async Task<ActionPlan> StartAsync(string id, ActionOptions options, CancellationToken cancellationToken)
    {
        context.RequireLeader();
        var runtime = Require(id);
        using var exit = await EnterAsync(runtime, "start", cancellationToken).ConfigureAwait(false);
        RequireGeneration(runtime, options.ExpectedGeneration);
        var plan = new ActionPlan("pipeline.start", id, options.DryRun,
            ["desired state: Running", "build a generation on the committed state; sources resume from the committed offsets"]);
        if (options.DryRun)
            return plan;
        if (runtime.Spec.Desired != DesiredState.Running)
            await CommitAsync(runtime, runtime.Spec with { Desired = DesiredState.Running, Generation = runtime.Spec.Generation + 1 },
                plan.Action, options.Caller, plan.Steps, cancellationToken).ConfigureAwait(false);
        if (runtime.Current is null)
        {
            ClearFaults(runtime);
            await StartCoreAsync(runtime, cancellationToken).ConfigureAwait(false);
        }
        return plan;
    }

    public async Task<ActionPlan> StopAsync(string id, ActionOptions options, CancellationToken cancellationToken)
    {
        context.RequireLeader();
        var runtime = Require(id);
        using var exit = await EnterAsync(runtime, "stop", cancellationToken).ConfigureAwait(false);
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
                      (reader.TimeUntilCutLoose is { } time ? $" (about {time:g} at the current rate)" : "") + " or it will need a reset");
            }
            catch (ServiceException)
            {
            }
        }
        var plan = new ActionPlan("pipeline.stop", id, options.DryRun, steps);
        if (options.DryRun)
            return plan;
        if (runtime.Spec.Desired != DesiredState.Stopped)
            await CommitAsync(runtime, runtime.Spec with { Desired = DesiredState.Stopped, Generation = runtime.Spec.Generation + 1 },
                plan.Action, options.Caller, steps, cancellationToken).ConfigureAwait(false);
        await StopCurrentAsync(runtime, context.Options.StopTimeout).ConfigureAwait(false);
        if (!runtime.Wedged)
            Set(runtime, PipelineState.Stopped);
        return plan;
    }

    public async Task<ActionPlan> RestartAsync(string id, ActionOptions options, CancellationToken cancellationToken)
    {
        context.RequireLeader();
        var runtime = Require(id);
        using var exit = await EnterAsync(runtime, "restart", cancellationToken).ConfigureAwait(false);
        RequireGeneration(runtime, options.ExpectedGeneration);
        var plan = new ActionPlan("pipeline.restart", id, options.DryRun,
            ["stop the generation (drain, commit)", "clear Failed and the restart count", "build a new generation on the same state"]);
        if (options.DryRun)
            return plan;
        using (var write = await context.Catalog.BeginAsync(cancellationToken).ConfigureAwait(false))
        {
            write.Audit(context.NodeId, context.Term, options.Caller, plan.Action, id, runtime.Spec.Generation, runtime.Spec.Generation, plan.Steps);
            await write.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        await StopCurrentAsync(runtime, context.Options.StopTimeout).ConfigureAwait(false);
        if (!runtime.Wedged)
        {
            ClearFaults(runtime);
            if (runtime.Spec.Desired == DesiredState.Running)
                await StartCoreAsync(runtime, cancellationToken).ConfigureAwait(false);
            else
                Set(runtime, PipelineState.Stopped);
        }
        return plan;
    }

    public async Task<ActionPlan> ResetAsync(string id, ActionOptions options, CancellationToken cancellationToken)
    {
        context.RequireLeader();
        var runtime = Require(id);
        using var exit = await EnterAsync(runtime, "reset", cancellationToken).ConfigureAwait(false);
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

        await CommitAsync(runtime, runtime.Spec with { Pending = "reset", Generation = runtime.Spec.Generation + 1 },
            plan.Action, options.Caller, steps, cancellationToken).ConfigureAwait(false);
        await StopCurrentAsync(runtime, context.Options.StopTimeout).ConfigureAwait(false);
        if (runtime.Wedged)
            return plan;
        await FinishResetAsync(runtime, cancellationToken).ConfigureAwait(false);
        ClearFaults(runtime);
        if (runtime.Spec.Desired == DesiredState.Running)
            await StartCoreAsync(runtime, cancellationToken).ConfigureAwait(false);
        else
            Set(runtime, PipelineState.Stopped);
        return plan;
    }

    public async Task<ActionPlan> RemoveAsync(string id, ActionOptions options, CancellationToken cancellationToken)
    {
        context.RequireLeader();
        var runtime = Require(id);
        using var exit = await EnterAsync(runtime, "remove", cancellationToken).ConfigureAwait(false);
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

        await CommitAsync(runtime, runtime.Spec with { Desired = DesiredState.Removed, Pending = "remove", Generation = runtime.Spec.Generation + 1 },
            plan.Action, options.Caller, steps, cancellationToken).ConfigureAwait(false);
        await StopCurrentAsync(runtime, context.Options.StopTimeout).ConfigureAwait(false);
        await FinishRemoveAsync(runtime, cancellationToken).ConfigureAwait(false);
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

    /// <summary>The second half of a reset, also run by a leader that finds one pending.</summary>
    private async Task FinishResetAsync(PipelineRuntime runtime, CancellationToken cancellationToken)
    {
        var spec = runtime.Spec;
        await context.Storage.Pipeline(runtime.Id).StartNewIncarnationAsync(cancellationToken).ConfigureAwait(false);
        foreach (var (slot, config) in spec.Sinks)
        {
            if (TryConnector(config) is not IResettableSinkConnector resettable)
                continue;
            await resettable.ResetAsync(new SinkContext(new SlotStart
            {
                Circuit = null!,
                PipelineId = runtime.Id,
                Slot = slot,
                Config = config,
                Json = runtime.Lease?.Json ?? LoadedPlugin.NewJson(),
                Logger = _logger,
                Services = context.Services,
                Leadership = context.Backend.Leadership,
                Cancellation = cancellationToken,
            }), cancellationToken).ConfigureAwait(false);
        }
        await CommitAsync(runtime, spec with { Pending = null, Generation = spec.Generation + 1 }, "pipeline.reset.finished", "service", null, cancellationToken)
            .ConfigureAwait(false);
    }

    private async Task FinishRemoveAsync(PipelineRuntime runtime, CancellationToken cancellationToken)
    {
        if (!runtime.Wedged)
        {
            await context.Storage.Pipeline(runtime.Id).StartNewIncarnationAsync(cancellationToken).ConfigureAwait(false);
            ReleaseBinding(runtime);
        }
        await CommitAsync(runtime, runtime.Spec, "pipeline.remove.finished", "service", null, cancellationToken, delete: true).ConfigureAwait(false);
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

    /// <summary>Builds and starts a generation. Caller holds the pipeline's queue. Never
    /// throws for a failure of the pipeline: that ends in Blocked, Backoff, Failed or
    /// NeedsReset.</summary>
    private async Task StartCoreAsync(PipelineRuntime runtime, CancellationToken cancellationToken)
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
                Bind(runtime, await context.Plugins.AcquireAsync(spec.Plugin, spec.Version, cancellationToken).ConfigureAwait(false));
            }
            generation = Build(runtime, number);
            generation.Circuit.Start();
            await StartEdgesAsync(runtime, generation).ConfigureAwait(false);
            runtime.Current = generation;
            Set(runtime, PipelineState.Running);
            Watch(runtime, generation);
        }
        catch (TableNotOpenException ex)
        {
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
            if (generation is not null)
                await TeardownAsync(runtime, generation, context.Options.StopTimeout).ConfigureAwait(false);
            if (!runtime.Wedged)
                HandleFault(runtime, ex, number);
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private Generation Build(PipelineRuntime runtime, long number)
    {
        var spec = runtime.Spec;
        var lease = runtime.Lease!;
        var definition = lease.Definition(spec.Definition);
        var settings = definition.BindSettings(spec.Settings, lease.Json);
        var builder = new CircuitBuilder(new CircuitOptions
        {
            Name = spec.Id,
            Workers = spec.Circuit.Workers,
            Storage = context.Storage.Pipeline(spec.Id),
            CheckpointInterval = spec.Circuit.CheckpointInterval,
            CheckpointMaxDelay = spec.Circuit.CheckpointMaxDelay,
            ErrorPolicy = spec.Circuit.ErrorPolicy,
            MaxStepItemsPerInput = spec.Circuit.MaxStepItemsPerInput ?? 8192,
            LoggerFactory = context.LoggerFactory,
        });
        var pipeline = new PipelineBuilder(spec, builder, context.Tables, _logger, context.Services);
        definition.Build(pipeline, settings);
        CheckBindings(spec, pipeline);
        runtime.Shape = pipeline.Shape;
        return new Generation { Number = number, Circuit = builder.Build(), Builder = pipeline };
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

        foreach (var input in generation.Builder.Inputs)
        {
            var config = spec.Inputs[input.Name];
            var source = await input.Start(context.Connector(config), For(input.Name, config)).ConfigureAwait(false);
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

    private void HandleFault(PipelineRuntime runtime, Exception ex, long generation)
    {
        lock (runtime.Faults)
        {
            runtime.Faults.Enqueue(new PipelineFault(DateTimeOffset.UtcNow, generation, ex.GetType().Name, ex.Message, ex.StackTrace));
            while (runtime.Faults.Count > context.Options.FaultBuffer)
                runtime.Faults.Dequeue();
        }
        _logger.LogError(ex, "Pipeline {Pipeline} generation {Generation} faulted", runtime.Id, runtime.Spec.Generation);

        if (IsFence(ex) || context.Storage.IsFenced || !context.IsLeader)
        {
            // The node is ending, or no longer leads: a restart could commit nothing.
            Set(runtime, PipelineState.Stopped, "this node no longer leads");
            return;
        }
        if (ex is ServiceException { Code: "too-many-plugin-versions" })
        {
            Set(runtime, PipelineState.Failed, ex.Message);
            return;
        }
        if (ex.Message.Contains("no longer retained", StringComparison.Ordinal))
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
            Set(runtime, PipelineState.Failed, $"{runtime.FaultTimes.Count} faults within {policy.Within}; the last: {ex.GetType().Name}: {ex.Message}");
            return;
        }

        var delay = policy.BackoffAfter(runtime.FaultTimes.Count);
        runtime.BackoffUntil = now + delay;
        runtime.Restarts++;
        Set(runtime, PipelineState.Backoff, $"{ex.GetType().Name}: {ex.Message}");
        ScheduleRetry(runtime, delay, PipelineState.Backoff);
    }

    private static bool IsFence(Exception? ex)
    {
        for (; ex is not null; ex = ex.InnerException)
            if (ex is ChangelogFencedException or TableLogFencedException)
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
                if (retry.IsCancellationRequested || runtime.State != expected || !runtime.Exists || !context.IsLeader)
                    return;
                runtime.Operation = "restart";
                await StartCoreAsync(runtime, CancellationToken.None).ConfigureAwait(false);
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
            _logger.LogWarning("Pipeline {Pipeline} did not drain within {Timeout}; abandoning its outputs", runtime.Id, stopTimeout);
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
        return true;
    }

    // ── the node's calls ───────────────────────────────────────────────────────────────

    /// <summary>A standby's view: what the catalog deploys, none of it built. Its state is
    /// followed as bytes and its circuits are built at takeover.</summary>
    internal void SyncFromCatalog(IReadOnlyDictionary<string, string> catalog)
    {
        var specs = catalog.Where(e => e.Key.StartsWith("pipeline/", StringComparison.Ordinal))
            .Select(e => ServiceJson.Read<PipelineSpec>(e.Value)).ToDictionary(s => s.Id, StringComparer.Ordinal);
        foreach (var (id, runtime) in _pipelines)
            if (!specs.ContainsKey(id) && runtime.Current is null)
                _pipelines.TryRemove(new KeyValuePair<string, PipelineRuntime>(id, runtime));
        foreach (var spec in specs.Values)
        {
            var runtime = _pipelines.GetOrAdd(spec.Id, _ => new PipelineRuntime(spec, context.Options));
            runtime.Spec = spec;
            runtime.Exists = true;
        }
    }

    /// <summary>The supervisor's part of a takeover: finish every pending reset and
    /// removal, then start every pipeline whose desired state is Running, in parallel,
    /// each contained.</summary>
    internal async Task StartLeadingAsync(IReadOnlyDictionary<string, string> catalog, CancellationToken cancellationToken)
    {
        SyncFromCatalog(catalog);
        await Task.WhenAll(_pipelines.Values.Select(async runtime =>
        {
            try
            {
                using var exit = await EnterAsync(runtime, "takeover", cancellationToken).ConfigureAwait(false);
                if (runtime.Spec.Pending == "remove")
                {
                    await FinishRemoveAsync(runtime, cancellationToken).ConfigureAwait(false);
                    return;
                }
                if (runtime.Spec.Pending == "reset")
                    await FinishResetAsync(runtime, cancellationToken).ConfigureAwait(false);
                if (runtime.Spec.Desired == DesiredState.Running)
                    await StartCoreAsync(runtime, cancellationToken).ConfigureAwait(false);
                else
                    Set(runtime, PipelineState.Stopped);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogError(ex, "Pipeline {Pipeline} could not be taken over", runtime.Id);
                HandleFault(runtime, ex, runtime.Generations);
            }
        })).ConfigureAwait(false);
    }

    /// <summary>Stops every pipeline in stop order and in parallel, within the budget.</summary>
    internal Task StopAllAsync(TimeSpan budget)
    {
        var stopTimeout = budget < context.Options.StopTimeout ? budget : context.Options.StopTimeout;
        return Task.WhenAll(_pipelines.Values.Select(async runtime =>
        {
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

    /// <summary>A fenced node's circuits can commit nothing: abandon them without draining.</summary>
    internal Task AbandonAllAsync() => Task.WhenAll(_pipelines.Values.Select(async runtime =>
    {
        CancelRetry(runtime);
        if (runtime.Current is not { } generation)
            return;
        foreach (var sink in generation.Builder.Sinks)
        {
            try
            {
                sink.Abandon(generation.Circuit);
            }
            catch
            {
            }
        }
        await TeardownAsync(runtime, generation, TimeSpan.Zero).ConfigureAwait(false);
        if (!runtime.Wedged)
            Set(runtime, PipelineState.Stopped, "this node no longer leads");
    }));

    /// <summary>A table was opened from its log again: the generations built on the old
    /// instance hold it, so they are rebuilt.</summary>
    internal void OnTableReopened(string table)
    {
        if (!context.IsLeader)
            return;
        foreach (var runtime in _pipelines.Values.Where(p => p.Spec.Reads.Contains(table) || p.Spec.Writes.Contains(table)))
        {
            Task.Run(async () =>
            {
                using var exit = await EnterAsync(runtime, "table reopened", CancellationToken.None).ConfigureAwait(false);
                if (runtime.Current is null && runtime.State != PipelineState.Blocked)
                    return;
                await StopCurrentAsync(runtime, context.Options.StopTimeout).ConfigureAwait(false);
                if (!runtime.Wedged && runtime.Spec.Desired == DesiredState.Running)
                    await StartCoreAsync(runtime, CancellationToken.None).ConfigureAwait(false);
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
