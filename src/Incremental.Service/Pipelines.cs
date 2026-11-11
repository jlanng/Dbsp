using System.Text.Json;
using Incremental.Tables;
using Microsoft.Extensions.Logging;

namespace Incremental.Service;

/// <summary>
/// A pipeline definition with no settings: a class with a public parameterless constructor
/// whose <see cref="Build"/> describes one circuit. <c>Build</c> runs many times — at
/// import, at every dry run, for every generation — and must do nothing but describe the
/// circuit: no I/O, no threads, no static writes.
/// </summary>
public interface IPipelineDefinition
{
    void Build(PipelineBuilder pipeline);
}

/// <summary>A pipeline definition with one settings type, bound from the spec's
/// <c>settings</c> and validated before anything is built.</summary>
public interface IPipelineDefinition<TSettings> where TSettings : class
{
    void Build(PipelineBuilder pipeline, TSettings settings);
}

/// <summary>Names a definition; without it the class name is the name.</summary>
[AttributeUsage(AttributeTargets.Class)]
public sealed class PipelineAttribute(string name) : Attribute
{
    public string Name { get; } = name;
    public string? Description { get; init; }
}

public enum SlotKind { AppendInput, KeyedInput, UpsertSink, AppendSink }

/// <summary>An input or sink slot: its kind and the type of its records; for a keyed input
/// and an upsert sink also the type of the key.</summary>
public sealed record SlotShape(string Name, SlotKind Kind, string Type, string? KeyType = null);

/// <summary>What a definition's <c>Build</c> declared: its slots and the tables it reads
/// and writes, by declared table name. Known for any pipeline that has ever validated.</summary>
public sealed record PipelineShape(
    IReadOnlyList<SlotShape> Inputs, IReadOnlyList<SlotShape> Sinks,
    IReadOnlyList<string> TablesRead, IReadOnlyList<string> TablesWritten,
    IReadOnlyList<TableSlotShape>? TableSlots = null);

/// <summary>The circuit a definition declared, node by node (the core's
/// <see cref="CircuitBuilder.Describe"/>), and which of its outputs is which sink slot.
/// Input slots and tables are in the nodes' kinds: <c>input[slot]</c>, <c>table[name]</c>.</summary>
public sealed record PipelineGraph(CircuitGraph Circuit, IReadOnlyDictionary<string, string> SinkSlots);

/// <summary>A dry run's result: the spec with its plugin version resolved, the slots and
/// tables, and the graph.</summary>
public sealed record PipelineValidation(PipelineSpec Spec, PipelineShape Shape, PipelineGraph Graph);

/// <summary>One state change of a pipeline on this node.</summary>
public sealed record PipelineTransition(DateTimeOffset Time, PipelineState State, long Generation, string? Why);

/// <summary>
/// What a definition builds against. <see cref="Circuit"/> is the core's builder, so
/// lookups, aggregates, event time and windows are the core API with nothing in between;
/// the methods here bind the circuit's edges to named slots, which the spec binds to
/// connectors and declared tables.
/// </summary>
public sealed class PipelineBuilder
{
    private readonly PipelineSpec _spec;
    private readonly ITableRegistry _tables;
    private readonly HashSet<string> _slots = new(StringComparer.Ordinal);

    // Describing a definition with nothing deployed: tables are empty stand-ins of the
    // definition's own shape, so that no table need be declared to learn the slots.
    private readonly bool _detached;
    private readonly Dictionary<string, object> _standIns = new(StringComparer.Ordinal);

    internal PipelineBuilder(
        PipelineSpec spec, CircuitBuilder circuit, ITableRegistry tables, ILogger logger, IServiceProvider services, bool detached = false)
    {
        _detached = detached;
        _spec = spec;
        _tables = tables;
        Circuit = circuit;
        Logger = logger;
        Services = services;
    }

    public string PipelineId => _spec.Id;

    /// <summary>Everything the core offers.</summary>
    public CircuitBuilder Circuit { get; }
    public ILogger Logger { get; }

    /// <summary>The host's services. A service the definition needs (a pricing library, a
    /// clock) comes from here, not from a static: statics pin the plugin's load context.</summary>
    public IServiceProvider Services { get; }

    internal List<InputSlot> Inputs { get; } = [];
    internal List<SinkSlot> Sinks { get; } = [];
    internal List<string> TablesRead { get; } = [];
    internal List<string> TablesWritten { get; } = [];

    /// <summary>Declared table → the key and row types the definition reads or writes it as.</summary>
    internal Dictionary<string, (Type Key, Type Row)> TableTypes { get; } = new(StringComparer.Ordinal);
    internal List<TableSlotShape> TableSlots { get; } = [];

    private SharedTable<TKey, TRow> Resolve<TKey, TRow>(TableDefinition<TKey, TRow> table, string slot, string name, bool written)
        where TKey : notnull
        where TRow : class
    {
        if (!TableSlots.Any(t => t.Slot == slot && t.Access == (written ? "write" : "read")))
            TableSlots.Add(new TableSlotShape(slot, name, written ? "write" : "read", typeof(TKey).Name, typeof(TRow).Name, table.GetType().FullName!));
        if (!_detached)
            return _tables.Get<TKey, TRow>(name);
        if (_standIns.TryGetValue(name, out var existing))
            return existing as SharedTable<TKey, TRow>
                ?? throw new InvalidOperationException($"Table '{name}' is used as two different tables by this definition.");
        var configured = table.Configured();
        var standIn = new SharedTable<TKey, TRow>(name, table.KeyOf, configured.Options);
        foreach (var index in configured.Indexes)
            index.Declare(standIn);
        _standIns[name] = standIn;
        return standIn;
    }

    private readonly Dictionary<string, string> _sinkOutputs = new(StringComparer.Ordinal);

    /// <summary>The output just registered belongs to <paramref name="slot"/>.</summary>
    private void MarkSink(string slot) => _sinkOutputs[Circuit.Describe().Outputs[^1].Name] = slot;

    internal PipelineGraph Graph => new(Circuit.Describe(), new Dictionary<string, string>(_sinkOutputs, StringComparer.Ordinal));

    internal PipelineShape Shape => new(
        Inputs.Select(i => new SlotShape(i.Name, i.Kind, i.Type, i.KeyType)).ToList(),
        Sinks.Select(s => new SlotShape(s.Name, s.Kind, s.Type, s.KeyType)).ToList(),
        [.. TablesRead], [.. TablesWritten], [.. TableSlots]);

    private void Claim(string slot)
    {
        ArgumentException.ThrowIfNullOrEmpty(slot);
        if (!_slots.Add(slot))
            throw new InvalidOperationException($"Slot '{slot}' is declared twice; slot names are unique within a definition.");
    }

    private string TableName(string slot) => _spec.Tables.GetValueOrDefault(slot, slot);

    /// <summary>A shared table to join or look up against. The definition gives the key
    /// and row types and the default slot name; the spec's <c>tables</c> binds the slot to
    /// a declared table, so one definition can read <c>instruments-eu</c> in one
    /// deployment and <c>instruments-us</c> in another.</summary>
    public TableRef<TKey, TRow> Table<TKey, TRow>(TableDefinition<TKey, TRow> table, string? slot = null)
        where TKey : notnull
        where TRow : class
    {
        ArgumentNullException.ThrowIfNull(table);
        var name = TableName(slot ?? table.Name);
        var shared = Resolve(table, slot ?? table.Name, name, written: false);
        if (!TablesRead.Contains(name))
            TablesRead.Add(name);
        TableTypes[name] = (typeof(TKey), typeof(TRow));
        return Circuit.Table(shared);
    }

    /// <summary>An append input: records, each inserted or removed.</summary>
    public InputStream<T> Input<T>(string slot) where T : notnull
    {
        Claim(slot);
        var stream = Circuit.Input<T>(slot);
        Inputs.Add(new InputSlot(slot, SlotKind.AppendInput, typeof(T).Name,
            (connector, start) => connector is IAppendSourceConnector append
                ? append.StartAsync(new SourceContext<T>(start, stream), start.Cancellation)
                : throw Mismatch(slot, connector, "an append input (Input<T>)")));
        return stream;
    }

    /// <summary>A keyed input: upserts and deletes by key.</summary>
    public KeyedInputStream<TKey, TValue> KeyedInput<TKey, TValue>(string slot, Func<TValue, TKey> key)
        where TKey : notnull
        where TValue : notnull
    {
        Claim(slot);
        var stream = Circuit.KeyedInput(slot, key);
        Inputs.Add(new InputSlot(slot, SlotKind.KeyedInput, typeof(TValue).Name,
            (connector, start) => connector is IKeyedSourceConnector keyed
                ? keyed.StartAsync(new KeyedSourceContext<TKey, TValue>(start, stream), start.Cancellation)
                : throw Mismatch(slot, connector, "a keyed input (KeyedInput<TKey, TValue>)"))
        { KeyType = typeof(TKey).Name });
        return stream;
    }

    /// <summary>A keyed result delivered as upserts and deletes, at least once.</summary>
    public void UpsertSink<TKey, TRow>(string slot, ZStream<KeyedValue<TKey, TRow>> rows)
        where TKey : notnull
        where TRow : notnull
    {
        Claim(slot);
        var output = rows.ToAcknowledgedOutput();
        MarkSink(slot);
        Sinks.Add(new SinkSlot(slot, SlotKind.UpsertSink, typeof(TRow).Name,
            (connector, start) =>
            {
                var sink = connector is IUpsertSinkConnector upsert
                    ? upsert.Create<TKey, TRow>(start.Sink)
                    : throw Mismatch(slot, connector, "an upsert sink");
                start.Own(sink);
                var observed = new ObservedUpsertSink<TKey, TRow>(start.Leadership.Leased(sink), start.Stats);
                return start.Circuit.Handle(output).DeliverToAsync(observed, start.Delivery);
            },
            circuit => circuit.Handle(output).Abandon())
        { KeyType = typeof(TKey).Name });
    }

    /// <summary>A result delivered as upserts keyed by <paramref name="key"/>. The stream
    /// must hold at most one row per key at a time.</summary>
    public void UpsertSink<T, TKey>(string slot, ZStream<T> rows, Func<T, TKey> key)
        where T : notnull
        where TKey : notnull
    {
        Claim(slot);
        var output = rows.ToAcknowledgedOutput();
        MarkSink(slot);
        Sinks.Add(new SinkSlot(slot, SlotKind.UpsertSink, typeof(T).Name,
            (connector, start) =>
            {
                var sink = connector is IUpsertSinkConnector upsert
                    ? upsert.Create<TKey, T>(start.Sink)
                    : throw Mismatch(slot, connector, "an upsert sink");
                start.Own(sink);
                var observed = new ObservedUpsertSink<TKey, T>(start.Leadership.Leased(sink), start.Stats);
                return start.Circuit.Handle(output).DeliverToAsync(observed, key, start.Delivery);
            },
            circuit => circuit.Handle(output).Abandon())
        { KeyType = typeof(TKey).Name });
    }

    /// <summary>An append-only result. A retraction on the stream faults the pipeline.</summary>
    public void AppendSink<T>(string slot, ZStream<T> rows) where T : notnull
    {
        Claim(slot);
        var output = rows.ToAcknowledgedOutput();
        MarkSink(slot);
        Sinks.Add(new SinkSlot(slot, SlotKind.AppendSink, typeof(T).Name,
            (connector, start) =>
            {
                var sink = connector is IAppendSinkConnector append
                    ? append.Create<T>(start.Sink)
                    : throw Mismatch(slot, connector, "an append sink");
                start.Own(sink);
                var observed = new ObservedAppendSink<T>(start.Leadership.Leased(sink), start.Stats);
                return start.Circuit.Handle(output).DeliverToAsync(observed, start.Delivery);
            },
            circuit => circuit.Handle(output).Abandon()));
    }

    /// <summary>Writes the stream into a declared table (a derived table): this pipeline
    /// is then the table's one writer.</summary>
    public void ToTable<TKey, TRow>(TableDefinition<TKey, TRow> table, ZStream<TRow> rows, string? slot = null)
        where TKey : notnull
        where TRow : class
    {
        ArgumentNullException.ThrowIfNull(table);
        var name = TableName(slot ?? table.Name);
        var shared = Resolve(table, slot ?? table.Name, name, written: true);
        if (TablesWritten.Contains(name))
            throw new InvalidOperationException($"Table '{name}' is written twice by this definition.");
        TablesWritten.Add(name);
        TableTypes[name] = (typeof(TKey), typeof(TRow));
        rows.ToTable(shared);
    }

    /// <inheritdoc cref="ToTable{TKey,TRow}(TableDefinition{TKey,TRow}, ZStream{TRow}, string?)"/>
    public void ToTable<TKey, TValue, TRow>(
        TableDefinition<TKey, TRow> table, KeyedZStream<TKey, TValue> rows, Func<TKey, TValue, TRow> row, string? slot = null)
        where TKey : notnull
        where TValue : notnull
        where TRow : class
    {
        ArgumentNullException.ThrowIfNull(row);
        ToTable(table, rows.Select(kv => row(kv.Key, kv.Value)), slot);
    }

    private static ServiceException Mismatch(string slot, IConnector connector, string kind) =>
        ServiceException.Invalid($"Slot '{slot}' is {kind}; connector {connector.GetType().Name} does not support that kind.");
}

internal sealed record InputSlot(string Name, SlotKind Kind, string Type, Func<IConnector, SlotStart, Task<IRunningSource>> Start)
{
    public string? KeyType { get; init; }
}

internal sealed record SinkSlot(string Name, SlotKind Kind, string Type, Func<IConnector, SlotStart, Task> Start, Action<Circuit> Abandon)
{
    public string? KeyType { get; init; }
}

/// <summary>What a slot's typed closure needs to start its source or delivery.</summary>
internal sealed class SlotStart
{
    public required Circuit Circuit { get; init; }
    public required string PipelineId { get; init; }
    public required string Slot { get; init; }
    public required JsonElement Config { get; init; }
    public required JsonSerializerOptions Json { get; init; }
    public required ILogger Logger { get; init; }
    public required IServiceProvider Services { get; init; }
    public required INodeLeadership Leadership { get; init; }
    public required CancellationToken Cancellation { get; init; }
    public SinkSlotStats Stats { get; init; } = new();

    /// <summary>Where a sink the connector made for this generation is kept so that it is
    /// disposed with the generation (a Kafka sink owns a producer).</summary>
    public List<object>? Owned { get; init; }

    public void Own(object sink)
    {
        if (sink is IAsyncDisposable or IDisposable)
            Owned?.Add(sink);
    }
    public SinkDeliveryOptions? Delivery { get; init; }

    public SinkContext Sink => new(this);
}

// ── the spec ───────────────────────────────────────────────────────────────────────────

public enum DesiredState { Running, Stopped, Removed }

/// <summary>The scalar members of <see cref="CircuitOptions"/> a spec may set. Storage is
/// always the service's changelog provider.</summary>
public sealed record PipelineCircuitOptions
{
    public int Workers { get; init; } = 1;
    public int CheckpointInterval { get; init; } = 1;
    public TimeSpan? CheckpointMaxDelay { get; init; }
    public RecordErrorPolicy ErrorPolicy { get; init; } = RecordErrorPolicy.Fault;
    public int? MaxStepItemsPerInput { get; init; }
}

/// <summary>
/// A deployed pipeline, as the catalog holds it under <c>pipeline/{id}</c>: which
/// definition of which plugin version, its settings, and what each of its slots is bound
/// to. The id is the pipeline's durable identity: it keys its state in the changelog and
/// names its circuit.
/// </summary>
public sealed record PipelineSpec
{
    public required string Id { get; init; }
    public required string Plugin { get; init; }

    /// <summary>May be omitted in a request when the plugin has exactly one stored
    /// version; the stored spec always carries it.</summary>
    public string Version { get; init; } = "";
    public required string Definition { get; init; }
    public DesiredState Desired { get; init; } = DesiredState.Running;

    /// <summary>Assigned by the leader: it goes up with every change to the entry, and is
    /// the concurrency token a mutation may carry.</summary>
    public long Generation { get; init; }
    public JsonElement? Settings { get; init; }
    public PipelineCircuitOptions Circuit { get; init; } = new();

    /// <summary>Table slot → declared table; a slot not listed binds to the table of its
    /// own name.</summary>
    public IReadOnlyDictionary<string, string> Tables { get; init; } = new Dictionary<string, string>();

    /// <summary>Input slot → connector config: <c>{ "connector": "memory", … }</c>.</summary>
    public IReadOnlyDictionary<string, JsonElement> Inputs { get; init; } = new Dictionary<string, JsonElement>();
    public IReadOnlyDictionary<string, JsonElement> Sinks { get; init; } = new Dictionary<string, JsonElement>();
    public RestartPolicy? Restart { get; init; }

    /// <summary><c>reset</c> or <c>remove</c> while that act is unfinished: a leader that
    /// finds it finishes the act before it does anything else with the pipeline.</summary>
    public string? Pending { get; init; }

    /// <summary>
    /// The spec this one replaced, while this one has not yet run. Assigned by the leader
    /// when a replace keeps the pipeline's state across a change, and cleared when the new
    /// generation first runs. If instead the new generation cannot be built on the state
    /// or its circuit refuses the checkpoint, the leader — this one, or one that takes
    /// over in the middle — puts this spec back and runs it. Never returned by the admin API.
    /// </summary>
    public PipelineSpec? Previous { get; init; }

    /// <summary>The declared tables the definition reads and writes, as the last dry run
    /// resolved them. Assigned by the leader.</summary>
    public IReadOnlyList<string> Reads { get; init; } = [];
    public IReadOnlyList<string> Writes { get; init; } = [];
}

public enum StateChoice { Unspecified, Keep, Reset }

public sealed record DeployOptions(
    StateChoice State = StateChoice.Unspecified, long? ExpectedGeneration = null, bool DryRun = false, string? Caller = null);

public sealed record DeployResult(PipelineSpec Spec, PipelineShape Shape, ActionPlan Plan);

/// <summary>A replace that was undone: generation <c>From</c> could not start on the state
/// it was told to keep, so the spec of generation <c>Restored</c> was put back as
/// generation <c>Now</c>. Kept on the node that did it until the pipeline is next
/// deployed; the audit entry <c>pipeline.replace.rolled-back</c> is the durable record.</summary>
public sealed record PipelineRollback(
    DateTimeOffset Time, long From, long Restored, long Now, string Plugin, string Version, string Definition, IReadOnlyList<string> Problems);

public enum PipelineState { Built, Starting, Running, Stopping, Stopped, Backoff, Blocked, Failed, NeedsReset, Unbuilt, Wedged }

public sealed record PipelineFault(DateTimeOffset Time, long Generation, string ExceptionType, string Message, string? Stack);

public sealed record DeadLetterRecord(
    long Sequence, DateTimeOffset Time, string Operator, long Weight, string ExceptionType, string Message, string? Record);

public sealed record SinkSlotStatus(string Slot, long Batches, long Upserts, long Deletes, long Appends, DateTimeOffset? WriteInFlightSince);

public sealed record SourceSlotStatus(string Slot, SourceStats Stats);

public sealed record PipelineStatus(
    string Id, PipelineState State, DesiredState Desired, long Generation, string Plugin, string Version, string Definition,
    string? BlockedOn, DateTimeOffset? Since, string? Operation, int Restarts, long DeadLetters,
    long? CommittedStep, IReadOnlyDictionary<string, long> CommittedOffsets,
    IReadOnlyList<PipelineFault> Faults, PipelineShape? Shape,
    IReadOnlyList<SourceSlotStatus> Sources, IReadOnlyList<SinkSlotStatus> Sinks,
    IReadOnlyList<PipelineTransition>? History = null,
    PipelineRollback? RolledBack = null,
    bool ReplaceUnconfirmed = false);

public interface IPipelineSupervisor
{
    IReadOnlyList<PipelineStatus> Pipelines { get; }
    PipelineStatus Get(string id);
    PipelineSpec Spec(string id);

    /// <summary>Deploys a new pipeline, or replaces the one of that id. A replace whose
    /// plugin version, definition, non-<see cref="StateNeutralAttribute"/> settings, input
    /// or table bindings or worker count differ is refused with <c>state-choice-required</c>
    /// unless <see cref="DeployOptions.State"/> says keep or reset. A replace that keeps
    /// state is refused with <c>state-incompatible</c>, before anything stops and at the
    /// dry run too, when the new circuit could not resume the committed checkpoint (another
    /// worker count, a committed store with no home, a table recreated). What only shows
    /// when the state is read — a value that does not fit its new type — is found when the
    /// new generation is built: the previous spec is then put back and started, and the
    /// call fails with <c>state-incompatible</c> saying so.</summary>
    Task<DeployResult> DeployAsync(PipelineSpec spec, DeployOptions options, CancellationToken cancellationToken);

    /// <summary>A dry run of a spec that need not be deployed: binds its settings, runs
    /// its definition and checks every slot and table. Nothing is touched.</summary>
    Task<PipelineShape> ValidateAsync(PipelineSpec spec, CancellationToken cancellationToken);

    /// <summary><see cref="ValidateAsync"/> with the circuit's graph and the resolved spec.</summary>
    Task<PipelineValidation> DescribeAsync(PipelineSpec spec, CancellationToken cancellationToken);

    /// <summary>The graph of the pipeline's circuit as this node last built or dry-ran it;
    /// null where it never has — a standby runs no plugin code.</summary>
    PipelineGraph? Graph(string id);

    /// <summary>The spec with every config property a connector declares secret replaced
    /// by <c>***</c>: the form to show.</summary>
    PipelineSpec RedactedSpec(string id);
    Task<ActionPlan> StartAsync(string id, ActionOptions options, CancellationToken cancellationToken);
    Task<ActionPlan> StopAsync(string id, ActionOptions options, CancellationToken cancellationToken);

    /// <summary>Tears the generation down and builds a new one on the same state; also
    /// clears <c>Failed</c>.</summary>
    Task<ActionPlan> RestartAsync(string id, ActionOptions options, CancellationToken cancellationToken);

    /// <summary>A new incarnation: state and input offsets gone, then start.</summary>
    Task<ActionPlan> ResetAsync(string id, ActionOptions options, CancellationToken cancellationToken);
    Task<ActionPlan> RemoveAsync(string id, ActionOptions options, CancellationToken cancellationToken);
    IReadOnlyList<DeadLetterRecord> DeadLetters(string id, long afterSequence = 0, int limit = 256);

    /// <summary>The newest <paramref name="limit"/> dead letters older than
    /// <paramref name="beforeSequence"/>, oldest first: a page back from the newest.</summary>
    IReadOnlyList<DeadLetterRecord> DeadLettersBefore(string id, long beforeSequence = long.MaxValue, int limit = 256);
}
