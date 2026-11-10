using System.Threading.Channels;
using Incremental.Operators;

namespace Incremental.Runtime;

internal abstract class InputRegistration
{
    public required string Name { get; init; }
    public required int NodeId { get; init; }
    public abstract IInputPort CreatePort(CircuitOptions options);
}

internal sealed class InputRegistration<T> : InputRegistration where T : notnull
{
    public override IInputPort CreatePort(CircuitOptions options) => new InputPort<T>(Name, NodeId, options);
}

internal sealed class KeyedInputRegistration<TKey, TValue> : InputRegistration
    where TKey : notnull
    where TValue : notnull
{
    public required Func<TValue, TKey> KeySelector { get; init; }

    /// <summary>Null on an unsharded circuit, where every command goes to the one worker.</summary>
    public required Func<TKey, uint>? RoutingHash { get; init; }

    public override IInputPort CreatePort(CircuitOptions options) =>
        new KeyedInputPort<TKey, TValue>(Name, NodeId, options, KeySelector, RoutingHash);
}

/// <summary>Encodes the checkpoint entry keys the committed-offsets marker uses: an
/// unstamped input's running count commits under its plain name, a stamped input's
/// per-partition positions commit under one <c>{name}#p{partition}</c> entry each — so the
/// marker stays a flat string → long map for every checkpoint provider. An attached shared
/// table's consumed version commits under <c>{table}#t</c>. Input and table names therefore
/// must not contain <c>'#'</c> (enforced at build time). A table the circuit writes commits the
/// version its checkpoint covers under <c>{table}#w</c>.</summary>
internal static class OffsetKeys
{
    private const string Separator = "#p";

    /// <summary>The key the committed step sequence travels under, beside the offsets.</summary>
    public const string StepMarker = "$step";

    public static string Encode(string inputName, int partition) => $"{inputName}{Separator}{partition}";

    public static string Table(string tableName) => $"{tableName}#t";

    /// <summary>The key a circuit that writes a table (<c>ToTable</c>) commits the table's
    /// version under: the version its checkpoint covers.</summary>
    public static string TableWrite(string tableName) => $"{tableName}#w";

    public static bool TryDecode(string key, out string inputName, out int partition)
    {
        var separator = key.LastIndexOf(Separator, StringComparison.Ordinal);
        if (separator > 0 && int.TryParse(key.AsSpan(separator + Separator.Length), out partition) && partition >= 0)
        {
            inputName = key[..separator];
            return true;
        }
        (inputName, partition) = ("", 0);
        return false;
    }
}

internal interface IInputPort
{
    string Name { get; }
    object Handle { get; }
    bool HasPending { get; }
    bool IsCompleted { get; }

    /// <summary>Records currently buffered in the bounded channel (for the queue-depth gauge).</summary>
    int BufferedCount { get; }

    Task<bool> WaitToReadAsync(CancellationToken cancellationToken);

    /// <summary>Seeds an unstamped input's position from a recovered checkpoint, before any
    /// step runs: numbering continues from it. Locks the input to unstamped records.</summary>
    void SeedUnstamped(long offset);

    /// <summary>Seeds one partition's position from a recovered checkpoint, before any step
    /// runs: replayed records stamped at or below it are dropped. Locks the input to
    /// stamped records.</summary>
    void SeedPartition(int partition, long offset);

    /// <summary>Adds this input's consumed positions to a checkpoint's offsets marker, under
    /// the keys <see cref="OffsetKeys"/> describes. Adds nothing when nothing has been
    /// consumed (and no checkpoint was recovered).</summary>
    void AppendConsumedOffsets(Dictionary<string, long> into);

    /// <summary>Returns the number of records drained (replay-dropped ones included — they
    /// were consumed from the channel), which feeds the records metric.</summary>
    int DrainInto(Worker[] workers, int maxItems);
    void CompleteWriter();

    /// <summary>Completes the input's channel with <paramref name="error"/> (the fault path):
    /// producers blocked on a full channel are released with it, and further sends fail fast
    /// instead of filling a channel no step will ever drain again.</summary>
    void Fail(Exception error);
}

/// <summary>An input whose drain can drop a record under the dead-letter policy.</summary>
internal interface IDeadLetteringInput
{
    /// <summary>Called once, before the circuit is handed out, when the circuit's policy is
    /// <see cref="RecordErrorPolicy.DeadLetter"/>.</summary>
    void DeadLetterInto(RecordErrors errors);
}

/// <summary>
/// The port of an attached shared table (<c>Tables/TablePort</c>): an input whose records
/// are the table's changes, plus the per-step pin that lets operators read the table's rows
/// as of one version. The circuit drives the pin's lifetime through this interface.
/// </summary>
internal interface ITablePort : IInputPort
{
    string TableName { get; }

    /// <summary>The identity of the table's history, for the state manifest.</summary>
    string Epoch { get; }

    /// <summary>Registers the circuit as a reader of the table, at the version seeded from
    /// the checkpoint or, with none, at the table's latest. From here until
    /// <see cref="Detach"/> the table keeps the changes the circuit has not consumed. Throws
    /// if the seeded version can no longer be resumed from.</summary>
    void Attach();

    /// <summary>Pins the table's latest version for the step about to run and gathers the
    /// changes since the previous step's, which <see cref="IInputPort.DrainInto"/> then
    /// stages. Completes synchronously unless the circuit is so far behind that some of
    /// them must be read from the table's log.</summary>
    ValueTask BeginStepAsync(CancellationToken cancellationToken);

    /// <summary>Releases the step's pin, if one is held. Only once no worker can still be
    /// reading through it.</summary>
    void ReleasePin();

    /// <summary>Releases the pin and stops the table retaining changes for this circuit.
    /// Idempotent.</summary>
    void Detach();
}

/// <summary>
/// An input's replay position — the consuming side of offset stamping, shared by plain and
/// keyed inputs. Stamping mode mirrors the handle's lock, but from the side that survives a
/// restart: a recovered checkpoint seeds the mode, so a producer that switches kinds across
/// a restart (committed positions of the other kind) faults the step instead of
/// deduplicating against a meaningless position.
/// </summary>
internal sealed class InputPosition(string inputName)
{
    private byte _mode; // 0 = nothing consumed or seeded, 1 = unstamped, 2 = stamped
    private long _unstamped = -1;                // running count (unstamped mode)
    private Dictionary<int, long>? _partitions;  // partition → highest consumed offset (stamped mode)

    public void SeedUnstamped(long offset)
    {
        _mode = 1;
        _unstamped = offset;
    }

    public void SeedPartition(int partition, long offset)
    {
        _mode = 2;
        (_partitions ??= new Dictionary<int, long>())[partition] = offset;
    }

    public void AppendConsumedOffsets(Dictionary<string, long> into)
    {
        if (_mode == 1 && _unstamped >= 0)
            into[inputName] = _unstamped;
        else if (_mode == 2)
            foreach (var (partition, offset) in _partitions!)
                into[OffsetKeys.Encode(inputName, partition)] = offset;
    }

    /// <summary>Advances the position past one consumed record (offset -1 = unstamped) and
    /// says whether the record is new. A stamped record must advance its partition's
    /// position: one at or below it — in particular one replayed after a restart, when the
    /// position starts at the committed checkpoint — is a duplicate, to be dropped.
    /// Unstamped records are numbered as they are consumed and always admitted.</summary>
    public bool Admit(int partition, long offset)
    {
        if (offset >= 0)
        {
            if (_mode == 1)
                throw MixedStamping(stamped: true);
            _mode = 2;
            _partitions ??= new Dictionary<int, long>();
            if (_partitions.TryGetValue(partition, out var position) && offset <= position)
                return false;
            _partitions[partition] = offset;
            return true;
        }

        if (_mode == 2)
            throw MixedStamping(stamped: false);
        _mode = 1;
        _unstamped++;
        return true;
    }

    /// <summary>The handle already refuses mixing within one run, so reaching this means the
    /// producer switched kinds across a restart: the recovered checkpoint committed positions
    /// of the other kind, and deduplicating against them would be meaningless.</summary>
    private InvalidOperationException MixedStamping(bool stamped) => new(
        $"Input '{inputName}' has consumed (or recovered a committed checkpoint of) " +
        $"{(stamped ? "unstamped" : "offset-stamped")} records, but received {(stamped ? "an offset-stamped" : "an unstamped")} " +
        "one. Mixing the two would corrupt the input's replay position (exactly-once dedup); stamp every record of an " +
        "input with source offsets, or none of them — across restarts too.");
}

internal sealed class InputPort<T> : IInputPort, IDeadLetteringInput where T : notnull
{
    private RecordErrors? _errors;

    public void DeadLetterInto(RecordErrors errors) => _errors = errors;

    private readonly Channel<InputEntry<T>> _channel;
    private readonly int _nodeId;
    private readonly InputPosition _position;

    public InputPort(string name, int nodeId, CircuitOptions options)
    {
        Name = InputPorts.ValidateName(name);
        _nodeId = nodeId;
        _position = new InputPosition(name);
        _channel = InputPorts.CreateChannel<InputEntry<T>>(options);
        Handle = new InputHandle<T>(_channel.Writer);
    }

    public string Name { get; }
    public object Handle { get; }
    public bool HasPending => _channel.Reader.TryPeek(out _);
    public bool IsCompleted => _channel.Reader.Completion.IsCompleted;
    public int BufferedCount => _channel.Reader.Count;

    public Task<bool> WaitToReadAsync(CancellationToken cancellationToken) =>
        _channel.Reader.WaitToReadAsync(cancellationToken).AsTask();

    public void SeedUnstamped(long offset) => _position.SeedUnstamped(offset);

    public void SeedPartition(int partition, long offset) => _position.SeedPartition(partition, offset);

    public void AppendConsumedOffsets(Dictionary<string, long> into) => _position.AppendConsumedOffsets(into);

    // The staging targets — each worker's input operator's current pending buffer — fetched
    // per drain (which of an operator's two buffers is current changes as steps run); only
    // the reference array is reused across drains, the Z-sets belong to the operators.
    private ZSet<T>[]? _staging;

    /// <summary>Moves up to <paramref name="maxItems"/> buffered records into the workers'
    /// input operators' pending batches for the next step, round-robin. Keyed operators
    /// re-shard via exchange anyway, so initial placement only needs to spread load.</summary>
    public int DrainInto(Worker[] workers, int maxItems)
    {
        ZSet<T>[]? staging = null;
        var target = 0;
        var drained = 0;
        while (drained < maxItems && _channel.Reader.TryRead(out var entry))
        {
            drained++;
            if (!_position.Admit(entry.Partition, entry.Offset))
                continue;

            // A record the source could not read (InputHandle.RejectAsync): admitted, so
            // its offset is consumed and a replay drops it; reported, or a fault.
            if (entry.Rejection is { } rejection)
            {
                if (_errors is null)
                    throw rejection.Fault(Name, entry.Partition, entry.Offset);
                _errors.Add(rejection.Record, 1, rejection.Error);
                continue;
            }

            if (staging is null)
            {
                staging = _staging is { } cached && cached.Length == workers.Length
                    ? cached
                    : _staging = new ZSet<T>[workers.Length];
                for (var i = 0; i < workers.Length; i++)
                    staging[i] = ((InputOperator<T>)workers[i].Operators[_nodeId]).Pending;
            }
            staging[target].Add(entry.Value, entry.Weight);
            target = (target + 1) % workers.Length;
        }
        return drained;
    }

    public void CompleteWriter() => _channel.Writer.TryComplete();

    public void Fail(Exception error) => _channel.Writer.TryComplete(error);
}

/// <summary>
/// The port of a keyed input: carries upsert/delete commands rather than weighted rows, and
/// places each on a worker chosen by its key — so every command for a key reaches the same
/// worker, in channel order, and that worker's <see cref="KeyedInputOperator{TKey,TValue}"/>
/// can resolve several commands for one key within a step as last-write-wins. On a single
/// process that worker also owns the key's upsert state; in a cluster the collapsed commands
/// then cross an exchange to the owning process.
/// </summary>
internal sealed class KeyedInputPort<TKey, TValue> : IInputPort, IDeadLetteringInput
    where TKey : notnull
    where TValue : notnull
{
    private RecordErrors? _errors;

    public void DeadLetterInto(RecordErrors errors)
    {
        _errors = errors;
        ((KeyedInputHandle<TKey, TValue>)Handle).ReportsBadKeys = true;
    }

    private readonly Channel<KeyedInputEntry<TKey, TValue>> _channel;
    private readonly int _nodeId;
    private readonly InputPosition _position;
    private readonly Func<TKey, uint>? _routingHash;

    public KeyedInputPort(string name, int nodeId, CircuitOptions options,
        Func<TValue, TKey> keySelector, Func<TKey, uint>? routingHash)
    {
        Name = InputPorts.ValidateName(name);
        _nodeId = nodeId;
        _position = new InputPosition(name);
        _routingHash = routingHash;
        _channel = InputPorts.CreateChannel<KeyedInputEntry<TKey, TValue>>(options);
        Handle = new KeyedInputHandle<TKey, TValue>(_channel.Writer, keySelector);
    }

    public string Name { get; }
    public object Handle { get; }
    public bool HasPending => _channel.Reader.TryPeek(out _);
    public bool IsCompleted => _channel.Reader.Completion.IsCompleted;
    public int BufferedCount => _channel.Reader.Count;

    public Task<bool> WaitToReadAsync(CancellationToken cancellationToken) =>
        _channel.Reader.WaitToReadAsync(cancellationToken).AsTask();

    public void SeedUnstamped(long offset) => _position.SeedUnstamped(offset);

    public void SeedPartition(int partition, long offset) => _position.SeedPartition(partition, offset);

    public void AppendConsumedOffsets(Dictionary<string, long> into) => _position.AppendConsumedOffsets(into);

    /// <summary>Moves up to <paramref name="maxItems"/> buffered commands onto the workers'
    /// pending lists, each on the worker its key hashes to (the same placement the exchange
    /// uses on a single process, which is why none is needed there).</summary>
    public int DrainInto(Worker[] workers, int maxItems)
    {
        var drained = 0;
        while (drained < maxItems && _channel.Reader.TryRead(out var entry))
        {
            drained++;
            if (!_position.Admit(entry.Partition, entry.Offset))
                continue;

            // An upsert whose key could not be derived (see KeyedInputHandle): it was
            // admitted, so its offset is consumed and a replay drops it, and it goes to the
            // dead letters instead of a worker.
            if (entry.Rejection is { } rejection)
            {
                if (_errors is null)
                    throw rejection.Fault(Name, entry.Partition, entry.Offset);
                _errors.Add(rejection.Record, 1, rejection.Error);
                continue;
            }
            if (entry.Error is not null)
            {
                _errors!.Add(entry.Value, 1, entry.Error);
                continue;
            }

            var target = _routingHash is null ? 0 : (int)(_routingHash(entry.Key) % (uint)workers.Length);
            ((KeyedInputOperator<TKey, TValue>)workers[target].Operators[_nodeId]).Pending
                .Add(new KeyedCommand<TKey, TValue>(entry.Key, entry.IsDelete, entry.Value));
        }
        return drained;
    }

    public void CompleteWriter() => _channel.Writer.TryComplete();

    public void Fail(Exception error) => _channel.Writer.TryComplete(error);
}

internal static class InputPorts
{
    public static string ValidateName(string name) =>
        name.Contains('#')
            ? throw new ArgumentException(
                $"Input name '{name}' contains '#', which is reserved for the checkpoint offset keys ({nameof(OffsetKeys)}).",
                nameof(name))
            : name == OffsetKeys.StepMarker
                ? throw new ArgumentException(
                    $"Input name '{name}' is reserved: the checkpoint commits the circuit's step sequence under it.",
                    nameof(name))
                : name;

    public static Channel<TEntry> CreateChannel<TEntry>(CircuitOptions options) =>
        Channel.CreateBounded<TEntry>(new BoundedChannelOptions(options.InputChannelCapacity)
        {
            SingleReader = true,
            FullMode = BoundedChannelFullMode.Wait,
        });
}

internal abstract class OutputRegistration
{
    public required string Name { get; init; }
    public required int NodeId { get; init; }
    public abstract IOutputPort CreatePort(CircuitOptions options);
}

internal sealed class OutputRegistration<T> : OutputRegistration where T : notnull
{
    public bool Snapshot { get; init; }

    public override IOutputPort CreatePort(CircuitOptions options) => new OutputPort<T>(Name, NodeId, options, Snapshot);
}

internal sealed class AcknowledgedOutputRegistration<T> : OutputRegistration where T : notnull
{

    public override IOutputPort CreatePort(CircuitOptions options) => new AcknowledgedOutputPort<T>(Name, NodeId, options);
}

internal interface IOutputPort
{
    string Name { get; }
    object Handle { get; }

    /// <summary>Deltas currently buffered in the bounded channel (for the queue-depth gauge).</summary>
    int BufferedCount { get; }

    /// <summary>Publishes the just-run step's merged delta, if it is not empty.
    /// <paramref name="step"/> is the step's sequence number, which an acknowledged output
    /// stamps on the batch; a plain output ignores it.</summary>
    ValueTask PublishAsync(Worker[] workers, long step, CancellationToken cancellationToken);

    /// <summary>Captures the just-run step's merged delta synchronously, without touching the
    /// channel — the auto-stepping circuit stages here and writes via
    /// <see cref="FlushStagedAsync"/> overlapped with the next step. The capture copies (as
    /// <see cref="PublishAsync"/> does), so the flush never depends on worker slots the next
    /// step is about to overwrite. At most one step may be staged at a time: the circuit
    /// awaits the previous flush before staging again. Returns whether anything was staged,
    /// so an all-empty step skips the flush task entirely.</summary>
    bool Stage(Worker[] workers, long step);

    /// <summary>Publishes the staged delta, if any. Awaiting the bounded write is what turns
    /// a slow consumer into backpressure — one step later than the synchronous path, which
    /// is the entire slack the staging introduces.</summary>
    ValueTask FlushStagedAsync(CancellationToken cancellationToken);
    void Complete(Exception? error);

    /// <summary>Wires the handle's snapshot read to the circuit (no-op for plain outputs).
    /// Called once, after the circuit's workers and ports exist.</summary>
    void BindSnapshot(Circuit circuit);
}

internal sealed class OutputPort<T> : IOutputPort where T : notnull
{
    private readonly Channel<ZSet<T>> _channel;
    private readonly int _nodeId;
    private readonly bool _snapshot;

    public OutputPort(string name, int nodeId, CircuitOptions options, bool snapshot)
    {
        Name = name;
        _nodeId = nodeId;
        _snapshot = snapshot;
        _channel = Channel.CreateBounded<ZSet<T>>(new BoundedChannelOptions(options.OutputChannelCapacity)
        {
            SingleWriter = true,
            FullMode = BoundedChannelFullMode.Wait,
        });
        Handle = new OutputHandle<T>(name, _channel.Reader);
    }

    public void BindSnapshot(Circuit circuit)
    {
        if (_snapshot)
            ((OutputHandle<T>)Handle).SnapshotReader =
                cancellationToken => circuit.ReadOutputSnapshotAsync<T>(_nodeId, DiscardPending, cancellationToken);
    }

    /// <summary>Drops every delta currently buffered in the channel. Only the snapshot read
    /// uses this: everything published before a snapshot is taken is subsumed by it, so a
    /// bootstrapping consumer must not integrate those deltas a second time.</summary>
    private void DiscardPending()
    {
        while (_channel.Reader.TryRead(out _)) { }
    }

    public string Name { get; }
    public object Handle { get; }
    public int BufferedCount => _channel.Reader.Count;

    /// <summary>Merges the per-worker deltas of this output's node for the step just run and,
    /// if non-empty, publishes them. Awaiting the bounded write is what propagates a slow
    /// consumer back into the circuit as backpressure.</summary>
    public async ValueTask PublishAsync(Worker[] workers, long step, CancellationToken cancellationToken)
    {
        if (OutputPorts.Merge<T>(workers, _nodeId) is { } merged)
            await _channel.Writer.WriteAsync(merged, cancellationToken).ConfigureAwait(false);
    }

    public bool Stage(Worker[] workers, long step) => (_staged = OutputPorts.Merge<T>(workers, _nodeId)) is not null;

    public async ValueTask FlushStagedAsync(CancellationToken cancellationToken)
    {
        if (_staged is { } staged)
        {
            _staged = null;
            await _channel.Writer.WriteAsync(staged, cancellationToken).ConfigureAwait(false);
        }
    }

    private ZSet<T>? _staged;

    public void Complete(Exception? error) => _channel.Writer.TryComplete(error);
}

internal static class OutputPorts
{
    /// <summary>Copies the per-worker deltas of an output's node into one new Z-set, or
    /// returns null when the step produced nothing for it. Always a copy: worker slots are
    /// reused by the next step.</summary>
    public static ZSet<T>? Merge<T>(Worker[] workers, int nodeId) where T : notnull
    {
        ZSet<T>? merged = null;
        foreach (var worker in workers)
        {
            if (worker.Slots[nodeId] is ZSet<T> { IsEmpty: false } delta)
            {
                merged ??= new ZSet<T>();
                merged.AddAll(delta);
            }
        }
        return merged is { IsEmpty: false } ? merged : null;
    }
}

/// <summary>What the circuit needs from an acknowledged output at its commit point.</summary>
internal interface IAcknowledgedOutputPort : IOutputPort
{
    /// <summary>Completes once the consumer has acknowledged every batch this output has
    /// published (immediately if none is outstanding). Faults if the consumer has stopped
    /// reading, since the batches can then never be confirmed. Only the coordinator calls
    /// this, between steps, so nothing is published while a wait is pending.</summary>
    Task WaitAcknowledgedAsync();
}

/// <summary>
/// The port of a derived table (<c>Tables/DerivedTablePort</c>): an acknowledged output whose
/// delivery is a shared table's version committing. What the circuit adds for it is the
/// table's position in the checkpoint, and the recovery that position makes possible.
/// </summary>
internal interface IDerivedTablePort : IAcknowledgedOutputPort
{
    string TableName { get; }

    /// <summary>The identity of the table's history, for the state manifest.</summary>
    string Epoch { get; }

    /// <summary>Whether the circuit owes the table a step whatever its inputs hold: the
    /// first one, which reconciles the table with the circuit's state.</summary>
    bool NeedsStep { get; }

    /// <summary>Called by <c>Start()</c>. <paramref name="resuming"/> says the circuit has
    /// a committed checkpoint; <paramref name="committedVersion"/> is the table version that
    /// checkpoint covers, null if it recorded none. Throws if the circuit cannot resume
    /// against the table as it is.</summary>
    void Open(bool resuming, long? committedVersion, bool checkpointing);

    /// <summary>Adds the table version this circuit has written up to, under
    /// <see cref="OffsetKeys.TableWrite"/>.</summary>
    void AppendWrittenVersion(Dictionary<string, long> into);

    /// <summary>The checkpoint carrying the last <see cref="AppendWrittenVersion"/> landed.</summary>
    void CheckpointCommitted();
}

/// <summary>
/// The port of an acknowledged output (<c>ToAcknowledgedOutput</c>): the same bounded
/// channel as a plain output, with each batch stamped by the sequence number of the step
/// that produced it, plus the bookkeeping the commit gate reads — the highest sequence
/// published and the highest the consumer has acknowledged. It keeps no integrated state.
/// </summary>
internal sealed class AcknowledgedOutputPort<T> : IAcknowledgedOutputPort where T : notnull
{
    private readonly Channel<OutputBatch<T>> _channel;
    private readonly AcknowledgedOutputHandle<T> _handle;
    private readonly int _nodeId;

    // Guards the four fields below: the coordinator publishes and waits, the consumer
    // acknowledges (and may abandon) from its own thread.
    private readonly object _ackLock = new();
    private long _published;      // sequence of the last batch handed to the channel (or staged for it)
    private long _acknowledged;   // highest sequence the consumer has confirmed
    private TaskCompletionSource? _waiter;
    private Exception? _failure;  // the consumer abandoned the output, or the circuit faulted
    private Exception? _abandonment;

    private bool _hasStaged;
    private OutputBatch<T> _staged;

    public AcknowledgedOutputPort(string name, int nodeId, CircuitOptions options)
    {
        Name = name;
        _nodeId = nodeId;
        _channel = Channel.CreateBounded<OutputBatch<T>>(new BoundedChannelOptions(options.OutputChannelCapacity)
        {
            SingleWriter = true,
            FullMode = BoundedChannelFullMode.Wait,
        });
        _handle = new AcknowledgedOutputHandle<T>(name, _channel.Reader, this);
    }

    public string Name { get; }
    public object Handle => _handle;
    public int BufferedCount => _channel.Reader.Count;

    public void BindSnapshot(Circuit circuit)
    {
    }

    public async ValueTask PublishAsync(Worker[] workers, long step, CancellationToken cancellationToken)
    {
        if (Stage(workers, step))
            await FlushStagedAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>The batch counts as published from here, before it reaches the channel: the
    /// commit gate must wait for it whether or not the flush has landed yet.</summary>
    public bool Stage(Worker[] workers, long step)
    {
        if (OutputPorts.Merge<T>(workers, _nodeId) is not { } merged)
            return false;
        _staged = new OutputBatch<T>(_handle, step, merged);
        _hasStaged = true;
        lock (_ackLock)
            _published = step;
        return true;
    }

    public async ValueTask FlushStagedAsync(CancellationToken cancellationToken)
    {
        if (!_hasStaged)
            return;
        var staged = _staged;
        _hasStaged = false;
        _staged = default;
        try
        {
            await _channel.Writer.WriteAsync(staged, cancellationToken).ConfigureAwait(false);
        }
        catch (ChannelClosedException) when (Abandonment is { } abandoned)
        {
            // Surface the reason rather than a bare closed channel: this faults the circuit.
            throw abandoned;
        }
    }

    public Task WaitAcknowledgedAsync()
    {
        lock (_ackLock)
        {
            if (_failure is { } failure)
                return Task.FromException(failure);
            if (_acknowledged >= _published)
                return Task.CompletedTask;
            // Asynchronous continuations: the acknowledging consumer must not find itself
            // running the circuit's commit and next step on its own thread.
            return (_waiter ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)).Task;
        }
    }

    public long AcknowledgedSequence
    {
        get
        {
            lock (_ackLock)
                return _acknowledged;
        }
    }

    /// <summary>Cumulative: confirms every batch up to and including
    /// <paramref name="sequence"/>. Acknowledging again, or out of order, is harmless.</summary>
    public void Acknowledge(long sequence)
    {
        TaskCompletionSource? release = null;
        lock (_ackLock)
        {
            if (sequence < 1 || sequence > _published)
                throw new ArgumentOutOfRangeException(nameof(sequence), sequence,
                    $"Output '{Name}' has published no batch with this sequence (the latest is {_published}); " +
                    "acknowledge the Sequence of a batch that was read.");
            if (sequence <= _acknowledged)
                return;
            _acknowledged = sequence;
            if (_acknowledged >= _published)
                (release, _waiter) = (_waiter, null);
        }
        release?.TrySetResult();
    }

    private Exception? Abandonment => Volatile.Read(ref _abandonment);

    /// <summary>The consumer stopped reading before the output completed. Nothing can
    /// deliver or confirm the rest, so the publish in flight and the commit gate both fail,
    /// which faults the circuit instead of leaving it stalled for good. The first call
    /// wins; <paramref name="cause"/> is the sink's own failure, when that is what ended it.</summary>
    public void Abandon(Exception? cause = null)
    {
        if (Abandonment is not null)
            return;
        var failure = new InvalidOperationException(
            $"The consumer of acknowledged output '{Name}' stopped reading before the output completed, so its " +
            "batches can no longer be delivered or acknowledged. The checkpoint does not advance past unacknowledged " +
            "output: rebuild the circuit from the last committed checkpoint and the batches since are delivered again.",
            cause);
        if (Interlocked.CompareExchange(ref _abandonment, failure, null) is not null)
            return;
        Fail(failure);
        _channel.Writer.TryComplete(failure);
    }

    private void Fail(Exception failure)
    {
        TaskCompletionSource? release;
        lock (_ackLock)
        {
            _failure ??= failure;
            (release, _waiter) = (_waiter, null);
        }
        release?.TrySetException(failure);
    }

    public void Complete(Exception? error)
    {
        if (error is not null)
            Fail(error);
        _channel.Writer.TryComplete(error);
    }
}
