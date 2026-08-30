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

    public override IInputPort CreatePort(CircuitOptions options) =>
        new InputPort<TValue>(Name, NodeId, options,
            writer => new KeyedInputHandle<TKey, TValue>(new InputHandle<TValue>(writer), KeySelector));
}

/// <summary>Encodes the checkpoint entry keys the committed-offsets marker uses: an
/// unstamped input's running count commits under its plain name, a stamped input's
/// per-partition positions commit under one <c>{name}#p{partition}</c> entry each — so the
/// marker stays a flat string → long map for every checkpoint provider. Input names
/// therefore must not contain <c>'#'</c> (enforced at build time).</summary>
internal static class OffsetKeys
{
    private const string Separator = "#p";

    public static string Encode(string inputName, int partition) => $"{inputName}{Separator}{partition}";

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

internal sealed class InputPort<T> : IInputPort where T : notnull
{
    private readonly Channel<InputEntry<T>> _channel;
    private readonly int _nodeId;

    // Stamping mode mirrors the handle's lock, but from the consuming side — which is the
    // side that survives a restart: a recovered checkpoint seeds the mode, so a producer
    // that switches kinds across a restart (committed positions of the other kind) faults
    // the step instead of deduplicating against a meaningless position.
    private byte _mode; // 0 = nothing consumed or seeded, 1 = unstamped, 2 = stamped
    private long _unstamped = -1;                // running count (unstamped mode)
    private Dictionary<int, long>? _partitions;  // partition → highest consumed offset (stamped mode)

    public InputPort(string name, int nodeId, CircuitOptions options,
        Func<ChannelWriter<InputEntry<T>>, object>? handleFactory = null)
    {
        if (name.Contains('#'))
            throw new ArgumentException(
                $"Input name '{name}' contains '#', which is reserved for the checkpoint offset keys ({nameof(OffsetKeys)}).",
                nameof(name));
        Name = name;
        _nodeId = nodeId;
        _channel = Channel.CreateBounded<InputEntry<T>>(new BoundedChannelOptions(options.InputChannelCapacity)
        {
            SingleReader = true,
            FullMode = BoundedChannelFullMode.Wait,
        });
        Handle = handleFactory?.Invoke(_channel.Writer) ?? new InputHandle<T>(_channel.Writer);
    }

    public string Name { get; }
    public object Handle { get; }
    public bool HasPending => _channel.Reader.TryPeek(out _);
    public bool IsCompleted => _channel.Reader.Completion.IsCompleted;
    public int BufferedCount => _channel.Reader.Count;

    public Task<bool> WaitToReadAsync(CancellationToken cancellationToken) =>
        _channel.Reader.WaitToReadAsync(cancellationToken).AsTask();

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
            into[Name] = _unstamped;
        else if (_mode == 2)
            foreach (var (partition, offset) in _partitions!)
                into[OffsetKeys.Encode(Name, partition)] = offset;
    }

    /// <summary>Moves up to <paramref name="maxItems"/> buffered records into the workers'
    /// input operators for the next step, round-robin. Keyed operators re-shard via exchange
    /// anyway, so initial placement only needs to spread load.</summary>
    public int DrainInto(Worker[] workers, int maxItems)
    {
        ZSet<T>?[]? buffers = null;
        var target = 0;
        var drained = 0;
        while (drained < maxItems && _channel.Reader.TryRead(out var entry))
        {
            drained++;
            if (entry.Offset >= 0)
            {
                if (_mode == 1)
                    throw MixedStamping(stamped: true);
                _mode = 2;

                // Stamped records must advance their partition's position: one at or below
                // it — in particular one replayed after a restart, when the position starts
                // at the committed checkpoint — is a duplicate and is dropped.
                _partitions ??= new Dictionary<int, long>();
                if (_partitions.TryGetValue(entry.Partition, out var position) && entry.Offset <= position)
                    continue;
                _partitions[entry.Partition] = entry.Offset;
            }
            else
            {
                if (_mode == 2)
                    throw MixedStamping(stamped: false);
                _mode = 1;
                _unstamped++; // unstamped records are numbered as they are consumed
            }

            buffers ??= new ZSet<T>?[workers.Length];
            (buffers[target] ??= new ZSet<T>()).Add(entry.Value, entry.Weight);
            target = (target + 1) % workers.Length;
        }

        if (buffers is not null)
            for (var i = 0; i < workers.Length; i++)
                if (buffers[i] is { } batch)
                    ((InputOperator<T>)workers[i].Operators[_nodeId]).Enqueue(batch);
        return drained;
    }

    public void CompleteWriter() => _channel.Writer.TryComplete();

    public void Fail(Exception error) => _channel.Writer.TryComplete(error);

    /// <summary>The handle already refuses mixing within one run, so reaching this means the
    /// producer switched kinds across a restart: the recovered checkpoint committed positions
    /// of the other kind, and deduplicating against them would be meaningless.</summary>
    private InvalidOperationException MixedStamping(bool stamped) => new(
        $"Input '{Name}' has consumed (or recovered a committed checkpoint of) " +
        $"{(stamped ? "unstamped" : "offset-stamped")} records, but received {(stamped ? "an offset-stamped" : "an unstamped")} " +
        "one. Mixing the two would corrupt the input's replay position (exactly-once dedup); stamp every record of an " +
        "input with source offsets, or none of them — across restarts too.");
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

internal interface IOutputPort
{
    string Name { get; }
    object Handle { get; }

    /// <summary>Deltas currently buffered in the bounded channel (for the queue-depth gauge).</summary>
    int BufferedCount { get; }
    ValueTask PublishAsync(Worker[] workers, CancellationToken cancellationToken);

    /// <summary>Captures the just-run step's merged delta synchronously, without touching the
    /// channel — the auto-stepping circuit stages here and writes via
    /// <see cref="FlushStagedAsync"/> overlapped with the next step. The capture copies (as
    /// <see cref="PublishAsync"/> does), so the flush never depends on worker slots the next
    /// step is about to overwrite. At most one step may be staged at a time: the circuit
    /// awaits the previous flush before staging again. Returns whether anything was staged,
    /// so an all-empty step skips the flush task entirely.</summary>
    bool Stage(Worker[] workers);

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
    public async ValueTask PublishAsync(Worker[] workers, CancellationToken cancellationToken)
    {
        if (Merge(workers) is { } merged)
            await _channel.Writer.WriteAsync(merged, cancellationToken).ConfigureAwait(false);
    }

    public bool Stage(Worker[] workers) => (_staged = Merge(workers)) is not null;

    public async ValueTask FlushStagedAsync(CancellationToken cancellationToken)
    {
        if (_staged is { } staged)
        {
            _staged = null;
            await _channel.Writer.WriteAsync(staged, cancellationToken).ConfigureAwait(false);
        }
    }

    private ZSet<T>? _staged;

    private ZSet<T>? Merge(Worker[] workers)
    {
        ZSet<T>? merged = null;
        foreach (var worker in workers)
        {
            if (worker.Slots[_nodeId] is ZSet<T> { IsEmpty: false } delta)
            {
                merged ??= new ZSet<T>();
                merged.AddAll(delta);
            }
        }
        return merged is { IsEmpty: false } ? merged : null;
    }

    public void Complete(Exception? error) => _channel.Writer.TryComplete(error);
}
