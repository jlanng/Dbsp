using System.Threading.Channels;

namespace Incremental;

/// <summary>One queued input record: a weighted value plus the source (partition, offset)
/// the producer stamped it with, or offset -1 when unstamped — the input then numbers
/// records as it consumes them (partition is ignored).</summary>
internal readonly record struct InputEntry<T>(T Value, long Weight, int Partition, long Offset) where T : notnull;

/// <summary>
/// Producer-side handle to a circuit input. Writes go through a bounded channel: when the
/// circuit cannot keep up, the returned <see cref="ValueTask"/>s suspend until capacity
/// frees up — backpressure is the await, there is nothing else to configure.
///
/// The overloads taking an <c>offset</c> stamp the record with its position in the source
/// it was read from (e.g. a Kafka partition offset); the overloads also taking a
/// <c>partition</c> say which partition of the source that position belongs to, so one
/// input can front a multi-partition source — the overloads without one use partition 0.
/// Offsets must be non-negative and increase monotonically within their partition; a
/// record whose offset is at or below its partition's current position is dropped as a
/// duplicate. With a checkpointing storage provider each partition's position starts at
/// the committed checkpoint after a restart, so a producer can replay every partition
/// from <see cref="Circuit.CommittedOffsets"/> (or earlier) and already-processed records
/// are ignored — exactly-once from the state's point of view.
///
/// An input carries either stamped or unstamped records, never both: mixing them would
/// silently corrupt the replay position, so the handle locks to whichever kind it sends
/// first and throws on the other kind.
/// </summary>
public sealed class InputHandle<T> where T : notnull
{
    private readonly ChannelWriter<InputEntry<T>> _writer;

    // 0 = no record sent yet, 1 = unstamped, 2 = stamped. Locked by the first send so a
    // producer that mixes the two kinds fails on the spot instead of corrupting the
    // input's replay position.
    private int _stamping;

    internal InputHandle(ChannelWriter<InputEntry<T>> writer) => _writer = writer;

    public ValueTask InsertAsync(T value, CancellationToken cancellationToken = default) =>
        SendAsync(value, 1, cancellationToken);

    /// <summary>Inserts <paramref name="value"/> stamped with its source offset (partition 0).</summary>
    public ValueTask InsertAsync(T value, long offset, CancellationToken cancellationToken = default) =>
        SendAsync(value, 1, offset, cancellationToken);

    /// <summary>Inserts <paramref name="value"/> stamped with its source partition and offset.</summary>
    public ValueTask InsertAsync(T value, int partition, long offset, CancellationToken cancellationToken = default) =>
        SendAsync(value, 1, partition, offset, cancellationToken);

    public ValueTask RemoveAsync(T value, CancellationToken cancellationToken = default) =>
        SendAsync(value, -1, cancellationToken);

    /// <summary>Retracts <paramref name="value"/>, stamped with its source offset (partition 0).</summary>
    public ValueTask RemoveAsync(T value, long offset, CancellationToken cancellationToken = default) =>
        SendAsync(value, -1, offset, cancellationToken);

    /// <summary>Retracts <paramref name="value"/>, stamped with its source partition and offset.</summary>
    public ValueTask RemoveAsync(T value, int partition, long offset, CancellationToken cancellationToken = default) =>
        SendAsync(value, -1, partition, offset, cancellationToken);

    public ValueTask SendAsync(T value, long weight, CancellationToken cancellationToken = default)
    {
        LockStamping(stamped: false);
        return WriteAsync(new InputEntry<T>(value, weight, -1, -1), cancellationToken);
    }

    /// <summary>Sends a weighted record stamped with its source offset (partition 0).</summary>
    public ValueTask SendAsync(T value, long weight, long offset, CancellationToken cancellationToken = default) =>
        SendAsync(value, weight, 0, offset, cancellationToken);

    /// <summary>Sends a weighted record stamped with its source partition and offset.</summary>
    public ValueTask SendAsync(T value, long weight, int partition, long offset, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(partition);
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        LockStamping(stamped: true);
        return WriteAsync(new InputEntry<T>(value, weight, partition, offset), cancellationToken);
    }

    public async ValueTask SendAsync(IEnumerable<WeightedValue<T>> batch, CancellationToken cancellationToken = default)
    {
        LockStamping(stamped: false);
        foreach (var entry in batch)
            await WriteAsync(new InputEntry<T>(entry.Value, entry.Weight, -1, -1), cancellationToken).ConfigureAwait(false);
    }

    private void LockStamping(bool stamped)
    {
        var mode = stamped ? 2 : 1;
        var seen = Interlocked.CompareExchange(ref _stamping, mode, 0);
        if (seen != 0 && seen != mode)
            throw new InvalidOperationException(
                $"This input already carries {(stamped ? "unstamped" : "offset-stamped")} records, so it cannot accept " +
                $"{(stamped ? "an offset-stamped" : "an unstamped")} one: mixing the two would corrupt the input's replay " +
                "position (exactly-once dedup). Stamp every record of an input with source offsets, or none of them.");
    }

    /// <summary>The channel write, with the fault path surfaced as what it is: when the
    /// circuit faults it completes each input's channel with a
    /// <see cref="CircuitFaultedException"/>, and this unwraps the resulting channel-closed
    /// error so producers — blocked mid-write on a full channel included — see that
    /// exception (original fault as its inner) rather than a bare closed-channel error.
    /// A channel completed normally (via <see cref="Complete"/> or circuit disposal) still
    /// throws <see cref="ChannelClosedException"/>. The synchronous completion path stays
    /// allocation-free.</summary>
    private ValueTask WriteAsync(InputEntry<T> entry, CancellationToken cancellationToken)
    {
        var write = _writer.WriteAsync(entry, cancellationToken);
        return write.IsCompletedSuccessfully ? write : Awaited(write);

        static async ValueTask Awaited(ValueTask write)
        {
            try
            {
                await write.ConfigureAwait(false);
            }
            catch (ChannelClosedException ex) when (ex.InnerException is CircuitFaultedException fault)
            {
                throw new CircuitFaultedException(fault.InnerException!);
            }
        }
    }

    /// <summary>Signals that no more data will arrive on this input. Once every input of a
    /// circuit is complete and drained, its outputs complete too.</summary>
    public void Complete() => _writer.TryComplete();
}

/// <summary>
/// Producer-side handle to a keyed circuit input: upsert/delete-by-key semantics on top of
/// the Z-set delta stream. The handle tracks the current row per key, so
/// <see cref="UpsertAsync"/> automatically retracts the previous version of the row before
/// asserting the new one — the natural way to feed reference/master data (and any stream
/// where rows are amended in place). Calls are serialized internally; state lives on the
/// producer side, so a keyed input should have a single logical producer.
/// </summary>
public sealed class KeyedInputHandle<TKey, TValue>
    where TKey : notnull
    where TValue : notnull
{
    private readonly InputHandle<TValue> _inner;
    private readonly Func<TValue, TKey> _keySelector;
    private readonly Dictionary<TKey, TValue> _current = new();
    private readonly SemaphoreSlim _gate = new(1, 1);

    internal KeyedInputHandle(InputHandle<TValue> inner, Func<TValue, TKey> keySelector)
    {
        _inner = inner;
        _keySelector = keySelector;
    }

    /// <summary>Number of keys currently present.</summary>
    public int Count
    {
        get { lock (_current) return _current.Count; }
    }

    /// <summary>Inserts the row, or replaces the existing row with its key (retraction +
    /// assertion). Upserting an identical row is a no-op.</summary>
    public async ValueTask UpsertAsync(TValue value, CancellationToken cancellationToken = default)
    {
        var key = _keySelector(value);
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            bool had;
            TValue? existing;
            lock (_current)
                had = _current.TryGetValue(key, out existing);
            if (had)
            {
                if (EqualityComparer<TValue>.Default.Equals(existing, value))
                    return;
                await _inner.SendAsync(existing!, -1, cancellationToken).ConfigureAwait(false);
            }
            await _inner.SendAsync(value, 1, cancellationToken).ConfigureAwait(false);
            lock (_current)
                _current[key] = value;
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>Retracts the current row for <paramref name="key"/>, if any.</summary>
    public async ValueTask DeleteAsync(TKey key, CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            bool removed;
            TValue? existing;
            lock (_current)
                removed = _current.Remove(key, out existing);
            if (removed)
                await _inner.SendAsync(existing!, -1, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>The current row for <paramref name="key"/>, as this handle last sent it.</summary>
    public bool TryGetCurrent(TKey key, [System.Diagnostics.CodeAnalysis.MaybeNullWhen(false)] out TValue value)
    {
        lock (_current)
            return _current.TryGetValue(key, out value);
    }

    public void Complete() => _inner.Complete();
}

/// <summary>
/// Consumer-side handle to a circuit output: a stream of Z-set deltas, one per step that
/// produced changes. Integrate the deltas (e.g. with <see cref="ZSet{T}.AddAll"/>) to
/// materialize the current state of the output collection.
/// </summary>
public sealed class OutputHandle<T> where T : notnull
{
    private readonly string _name;

    internal OutputHandle(string name, ChannelReader<ZSet<T>> reader)
    {
        _name = name;
        Reader = reader;
    }

    /// <summary>Set by the circuit for outputs declared with <c>snapshot: true</c>.</summary>
    internal Func<CancellationToken, Task<ZSet<T>>>? SnapshotReader { get; set; }

    public ChannelReader<ZSet<T>> Reader { get; }

    public IAsyncEnumerable<ZSet<T>> ReadAllAsync(CancellationToken cancellationToken = default) =>
        Reader.ReadAllAsync(cancellationToken);

    /// <summary>
    /// The current integrated state of the view, as of the last completed step — the
    /// bootstrap point for a consumer that cannot replay every delta since the beginning.
    /// Only valid on an output declared with <c>Output(name, snapshot: true)</c>.
    ///
    /// Bootstrap contract: take the snapshot first, then integrate the deltas read
    /// <em>afterwards</em> on top of it. Deltas still buffered when the snapshot is taken
    /// are already contained in it, so this call discards them — do not read deltas
    /// concurrently with it. After a circuit restart against a checkpointing storage
    /// provider the snapshot reflects the committed checkpoint; deltas published between
    /// the last commit and a crash are never replayed, so a consumer recovers by
    /// re-bootstrapping from here rather than expecting the gap to be filled. In a
    /// clustered circuit the snapshot covers this process's workers only, like the delta
    /// stream itself. The read waits for any in-flight step to finish, so it never observes
    /// a half-applied step.
    /// </summary>
    public Task<ZSet<T>> ReadSnapshotAsync(CancellationToken cancellationToken = default) =>
        SnapshotReader is { } read
            ? read(cancellationToken)
            : throw new InvalidOperationException(
                $"Output '{_name}' does not maintain a snapshot; declare it with .Output(\"{_name}\", snapshot: true) to read one.");
}
