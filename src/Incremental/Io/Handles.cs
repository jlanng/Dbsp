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

    /// <summary>Inserts <paramref name="value"/> (weight +1), unstamped.</summary>
    public ValueTask InsertAsync(T value, CancellationToken cancellationToken = default) =>
        SendAsync(value, 1, cancellationToken);

    /// <summary>Inserts <paramref name="value"/> stamped with its source offset (partition 0).</summary>
    public ValueTask InsertAsync(T value, long offset, CancellationToken cancellationToken = default) =>
        SendAsync(value, 1, offset, cancellationToken);

    /// <summary>Inserts <paramref name="value"/> stamped with its source partition and offset.</summary>
    public ValueTask InsertAsync(T value, int partition, long offset, CancellationToken cancellationToken = default) =>
        SendAsync(value, 1, partition, offset, cancellationToken);

    /// <summary>Retracts <paramref name="value"/> (weight -1), unstamped.</summary>
    public ValueTask RemoveAsync(T value, CancellationToken cancellationToken = default) =>
        SendAsync(value, -1, cancellationToken);

    /// <summary>Retracts <paramref name="value"/>, stamped with its source offset (partition 0).</summary>
    public ValueTask RemoveAsync(T value, long offset, CancellationToken cancellationToken = default) =>
        SendAsync(value, -1, offset, cancellationToken);

    /// <summary>Retracts <paramref name="value"/>, stamped with its source partition and offset.</summary>
    public ValueTask RemoveAsync(T value, int partition, long offset, CancellationToken cancellationToken = default) =>
        SendAsync(value, -1, partition, offset, cancellationToken);

    /// <summary>Sends <paramref name="value"/> with an explicit signed <paramref name="weight"/>, unstamped.</summary>
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

    /// <summary>Sends every entry of <paramref name="batch"/>, unstamped, in order. Not atomic:
    /// a step may drain part of the batch before the rest is written.</summary>
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

    /// <summary>The channel write, with the fault path surfaced as what it is: a faulting
    /// circuit completes each input's channel with a <see cref="CircuitFaultedException"/>,
    /// and this unwraps the resulting channel-closed error so producers — including ones
    /// blocked mid-write on a full channel — see that exception (original fault as its
    /// inner) rather than a bare closed-channel error. A channel completed normally (via
    /// <see cref="Complete"/> or circuit disposal) still throws
    /// <see cref="ChannelClosedException"/>. The synchronous completion path stays
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

    /// <summary>Signals that no more data will arrive on this input; see <see cref="InputHandle{T}.Complete"/>.</summary>
    public void Complete() => _inner.Complete();
}

/// <summary>
/// Consumer-side handle to a circuit output: a changefeed of Z-set batches, one per step
/// that produced changes. An output is a single bounded channel with exactly one consumer —
/// <see cref="Changes"/> (or <see cref="MaterializeInMemory"/>, which is that consumer) can
/// be claimed once; a second subscription throws instead of silently splitting the feed.
/// A slow consumer stalls the whole circuit via backpressure, deliberately — always run and
/// await your consumer.
/// </summary>
public sealed class OutputHandle<T> where T : notnull
{
    private readonly ChannelReader<ZSet<T>> _reader;
    private int _claimed;

    internal OutputHandle(string name, ChannelReader<ZSet<T>> reader)
    {
        Name = name;
        _reader = reader;
    }

    internal string Name { get; }

    /// <summary>Set by the circuit for bootstrap-enabled (table-shaped) outputs.</summary>
    internal Func<CancellationToken, Task<ZSet<T>>>? SnapshotReader { get; set; }

    /// <summary>Advanced: the output's raw channel, for step-level consumption (one batch
    /// per completed step) and non-blocking emptiness checks. Bypasses the bootstrap batch
    /// and the consumer claim — do not mix with <see cref="Changes"/> or
    /// <see cref="MaterializeInMemory"/>, which read the same channel.</summary>
    public ChannelReader<ZSet<T>> Reader => _reader;

    /// <summary>
    /// The output's changefeed: one batch per step that produced changes, holding every row
    /// the step touched with its signed weight. On a bootstrap-enabled output (a keyed or
    /// windowed result's <c>ToOutput()</c>, by default) the first batch asserts the current
    /// integrated state — empty on a fresh start — so a consumer that maintains state treats
    /// it as a reset; this is what makes consumers correct across restarts and mid-run
    /// subscription, since batches emitted between the last checkpoint and a crash are never
    /// replayed. A plain stream output starts straight into live batches (at-most-once
    /// across a crash). The enumeration completes when the circuit disposes and throws
    /// <see cref="CircuitFaultedException"/> if the circuit faults. One consumer only: a
    /// second call throws.
    /// </summary>
    public IAsyncEnumerable<ZSet<T>> Changes(CancellationToken cancellationToken = default)
    {
        Claim();
        return ChangesCore(cancellationToken);
    }

    private async IAsyncEnumerable<ZSet<T>> ChangesCore(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // The snapshot read discards batches buffered before it — they are contained in it.
        if (SnapshotReader is { } bootstrap)
            yield return await bootstrap(cancellationToken).ConfigureAwait(false);

        await foreach (var batch in _reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            yield return batch;
    }

    /// <summary>
    /// Maintains the output's integrated result in memory and becomes the output's single
    /// consumer. Requires a bootstrap-enabled output (a keyed or windowed result's
    /// <c>ToOutput()</c>) — the automatic resync is what makes the view correct across
    /// restarts and mid-run attachment. The cost is in the name: the view holds the full
    /// live result set on the heap — fine for windowed/evicted results and small
    /// dimensions; for an unbounded keyspace, make it a deliberate decision.
    /// </summary>
    public InMemoryView<T> MaterializeInMemory()
    {
        if (SnapshotReader is null)
            throw new InvalidOperationException(
                $"Output '{Name}' is a plain changefeed with no current state to materialize. " +
                "MaterializeInMemory needs a table-shaped output: call ToOutput() on a keyed or windowed " +
                "result (with its default bootstrap), not on a plain stream.");
        Claim();
        return new InMemoryView<T>(ChangesCore);
    }

    private void Claim()
    {
        if (Interlocked.Exchange(ref _claimed, 1) != 0)
            throw new InvalidOperationException(
                $"Output '{Name}' already has a consumer. An output is a single channel — a second subscription " +
                "would silently split the feed. Declare a second output on the same stream to consume it twice.");
    }
}
