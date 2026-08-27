using System.Threading.Channels;

namespace Incremental;

/// <summary>One queued input record: a weighted value plus the source offset the producer
/// stamped it with, or -1 when none — the input then numbers records as it consumes them.</summary>
internal readonly record struct InputEntry<T>(T Value, long Weight, long Offset) where T : notnull;

/// <summary>
/// Producer-side handle to a circuit input. Writes go through a bounded channel: when the
/// circuit cannot keep up, the returned <see cref="ValueTask"/>s suspend until capacity
/// frees up — backpressure is the await, there is nothing else to configure.
///
/// The overloads taking an <c>offset</c> stamp the record with its position in the source
/// it was read from (e.g. a Kafka partition offset). Offsets must be non-negative and
/// increase monotonically; a record whose offset is at or below the input's current
/// position is dropped as a duplicate. With a checkpointing storage provider that position
/// starts at the committed checkpoint after a restart, so a producer can replay its source
/// from <see cref="Circuit.CommittedOffset"/> (or earlier) and already-processed records
/// are ignored — exactly-once from the state's point of view.
/// </summary>
public sealed class InputHandle<T> where T : notnull
{
    private readonly ChannelWriter<InputEntry<T>> _writer;

    internal InputHandle(ChannelWriter<InputEntry<T>> writer) => _writer = writer;

    public ValueTask InsertAsync(T value, CancellationToken cancellationToken = default) =>
        SendAsync(value, 1, cancellationToken);

    /// <summary>Inserts <paramref name="value"/> stamped with its source offset.</summary>
    public ValueTask InsertAsync(T value, long offset, CancellationToken cancellationToken = default) =>
        SendAsync(value, 1, offset, cancellationToken);

    public ValueTask RemoveAsync(T value, CancellationToken cancellationToken = default) =>
        SendAsync(value, -1, cancellationToken);

    /// <summary>Retracts <paramref name="value"/>, stamped with its source offset.</summary>
    public ValueTask RemoveAsync(T value, long offset, CancellationToken cancellationToken = default) =>
        SendAsync(value, -1, offset, cancellationToken);

    public ValueTask SendAsync(T value, long weight, CancellationToken cancellationToken = default) =>
        WriteAsync(new InputEntry<T>(value, weight, -1), cancellationToken);

    /// <summary>Sends a weighted record stamped with its source offset.</summary>
    public ValueTask SendAsync(T value, long weight, long offset, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        return WriteAsync(new InputEntry<T>(value, weight, offset), cancellationToken);
    }

    public async ValueTask SendAsync(IEnumerable<WeightedValue<T>> batch, CancellationToken cancellationToken = default)
    {
        foreach (var entry in batch)
            await WriteAsync(new InputEntry<T>(entry.Value, entry.Weight, -1), cancellationToken).ConfigureAwait(false);
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
    public int Count => _current.Count;

    /// <summary>Inserts the row, or replaces the existing row with its key (retraction +
    /// assertion). Upserting an identical row is a no-op.</summary>
    public async ValueTask UpsertAsync(TValue value, CancellationToken cancellationToken = default)
    {
        var key = _keySelector(value);
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_current.TryGetValue(key, out var existing))
            {
                if (EqualityComparer<TValue>.Default.Equals(existing, value))
                    return;
                await _inner.SendAsync(existing, -1, cancellationToken).ConfigureAwait(false);
            }
            await _inner.SendAsync(value, 1, cancellationToken).ConfigureAwait(false);
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
            if (_current.Remove(key, out var existing))
                await _inner.SendAsync(existing, -1, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>The current row for <paramref name="key"/>, as this handle last sent it.</summary>
    public bool TryGetCurrent(TKey key, [System.Diagnostics.CodeAnalysis.MaybeNullWhen(false)] out TValue value) =>
        _current.TryGetValue(key, out value);

    public void Complete() => _inner.Complete();
}

/// <summary>
/// Consumer-side handle to a circuit output: a stream of Z-set deltas, one per step that
/// produced changes. Integrate the deltas (e.g. with <see cref="ZSet{T}.AddAll"/>) to
/// materialize the current state of the output collection.
/// </summary>
public sealed class OutputHandle<T> where T : notnull
{
    internal OutputHandle(ChannelReader<ZSet<T>> reader) => Reader = reader;

    public ChannelReader<ZSet<T>> Reader { get; }

    public IAsyncEnumerable<ZSet<T>> ReadAllAsync(CancellationToken cancellationToken = default) =>
        Reader.ReadAllAsync(cancellationToken);
}
