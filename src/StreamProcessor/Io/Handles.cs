using System.Threading.Channels;

namespace StreamProcessor;

/// <summary>
/// Producer-side handle to a circuit input. Writes go through a bounded channel: when the
/// circuit cannot keep up, the returned <see cref="ValueTask"/>s suspend until capacity
/// frees up — backpressure is the await, there is nothing else to configure.
/// </summary>
public sealed class InputHandle<T> where T : notnull
{
    private readonly ChannelWriter<WeightedValue<T>> _writer;

    internal InputHandle(ChannelWriter<WeightedValue<T>> writer) => _writer = writer;

    public ValueTask InsertAsync(T value, CancellationToken cancellationToken = default) =>
        SendAsync(value, 1, cancellationToken);

    public ValueTask RemoveAsync(T value, CancellationToken cancellationToken = default) =>
        SendAsync(value, -1, cancellationToken);

    public ValueTask SendAsync(T value, long weight, CancellationToken cancellationToken = default) =>
        _writer.WriteAsync(new WeightedValue<T>(value, weight), cancellationToken);

    public async ValueTask SendAsync(IEnumerable<WeightedValue<T>> batch, CancellationToken cancellationToken = default)
    {
        foreach (var entry in batch)
            await _writer.WriteAsync(entry, cancellationToken).ConfigureAwait(false);
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
