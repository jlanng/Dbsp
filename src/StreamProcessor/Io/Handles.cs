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
