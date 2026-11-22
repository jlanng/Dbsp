using System.Threading.Channels;

namespace Dbsp.Cluster;

/// <summary>
/// An in-process transport for testing clustered circuits: <see cref="CreateMesh"/> returns
/// one transport per "process", linked pairwise by bounded channels — the same ordering,
/// backpressure, and disconnect semantics as a socket transport, without the sockets.
/// Disposing a transport fails every peer's pending and future reads from it, which is how
/// a peer "crash" propagates in tests.
/// </summary>
public sealed class InMemoryExchangeTransport : IExchangeTransport
{
    private readonly Channel<ExchangeFrame>[,] _links; // [from, to]; diagonal unused
    private readonly CancellationTokenSource[] _closed; // shared: per-process shutdown signal

    private InMemoryExchangeTransport(int processIndex, Channel<ExchangeFrame>[,] links, CancellationTokenSource[] closed)
    {
        ProcessIndex = processIndex;
        _links = links;
        _closed = closed;
    }

    public int ProcessIndex { get; }
    public int ProcessCount => _closed.Length;

    /// <summary>Creates the fully-connected mesh: element <c>i</c> is process <c>i</c>'s
    /// transport. <paramref name="capacity"/> bounds each directed link's send queue.</summary>
    public static InMemoryExchangeTransport[] CreateMesh(int processCount, int capacity = 64)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(processCount, 1, nameof(processCount));
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 1, nameof(capacity));

        var links = new Channel<ExchangeFrame>[processCount, processCount];
        for (var from = 0; from < processCount; from++)
            for (var to = 0; to < processCount; to++)
                if (from != to)
                    links[from, to] = Channel.CreateBounded<ExchangeFrame>(new BoundedChannelOptions(capacity)
                    {
                        SingleReader = true,
                        SingleWriter = false,
                        FullMode = BoundedChannelFullMode.Wait,
                    });

        var closed = Enumerable.Range(0, processCount).Select(_ => new CancellationTokenSource()).ToArray();
        return [.. Enumerable.Range(0, processCount).Select(i => new InMemoryExchangeTransport(i, links, closed))];
    }

    public Task ConnectAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public async ValueTask SendAsync(int targetProcess, ExchangeFrame frame, CancellationToken cancellationToken)
    {
        // Link the target's shutdown in so a send to a dead peer fails instead of blocking
        // forever on a queue nobody drains.
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken, _closed[ProcessIndex].Token, _closed[targetProcess].Token);
        try
        {
            await _links[ProcessIndex, targetProcess].Writer.WriteAsync(frame, linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (_closed[targetProcess].IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            throw new IOException($"Cluster process {targetProcess} has shut down.");
        }
    }

    public async ValueTask<ExchangeFrame> ReceiveAsync(int senderProcess, CancellationToken cancellationToken)
    {
        try
        {
            return await _links[senderProcess, ProcessIndex].Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (ChannelClosedException ex)
        {
            throw new IOException($"Cluster process {senderProcess} has shut down.", ex);
        }
    }

    public ValueTask DisposeAsync()
    {
        _closed[ProcessIndex].Cancel();
        for (var to = 0; to < ProcessCount; to++)
            if (to != ProcessIndex)
                _links[ProcessIndex, to].Writer.TryComplete(new IOException($"Cluster process {ProcessIndex} has shut down."));
        return ValueTask.CompletedTask;
    }
}
