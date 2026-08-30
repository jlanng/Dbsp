using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using Incremental.Diagnostics;

namespace Incremental.Cluster;

/// <summary>
/// TCP transport for clustered circuits: a static mesh over plain sockets, no external
/// dependencies. Every process is given the same endpoint list — <c>endpoints[i]</c> is
/// where process <c>i</c> listens — and its own index. <see cref="ConnectAsync"/> listens on
/// the local endpoint's port and dials every peer (retrying until the whole mesh is up or
/// the connect timeout lapses, so processes may start in any order); each ordered pair of
/// processes gets one connection, identified by a 4-byte sender-index handshake, carrying
/// that direction's frames as length-prefixed messages. Sends go through a bounded per-peer
/// queue drained by a pump task: a slow peer fills the queue and stalls the sender's step —
/// bounded backpressure, never an unbounded buffer. A failed or closed connection fails the
/// transport's pending reads and writes, which faults the circuit instead of hanging it.
///
/// <para><b>Security: this transport is unauthenticated cleartext.</b> The handshake is a
/// bare peer index and frames are unencrypted on the socket — anything that can reach the
/// port can join the mesh and inject data, and every exchanged record crosses the wire
/// readable (the dense encoding is a compression, not a cipher). Run it only on a trusted,
/// isolated network segment (or over an encrypted
/// tunnel the deployment provides); the <c>maxFrameBytes</c> cap bounds what a single
/// frame can make the receiver allocate, but it is a robustness cap, not an
/// authentication mechanism.</para>
/// </summary>
public sealed class TcpExchangeTransport : IExchangeTransport, ITransportDiagnostics
{
    // Frame header: [int32 payload length][int64 step][int32 nodeId], little-endian.
    private const int HeaderSize = 16;

    private readonly string[] _endpoints;
    private readonly int _maxFrameBytes;
    private readonly TimeSpan _connectTimeout;
    private readonly Channel<ExchangeFrame>[] _sendQueues;
    private readonly NetworkStream?[] _inbound;
    private readonly NetworkStream?[] _outbound;
    private readonly List<TcpClient> _clients = [];
    private readonly CancellationTokenSource _closed = new();
    private readonly Task[] _pumps;
    private TcpListener? _listener;

    /// <param name="processIndex">This process's index into <paramref name="endpoints"/>.</param>
    /// <param name="endpoints">One <c>host:port</c> per process; identical on every process.</param>
    /// <param name="connectTimeout">How long <see cref="ConnectAsync"/> retries for the full
    /// mesh before giving up (default 15 s).</param>
    /// <param name="sendQueueCapacity">Frames buffered per peer before senders block.</param>
    /// <param name="maxFrameBytes">Largest frame payload this process will send or accept
    /// (default 64 MB). An inbound length prefix beyond it is rejected as a corrupt or
    /// hostile frame — a descriptive fault instead of an attacker-sized allocation. Must be
    /// identical on every process, and larger than the biggest shard a step can ship (scale
    /// with batch sizes when raising it).</param>
    public TcpExchangeTransport(
        int processIndex,
        IReadOnlyList<string> endpoints,
        TimeSpan? connectTimeout = null,
        int sendQueueCapacity = 256,
        int maxFrameBytes = 64 << 20)
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        ArgumentOutOfRangeException.ThrowIfLessThan(endpoints.Count, 1, nameof(endpoints));
        ArgumentOutOfRangeException.ThrowIfNegative(processIndex, nameof(processIndex));
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(processIndex, endpoints.Count, nameof(processIndex));
        ArgumentOutOfRangeException.ThrowIfLessThan(sendQueueCapacity, 1, nameof(sendQueueCapacity));
        ArgumentOutOfRangeException.ThrowIfLessThan(maxFrameBytes, 1024, nameof(maxFrameBytes));

        ProcessIndex = processIndex;
        _maxFrameBytes = maxFrameBytes;
        _endpoints = [.. endpoints];
        _connectTimeout = connectTimeout ?? TimeSpan.FromSeconds(15);
        _inbound = new NetworkStream?[endpoints.Count];
        _outbound = new NetworkStream?[endpoints.Count];
        _pumps = new Task[endpoints.Count];
        _sendQueues = new Channel<ExchangeFrame>[endpoints.Count];
        for (var p = 0; p < endpoints.Count; p++)
            if (p != processIndex)
                _sendQueues[p] = Channel.CreateBounded<ExchangeFrame>(new BoundedChannelOptions(sendQueueCapacity)
                {
                    SingleReader = true,
                    SingleWriter = false,
                    FullMode = BoundedChannelFullMode.Wait,
                });
    }

    public int ProcessIndex { get; }
    public int ProcessCount => _endpoints.Length;

    /// <summary>Feeds the <c>incremental.cluster.send_queue_depth</c> gauge.</summary>
    IEnumerable<(int Peer, int Depth)> ITransportDiagnostics.SendQueueDepths
    {
        get
        {
            for (var p = 0; p < _sendQueues.Length; p++)
                if (_sendQueues[p] is { } queue)
                    yield return (p, queue.Reader.Count);
        }
    }

    public async Task ConnectAsync(CancellationToken cancellationToken)
    {
        if (ProcessCount == 1)
            return;

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _closed.Token);
        timeout.CancelAfter(_connectTimeout);

        _listener = new TcpListener(IPAddress.Any, ParsePort(_endpoints[ProcessIndex]));
        _listener.Start();

        try
        {
            await Task.WhenAll(AcceptAllAsync(timeout.Token), DialAllAsync(timeout.Token)).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested && !_closed.IsCancellationRequested)
        {
            throw new TimeoutException(
                $"Cluster process {ProcessIndex} could not establish connections to all peers within {_connectTimeout}. " +
                "Every process of the cluster must be started with the same endpoint list.");
        }

        for (var p = 0; p < ProcessCount; p++)
        {
            if (p == ProcessIndex)
                continue;
            var peer = p; // capture per iteration — the loop variable outlives the lambda
            _pumps[peer] = Task.Run(() => PumpSendsAsync(peer, _outbound[peer]!));
        }
    }

    /// <summary>Accepts one inbound connection per peer; the peer's first 4 bytes are its index.</summary>
    private async Task AcceptAllAsync(CancellationToken cancellationToken)
    {
        var handshake = new byte[4];
        for (var accepted = 0; accepted < ProcessCount - 1; accepted++)
        {
            var client = await _listener!.AcceptTcpClientAsync(cancellationToken).ConfigureAwait(false);
            client.NoDelay = true;
            lock (_clients)
                _clients.Add(client);
            var stream = client.GetStream();
            await stream.ReadExactlyAsync(handshake, cancellationToken).ConfigureAwait(false);
            var sender = BinaryPrimitives.ReadInt32LittleEndian(handshake);
            if (sender < 0 || sender >= ProcessCount || sender == ProcessIndex || _inbound[sender] is not null)
                throw new InvalidOperationException($"Cluster process {ProcessIndex} received an invalid handshake (sender index {sender}).");
            _inbound[sender] = stream;
        }
    }

    private Task DialAllAsync(CancellationToken cancellationToken) =>
        Task.WhenAll(Enumerable.Range(0, ProcessCount).Where(p => p != ProcessIndex).Select(p => DialAsync(p, cancellationToken)));

    private async Task DialAsync(int peer, CancellationToken cancellationToken)
    {
        var (host, port) = ParseEndpoint(_endpoints[peer]);
        while (true)
        {
            var client = new TcpClient { NoDelay = true };
            try
            {
                await client.ConnectAsync(host, port, cancellationToken).ConfigureAwait(false);
            }
            catch (SocketException)
            {
                // The peer's listener may simply not be up yet; retry until the timeout.
                client.Dispose();
                await Task.Delay(100, cancellationToken).ConfigureAwait(false);
                continue;
            }

            lock (_clients)
                _clients.Add(client);
            var stream = client.GetStream();
            var handshake = new byte[4];
            BinaryPrimitives.WriteInt32LittleEndian(handshake, ProcessIndex);
            await stream.WriteAsync(handshake, cancellationToken).ConfigureAwait(false);
            _outbound[peer] = stream;
            return;
        }
    }

    /// <summary>Drains the bounded send queue of one peer onto its socket, in order.</summary>
    private async Task PumpSendsAsync(int peer, NetworkStream stream)
    {
        var header = new byte[HeaderSize];
        try
        {
            await foreach (var frame in _sendQueues[peer].Reader.ReadAllAsync(_closed.Token).ConfigureAwait(false))
            {
                BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(0), frame.Payload.Length);
                BinaryPrimitives.WriteInt64LittleEndian(header.AsSpan(4), frame.Step);
                BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(12), frame.NodeId);
                await stream.WriteAsync(header, _closed.Token).ConfigureAwait(false);
                if (frame.Payload.Length > 0)
                    await stream.WriteAsync(frame.Payload, _closed.Token).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            // Senders blocked on (or arriving at) this queue must fail, not fill it forever.
            _sendQueues[peer].Writer.TryComplete(
                ex is OperationCanceledException ? new IOException($"The connection to cluster process {peer} was closed.") : ex);
        }
    }

    public async ValueTask SendAsync(int targetProcess, ExchangeFrame frame, CancellationToken cancellationToken)
    {
        if (frame.Payload.Length > _maxFrameBytes)
            throw new InvalidOperationException(
                $"Refusing to send a {frame.Payload.Length}-byte frame to cluster process {targetProcess}: " +
                $"it exceeds maxFrameBytes ({_maxFrameBytes}), so the peer would reject it as corrupt. " +
                "Raise maxFrameBytes identically on every process, or reduce the step batch size.");
        try
        {
            await _sendQueues[targetProcess].Writer.WriteAsync(frame, cancellationToken).ConfigureAwait(false);
        }
        catch (ChannelClosedException ex)
        {
            throw new IOException($"The connection to cluster process {targetProcess} was closed.", ex.InnerException ?? ex);
        }
    }

    public async ValueTask<ExchangeFrame> ReceiveAsync(int senderProcess, CancellationToken cancellationToken)
    {
        var stream = _inbound[senderProcess]
            ?? throw new InvalidOperationException($"Not connected to cluster process {senderProcess}; call ConnectAsync first.");
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _closed.Token);

        var header = new byte[HeaderSize];
        await stream.ReadExactlyAsync(header, linked.Token).ConfigureAwait(false);
        // Validate before allocating: the length prefix is untrusted input, and a corrupt or
        // hostile value must produce a descriptive fault, not an attacker-sized allocation.
        var length = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(0));
        if (length < 0 || length > _maxFrameBytes)
            throw new InvalidOperationException(
                $"Corrupt frame from cluster process {senderProcess}: payload length {length} is outside " +
                $"[0, {_maxFrameBytes}] (maxFrameBytes). Either the connection carries non-cluster traffic " +
                "or the peers were configured with different maxFrameBytes values.");
        var payload = length == 0 ? [] : new byte[length];
        if (length > 0)
            await stream.ReadExactlyAsync(payload, linked.Token).ConfigureAwait(false);
        return new ExchangeFrame(
            BinaryPrimitives.ReadInt64LittleEndian(header.AsSpan(4)),
            BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(12)),
            payload);
    }

    public async ValueTask DisposeAsync()
    {
        _closed.Cancel();
        foreach (var queue in _sendQueues)
            queue?.Writer.TryComplete(new IOException($"Cluster process {ProcessIndex} has shut down."));
        _listener?.Stop();
        lock (_clients)
        {
            foreach (var client in _clients)
                client.Dispose();
        }
        foreach (var pump in _pumps)
        {
            if (pump is null)
                continue;
            try
            {
                await pump.ConfigureAwait(false);
            }
            catch
            {
                // Pump failures were already surfaced on the send queues.
            }
        }
        // _closed is deliberately not disposed: late receives may still link against its
        // token, and an undisposed CTS without timers holds no unmanaged resources.
    }

    private static int ParsePort(string endpoint) => ParseEndpoint(endpoint).Port;

    private static (string Host, int Port) ParseEndpoint(string endpoint)
    {
        var separator = endpoint.LastIndexOf(':');
        if (separator <= 0 || !int.TryParse(endpoint[(separator + 1)..], out var port) || port is < 1 or > 65535)
            throw new ArgumentException($"Cluster endpoint '{endpoint}' is not of the form host:port.");
        return (endpoint[..separator], port);
    }
}
