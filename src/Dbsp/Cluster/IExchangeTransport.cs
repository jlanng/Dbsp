namespace Dbsp.Cluster;

/// <summary>
/// One frame of cross-process exchange traffic: the shards of one exchange node for one
/// step (serialized (value, weight) pairs, one shard per local worker of the receiving
/// process), or a step-control frame (negative <see cref="NodeId"/>). Frames from a given
/// sender arrive strictly in the order they were sent — every process executes the same
/// nodes in the same topological order each step, so that order is what keeps steps and
/// exchanges aligned across processes without any per-frame handshake.
/// </summary>
public readonly record struct ExchangeFrame(long Step, int NodeId, byte[] Payload);

/// <summary>
/// Connects the processes of a clustered circuit: a static, fully-connected topology in
/// which every process can send frames to and receive frames from every peer. The contract
/// the runtime relies on:
/// <list type="bullet">
/// <item><see cref="SendAsync"/> preserves per-target order and may suspend when the bounded
/// send queue is full — a slow peer stalls the sender's step instead of buffering
/// unboundedly (backpressure).</item>
/// <item><see cref="ReceiveAsync"/> yields the frames of one sender in exactly the order
/// they were sent, and throws when the peer disconnects or the transport is disposed — a
/// waiting circuit must fault, never hang.</item>
/// </list>
/// The circuit takes ownership of the transport and disposes it with the circuit.
/// </summary>
public interface IExchangeTransport : IAsyncDisposable
{
    /// <summary>This process's index in the cluster, in [0, <see cref="ProcessCount"/>).</summary>
    int ProcessIndex { get; }

    /// <summary>Total number of processes in the cluster.</summary>
    int ProcessCount { get; }

    /// <summary>Establishes the links to every peer. Called once by the circuit before its
    /// first step; peers may start in any order, so implementations should retry until the
    /// whole mesh is up (or a timeout).</summary>
    Task ConnectAsync(CancellationToken cancellationToken);

    /// <summary>Queues a frame for <paramref name="targetProcess"/>, preserving send order.
    /// Suspends when the bounded send queue is full.</summary>
    ValueTask SendAsync(int targetProcess, ExchangeFrame frame, CancellationToken cancellationToken);

    /// <summary>The next frame from <paramref name="senderProcess"/>, in send order. The
    /// runtime has exactly one reader per sender. Throws when the peer is gone.</summary>
    ValueTask<ExchangeFrame> ReceiveAsync(int senderProcess, CancellationToken cancellationToken);
}
