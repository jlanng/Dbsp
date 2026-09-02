using System.Buffers.Binary;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Threading.Channels;
using Incremental.Diagnostics;
using Incremental.Operators;
using Incremental.Runtime;
using Microsoft.Extensions.Logging;

namespace Incremental.Cluster;

/// <summary>
/// The per-circuit engine behind cross-process exchange. Every process runs the same
/// circuit definition and steps in lockstep; this class owns the traffic that keeps them
/// aligned, all of it riding on one invariant: frames from a given peer arrive in send
/// order, and every process sends the same deterministic frame sequence per step (one data
/// frame per exchange node per peer — empty shards included — then one StepEnd), so the
/// receiver always knows exactly which frame comes next and a mismatch is a desync fault,
/// not a reordering puzzle.
///
/// A demux task per peer splits the inbound stream: data frames feed the bounded per-peer
/// shard queue the designated worker (local id 0) drains mid-step at each exchange, StepEnd
/// frames feed the coordinator's end-of-step alignment (which also carries the watermark
/// observations for the global min-reduction), and a Fault frame — or a dropped connection —
/// fails both queues, so a peer's death faults this circuit through the ordinary worker
/// fault path instead of hanging a barrier.
/// </summary>
internal sealed class ClusterRuntime(
    IExchangeTransport transport, int localWorkers, SpinBarrier barrier, TimeSpan? exchangeTimeout,
    CircuitDiagnostics diagnostics) : IAsyncDisposable
{
    internal const int StepEndNode = -1;
    internal const int FaultNode = -2;

    private readonly IExchangeTransport _transport = transport;
    private readonly Channel<ExchangeFrame>[] _shards = CreatePeerChannels(transport, capacity: 32);
    private readonly Channel<ExchangeFrame>[] _stepEnds = CreatePeerChannels(transport, capacity: 4);
    private readonly CancellationTokenSource _stop = new();
    private Task? _started;
    private Task[]? _demux;
    private int _poisoned;

    public int ProcessIndex => _transport.ProcessIndex;
    public int ProcessCount => _transport.ProcessCount;
    public int GlobalWorkers => localWorkers * ProcessCount;
    public int FirstGlobalWorker => localWorkers * ProcessIndex;
    public int LocalWorkers => localWorkers;

    private static Channel<ExchangeFrame>[] CreatePeerChannels(IExchangeTransport transport, int capacity)
    {
        var channels = new Channel<ExchangeFrame>[transport.ProcessCount];
        for (var p = 0; p < transport.ProcessCount; p++)
            if (p != transport.ProcessIndex)
                channels[p] = Channel.CreateBounded<ExchangeFrame>(new BoundedChannelOptions(capacity)
                {
                    SingleReader = true,
                    SingleWriter = true,
                    FullMode = BoundedChannelFullMode.Wait,
                });
        return channels;
    }

    /// <summary>Connects the transport and starts the demux tasks. Called by the coordinator
    /// before the first step; idempotent.</summary>
    public Task EnsureStartedAsync() => _started ??= StartAsync();

    private async Task StartAsync()
    {
        await _transport.ConnectAsync(_stop.Token).ConfigureAwait(false);
        diagnostics.ClusterLogger.LogInformation(
            "Cluster mesh connected: process {ProcessIndex} of {ProcessCount}, {GlobalWorkers} global workers",
            ProcessIndex, ProcessCount, GlobalWorkers);
        _demux = [.. Peers().Select(p => Task.Run(() => DemuxAsync(p)))];
    }

    private IEnumerable<int> Peers() => Enumerable.Range(0, ProcessCount).Where(p => p != ProcessIndex);

    private async Task DemuxAsync(int peer)
    {
        try
        {
            while (true)
            {
                var frame = await _transport.ReceiveAsync(peer, _stop.Token).ConfigureAwait(false);
                diagnostics.FrameReceived(peer, frame.Payload.Length);
                switch (frame.NodeId)
                {
                    case StepEndNode:
                        await _stepEnds[peer].Writer.WriteAsync(frame, _stop.Token).ConfigureAwait(false);
                        break;
                    case FaultNode:
                        diagnostics.ClusterLogger.LogError(
                            "Cluster process {Peer} reported a fault: {PeerFault}", peer, Encoding.UTF8.GetString(frame.Payload));
                        Fail(peer, new InvalidOperationException(
                            $"Cluster process {peer} faulted: {Encoding.UTF8.GetString(frame.Payload)}"));
                        return;
                    default:
                        await _shards[peer].Writer.WriteAsync(frame, _stop.Token).ConfigureAwait(false);
                        break;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Local shutdown or poison; the local waiters are released by the same token.
            Fail(peer, null);
        }
        catch (Exception ex)
        {
            diagnostics.ClusterLogger.LogWarning(ex, "Lost the connection to cluster process {Peer}", peer);
            Fail(peer, new InvalidOperationException($"Lost the connection to cluster process {peer}: {ex.Message}", ex));
        }
    }

    private void Fail(int peer, Exception? error)
    {
        _shards[peer].Writer.TryComplete(error);
        _stepEnds[peer].Writer.TryComplete(error);
    }

    /// <summary>End-of-step alignment, on the coordinator after all local workers finish:
    /// broadcasts this process's StepEnd — carrying its watermark observations — and waits
    /// for every peer's, folding their observations into the clock. Once this returns, every
    /// process has finished the step and every process's subsequent
    /// <see cref="WatermarkClock.Freeze"/> computes the same global watermark (min over
    /// sources of the max event time seen on ANY process), so shard-local eviction and
    /// window-close decisions are globally consistent.</summary>
    public async Task AwaitStepEndsAsync(long step, WatermarkClock? clock)
    {
        var payload = clock is null ? [] : EncodeWatermarks(clock.CollectMaxSeen());
        foreach (var peer in Peers())
            await SendAsync(peer, new ExchangeFrame(step, StepEndNode, payload),
                $"waiting for room to send the step-end frame of step {step} to process {peer}").ConfigureAwait(false);
        foreach (var peer in Peers())
        {
            var frame = await ReadAsync(_stepEnds[peer],
                $"waiting for process {peer} to finish step {step}").ConfigureAwait(false);
            Validate(frame, step, StepEndNode, peer);
            if (clock is not null && frame.Payload.Length > 0)
                clock.ObserveRemote(DecodeWatermarks(frame.Payload));
        }
    }

    /// <summary>Ships one exchange node's shards for one peer process (called by the
    /// designated worker, mid-step, in node order). Blocks the worker thread when the
    /// bounded send queue is full — backpressure from a slow peer stalls the step, bounded
    /// by the exchange timeout when one is configured.</summary>
    public void SendShards(long step, int node, int targetProcess, byte[] payload) =>
        Wait(SendAsync(targetProcess, new ExchangeFrame(step, node, payload),
            $"waiting for room to ship the shards of exchange node {node} (step {step}) to process {targetProcess}").AsTask());

    /// <summary>The inbound shards of one exchange node from one peer (designated worker,
    /// mid-step). Blocks until the frame arrives, the peer dies (descriptive fault), the
    /// exchange timeout lapses (descriptive fault), or the local circuit is poisoned
    /// (silent unwind, like a poisoned barrier).</summary>
    public byte[] ReceiveShards(long step, int node, int fromProcess)
    {
        var frame = Wait(ReadAsync(_shards[fromProcess],
            $"waiting for the shards of exchange node {node} (step {step}) from process {fromProcess}"));
        Validate(frame, step, node, fromProcess);
        return frame.Payload;
    }

    /// <summary>The local fault path: releases every local wait on cluster traffic and tells
    /// the peers — a best-effort Fault frame with the message, backstopped by the disconnect
    /// when the transport is torn down — so their circuits fault promptly too. Idempotent.</summary>
    public void Poison(Exception cause)
    {
        if (Interlocked.Exchange(ref _poisoned, 1) != 0)
            return;

        _stop.Cancel();
        // The designated worker unwinds through its released cluster wait; its local peers
        // may be spinning at the exchange's second barrier, so poison that too — otherwise
        // a wait released here would leave them spinning forever.
        barrier.Poison();
        var payload = Encoding.UTF8.GetBytes(cause.Message);
        _ = Task.Run(async () =>
        {
            using var courtesy = new CancellationTokenSource(TimeSpan.FromSeconds(1));
            foreach (var peer in Peers())
            {
                try
                {
                    await _transport.SendAsync(peer, new ExchangeFrame(0, FaultNode, payload), courtesy.Token).ConfigureAwait(false);
                }
                catch
                {
                    // The disconnect below reaches this peer instead.
                }
            }
            try
            {
                await _transport.DisposeAsync().ConfigureAwait(false);
            }
            catch
            {
                // Teardown is best-effort; the circuit is already faulted.
            }
        });
    }

    private static void Validate(in ExchangeFrame frame, long step, int node, int peer)
    {
        if (frame.Step != step || frame.NodeId != node)
            throw new InvalidOperationException(
                $"Cluster desync: process {peer} sent (step {frame.Step}, node {frame.NodeId}) where (step {step}, node {node}) was expected. " +
                "Every process must run the same circuit definition with the same options and step in lockstep.");
    }

    private async ValueTask<ExchangeFrame> ReadAsync(Channel<ExchangeFrame> channel, string awaiting)
    {
        using var deadline = StartDeadline();
        try
        {
            return await channel.Reader.ReadAsync(deadline?.Token ?? _stop.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (deadline is not null && !_stop.IsCancellationRequested)
        {
            throw Timeout(awaiting);
        }
        catch (ChannelClosedException ex) when (ex.InnerException is { } inner)
        {
            ExceptionDispatchInfo.Capture(inner).Throw();
            throw; // unreachable
        }
        catch (ChannelClosedException)
        {
            // Completed without an error: local shutdown/poison raced ahead of the token.
            throw new BarrierPoisonedException();
        }
    }

    private async ValueTask SendAsync(int peer, ExchangeFrame frame, string awaiting)
    {
        using var deadline = StartDeadline();
        try
        {
            await _transport.SendAsync(peer, frame, deadline?.Token ?? _stop.Token).ConfigureAwait(false);
            diagnostics.FrameSent(peer, frame.Payload.Length);
        }
        catch (OperationCanceledException) when (deadline is not null && !_stop.IsCancellationRequested)
        {
            throw Timeout(awaiting);
        }
    }

    /// <summary>Null when no exchange timeout is configured — waits then ride the plain
    /// poison/shutdown token alone.</summary>
    private CancellationTokenSource? StartDeadline()
    {
        if (exchangeTimeout is not { } timeout)
            return null;
        var deadline = CancellationTokenSource.CreateLinkedTokenSource(_stop.Token);
        deadline.CancelAfter(timeout);
        return deadline;
    }

    private TimeoutException Timeout(string awaiting)
    {
        diagnostics.ClusterLogger.LogError(
            "Cluster process {ProcessIndex} timed out after {Timeout} {Awaiting}; faulting the circuit",
            ProcessIndex, exchangeTimeout, awaiting);
        return new TimeoutException(
            $"Cluster process {ProcessIndex} timed out after {exchangeTimeout} {awaiting}. The peer is stalled or " +
            "unreachable; this circuit is now faulted (terminal). Rebuild every process from the last committed " +
            "checkpoint once the cluster is healthy, or raise CircuitOptions.ExchangeTimeout if healthy steps can " +
            "legitimately take this long.");
    }

    private T Wait<T>(ValueTask<T> task) => Wait(task.AsTask());

    private T Wait<T>(Task<T> task)
    {
        Wait((Task)task);
        return task.Result;
    }

    /// <summary>Synchronously waits on the worker thread (dedicated, so blocking is the
    /// intended backpressure), translating a local poison/shutdown cancellation into the
    /// silent barrier-poison unwind — the real fault was recorded by whoever poisoned.</summary>
    private void Wait(Task task)
    {
        try
        {
            task.GetAwaiter().GetResult();
        }
        catch (OperationCanceledException) when (_stop.IsCancellationRequested)
        {
            throw new BarrierPoisonedException();
        }
    }

    private static byte[] EncodeWatermarks(long[] maxSeen)
    {
        var payload = new byte[maxSeen.Length * 8];
        for (var t = 0; t < maxSeen.Length; t++)
            BinaryPrimitives.WriteInt64LittleEndian(payload.AsSpan(t * 8), maxSeen[t]);
        return payload;
    }

    private static long[] DecodeWatermarks(byte[] payload)
    {
        var maxSeen = new long[payload.Length / 8];
        for (var t = 0; t < maxSeen.Length; t++)
            maxSeen[t] = BinaryPrimitives.ReadInt64LittleEndian(payload.AsSpan(t * 8));
        return maxSeen;
    }

    public async ValueTask DisposeAsync()
    {
        _stop.Cancel();
        // A dispose racing an in-flight step (a timed-out test, a crashing host) must not
        // leave workers spinning at an exchange barrier whose designated worker just
        // unwound; on a clean dispose the workers have already exited and this is inert.
        barrier.Poison();
        try
        {
            await _transport.DisposeAsync().ConfigureAwait(false);
        }
        catch
        {
            // Transport teardown is best-effort.
        }
        if (_demux is { } demux)
        {
            foreach (var task in demux)
            {
                try
                {
                    await task.ConfigureAwait(false);
                }
                catch
                {
                    // Demux failures already surfaced on the queues.
                }
            }
        }
    }
}
