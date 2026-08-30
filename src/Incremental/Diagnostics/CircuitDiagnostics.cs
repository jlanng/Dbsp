using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Incremental.Runtime;
using Microsoft.Extensions.Logging;

namespace Incremental.Diagnostics;

/// <summary>Implemented by transports that can report their internal queue depths; the
/// send-queue gauge covers transports that opt in and is simply absent for others.</summary>
internal interface ITransportDiagnostics
{
    IEnumerable<(int Peer, int Depth)> SendQueueDepths { get; }
}

/// <summary>
/// The library's observability surface. All instruments hang off one static
/// <see cref="System.Diagnostics.Metrics.Meter"/> named <c>Incremental</c> — the BCL meter
/// is effectively free with no listener attached, so instrumentation stays on
/// unconditionally — and every measurement carries a <c>circuit</c> tag
/// (<see cref="CircuitOptions.Name"/>) so circuits sharing a process stay distinguishable.
/// One instance exists per circuit: it holds the circuit's tag, its loggers, and the live
/// state the observable gauges (queue depths, watermark lag) read on demand; instances
/// register themselves for the gauges and unregister on dispose.
/// </summary>
internal sealed class CircuitDiagnostics : IDisposable
{
    public static readonly Meter Meter = new("Incremental", "0.1.0");

    private static readonly Counter<long> Steps = Meter.CreateCounter<long>(
        "incremental.circuit.steps", "{step}", "Steps completed.");
    private static readonly Histogram<double> StepDuration = Meter.CreateHistogram<double>(
        "incremental.circuit.step_duration", "s", "Wall-clock duration of a completed step, input drain through output publish.");
    private static readonly Counter<long> Records = Meter.CreateCounter<long>(
        "incremental.circuit.records", "{record}", "Input records drained into steps (after replay dedup).");
    private static readonly Counter<long> Faults = Meter.CreateCounter<long>(
        "incremental.circuit.faults", "{fault}", "Transitions to the terminal faulted state.");
    private static readonly Counter<long> CheckpointCommits = Meter.CreateCounter<long>(
        "incremental.circuit.checkpoint_commits", "{commit}", "Checkpoint commits (state + input offsets, atomic).");
    private static readonly Histogram<double> CheckpointCommitDuration = Meter.CreateHistogram<double>(
        "incremental.circuit.checkpoint_commit_duration", "s", "Wall-clock duration of a checkpoint commit.");
    private static readonly Counter<long> ClusterFramesSent = Meter.CreateCounter<long>(
        "incremental.cluster.frames_sent", "{frame}", "Exchange frames handed to the transport.");
    private static readonly Counter<long> ClusterFramesReceived = Meter.CreateCounter<long>(
        "incremental.cluster.frames_received", "{frame}", "Exchange frames received from peers.");
    private static readonly Counter<long> ClusterBytesSent = Meter.CreateCounter<long>(
        "incremental.cluster.bytes_sent", "By", "Payload bytes handed to the transport.");
    private static readonly Counter<long> ClusterBytesReceived = Meter.CreateCounter<long>(
        "incremental.cluster.bytes_received", "By", "Payload bytes received from peers.");

    // The observable gauges are static (a Meter instrument is created once) and fan out over
    // every live circuit's registered state when a collector polls.
    private static readonly ConcurrentDictionary<CircuitDiagnostics, byte> Registry = new();

    static CircuitDiagnostics()
    {
        Meter.CreateObservableGauge("incremental.circuit.input_queue_depth",
            ObserveInputDepths, "{record}", "Records buffered in each input's bounded channel.");
        Meter.CreateObservableGauge("incremental.circuit.output_queue_depth",
            ObserveOutputDepths, "{delta}", "Deltas buffered in each output's bounded channel.");
        Meter.CreateObservableGauge("incremental.circuit.watermark_lag",
            ObserveWatermarkLag, "s", "Wall-clock time minus the frozen event-time watermark.");
        Meter.CreateObservableGauge("incremental.cluster.send_queue_depth",
            ObserveSendQueueDepths, "{frame}", "Frames buffered in each peer's bounded send queue.");
    }

    private readonly KeyValuePair<string, object?> _tag;
    private readonly KeyValuePair<string, object?>[] _peerTags;
    private IInputPort[] _inputs = [];
    private IOutputPort[] _outputs = [];
    private Func<DateTimeOffset?>? _watermark;
    private ITransportDiagnostics? _transport;

    public CircuitDiagnostics(string circuitName, ILoggerFactory loggerFactory, int processCount)
    {
        CircuitName = circuitName;
        _tag = new KeyValuePair<string, object?>("circuit", circuitName);
        _peerTags = new KeyValuePair<string, object?>[processCount];
        for (var p = 0; p < processCount; p++)
            _peerTags[p] = new KeyValuePair<string, object?>("peer", p);
        CircuitLogger = loggerFactory.CreateLogger("Incremental.Circuit");
        ClusterLogger = loggerFactory.CreateLogger("Incremental.Cluster");
    }

    public string CircuitName { get; }
    public ILogger CircuitLogger { get; }
    public ILogger ClusterLogger { get; }

    /// <summary>Hooks up the state the observable gauges read; called once the circuit's
    /// ports exist. Gauge reads race steps harmlessly — a queue depth is a point-in-time
    /// approximation by nature.</summary>
    public void RegisterGauges(IInputPort[] inputs, IOutputPort[] outputs,
        Func<DateTimeOffset?> watermark, object? transport)
    {
        _inputs = inputs;
        _outputs = outputs;
        _watermark = watermark;
        _transport = transport as ITransportDiagnostics;
        Registry.TryAdd(this, 0);
    }

    public void StepCompleted(double seconds)
    {
        Steps.Add(1, _tag);
        StepDuration.Record(seconds, _tag);
    }

    public void RecordsDrained(long count)
    {
        if (count > 0)
            Records.Add(count, _tag);
    }

    public void CircuitFaulted() => Faults.Add(1, _tag);

    public void CheckpointCommitted(double seconds)
    {
        CheckpointCommits.Add(1, _tag);
        CheckpointCommitDuration.Record(seconds, _tag);
    }

    public void FrameSent(int peer, int payloadBytes)
    {
        ClusterFramesSent.Add(1, _tag, _peerTags[peer]);
        ClusterBytesSent.Add(payloadBytes, _tag, _peerTags[peer]);
    }

    public void FrameReceived(int peer, int payloadBytes)
    {
        ClusterFramesReceived.Add(1, _tag, _peerTags[peer]);
        ClusterBytesReceived.Add(payloadBytes, _tag, _peerTags[peer]);
    }

    public void Dispose() => Registry.TryRemove(this, out _);

    private static IEnumerable<Measurement<long>> ObserveInputDepths()
    {
        foreach (var circuit in Registry.Keys)
            foreach (var input in circuit._inputs)
                yield return new Measurement<long>(input.BufferedCount, circuit._tag,
                    new KeyValuePair<string, object?>("input", input.Name));
    }

    private static IEnumerable<Measurement<long>> ObserveOutputDepths()
    {
        foreach (var circuit in Registry.Keys)
            foreach (var output in circuit._outputs)
                yield return new Measurement<long>(output.BufferedCount, circuit._tag,
                    new KeyValuePair<string, object?>("output", output.Name));
    }

    private static IEnumerable<Measurement<double>> ObserveWatermarkLag()
    {
        foreach (var circuit in Registry.Keys)
            if (circuit._watermark?.Invoke() is { } watermark)
                yield return new Measurement<double>(
                    (DateTimeOffset.UtcNow - watermark).TotalSeconds, circuit._tag);
    }

    private static IEnumerable<Measurement<long>> ObserveSendQueueDepths()
    {
        foreach (var circuit in Registry.Keys)
            if (circuit._transport is { } transport)
                foreach (var (peer, depth) in transport.SendQueueDepths)
                    yield return new Measurement<long>(depth, circuit._tag, circuit._peerTags[peer]);
    }
}
