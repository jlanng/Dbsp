using Dbsp.Cluster;
using Dbsp.Storage;
using Microsoft.Extensions.Logging;

namespace Dbsp;

public sealed class CircuitOptions
{
    /// <summary>Names the circuit in its logs and as the <c>circuit</c> tag on every metric
    /// from the <c>Dbsp</c> meter, so circuits sharing a process stay
    /// distinguishable. Null (default) auto-assigns <c>circuit-1</c>, <c>circuit-2</c>, …
    /// in construction order.</summary>
    public string? Name { get; init; }

    /// <summary>Receives the circuit's logs, under the categories
    /// <c>Dbsp.Circuit</c> and <c>Dbsp.Cluster</c>: lifecycle at
    /// Information, checkpoint commits at Debug, faults and cluster timeouts at Error.
    /// Record contents are never logged. Null (default) — no logging. Metrics need no
    /// wiring: the <c>Dbsp</c> <see cref="System.Diagnostics.Metrics.Meter"/> is
    /// always on (steps, step duration, records, queue depths, watermark lag, checkpoint
    /// commits, sink acknowledgement wait, faults, cluster frames/bytes) and costs nothing unlistened.</summary>
    public ILoggerFactory? LoggerFactory { get; init; }

    /// <summary>Number of worker threads <em>on this process</em>. Keyed operators shard
    /// their state across workers via the exchange operators <c>GroupBy</c>/<c>Distinct</c>
    /// insert — across all processes' workers when <see cref="Cluster"/> is set.</summary>
    public int Workers { get; init; } = 1;

    /// <summary>Capacity of each input's bounded channel. When it is full, <c>InsertAsync</c>
    /// and the other sends suspend the producer — this is where backpressure originates.</summary>
    public int InputChannelCapacity { get; init; } = 8192;

    /// <summary>Capacity of each output's bounded channel. A slow output consumer eventually
    /// stalls the circuit, which stops draining inputs, which suspends producers.</summary>
    public int OutputChannelCapacity { get; init; } = 256;

    /// <summary>Maximum records drained from each input per step (the batch size of a step).</summary>
    public int MaxStepItemsPerInput { get; init; } = 8192;

    /// <summary>When true (default) a pump task runs steps whenever input is available.
    /// When false the circuit only advances on explicit <see cref="Circuit.StepAsync"/> calls,
    /// which gives deterministic step boundaries (useful for tests and batch workloads).</summary>
    public bool AutoStep { get; init; } = true;

    /// <summary>Where stateful operators (join, aggregate, distinct) keep their state.</summary>
    public IStorageProvider Storage { get; init; } = new InMemoryStorageProvider();

    /// <summary>Number of steps between checkpoint commits when <see cref="Storage"/> is an
    /// <see cref="ICheckpointStorageProvider"/>. Each commit atomically persists the state
    /// changes and input offsets of the steps since the previous one; a larger interval
    /// amortizes commit cost at the price of a longer replay window after a crash. Ignored —
    /// at zero cost — when the provider does not checkpoint.</summary>
    public int CheckpointInterval { get; init; } = 1;

    /// <summary>An upper bound on how long a completed step may stay uncommitted, beside
    /// <see cref="CheckpointInterval"/>: a commit is due at the end of the step that reaches
    /// the interval, or of the first step that ends after the oldest uncommitted one has
    /// waited this long — and an auto-stepping circuit that goes idle with steps uncommitted
    /// commits when the time runs out, without waiting for the next record. With manual
    /// stepping the bound is checked only as steps end. Null (default) — the step count
    /// alone. It is the better knob for a circuit with acknowledged outputs, where every
    /// commit waits for the sinks: a large interval with a delay of, say, a second keeps
    /// steps flowing between commits and still bounds what a crash replays, in time rather
    /// than in steps. Not available on a clustered circuit, whose processes must commit on
    /// the same steps.</summary>
    public TimeSpan? CheckpointMaxDelay { get; init; }

    /// <summary>What the circuit does with a record one of its operators cannot process — a
    /// user delegate (<c>Select</c>, <c>Where</c>, a key selector, an aggregate, a join's
    /// result selector) that throws on it, or a null key or result.
    /// <see cref="RecordErrorPolicy.Fault"/> (default) faults the circuit, as before.
    /// <see cref="RecordErrorPolicy.DeadLetter"/> drops the record, reports it on
    /// <see cref="Circuit.DeadLetters"/> — which must then be read — and carries on;
    /// <see cref="DeadLetter"/> says what that asks of the delegates. Override it for one
    /// operator with <c>OnError</c> on the stream it returns. A keyed input follows this
    /// setting only (it has no operator to override): under <c>DeadLetter</c> an upsert
    /// whose key selector throws or returns null is reported instead of throwing to the
    /// producer.</summary>
    public RecordErrorPolicy ErrorPolicy { get; init; } = RecordErrorPolicy.Fault;

    /// <summary>Capacity of the dead-letter output's bounded channel. When it is full the
    /// circuit waits for its reader, like any output: dead letters are backpressure, never
    /// unbounded memory.</summary>
    public int DeadLetterCapacity { get; init; } = 1024;

    /// <summary>Makes the dead-letter output an acknowledged one: dropped records are
    /// published a step at a time on <see cref="Circuit.DeadLetterBatches"/> (instead of
    /// one by one on <see cref="Circuit.DeadLetters"/>), and a checkpoint is committed only
    /// once the reader has acknowledged every batch published so far — commit after
    /// delivery, as for <c>ToAcknowledgedOutput</c>. Use it when dead letters are stored
    /// somewhere durable and must not be lost to a crash between the checkpoint and the
    /// store. Its channel holds <see cref="OutputChannelCapacity"/> batches. Default false.</summary>
    public bool AcknowledgeDeadLetters { get; init; }

    /// <summary>How far ahead of the node's clock, and of the newest event time the input
    /// has brought so far, a record's event time may be. A record beyond it is a record in
    /// error: <see cref="ErrorPolicy"/> decides, as for a record its source could not read
    /// — reported on the dead letters and passed over, or a fault. Without a bound
    /// (null, the default) one record stamped far in the future moves the watermark to
    /// its time, every record after it is late and dropped, and every open window and
    /// key closes: the pipeline is stalled until the feed's time catches up.
    /// <para>
    /// The bound is applied where a record enters, by the input the event time is
    /// declared on (<c>input.WithEventTime(…, lateness)</c>, on an unclustered circuit);
    /// a declaration further down — after a <c>Select</c>, say — has none. It is judged
    /// against the later of two clocks: the newest event time that input has accepted,
    /// so that a feed whose time runs ahead of the node's (a replay of recorded data
    /// with shifted times, a test) goes on as long as it moves in steps smaller than the
    /// bound; and the node's own, so that a feed that has been quiet for longer than
    /// the bound is taken up again. Because the node's clock is one of the two, a
    /// record refused and not yet committed can pass when it is read again later — after
    /// a restart, say; what a committed step did with it stays done. Set it well above
    /// the skew between the clocks that stamp the records and this node's.
    /// </para></summary>
    public TimeSpan? MaxEventTimeAhead { get; init; }

    /// <summary>The clock <see cref="MaxEventTimeAhead"/> is judged against. Tests
    /// replace it.</summary>
    public TimeProvider TimeProvider { get; init; } = TimeProvider.System;

    /// <summary>Runs this circuit as one process of a cluster: the same circuit definition
    /// runs on every process, each hosting a contiguous slice of the global worker set, and
    /// exchange operators shuffle across all of them. Null (default) — single process.</summary>
    public ClusterOptions? Cluster { get; init; }

    /// <summary>Upper bound on any single cross-process wait in a clustered step: receiving
    /// an exchange node's shards from a peer, the end-of-step alignment frames, or space in
    /// a peer's bounded send queue. Null (default) waits forever — a peer that is merely
    /// slow (a long GC pause, a big shard) then stalls the step, which is ordinary
    /// backpressure. Set it in production clusters so a peer that is <em>stuck</em> — a
    /// wedged user delegate, a half-dead host that still holds its connections — faults the
    /// circuit with a descriptive <see cref="TimeoutException"/> instead of hanging every
    /// process forever. A fired timeout is terminal, like any other fault: rebuild the
    /// circuit from the last committed checkpoint once the cluster is healthy. Size it well
    /// above the slowest healthy step (including output-consumer backpressure, which crosses
    /// the wire). Ignored when <see cref="Cluster"/> is null: local waits are in-process and
    /// diagnosable, and a stuck local delegate is a bug the timeout could only mislabel.</summary>
    public TimeSpan? ExchangeTimeout { get; init; }

    /// <summary>Whether exchange nodes are needed: more than one worker shard exists, either
    /// as local threads or across cluster processes.</summary>
    internal bool IsSharded => Workers > 1 || Cluster is not null;
}

/// <summary>
/// Declares this process's place in a clustered circuit. The same circuit definition must
/// be built on every process with the same <see cref="CircuitOptions"/> (same
/// <see cref="CircuitOptions.Workers"/>, operators, inputs, outputs — everything except the
/// transport's process index): process <c>p</c> then hosts global workers
/// <c>[p·Workers, (p+1)·Workers)</c>, and every exchange hash-partitions by key across the
/// <c>Workers × ProcessCount</c> global workers, shipping remote-bound shards over
/// <see cref="Transport"/>. Clustered circuits require <c>AutoStep = false</c>: every
/// process must call <see cref="Circuit.StepAsync"/> for every step, in lockstep — a step on
/// one process blocks at its first exchange (and at the end-of-step alignment) until every
/// process runs the same step, so empty steps still align and backpressure propagates
/// across the cluster.
/// </summary>
public sealed class ClusterOptions
{
    /// <summary>The transport connecting this process to its peers; it carries the static
    /// topology (this process's index and the process count). The circuit takes ownership
    /// and disposes it. <see cref="TcpExchangeTransport"/> for machines,
    /// <see cref="InMemoryExchangeTransport"/> for tests.</summary>
    public required IExchangeTransport Transport { get; init; }

    /// <summary>This process's index in the cluster (from the transport).</summary>
    public int ProcessIndex => Transport.ProcessIndex;

    /// <summary>Total number of processes in the cluster (from the transport).</summary>
    public int ProcessCount => Transport.ProcessCount;
}
