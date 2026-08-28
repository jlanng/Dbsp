using Incremental.Cluster;
using Incremental.Storage;

namespace Incremental;

public sealed class CircuitOptions
{
    /// <summary>Number of worker threads <em>on this process</em>. Keyed operators shard
    /// their state across workers via exchange operators inserted by
    /// <c>IndexBy</c>/<c>Distinct</c> — across all processes' workers when
    /// <see cref="Cluster"/> is set.</summary>
    public int Workers { get; init; } = 1;

    /// <summary>Capacity of each input's bounded channel. When full, <c>InsertAsync</c> etc.
    /// suspend the producer — this is where backpressure originates.</summary>
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

    /// <summary>Runs this circuit as one process of a cluster: the same circuit definition
    /// runs on every process, each hosting a contiguous slice of the global worker set, and
    /// exchange operators shuffle across all of them. Null (default) — single process.</summary>
    public ClusterOptions? Cluster { get; init; }

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
