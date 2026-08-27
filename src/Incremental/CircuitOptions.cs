using Incremental.Storage;

namespace Incremental;

public sealed class CircuitOptions
{
    /// <summary>Number of worker threads. Keyed operators shard their state across workers
    /// via exchange operators inserted by <c>IndexBy</c>/<c>Distinct</c>.</summary>
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
}
