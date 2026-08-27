using StreamProcessor.Operators;

namespace StreamProcessor.Runtime;

/// <summary>
/// One worker of a running circuit: its own instances of every operator (and thus its own
/// shard of all operator state), plus the per-step output slot of every node.
/// </summary>
internal sealed class Worker(int id, IStepOperator[] operators, object?[] slots)
{
    public int Id { get; } = id;
    public IStepOperator[] Operators { get; } = operators;
    public object?[] Slots { get; } = slots;

    /// <summary>Released once by the coordinator for every step this worker should run.</summary>
    public SemaphoreSlim Go { get; } = new(0);

    public Thread? Thread { get; set; }
}
