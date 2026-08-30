using Incremental.Operators;

namespace Incremental.Runtime;

/// <summary>
/// One worker of a running circuit: its own instances of every operator (and thus its own
/// shard of all operator state), plus the per-step output slot of every node.
/// </summary>
internal sealed class Worker(
    int id,
    IStepOperator[] operators,
    object?[] slots,
    object?[][] nodeInputs,
    object?[][] exchangeIncoming)
{
    public int Id { get; } = id;
    public IStepOperator[] Operators { get; } = operators;
    public object?[] Slots { get; } = slots;

    /// <summary>Per-node argument array handed to <see cref="IStepOperator.Step"/>, allocated
    /// once instead of per step: its length is the node's input count, and an operator reads
    /// its arguments within the call and never retains the array. Indexed by node.</summary>
    public object?[][] NodeInputs { get; } = nodeInputs;

    /// <summary>Per-exchange-node buffer collecting the fragments addressed to this worker
    /// (local senders, then one shard per remote peer), reused across steps for the same
    /// reason: the exchange merges it within the step and keeps no reference. Empty for
    /// nodes that are not exchanges.</summary>
    public object?[][] ExchangeIncoming { get; } = exchangeIncoming;

    /// <summary>Released once by the coordinator for every step this worker should run.</summary>
    public SemaphoreSlim Go { get; } = new(0);

    public Thread? Thread { get; set; }
}
