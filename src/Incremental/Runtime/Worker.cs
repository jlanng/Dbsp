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

    /// <summary>Cumulative <see cref="System.Diagnostics.Stopwatch"/> ticks this worker has
    /// spent blocked in <see cref="SpinBarrier.SignalAndWait"/> (exchange shuffles and loop
    /// fixed-point reductions). Written only by the owning worker thread, mid-step, with no
    /// synchronization — the diagnostics gauge reads it racily, which is fine for a
    /// monotonically growing time counter.</summary>
    public long BarrierWaitTicks;

    /// <summary>Point-to-point exchange mode only: how many times this worker has executed
    /// each exchange node, indexed by node — the sequence number it publishes fragments
    /// under. Every worker executes every node the same number of times (loop iteration
    /// counts are a global decision), so a worker's own count is also the sequence it
    /// expects from every peer. Empty when the circuit synchronizes on the barrier.</summary>
    public long[] ExchangeSeq { get; set; } = [];

    // Fragment streaming only (all empty otherwise; see docs/design/fragment-streaming.md).
    // All are written only by the owning worker thread — the cross-thread words are the
    // circuit's fragment-counter and sequence arrays, not these.

    /// <summary>Cumulative fragments this worker has consumed per (node, sender), indexed
    /// <c>node * workers + sender</c>. Monotonic across executions, so no per-step reset and
    /// no ambiguity about which execution a published counter belongs to.</summary>
    public long[] FragConsumed { get; set; } = [];

    /// <summary>Cumulative fragments this worker has published per exchange node — the value
    /// it last release-wrote to its fragment-counter slot, kept here so publishing does not
    /// re-read the shared line.</summary>
    public long[] FragPublished { get; set; } = [];

    /// <summary>Per fragment-sink node: fragments fed to the sink since its last completion,
    /// so the sink's sweep position knows it must run <c>Step</c> even though the streamed
    /// input slots are empty. Read and reset by the sink's node execution.</summary>
    public long[] SinkPending { get; set; } = [];

    /// <summary>Consume-loop scratch: which senders have completed the streamed exchange
    /// being consumed. One array serves every node — the loop finishes a node before the
    /// sweep reaches the next.</summary>
    public bool[] SenderDone { get; set; } = [];

    /// <summary>Per streamed exchange node, the reused chunk Z-sets the producer splits its
    /// input into (created lazily from the input's own type; only deltas above the chunk
    /// target ever populate more than the sweep already had).</summary>
    public IZSet?[]?[] ChunkScratch { get; set; } = [];

    /// <summary>Fragments this worker published/consumed, cumulative (phase-stats reporting;
    /// the counters are cheap enough to keep unconditionally).</summary>
    public long FragmentsPublished, FragmentsConsumed;

    // PHASE-STATS (temporary probe instrumentation — revert before committing). Cumulative
    // ticks per phase of the exchange path, plus per-step span timestamps for launch/land
    // attribution. Written only by the owning worker thread; read racily at dispose.
    public long PartitionTicks, MergeTicks, SpanTicks, SlowWaits, ExchangeExecutions, EmptyExchangeSkips;
    public long StepStartTs, StepEndTs;

    /// <summary>Released once by the coordinator for every step this worker should run.</summary>
    public SemaphoreSlim Go { get; } = new(0);

    public Thread? Thread { get; set; }
}
