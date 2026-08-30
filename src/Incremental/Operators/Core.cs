using Incremental.Storage;

namespace Incremental.Operators;

/// <summary>Per-worker, per-node context handed to operator factories.</summary>
public sealed class OperatorContext
{
    public required int WorkerId { get; init; }
    public required int WorkerCount { get; init; }
    public required IStorageProvider Storage { get; init; }
    public required string NodeName { get; init; }

    /// <summary>The worker's id in the global worker set: equal to <see cref="WorkerId"/> on
    /// a single process, offset by the process's slice in a clustered circuit — which keeps
    /// state store names distinct when processes share one physical store.</summary>
    public int GlobalWorkerId { get; init; }

    /// <summary>Collects every store name the circuit's operators mint (all creation goes
    /// through <see cref="StateName"/>, in operator constructors), so the circuit can commit
    /// a state manifest with each checkpoint and refuse a mismatched restore.</summary>
    internal Action<string>? ManifestObserver { get; init; }

    /// <summary>A store name unique to this node, worker, and purpose (cluster-wide).</summary>
    public string StateName(string suffix)
    {
        var name = $"{NodeName}.{suffix}.w{GlobalWorkerId}";
        ManifestObserver?.Invoke(name);
        return name;
    }
}

/// <summary>
/// A circuit operator. Once per step, <see cref="Step"/> receives the Z-set deltas produced
/// by its input nodes this step (boxed <see cref="ZSet{T}"/> instances) and returns the
/// delta it produces. Operators are instantiated once per worker; stateful operators keep
/// worker-local state keyed by the exchange partitioning upstream of them.
/// </summary>
internal interface IStepOperator
{
    object Step(object?[] inputs);

    /// <summary>
    /// The delta to publish when every input delta this step is empty, letting the scheduler
    /// skip <see cref="Step"/> altogether — for a keyed operator downstream of an exchange
    /// that is the common case, since a small delta touches one worker's shard and leaves the
    /// others with nothing to do.
    /// <para>
    /// Null (the default) means "run me anyway", which is the safe answer and the required one
    /// for any operator that does work a delta cannot account for: reading the staged input
    /// batch, reacting to a watermark, evicting closed keys, or carrying loop state between
    /// iterations. Only return a value when <c>Step</c> over empty inputs provably changes no
    /// state and produces nothing.
    /// </para>
    /// <para>
    /// The instance is reused across steps and shared with every downstream operator that
    /// receives it, so it must stay empty. That is safe because operators treat their inputs
    /// as read-only — the same property that already lets pass-through operators return their
    /// input unchanged.
    /// </para>
    /// </summary>
    object? EmptyStepResult => null;
}

/// <summary>
/// An exchange (shuffle) operator: repartitions its input delta across workers by hash so
/// that all records with the same routing key land on the same worker. The runtime calls
/// <see cref="Partition"/>, synchronizes all workers on a barrier, then calls
/// <see cref="Merge"/> with the fragments addressed to this worker. With a single worker
/// the runtime skips the shuffle and calls <see cref="IStepOperator.Step"/> (identity).
/// </summary>
internal interface IExchangeStepOperator : IStepOperator
{
    object[] Partition(object? input, int parts);
    object Merge(object?[] parts);

    /// <summary>Cluster path: merges every local sender's fragments for the
    /// <paramref name="count"/> partitions starting at <paramref name="firstShard"/> (a peer
    /// process's contiguous global-worker range) and serializes them for the wire, one shard
    /// per local worker of the receiving process.</summary>
    byte[] SerializeShards(object[][] senderFragments, int firstShard, int count);

    /// <summary>Inverse of <see cref="SerializeShards"/>: the received shards as Z-sets,
    /// indexed by the receiving process's local worker id.</summary>
    object[] DeserializeShards(byte[] payload);
}

/// <summary>Entry point of a circuit: emits whatever batch the coordinator staged for this worker.</summary>
internal sealed class InputOperator<T> : IStepOperator where T : notnull
{
    private ZSet<T> _pending = new();

    /// <summary>Called by the coordinator (between steps) to stage a batch for the next step.</summary>
    public void Enqueue(ZSet<T> batch)
    {
        if (_pending.IsEmpty)
            _pending = batch;
        else
            _pending.AddAll(batch);
    }

    public object Step(object?[] inputs)
    {
        var batch = _pending;
        // Only mint a replacement once this one has been filled: an idle input would
        // otherwise allocate a fresh empty Z-set every step, on every worker. Handing the
        // same empty batch out repeatedly is safe — downstream reads it and PublishAsync
        // copies before anything reaches a consumer — and Enqueue only writes to it between
        // steps, once the step that saw it has finished.
        if (!batch.IsEmpty)
            _pending = new ZSet<T>();
        return batch;
    }
}

internal sealed class MapOperator<TIn, TOut>(Func<TIn, TOut> selector) : IStepOperator
    where TIn : notnull
    where TOut : notnull
{
    private readonly ZSet<TOut> _empty = new();

    public object? EmptyStepResult => _empty;

    public object Step(object?[] inputs)
    {
        var delta = (ZSet<TIn>)inputs[0]!;
        var output = new ZSet<TOut>(delta.DistinctCount);
        foreach (var (value, weight) in delta)
            output.Add(selector(value), weight);
        return output;
    }
}

internal sealed class FilterOperator<T>(Func<T, bool> predicate) : IStepOperator where T : notnull
{
    private readonly ZSet<T> _empty = new();

    public object? EmptyStepResult => _empty;

    public object Step(object?[] inputs)
    {
        var delta = (ZSet<T>)inputs[0]!;
        var output = new ZSet<T>(delta.DistinctCount);
        foreach (var (value, weight) in delta)
            if (predicate(value))
                output.Add(value, weight);
        return output;
    }
}

internal sealed class FlatMapOperator<TIn, TOut>(Func<TIn, IEnumerable<TOut>> selector) : IStepOperator
    where TIn : notnull
    where TOut : notnull
{
    private readonly ZSet<TOut> _empty = new();

    public object? EmptyStepResult => _empty;

    public object Step(object?[] inputs)
    {
        var delta = (ZSet<TIn>)inputs[0]!;
        var output = new ZSet<TOut>(delta.DistinctCount);
        foreach (var (value, weight) in delta)
            foreach (var expanded in selector(value))
                output.Add(expanded, weight);
        return output;
    }
}

internal sealed class NegateOperator<T> : IStepOperator where T : notnull
{
    private readonly ZSet<T> _empty = new();

    public object? EmptyStepResult => _empty;

    public object Step(object?[] inputs) => ((ZSet<T>)inputs[0]!).Negated();
}

/// <summary>Z-set addition (bag union) of any number of input streams.</summary>
internal sealed class PlusOperator<T> : IStepOperator where T : notnull
{
    private readonly ZSet<T> _empty = new();

    public object? EmptyStepResult => _empty;

    public object Step(object?[] inputs)
    {
        var total = 0;
        foreach (var input in inputs)
            total += ((ZSet<T>)input!).DistinctCount;
        var output = new ZSet<T>(total);
        foreach (var input in inputs)
            output.AddAll((ZSet<T>)input!);
        return output;
    }
}

/// <summary>The routing hash must be process- and machine-stable (see <see cref="StableHash"/>):
/// cluster peers partition independently and must agree, and a restart against checkpointed
/// state must route every key to the worker whose shard holds it.</summary>
internal sealed class ExchangeOperator<T>(Func<T, uint> routingHash) : IExchangeStepOperator where T : notnull
{
    // Both the fragment array and the Z-sets in it are reused across steps rather than
    // reallocated. The array is fixed-size (the global worker count cannot change mid-run),
    // and the previous step's fragments are dead by the time this worker repartitions: every
    // worker runs every node in the same order, so between one step's Merge (which reads
    // peers' arrays, after the exchange barrier) and the next Partition on this node there is
    // always at least that barrier plus the step boundary — and, for an exchange inside a
    // Recurse region, the loop's own flag barriers. Reusing the Z-sets matters more than
    // reusing the array: a shuffle refills the same shards every step, so after a few steps
    // they are the right size and stop re-walking the dictionary's doubling ladder. Merge
    // therefore copies rather than adopting a fragment — the fragment is about to be cleared.
    private ZSet<T>[]? _fragments;

    // Single-worker fast path: no shuffle, pass the delta through untouched.
    public object Step(object?[] inputs) => inputs[0]!;

    public object[] Partition(object? input, int parts)
    {
        var fragments = _fragments;
        if (fragments is null || fragments.Length != parts)
        {
            fragments = _fragments = new ZSet<T>[parts];
            for (var i = 0; i < parts; i++)
                fragments[i] = new ZSet<T>();
        }
        else
        {
            // Clearing an already-empty dictionary is free, so the sparse case — most shards
            // empty, most executions routing nothing — costs a handful of length checks.
            foreach (var fragment in fragments)
                fragment.Clear();
        }

        var delta = (ZSet<T>)input!;
        if (delta.IsEmpty)
            return fragments;

        foreach (var (value, weight) in delta)
            fragments[(int)(routingHash(value) % (uint)parts)].Add(value, weight);
        return fragments;
    }

    /// <summary>Merged output for a worker that was sent nothing. Kept apart from the reused
    /// fragments: this one does travel downstream as the node's delta and must stay empty
    /// forever, whereas the fragments are cleared and refilled every step.</summary>
    private readonly ZSet<T> _emptyMerge = new();

    /// <summary>An exchange over all-empty inputs shuffles nothing and changes no state, so
    /// the scheduler may skip it wholesale — single-worker via the ordinary empty-input skip,
    /// point-to-point via the empty bit each producer publishes with its sequence — as long
    /// as it still publishes/consumes sequences where peers expect them.</summary>
    public object? EmptyStepResult => _emptyMerge;

    public object Merge(object?[] parts)
    {
        // An exchange cannot be skipped the way a plain operator can — its barrier keeps the
        // workers aligned — but the worker no key routed to this step still need not allocate
        // a Z-set to say so. Sizing the merge up front is the point of the counting pass:
        // the shards are disjoint by key, so their entry counts sum to the merged size
        // exactly, and the result is allocated once at its final size.
        var total = 0;
        foreach (var part in parts)
            if (part is ZSet<T> fragment)
                total += fragment.DistinctCount;
        if (total == 0)
            return _emptyMerge;

        var merged = new ZSet<T>(total);
        foreach (var part in parts)
            if (part is ZSet<T> { IsEmpty: false } fragment)
                merged.AddAll(fragment);
        return merged;
    }

    // Wire encoding reuses the storage serializer (JSON with fields included, so tuple keys
    // round-trip): a delta batch is just (value, weight) pairs, one array per target worker.

    public byte[] SerializeShards(object[][] senderFragments, int firstShard, int count)
    {
        var shards = new WeightedValue<T>[count][];
        for (var k = 0; k < count; k++)
        {
            var merged = new ZSet<T>();
            foreach (var fragments in senderFragments)
                if (fragments[firstShard + k] is ZSet<T> { IsEmpty: false } fragment)
                    merged.AddAll(fragment);
            shards[k] = [.. merged];
        }
        return System.Text.Encoding.UTF8.GetBytes(JsonStateSerializer.Instance.Serialize(shards));
    }

    public object[] DeserializeShards(byte[] payload)
    {
        var shards = JsonStateSerializer.Instance.Deserialize<WeightedValue<T>[][]>(
            System.Text.Encoding.UTF8.GetString(payload));
        var result = new object[shards.Length];
        for (var k = 0; k < shards.Length; k++)
            result[k] = new ZSet<T>(shards[k]);
        return result;
    }
}
