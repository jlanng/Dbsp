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

    /// <summary>A store name unique to this node, worker, and purpose (cluster-wide).</summary>
    public string StateName(string suffix) => $"{NodeName}.{suffix}.w{GlobalWorkerId}";
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
        _pending = new ZSet<T>();
        return batch;
    }
}

internal sealed class MapOperator<TIn, TOut>(Func<TIn, TOut> selector) : IStepOperator
    where TIn : notnull
    where TOut : notnull
{
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
    public object Step(object?[] inputs)
    {
        var delta = (ZSet<T>)inputs[0]!;
        var output = new ZSet<T>();
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
    public object Step(object?[] inputs)
    {
        var delta = (ZSet<TIn>)inputs[0]!;
        var output = new ZSet<TOut>();
        foreach (var (value, weight) in delta)
            foreach (var expanded in selector(value))
                output.Add(expanded, weight);
        return output;
    }
}

internal sealed class NegateOperator<T> : IStepOperator where T : notnull
{
    public object Step(object?[] inputs) => ((ZSet<T>)inputs[0]!).Negated();
}

/// <summary>Z-set addition (bag union) of any number of input streams.</summary>
internal sealed class PlusOperator<T> : IStepOperator where T : notnull
{
    public object Step(object?[] inputs)
    {
        var output = new ZSet<T>();
        foreach (var input in inputs)
            output.AddAll((ZSet<T>)input!);
        return output;
    }
}

internal sealed class ExchangeOperator<T>(Func<T, int> routingHash) : IExchangeStepOperator where T : notnull
{
    // Single-worker fast path: no shuffle, pass the delta through untouched.
    public object Step(object?[] inputs) => inputs[0]!;

    public object[] Partition(object? input, int parts)
    {
        var delta = (ZSet<T>)input!;
        var fragments = new ZSet<T>[parts];
        for (var i = 0; i < parts; i++)
            fragments[i] = new ZSet<T>();

        foreach (var (value, weight) in delta)
        {
            var target = (int)((uint)routingHash(value) % (uint)parts);
            fragments[target].Add(value, weight);
        }
        return fragments;
    }

    public object Merge(object?[] parts)
    {
        var merged = new ZSet<T>();
        foreach (var part in parts)
            merged.AddAll((ZSet<T>)part!);
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
                merged.AddAll((ZSet<T>)fragments[firstShard + k]);
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
