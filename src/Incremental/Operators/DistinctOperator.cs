using Incremental.Storage;

namespace Incremental.Operators;

/// <summary>
/// Incremental DISTINCT: maintains the integrated weight of every value and emits a delta
/// only when a value crosses between "present" (integrated weight &gt; 0) and "absent".
/// Requires all occurrences of a value to be routed to the same worker; the builder inserts
/// an exchange on the value hash upstream when the circuit runs with multiple workers.
/// </summary>
internal sealed class DistinctOperator<T> : IStepOperator where T : notnull
{
    private readonly IWeightStore<T> _integrated;

    public DistinctOperator(OperatorContext context) =>
        _integrated = context.Storage.CreateWeightStore<T>(context.StateName("integrated"));

    public object Step(object?[] inputs)
    {
        var delta = (ZSet<T>)inputs[0]!;
        var output = new ZSet<T>();
        foreach (var (value, weight) in delta)
        {
            var before = _integrated.WeightOf(value);
            var after = _integrated.Update(value, weight);
            var change = (after > 0 ? 1 : 0) - (before > 0 ? 1 : 0);
            if (change != 0)
                output.Add(value, change);
        }
        return output;
    }
}

/// <summary>
/// DISTINCT over an already key-partitioned stream. Semantically identical to
/// <see cref="DistinctOperator{T}"/> over the (key, value) pairs, but the integrated weights
/// are stored grouped by key, so a watermark-driven eviction policy can drop a closed key's
/// state wholesale. Needs no extra exchange: the keyed stream's partitioning already routes
/// equal pairs to the same worker.
/// </summary>
internal sealed class KeyedDistinctOperator<TKey, TValue> : IStepOperator
    where TKey : notnull
    where TValue : notnull
{
    private readonly IIndexedWeightStore<TKey, TValue> _integrated;
    private readonly EvictionTracker<TKey>? _eviction;

    public KeyedDistinctOperator(OperatorContext context, KeyEviction<TKey>? eviction)
    {
        _integrated = context.Storage.CreateIndexedWeightStore<TKey, TValue>(context.StateName("integrated"));
        _eviction = eviction is null ? null : new EvictionTracker<TKey>(eviction);
    }

    public object Step(object?[] inputs)
    {
        var delta = (ZSet<KeyedValue<TKey, TValue>>)inputs[0]!;
        var output = new ZSet<KeyedValue<TKey, TValue>>();
        foreach (var ((key, value), weight) in delta)
        {
            var after = _integrated.Update(key, value, weight);
            var before = after - weight;
            var change = (after > 0 ? 1 : 0) - (before > 0 ? 1 : 0);
            if (change != 0)
                output.Add(new KeyedValue<TKey, TValue>(key, value), change);
            _eviction?.Track(key);
        }

        // Closed keys can never be touched again; their already-emitted presence stays final.
        _eviction?.EvictClosed(_integrated.RemoveGroup);

        return output;
    }
}
