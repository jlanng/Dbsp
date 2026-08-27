using StreamProcessor.Storage;

namespace StreamProcessor.Operators;

/// <summary>
/// Incremental per-key TOP-K: keeps the integrated group per key and the set of rows it
/// last emitted, and on every touched key re-ranks the group and emits only the membership
/// difference — displaced rows are retracted, newly ranked rows asserted. Multiplicities
/// count toward K (a value with weight 3 occupies three ranks). Ties are broken
/// arbitrarily. TODO: deterministic tie-break option, and an ordered store so re-ranking
/// doesn't scan the whole group.
/// </summary>
internal sealed class TopKOperator<TKey, TValue, TOrder> : IStepOperator
    where TKey : notnull
    where TValue : notnull
{
    private readonly IIndexedWeightStore<TKey, TValue> _groups;
    private readonly IValueStore<TKey, WeightedValue<TValue>[]> _lastEmitted;
    private readonly Func<TValue, TOrder> _orderBy;
    private readonly int _count;
    private readonly int _direction; // -1: largest first (descending), +1: smallest first
    private readonly EvictionTracker<TKey>? _eviction;

    public TopKOperator(OperatorContext context, int count, Func<TValue, TOrder> orderBy, bool descending, KeyEviction<TKey>? eviction)
    {
        _groups = context.Storage.CreateIndexedWeightStore<TKey, TValue>(context.StateName("groups"));
        _lastEmitted = context.Storage.CreateValueStore<TKey, WeightedValue<TValue>[]>(context.StateName("emitted"));
        _orderBy = orderBy;
        _count = count;
        _direction = descending ? -1 : 1;
        _eviction = eviction is null ? null : new EvictionTracker<TKey>(eviction);
    }

    public object Step(object?[] inputs)
    {
        var delta = (ZSet<KeyedValue<TKey, TValue>>)inputs[0]!;
        var output = new ZSet<KeyedValue<TKey, TValue>>();

        var touched = new HashSet<TKey>();
        foreach (var ((key, value), weight) in delta)
        {
            _groups.Update(key, value, weight);
            touched.Add(key);
            _eviction?.Track(key);
        }

        var comparer = Comparer<TOrder>.Default;
        foreach (var key in touched)
        {
            var ranked = new List<(TOrder Order, WeightedValue<TValue> Entry)>();
            foreach (var entry in _groups.GetGroup(key))
                if (entry.Weight > 0)
                    ranked.Add((_orderBy(entry.Value), entry));
            ranked.Sort((a, b) => _direction * comparer.Compare(a.Order, b.Order));

            var top = new List<WeightedValue<TValue>>();
            long remaining = _count;
            foreach (var (_, entry) in ranked)
            {
                if (remaining == 0)
                    break;
                var take = Math.Min(entry.Weight, remaining);
                top.Add(new WeightedValue<TValue>(entry.Value, take));
                remaining -= take;
            }

            var hadPrevious = _lastEmitted.TryGet(key, out var previous);
            foreach (var (value, weight) in top)
                output.Add(new KeyedValue<TKey, TValue>(key, value), weight);
            if (hadPrevious)
                foreach (var (value, weight) in previous!)
                    output.Add(new KeyedValue<TKey, TValue>(key, value), -weight);

            if (top.Count == 0)
            {
                if (hadPrevious)
                    _lastEmitted.Remove(key);
            }
            else if (!hadPrevious || !top.SequenceEqual(previous!))
            {
                _lastEmitted.Put(key, [.. top]);
            }
        }

        _eviction?.EvictClosed(key =>
        {
            _groups.RemoveGroup(key);
            _lastEmitted.Remove(key);
        });

        return output;
    }
}
