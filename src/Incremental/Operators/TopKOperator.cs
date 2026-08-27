using Incremental.Storage;

namespace Incremental.Operators;

/// <summary>
/// Incremental per-key TOP-K: keeps the integrated group per key and the set of rows it
/// last emitted, and on a touched key re-ranks the group and emits only the membership
/// difference — displaced rows are retracted, newly ranked rows asserted. Multiplicities
/// count toward K (a value with weight 3 occupies three ranks). Ties are broken
/// arbitrarily.
///
/// Threshold short-circuit: when the emitted top is full and every one of a key's delta
/// rows ranks strictly worse than the current K-th entry, the top provably cannot change —
/// inserts below the cutoff can't enter it, and retractions below the cutoff can't be part
/// of it — so the group scan and sort are skipped entirely. This keeps steady-state cost
/// O(delta) even for huge groups; full re-ranks happen only when the top actually moves.
/// TODO: deterministic tie-break option, and an ordered store so even those re-ranks don't
/// scan the whole group.
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

        var entries = new List<(TKey, TValue, long)>(delta.DistinctCount);
        var touchedKeys = new List<TKey>();
        var touched = new Dictionary<TKey, List<WeightedValue<TValue>>>();
        foreach (var ((key, value), weight) in delta)
        {
            entries.Add((key, value, weight));
            if (!touched.TryGetValue(key, out var rows))
            {
                touched[key] = rows = [];
                touchedKeys.Add(key);
            }
            rows.Add(new WeightedValue<TValue>(value, weight));
            _eviction?.Track(key);
        }
        _groups.UpdateAll(entries);

        var comparer = Comparer<TOrder>.Default;
        var lastResults = _lastEmitted.TryGetAll(touchedKeys);

        var rerankKeys = new List<TKey>();
        var rerankPrevious = new List<WeightedValue<TValue>[]?>();
        for (var i = 0; i < touchedKeys.Count; i++)
        {
            var (hadPrevious, emitted) = lastResults[i];
            if (hadPrevious && TopUnaffected(emitted!, touched[touchedKeys[i]], comparer))
                continue;
            rerankKeys.Add(touchedKeys[i]);
            rerankPrevious.Add(hadPrevious ? emitted : null);
        }

        var groups = _groups.GetGroups(rerankKeys);
        var puts = new List<(TKey, WeightedValue<TValue>[])>();
        var removes = new List<TKey>();

        for (var i = 0; i < rerankKeys.Count; i++)
        {
            var key = rerankKeys[i];
            var previous = rerankPrevious[i];

            var ranked = new List<(TOrder Order, WeightedValue<TValue> Entry)>();
            foreach (var entry in groups[i])
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

            foreach (var (value, weight) in top)
                output.Add(new KeyedValue<TKey, TValue>(key, value), weight);
            if (previous is not null)
                foreach (var (value, weight) in previous)
                    output.Add(new KeyedValue<TKey, TValue>(key, value), -weight);

            if (top.Count == 0)
            {
                if (previous is not null)
                    removes.Add(key);
            }
            else if (previous is null || !top.SequenceEqual(previous))
            {
                puts.Add((key, [.. top]));
            }
        }

        _lastEmitted.PutAll(puts);
        _lastEmitted.RemoveAll(removes);

        if (_eviction?.DrainClosed() is { Count: > 0 } closed)
        {
            _groups.RemoveGroups(closed);
            _lastEmitted.RemoveAll(closed);
        }

        return output;
    }

    /// <summary>True when this step's delta rows provably cannot change the emitted top:
    /// it is full (K rows counting multiplicity) and every delta row ranks strictly worse
    /// than the K-th entry. Ties with the cutoff conservatively force a re-rank.</summary>
    private bool TopUnaffected(
        WeightedValue<TValue>[] emitted,
        List<WeightedValue<TValue>> deltaRows,
        Comparer<TOrder> comparer)
    {
        long emittedWeight = 0;
        foreach (var (_, weight) in emitted)
            emittedWeight += weight;
        if (emittedWeight < _count)
            return false; // room left in the top: any insert may enter it

        // Emitted rows are stored in rank order; the last one is the K-th (worst) entry.
        var cutoff = _orderBy(emitted[^1].Value);
        foreach (var (value, _) in deltaRows)
            if (_direction * comparer.Compare(_orderBy(value), cutoff) <= 0)
                return false; // ranks at or above the cutoff: could enter or leave the top
        return true;
    }
}
