using Incremental.Storage;

namespace Incremental.Operators;

/// <summary>
/// Incremental equi-join. A join is bilinear, so its incremental form is
/// Δ(L ⋈ R) = ΔL ⋈ R + L ⋈ ΔR + ΔL ⋈ ΔR: each side keeps its integrated state in an
/// indexed store, and each step only probes the state with the new deltas — no
/// recomputation over historical data. Both inputs must be co-partitioned by key, which
/// <c>IndexBy</c> guarantees via its exchange.
/// </summary>
internal sealed class JoinOperator<TKey, TLeft, TRight, TOut> : IStepOperator
    where TKey : notnull
    where TLeft : notnull
    where TRight : notnull
    where TOut : notnull
{
    private readonly IIndexedWeightStore<TKey, TLeft> _left;
    private readonly IIndexedWeightStore<TKey, TRight> _right;
    private readonly Func<TKey, TLeft, TRight, TOut> _resultSelector;
    private readonly EvictionTracker<TKey>? _eviction;

    public JoinOperator(OperatorContext context, Func<TKey, TLeft, TRight, TOut> resultSelector, KeyEviction<TKey>? eviction)
    {
        _left = context.Storage.CreateIndexedWeightStore<TKey, TLeft>(context.StateName("left"));
        _right = context.Storage.CreateIndexedWeightStore<TKey, TRight>(context.StateName("right"));
        _resultSelector = resultSelector;
        _eviction = eviction is null ? null : new EvictionTracker<TKey>(eviction);
    }

    private readonly ZSet<TOut> _empty = new();

    // Every state change below is driven by a delta row, so an all-empty step is a
    // no-op — unless a KeyEviction is attached, whose DrainClosed fires off the
    // watermark instead and must run whether or not data arrived.
    public object? EmptyStepResult => _eviction is null ? _empty : null;

    public object Step(object?[] inputs)
    {
        var deltaLeft = (ZSet<KeyedValue<TKey, TLeft>>)inputs[0]!;
        var deltaRight = (ZSet<KeyedValue<TKey, TRight>>)inputs[1]!;
        var output = new ZSet<TOut>();

        // ΔL ⋈ R (right state not yet updated with ΔR)
        var rightGroups = FetchGroups(_right, deltaLeft, out var leftEntries);
        foreach (var ((key, left), leftWeight) in deltaLeft)
        {
            foreach (var (right, rightWeight) in rightGroups[key])
                output.Add(_resultSelector(key, left, right), checked(leftWeight * rightWeight));
            _eviction?.Track(key);
        }

        // Integrate ΔL, so the pass below also covers the ΔL ⋈ ΔR cross term.
        _left.UpdateAll(leftEntries);

        // (L + ΔL) ⋈ ΔR
        var leftGroups = FetchGroups(_left, deltaRight, out var rightEntries);
        foreach (var ((key, right), rightWeight) in deltaRight)
        {
            foreach (var (left, leftWeight) in leftGroups[key])
                output.Add(_resultSelector(key, left, right), checked(leftWeight * rightWeight));
            _eviction?.Track(key);
        }

        _right.UpdateAll(rightEntries);

        // Closed keys can never be probed again (the watermark drops their events), so
        // both sides' state for them is deleted; results already emitted remain final.
        if (_eviction?.DrainClosed() is { Count: > 0 } closed)
        {
            _left.RemoveGroups(closed);
            _right.RemoveGroups(closed);
        }

        return output;
    }

    /// <summary>One batched read of the groups a delta will probe, plus that delta flattened
    /// into the entry list its own side integrates.</summary>
    private static Dictionary<TKey, IReadOnlyList<WeightedValue<TValue>>> FetchGroups<TValue, TOther>(
        IIndexedWeightStore<TKey, TValue> store,
        ZSet<KeyedValue<TKey, TOther>> delta,
        out List<(TKey Key, TOther Value, long Delta)> entries)
        where TValue : notnull
        where TOther : notnull
    {
        entries = new List<(TKey, TOther, long)>(delta.DistinctCount);
        var keys = new List<TKey>();
        var groups = new Dictionary<TKey, IReadOnlyList<WeightedValue<TValue>>>();
        foreach (var ((key, value), weight) in delta)
        {
            entries.Add((key, value, weight));
            if (groups.TryAdd(key, []))
                keys.Add(key);
        }

        var fetched = store.GetGroups(keys);
        for (var i = 0; i < keys.Count; i++)
            groups[keys[i]] = fetched[i];
        return groups;
    }
}
