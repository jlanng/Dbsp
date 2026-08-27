using Incremental.Storage;

namespace Incremental.Operators;

/// <summary>
/// Incremental grouped aggregation. Maintains the integrated group contents per key plus the
/// last emitted result per key; when a step touches a key, the aggregate is recomputed over
/// that group and the output delta retracts the stale result (-1) and asserts the new one (+1).
/// Recomputing per touched group keeps every aggregate — including non-invertible ones like
/// MIN/MAX — correct under deletions. This is the general path: linear aggregates
/// (COUNT/SUM) have <see cref="LinearAggregateOperator{TKey,TValue,TResult}"/> and MIN/MAX
/// have <see cref="ExtremumOperator{TKey,TValue,TProjected}"/>, both of which avoid storing
/// or rescanning group contents.
/// </summary>
internal sealed class AggregateOperator<TKey, TValue, TResult> : IStepOperator
    where TKey : notnull
    where TValue : notnull
    where TResult : notnull
{
    private readonly IIndexedWeightStore<TKey, TValue> _groups;
    private readonly IValueStore<TKey, TResult> _lastEmitted;
    private readonly IAggregateFunction<TValue, TResult> _aggregate;
    private readonly EvictionTracker<TKey>? _eviction;

    public AggregateOperator(OperatorContext context, IAggregateFunction<TValue, TResult> aggregate, KeyEviction<TKey>? eviction)
    {
        _groups = context.Storage.CreateIndexedWeightStore<TKey, TValue>(context.StateName("groups"));
        _lastEmitted = context.Storage.CreateValueStore<TKey, TResult>(context.StateName("results"));
        _aggregate = aggregate;
        _eviction = eviction is null ? null : new EvictionTracker<TKey>(eviction);
    }

    public object Step(object?[] inputs)
    {
        var delta = (ZSet<KeyedValue<TKey, TValue>>)inputs[0]!;
        var output = new ZSet<KeyedValue<TKey, TResult>>();

        var entries = new List<(TKey, TValue, long)>(delta.DistinctCount);
        var touched = new List<TKey>();
        var seen = new HashSet<TKey>();
        foreach (var ((key, value), weight) in delta)
        {
            entries.Add((key, value, weight));
            if (seen.Add(key))
                touched.Add(key);
            _eviction?.Track(key);
        }
        _groups.UpdateAll(entries);

        var groups = _groups.GetGroups(touched);
        var lastResults = _lastEmitted.TryGetAll(touched);
        var puts = new List<(TKey, TResult)>();
        var removes = new List<TKey>();

        for (var i = 0; i < touched.Count; i++)
        {
            var key = touched[i];
            var group = groups[i];
            var (hadPrevious, previous) = lastResults[i];

            if (group.Count == 0)
            {
                // Group vanished: retract the last result, emit nothing new.
                if (hadPrevious)
                {
                    output.Add(new KeyedValue<TKey, TResult>(key, previous!), -1);
                    removes.Add(key);
                }
                continue;
            }

            var next = _aggregate.Aggregate(group);
            if (hadPrevious && EqualityComparer<TResult>.Default.Equals(previous, next))
                continue;

            if (hadPrevious)
                output.Add(new KeyedValue<TKey, TResult>(key, previous!), -1);
            output.Add(new KeyedValue<TKey, TResult>(key, next), 1);
            puts.Add((key, next));
        }

        _lastEmitted.PutAll(puts);
        _lastEmitted.RemoveAll(removes);

        // Closed keys can never be touched again (their events are dropped at the
        // watermark), so their state is deleted without emitting retractions: results
        // already published for them simply remain final.
        if (_eviction?.DrainClosed() is { Count: > 0 } closed)
        {
            _groups.RemoveGroups(closed);
            _lastEmitted.RemoveAll(closed);
        }

        return output;
    }
}
