using StreamProcessor.Storage;

namespace StreamProcessor.Operators;

/// <summary>
/// Grouped aggregation for linear aggregates (COUNT, SUM, …). Because the aggregate is a
/// group homomorphism, the operator folds the step's delta straight into a per-key
/// accumulator — it keeps no group contents at all. State per key is (row count, accumulator);
/// the row count exists only to detect a group vanishing, which the accumulator alone cannot
/// (a SUM of 0 is not an empty group). Output semantics match
/// <see cref="AggregateOperator{TKey,TValue,TResult}"/>: stale results are retracted.
/// </summary>
internal sealed class LinearAggregateOperator<TKey, TValue, TResult> : IStepOperator
    where TKey : notnull
    where TValue : notnull
    where TResult : notnull
{
    private readonly IValueStore<TKey, (long Rows, TResult Acc)> _state;
    private readonly ILinearAggregate<TValue, TResult> _aggregate;
    private readonly EvictionTracker<TKey>? _eviction;

    public LinearAggregateOperator(OperatorContext context, ILinearAggregate<TValue, TResult> aggregate, KeyEviction<TKey>? eviction)
    {
        _state = context.Storage.CreateValueStore<TKey, (long, TResult)>(context.StateName("state"));
        _aggregate = aggregate;
        _eviction = eviction is null ? null : new EvictionTracker<TKey>(eviction);
    }

    public object Step(object?[] inputs)
    {
        var delta = (ZSet<KeyedValue<TKey, TValue>>)inputs[0]!;
        var output = new ZSet<KeyedValue<TKey, TResult>>();

        var perKey = new Dictionary<TKey, (long Rows, TResult Acc)>();
        foreach (var ((key, value), weight) in delta)
        {
            var entry = perKey.TryGetValue(key, out var current) ? current : (Rows: 0L, Acc: _aggregate.Zero);
            perKey[key] = (entry.Rows + weight, _aggregate.Combine(entry.Acc, _aggregate.Map(value, weight)));
        }

        foreach (var (key, change) in perKey)
        {
            _eviction?.Track(key);

            var had = _state.TryGet(key, out var previous);
            var rows = (had ? previous.Rows : 0) + change.Rows;
            var acc = had ? _aggregate.Combine(previous.Acc, change.Acc) : change.Acc;

            var wasEmitted = had && previous.Rows > 0;
            var isEmitted = rows > 0;
            var unchanged = wasEmitted && isEmitted && EqualityComparer<TResult>.Default.Equals(previous.Acc, acc);

            if (!unchanged)
            {
                if (wasEmitted)
                    output.Add(new KeyedValue<TKey, TResult>(key, previous.Acc), -1);
                if (isEmitted)
                    output.Add(new KeyedValue<TKey, TResult>(key, acc), 1);
            }

            if (rows == 0)
                _state.Remove(key);
            else
                _state.Put(key, (rows, acc));
        }

        _eviction?.EvictClosed(_state.Remove);

        return output;
    }
}
