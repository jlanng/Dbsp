using Incremental.Storage;

namespace Incremental.Operators;

/// <summary>
/// Incremental anti-join: emits the left rows whose key has no right-side match. State is
/// the integrated left side plus a per-key row count of the right side. Per step:
/// (1) apply the right delta's per-key row-count changes — a key whose presence flips
/// retracts (now matched) or re-asserts (now unmatched) all of its integrated left rows;
/// (2) apply the left delta, emitting each row whose key is currently unmatched.
/// </summary>
internal sealed class AntiJoinOperator<TKey, TLeft, TRight> : IStepOperator
    where TKey : notnull
    where TLeft : notnull
    where TRight : notnull
{
    private readonly IIndexedWeightStore<TKey, TLeft> _left;
    private readonly IWeightStore<TKey> _rightRowCount;

    public AntiJoinOperator(OperatorContext context)
    {
        _left = context.Storage.CreateIndexedWeightStore<TKey, TLeft>(context.StateName("left"));
        _rightRowCount = context.Storage.CreateWeightStore<TKey>(context.StateName("rightRows"));
    }

    public object Step(object?[] inputs)
    {
        var deltaLeft = (ZSet<KeyedValue<TKey, TLeft>>)inputs[0]!;
        var deltaRight = (ZSet<KeyedValue<TKey, TRight>>)inputs[1]!;
        var output = new ZSet<KeyedValue<TKey, TLeft>>();

        var rightKeyDeltas = new Dictionary<TKey, long>();
        foreach (var ((key, _), weight) in deltaRight)
            rightKeyDeltas[key] = rightKeyDeltas.GetValueOrDefault(key) + weight;

        foreach (var (key, keyDelta) in rightKeyDeltas)
        {
            var before = _rightRowCount.WeightOf(key);
            var after = _rightRowCount.Update(key, keyDelta);
            var wasMatched = before > 0;
            var isMatched = after > 0;
            if (wasMatched == isMatched)
                continue;

            // Presence flipped: every integrated left row for this key changes sides.
            var sign = isMatched ? -1 : +1;
            foreach (var (leftValue, leftWeight) in _left.GetGroup(key))
                output.Add(new KeyedValue<TKey, TLeft>(key, leftValue), sign * leftWeight);
        }

        foreach (var ((key, leftValue), weight) in deltaLeft)
        {
            _left.Update(key, leftValue, weight);
            if (_rightRowCount.WeightOf(key) <= 0)
                output.Add(new KeyedValue<TKey, TLeft>(key, leftValue), weight);
        }

        return output;
    }
}
