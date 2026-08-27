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
    private readonly EvictionTracker<TKey>? _eviction;

    public AntiJoinOperator(OperatorContext context, KeyEviction<TKey>? eviction)
    {
        _left = context.Storage.CreateIndexedWeightStore<TKey, TLeft>(context.StateName("left"));
        _rightRowCount = context.Storage.CreateWeightStore<TKey>(context.StateName("rightRows"));
        _eviction = eviction is null ? null : new EvictionTracker<TKey>(eviction);
    }

    public object Step(object?[] inputs)
    {
        var deltaLeft = (ZSet<KeyedValue<TKey, TLeft>>)inputs[0]!;
        var deltaRight = (ZSet<KeyedValue<TKey, TRight>>)inputs[1]!;
        var output = new ZSet<KeyedValue<TKey, TLeft>>();

        var rightKeyDeltas = new Dictionary<TKey, long>();
        foreach (var ((key, _), weight) in deltaRight)
            rightKeyDeltas[key] = rightKeyDeltas.GetValueOrDefault(key) + weight;

        var countEntries = new List<(TKey, long)>(rightKeyDeltas.Count);
        foreach (var (key, keyDelta) in rightKeyDeltas)
        {
            countEntries.Add((key, keyDelta));
            _eviction?.Track(key);
        }

        var after = _rightRowCount.UpdateAll(countEntries);
        var flipped = new List<TKey>();
        var flipSigns = new List<long>();
        for (var i = 0; i < countEntries.Count; i++)
        {
            var (key, keyDelta) = countEntries[i];
            var before = after[i] - keyDelta;
            var wasMatched = before > 0;
            var isMatched = after[i] > 0;
            if (wasMatched == isMatched)
                continue;
            flipped.Add(key);
            flipSigns.Add(isMatched ? -1 : +1);
        }

        // Presence flipped: every integrated left row for these keys changes sides.
        var flippedGroups = _left.GetGroups(flipped);
        for (var i = 0; i < flipped.Count; i++)
            foreach (var (leftValue, leftWeight) in flippedGroups[i])
                output.Add(new KeyedValue<TKey, TLeft>(flipped[i], leftValue), flipSigns[i] * leftWeight);

        var leftEntries = new List<(TKey, TLeft, long)>(deltaLeft.DistinctCount);
        var leftKeys = new List<TKey>();
        var matchIndex = new Dictionary<TKey, int>();
        foreach (var ((key, leftValue), weight) in deltaLeft)
        {
            leftEntries.Add((key, leftValue, weight));
            _eviction?.Track(key);
            if (matchIndex.TryAdd(key, leftKeys.Count))
                leftKeys.Add(key);
        }

        _left.UpdateAll(leftEntries);
        var rightCounts = _rightRowCount.WeightOfAll(leftKeys);
        foreach (var (key, leftValue, weight) in leftEntries)
            if (rightCounts[matchIndex[key]] <= 0)
                output.Add(new KeyedValue<TKey, TLeft>(key, leftValue), weight);

        // Closed keys can never flip again (the watermark drops their events), so both the
        // integrated left rows and the right row count are deleted without retractions:
        // rows already emitted as unmatched simply remain final.
        if (_eviction?.DrainClosed() is { Count: > 0 } closed)
        {
            _left.RemoveGroups(closed);
            _rightRowCount.RemoveAll(closed);
        }

        return output;
    }
}
