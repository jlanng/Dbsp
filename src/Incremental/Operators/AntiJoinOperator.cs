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

    private readonly ZSet<KeyedValue<TKey, TLeft>> _empty = new();

    // Every state change below is driven by a delta row, so an all-empty step is a
    // no-op — unless a KeyEviction is attached, whose DrainClosed fires off the
    // watermark instead and must run whether or not data arrived.
    public object? EmptyStepResult => _eviction is null ? _empty : null;

    // Per-step scratch, reused across steps (single-threaded per worker).
    private readonly Dictionary<TKey, long> _rightKeyDeltas = [];
    private readonly List<(TKey, long)> _countEntries = [];
    private readonly List<TKey> _flipped = [];
    private readonly List<long> _flipSigns = [];
    private readonly List<WeightedValue<TLeft>> _flippedRows = [];
    private readonly List<int> _flippedBounds = [];
    private readonly List<(TKey, TLeft, long)> _leftEntries = [];
    private readonly List<TKey> _leftKeys = [];
    private readonly Dictionary<TKey, int> _matchIndex = [];
    private readonly OutputBuffer<KeyedValue<TKey, TLeft>> _output = new();

    public object Step(object?[] inputs)
    {
        var deltaLeft = (ZSet<KeyedValue<TKey, TLeft>>)inputs[0]!;
        var deltaRight = (ZSet<KeyedValue<TKey, TRight>>)inputs[1]!;
        var output = _output.Next(deltaLeft.DistinctCount);

        var rightKeyDeltas = _rightKeyDeltas;
        rightKeyDeltas.Clear();
        foreach (var ((key, _), weight) in deltaRight)
            rightKeyDeltas[key] = rightKeyDeltas.GetValueOrDefault(key) + weight;

        var countEntries = _countEntries;
        countEntries.Clear();
        countEntries.EnsureCapacity(rightKeyDeltas.Count);
        foreach (var (key, keyDelta) in rightKeyDeltas)
        {
            countEntries.Add((key, keyDelta));
            _eviction?.Track(key);
        }

        // UpdateAll rather than ApplyAll, deliberately: the updated counts are read below to
        // detect keys whose presence flipped.
        var after = _rightRowCount.UpdateAll(countEntries);
        var flipped = _flipped;
        var flipSigns = _flipSigns;
        flipped.Clear();
        flipSigns.Clear();
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
        if (flipped.Count > 0)
        {
            _flippedRows.Clear();
            _flippedBounds.Clear();
            _left.GetGroupsInto(flipped, _flippedRows, _flippedBounds);
            for (var i = 0; i < flipped.Count; i++)
                for (var r = i == 0 ? 0 : _flippedBounds[i - 1]; r < _flippedBounds[i]; r++)
                {
                    var (leftValue, leftWeight) = _flippedRows[r];
                    output.Add(new KeyedValue<TKey, TLeft>(flipped[i], leftValue), flipSigns[i] * leftWeight);
                }
        }

        var leftEntries = _leftEntries;
        var leftKeys = _leftKeys;
        leftEntries.Clear();
        leftKeys.Clear();
        _matchIndex.Clear();
        leftEntries.EnsureCapacity(deltaLeft.DistinctCount);
        foreach (var ((key, leftValue), weight) in deltaLeft)
        {
            leftEntries.Add((key, leftValue, weight));
            _eviction?.Track(key);
            if (_matchIndex.TryAdd(key, leftKeys.Count))
                leftKeys.Add(key);
        }

        _left.ApplyAll(leftEntries);
        var rightCounts = _rightRowCount.WeightOfAll(leftKeys);
        foreach (var (key, leftValue, weight) in leftEntries)
            if (rightCounts[_matchIndex[key]] <= 0)
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
