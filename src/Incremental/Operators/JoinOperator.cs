using Incremental.Storage;

namespace Incremental.Operators;

/// <summary>
/// Incremental equi-join. A join is bilinear, so its incremental form is
/// Δ(L ⋈ R) = ΔL ⋈ R + L ⋈ ΔR + ΔL ⋈ ΔR: each side keeps its integrated state in an
/// indexed store, and each step only probes the state with the new deltas — no
/// recomputation over historical data. Both inputs must be co-partitioned by key, which
/// <c>GroupBy</c> guarantees via its exchange.
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
        _eviction = eviction is null ? null : new EvictionTracker<TKey>(context, eviction);
        _errors = context.Errors;
    }

    // Null under the default policy. Each application reads it into a local once and
    // branches on that per pair, which is all the default path pays for the other policy.
    private readonly RecordErrors? _errors;

    private readonly ZSet<TOut> _empty = new();

    // Every state change below is driven by a delta row, so an all-empty step is a
    // no-op — unless a KeyEviction is attached, whose DrainClosed fires off the
    // watermark instead and must run whether or not data arrived.
    public object? EmptyStepResult => _eviction is null ? _empty : null;

    // Per-step scratch, reused across steps: this operator is single-threaded per worker and
    // the store's batched calls read these within the call. A join sits on the busiest edge
    // of most graphs, so what it allocates per step it allocates per record.
    private readonly List<(TKey Key, TLeft Value, long Delta)> _leftEntries = [];
    private readonly List<(TKey Key, TRight Value, long Delta)> _rightEntries = [];
    private readonly List<TKey> _probeKeys = [];
    private readonly Dictionary<TKey, int> _probeIndex = [];
    private readonly List<int> _groupOfEntry = [];
    private readonly List<int> _bounds = [];
    private readonly List<WeightedValue<TLeft>> _leftRows = [];
    private readonly List<WeightedValue<TRight>> _rightRows = [];
    private readonly OutputBuffer<TOut> _output = new();

    // The output delta under construction, shared by the left and right applications within
    // one step and handed out once at the end.
    private ZSet<TOut>? _current;

    public object Step(object?[] inputs)
    {
        if ((ZSet<KeyedValue<TKey, TLeft>>)inputs[0]! is { IsEmpty: false } deltaLeft)
            ApplyLeft(deltaLeft);
        if ((ZSet<KeyedValue<TKey, TRight>>)inputs[1]! is { IsEmpty: false } deltaRight)
            ApplyRight(deltaRight);

        // Closed keys can never be probed again (the watermark drops their events), so
        // both sides' state for them is deleted; results already emitted remain final.
        if (_eviction?.DrainClosed() is { Count: > 0 } closed)
        {
            _left.RemoveGroups(closed);
            _right.RemoveGroups(closed);
        }

        var output = _current ?? _output.Next(0);
        _current = null;
        return output;
    }

    /// <summary>ΔL ⋈ R with R's current state, then ΔL integrated — so the right-side
    /// delta covers the ΔL ⋈ ΔR cross term by probing the updated L.</summary>
    private void ApplyLeft(ZSet<KeyedValue<TKey, TLeft>> delta)
    {
        var output = _current ??= _output.Next(delta.DistinctCount);
        output.EnsureCapacity(output.DistinctCount + delta.DistinctCount);
        Flatten(delta, _leftEntries);
        _rightRows.Clear();
        _bounds.Clear();
        _right.GetGroupsInto(_probeKeys, _rightRows, _bounds);
        var errors = _errors;
        for (var i = 0; i < _leftEntries.Count; i++)
        {
            var (key, left, leftWeight) = _leftEntries[i];
            var group = _groupOfEntry[i];
            for (var r = group == 0 ? 0 : _bounds[group - 1]; r < _bounds[group]; r++)
            {
                var (right, rightWeight) = _rightRows[r];
                if (errors is null)
                    output.Add(_resultSelector(key, left, right), checked(leftWeight * rightWeight));
                else
                    errors.Emit(output, _resultSelector, key, left, right, checked(leftWeight * rightWeight),
                        new KeyedValue<TKey, TLeft>(key, left));
            }
            _eviction?.Track(key);
        }
        _left.ApplyAll(_leftEntries);
    }

    /// <summary>The mirror image: ΔR ⋈ L (ΔL applied first), then ΔR integrated.</summary>
    private void ApplyRight(ZSet<KeyedValue<TKey, TRight>> delta)
    {
        var output = _current ??= _output.Next(delta.DistinctCount);
        output.EnsureCapacity(output.DistinctCount + delta.DistinctCount);
        Flatten(delta, _rightEntries);
        _leftRows.Clear();
        _bounds.Clear();
        _left.GetGroupsInto(_probeKeys, _leftRows, _bounds);
        var errors = _errors;
        for (var i = 0; i < _rightEntries.Count; i++)
        {
            var (key, right, rightWeight) = _rightEntries[i];
            var group = _groupOfEntry[i];
            for (var l = group == 0 ? 0 : _bounds[group - 1]; l < _bounds[group]; l++)
            {
                var (left, leftWeight) = _leftRows[l];
                if (errors is null)
                    output.Add(_resultSelector(key, left, right), checked(leftWeight * rightWeight));
                else
                    errors.Emit(output, _resultSelector, key, left, right, checked(leftWeight * rightWeight),
                        new KeyedValue<TKey, TRight>(key, right));
            }
            _eviction?.Track(key);
        }
        _right.ApplyAll(_rightEntries);
    }

    /// <summary>Flattens a delta into <paramref name="entries"/> (the list its own side
    /// integrates) while building the distinct probe-key list and, for each entry, the index
    /// of its key's group — so the probe loop reads the fetched groups by offset instead of
    /// hashing the key a second time.</summary>
    private void Flatten<TValue>(ZSet<KeyedValue<TKey, TValue>> delta, List<(TKey, TValue, long)> entries)
        where TValue : notnull
    {
        entries.Clear();
        _probeKeys.Clear();
        _probeIndex.Clear();
        _groupOfEntry.Clear();
        entries.EnsureCapacity(delta.DistinctCount);
        _groupOfEntry.EnsureCapacity(delta.DistinctCount);
        foreach (var ((key, value), weight) in delta)
        {
            entries.Add((key, value, weight));
            ref var group = ref System.Runtime.InteropServices.CollectionsMarshal
                .GetValueRefOrAddDefault(_probeIndex, key, out var known);
            if (!known)
            {
                group = _probeKeys.Count;
                _probeKeys.Add(key);
            }
            _groupOfEntry.Add(group);
        }
    }
}
