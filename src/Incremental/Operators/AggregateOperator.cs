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

    private readonly ZSet<KeyedValue<TKey, TResult>> _empty = new();

    // Every state change below is driven by a delta row, so an all-empty step is a
    // no-op — unless a KeyEviction is attached, whose DrainClosed fires off the
    // watermark instead and must run whether or not data arrived.
    public object? EmptyStepResult => _eviction is null ? _empty : null;

    // Per-step scratch, reused across steps (single-threaded per worker).
    private readonly List<(TKey, TValue, long)> _entries = [];
    private readonly List<TKey> _touched = [];
    private readonly HashSet<TKey> _seen = [];
    private readonly List<(TKey, TResult)> _puts = [];
    private readonly List<TKey> _removes = [];
    private readonly OutputBuffer<KeyedValue<TKey, TResult>> _output = new();

    public object Step(object?[] inputs)
    {
        var delta = (ZSet<KeyedValue<TKey, TValue>>)inputs[0]!;

        var entries = _entries;
        var touched = _touched;
        entries.Clear();
        touched.Clear();
        _seen.Clear();
        entries.EnsureCapacity(delta.DistinctCount);
        foreach (var ((key, value), weight) in delta)
        {
            entries.Add((key, value, weight));
            if (_seen.Add(key))
                touched.Add(key);
            _eviction?.Track(key);
        }
        _groups.ApplyAll(entries);

        // At most one retraction and one assertion per touched key.
        var output = _output.Next(touched.Count * 2);

        // GetGroups rather than GetGroupsInto, deliberately: each group is handed to a user
        // aggregate, so it must be a plain materialized list, not a window over reused scratch.
        var groups = _groups.GetGroups(touched);
        var lastResults = _lastEmitted.TryGetAll(touched);
        var puts = _puts;
        var removes = _removes;
        puts.Clear();
        removes.Clear();

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
