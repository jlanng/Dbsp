using Incremental.Storage;

namespace Incremental.Operators;

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

    private readonly ZSet<KeyedValue<TKey, TResult>> _empty = new();

    // Every state change below is driven by a delta row, so an all-empty step is a
    // no-op — unless a KeyEviction is attached, whose DrainClosed fires off the
    // watermark instead and must run whether or not data arrived.
    public object? EmptyStepResult => _eviction is null ? _empty : null;

    // Per-step scratch, reused across steps (single-threaded per worker; the store's batched
    // calls read these within the call).
    private readonly Dictionary<TKey, (long Rows, TResult Acc)> _perKey = [];
    private readonly List<TKey> _keys = [];
    private readonly List<(TKey, (long, TResult))> _puts = [];
    private readonly List<TKey> _removes = [];

    public object Step(object?[] inputs)
    {
        var delta = (ZSet<KeyedValue<TKey, TValue>>)inputs[0]!;

        var perKey = _perKey;
        perKey.Clear();
        foreach (var ((key, value), weight) in delta)
        {
            ref var entry = ref System.Runtime.InteropServices.CollectionsMarshal
                .GetValueRefOrAddDefault(perKey, key, out var seen);
            if (!seen)
                entry = (0L, _aggregate.Zero);
            entry = (entry.Rows + weight, _aggregate.Combine(entry.Acc, _aggregate.Map(value, weight)));
        }

        // At most one retraction and one assertion per touched key.
        var output = new ZSet<KeyedValue<TKey, TResult>>(perKey.Count * 2);

        var keys = _keys;
        keys.Clear();
        keys.EnsureCapacity(perKey.Count);
        foreach (var (key, _) in perKey)
        {
            keys.Add(key);
            _eviction?.Track(key);
        }

        var states = _state.TryGetAll(keys);
        var puts = _puts;
        var removes = _removes;
        puts.Clear();
        removes.Clear();

        for (var i = 0; i < keys.Count; i++)
        {
            var key = keys[i];
            var change = perKey[key];
            var (had, previous) = states[i];
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
                removes.Add(key);
            else
                puts.Add((key, (rows, acc)));
        }

        _state.PutAll(puts);
        _state.RemoveAll(removes);

        if (_eviction?.DrainClosed() is { Count: > 0 } closed)
            _state.RemoveAll(closed);

        return output;
    }
}
