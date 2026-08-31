using Incremental.Storage;

namespace Incremental.Operators;

/// <summary>
/// Incremental DISTINCT: maintains the integrated weight of every value and emits a delta
/// only when a value crosses between "present" (integrated weight &gt; 0) and "absent".
/// Requires all occurrences of a value to be routed to the same worker; the builder inserts
/// an exchange on the value hash upstream when the circuit runs with multiple workers.
/// </summary>
internal sealed class DistinctOperator<T> : IStepOperator where T : notnull
{
    private readonly ZSet<T> _empty = new();

    public object? EmptyStepResult => _empty;

    private readonly IWeightStore<T> _integrated;

    public DistinctOperator(OperatorContext context) =>
        _integrated = context.Storage.CreateWeightStore<T>(context.StateName("integrated"));

    private readonly List<(T, long)> _entries = [];
    private readonly OutputBuffer<T> _output = new();

    public object Step(object?[] inputs)
    {
        var delta = (ZSet<T>)inputs[0]!;
        var output = _output.Next(delta.DistinctCount);

        var entries = _entries;
        entries.Clear();
        entries.EnsureCapacity(delta.DistinctCount);
        foreach (var (value, weight) in delta)
            entries.Add((value, weight));

        var after = _integrated.UpdateAll(entries);
        for (var i = 0; i < entries.Count; i++)
        {
            var (value, weight) = entries[i];
            var before = after[i] - weight;
            var change = (after[i] > 0 ? 1 : 0) - (before > 0 ? 1 : 0);
            if (change != 0)
                output.Add(value, change);
        }
        return output;
    }
}

/// <summary>
/// DISTINCT over an already key-partitioned stream. Semantically identical to
/// <see cref="DistinctOperator{T}"/> over the (key, value) pairs, but the integrated weights
/// are stored grouped by key, so a watermark-driven eviction policy can drop a closed key's
/// state wholesale. Needs no extra exchange: the keyed stream's partitioning already routes
/// equal pairs to the same worker.
/// </summary>
internal sealed class KeyedDistinctOperator<TKey, TValue> : IStepOperator
    where TKey : notnull
    where TValue : notnull
{
    private readonly IIndexedWeightStore<TKey, TValue> _integrated;
    private readonly EvictionTracker<TKey>? _eviction;

    public KeyedDistinctOperator(OperatorContext context, KeyEviction<TKey>? eviction)
    {
        _integrated = context.Storage.CreateIndexedWeightStore<TKey, TValue>(context.StateName("integrated"));
        _eviction = eviction is null ? null : new EvictionTracker<TKey>(eviction);
    }

    private readonly ZSet<KeyedValue<TKey, TValue>> _empty = new();

    // Same gate as the other keyed operators: no delta, no state change — but an
    // attached KeyEviction still has to drain against the watermark every step.
    public object? EmptyStepResult => _eviction is null ? _empty : null;

    private readonly List<(TKey, TValue, long)> _entries = [];
    private readonly OutputBuffer<KeyedValue<TKey, TValue>> _output = new();

    public object Step(object?[] inputs)
    {
        var delta = (ZSet<KeyedValue<TKey, TValue>>)inputs[0]!;
        var output = _output.Next(delta.DistinctCount);

        var entries = _entries;
        entries.Clear();
        entries.EnsureCapacity(delta.DistinctCount);
        foreach (var ((key, value), weight) in delta)
        {
            entries.Add((key, value, weight));
            _eviction?.Track(key);
        }

        var after = _integrated.UpdateAll(entries);
        for (var i = 0; i < entries.Count; i++)
        {
            var (key, value, weight) = entries[i];
            var before = after[i] - weight;
            var change = (after[i] > 0 ? 1 : 0) - (before > 0 ? 1 : 0);
            if (change != 0)
                output.Add(new KeyedValue<TKey, TValue>(key, value), change);
        }

        // Closed keys can never be touched again; their already-emitted presence stays final.
        if (_eviction?.DrainClosed() is { Count: > 0 } closed)
            _integrated.RemoveGroups(closed);

        return output;
    }
}
