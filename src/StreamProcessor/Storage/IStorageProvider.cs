using System.Diagnostics.CodeAnalysis;

namespace StreamProcessor.Storage;

/// <summary>
/// Factory for the state stores that stateful operators (join, aggregate, distinct) use to
/// hold their integrated state. Stores are created per operator, per worker — the runtime
/// guarantees a store is only ever touched by the worker thread that owns it, so
/// implementations do not need to be thread-safe across stores with different names.
/// </summary>
public interface IStorageProvider
{
    /// <summary>A value → weight map (an integrated Z-set). Used by e.g. <c>Distinct</c>.</summary>
    IWeightStore<T> CreateWeightStore<T>(string name) where T : notnull;

    /// <summary>A key → (value → weight) map (an integrated indexed Z-set). Used by joins and aggregations.</summary>
    IIndexedWeightStore<TKey, TValue> CreateIndexedWeightStore<TKey, TValue>(string name)
        where TKey : notnull
        where TValue : notnull;

    /// <summary>A plain key → value map. Used to remember previously emitted results for retraction.</summary>
    IValueStore<TKey, TValue> CreateValueStore<TKey, TValue>(string name) where TKey : notnull;
}

public interface IWeightStore<T> where T : notnull
{
    /// <summary>Adds <paramref name="delta"/> to the weight of <paramref name="value"/> and returns the
    /// resulting weight. Implementations must drop entries whose weight reaches zero.</summary>
    long Update(T value, long delta);

    /// <summary>The current weight of <paramref name="value"/>, or 0 if absent.</summary>
    long WeightOf(T value);
}

public interface IIndexedWeightStore<TKey, TValue>
    where TKey : notnull
    where TValue : notnull
{
    /// <summary>Adds <paramref name="delta"/> to the weight of (<paramref name="key"/>, <paramref name="value"/>)
    /// and returns the resulting weight. Entries whose weight reaches zero must be dropped.</summary>
    long Update(TKey key, TValue value, long delta);

    /// <summary>All values (with weights) currently grouped under <paramref name="key"/>. Empty if none.</summary>
    IReadOnlyList<WeightedValue<TValue>> GetGroup(TKey key);

    /// <summary>Drops the entire group for <paramref name="key"/> (watermark-driven eviction). No-op if absent.</summary>
    void RemoveGroup(TKey key);
}

public interface IValueStore<TKey, TValue> where TKey : notnull
{
    bool TryGet(TKey key, [MaybeNullWhen(false)] out TValue value);
    void Put(TKey key, TValue value);
    void Remove(TKey key);
}
