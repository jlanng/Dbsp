using System.Diagnostics.CodeAnalysis;

namespace Incremental.Storage;

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

    /// <summary>Drops <paramref name="value"/> regardless of its weight (watermark-driven eviction). No-op if absent.</summary>
    void Remove(T value);

    /// <summary>Batched <see cref="Update"/>: applies every entry in order and returns the resulting
    /// weights, aligned with the input. Providers with per-call latency override this to issue one
    /// round trip for the whole batch.</summary>
    long[] UpdateAll(IReadOnlyList<(T Value, long Delta)> entries)
    {
        var results = new long[entries.Count];
        for (var i = 0; i < entries.Count; i++)
            results[i] = Update(entries[i].Value, entries[i].Delta);
        return results;
    }

    /// <summary>Batched <see cref="WeightOf"/>, aligned with the input.</summary>
    long[] WeightOfAll(IReadOnlyList<T> values)
    {
        var results = new long[values.Count];
        for (var i = 0; i < values.Count; i++)
            results[i] = WeightOf(values[i]);
        return results;
    }

    /// <summary>Batched <see cref="Remove"/>.</summary>
    void RemoveAll(IReadOnlyList<T> values)
    {
        foreach (var value in values)
            Remove(value);
    }
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

    /// <summary>Batched <see cref="Update"/>: applies every entry in order and returns the resulting
    /// weights, aligned with the input. Providers with per-call latency override this to issue one
    /// round trip for the whole batch.</summary>
    long[] UpdateAll(IReadOnlyList<(TKey Key, TValue Value, long Delta)> entries)
    {
        var results = new long[entries.Count];
        for (var i = 0; i < entries.Count; i++)
            results[i] = Update(entries[i].Key, entries[i].Value, entries[i].Delta);
        return results;
    }

    /// <summary>Batched <see cref="GetGroup"/>, aligned with the input.</summary>
    IReadOnlyList<WeightedValue<TValue>>[] GetGroups(IReadOnlyList<TKey> keys)
    {
        var results = new IReadOnlyList<WeightedValue<TValue>>[keys.Count];
        for (var i = 0; i < keys.Count; i++)
            results[i] = GetGroup(keys[i]);
        return results;
    }

    /// <summary>Batched <see cref="RemoveGroup"/>.</summary>
    void RemoveGroups(IReadOnlyList<TKey> keys)
    {
        foreach (var key in keys)
            RemoveGroup(key);
    }
}

public interface IValueStore<TKey, TValue> where TKey : notnull
{
    bool TryGet(TKey key, [MaybeNullWhen(false)] out TValue value);
    void Put(TKey key, TValue value);
    void Remove(TKey key);

    /// <summary>Batched <see cref="TryGet"/>, aligned with the input. <c>Value</c> is default
    /// when <c>Found</c> is false. Providers with per-call latency override this to issue one
    /// round trip for the whole batch.</summary>
    (bool Found, TValue? Value)[] TryGetAll(IReadOnlyList<TKey> keys)
    {
        var results = new (bool, TValue?)[keys.Count];
        for (var i = 0; i < keys.Count; i++)
            results[i] = (TryGet(keys[i], out var value), value);
        return results;
    }

    /// <summary>Batched <see cref="Put"/>.</summary>
    void PutAll(IReadOnlyList<(TKey Key, TValue Value)> pairs)
    {
        foreach (var (key, value) in pairs)
            Put(key, value);
    }

    /// <summary>Batched <see cref="Remove"/>.</summary>
    void RemoveAll(IReadOnlyList<TKey> keys)
    {
        foreach (var key in keys)
            Remove(key);
    }
}
