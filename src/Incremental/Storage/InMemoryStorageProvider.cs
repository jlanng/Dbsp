using System.Diagnostics.CodeAnalysis;

namespace Incremental.Storage;

/// <summary>
/// Heap-backed storage. State lives for the lifetime of the operator instances that own it;
/// nothing survives a circuit restart. This is the default provider.
/// </summary>
public sealed class InMemoryStorageProvider : IStorageProvider
{
    public IWeightStore<T> CreateWeightStore<T>(string name) where T : notnull =>
        new InMemoryWeightStore<T>();

    public IIndexedWeightStore<TKey, TValue> CreateIndexedWeightStore<TKey, TValue>(string name)
        where TKey : notnull
        where TValue : notnull =>
        new InMemoryIndexedWeightStore<TKey, TValue>();

    public IValueStore<TKey, TValue> CreateValueStore<TKey, TValue>(string name) where TKey : notnull =>
        new InMemoryValueStore<TKey, TValue>();

    private sealed class InMemoryWeightStore<T> : IWeightStore<T> where T : notnull
    {
        private readonly Dictionary<T, long> _weights = new();

        public long Update(T value, long delta)
        {
            var updated = _weights.GetValueOrDefault(value) + delta;
            if (updated == 0)
                _weights.Remove(value);
            else
                _weights[value] = updated;
            return updated;
        }

        public long WeightOf(T value) => _weights.GetValueOrDefault(value);

        public void Remove(T value) => _weights.Remove(value);
    }

    private sealed class InMemoryIndexedWeightStore<TKey, TValue> : IIndexedWeightStore<TKey, TValue>
        where TKey : notnull
        where TValue : notnull
    {
        private readonly Dictionary<TKey, Dictionary<TValue, long>> _groups = new();

        public long Update(TKey key, TValue value, long delta)
        {
            if (!_groups.TryGetValue(key, out var group))
                _groups[key] = group = new Dictionary<TValue, long>();

            var updated = group.GetValueOrDefault(value) + delta;
            if (updated == 0)
            {
                group.Remove(value);
                if (group.Count == 0)
                    _groups.Remove(key);
            }
            else
            {
                group[value] = updated;
            }
            return updated;
        }

        public IReadOnlyList<WeightedValue<TValue>> GetGroup(TKey key)
        {
            if (!_groups.TryGetValue(key, out var group))
                return [];

            var result = new WeightedValue<TValue>[group.Count];
            var i = 0;
            foreach (var kv in group)
                result[i++] = new WeightedValue<TValue>(kv.Key, kv.Value);
            return result;
        }

        public void RemoveGroup(TKey key) => _groups.Remove(key);
    }

    private sealed class InMemoryValueStore<TKey, TValue> : IValueStore<TKey, TValue> where TKey : notnull
    {
        private readonly Dictionary<TKey, TValue> _values = new();

        public bool TryGet(TKey key, [MaybeNullWhen(false)] out TValue value) => _values.TryGetValue(key, out value);
        public void Put(TKey key, TValue value) => _values[key] = value;
        public void Remove(TKey key) => _values.Remove(key);
    }
}
