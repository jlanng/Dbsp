using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;

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
            // One probe locates the slot and writes it; only the (rare) fall-to-zero case
            // pays a second lookup to remove the entry.
            ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(_weights, value, out _);
            var updated = slot + delta;
            if (updated == 0)
                _weights.Remove(value);
            else
                slot = updated;
            return updated;
        }

        public void ApplyAll(IReadOnlyList<(T Value, long Delta)> entries)
        {
            for (var i = 0; i < entries.Count; i++)
                Update(entries[i].Value, entries[i].Delta);
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
            ref var group = ref CollectionsMarshal.GetValueRefOrAddDefault(_groups, key, out var hadGroup);
            if (!hadGroup)
                group = new Dictionary<TValue, long>();

            ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(group!, value, out _);
            var updated = slot + delta;
            if (updated == 0)
            {
                group!.Remove(value);
                if (group.Count == 0)
                    _groups.Remove(key);
            }
            else
            {
                slot = updated;
            }
            return updated;
        }

        public void ApplyAll(IReadOnlyList<(TKey Key, TValue Value, long Delta)> entries)
        {
            for (var i = 0; i < entries.Count; i++)
            {
                var (key, value, delta) = entries[i];
                Update(key, value, delta);
            }
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

        /// <summary>Reads whole groups without materializing one array per group: the entries
        /// go straight from the live dictionaries into the caller's buffer, which the hot
        /// operators reuse across steps. Safe precisely because the contract is a copy —
        /// callers get values, not a view onto state that the next update would move.</summary>
        public void GetGroupsInto(IReadOnlyList<TKey> keys, List<WeightedValue<TValue>> into, List<int> bounds)
        {
            for (var i = 0; i < keys.Count; i++)
            {
                if (_groups.TryGetValue(keys[i], out var group))
                {
                    into.EnsureCapacity(into.Count + group.Count);
                    foreach (var kv in group)
                        into.Add(new WeightedValue<TValue>(kv.Key, kv.Value));
                }
                bounds.Add(into.Count);
            }
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
