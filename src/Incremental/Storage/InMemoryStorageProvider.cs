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

        // Groups appear and vanish constantly in a sliding workload — a key whose last row is
        // retracted takes its dictionary with it, and the next insert on that key allocates
        // another. Emptied dictionaries are parked here instead (bounded, and empty, so this
        // holds nothing but their capacity) and handed back out on the next create.
        private readonly Stack<Dictionary<TValue, long>> _spare = new();
        private const int MaxSpare = 32;

        public long Update(TKey key, TValue value, long delta)
        {
            ref var entry = ref CollectionsMarshal.GetValueRefOrAddDefault(_groups, key, out var hadGroup);
            if (!hadGroup)
                entry = _spare.Count > 0 ? _spare.Pop() : new Dictionary<TValue, long>();
            // Held as a local, not through the ref: removing the key below clears the slot
            // the ref points at, and the group is still needed after that.
            var group = entry!;

            ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(group, value, out _);
            var updated = slot + delta;
            if (updated == 0)
            {
                group.Remove(value);
                if (group.Count == 0)
                {
                    _groups.Remove(key);
                    if (_spare.Count < MaxSpare)
                        _spare.Push(group);
                }
            }
            else
            {
                slot = updated;
            }
            return updated;
        }

        /// <summary>Batched update that remembers the group it last touched. Callers arrive
        /// with entries grouped by key far more often than not — a snapshot output writes
        /// every row of a step under one group, and the Min/Max tree emits its updates
        /// bucket by bucket — and for a key like a tournament-tree slot or a category string
        /// the group lookup is the expensive half of the update.</summary>
        public void ApplyAll(IReadOnlyList<(TKey Key, TValue Value, long Delta)> entries)
        {
            var comparer = EqualityComparer<TKey>.Default;
            var lastKey = default(TKey);
            Dictionary<TValue, long>? lastGroup = null;

            for (var i = 0; i < entries.Count; i++)
            {
                var (key, value, delta) = entries[i];
                Dictionary<TValue, long> group;
                if (lastGroup is not null && comparer.Equals(key, lastKey!))
                {
                    group = lastGroup;
                }
                else
                {
                    ref var entry = ref CollectionsMarshal.GetValueRefOrAddDefault(_groups, key, out var hadGroup);
                    if (!hadGroup)
                        entry = _spare.Count > 0 ? _spare.Pop() : new Dictionary<TValue, long>();
                    group = entry!;
                    (lastKey, lastGroup) = (key, group);
                }

                ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(group, value, out _);
                var updated = slot + delta;
                if (updated == 0)
                {
                    group.Remove(value);
                    if (group.Count == 0)
                    {
                        _groups.Remove(key);
                        if (_spare.Count < MaxSpare)
                            _spare.Push(group);
                        lastGroup = null;  // no longer reachable through the map
                    }
                }
                else
                {
                    slot = updated;
                }
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
