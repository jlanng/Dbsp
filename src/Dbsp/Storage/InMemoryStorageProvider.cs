using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;

namespace Dbsp.Storage;

/// <summary>
/// Heap-backed storage. State lives for the lifetime of the operator instances that own it;
/// nothing survives a circuit restart. This is the default provider.
/// </summary>
public sealed class InMemoryStorageProvider : IStorageProvider
{
    public IWeightStore<T> CreateWeightStore<T>(string name) where T : notnull =>
        new WeightStore<T>();

    public IIndexedWeightStore<TKey, TValue> CreateIndexedWeightStore<TKey, TValue>(string name)
        where TKey : notnull
        where TValue : notnull =>
        new IndexedWeightStore<TKey, TValue>();

    public IValueStore<TKey, TValue> CreateValueStore<TKey, TValue>(string name) where TKey : notnull =>
        new ValueStore<TKey, TValue>();

    public IDeadlineStore<TKey> CreateDeadlineStore<TKey>(string name) where TKey : notnull =>
        new DeadlineStore<TKey>();

    // The stores are internal, not private, because ChangelogStorageProvider keeps its
    // state in them: the same structures and the same read path, with change capture
    // wrapped around the writes. The members below each store's interface are for that.

    internal sealed class WeightStore<T> : IWeightStore<T> where T : notnull
    {
        private readonly Dictionary<T, long> _weights = new();

        /// <summary>Every value with its weight.</summary>
        internal Dictionary<T, long> Entries => _weights;

        /// <summary>Makes <paramref name="value"/>'s weight exactly <paramref name="weight"/>.</summary>
        internal void Set(T value, long weight)
        {
            if (weight == 0)
                _weights.Remove(value);
            else
                _weights[value] = weight;
        }

        internal void Clear() => _weights.Clear();

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

    internal sealed class IndexedWeightStore<TKey, TValue> : IIndexedWeightStore<TKey, TValue>
        where TKey : notnull
        where TValue : notnull
    {
        /// <summary>One key's values. A group of exactly one value is held inline: state
        /// keyed by something row-like — what a lookup remembers per fact, the facts joined
        /// on a unique key — is all such groups, and a dictionary each would be a couple of
        /// hundred bytes allocated per record that arrives and thrown away per record that
        /// leaves. A second distinct value moves the group into <see cref="Many"/>, where
        /// it stays until it empties.</summary>
        private struct Group
        {
            public TValue Single;
            public long Weight; // of Single; unused once Many is set
            public Dictionary<TValue, long>? Many;
        }

        private readonly Dictionary<TKey, Group> _groups = new();

        // Entries held, and groups that have outgrown their inline slot: this thread's
        // counts, for whoever asks what the store holds without walking it.
        private long _entryCount;
        private long _manyGroups;

        /// <summary>Entries held, across all groups.</summary>
        internal long EntryCount => Volatile.Read(ref _entryCount);

        /// <summary>Groups held, groups of more than one value, and the room the group map
        /// has: what the store's own structures cost.</summary>
        internal (int Groups, long ManyGroups, int Capacity) Shape => (_groups.Count, Volatile.Read(ref _manyGroups), _groups.EnsureCapacity(0));

        // Multi-valued groups appear and vanish too in a sliding workload. Their emptied
        // dictionaries are parked here (bounded, and empty, so this holds nothing but their
        // capacity) and handed back out to the next group that outgrows one value.
        private readonly Stack<Dictionary<TValue, long>> _spare = new();
        private const int MaxSpare = 32;

        public long Update(TKey key, TValue value, long delta) => Update(key, value, delta, out _);

        /// <summary><paramref name="many"/> is the group's dictionary when it has one and
        /// is still reachable through the map afterwards, for <see cref="ApplyAll"/>.</summary>
        private long Update(TKey key, TValue value, long delta, out Dictionary<TValue, long>? many)
        {
            ref var group = ref CollectionsMarshal.GetValueRefOrAddDefault(_groups, key, out var hadGroup);
            many = hadGroup ? group.Many : null;
            if (many is not null)
                return UpdateMany(key, many, value, delta, ref many);

            long updated;
            if (!hadGroup)
            {
                group.Single = value;
                updated = delta;
            }
            else if (EqualityComparer<TValue>.Default.Equals(group.Single, value))
            {
                updated = group.Weight + delta;
            }
            else
            {
                // A second value under the key: the group outgrows its inline slot.
                if (delta == 0)
                    return 0;
                many = _spare.Count > 0 ? _spare.Pop() : new Dictionary<TValue, long>();
                many.Add(group.Single, group.Weight);
                many.Add(value, delta);
                group = new Group { Many = many };
                _entryCount++;
                _manyGroups++;
                return delta;
            }

            // Written through the ref before any removal: removing the key clears the slot.
            group.Weight = updated;
            if (updated == 0)
            {
                _groups.Remove(key);
                if (hadGroup)
                    _entryCount--;
            }
            else if (!hadGroup)
                _entryCount++;
            return updated;
        }

        private long UpdateMany(TKey key, Dictionary<TValue, long> group, TValue value, long delta, ref Dictionary<TValue, long>? reachable)
        {
            ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(group, value, out var had);
            var updated = slot + delta;
            if (updated != 0)
            {
                slot = updated;
                if (!had)
                    _entryCount++;
                return updated;
            }

            group.Remove(value);
            if (had)
                _entryCount--;
            if (group.Count == 0)
            {
                _manyGroups--;
                _groups.Remove(key);
                if (_spare.Count < MaxSpare)
                    _spare.Push(group);
                reachable = null;
            }
            return 0;
        }

        /// <summary>Batched update that remembers the multi-valued group it last touched.
        /// Callers arrive with entries grouped by key far more often than not — a snapshot
        /// output writes every row of a step under one group, and the Min/Max tree emits its
        /// updates bucket by bucket — and for a key like a tournament-tree slot or a category
        /// string the group lookup is the expensive half of the update.</summary>
        public void ApplyAll(IReadOnlyList<(TKey Key, TValue Value, long Delta)> entries)
        {
            var comparer = EqualityComparer<TKey>.Default;
            var lastKey = default(TKey);
            Dictionary<TValue, long>? lastGroup = null;

            for (var i = 0; i < entries.Count; i++)
            {
                var (key, value, delta) = entries[i];
                if (lastGroup is not null && comparer.Equals(key, lastKey!))
                    UpdateMany(key, lastGroup, value, delta, ref lastGroup);
                else
                {
                    Update(key, value, delta, out lastGroup);
                    lastKey = key;
                }
            }
        }

        public IReadOnlyList<WeightedValue<TValue>> GetGroup(TKey key)
        {
            if (!_groups.TryGetValue(key, out var group))
                return [];
            if (group.Many is not { } many)
                return [new WeightedValue<TValue>(group.Single, group.Weight)];

            var result = new WeightedValue<TValue>[many.Count];
            var i = 0;
            foreach (var kv in many)
                result[i++] = new WeightedValue<TValue>(kv.Key, kv.Value);
            return result;
        }

        /// <summary>Reads whole groups without materializing one array per group: the entries
        /// go straight from the live groups into the caller's buffer, which the hot
        /// operators reuse across steps. Safe precisely because the contract is a copy —
        /// callers get values, not a view onto state that the next update would move.</summary>
        public void GetGroupsInto(IReadOnlyList<TKey> keys, List<WeightedValue<TValue>> into, List<int> bounds)
        {
            for (var i = 0; i < keys.Count; i++)
            {
                if (_groups.TryGetValue(keys[i], out var group))
                {
                    if (group.Many is { } many)
                    {
                        into.EnsureCapacity(into.Count + many.Count);
                        foreach (var kv in many)
                            into.Add(new WeightedValue<TValue>(kv.Key, kv.Value));
                    }
                    else
                    {
                        into.Add(new WeightedValue<TValue>(group.Single, group.Weight));
                    }
                }
                bounds.Add(into.Count);
            }
        }

        public void RemoveGroup(TKey key)
        {
            if (!_groups.Remove(key, out var group))
                return;
            if (group.Many is { } many)
            {
                _entryCount -= many.Count;
                _manyGroups--;
            }
            else
                _entryCount--;
        }

        public void RemoveEntries(IReadOnlyList<KeyedValue<TKey, TValue>> entries)
        {
            for (var i = 0; i < entries.Count; i++)
            {
                var (key, value) = entries[i];
                if (WeightOf(key, value) is var weight and not 0)
                    Update(key, value, -weight);
            }
        }

        /// <summary>The weight of one value under <paramref name="key"/>, 0 if absent.</summary>
        internal long WeightOf(TKey key, TValue value)
        {
            if (!_groups.TryGetValue(key, out var group))
                return 0;
            if (group.Many is { } many)
                return many.GetValueOrDefault(value);
            return EqualityComparer<TValue>.Default.Equals(group.Single, value) ? group.Weight : 0;
        }

        /// <summary>Makes the weight of (<paramref name="key"/>, <paramref name="value"/>)
        /// exactly <paramref name="weight"/>.</summary>
        internal void Set(TKey key, TValue value, long weight)
        {
            var delta = weight - WeightOf(key, value);
            if (delta != 0)
                Update(key, value, delta);
        }

        /// <summary>Appends <paramref name="key"/>'s group to <paramref name="into"/>.</summary>
        internal void CopyGroup(TKey key, List<WeightedValue<TValue>> into)
        {
            if (!_groups.TryGetValue(key, out var group))
                return;
            if (group.Many is { } many)
            {
                foreach (var kv in many)
                    into.Add(new WeightedValue<TValue>(kv.Key, kv.Value));
            }
            else
            {
                into.Add(new WeightedValue<TValue>(group.Single, group.Weight));
            }
        }

        /// <summary>Every (key, value, weight) held.</summary>
        internal IEnumerable<(TKey Key, TValue Value, long Weight)> Entries()
        {
            foreach (var (key, group) in _groups)
            {
                if (group.Many is { } many)
                {
                    foreach (var (value, weight) in many)
                        yield return (key, value, weight);
                }
                else
                {
                    yield return (key, group.Single, group.Weight);
                }
            }
        }

        internal void Clear()
        {
            _groups.Clear();
            _entryCount = 0;
            _manyGroups = 0;
        }
    }

    internal sealed class ValueStore<TKey, TValue> : IValueStore<TKey, TValue> where TKey : notnull
    {
        private readonly Dictionary<TKey, TValue> _values = new();

        /// <summary>Every key with its value.</summary>
        internal Dictionary<TKey, TValue> Entries => _values;

        public bool TryGet(TKey key, [MaybeNullWhen(false)] out TValue value) => _values.TryGetValue(key, out value);
        public void Put(TKey key, TValue value) => _values[key] = value;
        public void Remove(TKey key) => _values.Remove(key);
    }

    /// <summary>The deadlines as a map, for the lookups, and as a queue, for the order.
    /// The queue is never searched: an entry whose key has since moved or gone is stale,
    /// and is skipped when it reaches the front. Nothing has to take it for that to
    /// happen — a store that is only ever written (a follower's, which applies a leader's
    /// records and takes nothing) drops stale entries off the front at every removal, and
    /// rebuilds the queue from the map when stale entries outnumber the live ones — so
    /// the queue holds at most twice the map's entries and <see cref="Slack"/> more,
    /// whoever writes it and for however long.</summary>
    internal sealed class DeadlineStore<TKey> : IDeadlineStore<TKey> where TKey : notnull
    {
        /// <summary>Stale queue entries allowed beyond the number of live ones before
        /// the queue is rebuilt: a small store is not rebuilt at every other write.</summary>
        internal const int Slack = 64;

        private readonly Dictionary<TKey, long> _deadlines = new();
        private readonly PriorityQueue<TKey, long> _queue = new();

        /// <summary>Every key with its deadline.</summary>
        internal Dictionary<TKey, long> Entries => _deadlines;

        /// <summary>Entries in the queue, stale ones included.</summary>
        internal int QueueCount => _queue.Count;

        /// <summary>Entries the queue's array has room for: what it costs in memory.</summary>
        internal int QueueCapacity => _queue.EnsureCapacity(0);

        internal bool TryGet(TKey key, out long deadline) => _deadlines.TryGetValue(key, out deadline);

        internal void Clear()
        {
            _deadlines.Clear();
            _queue.Clear();
            _queue.TrimExcess();
        }

        public void AddAll(IReadOnlyList<TKey> keys, Func<TKey, long> deadlineOf)
        {
            for (var i = 0; i < keys.Count; i++)
            {
                var key = keys[i];
                if (_deadlines.ContainsKey(key))
                    continue;
                var deadline = deadlineOf(key);
                _deadlines.Add(key, deadline);
                _queue.Enqueue(key, deadline);
            }
        }

        public void Set(TKey key, long deadline)
        {
            ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(_deadlines, key, out var held);
            if (held && slot == deadline)
                return;
            slot = deadline;
            _queue.Enqueue(key, deadline);

            // A key that moved left its old entry behind.
            if (held)
                Compact();
        }

        public void Remove(TKey key)
        {
            if (_deadlines.Remove(key))
                Compact();
        }

        /// <summary>Keeps the queue within its bound after a write that left an entry
        /// stale. Keys mostly go in the order of their deadlines (a leader takes what is
        /// due and a follower applies the tombstones), so the stale entry is usually at
        /// the front and goes at once; one that is not waits there, and when such entries
        /// outnumber the live ones the queue is built again from the map — a cost in
        /// proportion to the writes that made them stale.</summary>
        private void Compact()
        {
            while (_queue.TryPeek(out var front, out var deadline)
                   && !(_deadlines.TryGetValue(front, out var current) && current == deadline))
                _queue.Dequeue();

            if (_queue.Count <= 2 * (long)_deadlines.Count + Slack)
                return;
            _queue.Clear();
            foreach (var (key, deadline) in _deadlines)
                _queue.Enqueue(key, deadline);
            if (_queue.EnsureCapacity(0) > 2 * (long)_queue.Count + Slack)
                _queue.TrimExcess();
        }

        public void TakeDue(long upTo, List<TKey> into) => TakeDue(upTo, into, null);

        /// <summary><see cref="TakeDue(long, List{TKey})"/>, also appending each key's
        /// deadline to <paramref name="deadlines"/>: what a store that can undo needs.</summary>
        internal void TakeDue(long upTo, List<TKey> into, List<long>? deadlines)
        {
            while (_queue.TryPeek(out var key, out var deadline) && deadline <= upTo)
            {
                _queue.Dequeue();
                // Stale unless it is still the key's deadline: the key moved, or went, after
                // this entry was queued. (Queued twice under one deadline, the first entry
                // takes the key and the second finds it gone.)
                if (_deadlines.TryGetValue(key, out var current) && current == deadline)
                {
                    _deadlines.Remove(key);
                    into.Add(key);
                    deadlines?.Add(deadline);
                }
            }
        }
    }
}
