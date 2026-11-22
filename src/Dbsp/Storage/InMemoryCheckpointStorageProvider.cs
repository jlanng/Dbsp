using System.Diagnostics.CodeAnalysis;

namespace Dbsp.Storage;

/// <summary>
/// Heap-backed checkpointing storage: committed state and staged (uncommitted) writes live
/// in separate layers, <see cref="Commit"/> promotes the staged layer, and stores are
/// remembered by name — so a circuit rebuilt against the same provider instance resumes
/// from the committed state, exactly like the Redis provider across a process restart.
/// An in-process stand-in for testing checkpoint/restart semantics without Redis.
/// </summary>
public sealed class InMemoryCheckpointStorageProvider : ICheckpointStorageProvider
{
    private readonly Dictionary<string, IStagedStore> _stores = new();
    private readonly Dictionary<string, string> _stagedManifests = new();
    private readonly Dictionary<string, string> _committedManifests = new();
    private Dictionary<string, long> _committedOffsets = new();

    public IWeightStore<T> CreateWeightStore<T>(string name) where T : notnull =>
        GetOrCreate(name, static () => new StagedWeightStore<T>());

    public IIndexedWeightStore<TKey, TValue> CreateIndexedWeightStore<TKey, TValue>(string name)
        where TKey : notnull
        where TValue : notnull =>
        GetOrCreate(name, static () => new StagedIndexedWeightStore<TKey, TValue>());

    public IValueStore<TKey, TValue> CreateValueStore<TKey, TValue>(string name) where TKey : notnull =>
        GetOrCreate(name, static () => new StagedValueStore<TKey, TValue>());

    public IDeadlineStore<TKey> CreateDeadlineStore<TKey>(string name) where TKey : notnull =>
        GetOrCreate(name, static () => new StagedDeadlineStore<TKey>());

    public IReadOnlyDictionary<string, long> Recover()
    {
        lock (_stores)
        {
            foreach (var store in _stores.Values)
                store.Rollback();
            _stagedManifests.Clear();
            return new Dictionary<string, long>(_committedOffsets);
        }
    }

    public void Commit(IReadOnlyDictionary<string, long> inputOffsets)
    {
        lock (_stores)
        {
            foreach (var store in _stores.Values)
                store.Promote();
            foreach (var (scope, manifest) in _stagedManifests)
                _committedManifests[scope] = manifest;
            _stagedManifests.Clear();
            _committedOffsets = new Dictionary<string, long>(inputOffsets);
        }
    }

    /// <summary>The offsets of the last commit, as a copy.</summary>
    internal IReadOnlyDictionary<string, long> CommittedOffsets
    {
        get
        {
            lock (_stores)
                return new Dictionary<string, long>(_committedOffsets);
        }
    }

    /// <summary>How many entries the committed state holds, across every store: the size
    /// of the state, for a test that asks whether it grows.</summary>
    internal long CommittedEntries
    {
        get
        {
            lock (_stores)
                return _stores.Values.Sum(store => store.CommittedCount);
        }
    }

    /// <summary>Entries the committed state holds, store by store (deadline stores
    /// included): what a test reads to ask whether a pipeline's state returns to a bound.</summary>
    public IReadOnlyDictionary<string, long> CommittedEntriesByStore()
    {
        lock (_stores)
            return _stores.ToDictionary(store => store.Key, store => store.Value.CommittedCount, StringComparer.Ordinal);
    }

    public string? CommittedManifest(string scope)
    {
        lock (_stores)
            return _committedManifests.GetValueOrDefault(scope);
    }

    public void StageManifest(string scope, string manifest)
    {
        lock (_stores)
            _stagedManifests[scope] = manifest;
    }

    private TStore GetOrCreate<TStore>(string name, Func<TStore> factory) where TStore : class, IStagedStore
    {
        lock (_stores)
        {
            if (_stores.TryGetValue(name, out var existing))
                return existing as TStore
                    ?? throw new InvalidOperationException($"Store '{name}' was already created with different key/value types.");
            var store = factory();
            _stores[name] = store;
            return store;
        }
    }

    /// <summary>Promote applies the staged layer to the committed one (a commit); Rollback
    /// discards it (recovery after a crash between commits).</summary>
    private interface IStagedStore
    {
        void Promote();
        void Rollback();

        /// <summary>Entries in the committed layer.</summary>
        long CommittedCount { get; }
    }

    private sealed class StagedWeightStore<T> : IWeightStore<T>, IStagedStore where T : notnull
    {
        private readonly Dictionary<T, long> _committed = new();
        private readonly Dictionary<T, long> _staged = new(); // absolute weights; 0 = deleted

        public long Update(T value, long delta)
        {
            var updated = WeightOf(value) + delta;
            _staged[value] = updated;
            return updated;
        }

        public long WeightOf(T value) =>
            _staged.TryGetValue(value, out var staged) ? staged : _committed.GetValueOrDefault(value);

        public void Remove(T value) => _staged[value] = 0;

        public void Promote()
        {
            foreach (var (value, weight) in _staged)
            {
                if (weight == 0)
                    _committed.Remove(value);
                else
                    _committed[value] = weight;
            }
            _staged.Clear();
        }

        public void Rollback() => _staged.Clear();

        public long CommittedCount => _committed.Count;
    }

    private sealed class StagedIndexedWeightStore<TKey, TValue> : IIndexedWeightStore<TKey, TValue>, IStagedStore
        where TKey : notnull
        where TValue : notnull
    {
        private readonly Dictionary<TKey, Dictionary<TValue, long>> _committed = new();
        private readonly Dictionary<TKey, StagedGroup> _staged = new();

        private sealed class StagedGroup
        {
            public bool Cleared;                                // RemoveGroup: committed entries are gone
            public readonly Dictionary<TValue, long> Values = new(); // absolute weights; 0 = deleted
        }

        public long Update(TKey key, TValue value, long delta)
        {
            var group = GroupFor(key);
            var updated = (group.Values.TryGetValue(value, out var staged)
                ? staged
                : CommittedWeight(key, value, group.Cleared)) + delta;
            group.Values[value] = updated;
            return updated;
        }

        public IReadOnlyList<WeightedValue<TValue>> GetGroup(TKey key)
        {
            _staged.TryGetValue(key, out var group);
            var merged = new Dictionary<TValue, long>();
            if (group is not { Cleared: true } && _committed.TryGetValue(key, out var committed))
                foreach (var (value, weight) in committed)
                    merged[value] = weight;
            if (group is not null)
            {
                foreach (var (value, weight) in group.Values)
                {
                    if (weight == 0)
                        merged.Remove(value);
                    else
                        merged[value] = weight;
                }
            }

            if (merged.Count == 0)
                return [];
            var result = new WeightedValue<TValue>[merged.Count];
            var i = 0;
            foreach (var (value, weight) in merged)
                result[i++] = new WeightedValue<TValue>(value, weight);
            return result;
        }

        public void RemoveGroup(TKey key)
        {
            var group = GroupFor(key);
            group.Cleared = true;
            group.Values.Clear();
        }

        public void RemoveEntries(IReadOnlyList<KeyedValue<TKey, TValue>> entries)
        {
            for (var i = 0; i < entries.Count; i++)
            {
                var (key, value) = entries[i];
                _staged.TryGetValue(key, out var group);
                var weight = group is not null && group.Values.TryGetValue(value, out var staged)
                    ? staged
                    : CommittedWeight(key, value, group?.Cleared ?? false);
                if (weight != 0)
                    Update(key, value, -weight);
            }
        }

        public void Promote()
        {
            foreach (var (key, group) in _staged)
            {
                if (group.Cleared)
                    _committed.Remove(key);
                if (group.Values.Count == 0)
                    continue;

                if (!_committed.TryGetValue(key, out var committed))
                    _committed[key] = committed = new Dictionary<TValue, long>();
                foreach (var (value, weight) in group.Values)
                {
                    if (weight == 0)
                        committed.Remove(value);
                    else
                        committed[value] = weight;
                }
                if (committed.Count == 0)
                    _committed.Remove(key);
            }
            _staged.Clear();
        }

        public void Rollback() => _staged.Clear();

        public long CommittedCount => _committed.Values.Sum(group => (long)group.Count);

        private StagedGroup GroupFor(TKey key) =>
            _staged.TryGetValue(key, out var group) ? group : _staged[key] = new StagedGroup();

        private long CommittedWeight(TKey key, TValue value, bool cleared) =>
            !cleared && _committed.TryGetValue(key, out var committed) ? committed.GetValueOrDefault(value) : 0;
    }

    private sealed class StagedValueStore<TKey, TValue> : IValueStore<TKey, TValue>, IStagedStore where TKey : notnull
    {
        private readonly Dictionary<TKey, TValue> _committed = new();
        private readonly Dictionary<TKey, (bool Exists, TValue? Value)> _staged = new();

        public bool TryGet(TKey key, [MaybeNullWhen(false)] out TValue value)
        {
            if (_staged.TryGetValue(key, out var staged))
            {
                value = staged.Value!;
                return staged.Exists;
            }
            return _committed.TryGetValue(key, out value);
        }

        public void Put(TKey key, TValue value) => _staged[key] = (true, value);

        public void Remove(TKey key) => _staged[key] = (false, default);

        public void Promote()
        {
            foreach (var (key, (exists, value)) in _staged)
            {
                if (exists)
                    _committed[key] = value!;
                else
                    _committed.Remove(key);
            }
            _staged.Clear();
        }

        public void Rollback() => _staged.Clear();

        public long CommittedCount => _committed.Count;
    }

    /// <summary>Written straight through to the live deadlines, with the deadline each
    /// touched key had at the last commit kept beside them: a commit forgets those, a
    /// rollback puts them back. (The other stores layer staged over committed; an ordered
    /// store would have to merge two orders on every read.)</summary>
    private sealed class StagedDeadlineStore<TKey> : IDeadlineStore<TKey>, IStagedStore where TKey : notnull
    {
        private readonly InMemoryStorageProvider.DeadlineStore<TKey> _live = new();
        private readonly Dictionary<TKey, (bool Held, long Deadline)> _before = new();
        private readonly List<long> _dueDeadlines = [];

        public void AddAll(IReadOnlyList<TKey> keys, Func<TKey, long> deadlineOf)
        {
            for (var i = 0; i < keys.Count; i++)
                if (!_live.TryGet(keys[i], out _))
                    Set(keys[i], deadlineOf(keys[i]));
        }

        public void Set(TKey key, long deadline)
        {
            Touch(key);
            _live.Set(key, deadline);
        }

        public void Remove(TKey key)
        {
            Touch(key);
            _live.Remove(key);
        }

        public void TakeDue(long upTo, List<TKey> into)
        {
            var first = into.Count;
            _dueDeadlines.Clear();
            _live.TakeDue(upTo, into, _dueDeadlines);
            for (var i = first; i < into.Count; i++)
                _before.TryAdd(into[i], (true, _dueDeadlines[i - first]));
        }

        private void Touch(TKey key)
        {
            if (_before.ContainsKey(key))
                return;
            var held = _live.TryGet(key, out var deadline);
            _before.Add(key, (held, deadline));
        }

        public void Promote() => _before.Clear();

        /// <summary>Counts the live deadlines: the committed ones once nothing is staged,
        /// which is when a test reads it.</summary>
        public long CommittedCount => _live.Entries.Count;

        public void Rollback()
        {
            foreach (var (key, (held, deadline)) in _before)
            {
                if (held)
                    _live.Set(key, deadline);
                else
                    _live.Remove(key);
            }
            _before.Clear();
        }
    }
}
