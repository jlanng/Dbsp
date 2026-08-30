using System.Diagnostics.CodeAnalysis;

namespace Incremental.Storage;

/// <summary>
/// Heap-backed checkpointing storage: committed state and staged (uncommitted) writes live
/// in separate layers, <see cref="Commit"/> promotes the staged layer, and stores are
/// remembered by name — so a circuit rebuilt against the same provider instance resumes
/// from the committed state, exactly like the Redis provider across a process restart.
/// An in-process stand-in for testing checkpoint/restart semantics without a Redis.
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
                    ?? throw new InvalidOperationException($"Store '{name}' was already created with a different shape.");
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
    }
}
