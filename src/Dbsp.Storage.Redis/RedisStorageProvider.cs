using System.Diagnostics.CodeAnalysis;
using StackExchange.Redis;
using Dbsp.Storage;

namespace Dbsp.Storage.Redis;

public sealed class RedisStorageOptions
{
    /// <summary>Prefix for every Redis key this provider writes. Required, and must be
    /// unique per circuit: store names inside a prefix are positional (node/worker ids),
    /// so two circuits sharing one prefix silently interleave their state.</summary>
    public required string KeyPrefix { get; init; }

    /// <summary>Redis database number, or -1 for the connection default.</summary>
    public int Database { get; init; } = -1;

    /// <summary>Serializes keys and values into the Redis hashes. This is a persisted format —
    /// what one run writes, the next run reads back — so it must stay stable across versions;
    /// the default is JSON.</summary>
    public IStateSerializer Serializer { get; init; } = JsonStateSerializer.Instance;
}

/// <summary>
/// Keeps operator state in Redis hashes, which makes it survive process restarts and lets
/// state grow beyond the heap — and checkpoints it: writes are staged in per-store overlays
/// (reads merge the overlay over committed state) and <see cref="Commit"/> flushes every
/// staged write plus the input offsets it reflects in a single MULTI/EXEC transaction, so
/// durable state and offsets always agree, even across a crash mid-commit. Flushed weights
/// are absolute (HSET, not HINCRBY), so a commit can never half-apply or double-apply.
///
/// Layout (all under <see cref="RedisStorageOptions.KeyPrefix"/>):
///   weight store       {prefix}:{store}            hash: serialized value → weight
///   indexed store      {prefix}:{store}:{key}      hash: serialized value → weight, one hash per group
///   value store        {prefix}:{store}            hash: serialized key → serialized value
///   deadline store     {prefix}:{store}            hash: serialized key → deadline (UTC ticks)
///                      {prefix}:{store}:due        sorted set: serialized key, scored by deadline / 1024
///   checkpoint         {prefix}:checkpoint         hash: input name → committed offset
///   manifest           {prefix}:manifest           hash: scope (process) → committed state manifest
///
/// Store names already encode the node and worker id, so concurrent workers never share a
/// store. Each batch operation costs at most one pipelined round trip (HMGET/HGETALL for the
/// committed values the overlay does not hold); overlays are dropped once committed, so
/// memory is bounded by the state touched between checkpoints.
/// </summary>
public sealed class RedisStorageProvider : ICheckpointStorageProvider
{
    private readonly IDatabase _db;
    private readonly RedisStorageOptions _options;
    private readonly string _checkpointKey;
    private readonly string _manifestKey;
    private readonly Dictionary<string, IStagedStore> _stores = [];
    private readonly Dictionary<string, string> _stagedManifests = [];

    public RedisStorageProvider(IConnectionMultiplexer connection, RedisStorageOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (string.IsNullOrEmpty(options.KeyPrefix) || options.KeyPrefix.Any(char.IsWhiteSpace))
            throw new ArgumentException(
                "KeyPrefix must be a non-empty string without whitespace, and unique per circuit sharing the Redis instance.",
                nameof(options));
        _options = options;
        _db = connection.GetDatabase(_options.Database);
        _checkpointKey = $"{_options.KeyPrefix}:checkpoint";
        _manifestKey = $"{_options.KeyPrefix}:manifest";
    }

    public IWeightStore<T> CreateWeightStore<T>(string name) where T : notnull =>
        GetOrCreate(name, () => new RedisWeightStore<T>(_db, $"{_options.KeyPrefix}:{name}", _options.Serializer));

    public IIndexedWeightStore<TKey, TValue> CreateIndexedWeightStore<TKey, TValue>(string name)
        where TKey : notnull
        where TValue : notnull =>
        GetOrCreate(name, () => new RedisIndexedWeightStore<TKey, TValue>(_db, $"{_options.KeyPrefix}:{name}", _options.Serializer));

    public IValueStore<TKey, TValue> CreateValueStore<TKey, TValue>(string name) where TKey : notnull =>
        GetOrCreate(name, () => new RedisValueStore<TKey, TValue>(_db, $"{_options.KeyPrefix}:{name}", _options.Serializer));

    public IDeadlineStore<TKey> CreateDeadlineStore<TKey>(string name) where TKey : notnull =>
        GetOrCreate(name, () => new RedisDeadlineStore<TKey>(_db, $"{_options.KeyPrefix}:{name}", _options.Serializer));

    public IReadOnlyDictionary<string, long> Recover()
    {
        lock (_stores)
        {
            foreach (var store in _stores.Values)
                store.Discard();
            _stagedManifests.Clear();

            var entries = _db.HashGetAll(_checkpointKey);
            var offsets = new Dictionary<string, long>(entries.Length);
            foreach (var entry in entries)
                offsets[entry.Name!] = (long)entry.Value;
            return offsets;
        }
    }

    public void Commit(IReadOnlyDictionary<string, long> inputOffsets)
    {
        lock (_stores)
        {
            var (transaction, pending) = BuildCommit(inputOffsets);

            // MULTI/EXEC: the commands travel as one unit and Redis applies them on EXEC,
            // so a crash anywhere before that leaves the previous checkpoint fully intact.
            if (!transaction.Execute())
                throw CommitRejected();

            // EXEC does not roll back on per-command errors (e.g. WRONGTYPE against a
            // colliding key), so a true Execute() is not enough: every queued command must
            // have succeeded, or the checkpoint must not be reported committed.
            try
            {
                Task.WaitAll([.. pending]);
            }
            catch (AggregateException ex)
            {
                throw CommandsFailed(ex.InnerExceptions.Count == 1 ? ex.InnerException! : ex);
            }

            CompleteCommit();
        }
    }

    /// <summary>The same commit without holding a thread across the round trip. The circuit
    /// runs no step while a commit is pending, so nothing stages into the stores between
    /// building the transaction and discarding the overlays; the lock is taken only around
    /// those two ends (a monitor cannot be held across an await).</summary>
    public async ValueTask CommitAsync(IReadOnlyDictionary<string, long> inputOffsets, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        ITransaction transaction;
        List<Task> pending;
        lock (_stores)
            (transaction, pending) = BuildCommit(inputOffsets);

        // Not cancellable from here: once EXEC is on the wire the outcome is Redis's, and
        // abandoning the wait would leave the overlays out of step with what was committed.
        if (!await transaction.ExecuteAsync().ConfigureAwait(false))
            throw CommitRejected();

        try
        {
            await Task.WhenAll(pending).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            throw CommandsFailed(ex);
        }

        lock (_stores)
            CompleteCommit();
    }

    /// <summary>Queues every staged write, the offsets marker and the staged manifests into
    /// one transaction. Caller holds the lock.</summary>
    private (ITransaction Transaction, List<Task> Pending) BuildCommit(IReadOnlyDictionary<string, long> inputOffsets)
    {
        var transaction = _db.CreateTransaction();
        var pending = new List<Task>();
        foreach (var store in _stores.Values)
            store.Flush(transaction, pending);

        if (inputOffsets.Count > 0)
        {
            var marker = new HashEntry[inputOffsets.Count];
            var i = 0;
            foreach (var (input, offset) in inputOffsets)
                marker[i++] = new HashEntry(input, offset);
            pending.Add(transaction.HashSetAsync(_checkpointKey, marker));
        }

        // The staged manifests ride the same transaction, so a committed manifest always
        // describes committed state; once landed they need no rewriting (cleared after).
        foreach (var (scope, manifest) in _stagedManifests)
            pending.Add(transaction.HashSetAsync(_manifestKey, scope, manifest));
        return (transaction, pending);
    }

    /// <summary>The overlays' content is now the committed state. Caller holds the lock.</summary>
    private void CompleteCommit()
    {
        foreach (var store in _stores.Values)
            store.Discard();
        _stagedManifests.Clear();
    }

    private static InvalidOperationException CommitRejected() => new(
        "The checkpoint commit transaction was rejected by Redis; nothing was applied, " +
        "so the previous checkpoint is intact.");

    private static InvalidOperationException CommandsFailed(Exception inner) => new(
        "The checkpoint commit transaction executed, but one or more of its commands failed; " +
        "the committed checkpoint may be incomplete and must not be trusted.",
        inner);

    public string? CommittedManifest(string scope)
    {
        lock (_stores)
        {
            var stored = _db.HashGet(_manifestKey, scope);
            return stored.IsNull ? null : (string?)stored;
        }
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

    /// <summary>Staged-overlay store: <see cref="Flush"/> queues every staged write into the
    /// commit transaction; <see cref="Discard"/> drops the overlay and read cache — after a
    /// successful commit (the overlay has become committed state) and on
    /// <see cref="RedisStorageProvider.Recover"/> (rolling back a crashed run).</summary>
    private interface IStagedStore
    {
        void Flush(ITransaction transaction, List<Task> pending);
        void Discard();
    }

    private sealed class RedisWeightStore<T>(IDatabase db, string key, IStateSerializer serializer)
        : IWeightStore<T>, IStagedStore where T : notnull
    {
        // Absolute weights staged since the last commit (0 = delete at flush), and committed
        // weights read from Redis this interval (0 = known absent). Lookups go staged →
        // cache → HGET; both maps are dropped on commit, bounding them to the interval.
        private readonly Dictionary<RedisValue, long> _staged = [];
        private readonly Dictionary<RedisValue, long> _cache = [];

        public long Update(T value, long delta)
        {
            var field = Field(value);
            var updated = WeightOf(field) + delta;
            _staged[field] = updated;
            return updated;
        }

        public long WeightOf(T value) => WeightOf(Field(value));

        public void Remove(T value) => _staged[Field(value)] = 0;

        public long[] UpdateAll(IReadOnlyList<(T Value, long Delta)> entries)
        {
            if (entries.Count == 0)
                return [];

            var fields = new RedisValue[entries.Count];
            for (var i = 0; i < entries.Count; i++)
                fields[i] = Field(entries[i].Value);
            Prefetch(fields);

            var results = new long[entries.Count];
            for (var i = 0; i < entries.Count; i++)
            {
                // Sequential local arithmetic: a duplicate value whose running weight
                // passes through zero within the batch resolves correctly by construction.
                results[i] = WeightOf(fields[i]) + entries[i].Delta;
                _staged[fields[i]] = results[i];
            }
            return results;
        }

        public long[] WeightOfAll(IReadOnlyList<T> values)
        {
            if (values.Count == 0)
                return [];

            var fields = new RedisValue[values.Count];
            for (var i = 0; i < values.Count; i++)
                fields[i] = Field(values[i]);
            Prefetch(fields);

            var results = new long[values.Count];
            for (var i = 0; i < values.Count; i++)
                results[i] = WeightOf(fields[i]);
            return results;
        }

        public void RemoveAll(IReadOnlyList<T> values)
        {
            foreach (var value in values)
                Remove(value);
        }

        public void Flush(ITransaction transaction, List<Task> pending)
        {
            List<RedisValue>? deletes = null;
            List<HashEntry>? sets = null;
            foreach (var (field, weight) in _staged)
            {
                if (weight == 0)
                    (deletes ??= []).Add(field);
                else
                    (sets ??= []).Add(new HashEntry(field, weight));
            }
            if (deletes is not null)
                pending.Add(transaction.HashDeleteAsync(key, [.. deletes]));
            if (sets is not null)
                pending.Add(transaction.HashSetAsync(key, [.. sets]));
        }

        public void Discard()
        {
            _staged.Clear();
            _cache.Clear();
        }

        private RedisValue Field(T value) => serializer.Serialize(value);

        private long WeightOf(RedisValue field)
        {
            if (_staged.TryGetValue(field, out var staged))
                return staged;
            if (_cache.TryGetValue(field, out var cached))
                return cached;
            var stored = db.HashGet(key, field);
            return _cache[field] = stored.IsNull ? 0 : (long)stored;
        }

        /// <summary>One HMGET round trip for the batch's fields whose committed weight is unknown.</summary>
        private void Prefetch(RedisValue[] fields)
        {
            HashSet<RedisValue>? seen = null;
            List<RedisValue>? missing = null;
            foreach (var field in fields)
                if (!_staged.ContainsKey(field) && !_cache.ContainsKey(field) && (seen ??= []).Add(field))
                    (missing ??= []).Add(field);
            if (missing is null)
                return;

            var stored = db.HashGet(key, [.. missing]);
            for (var i = 0; i < missing.Count; i++)
                _cache[missing[i]] = stored[i].IsNull ? 0 : (long)stored[i];
        }
    }

    private sealed class RedisIndexedWeightStore<TKey, TValue>(IDatabase db, string baseKey, IStateSerializer serializer)
        : IIndexedWeightStore<TKey, TValue>, IStagedStore
        where TKey : notnull
        where TValue : notnull
    {
        private readonly Dictionary<RedisKey, Group> _groups = [];

        /// <summary>Per-group overlay over the committed hash. Reads check Staged, then the
        /// Cleared tombstone, then Cache (FullyCached marks it complete), then Redis.</summary>
        private sealed class Group
        {
            public bool Cleared;                                       // RemoveGroup: committed entries are gone
            public readonly Dictionary<RedisValue, long> Staged = [];  // absolute weights; 0 = delete
            public readonly Dictionary<RedisValue, long> Cache = [];   // committed weights read so far (0 = absent)
            public bool FullyCached;                                   // Cache holds the whole committed group
        }

        public long Update(TKey key, TValue value, long delta)
        {
            var groupKey = GroupKey(key);
            var group = GroupFor(groupKey);
            var field = (RedisValue)serializer.Serialize(value);
            var updated = WeightOf(groupKey, group, field) + delta;
            group.Staged[field] = updated;
            return updated;
        }

        public IReadOnlyList<WeightedValue<TValue>> GetGroup(TKey key)
        {
            var groupKey = GroupKey(key);
            var group = GroupFor(groupKey);
            if (!group.Cleared && !group.FullyCached)
            {
                foreach (var entry in db.HashGetAll(groupKey))
                    group.Cache[entry.Name] = (long)entry.Value;
                group.FullyCached = true;
            }
            return Materialize(group);
        }

        public void RemoveGroup(TKey key)
        {
            var group = GroupFor(GroupKey(key));
            group.Cleared = true;
            group.Staged.Clear();
            group.Cache.Clear();
        }

        public long[] UpdateAll(IReadOnlyList<(TKey Key, TValue Value, long Delta)> entries)
        {
            if (entries.Count == 0)
                return [];

            var addresses = new (RedisKey GroupKey, Group Group, RedisValue Field)[entries.Count];
            for (var i = 0; i < entries.Count; i++)
            {
                var groupKey = GroupKey(entries[i].Key);
                addresses[i] = (groupKey, GroupFor(groupKey), serializer.Serialize(entries[i].Value));
            }
            Prefetch(addresses);

            var results = new long[entries.Count];
            for (var i = 0; i < entries.Count; i++)
            {
                var (groupKey, group, field) = addresses[i];
                results[i] = WeightOf(groupKey, group, field) + entries[i].Delta;
                group.Staged[field] = results[i];
            }
            return results;
        }

        public IReadOnlyList<WeightedValue<TValue>>[] GetGroups(IReadOnlyList<TKey> keys)
        {
            if (keys.Count == 0)
                return [];

            var groups = new Group[keys.Count];
            HashSet<Group>? loading = null;
            List<(Group Group, Task<HashEntry[]> Entries)>? loads = null;
            IBatch? batch = null;
            for (var i = 0; i < keys.Count; i++)
            {
                var groupKey = GroupKey(keys[i]);
                var group = GroupFor(groupKey);
                groups[i] = group;
                if (group.Cleared || group.FullyCached || !(loading ??= []).Add(group))
                    continue;
                batch ??= db.CreateBatch();
                (loads ??= []).Add((group, batch.HashGetAllAsync(groupKey)));
            }

            if (loads is not null)
            {
                batch!.Execute();
                Task.WaitAll(loads.Select(l => (Task)l.Entries).ToArray());
                foreach (var (group, entries) in loads)
                {
                    foreach (var entry in entries.Result)
                        group.Cache[entry.Name] = (long)entry.Value;
                    group.FullyCached = true;
                }
            }

            var results = new IReadOnlyList<WeightedValue<TValue>>[keys.Count];
            for (var i = 0; i < keys.Count; i++)
                results[i] = Materialize(groups[i]);
            return results;
        }

        public void RemoveGroups(IReadOnlyList<TKey> keys)
        {
            foreach (var key in keys)
                RemoveGroup(key);
        }

        public void Flush(ITransaction transaction, List<Task> pending)
        {
            foreach (var (groupKey, group) in _groups)
            {
                // A cleared group is deleted wholesale; staged survivors are re-asserted
                // after it (MULTI preserves command order).
                if (group.Cleared)
                    pending.Add(transaction.KeyDeleteAsync(groupKey));

                List<RedisValue>? deletes = null;
                List<HashEntry>? sets = null;
                foreach (var (field, weight) in group.Staged)
                {
                    if (weight == 0)
                        (deletes ??= []).Add(field);
                    else
                        (sets ??= []).Add(new HashEntry(field, weight));
                }
                if (!group.Cleared && deletes is not null)
                    pending.Add(transaction.HashDeleteAsync(groupKey, [.. deletes]));
                if (sets is not null)
                    pending.Add(transaction.HashSetAsync(groupKey, [.. sets]));
            }
        }

        public void Discard() => _groups.Clear();

        private RedisKey GroupKey(TKey key) => $"{baseKey}:{serializer.Serialize(key)}";

        private Group GroupFor(RedisKey groupKey) =>
            _groups.TryGetValue(groupKey, out var group) ? group : _groups[groupKey] = new Group();

        private long WeightOf(RedisKey groupKey, Group group, RedisValue field)
        {
            if (group.Staged.TryGetValue(field, out var staged))
                return staged;
            if (group.Cleared)
                return 0;
            if (group.Cache.TryGetValue(field, out var cached))
                return cached;
            if (group.FullyCached)
                return 0;
            var stored = db.HashGet(groupKey, field);
            return group.Cache[field] = stored.IsNull ? 0 : (long)stored;
        }

        /// <summary>One pipelined round trip (HMGET per touched group) for the committed
        /// weights the batch needs but neither the overlay nor the cache holds.</summary>
        private void Prefetch((RedisKey GroupKey, Group Group, RedisValue Field)[] addresses)
        {
            Dictionary<RedisKey, (Group Group, List<RedisValue> Fields, HashSet<RedisValue> Seen)>? missing = null;
            foreach (var (groupKey, group, field) in addresses)
            {
                if (group.Staged.ContainsKey(field) || group.Cleared || group.FullyCached || group.Cache.ContainsKey(field))
                    continue;
                missing ??= [];
                if (!missing.TryGetValue(groupKey, out var pending))
                    missing[groupKey] = pending = (group, [], []);
                if (pending.Seen.Add(field))
                    pending.Fields.Add(field);
            }
            if (missing is null)
                return;

            var batch = db.CreateBatch();
            var reads = new List<(Group Group, List<RedisValue> Fields, Task<RedisValue[]> Stored)>(missing.Count);
            foreach (var (groupKey, (group, fields, _)) in missing)
                reads.Add((group, fields, batch.HashGetAsync(groupKey, [.. fields])));
            batch.Execute();
            Task.WaitAll(reads.Select(r => (Task)r.Stored).ToArray());

            foreach (var (group, fields, stored) in reads)
                for (var i = 0; i < fields.Count; i++)
                    group.Cache[fields[i]] = stored.Result[i].IsNull ? 0 : (long)stored.Result[i];
        }

        private IReadOnlyList<WeightedValue<TValue>> Materialize(Group group)
        {
            var merged = new Dictionary<RedisValue, long>();
            if (!group.Cleared)
                foreach (var (field, weight) in group.Cache)
                    if (weight != 0)
                        merged[field] = weight;
            foreach (var (field, weight) in group.Staged)
            {
                if (weight == 0)
                    merged.Remove(field);
                else
                    merged[field] = weight;
            }

            if (merged.Count == 0)
                return [];
            var result = new WeightedValue<TValue>[merged.Count];
            var i = 0;
            foreach (var (field, weight) in merged)
                result[i++] = new WeightedValue<TValue>(serializer.Deserialize<TValue>(field!), weight);
            return result;
        }
    }

    private sealed class RedisValueStore<TKey, TValue>(IDatabase db, string key, IStateSerializer serializer)
        : IValueStore<TKey, TValue>, IStagedStore where TKey : notnull
    {
        // Serialized values staged since the last commit (Null = remove at flush), and
        // committed values read from Redis this interval (Null = known absent).
        private readonly Dictionary<RedisValue, RedisValue> _staged = [];
        private readonly Dictionary<RedisValue, RedisValue> _cache = [];

        public bool TryGet(TKey k, [MaybeNullWhen(false)] out TValue value)
        {
            var field = Field(k);
            if (!_staged.TryGetValue(field, out var stored) && !_cache.TryGetValue(field, out stored))
                stored = _cache[field] = db.HashGet(key, field);
            if (stored.IsNull)
            {
                value = default;
                return false;
            }
            value = serializer.Deserialize<TValue>(stored!);
            return true;
        }

        public void Put(TKey k, TValue value) => _staged[Field(k)] = serializer.Serialize(value);

        public void Remove(TKey k) => _staged[Field(k)] = RedisValue.Null;

        public (bool Found, TValue? Value)[] TryGetAll(IReadOnlyList<TKey> keys)
        {
            if (keys.Count == 0)
                return [];

            var fields = new RedisValue[keys.Count];
            for (var i = 0; i < keys.Count; i++)
                fields[i] = Field(keys[i]);

            HashSet<RedisValue>? seen = null;
            List<RedisValue>? missing = null;
            foreach (var field in fields)
                if (!_staged.ContainsKey(field) && !_cache.ContainsKey(field) && (seen ??= []).Add(field))
                    (missing ??= []).Add(field);
            if (missing is not null)
            {
                var stored = db.HashGet(key, [.. missing]);
                for (var i = 0; i < missing.Count; i++)
                    _cache[missing[i]] = stored[i];
            }

            var results = new (bool, TValue?)[keys.Count];
            for (var i = 0; i < keys.Count; i++)
            {
                if (!_staged.TryGetValue(fields[i], out var stored))
                    stored = _cache[fields[i]];
                results[i] = stored.IsNull ? (false, default) : (true, serializer.Deserialize<TValue>(stored!));
            }
            return results;
        }

        public void PutAll(IReadOnlyList<(TKey Key, TValue Value)> pairs)
        {
            foreach (var (k, value) in pairs)
                Put(k, value);
        }

        public void RemoveAll(IReadOnlyList<TKey> keys)
        {
            foreach (var k in keys)
                Remove(k);
        }

        public void Flush(ITransaction transaction, List<Task> pending)
        {
            List<RedisValue>? deletes = null;
            List<HashEntry>? sets = null;
            foreach (var (field, value) in _staged)
            {
                if (value.IsNull)
                    (deletes ??= []).Add(field);
                else
                    (sets ??= []).Add(new HashEntry(field, value));
            }
            if (deletes is not null)
                pending.Add(transaction.HashDeleteAsync(key, [.. deletes]));
            if (sets is not null)
                pending.Add(transaction.HashSetAsync(key, [.. sets]));
        }

        public void Discard()
        {
            _staged.Clear();
            _cache.Clear();
        }

        private RedisValue Field(TKey k) => serializer.Serialize(k);
    }

    /// <summary>
    /// Deadlines in Redis: the hash holds each key's exact deadline, the sorted set their
    /// order. A sorted set's score is a double, which cannot hold every tick count, so the
    /// score is the deadline in units of 1,024 ticks (exact in a double for any date) and a
    /// range read is a superset the hash then decides — due means the exact deadline.
    /// <para>
    /// Nothing is read at start, whatever the store holds. <see cref="TakeDue"/> asks Redis
    /// only when the watermark has reached <c>_floor</c>, a lower bound on the committed
    /// scores still to be taken, so a step in which nothing closes costs no round trip.
    /// The floor never passes an entry whose removal is still staged: after a rollback
    /// that entry is due again.
    /// </para>
    /// </summary>
    private sealed class RedisDeadlineStore<TKey>(IDatabase db, string key, IStateSerializer serializer)
        : IDeadlineStore<TKey>, IStagedStore where TKey : notnull
    {
        private const int ScoreShift = 10;
        private const long FloorUnknown = long.MinValue;

        private readonly RedisKey _dueKey = key + ":due";

        // Deadlines staged since the last commit (not held = remove at flush), and committed
        // ones read from Redis this interval (not held = known absent).
        private readonly Dictionary<RedisValue, (bool Held, long Deadline)> _staged = [];
        private readonly Dictionary<RedisValue, (bool Held, long Deadline)> _cache = [];

        // At or below every staged deadline that is held: no scan of the overlay below it.
        private long _stagedMin = long.MaxValue;
        private long _floor = FloorUnknown;
        private readonly List<RedisValue> _scratch = [];

        public void AddAll(IReadOnlyList<TKey> keys, Func<TKey, long> deadlineOf)
        {
            if (keys.Count == 0)
                return;

            var fields = new RedisValue[keys.Count];
            for (var i = 0; i < keys.Count; i++)
                fields[i] = Field(keys[i]);
            Prefetch(fields);

            for (var i = 0; i < keys.Count; i++)
            {
                if (!_staged.TryGetValue(fields[i], out var current))
                    current = _cache[fields[i]];
                if (!current.Held)
                    Stage(fields[i], deadlineOf(keys[i]));
            }
        }

        public void Set(TKey k, long deadline) => Stage(Field(k), deadline);

        public void Remove(TKey k) => _staged[Field(k)] = (false, 0);

        public void TakeDue(long upTo, List<TKey> into)
        {
            if (upTo >= _stagedMin)
            {
                _scratch.Clear();
                var min = long.MaxValue;
                foreach (var (field, (held, deadline)) in _staged)
                {
                    if (!held)
                        continue;
                    if (deadline <= upTo)
                        _scratch.Add(field);
                    else
                        min = Math.Min(min, deadline);
                }
                _stagedMin = min;
                foreach (var field in _scratch)
                    Take(field, into);
            }

            var bucket = upTo >> ScoreShift;
            if (_floor != FloorUnknown && bucket < _floor)
                return;

            var candidates = db.SortedSetRangeByScoreWithScores(_dueKey, double.NegativeInfinity, bucket);
            if (candidates.Length == 0)
            {
                // Nothing at or below the watermark: the floor is the next score above it.
                var next = db.SortedSetRangeByScoreWithScores(_dueKey, bucket, double.PositiveInfinity, Exclude.Start, take: 1);
                _floor = next.Length == 0 ? long.MaxValue : (long)next[0].Score;
                return;
            }

            // Every candidate stays in the sorted set until the next commit, taken or not,
            // so the floor stays at the lowest of them.
            _floor = (long)candidates[0].Score;
            _scratch.Clear();
            foreach (var candidate in candidates)
                if (!_staged.ContainsKey(candidate.Element)) // else the overlay has decided it
                    _scratch.Add(candidate.Element);
            if (_scratch.Count == 0)
                return;

            Prefetch([.. _scratch]);
            foreach (var field in _scratch)
                if (_cache[field] is { Held: true, Deadline: var deadline } && deadline <= upTo)
                    Take(field, into);
        }

        private void Take(RedisValue field, List<TKey> into)
        {
            _staged[field] = (false, 0);
            into.Add(serializer.Deserialize<TKey>(field!));
        }

        private void Stage(RedisValue field, long deadline)
        {
            _staged[field] = (true, deadline);
            if (deadline < _stagedMin)
                _stagedMin = deadline;
        }

        public void Flush(ITransaction transaction, List<Task> pending)
        {
            List<RedisValue>? deletes = null;
            List<HashEntry>? sets = null;
            List<SortedSetEntry>? scores = null;
            foreach (var (field, (held, deadline)) in _staged)
            {
                if (!held)
                {
                    (deletes ??= []).Add(field);
                    continue;
                }
                (sets ??= []).Add(new HashEntry(field, deadline));
                (scores ??= []).Add(new SortedSetEntry(field, deadline >> ScoreShift));
                if (_floor != FloorUnknown && deadline >> ScoreShift < _floor)
                    _floor = deadline >> ScoreShift;
            }
            if (deletes is not null)
            {
                pending.Add(transaction.HashDeleteAsync(key, [.. deletes]));
                pending.Add(transaction.SortedSetRemoveAsync(_dueKey, [.. deletes]));
            }
            if (sets is not null)
            {
                pending.Add(transaction.HashSetAsync(key, [.. sets]));
                pending.Add(transaction.SortedSetAddAsync(_dueKey, [.. scores!]));
            }
        }

        public void Discard()
        {
            _staged.Clear();
            _cache.Clear();
            _stagedMin = long.MaxValue;
        }

        private RedisValue Field(TKey k) => serializer.Serialize(k);

        /// <summary>One HMGET round trip for the fields whose committed deadline is unknown.</summary>
        private void Prefetch(RedisValue[] fields)
        {
            HashSet<RedisValue>? seen = null;
            List<RedisValue>? missing = null;
            foreach (var field in fields)
                if (!_staged.ContainsKey(field) && !_cache.ContainsKey(field) && (seen ??= []).Add(field))
                    (missing ??= []).Add(field);
            if (missing is null)
                return;

            var stored = db.HashGet(key, [.. missing]);
            for (var i = 0; i < missing.Count; i++)
                _cache[missing[i]] = stored[i].IsNull ? (false, 0) : (true, (long)stored[i]);
        }
    }
}
