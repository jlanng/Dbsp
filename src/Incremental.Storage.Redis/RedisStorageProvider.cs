using System.Diagnostics.CodeAnalysis;
using StackExchange.Redis;
using Incremental.Storage;

namespace Incremental.Storage.Redis;

public sealed class RedisStorageOptions
{
    /// <summary>Prefix for every Redis key this provider writes.</summary>
    public string KeyPrefix { get; init; } = "sp";

    /// <summary>Redis database number, or -1 for the connection default.</summary>
    public int Database { get; init; } = -1;

    public IStateSerializer Serializer { get; init; } = JsonStateSerializer.Instance;
}

/// <summary>
/// Keeps operator state in Redis hashes, which makes it survive process restarts and lets
/// state grow beyond the heap. Weight maps use HINCRBY for atomic weight arithmetic.
///
/// Layout (all under <see cref="RedisStorageOptions.KeyPrefix"/>):
///   weight store       {prefix}:{store}            hash: serialized value → weight
///   indexed store      {prefix}:{store}:{key}      hash: serialized value → weight, one hash per group
///   value store        {prefix}:{store}            hash: serialized key → serialized value
///
/// Store names already encode the node and worker id, so concurrent workers never share a
/// store. The batch operations pipeline all commands for a call into a single round trip
/// (variadic commands like HMGET/HDEL/DEL where one command covers the batch); operators
/// drive them with everything a step touches, so per-step latency is O(waves), not O(rows).
/// TODO: snapshot input channel offsets alongside state for exactly-once restarts.
/// </summary>
public sealed class RedisStorageProvider : IStorageProvider
{
    private readonly IDatabase _db;
    private readonly RedisStorageOptions _options;

    public RedisStorageProvider(IConnectionMultiplexer connection, RedisStorageOptions? options = null)
    {
        _options = options ?? new RedisStorageOptions();
        _db = connection.GetDatabase(_options.Database);
    }

    public IWeightStore<T> CreateWeightStore<T>(string name) where T : notnull =>
        new RedisWeightStore<T>(_db, $"{_options.KeyPrefix}:{name}", _options.Serializer);

    public IIndexedWeightStore<TKey, TValue> CreateIndexedWeightStore<TKey, TValue>(string name)
        where TKey : notnull
        where TValue : notnull =>
        new RedisIndexedWeightStore<TKey, TValue>(_db, $"{_options.KeyPrefix}:{name}", _options.Serializer);

    public IValueStore<TKey, TValue> CreateValueStore<TKey, TValue>(string name) where TKey : notnull =>
        new RedisValueStore<TKey, TValue>(_db, $"{_options.KeyPrefix}:{name}", _options.Serializer);

    private sealed class RedisWeightStore<T>(IDatabase db, string key, IStateSerializer serializer)
        : IWeightStore<T> where T : notnull
    {
        public long Update(T value, long delta)
        {
            var field = serializer.Serialize(value);
            var updated = db.HashIncrement(key, field, delta);
            if (updated == 0)
                db.HashDelete(key, field);
            return updated;
        }

        public long WeightOf(T value)
        {
            var stored = db.HashGet(key, serializer.Serialize(value));
            return stored.IsNull ? 0 : (long)stored;
        }

        public void Remove(T value) => db.HashDelete(key, serializer.Serialize(value));

        public long[] UpdateAll(IReadOnlyList<(T Value, long Delta)> entries)
        {
            if (entries.Count == 0)
                return [];

            var batch = db.CreateBatch();
            var fields = new RedisValue[entries.Count];
            var tasks = new Task<long>[entries.Count];
            for (var i = 0; i < entries.Count; i++)
            {
                fields[i] = serializer.Serialize(entries[i].Value);
                tasks[i] = batch.HashIncrementAsync(key, fields[i], entries[i].Delta);
            }
            batch.Execute();
            Task.WaitAll(tasks);

            var results = new long[entries.Count];
            // A field's final weight is its last entry's result — only fields that *end* at
            // zero are deleted, so a duplicate value whose running weight passes through
            // zero is not dropped by mistake.
            var final = new Dictionary<RedisValue, long>(entries.Count);
            for (var i = 0; i < entries.Count; i++)
            {
                results[i] = tasks[i].Result;
                final[fields[i]] = results[i];
            }

            var zeroed = new List<RedisValue>();
            foreach (var (field, weight) in final)
                if (weight == 0)
                    zeroed.Add(field);
            if (zeroed.Count > 0)
                db.HashDelete(key, [.. zeroed]);

            return results;
        }

        public long[] WeightOfAll(IReadOnlyList<T> values)
        {
            if (values.Count == 0)
                return [];

            var fields = new RedisValue[values.Count];
            for (var i = 0; i < values.Count; i++)
                fields[i] = serializer.Serialize(values[i]);

            var stored = db.HashGet(key, fields);
            var results = new long[values.Count];
            for (var i = 0; i < values.Count; i++)
                results[i] = stored[i].IsNull ? 0 : (long)stored[i];
            return results;
        }

        public void RemoveAll(IReadOnlyList<T> values)
        {
            if (values.Count == 0)
                return;

            var fields = new RedisValue[values.Count];
            for (var i = 0; i < values.Count; i++)
                fields[i] = serializer.Serialize(values[i]);
            db.HashDelete(key, fields);
        }
    }

    private sealed class RedisIndexedWeightStore<TKey, TValue>(IDatabase db, string baseKey, IStateSerializer serializer)
        : IIndexedWeightStore<TKey, TValue>
        where TKey : notnull
        where TValue : notnull
    {
        private string GroupKey(TKey key) => $"{baseKey}:{serializer.Serialize(key)}";

        public long Update(TKey key, TValue value, long delta)
        {
            var groupKey = GroupKey(key);
            var field = serializer.Serialize(value);
            var updated = db.HashIncrement(groupKey, field, delta);
            if (updated == 0)
                db.HashDelete(groupKey, field);
            return updated;
        }

        public IReadOnlyList<WeightedValue<TValue>> GetGroup(TKey key)
        {
            var entries = db.HashGetAll(GroupKey(key));
            return Materialize(entries);
        }

        public void RemoveGroup(TKey key) => db.KeyDelete(GroupKey(key));

        public long[] UpdateAll(IReadOnlyList<(TKey Key, TValue Value, long Delta)> entries)
        {
            if (entries.Count == 0)
                return [];

            var batch = db.CreateBatch();
            var addresses = new (RedisKey Group, RedisValue Field)[entries.Count];
            var tasks = new Task<long>[entries.Count];
            for (var i = 0; i < entries.Count; i++)
            {
                addresses[i] = (GroupKey(entries[i].Key), serializer.Serialize(entries[i].Value));
                tasks[i] = batch.HashIncrementAsync(addresses[i].Group, addresses[i].Field, entries[i].Delta);
            }
            batch.Execute();
            Task.WaitAll(tasks);

            var results = new long[entries.Count];
            // Final weight per (group, field) — see RedisWeightStore.UpdateAll.
            var final = new Dictionary<(RedisKey, RedisValue), long>(entries.Count);
            for (var i = 0; i < entries.Count; i++)
            {
                results[i] = tasks[i].Result;
                final[addresses[i]] = results[i];
            }

            Dictionary<RedisKey, List<RedisValue>>? zeroed = null;
            foreach (var ((group, field), weight) in final)
            {
                if (weight != 0)
                    continue;
                zeroed ??= [];
                if (!zeroed.TryGetValue(group, out var fields))
                    zeroed[group] = fields = [];
                fields.Add(field);
            }
            if (zeroed is not null)
            {
                var cleanup = db.CreateBatch();
                var deletes = new List<Task>(zeroed.Count);
                foreach (var (group, fields) in zeroed)
                    deletes.Add(cleanup.HashDeleteAsync(group, [.. fields]));
                cleanup.Execute();
                Task.WaitAll([.. deletes]);
            }

            return results;
        }

        public IReadOnlyList<WeightedValue<TValue>>[] GetGroups(IReadOnlyList<TKey> keys)
        {
            if (keys.Count == 0)
                return [];

            var batch = db.CreateBatch();
            var tasks = new Task<HashEntry[]>[keys.Count];
            for (var i = 0; i < keys.Count; i++)
                tasks[i] = batch.HashGetAllAsync(GroupKey(keys[i]));
            batch.Execute();
            Task.WaitAll(tasks);

            var results = new IReadOnlyList<WeightedValue<TValue>>[keys.Count];
            for (var i = 0; i < keys.Count; i++)
                results[i] = Materialize(tasks[i].Result);
            return results;
        }

        public void RemoveGroups(IReadOnlyList<TKey> keys)
        {
            if (keys.Count == 0)
                return;

            var groupKeys = new RedisKey[keys.Count];
            for (var i = 0; i < keys.Count; i++)
                groupKeys[i] = GroupKey(keys[i]);
            db.KeyDelete(groupKeys);
        }

        private IReadOnlyList<WeightedValue<TValue>> Materialize(HashEntry[] entries)
        {
            if (entries.Length == 0)
                return [];

            var group = new WeightedValue<TValue>[entries.Length];
            for (var i = 0; i < entries.Length; i++)
                group[i] = new WeightedValue<TValue>(serializer.Deserialize<TValue>(entries[i].Name!), (long)entries[i].Value);
            return group;
        }
    }

    private sealed class RedisValueStore<TKey, TValue>(IDatabase db, string key, IStateSerializer serializer)
        : IValueStore<TKey, TValue> where TKey : notnull
    {
        public bool TryGet(TKey k, [MaybeNullWhen(false)] out TValue value)
        {
            var stored = db.HashGet(key, serializer.Serialize(k));
            if (stored.IsNull)
            {
                value = default;
                return false;
            }
            value = serializer.Deserialize<TValue>(stored!);
            return true;
        }

        public void Put(TKey k, TValue value) =>
            db.HashSet(key, serializer.Serialize(k), serializer.Serialize(value));

        public void Remove(TKey k) => db.HashDelete(key, serializer.Serialize(k));

        public (bool Found, TValue? Value)[] TryGetAll(IReadOnlyList<TKey> keys)
        {
            if (keys.Count == 0)
                return [];

            var fields = new RedisValue[keys.Count];
            for (var i = 0; i < keys.Count; i++)
                fields[i] = serializer.Serialize(keys[i]);

            var stored = db.HashGet(key, fields);
            var results = new (bool, TValue?)[keys.Count];
            for (var i = 0; i < keys.Count; i++)
                results[i] = stored[i].IsNull
                    ? (false, default)
                    : (true, serializer.Deserialize<TValue>(stored[i]!));
            return results;
        }

        public void PutAll(IReadOnlyList<(TKey Key, TValue Value)> pairs)
        {
            if (pairs.Count == 0)
                return;

            var entries = new HashEntry[pairs.Count];
            for (var i = 0; i < pairs.Count; i++)
                entries[i] = new HashEntry(serializer.Serialize(pairs[i].Key), serializer.Serialize(pairs[i].Value));
            db.HashSet(key, entries);
        }

        public void RemoveAll(IReadOnlyList<TKey> keys)
        {
            if (keys.Count == 0)
                return;

            var fields = new RedisValue[keys.Count];
            for (var i = 0; i < keys.Count; i++)
                fields[i] = serializer.Serialize(keys[i]);
            db.HashDelete(key, fields);
        }
    }
}
