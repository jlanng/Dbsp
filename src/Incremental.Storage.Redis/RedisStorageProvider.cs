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
/// store. TODO: pipeline/batch the per-step round trips, and snapshot input channel offsets
/// alongside state for exactly-once restarts.
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
            if (entries.Length == 0)
                return [];

            var group = new WeightedValue<TValue>[entries.Length];
            for (var i = 0; i < entries.Length; i++)
                group[i] = new WeightedValue<TValue>(serializer.Deserialize<TValue>(entries[i].Name!), (long)entries[i].Value);
            return group;
        }

        public void RemoveGroup(TKey key) => db.KeyDelete(GroupKey(key));
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
    }
}
