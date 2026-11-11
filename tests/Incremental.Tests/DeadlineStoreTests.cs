using Incremental.Storage;
using Incremental.Storage.Redis;
using StackExchange.Redis;

namespace Incremental.Tests;

/// <summary>
/// The deadline store's contract, held against every provider: random operations — add if
/// absent, set, remove, take what is due, commit, roll back, and reopen from what is
/// durable — checked step by step against a dictionary. Deadlines crowd into a few
/// thousand ticks, so many share one of the Redis store's 1,024-tick scores and the exact
/// comparison is what decides.
/// </summary>
public class DeadlineStoreTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(20);
    private const long Base = 638_000_000_000_000_000; // a tick count of the 2020s: beyond a double's exact integers

    private interface IBacking : IAsyncDisposable
    {
        IDeadlineStore<string> Store { get; }
        bool Durable { get; }
        void Commit();
        void Rollback();

        /// <summary>A process that starts on what was committed.</summary>
        Task ReopenAsync();
    }

    private sealed class Heap : IBacking
    {
        public IDeadlineStore<string> Store { get; } = new InMemoryStorageProvider().CreateDeadlineStore<string>("d");
        public bool Durable => false;
        public void Commit() { }
        public void Rollback() => throw new NotSupportedException();
        public Task ReopenAsync() => throw new NotSupportedException();
        public ValueTask DisposeAsync() => default;
    }

    private sealed class InMemory : IBacking
    {
        private readonly InMemoryCheckpointStorageProvider _provider = new();
        public IDeadlineStore<string> Store => _provider.CreateDeadlineStore<string>("d");
        public bool Durable => true;
        public void Commit() => _provider.Commit(new Dictionary<string, long>());
        public void Rollback() => _provider.Recover();

        public Task ReopenAsync()
        {
            _provider.Recover();
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync() => default;
    }

    private sealed class Changelog : IBacking
    {
        private readonly InMemoryStateChangelog _log = new(partitions: 1);
        private ChangelogStorage _storage = null!;
        private readonly List<ChangelogStorage> _followers = [];

        public static async Task<Changelog> OpenAsync()
        {
            var backing = new Changelog();
            backing._storage = await ChangelogStorage.OpenAsync(backing._log).WaitAsync(Timeout);
            return backing;
        }

        private ChangelogStorageProvider Provider => _storage.Pipeline("p");
        public IDeadlineStore<string> Store => Provider.CreateDeadlineStore<string>("d");
        public bool Durable => true;
        public void Commit() => Provider.Commit(new Dictionary<string, long>());
        public void Rollback() => Provider.Recover();

        /// <summary>Every other time, a follower that held the store typed all along and
        /// is promoted; otherwise a cold read of the log.</summary>
        public async Task ReopenAsync()
        {
            var old = _storage;
            if (_followers.Count > 0)
            {
                _storage = _followers[0];
                _followers.Clear();
                await _storage.PromoteAsync().WaitAsync(Timeout);
            }
            else
            {
                _storage = await ChangelogStorage.OpenAsync(_log).WaitAsync(Timeout);
                var follower = ChangelogStorage.Follow(_log);
                follower.Pipeline("p").CreateDeadlineStore<string>("d");
                _followers.Add(follower);
            }
            await old.DisposeAsync();
            Provider.CreateDeadlineStore<string>("d");
            Provider.Recover();
        }

        public async ValueTask DisposeAsync()
        {
            foreach (var follower in _followers)
                await follower.DisposeAsync();
            await _storage.DisposeAsync();
        }
    }

    private sealed class Redis : IBacking
    {
        private readonly ConnectionMultiplexer _connection;
        private readonly string _prefix = $"sp-test:{Guid.NewGuid():N}";
        private RedisStorageProvider _provider;

        public Redis()
        {
            try
            {
                _connection = ConnectionMultiplexer.Connect("localhost:6379,connectTimeout=1000,abortConnect=true,allowAdmin=true");
            }
            catch (RedisConnectionException)
            {
                throw Xunit.Sdk.SkipException.ForSkip("Redis is not reachable on localhost:6379 — run `docker compose up -d`.");
            }
            _provider = Open();
        }

        private RedisStorageProvider Open() => new(_connection, new RedisStorageOptions { KeyPrefix = _prefix });
        public IDeadlineStore<string> Store => _provider.CreateDeadlineStore<string>("d");
        public bool Durable => true;
        public void Commit() => _provider.Commit(new Dictionary<string, long> { ["x"] = 1 });
        public void Rollback() => _provider.Recover();

        public Task ReopenAsync()
        {
            _provider = Open();
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            foreach (var key in _connection.GetServers()[0].Keys(pattern: _prefix + ":*").ToArray())
                _connection.GetDatabase().KeyDelete(key);
            _connection.Dispose();
            return default;
        }
    }

    private static async Task<IBacking> OpenAsync(string backing) => backing switch
    {
        "heap" => new Heap(),
        "in-memory" => new InMemory(),
        "changelog" => await Changelog.OpenAsync(),
        _ => new Redis(),
    };

    [Theory]
    [InlineData("heap")]
    [InlineData("in-memory")]
    [InlineData("changelog")]
    [InlineData("redis")]
    public async Task It_holds_what_a_dictionary_would_through_commits_rollbacks_and_reopens(string name)
    {
        for (var seed = 1; seed <= 6; seed++)
        {
            await using var backing = await OpenAsync(name);
            var random = new Random(seed);
            var live = new Dictionary<string, long>();
            var committed = new Dictionary<string, long>();
            var clock = Base;
            int takes = 0, taken = 0, rollbacks = 0, reopens = 0;

            long Deadline() => clock + random.Next(-1500, 4000);
            string Key() => $"k{random.Next(40)}";

            for (var op = 0; op < 400; op++)
            {
                var store = backing.Store;
                switch (random.Next(12))
                {
                    case 0 or 1 or 2:
                    {
                        // Add if absent, a few keys at once, some of them twice.
                        var keys = Enumerable.Range(0, random.Next(1, 6)).Select(_ => Key()).ToList();
                        var deadlines = keys.Distinct().ToDictionary(key => key, _ => Deadline());
                        var asked = new List<string>();
                        store.AddAll(keys, key =>
                        {
                            asked.Add(key);
                            return deadlines[key];
                        });
                        Assert.Equal(keys.Distinct().Where(key => !live.ContainsKey(key)), asked);
                        foreach (var key in asked)
                            live.Add(key, deadlines[key]);
                        break;
                    }
                    case 3 or 4:
                    {
                        var (key, deadline) = (Key(), Deadline());
                        store.Set(key, deadline);
                        live[key] = deadline;
                        break;
                    }
                    case 5:
                    {
                        var key = Key();
                        store.Remove(key);
                        live.Remove(key);
                        break;
                    }
                    case 6 or 7 or 8:
                    {
                        // Mostly forwards, as a watermark goes; the contract does not ask for it.
                        clock += random.Next(-200, 900);
                        var due = new List<string> { "kept" };
                        store.TakeDue(clock, due);
                        var expected = live.Where(entry => entry.Value <= clock).Select(entry => entry.Key).Order().ToList();
                        Assert.Equal("kept", due[0]);
                        Assert.Equal(expected, due.Skip(1).Order());
                        foreach (var key in expected)
                            live.Remove(key);
                        takes++;
                        taken += expected.Count;
                        break;
                    }
                    case 9:
                        backing.Commit();
                        if (backing.Durable)
                            committed = new Dictionary<string, long>(live);
                        break;
                    case 10 when backing.Durable:
                        backing.Rollback();
                        live = new Dictionary<string, long>(committed);
                        rollbacks++;
                        break;
                    case 11 when backing.Durable && random.Next(3) == 0:
                        await backing.ReopenAsync();
                        live = new Dictionary<string, long>(committed);
                        reopens++;
                        break;
                }
            }

            // Everything still held comes out, and nothing else does.
            var rest = new List<string>();
            backing.Store.TakeDue(long.MaxValue, rest);
            Assert.Equal(live.Keys.Order(), rest.Order());
            Assert.True(takes > 50 && taken > 20, $"{takes} takes took {taken}");
            Assert.True(!backing.Durable || (rollbacks > 5 && reopens > 2), $"{rollbacks} rollbacks, {reopens} reopens");
        }
    }

    [Fact]
    public async Task On_the_changelog_a_deadline_is_one_record_however_its_key_is_written()
    {
        // 10.0m equals 10.00m and serializes differently: the record written under the
        // first must be the one the second removes, or the log keeps a deadline for good.
        var log = new InMemoryStateChangelog(partitions: 1);
        await using var storage = await ChangelogStorage.OpenAsync(log).WaitAsync(Timeout);
        var provider = storage.Pipeline("p");
        var store = provider.CreateDeadlineStore<decimal>("d");
        store.Set(10.0m, 5);
        store.Set(20.0m, 6);
        provider.Commit(new Dictionary<string, long>());
        store.Set(10.00m, 7);
        provider.Commit(new Dictionary<string, long>());
        Assert.Equal(["d|10.0=7", "d|20.0=6"], provider.DescribeState());

        store.Remove(10.00m);
        var due = new List<decimal>();
        store.TakeDue(6, due);
        Assert.Equal([20.0m], due);
        provider.Commit(new Dictionary<string, long>());

        await using var cold = ChangelogStorage.Follow(log);
        await cold.WhenCaughtUpAsync().WaitAsync(Timeout);
        Assert.Empty(cold.Pipeline("p").DescribeState());
    }

    [Fact]
    public void The_redis_store_asks_redis_nothing_while_nothing_can_be_due()
    {
        using var backing = new CountingRedis();
        var store = backing.Provider.CreateDeadlineStore<string>("d");
        store.Set("a", Base + 50_000);
        store.Set("b", Base + 90_000);
        backing.Provider.Commit(new Dictionary<string, long> { ["x"] = 1 });

        // The first take finds where the committed deadlines start; the ones after it,
        // below that, are answered from memory.
        var due = new List<string>();
        store.TakeDue(Base, due);
        var after = backing.Commands();
        for (var step = 1; step <= 40; step++)
            store.TakeDue(Base + step * 1000, due);
        Assert.Empty(due);
        Assert.Equal(after, backing.Commands());

        store.TakeDue(Base + 60_000, due);
        Assert.Equal(["a"], due);
        Assert.True(backing.Commands() > after);
    }

    private sealed class CountingRedis : IDisposable
    {
        private readonly ConnectionMultiplexer _connection;
        private readonly string _prefix = $"sp-test:{Guid.NewGuid():N}";

        public CountingRedis()
        {
            try
            {
                _connection = ConnectionMultiplexer.Connect("localhost:6379,connectTimeout=1000,abortConnect=true,allowAdmin=true");
            }
            catch (RedisConnectionException)
            {
                throw Xunit.Sdk.SkipException.ForSkip("Redis is not reachable on localhost:6379 — run `docker compose up -d`.");
            }
            Provider = new RedisStorageProvider(_connection, new RedisStorageOptions { KeyPrefix = _prefix });
        }

        public RedisStorageProvider Provider { get; }

        /// <summary>Operations this connection has completed.</summary>
        public long Commands() => _connection.GetCounters().Interactive.OperationCount;

        public void Dispose()
        {
            foreach (var key in _connection.GetServers()[0].Keys(pattern: _prefix + ":*").ToArray())
                _connection.GetDatabase().KeyDelete(key);
            _connection.Dispose();
        }
    }
}
