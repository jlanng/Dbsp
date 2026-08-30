using StackExchange.Redis;
using Incremental.Storage;
using Incremental.Storage.Redis;

namespace Incremental.Tests;

/// <summary>
/// End-to-end circuit runs with Redis-backed operator state. Requires a local Redis
/// (`docker compose up -d`); skipped automatically when none is reachable.
/// </summary>
public class RedisStorageTests
{
    private static ConnectionMultiplexer ConnectOrSkip()
    {
        try
        {
            return ConnectionMultiplexer.Connect("localhost:6379,connectTimeout=1000,abortConnect=true");
        }
        catch (RedisConnectionException)
        {
            throw new SkipException("Redis is not reachable on localhost:6379 — run `docker compose up -d`.");
        }
    }

    [SkippableFact]
    public async Task Join_and_count_with_redis_state()
    {
        using var redis = ConnectOrSkip();
        var storage = new RedisStorageProvider(redis, new RedisStorageOptions
        {
            // Unique prefix per run so stale state from earlier runs can't leak in.
            KeyPrefix = $"sp-test:{Guid.NewGuid():N}",
        });

        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Storage = storage });
        var orders = builder.Input<(int Id, string Region, int Amount)>("orders");
        var countsDef = orders.GroupBy(o => o.Region)
            .Count()
            .ToOutput(bootstrap: false);

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(orders);
        var output = circuit.Handle(countsDef);

        await input.InsertAsync((1, "emea", 10));
        await input.InsertAsync((2, "emea", 20));
        await circuit.StepAsync();
        var delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(new KeyedValue<string, long>("emea", 2)));

        // Retraction must read the integrated group + last result back out of Redis.
        await input.RemoveAsync((1, "emea", 10));
        await circuit.StepAsync();
        delta = await output.Reader.ReadAsync();
        Assert.Equal(-1, delta.WeightOf(new KeyedValue<string, long>("emea", 2)));
        Assert.Equal(1, delta.WeightOf(new KeyedValue<string, long>("emea", 1)));
    }

    [SkippableFact]
    public async Task TopK_with_tuple_rows_round_trips_through_redis()
    {
        using var redis = ConnectOrSkip();
        var storage = new RedisStorageProvider(redis, new RedisStorageOptions
        {
            KeyPrefix = $"sp-test:{Guid.NewGuid():N}",
        });

        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Storage = storage });
        var scores = builder.Input<(string Grp, string Name, int Score)>("scores");
        var topDef = scores
            .GroupBy(s => s.Grp)
            .TopK(1, s => s.Score)
            .ToOutput(bootstrap: false);

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(scores);
        var output = circuit.Handle(topDef);

        await input.InsertAsync(("g", "a", 1));
        await circuit.StepAsync();
        var delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(new KeyedValue<string, (string, string, int)>("g", ("g", "a", 1))));

        await input.InsertAsync(("g", "b", 2));
        await circuit.StepAsync();
        delta = await output.Reader.ReadAsync();
        Assert.Equal(-1, delta.WeightOf(new KeyedValue<string, (string, string, int)>("g", ("g", "a", 1))));
        Assert.Equal(1, delta.WeightOf(new KeyedValue<string, (string, string, int)>("g", ("g", "b", 2))));
    }

    [SkippableFact]
    public async Task Min_tournament_tree_round_trips_through_redis()
    {
        using var redis = ConnectOrSkip();
        var storage = new RedisStorageProvider(redis, new RedisStorageOptions
        {
            KeyPrefix = $"sp-test:{Guid.NewGuid():N}",
        });

        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Storage = storage });
        var readings = builder.Input<(string Key, int Value)>("readings");
        var minimumsDef = readings
            .GroupBy(r => r.Key)
            .Min(r => r.Value)
            .ToOutput(bootstrap: false);

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(readings);
        var output = circuit.Handle(minimumsDef);

        await input.InsertAsync(("k", 5));
        await input.InsertAsync(("k", 3));
        await circuit.StepAsync();
        var delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(new KeyedValue<string, int>("k", 3)));

        // Deleting the minimum forces the tree walk to read buckets back out of Redis.
        await input.RemoveAsync(("k", 3));
        await circuit.StepAsync();
        delta = await output.Reader.ReadAsync();
        Assert.Equal(-1, delta.WeightOf(new KeyedValue<string, int>("k", 3)));
        Assert.Equal(1, delta.WeightOf(new KeyedValue<string, int>("k", 5)));
    }

    private static RedisStorageProvider FreshProvider(ConnectionMultiplexer redis) =>
        new(redis, new RedisStorageOptions { KeyPrefix = $"sp-test:{Guid.NewGuid():N}" });

    [SkippableFact]
    public void Weight_store_batches_pipeline_and_drop_zeroed_entries()
    {
        using var redis = ConnectOrSkip();
        var store = FreshProvider(redis).CreateWeightStore<string>("batch");

        var results = store.UpdateAll([("a", 2), ("b", 3), ("c", 1)]);
        Assert.Equal([2, 3, 1], results);

        // "b" ends at zero and must be dropped; "c" passes through zero across duplicate
        // entries but ends non-zero and must survive.
        results = store.UpdateAll([("b", -3), ("c", -1), ("c", 4)]);
        Assert.Equal([0, 0, 4], results);

        Assert.Equal([2, 0, 4, 0], store.WeightOfAll(["a", "b", "c", "missing"]));
        // A dropped entry re-added starts from zero, proving the hash field was deleted.
        Assert.Equal(7, store.Update("b", 7));

        store.RemoveAll(["a", "c"]);
        Assert.Equal([0, 7, 0], store.WeightOfAll(["a", "b", "c"]));
    }

    [SkippableFact]
    public void Indexed_store_batches_span_groups()
    {
        using var redis = ConnectOrSkip();
        var store = FreshProvider(redis).CreateIndexedWeightStore<string, string>("batch");

        var results = store.UpdateAll([("g1", "x", 1), ("g1", "y", 2), ("g2", "x", 3), ("g1", "y", -2)]);
        Assert.Equal([1, 2, 3, 0], results);

        var groups = store.GetGroups(["g1", "g2", "empty"]);
        var g1 = Assert.Single(groups[0]);
        Assert.Equal(new WeightedValue<string>("x", 1), g1);
        var g2 = Assert.Single(groups[1]);
        Assert.Equal(new WeightedValue<string>("x", 3), g2);
        Assert.Empty(groups[2]);

        store.RemoveGroups(["g1", "g2"]);
        Assert.All(store.GetGroups(["g1", "g2"]), g => Assert.Empty(g));
    }

    [SkippableFact]
    public async Task Recursive_closure_with_redis_state()
    {
        // The loop operators' state is iteration-profiled — (row, iteration) tuple members
        // for the join, per-value iteration profiles for the distinct — and must round-trip
        // through the serializer. Deletion exercises reading the profiles back.
        using var redis = ConnectOrSkip();

        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Storage = FreshProvider(redis) });
        var edges = builder.Input<(string From, string To)>("edges");
        var closureDef = edges.Recurse(paths => paths
                .GroupBy(p => p.To)
                .Join(edges.GroupBy(e => e.From), (_, p, e) => (p.From, e.To)))
            .ToOutput();

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(edges);
        var output = circuit.Handle(closureDef);

        await input.InsertAsync(("a", "b"));
        await input.InsertAsync(("b", "c"));
        await circuit.StepAsync();
        var delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(("a", "b")));
        Assert.Equal(1, delta.WeightOf(("b", "c")));
        Assert.Equal(1, delta.WeightOf(("a", "c")));

        await input.RemoveAsync(("b", "c"));
        await circuit.StepAsync();
        delta = await output.Reader.ReadAsync();
        Assert.Equal(-1, delta.WeightOf(("b", "c")));
        Assert.Equal(-1, delta.WeightOf(("a", "c")));
        Assert.Equal(2, delta.DistinctCount);
    }

    [SkippableFact]
    public void Value_store_batches_round_trip()
    {
        using var redis = ConnectOrSkip();
        var store = FreshProvider(redis).CreateValueStore<string, (int A, string B)>("batch");

        store.PutAll([("k1", (1, "one")), ("k2", (2, "two"))]);

        var results = store.TryGetAll(["k1", "missing", "k2"]);
        Assert.Equal((true, (1, "one")), results[0]);
        Assert.False(results[1].Found);
        Assert.Equal((true, (2, "two")), results[2]);

        store.RemoveAll(["k1", "missing"]);
        results = store.TryGetAll(["k1", "k2"]);
        Assert.False(results[0].Found);
        Assert.True(results[1].Found);
    }
}
