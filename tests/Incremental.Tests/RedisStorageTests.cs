using StackExchange.Redis;
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
        orders.IndexBy(o => o.Region)
            .Count()
            .Output("counts");

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Input<(int Id, string Region, int Amount)>("orders");
        var output = circuit.Output<KeyedValue<string, long>>("counts");

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
        builder.Input<(string Grp, string Name, int Score)>("scores")
            .IndexBy(s => s.Grp)
            .TopK(1, s => s.Score)
            .Output("top");

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Input<(string Grp, string Name, int Score)>("scores");
        var output = circuit.Output<KeyedValue<string, (string Grp, string Name, int Score)>>("top");

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
}
