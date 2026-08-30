using StackExchange.Redis;
using Incremental.Storage;
using Incremental.Storage.Redis;

namespace Incremental.Tests;

/// <summary>
/// Composite (partition, offset) input stamping: one input fronting a multi-partition
/// source (e.g. a Kafka topic) deduplicates replays per partition, the committed offsets
/// marker carries one position per partition, and mixing stamped and unstamped records on
/// one input — the silent-corruption path — fails loudly, within a run and across restarts.
/// </summary>
public class PartitionedOffsetTests
{
    private static CircuitBuilder CountByRegion(IStorageProvider storage)
    {
        var builder = new CircuitBuilder(new CircuitOptions
        {
            AutoStep = false,
            Storage = storage,
            CheckpointInterval = 1,
        });
        builder.Input<(int Id, string Region)>("orders")
            .IndexBy(o => o.Region)
            .Count()
            .Output("counts");
        return builder;
    }

    private static KeyedValue<string, long> Row(string region, long count) => new(region, count);

    [Fact]
    public async Task Multi_partition_replay_is_exactly_once_per_partition()
    {
        var storage = new InMemoryCheckpointStorageProvider();

        await using (var circuit = CountByRegion(storage).Build())
        {
            circuit.Start();
            var input = circuit.Input<(int Id, string Region)>("orders");
            var output = circuit.Output<KeyedValue<string, long>>("counts");
            await input.SendAsync((1, "emea"), 1, partition: 0, offset: 10);
            await input.SendAsync((2, "emea"), 1, partition: 0, offset: 11);
            await input.SendAsync((3, "emea"), 1, partition: 1, offset: 5);
            await circuit.StepAsync();
            var delta = await output.Reader.ReadAsync();
            Assert.Equal(1, delta.WeightOf(Row("emea", 3)));
        }

        // "Restart": each partition resumes at its own committed position — a replay on one
        // partition cannot be deduplicated against another's offsets.
        await using var resumed = CountByRegion(storage).Build();
        resumed.Start();
        Assert.Equal(new Dictionary<int, long> { [0] = 11, [1] = 5 }, resumed.CommittedOffsets("orders"));

        var replay = resumed.Input<(int Id, string Region)>("orders");
        var counts = resumed.Output<KeyedValue<string, long>>("counts");
        await replay.SendAsync((1, "emea"), 1, partition: 0, offset: 10); // replayed — dropped
        await replay.SendAsync((2, "emea"), 1, partition: 0, offset: 11); // replayed — dropped
        await replay.SendAsync((4, "emea"), 1, partition: 0, offset: 12); // new
        await replay.SendAsync((3, "emea"), 1, partition: 1, offset: 5);  // replayed — dropped
        await replay.SendAsync((5, "emea"), 1, partition: 1, offset: 6);  // new
        await resumed.StepAsync();
        var resumedDelta = await counts.Reader.ReadAsync();

        // Exactly the two new records applied on top of the recovered state: 3 → 5.
        Assert.Equal(-1, resumedDelta.WeightOf(Row("emea", 3)));
        Assert.Equal(1, resumedDelta.WeightOf(Row("emea", 5)));
        Assert.Equal(new Dictionary<int, long> { [0] = 12, [1] = 6 }, resumed.CommittedOffsets("orders"));
    }

    [Fact]
    public async Task CommittedOffset_reads_the_single_partition_and_refuses_several()
    {
        var storage = new InMemoryCheckpointStorageProvider();
        await using var circuit = CountByRegion(storage).Build();
        circuit.Start();
        var input = circuit.Input<(int Id, string Region)>("orders");

        // The partitionless overloads stamp partition 0, so the legacy single-offset shape
        // keeps its one-value read.
        await input.SendAsync((1, "emea"), 1, offset: 10);
        await circuit.StepAsync();
        Assert.Equal(10L, circuit.CommittedOffset("orders"));

        await input.SendAsync((2, "emea"), 1, partition: 1, offset: 3);
        await circuit.StepAsync();
        var error = Assert.Throws<InvalidOperationException>(() => circuit.CommittedOffset("orders"));
        Assert.Contains("CommittedOffsets", error.Message);
        Assert.Equal(new Dictionary<int, long> { [0] = 10, [1] = 3 }, circuit.CommittedOffsets("orders"));
    }

    [Fact]
    public async Task Mixing_stamped_and_unstamped_throws_at_the_handle()
    {
        await using var circuit = CountByRegion(new InMemoryCheckpointStorageProvider()).Build();
        circuit.Start();

        var input = circuit.Input<(int Id, string Region)>("orders");
        await input.SendAsync((1, "emea"), 1, offset: 0);
        var error = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await input.InsertAsync((2, "emea")));
        Assert.Contains("replay position", error.Message);

        // And the other way round, on a fresh circuit.
        await using var other = CountByRegion(new InMemoryCheckpointStorageProvider()).Build();
        other.Start();
        var unstamped = other.Input<(int Id, string Region)>("orders");
        await unstamped.InsertAsync((1, "emea"));
        await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await unstamped.SendAsync((2, "emea"), 1, offset: 0));
    }

    [Fact]
    public async Task Switching_stamping_across_a_restart_faults_the_circuit()
    {
        var storage = new InMemoryCheckpointStorageProvider();

        await using (var circuit = CountByRegion(storage).Build())
        {
            circuit.Start();
            var input = circuit.Input<(int Id, string Region)>("orders");
            await input.SendAsync((1, "emea"), 1, offset: 10);
            await circuit.StepAsync();
        }

        // A fresh handle has no send history, so only the input's recovered committed
        // position can catch the switch — at the step, as a fault, not silently.
        await using var resumed = CountByRegion(storage).Build();
        resumed.Start();
        var replay = resumed.Input<(int Id, string Region)>("orders");
        await replay.InsertAsync((2, "emea"));
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => resumed.StepAsync());
        Assert.Contains("orders", error.Message);
        Assert.Contains("replay position", error.Message);
        Assert.NotNull(resumed.Fault);
    }

    [Fact]
    public async Task Unstamped_numbering_continues_across_restart_with_no_partitions()
    {
        var storage = new InMemoryCheckpointStorageProvider();

        await using (var circuit = CountByRegion(storage).Build())
        {
            circuit.Start();
            var input = circuit.Input<(int Id, string Region)>("orders");
            await input.InsertAsync((1, "emea"));
            await input.InsertAsync((2, "emea"));
            await circuit.StepAsync();
            Assert.Equal(1L, circuit.CommittedOffset("orders")); // numbered 0, 1, ...
        }

        await using var resumed = CountByRegion(storage).Build();
        resumed.Start();
        Assert.Equal(1L, resumed.CommittedOffset("orders"));
        Assert.Empty(resumed.CommittedOffsets("orders")); // a running count has no partitions

        var input2 = resumed.Input<(int Id, string Region)>("orders");
        await input2.InsertAsync((3, "emea"));
        await resumed.StepAsync();
        Assert.Equal(2L, resumed.CommittedOffset("orders"));
    }

    [SkippableFact]
    public async Task Multi_partition_checkpoint_round_trips_through_redis()
    {
        using var redis = ConnectOrSkip();
        var prefix = $"sp-test:{Guid.NewGuid():N}";
        RedisStorageProvider Storage() => new(redis, new RedisStorageOptions { KeyPrefix = prefix });

        await using (var circuit = CountByRegion(Storage()).Build())
        {
            circuit.Start();
            var input = circuit.Input<(int Id, string Region)>("orders");
            await input.SendAsync((1, "emea"), 1, partition: 0, offset: 7);
            await input.SendAsync((2, "emea"), 1, partition: 3, offset: 40);
            await circuit.StepAsync();
        }

        // A fresh provider over the same prefix: the per-partition marker is durable, and
        // replays deduplicate per partition against it.
        await using var resumed = CountByRegion(Storage()).Build();
        resumed.Start();
        Assert.Equal(new Dictionary<int, long> { [0] = 7, [3] = 40 }, resumed.CommittedOffsets("orders"));

        var replay = resumed.Input<(int Id, string Region)>("orders");
        var counts = resumed.Output<KeyedValue<string, long>>("counts");
        await replay.SendAsync((1, "emea"), 1, partition: 0, offset: 7);  // replayed — dropped
        await replay.SendAsync((3, "emea"), 1, partition: 3, offset: 41); // new
        await resumed.StepAsync();
        var delta = await counts.Reader.ReadAsync();
        Assert.Equal(-1, delta.WeightOf(Row("emea", 2)));
        Assert.Equal(1, delta.WeightOf(Row("emea", 3)));
    }

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
}
