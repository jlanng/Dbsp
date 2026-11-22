using StackExchange.Redis;
using Dbsp.Storage;
using Dbsp.Storage.Redis;

namespace Dbsp.Tests;

/// <summary>
/// Composite (partition, offset) input stamping: one input fronting a multi-partition
/// source (e.g. a Kafka topic) deduplicates replays per partition, the committed offsets
/// marker carries one position per partition, and mixing stamped and unstamped records on
/// one input — the silent-corruption path — fails loudly, within a run and across restarts.
/// </summary>
public class PartitionedOffsetTests
{
    private sealed record CountCircuit(
        CircuitBuilder Builder,
        InputStream<(int Id, string Region)> Orders,
        Output<KeyedValue<string, long>> Counts);

    private static CountCircuit CountByRegion(IStorageProvider storage)
    {
        var builder = new CircuitBuilder(new CircuitOptions
        {
            AutoStep = false,
            Storage = storage,
            CheckpointInterval = 1,
        });
        var orders = builder.Input<(int Id, string Region)>("orders");
        var counts = orders
            .GroupBy(o => o.Region)
            .Count()
            .ToOutput(bootstrap: false);
        return new CountCircuit(builder, orders, counts);
    }

    private static KeyedValue<string, long> Row(string region, long count) => new(region, count);

    [Fact]
    public async Task Multi_partition_replay_is_exactly_once_per_partition()
    {
        var storage = new InMemoryCheckpointStorageProvider();

        var first = CountByRegion(storage);
        await using (var circuit = first.Builder.Build())
        {
            circuit.Start();
            var input = circuit.Handle(first.Orders);
            var output = circuit.Handle(first.Counts);
            await input.SendAsync((1, "emea"), 1, partition: 0, offset: 10);
            await input.SendAsync((2, "emea"), 1, partition: 0, offset: 11);
            await input.SendAsync((3, "emea"), 1, partition: 1, offset: 5);
            await circuit.StepAsync();
            var delta = await output.Reader.ReadAsync();
            Assert.Equal(1, delta.WeightOf(Row("emea", 3)));
        }

        // "Restart": each partition resumes at its own committed position — a replay on one
        // partition cannot be deduplicated against another's offsets.
        var second = CountByRegion(storage);
        await using var resumed = second.Builder.Build();
        resumed.Start();
        Assert.Equal(new Dictionary<int, long> { [0] = 11, [1] = 5 }, resumed.CommittedOffsets("orders"));

        var replay = resumed.Handle(second.Orders);
        var counts = resumed.Handle(second.Counts);
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
        var pipeline = CountByRegion(storage);
        await using var circuit = pipeline.Builder.Build();
        circuit.Start();
        var input = circuit.Handle(pipeline.Orders);

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
        var pipeline = CountByRegion(new InMemoryCheckpointStorageProvider());
        await using var circuit = pipeline.Builder.Build();
        circuit.Start();

        var input = circuit.Handle(pipeline.Orders);
        await input.SendAsync((1, "emea"), 1, offset: 0);
        var error = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await input.InsertAsync((2, "emea")));
        Assert.Contains("replay position", error.Message);

        // And the other way round, on a fresh circuit.
        var otherPipeline = CountByRegion(new InMemoryCheckpointStorageProvider());
        await using var other = otherPipeline.Builder.Build();
        other.Start();
        var unstamped = other.Handle(otherPipeline.Orders);
        await unstamped.InsertAsync((1, "emea"));
        await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await unstamped.SendAsync((2, "emea"), 1, offset: 0));
    }

    [Fact]
    public async Task Switching_stamping_across_a_restart_faults_the_circuit()
    {
        var storage = new InMemoryCheckpointStorageProvider();

        var first = CountByRegion(storage);
        await using (var circuit = first.Builder.Build())
        {
            circuit.Start();
            var input = circuit.Handle(first.Orders);
            await input.SendAsync((1, "emea"), 1, offset: 10);
            await circuit.StepAsync();
        }

        // A fresh handle has no send history, so only the input's recovered committed
        // position can catch the switch — at the step, as a fault, not silently.
        var second = CountByRegion(storage);
        await using var resumed = second.Builder.Build();
        resumed.Start();
        var replay = resumed.Handle(second.Orders);
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

        var first = CountByRegion(storage);
        await using (var circuit = first.Builder.Build())
        {
            circuit.Start();
            var input = circuit.Handle(first.Orders);
            await input.InsertAsync((1, "emea"));
            await input.InsertAsync((2, "emea"));
            await circuit.StepAsync();
            Assert.Equal(1L, circuit.CommittedOffset("orders")); // numbered 0, 1, ...
        }

        var second = CountByRegion(storage);
        await using var resumed = second.Builder.Build();
        resumed.Start();
        Assert.Equal(1L, resumed.CommittedOffset("orders"));
        Assert.Empty(resumed.CommittedOffsets("orders")); // a running count has no partitions

        var input2 = resumed.Handle(second.Orders);
        await input2.InsertAsync((3, "emea"));
        await resumed.StepAsync();
        Assert.Equal(2L, resumed.CommittedOffset("orders"));
    }

    [Fact]
    public async Task Multi_partition_checkpoint_round_trips_through_redis()
    {
        using var redis = ConnectOrSkip();
        var prefix = $"sp-test:{Guid.NewGuid():N}";
        RedisStorageProvider Storage() => new(redis, new RedisStorageOptions { KeyPrefix = prefix });

        var first = CountByRegion(Storage());
        await using (var circuit = first.Builder.Build())
        {
            circuit.Start();
            var input = circuit.Handle(first.Orders);
            await input.SendAsync((1, "emea"), 1, partition: 0, offset: 7);
            await input.SendAsync((2, "emea"), 1, partition: 3, offset: 40);
            await circuit.StepAsync();
        }

        // A fresh provider over the same prefix: the per-partition marker is durable, and
        // replays deduplicate per partition against it.
        var second = CountByRegion(Storage());
        await using var resumed = second.Builder.Build();
        resumed.Start();
        Assert.Equal(new Dictionary<int, long> { [0] = 7, [3] = 40 }, resumed.CommittedOffsets("orders"));

        var replay = resumed.Handle(second.Orders);
        var counts = resumed.Handle(second.Counts);
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
            throw Xunit.Sdk.SkipException.ForSkip("Redis is not reachable on localhost:6379 — run `docker compose up -d`.");
        }
    }
    /// <summary>A position with no record behind it: a source that has read past a
    /// transaction marker, or a record that carried nothing for the input, says so, and the
    /// checkpoint records it like a record's offset.</summary>
    [Fact]
    public async Task An_advance_moves_the_committed_position_without_a_record()
    {
        var storage = new InMemoryCheckpointStorageProvider();
        Circuit Build(out InputStream<(int Id, string Region)> orders, out KeyedInputStream<int, string> names, out Output<int> lengths)
        {
            var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Storage = storage, Name = "advance" });
            orders = builder.Input<(int Id, string Region)>("orders");
            names = builder.KeyedInput<int, string>("names", s => s.Length);
            lengths = orders.Select(o => o.Id).Concat(names.Select(s => s.Length)).ToOutput();
            return builder.Build();
        }

        var circuit = Build(out var orders, out var names, out var lengths);
        circuit.Start();
        await circuit.Handle(orders).InsertAsync((1, "emea"), 0, 10);
        await circuit.Handle(orders).AdvanceAsync(0, 12);         // 11 and 12 held nothing for us
        await circuit.Handle(orders).AdvanceAsync(3, 99);         // a partition that has handed on no record at all
        await circuit.Handle(names).UpsertAsync("ab", 1, 5);
        await circuit.Handle(names).AdvanceAsync(1, 6);
        await circuit.StepAsync();
        var delta = await circuit.Handle(lengths).Reader.ReadAsync();
        Assert.Equal(2, delta.DistinctCount);                             // the two records, and nothing for the advances
        Assert.Equal(new Dictionary<int, long> { [0] = 12, [3] = 99 }, circuit.CommittedOffsets("orders"));
        Assert.Equal(new Dictionary<int, long> { [1] = 6 }, circuit.CommittedOffsets("names"));

        // An advance alone is a step's worth of input: it is committed without a record.
        await circuit.Handle(orders).AdvanceAsync(0, 20);
        await circuit.StepAsync();
        Assert.Equal(20, circuit.CommittedOffsets("orders")[0]);
        Assert.False(circuit.Handle(lengths).Reader.TryRead(out _));
        await circuit.DisposeAsync();

        // After a restart the position holds: a record at or below it is a replay, and an
        // advance to a lower offset moves nothing back.
        var restarted = Build(out orders, out names, out lengths);
        restarted.Start();
        Assert.Equal(new Dictionary<int, long> { [0] = 20, [3] = 99 }, restarted.CommittedOffsets("orders"));
        await restarted.Handle(orders).InsertAsync((2, "emea"), 0, 20);   // dropped
        await restarted.Handle(orders).AdvanceAsync(0, 15);               // nothing
        await restarted.Handle(orders).InsertAsync((3, "emea"), 0, 21);
        await restarted.StepAsync();
        var resumed = await restarted.Handle(lengths).Reader.ReadAsync();
        Assert.Equal(1, resumed.WeightOf(3));
        Assert.Equal(0, resumed.WeightOf(2));
        Assert.Equal(21, restarted.CommittedOffsets("orders")[0]);

        // An advance is a stamped entry: it cannot be mixed with unstamped records.
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await restarted.Handle(orders).InsertAsync((9, "x")));
        await restarted.DisposeAsync();
    }
}
