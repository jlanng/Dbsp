using StackExchange.Redis;
using Dbsp.Storage;
using Dbsp.Storage.Redis;

namespace Dbsp.Tests;

/// <summary>
/// Checkpointing: a circuit whose storage provider implements
/// <see cref="ICheckpointStorageProvider"/> commits {staged state changes + consumed input
/// offsets} atomically every <see cref="CircuitOptions.CheckpointInterval"/> steps. The
/// semantics are tested against the in-memory provider ("crash" = abandoning a circuit
/// without disposing, so nothing staged is committed); the Redis-backed tests (skipped when
/// no local Redis is reachable — `docker compose up -d`) crash a real circuit mid-stream
/// and verify the resumed run is exactly-once.
/// </summary>
public class CheckpointTests
{
    private sealed record CountPipeline(
        CircuitBuilder Builder,
        InputStream<(int Id, string Region)> Orders,
        Output<KeyedValue<string, long>> Counts);

    private static CountPipeline CountByRegion(IStorageProvider storage, int workers = 1, int interval = 1)
    {
        var builder = new CircuitBuilder(new CircuitOptions
        {
            AutoStep = false,
            Workers = workers,
            Storage = storage,
            CheckpointInterval = interval,
        });
        var orders = builder.Input<(int Id, string Region)>("orders");
        // bootstrap: false — these tests integrate raw per-step batches themselves.
        var counts = orders.GroupBy(o => o.Region).Count().ToOutput(bootstrap: false);
        return new CountPipeline(builder, orders, counts);
    }

    private static KeyedValue<string, long> Row(string region, long count) => new(region, count);

    [Fact]
    public async Task Offsets_advance_as_steps_commit()
    {
        var storage = new InMemoryCheckpointStorageProvider();
        var pipeline = CountByRegion(storage);
        await using var circuit = pipeline.Builder.Build();
        circuit.Start();
        var input = circuit.Handle(pipeline.Orders);

        Assert.Null(circuit.CommittedOffset("orders"));
        Assert.Throws<KeyNotFoundException>(() => circuit.CommittedOffset("nope"));

        await input.InsertAsync((1, "emea"));
        await input.InsertAsync((2, "apac"));
        await circuit.StepAsync();
        Assert.Equal(1L, circuit.CommittedOffset("orders")); // unstamped records are numbered 0, 1, ...

        await input.InsertAsync((3, "emea"));
        await circuit.StepAsync();
        Assert.Equal(2L, circuit.CommittedOffset("orders"));
    }

    [Fact]
    public async Task Restart_resumes_at_committed_offset_and_ignores_replay()
    {
        var storage = new InMemoryCheckpointStorageProvider();

        var pipeline = CountByRegion(storage);
        await using (var circuit = pipeline.Builder.Build())
        {
            circuit.Start();
            var input = circuit.Handle(pipeline.Orders);
            var output = circuit.Handle(pipeline.Counts);
            await input.SendAsync((1, "emea"), 1, offset: 10);
            await input.SendAsync((2, "emea"), 1, offset: 11);
            await circuit.StepAsync();
            var delta = await output.Reader.ReadAsync();
            Assert.Equal(1, delta.WeightOf(Row("emea", 2)));
        }

        // "Restart": a new circuit over the same provider recovers the committed offsets,
        // and a producer replaying its source is deduplicated up to the checkpoint.
        var resumedPipeline = CountByRegion(storage);
        await using var resumed = resumedPipeline.Builder.Build();
        resumed.Start();
        Assert.Equal(11L, resumed.CommittedOffset("orders"));

        var replay = resumed.Handle(resumedPipeline.Orders);
        var counts = resumed.Handle(resumedPipeline.Counts);
        await replay.SendAsync((1, "emea"), 1, offset: 10); // replayed — dropped
        await replay.SendAsync((2, "emea"), 1, offset: 11); // replayed — dropped
        await replay.SendAsync((3, "emea"), 1, offset: 12); // new
        await resumed.StepAsync();
        var resumedDelta = await counts.Reader.ReadAsync();

        // Exactly one new record applied on top of the recovered state: 2 → 3, not 2 → 5.
        Assert.Equal(-1, resumedDelta.WeightOf(Row("emea", 2)));
        Assert.Equal(1, resumedDelta.WeightOf(Row("emea", 3)));
        Assert.Equal(12L, resumed.CommittedOffset("orders"));
    }

    [Fact]
    public async Task Uncommitted_writes_are_not_visible_after_restart()
    {
        var storage = new InMemoryCheckpointStorageProvider();

        // Interval 100: the step runs but commits nothing — then the circuit is abandoned
        // without disposing, the in-process equivalent of a crash between checkpoints.
        var crashedPipeline = CountByRegion(storage, interval: 100);
        var crashed = crashedPipeline.Builder.Build();
        crashed.Start();
        var input = crashed.Handle(crashedPipeline.Orders);
        var output = crashed.Handle(crashedPipeline.Counts);
        await input.SendAsync((1, "emea"), 1, offset: 0);
        await crashed.StepAsync();
        var delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(Row("emea", 1))); // staged state is visible in-run
        Assert.Null(crashed.CommittedOffset("orders"));  // but nothing is durable yet

        var resumedPipeline = CountByRegion(storage);
        await using var resumed = resumedPipeline.Builder.Build();
        resumed.Start();
        Assert.Null(resumed.CommittedOffset("orders"));

        // Recovery rolled the staged count back, so re-sending offset 0 is not a duplicate
        // and counts from a clean slate — once, not twice.
        var replay = resumed.Handle(resumedPipeline.Orders);
        var counts = resumed.Handle(resumedPipeline.Counts);
        await replay.SendAsync((1, "emea"), 1, offset: 0);
        await resumed.StepAsync();
        var resumedDelta = await counts.Reader.ReadAsync();
        Assert.Equal(1, resumedDelta.WeightOf(Row("emea", 1)));
        Assert.Equal(0, resumedDelta.WeightOf(Row("emea", 2)));
    }

    [Fact]
    public async Task Interval_batches_commits_and_clean_shutdown_commits_the_tail()
    {
        var storage = new InMemoryCheckpointStorageProvider();
        var pipeline = CountByRegion(storage, interval: 2);
        await using (var circuit = pipeline.Builder.Build())
        {
            circuit.Start();
            var input = circuit.Handle(pipeline.Orders);

            await input.InsertAsync((1, "emea"));
            await circuit.StepAsync();
            Assert.Null(circuit.CommittedOffset("orders"));      // staged, below the interval

            await input.InsertAsync((2, "emea"));
            await circuit.StepAsync();
            Assert.Equal(1L, circuit.CommittedOffset("orders")); // interval reached: both steps commit

            await input.InsertAsync((3, "emea"));
            await circuit.StepAsync();
            Assert.Equal(1L, circuit.CommittedOffset("orders")); // staged again...
        } // ...and committed by the clean shutdown

        await using var resumed = CountByRegion(storage).Builder.Build();
        resumed.Start();
        Assert.Equal(2L, resumed.CommittedOffset("orders"));
    }

    [Fact]
    public async Task Mid_run_redelivery_with_offsets_is_dropped()
    {
        var storage = new InMemoryCheckpointStorageProvider();
        var pipeline = CountByRegion(storage);
        await using var circuit = pipeline.Builder.Build();
        circuit.Start();
        var input = circuit.Handle(pipeline.Orders);
        var output = circuit.Handle(pipeline.Counts);

        await input.SendAsync((1, "emea"), 1, offset: 5);
        await circuit.StepAsync();
        await output.Reader.ReadAsync();

        await input.SendAsync((1, "emea"), 1, offset: 5); // redelivered — dropped
        await input.SendAsync((2, "emea"), 1, offset: 6);
        await circuit.StepAsync();
        var delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(Row("emea", 2)));
        Assert.Equal(0, delta.WeightOf(Row("emea", 3)));
    }

    // ---- Redis-backed: crash a real circuit mid-stream, resume against the same Redis ----

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

    private static (int Id, string Region) Record(int i) => (i, $"r{i % 7}");

    private static void AssertSameZSet(ZSet<KeyedValue<string, long>> expected, ZSet<KeyedValue<string, long>> actual)
    {
        Assert.Equal(expected.DistinctCount, actual.DistinctCount);
        foreach (var (row, weight) in expected)
            Assert.Equal(weight, actual.WeightOf(row));
    }

    [Fact]
    public async Task Crashed_circuit_resumes_exactly_once_from_redis()
    {
        using var redis = ConnectOrSkip();
        var prefix = $"sp-test:{Guid.NewGuid():N}";

        // Reference: the same 100 records through a never-crashed run (its own prefix).
        var reference = new ZSet<KeyedValue<string, long>>();
        var pristinePipeline = CountByRegion(
            new RedisStorageProvider(redis, new RedisStorageOptions { KeyPrefix = $"{prefix}:ref" }));
        await using (var pristine = pristinePipeline.Builder.Build())
        {
            pristine.Start();
            var input = pristine.Handle(pristinePipeline.Orders);
            var output = pristine.Handle(pristinePipeline.Counts);
            for (var i = 0; i < 100; i++)
                await input.SendAsync(Record(i), 1, offset: i);
            await pristine.StepAsync();
            while (output.Reader.TryRead(out var delta))
                reference.AddAll(delta);
        }

        // Crashing run: consume the first 40 records across two steps (two commits), then
        // abandon the circuit without disposing. Redis holds the last committed step.
        var integrated = new ZSet<KeyedValue<string, long>>();
        {
            var crashedPipeline = CountByRegion(
                new RedisStorageProvider(redis, new RedisStorageOptions { KeyPrefix = prefix }));
            var crashed = crashedPipeline.Builder.Build();
            crashed.Start();
            var input = crashed.Handle(crashedPipeline.Orders);
            var output = crashed.Handle(crashedPipeline.Counts);
            for (var i = 0; i < 20; i++)
                await input.SendAsync(Record(i), 1, offset: i);
            await crashed.StepAsync();
            for (var i = 20; i < 40; i++)
                await input.SendAsync(Record(i), 1, offset: i);
            await crashed.StepAsync();
            while (output.Reader.TryRead(out var delta))
                integrated.AddAll(delta);
        }

        // Resume: a fresh provider over the same prefix. The circuit reports where the
        // crashed run got to; the producer seeks there — sloppily, replaying a few
        // already-committed records, which must be dropped.
        var resumedPipeline = CountByRegion(
            new RedisStorageProvider(redis, new RedisStorageOptions { KeyPrefix = prefix }));
        await using var resumed = resumedPipeline.Builder.Build();
        resumed.Start();
        var committed = resumed.CommittedOffset("orders");
        Assert.Equal(39L, committed);

        var replay = resumed.Handle(resumedPipeline.Orders);
        var counts = resumed.Handle(resumedPipeline.Counts);
        for (var i = (int)committed!.Value - 5; i < 100; i++)
            await replay.SendAsync(Record(i), 1, offset: i);
        await resumed.StepAsync();
        while (counts.Reader.TryRead(out var delta))
            integrated.AddAll(delta);

        // Crashed deltas + resumed deltas integrate to exactly the never-crashed result.
        AssertSameZSet(reference, integrated);
    }

    [Fact]
    public async Task Multi_worker_crash_and_resume_is_exactly_once()
    {
        using var redis = ConnectOrSkip();
        var prefix = $"sp-test:{Guid.NewGuid():N}";
        var integrated = new ZSet<KeyedValue<string, long>>();

        {
            var crashedPipeline = CountByRegion(
                new RedisStorageProvider(redis, new RedisStorageOptions { KeyPrefix = prefix }), workers: 4);
            var crashed = crashedPipeline.Builder.Build();
            crashed.Start();
            var input = crashed.Handle(crashedPipeline.Orders);
            var output = crashed.Handle(crashedPipeline.Counts);
            for (var i = 0; i < 30; i++)
                await input.SendAsync(Record(i), 1, offset: i);
            await crashed.StepAsync();
            for (var i = 30; i < 60; i++)
                await input.SendAsync(Record(i), 1, offset: i);
            await crashed.StepAsync();
            while (output.Reader.TryRead(out var delta))
                integrated.AddAll(delta);
            // Abandoned mid-stream: the commit marker covers all four workers' shards.
        }

        var resumedPipeline = CountByRegion(
            new RedisStorageProvider(redis, new RedisStorageOptions { KeyPrefix = prefix }), workers: 4);
        await using var resumed = resumedPipeline.Builder.Build();
        resumed.Start();
        Assert.Equal(59L, resumed.CommittedOffset("orders"));

        var replay = resumed.Handle(resumedPipeline.Orders);
        var counts = resumed.Handle(resumedPipeline.Counts);
        for (var i = 50; i < 150; i++) // overlaps the checkpoint by 10 records
            await replay.SendAsync(Record(i), 1, offset: i);
        await resumed.StepAsync();
        while (counts.Reader.TryRead(out var delta))
            integrated.AddAll(delta);

        // Integrated across crash + resume: one (region, total) row per region, weight +1.
        var expected = new Dictionary<string, long>();
        for (var i = 0; i < 150; i++)
            expected[Record(i).Region] = expected.GetValueOrDefault(Record(i).Region) + 1;
        Assert.Equal(expected.Count, integrated.DistinctCount);
        foreach (var (region, count) in expected)
            Assert.Equal(1, integrated.WeightOf(Row(region, count)));
    }

    [Fact]
    public void Uncommitted_redis_writes_are_invisible_until_commit()
    {
        using var redis = ConnectOrSkip();
        var options = new RedisStorageOptions { KeyPrefix = $"sp-test:{Guid.NewGuid():N}" };

        var provider = new RedisStorageProvider(redis, options);
        var store = provider.CreateWeightStore<string>("staged");
        store.Update("a", 5);

        // Staged only: a second provider over the same prefix sees neither state nor offsets.
        var observer = new RedisStorageProvider(redis, options);
        Assert.Empty(observer.Recover());
        Assert.Equal(0, observer.CreateWeightStore<string>("staged").WeightOf("a"));

        // One commit publishes state and offsets together.
        provider.Commit(new Dictionary<string, long> { ["orders"] = 7 });
        var recovered = new RedisStorageProvider(redis, options);
        Assert.Equal(7, recovered.Recover()["orders"]);
        Assert.Equal(5, recovered.CreateWeightStore<string>("staged").WeightOf("a"));
    }
}
