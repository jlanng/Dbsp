using StackExchange.Redis;
using Incremental.Cluster;
using Incremental.Storage;
using Incremental.Storage.Redis;

namespace Incremental.Tests;

/// <summary>
/// Keyed inputs keep their current rows as checkpointed circuit state, so a restart can
/// re-feed them: re-upserting rows the recovered state already holds changes nothing, a
/// full replay of the command log converges on the log's final table, and offset-stamped
/// commands replay exactly-once. Also: several commands for one key within a step resolve
/// last-write-wins on every worker count, and across a cluster.
/// </summary>
public class KeyedInputRestartTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(20);

    private sealed record RegionCircuit(
        CircuitBuilder Builder,
        KeyedInputStream<int, (int Id, string Region)> Customers,
        Output<(int Id, string Region)> Rows,
        Output<KeyedValue<string, long>> Counts);

    private static RegionCircuit RegionCounts(CircuitOptions options)
    {
        var builder = new CircuitBuilder(options);
        var customers = builder.KeyedInput<int, (int Id, string Region)>("customers", c => c.Id);
        var rows = customers.ToOutput();
        var counts = customers.GroupBy(c => c.Region).Count().ToOutput();
        return new RegionCircuit(builder, customers, rows, counts);
    }

    private static CircuitOptions Checkpointed(IStorageProvider storage, int workers = 1, int interval = 1) => new()
    {
        AutoStep = false,
        Storage = storage,
        CheckpointInterval = interval,
        Workers = workers,
    };

    private static void Integrate<T>(ZSet<T> into, OutputHandle<T> output) where T : notnull
    {
        while (output.Reader.TryRead(out var delta))
            into.AddAll(delta);
    }

    private static void AssertSameZSet<T>(ZSet<T> expected, ZSet<T> actual) where T : notnull
    {
        Assert.Equal(expected.DistinctCount, actual.DistinctCount);
        foreach (var (value, weight) in expected)
            Assert.Equal(weight, actual.WeightOf(value));
    }

    private static ZSet<KeyedValue<string, long>> ExpectedCounts(IEnumerable<(int Id, string Region)> table)
    {
        var expected = new ZSet<KeyedValue<string, long>>();
        foreach (var group in table.GroupBy(r => r.Region))
            expected.Add(new KeyedValue<string, long>(group.Key, group.Count()), 1);
        return expected;
    }

    /// <summary>The integrated state of a table-shaped output: its bootstrap batch.</summary>
    private static async Task<ZSet<T>> Snapshot<T>(OutputHandle<T> output) where T : notnull
    {
        await using var changes = output.Changes().GetAsyncEnumerator();
        Assert.True(await changes.MoveNextAsync().AsTask().WaitAsync(Timeout));
        return changes.Current;
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task Several_commands_for_one_key_in_a_step_resolve_last_write_wins(int workers)
    {
        var defs = RegionCounts(new CircuitOptions { AutoStep = false, Workers = workers });
        await using var circuit = defs.Builder.Build();
        circuit.Start();
        var input = circuit.Handle(defs.Customers);
        var rows = circuit.Handle(defs.Rows);

        await input.UpsertAsync((1, "a"));
        await input.UpsertAsync((1, "b"));
        await input.DeleteAsync(1);
        await input.UpsertAsync((1, "c"));
        await input.UpsertAsync((2, "x"));
        await input.DeleteAsync(2);          // inserted and deleted within the step: never visible
        await input.DeleteAsync(3);          // absent: nothing
        await circuit.StepAsync();
        var delta = new ZSet<(int, string)>();
        Integrate(delta, rows);
        AssertSameZSet(new ZSet<(int, string)> { { (1, "c"), 1 } }, delta);

        // A detour that ends where it started changes nothing.
        await input.UpsertAsync((1, "d"));
        await input.UpsertAsync((1, "c"));
        await circuit.StepAsync();
        Assert.False(rows.Reader.TryRead(out _));

        // The delete-then-reinsert of a stored row, resolved within a step, is also a no-op.
        await input.DeleteAsync(1);
        await input.UpsertAsync((1, "c"));
        await circuit.StepAsync();
        Assert.False(rows.Reader.TryRead(out _));
    }

    [Fact]
    public async Task Re_upserting_the_table_after_a_restart_changes_nothing()
    {
        var storage = new InMemoryCheckpointStorageProvider();
        var rows = new ZSet<(int, string)>();

        var first = RegionCounts(Checkpointed(storage));
        {
            var crashed = first.Builder.Build();   // abandoned below, never disposed
            crashed.Start();
            var input = crashed.Handle(first.Customers);
            await input.UpsertAsync((1, "emea"));
            await input.UpsertAsync((2, "emea"));
            await input.UpsertAsync((3, "apac"));
            await crashed.StepAsync();
            await input.UpsertAsync((2, "apac"));
            await crashed.StepAsync();
            Integrate(rows, crashed.Handle(first.Rows));
        }

        var second = RegionCounts(Checkpointed(storage));
        await using var resumed = second.Builder.Build();
        resumed.Start();
        var refeed = resumed.Handle(second.Customers);
        var resumedRows = resumed.Handle(second.Rows);

        // The producer re-sends its whole table: the recovered state already holds it.
        foreach (var row in new[] { (1, "emea"), (2, "apac"), (3, "apac") })
            await refeed.UpsertAsync(row);
        await resumed.StepAsync();
        Assert.False(resumedRows.Reader.TryRead(out _));

        // And an amendment after the restart retracts the recovered row — not nothing.
        await refeed.UpsertAsync((3, "amer"));
        await resumed.StepAsync();
        Integrate(rows, resumedRows);

        AssertSameZSet(new ZSet<(int, string)> { { (1, "emea"), 1 }, { (2, "apac"), 1 }, { (3, "amer"), 1 } }, rows);
        AssertSameZSet(ExpectedCounts([(1, "emea"), (2, "apac"), (3, "amer")]), await Snapshot(resumed.Handle(second.Counts)));
    }

    [Fact]
    public async Task Stamped_commands_replay_exactly_once()
    {
        var storage = new InMemoryCheckpointStorageProvider();
        (int Id, string Region, bool Delete)[] log =
        [
            (1, "emea", false), (2, "emea", false), (3, "apac", false), (2, "apac", false),
            (1, "", true), (4, "amer", false), (4, "emea", false),
        ];

        async Task Send(KeyedInputHandle<int, (int Id, string Region)> input, int offset)
        {
            var (id, region, delete) = log[offset];
            if (delete)
                await input.DeleteAsync(id, offset: offset);
            else
                await input.UpsertAsync((id, region), offset: offset);
        }

        var first = RegionCounts(Checkpointed(storage));
        {
            var crashed = first.Builder.Build();
            crashed.Start();
            var input = crashed.Handle(first.Customers);
            for (var offset = 0; offset < 5; offset++)
                await Send(input, offset);
            await crashed.StepAsync();
        }

        var second = RegionCounts(Checkpointed(storage));
        await using var resumed = second.Builder.Build();
        resumed.Start();
        Assert.Equal(4L, resumed.CommittedOffset("customers"));

        // Replay the whole log from the start: the committed prefix is dropped, not
        // re-applied — the replayed (1, "emea") upsert must not resurrect deleted key 1.
        var replay = resumed.Handle(second.Customers);
        var rows = resumed.Handle(second.Rows);
        for (var offset = 0; offset < log.Length; offset++)
            await Send(replay, offset);
        await resumed.StepAsync();
        var delta = new ZSet<(int, string)>();
        Integrate(delta, rows);

        AssertSameZSet(new ZSet<(int, string)> { { (4, "emea"), 1 } }, delta);
        Assert.Equal(6L, resumed.CommittedOffset("customers"));
        AssertSameZSet(ExpectedCounts([(2, "apac"), (3, "apac"), (4, "emea")]), await Snapshot(resumed.Handle(second.Counts)));
    }

    /// <summary>A random command log over a small keyspace, stepped in random chunks, crashed
    /// mid-log with uncommitted steps lost (CheckpointInterval 3), then recovered by
    /// re-feeding — the whole log unstamped, or from the start stamped — and continued.
    /// Either way the circuit's table must equal the log's final table.</summary>
    [Theory]
    [InlineData(1, false, 11)]
    [InlineData(3, false, 12)]
    [InlineData(1, true, 13)]
    [InlineData(3, true, 14)]
    public async Task Random_command_log_survives_a_crash_and_refeed(int workers, bool stamped, int seed)
    {
        var rng = new Random(seed);
        var regions = new[] { "emea", "apac", "amer", "latam" };
        var log = Enumerable.Range(0, 400)
            .Select(_ => (Id: rng.Next(40), Region: regions[rng.Next(regions.Length)], Delete: rng.NextDouble() < 0.2))
            .ToArray();

        async Task Send(KeyedInputHandle<int, (int Id, string Region)> input, int offset)
        {
            var (id, region, delete) = log[offset];
            if (stamped && delete) await input.DeleteAsync(id, offset: offset);
            else if (stamped) await input.UpsertAsync((id, region), offset: offset);
            else if (delete) await input.DeleteAsync(id);
            else await input.UpsertAsync((id, region));
        }

        async Task Feed(Circuit circuit, KeyedInputHandle<int, (int Id, string Region)> input, int from, int to)
        {
            for (var offset = from; offset < to;)
            {
                var chunk = Math.Min(to - offset, 1 + rng.Next(25));
                for (var end = offset + chunk; offset < end; offset++)
                    await Send(input, offset);
                await circuit.StepAsync().WaitAsync(Timeout);
            }
        }

        var storage = new InMemoryCheckpointStorageProvider();
        var first = RegionCounts(Checkpointed(storage, workers, interval: 3));
        {
            var crashed = first.Builder.Build();
            crashed.Start();
            await Feed(crashed, crashed.Handle(first.Customers), 0, 250);
        }

        var second = RegionCounts(Checkpointed(storage, workers, interval: 3));
        await using var resumed = second.Builder.Build();
        resumed.Start();
        await Feed(resumed, resumed.Handle(second.Customers), 0, log.Length);

        var table = new Dictionary<int, (int Id, string Region)>();
        foreach (var (id, region, delete) in log)
            if (delete) table.Remove(id);
            else table[id] = (id, region);

        AssertSameZSet(ExpectedCounts(table.Values), await Snapshot(resumed.Handle(second.Counts)));
    }

    [Fact]
    public async Task Keyed_input_state_is_named_by_the_input_and_survives_topology_edits()
    {
        var storage = new InMemoryCheckpointStorageProvider();
        {
            var builder = new CircuitBuilder(Checkpointed(storage));
            var customers = builder.KeyedInput<int, (int Id, string Region)>("customers", c => c.Id);
            customers.ToOutput();
            await using var circuit = builder.Build();
            circuit.Start();
            await circuit.Handle(customers).UpsertAsync((1, "emea"));
            await circuit.StepAsync();
        }

        // Another input declared first renumbers every node — the keyed input's state
        // is not positional, so the circuit still resumes and still retracts.
        var edited = new CircuitBuilder(Checkpointed(storage));
        edited.Input<int>("other").ToOutput();
        var resumedCustomers = edited.KeyedInput<int, (int Id, string Region)>("customers", c => c.Id);
        var rowsDef = resumedCustomers.ToOutput();
        await using var resumed = edited.Build();
        resumed.Start();
        await resumed.Handle(resumedCustomers).UpsertAsync((1, "apac"));
        await resumed.StepAsync();
        var delta = await resumed.Handle(rowsDef).Reader.ReadAsync();
        Assert.Equal(-1, delta.WeightOf((1, "emea")));
        Assert.Equal(1, delta.WeightOf((1, "apac")));

        var error = Assert.Throws<InvalidOperationException>(() =>
            new CircuitBuilder().KeyedInput<int, (int Id, string Region)>("x", c => c.Id).Named("renamed"));
        Assert.Contains("input name", error.Message);
    }

    [Fact]
    public async Task Keyed_input_across_two_processes_matches_single_process()
    {
        var mesh = InMemoryExchangeTransport.CreateMesh(2);
        CircuitOptions Clustered(IExchangeTransport transport) => new()
        {
            Workers = 2,
            AutoStep = false,
            Cluster = new ClusterOptions { Transport = transport },
        };
        var defs = new[] { RegionCounts(Clustered(mesh[0])), RegionCounts(Clustered(mesh[1])) };
        await using var process0 = defs[0].Builder.Build();
        await using var process1 = defs[1].Builder.Build();
        Circuit[] processes = [process0, process1];
        foreach (var process in processes)
            process.Start();
        Task StepAll() => Task.WhenAll(processes.Select(p => p.StepAsync())).WaitAsync(Timeout);

        // Each step's commands come from one process per key; keys move between producing
        // processes across steps, so retractions must be found in the owning shard, not the
        // sender's.
        var rng = new Random(7);
        var regions = new[] { "emea", "apac", "amer" };
        var table = new Dictionary<int, (int Id, string Region)>();
        for (var step = 0; step < 30; step++)
        {
            foreach (var id in Enumerable.Range(0, 30).OrderBy(_ => rng.Next()).Take(8))
            {
                var process = rng.Next(2);
                var input = processes[process].Handle(defs[process].Customers);
                if (rng.NextDouble() < 0.2)
                {
                    await input.DeleteAsync(id);
                    table.Remove(id);
                }
                else
                {
                    var row = (id, regions[rng.Next(regions.Length)]);
                    await input.UpsertAsync(row);
                    await input.UpsertAsync((id, "detour"));   // same process, same step: overwritten
                    await input.UpsertAsync(row);
                    table[id] = row;
                }
            }
            await StepAll();
        }

        var rows = new ZSet<(int, string)>();
        Integrate(rows, process0.Handle(defs[0].Rows));
        Integrate(rows, process1.Handle(defs[1].Rows));
        var expected = new ZSet<(int, string)>();
        foreach (var row in table.Values)
            expected.Add(row, 1);
        AssertSameZSet(expected, rows);
    }

    [Fact]
    public async Task Keyed_input_resumes_from_redis_and_takes_a_refeed()
    {
        ConnectionMultiplexer redis;
        try
        {
            redis = ConnectionMultiplexer.Connect("localhost:6379,connectTimeout=1000,abortConnect=true");
        }
        catch (RedisConnectionException)
        {
            throw Xunit.Sdk.SkipException.ForSkip("Redis is not reachable on localhost:6379 — run `docker compose up -d`.");
        }
        using var _ = redis;
        var options = new RedisStorageOptions { KeyPrefix = $"keyed-test:{Guid.NewGuid():N}" };

        var first = RegionCounts(Checkpointed(new RedisStorageProvider(redis, options), workers: 2));
        {
            var crashed = first.Builder.Build();
            crashed.Start();
            var input = crashed.Handle(first.Customers);
            foreach (var row in new[] { (1, "emea"), (2, "emea"), (3, "apac") })
                await input.UpsertAsync(row);
            await crashed.StepAsync();
        }

        var second = RegionCounts(Checkpointed(new RedisStorageProvider(redis, options), workers: 2));
        await using var resumed = second.Builder.Build();
        resumed.Start();
        var refeed = resumed.Handle(second.Customers);
        foreach (var row in new[] { (1, "emea"), (2, "apac"), (3, "apac") })
            await refeed.UpsertAsync(row);
        await resumed.StepAsync();

        var delta = new ZSet<(int, string)>();
        Integrate(delta, resumed.Handle(second.Rows));
        AssertSameZSet(new ZSet<(int, string)> { { (2, "emea"), -1 }, { (2, "apac"), 1 } }, delta);
        AssertSameZSet(ExpectedCounts([(1, "emea"), (2, "apac"), (3, "apac")]), await Snapshot(resumed.Handle(second.Counts)));
    }
}
