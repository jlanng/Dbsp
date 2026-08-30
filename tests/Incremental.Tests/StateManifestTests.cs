using StackExchange.Redis;
using Incremental.Storage;
using Incremental.Storage.Redis;

namespace Incremental.Tests;

/// <summary>
/// State-manifest validation: every checkpoint commits a manifest of the circuit's state
/// layout (store names + worker/process shape), and <see cref="Circuit.Start"/> refuses to
/// resume a checkpoint the rebuilt circuit no longer matches — positional state identities
/// renumber under topology changes, so a silent restore would read the wrong operators'
/// state. <c>.Named</c> pins an operator's identity so deliberate evolution keeps its state.
/// </summary>
public class StateManifestTests
{
    private static CircuitBuilder Pipeline(
        IStorageProvider storage,
        int workers = 1,
        bool extraFilter = false,
        bool named = false,
        bool extraBranch = false)
    {
        var builder = new CircuitBuilder(new CircuitOptions
        {
            AutoStep = false,
            Workers = workers,
            Storage = storage,
        });
        var orders = builder.Input<(int Id, string Region)>("orders");
        var stream = extraFilter ? orders.Filter(o => o.Id != 0) : orders;
        var counts = stream.IndexBy(o => o.Region).Count();
        if (named)
            counts = counts.Named("region-counts");
        counts.Output("counts");
        if (extraBranch)
            orders.IndexBy(o => o.Id).Count().Output("byId");
        return builder;
    }

    private static KeyedValue<string, long> Row(string region, long count) => new(region, count);

    private static async Task<InMemoryCheckpointStorageProvider> CommittedRun(bool named = false, int workers = 1)
    {
        var storage = new InMemoryCheckpointStorageProvider();
        await using var circuit = Pipeline(storage, workers: workers, named: named).Build();
        circuit.Start();
        var input = circuit.Input<(int Id, string Region)>("orders");
        await input.SendAsync((1, "emea"), 1, offset: 10);
        await input.SendAsync((2, "emea"), 1, offset: 11);
        await circuit.StepAsync();
        var delta = await circuit.Output<KeyedValue<string, long>>("counts").Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(Row("emea", 2)));
        return storage;
    }

    [Fact]
    public async Task Inserting_an_operator_refuses_the_restore()
    {
        var storage = await CommittedRun();

        // The extra filter renumbers every node downstream of the input, so the count's
        // positional store name no longer matches the committed one — silent corruption
        // territory, refused loudly at Start instead.
        await using var evolved = Pipeline(storage, extraFilter: true).Build();
        var error = Assert.Throws<InvalidOperationException>(evolved.Start);
        Assert.Contains("does not match the checkpoint", error.Message);
        Assert.Contains(".Named", error.Message);
        Assert.Contains("aggregateLinear", error.Message); // names the orphaned store
    }

    [Fact]
    public async Task Named_operator_survives_topology_evolution()
    {
        var storage = await CommittedRun(named: true);

        // Same evolution, but the count's state is keyed by its name, not its position:
        // the rebuilt circuit resumes it and the new record lands on the recovered 2.
        await using var evolved = Pipeline(storage, extraFilter: true, named: true).Build();
        evolved.Start();
        Assert.Equal(11L, evolved.CommittedOffset("orders"));

        var input = evolved.Input<(int Id, string Region)>("orders");
        await input.SendAsync((3, "emea"), 1, offset: 12);
        await evolved.StepAsync();
        var delta = await evolved.Output<KeyedValue<string, long>>("counts").Reader.ReadAsync();
        Assert.Equal(-1, delta.WeightOf(Row("emea", 2)));
        Assert.Equal(1, delta.WeightOf(Row("emea", 3)));
    }

    [Fact]
    public async Task Worker_count_change_refuses_the_restore()
    {
        var storage = await CommittedRun(workers: 2);

        await using var evolved = Pipeline(storage, workers: 1).Build();
        var error = Assert.Throws<InvalidOperationException>(evolved.Start);
        Assert.Contains("workers", error.Message);
        Assert.Contains("workers=2", error.Message);
        Assert.Contains("workers=1", error.Message);
    }

    [Fact]
    public async Task Pure_additions_are_allowed()
    {
        var storage = await CommittedRun();

        // Appending a new branch adds stores (they start empty) without disturbing any
        // existing identity: the restore proceeds and the old state is intact.
        await using var evolved = Pipeline(storage, extraBranch: true).Build();
        evolved.Start();

        var input = evolved.Input<(int Id, string Region)>("orders");
        await input.SendAsync((3, "emea"), 1, offset: 12);
        await evolved.StepAsync();
        var delta = await evolved.Output<KeyedValue<string, long>>("counts").Reader.ReadAsync();
        Assert.Equal(-1, delta.WeightOf(Row("emea", 2)));
        Assert.Equal(1, delta.WeightOf(Row("emea", 3)));
    }

    [Fact]
    public void Duplicate_operator_names_are_rejected_at_build()
    {
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        var orders = builder.Input<(int Id, string Region)>("orders");
        orders.IndexBy(o => o.Region).Count().Named("counts").Output("byRegion");
        orders.IndexBy(o => o.Id).Count().Named("counts").Output("byId");
        var error = Assert.Throws<InvalidOperationException>(() => builder.Build());
        Assert.Contains("'counts'", error.Message);
    }

    [Fact]
    public void Invalid_operator_names_are_rejected_immediately()
    {
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        var counts = builder.Input<(int Id, string Region)>("orders").IndexBy(o => o.Region).Count();
        Assert.Throws<ArgumentException>(() => counts.Named("has space"));
        Assert.Throws<ArgumentException>(() => counts.Named("dotted.name"));
        Assert.Throws<ArgumentException>(() => counts.Named(""));
        counts.Named("region-counts_1");
        Assert.Throws<InvalidOperationException>(() => counts.Named("renamed")); // identity already pinned
    }

    [SkippableFact]
    public async Task Redis_restore_with_changed_topology_is_refused()
    {
        using var redis = ConnectOrSkip();
        var prefix = $"sp-test:{Guid.NewGuid():N}";

        var storage = new RedisStorageProvider(redis, new RedisStorageOptions { KeyPrefix = prefix });
        await using (var circuit = Pipeline(storage).Build())
        {
            circuit.Start();
            var input = circuit.Input<(int Id, string Region)>("orders");
            await input.SendAsync((1, "emea"), 1, offset: 10);
            await circuit.StepAsync();
            var delta = await circuit.Output<KeyedValue<string, long>>("counts").Reader.ReadAsync();
            Assert.Equal(1, delta.WeightOf(Row("emea", 1)));
        }

        // A fresh provider over the same prefix — the real restart shape. The identical
        // circuit resumes; the evolved one is refused by the committed manifest.
        var resumedStorage = new RedisStorageProvider(redis, new RedisStorageOptions { KeyPrefix = prefix });
        await using (var resumed = Pipeline(resumedStorage).Build())
        {
            resumed.Start();
            Assert.Equal(10L, resumed.CommittedOffset("orders"));
        }

        var evolvedStorage = new RedisStorageProvider(redis, new RedisStorageOptions { KeyPrefix = prefix });
        await using var evolved = Pipeline(evolvedStorage, extraFilter: true).Build();
        var error = Assert.Throws<InvalidOperationException>(evolved.Start);
        Assert.Contains("does not match the checkpoint", error.Message);
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
