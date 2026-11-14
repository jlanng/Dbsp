using System.Diagnostics;
using Confluent.Kafka;
using Incremental.Storage;
using Incremental.Tables;

namespace Incremental.Kafka.Tests;

/// <summary>
/// The whole durable path on a real broker: a circuit on <see cref="ChangelogStorage"/>
/// over <see cref="KafkaStateChangelog"/>, reading one shared table and writing another,
/// both over <see cref="KafkaTableLog{TKey,TRow}"/>, delivering to keyed upsert sinks. The
/// active process is dropped where it stands — never shut down — and replaced, by a warm
/// standby that was following (promotion) or by a fresh process that restores from the
/// topics (cold start). The sinks and the derived table must end up holding what an
/// uninterrupted in-memory run produces.
/// <para>
/// The harness plays the sources, as <c>ChangelogFailoverTests</c> does in the core suite:
/// inputs carry offsets, and after a takeover it re-sends everything since the last
/// checkpoint cut into the steps it was first run as; reference-table changes are committed
/// only when the circuit has nothing uncommitted. Those two conditions are what make keyed
/// sinks converge exactly rather than at-least-once (durability-and-standby.md).
/// </para>
/// </summary>
public class KafkaEndToEndTests(ITestOutputHelper output)
{
    private static readonly TimeSpan Timeout = KafkaEnv.Timeout;
    private const string PipelineId = "enrichment";

    private sealed record Order(int Id, int Customer, int Product, int Qty);
    private sealed record Customer(int Id, string Region);
    private sealed record Product(int Id, string Category, int Price);
    private sealed record OrderRegion(int OrderId, int Product, int Qty, string Region);
    private sealed record Enriched(int OrderId, string Region, string Category, long Value);
    private sealed record Priced(int OrderId, int Price);
    private sealed record RegionTotal(string Region, long Total);

    private sealed record Definition(
        CircuitBuilder Builder,
        KeyedInputStream<int, Order> Orders,
        InputStream<Customer> Customers,
        AcknowledgedOutput<KeyedValue<string, long>> Totals,
        AcknowledgedOutput<KeyedValue<string, long>> Largest,
        AcknowledgedOutput<Priced> Priced,
        AcknowledgedOutput<int> Buyers);

    private static Definition Define(
        IStorageProvider storage, SharedTable<int, Product> products, SharedTable<string, RegionTotal> regionTotals, int workers, int interval)
    {
        var builder = new CircuitBuilder(new CircuitOptions
        {
            AutoStep = false,
            Workers = workers,
            Storage = storage,
            CheckpointInterval = interval,
        });
        var orders = builder.KeyedInput<int, Order>("orders", o => o.Id);       // keyed input
        var customers = builder.Input<Customer>("customers");
        var catalogue = builder.Table(products);

        var enriched = orders.GroupBy(o => o.Customer)
            .Join(customers.GroupBy(c => c.Id), (_, o, c) => new OrderRegion(o.Id, o.Product, o.Qty, c.Region)) // stream join
            .GroupBy(x => x.Product)
            .Join(catalogue, (_, x, p) => new Enriched(x.OrderId, x.Region, p.Category, (long)x.Qty * p.Price)); // table join

        var totals = enriched.GroupBy(e => e.Region + "/" + e.Category).Sum(e => e.Value).ToAcknowledgedOutput(); // linear aggregate
        var largest = enriched.GroupBy(e => e.Region).Max(e => e.Value).ToAcknowledgedOutput();                   // tree aggregate
        var priced = orders
            .Lookup(catalogue, o => o.Product, (o, p) => new Priced(o.Id, p.Price), missing: o => new Priced(o.Id, -1)) // lookup
            .ToAcknowledgedOutput();
        var buyers = orders.Select(o => o.Customer).Distinct().ToAcknowledgedOutput();                             // weight store
        enriched.GroupBy(e => e.Region).Sum(e => e.Value)
            .ToTable(regionTotals, (region, total) => new RegionTotal(region, total));                             // derived table
        return new Definition(builder, orders, customers, totals, largest, priced, buyers);
    }

    /// <summary>The sinks: outside the process, so they outlive every active.</summary>
    private sealed class Sinks
    {
        public readonly InMemoryUpsertSink<string, long> Totals = new();
        public readonly InMemoryUpsertSink<string, long> Largest = new();
        public readonly InMemoryUpsertSink<int, Priced> Priced = new();
        public readonly InMemoryUpsertSink<int, int> Buyers = new();

        public void AssertEqual(Sinks expected)
        {
            Assert.Equal(expected.Totals.Rows, Totals.Rows);
            Assert.Equal(expected.Largest.Rows, Largest.Rows);
            Assert.Equal(expected.Priced.Rows, Priced.Rows);
            Assert.Equal(expected.Buyers.Rows, Buyers.Rows);
        }
    }

    /// <summary>One process: its Kafka connection, its storage, its tables, its circuit.</summary>
    private sealed class Node
    {
        public KafkaLog? Log;
        public ChangelogStorage? Storage;
        public required SharedTable<int, Product> Products;
        public required SharedTable<string, RegionTotal> RegionTotals;
        public required Definition Definition;
        public required Circuit Circuit;
        public readonly CancellationTokenSource Kill = new();
        public Task Deliveries = Task.CompletedTask;

        /// <summary>Set when the process is "dead": whatever it still delivers goes nowhere.
        /// (A real zombie's late delivery is the sink gap the design records; these tests
        /// are about what it can make durable, which is nothing.)</summary>
        public volatile bool Detached;

        public KeyedInputHandle<int, Order> Orders => Circuit.Handle(Definition.Orders);
        public InputHandle<Customer> Customers => Circuit.Handle(Definition.Customers);

        public void StartDelivering(Sinks sinks)
        {
            var options = new SinkDeliveryOptions { MaxRetries = 0 };
            Deliveries = Task.WhenAll(
                Circuit.Handle(Definition.Totals).DeliverToAsync(new Detachable<string, long>(sinks.Totals, this), options, Kill.Token),
                Circuit.Handle(Definition.Largest).DeliverToAsync(new Detachable<string, long>(sinks.Largest, this), options, Kill.Token),
                Circuit.Handle(Definition.Priced).DeliverToAsync(new Detachable<int, Priced>(sinks.Priced, this), p => p.OrderId, options, Kill.Token),
                Circuit.Handle(Definition.Buyers).DeliverToAsync(new Detachable<int, int>(sinks.Buyers, this), c => c, options, Kill.Token));
            _ = Deliveries.ContinueWith(t => t.Exception, TaskScheduler.Default); // a killed node's deliveries end in whatever way
        }

        public Dictionary<string, RegionTotal> RegionRows()
        {
            using var snapshot = RegionTotals.Snapshot();
            var rows = new Dictionary<string, RegionTotal>();
            foreach (var region in Regions)
                if (snapshot.TryGet(region, out var row))
                    rows[region] = row;
            return rows;
        }
    }

    private sealed class Detachable<TKey, TRow>(IUpsertSink<TKey, TRow> inner, Node node) : IUpsertSink<TKey, TRow>
        where TKey : notnull
    {
        public ValueTask WriteAsync(UpsertBatch<TKey, TRow> batch, CancellationToken cancellationToken) =>
            node.Detached ? default : inner.WriteAsync(batch, cancellationToken);
    }

    private sealed class Deployment(KafkaEnv env, int workers, int interval, KafkaLogOptions options)
    {
        public readonly Sinks Sinks = new();

        /// <summary>A cold start of the active: connect, take the writer (fencing whoever
        /// had it), restore tables and state from the topics, start.</summary>
        public async Task<Node> StartActiveAsync()
        {
            var log = await env.ConnectAsync(options);
            await log.AcquireWriterAsync().WaitAsync(Timeout);
            var products = await SharedTable<int, Product>.OpenAsync(
                "products", p => p.Id, await log.OpenTableLogAsync<int, Product>("products")).WaitAsync(Timeout);
            var regionTotals = await SharedTable<string, RegionTotal>.OpenAsync(
                "region-totals", r => r.Region, await log.OpenTableLogAsync<string, RegionTotal>("region-totals")).WaitAsync(Timeout);
            var storage = await ChangelogStorage.OpenAsync(log.State).WaitAsync(Timeout);
            var definition = Define(storage.Pipeline(PipelineId), products, regionTotals, workers, interval);
            var node = new Node
            {
                Log = log, Storage = storage, Products = products, RegionTotals = regionTotals,
                Definition = definition, Circuit = definition.Builder.Build(),
            };
            node.Circuit.Start();
            node.StartDelivering(Sinks);
            return node;
        }

        /// <summary>The standby: followers on the state changelog and both table logs, and
        /// the circuit built on what they feed — but not started.</summary>
        public async Task<Node> StartStandbyAsync()
        {
            var log = await env.ConnectAsync(options);
            var storage = ChangelogStorage.Follow(log.State);
            var products = await SharedTable<int, Product>.FollowAsync(
                "products", p => p.Id, await log.OpenTableLogAsync<int, Product>("products")).WaitAsync(Timeout);
            var regionTotals = await SharedTable<string, RegionTotal>.FollowAsync(
                "region-totals", r => r.Region, await log.OpenTableLogAsync<string, RegionTotal>("region-totals")).WaitAsync(Timeout);
            var definition = Define(storage.Pipeline(PipelineId), products, regionTotals, workers, interval);
            return new Node
            {
                Log = log, Storage = storage, Products = products, RegionTotals = regionTotals,
                Definition = definition, Circuit = definition.Builder.Build(),
            };
        }

        /// <summary>The takeover: fence, tables before circuits, start. Returns how long
        /// each part took.</summary>
        public async Task<string> PromoteAsync(Node standby)
        {
            var clock = Stopwatch.StartNew();
            await standby.Log!.AcquireWriterAsync().WaitAsync(Timeout);
            var fenced = clock.Elapsed;
            await standby.Products.PromoteAsync().WaitAsync(Timeout);
            await standby.RegionTotals.PromoteAsync().WaitAsync(Timeout);
            var tables = clock.Elapsed;
            await standby.Storage!.PromoteAsync().WaitAsync(Timeout);
            var storage = clock.Elapsed;
            standby.Circuit.Start();
            clock.Stop();
            standby.StartDelivering(Sinks);
            return $"{clock.Elapsed.TotalMilliseconds:F0} ms (fence and term {fenced.TotalMilliseconds:F0}, two tables " +
                $"{(tables - fenced).TotalMilliseconds:F0}, state {(storage - tables).TotalMilliseconds:F0}, start {(clock.Elapsed - storage).TotalMilliseconds:F0})";
        }
    }

    /// <summary>The uninterrupted run: the same circuit on plain in-memory state and plain
    /// tables, fed everything once.</summary>
    private sealed class Reference : IAsyncDisposable
    {
        public readonly Sinks Sinks = new();
        public readonly Node Node;

        public Reference(int workers)
        {
            var products = new SharedTable<int, Product>("products", p => p.Id);
            var regionTotals = new SharedTable<string, RegionTotal>("region-totals", r => r.Region);
            var definition = Define(new InMemoryStorageProvider(), products, regionTotals, workers, interval: 1);
            Node = new Node { Products = products, RegionTotals = regionTotals, Definition = definition, Circuit = definition.Builder.Build() };
            Node.Circuit.Start();
            Node.StartDelivering(Sinks);
        }

        public async ValueTask DisposeAsync()
        {
            await Node.Circuit.DisposeAsync().AsTask().WaitAsync(Timeout);
            await Node.Deliveries.WaitAsync(Timeout);
        }
    }

    // ── the workload ───────────────────────────────────────────────────────────────────

    private abstract record Op;
    private sealed record OrderOp(long Offset, int Id, Order? Row) : Op;         // null row: delete
    private sealed record CustomerOp(long Offset, Customer Row, bool Insert) : Op;
    private sealed record ProductOp(int Id, Product? Row) : Op;                  // null row: delete
    private sealed record StepOp : Op;

    private static readonly string[] Regions = ["emea", "apac", "amer"];
    private static readonly string[] Categories = ["tools", "toys"];

    /// <summary>A valid random workload ending in <paramref name="steps"/> steps: customers
    /// are a set, so the outputs stay table-shaped.</summary>
    private static List<Op> Plan(int seed, int steps)
    {
        var random = new Random(seed);
        var plan = new List<Op>();
        var customers = new Dictionary<int, Customer>();
        long orderOffset = 0, customerOffset = 0;
        for (var stepped = 0; stepped < steps;)
        {
            var (kind, a, b, c) = (random.Next(13), random.Next(8), random.Next(8), random.Next(10));
            switch (kind)
            {
                case 0 or 1 or 2 or 3:
                    plan.Add(new OrderOp(orderOffset++, a, new Order(a, b % 4, c % 5, 1 + c % 3)));
                    break;
                case 4:
                    plan.Add(new OrderOp(orderOffset++, a, null));
                    break;
                case 5 or 6:
                    var id = a % 4;
                    if (customers.Remove(id, out var present))
                        plan.Add(new CustomerOp(customerOffset++, present, Insert: false));
                    if (kind == 5 || customers.Count < 2)
                    {
                        var customer = new Customer(id, Regions[b % Regions.Length]);
                        customers[id] = customer;
                        plan.Add(new CustomerOp(customerOffset++, customer, Insert: true));
                    }
                    break;
                case 7 or 8:
                    plan.Add(new ProductOp(a % 5, new Product(a % 5, Categories[b % Categories.Length], 1 + c)));
                    break;
                case 9:
                    plan.Add(new ProductOp(a % 5, null));
                    break;
                default:
                    plan.Add(new StepOp());
                    stepped++;
                    break;
            }
        }
        return plan;
    }

    private static async Task SendAsync(Node node, Op op, bool stamped)
    {
        switch (op)
        {
            case OrderOp { Row: { } row } order when stamped: await node.Orders.UpsertAsync(row, order.Offset); break;
            case OrderOp { Row: { } row }: await node.Orders.UpsertAsync(row); break;
            case OrderOp order when stamped: await node.Orders.DeleteAsync(order.Id, order.Offset); break;
            case OrderOp order: await node.Orders.DeleteAsync(order.Id); break;
            case CustomerOp { Insert: true } customer when stamped: await node.Customers.InsertAsync(customer.Row, customer.Offset); break;
            case CustomerOp { Insert: true } customer: await node.Customers.InsertAsync(customer.Row); break;
            case CustomerOp customer when stamped: await node.Customers.RemoveAsync(customer.Row, customer.Offset); break;
            case CustomerOp customer: await node.Customers.RemoveAsync(customer.Row); break;
        }
    }

    private static async Task WriteProductsAsync(SharedTable<int, Product> table, List<ProductOp> ops)
    {
        foreach (var op in ops)
        {
            if (op.Row is { } row)
                await table.Writer.UpsertAsync(row);
            else
                await table.Writer.DeleteAsync(op.Id);
        }
        await table.Writer.CommitAsync().AsTask().WaitAsync(Timeout);
    }

    private enum Crash
    {
        /// <summary>The active is dropped and the standby that was following is promoted.</summary>
        Standby,

        /// <summary>The active is dropped and a new process restores from the topics.</summary>
        Cold,
    }

    private static bool Mentions<T>(Exception? exception) where T : Exception
    {
        for (; exception is not null; exception = exception.InnerException)
        {
            if (exception is T)
                return true;
            if (exception is AggregateException aggregate && aggregate.InnerExceptions.Any(Mentions<T>))
                return true;
        }
        return false;
    }

    /// <summary>Runs the workload against an active on Kafka, crashing it after the steps
    /// <paramref name="crashes"/> names, and against the uninterrupted reference; then
    /// compares the sinks and the derived table.</summary>
    private async Task RunAsync(
        int seed, int steps, int workers, int interval, IReadOnlyDictionary<int, Crash> crashes,
        IReadOnlyDictionary<string, string>? stateTopic = null, Func<KafkaLog, Task>? beforeCrash = null, int statePartitions = 3)
    {
        await using var env = KafkaEnv.Create();
        var options = env.Options(statePartitions: statePartitions, stateTopic: stateTopic);
        var plan = Plan(seed, steps);
        var cluster = new Deployment(env, workers, interval, options);
        await using var reference = new Reference(workers);
        var leader = await cluster.StartActiveAsync();
        Assert.Equal(1, leader.Storage!.Term);
        var standby = await cluster.StartStandbyAsync();
        var dropped = new List<Node>();

        var unstepped = new List<Op>();          // sent since the last step
        var uncommitted = new List<List<Op>>();  // the steps since the last checkpoint, by their inputs
        var products = new List<ProductOp>();    // waiting for a commit boundary
        long? committedStep = null;
        int replayed = 0, takeovers = 0, stepsRun = 0;

        async Task StepLeaderAsync()
        {
            await leader.Circuit.StepAsync().WaitAsync(Timeout);
            if (leader.Circuit.CommittedStep != committedStep)
            {
                committedStep = leader.Circuit.CommittedStep;
                uncommitted.Clear();
            }
        }

        async Task CrashAsync(Crash kind)
        {
            // Dropped where it stands: no dispose, no final commit, cut off from the sinks.
            var killed = leader;
            killed.Detached = true;
            dropped.Add(killed);
            var termBefore = killed.Log!.Term;
            takeovers++;

            if (kind == Crash.Standby)
            {
                var took = await cluster.PromoteAsync(standby);
                KafkaEnv.Measured(output,
                    $"takeover by a caught-up standby: {took}");
                leader = standby;
                standby = await cluster.StartStandbyAsync();
            }
            else
            {
                // The follower is of no use to a cold start; retire it, as a restart of
                // both processes would.
                await standby.Circuit.DisposeAsync().AsTask().WaitAsync(Timeout);
                await standby.Storage!.DisposeAsync();
                await standby.Products.DisposeAsync();
                await standby.RegionTotals.DisposeAsync();
                leader = await cluster.StartActiveAsync();
                standby = await cluster.StartStandbyAsync();
            }

            // The new active resumes from the checkpoint the old one last committed, under
            // the next term, with the reference table as the old one left it.
            Assert.Equal(termBefore + 1, leader.Log!.Term);
            Assert.Equal(leader.Log.Term, leader.Storage!.Term);
            Assert.Equal(committedStep, leader.Circuit.CommittedStep);
            Assert.Equal(killed.Products.LatestVersion, leader.Products.LatestVersion);
            Assert.Equal(killed.Products.Count, leader.Products.Count);

            // The old process is a zombie now: whatever it tries to make durable is fenced.
            // Its circuit faults, and nothing it does reaches the topics.
            await SendAsync(killed, new OrderOp(1_000_000, 7, new Order(7, 0, 0, 99)), stamped: true);
            for (var i = 0; i < interval && killed.Circuit.Fault is null; i++)
            {
                try
                {
                    await killed.Circuit.StepAsync().WaitAsync(Timeout);
                }
                catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
                {
                }
            }
            Assert.True(Mentions<KafkaLogFencedException>(killed.Circuit.Fault),
                $"the zombie's circuit should have faulted as fenced; its fault is: {killed.Circuit.Fault}");
            await killed.Log.WhenFenced.WaitAsync(Timeout);
            Assert.True(killed.Storage!.IsFenced || Mentions<TableLogFencedException>(killed.Circuit.Fault));

            // Replay: the uncommitted steps, cut as they were, then what was never stepped.
            var replay = uncommitted.ToList();
            uncommitted.Clear();
            foreach (var step in replay)
            {
                replayed++;
                foreach (var op in step)
                    await SendAsync(leader, op, stamped: true);
                uncommitted.Add(step);
                await StepLeaderAsync();
            }
            foreach (var op in unstepped)
                await SendAsync(leader, op, stamped: true);
        }

        foreach (var op in plan)
        {
            switch (op)
            {
                case ProductOp product:
                    products.Add(product);
                    continue;
                case OrderOp or CustomerOp:
                    await SendAsync(leader, op, stamped: true);
                    await SendAsync(reference.Node, op, stamped: false);
                    unstepped.Add(op);
                    continue;
            }

            if (uncommitted.Count == 0 && products.Count > 0)
            {
                await WriteProductsAsync(leader.Products, products);
                await WriteProductsAsync(reference.Node.Products, products);
                products.Clear();
            }
            await reference.Node.Circuit.StepAsync().WaitAsync(Timeout);
            uncommitted.Add([.. unstepped]);
            unstepped.Clear();
            await StepLeaderAsync();
            stepsRun++;

            if (crashes.TryGetValue(stepsRun, out var crash))
            {
                if (beforeCrash is not null)
                    await beforeCrash(leader.Log!);
                await CrashAsync(crash);
            }
        }

        // A clean shutdown commits the tail and completes the outputs.
        await leader.Circuit.DisposeAsync().AsTask().WaitAsync(Timeout);
        await leader.Deliveries.WaitAsync(Timeout);
        await reference.DisposeAsync();
        cluster.Sinks.AssertEqual(reference.Sinks);
        Assert.Equal(reference.Node.RegionRows(), leader.RegionRows());
        Assert.Equal(reference.Node.Products.Count, leader.Products.Count);
        Assert.NotEmpty(cluster.Sinks.Totals.Rows);
        Assert.NotEmpty(leader.RegionRows());

        // The run was worth something: it crashed, and it had steps to replay.
        Assert.Equal(crashes.Count, takeovers);
        Assert.True(interval == 1 || replayed > 0, "no crash fell between checkpoints, so nothing was replayed");

        // And a process that has seen none of it reads the same tables out of the topics.
        var last = await env.ConnectAsync(options);
        await last.AcquireWriterAsync().WaitAsync(Timeout);
        var coldTotals = await SharedTable<string, RegionTotal>.OpenAsync(
            "region-totals", r => r.Region, await last.OpenTableLogAsync<string, RegionTotal>("region-totals")).WaitAsync(Timeout);
        Assert.Equal(leader.RegionTotals.LatestVersion, coldTotals.LatestVersion);
        Assert.Equal(leader.RegionTotals.Count, coldTotals.Count);
        var coldStorage = await ChangelogStorage.OpenAsync(last.State).WaitAsync(Timeout);
        Assert.Equal(crashes.Count + 2, coldStorage.Term);
        await coldStorage.DisposeAsync();

        await standby.Circuit.DisposeAsync().AsTask().WaitAsync(Timeout);
        await standby.Storage!.DisposeAsync();
        await standby.Products.DisposeAsync();
        await standby.RegionTotals.DisposeAsync();
        await leader.Storage!.DisposeAsync();
        foreach (var node in dropped)
        {
            node.Kill.Cancel();
            await node.Circuit.DisposeAsync().AsTask().WaitAsync(Timeout);
            await node.Storage!.DisposeAsync();
        }
    }

    [Theory]
    [InlineData(1, 1, 11)]
    [InlineData(1, 3, 12)]
    [InlineData(2, 2, 13)]
    public async Task Dropped_active_is_replaced_by_its_standby_then_by_a_cold_start_and_the_sinks_match_an_uninterrupted_run(
        int workers, int interval, int seed)
    {
        // Crash after steps that are not multiples of the interval, so that uncommitted
        // steps are lost with the process and must be replayed.
        await RunAsync(seed, steps: 36, workers, interval, new Dictionary<int, Crash>
        {
            [10] = Crash.Standby,
            [20] = Crash.Cold,
            [29] = Crash.Standby,
        });
    }

    [Fact]
    public async Task Cold_start_from_a_compacted_state_topic_continues_where_the_active_stopped()
    {
        // Small segments and an eager cleaner, so that by the time the active is dropped
        // the state topic has been compacted under it: superseded records are gone, and
        // the restore reads what the cleaner left.
        var stateTopic = new Dictionary<string, string>
        {
            ["segment.bytes"] = "16384",
            ["min.cleanable.dirty.ratio"] = "0.01",
            ["min.compaction.lag.ms"] = "0",
            ["max.compaction.lag.ms"] = "1000",
        };
        var compacted = false;
        var left = "";

        async Task WaitForCompactionAsync(KafkaLog log)
        {
            // Offsets count every record ever written plus one marker per transaction, and
            // a transaction holds at least one record: fewer records than a quarter of the
            // end offset means more than half of what was written is gone.
            var partition = new TopicPartition(log.State.Topic, 0); // the one state partition of this test
            var waited = Stopwatch.StartNew();
            long held, end;
            do
            {
                await Task.Delay(1000);
                end = await log.EndOffsetAsync(partition, default);
                held = await CountAsync(log, partition);
            }
            while (held > end / 4 && waited.Elapsed < TimeSpan.FromSeconds(90));
            compacted = held <= end / 4;
            left = $"end offset {end}, {held} records left";
            if (compacted)
                KafkaEnv.Measured(output, $"state topic: end offset {end}, {held} records left after compaction ({waited.Elapsed.TotalSeconds:F0} s)");
        }

        await RunSinglePartitionAsync(stateTopic, WaitForCompactionAsync);
        if (!compacted)
            Assert.Skip($"The broker's log cleaner did not compact the state topic within 90 s ({left}).");
    }

    private Task RunSinglePartitionAsync(IReadOnlyDictionary<string, string> stateTopic, Func<KafkaLog, Task> beforeCrash) =>
        RunAsync(seed: 21, steps: 150, workers: 1, interval: 1, new Dictionary<int, Crash> { [140] = Crash.Cold }, stateTopic, beforeCrash, statePartitions: 1);

    private static async Task<long> CountAsync(KafkaLog log, TopicPartition partition)
    {
        var end = await log.EndOffsetAsync(partition, default);
        var count = 0L;
        if (end == 0)
            return count;
        using var stop = new CancellationTokenSource(Timeout);
        await foreach (var batch in log.ReadBatchesAsync(partition, Offset.Beginning, stop.Token))
        {
            count += batch.Records.Count;
            if (batch.NextOffset >= end)
                break;
        }
        return count;
    }
}
