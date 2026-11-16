using CsCheck;
using Incremental.Cluster;
using Incremental.Storage;
using Incremental.Tables;

namespace Incremental.Tests;

/// <summary>
/// Circuits on <see cref="ChangelogStorage"/>: restart from the log, and warm-standby
/// failover. The standby follows the state changelog and the table log, builds the same
/// circuit without starting it, and is promoted when the leader is killed — dropped where
/// it stands, never shut down. The circuit under test has a stream join, a table join, a
/// remembered lookup, a linear and a tree aggregate, a distinct and a keyed input, and
/// delivers to keyed upsert sinks; the sinks must end up holding what an uninterrupted run
/// produces. Every wait is fenced with a timeout.
/// </summary>
public class ChangelogFailoverTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(20);
    private const string PipelineId = "enrichment";

    private sealed record Order(int Id, int Customer, int Product, int Qty);
    private sealed record Customer(int Id, string Region);
    private sealed record Product(int Id, string Category, int Price);
    private sealed record OrderRegion(int OrderId, int Product, int Qty, string Region);
    private sealed record Enriched(int OrderId, string Region, string Category, long Value);
    private sealed record Priced(int OrderId, int Price);

    private sealed record Definition(
        CircuitBuilder Builder,
        KeyedInputStream<int, Order> Orders,
        InputStream<Customer> Customers,
        AcknowledgedOutput<KeyedValue<string, long>> Totals,
        AcknowledgedOutput<KeyedValue<string, long>> Largest,
        AcknowledgedOutput<Priced> Priced,
        AcknowledgedOutput<int> Buyers);

    private static Definition Define(IStorageProvider storage, SharedTable<int, Product> products, int workers, int interval)
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
        return new Definition(builder, orders, customers, totals, largest, priced, buyers);
    }

    /// <summary>The sinks: outside the process, so they outlive every leader.</summary>
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

    /// <summary>A sink that can be told to take a write and never come back: the batch is
    /// delivered, the acknowledgement is not, and the process "dies" there.</summary>
    private sealed class DyingSink<TKey, TRow>(IUpsertSink<TKey, TRow> inner, Death death) : IUpsertSink<TKey, TRow>
        where TKey : notnull
    {
        public async ValueTask WriteAsync(UpsertBatch<TKey, TRow> batch, CancellationToken cancellationToken)
        {
            await inner.WriteAsync(batch, cancellationToken);
            if (death.AfterDelivery)
            {
                death.Delivered.TrySetResult();
                await Task.Delay(System.Threading.Timeout.Infinite, cancellationToken);
            }
        }
    }

    private sealed class Death
    {
        public volatile bool AfterDelivery;
        public readonly TaskCompletionSource Delivered = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    /// <summary>One process: its storage, its table, its circuit.</summary>
    private sealed class Node
    {
        public required ChangelogStorage Storage;
        public required SharedTable<int, Product> Products;
        public required InMemoryTableLog<int, Product> TableLog;
        public required Definition Definition;
        public required Circuit Circuit;
        public readonly CancellationTokenSource Kill = new();
        public readonly Death Death = new();
        public Task Deliveries = Task.CompletedTask;

        public KeyedInputHandle<int, Order> Orders => Circuit.Handle(Definition.Orders);
        public InputHandle<Customer> Customers => Circuit.Handle(Definition.Customers);

        public void StartDelivering(Sinks sinks)
        {
            var options = new SinkDeliveryOptions { MaxRetries = 0 };
            Deliveries = Task.WhenAll(
                Circuit.Handle(Definition.Totals).DeliverToAsync(new DyingSink<string, long>(sinks.Totals, Death), options, Kill.Token),
                Circuit.Handle(Definition.Largest).DeliverToAsync(new DyingSink<string, long>(sinks.Largest, Death), options, Kill.Token),
                Circuit.Handle(Definition.Priced).DeliverToAsync(new DyingSink<int, Priced>(sinks.Priced, Death), p => p.OrderId, options, Kill.Token),
                Circuit.Handle(Definition.Buyers).DeliverToAsync(new DyingSink<int, int>(sinks.Buyers, Death), c => c, options, Kill.Token));
            _ = Deliveries.ContinueWith(t => t.Exception, TaskScheduler.Default); // a killed node's deliveries end in whatever way
        }
    }

    private sealed class Deployment(int workers, int interval, int partitions = 3)
    {
        public readonly InMemoryStateChangelog Changelog = new(partitions);
        public readonly InMemoryTableLog<int, Product> TableLog = new();
        public readonly Sinks Sinks = new();

        public async Task<Node> StartLeaderAsync()
        {
            var storage = await ChangelogStorage.OpenAsync(Changelog).WaitAsync(Timeout);
            var products = await SharedTable<int, Product>.OpenAsync("products", p => p.Id, TableLog);
            var definition = Define(storage.Pipeline(PipelineId), products, workers, interval);
            var node = new Node { Storage = storage, Products = products, TableLog = TableLog, Definition = definition, Circuit = definition.Builder.Build() };
            node.Circuit.Start();
            node.StartDelivering(Sinks);
            return node;
        }

        /// <summary>The standby: followers on both logs, and the circuit built on what they
        /// feed — but not started.</summary>
        public async Task<Node> StartStandbyAsync()
        {
            var storage = ChangelogStorage.Follow(Changelog);
            var log = TableLog.Connect();
            var products = await SharedTable<int, Product>.FollowAsync("products", p => p.Id, log);
            var definition = Define(storage.Pipeline(PipelineId), products, workers, interval);
            return new Node { Storage = storage, Products = products, TableLog = log, Definition = definition, Circuit = definition.Builder.Build() };
        }

        /// <summary>The takeover: fence, read to the end, tables before circuits, start.</summary>
        public async Task PromoteAsync(Node standby)
        {
            standby.TableLog.TakeOver();
            await standby.Products.PromoteAsync().WaitAsync(Timeout);
            await standby.Storage.PromoteAsync().WaitAsync(Timeout);
            standby.Circuit.Start();
            standby.StartDelivering(Sinks);
        }
    }

    /// <summary>The uninterrupted run: the same circuit on plain in-memory state and a
    /// plain table, fed everything once.</summary>
    private sealed class Reference : IAsyncDisposable
    {
        public readonly Sinks Sinks = new();
        public readonly SharedTable<int, Product> Products = new("products", p => p.Id);
        private readonly Node _node;

        public Reference(int workers)
        {
            var definition = Define(new InMemoryStorageProvider(), Products, workers, interval: 1);
            _node = new Node { Storage = null!, Products = Products, TableLog = null!, Definition = definition, Circuit = definition.Builder.Build() };
            _node.Circuit.Start();
            _node.StartDelivering(Sinks);
        }

        public Node Node => _node;

        public async ValueTask DisposeAsync()
        {
            await _node.Circuit.DisposeAsync().AsTask().WaitAsync(Timeout);
            await _node.Deliveries.WaitAsync(Timeout);
        }
    }

    // ── the workload ───────────────────────────────────────────────────────────────────

    private abstract record Op;
    private sealed record OrderOp(long Offset, int Id, Order? Row) : Op;         // null row: delete
    private sealed record CustomerOp(long Offset, Customer Row, bool Insert) : Op;
    private sealed record ProductOp(int Id, Product? Row) : Op;                  // null row: delete
    private sealed record StepOp(int Crash) : Op;                                // 0: none

    private static readonly string[] Regions = ["emea", "apac", "amer"];
    private static readonly string[] Categories = ["tools", "toys"];

    /// <summary>Turns generated numbers into a valid workload: customers are a set (a
    /// customer is removed only while present, and re-added under whatever region), so the
    /// outputs stay table-shaped.</summary>
    private static List<Op> Plan(List<(int Kind, int A, int B, int C)> commands)
    {
        var plan = new List<Op>();
        var customers = new Dictionary<int, Customer>();
        long orderOffset = 0, customerOffset = 0;
        foreach (var (kind, a, b, c) in commands)
        {
            switch (kind)
            {
                case 0 or 1 or 2:
                    plan.Add(new OrderOp(orderOffset++, a, new Order(a, b % 4, c % 5, 1 + c % 3)));
                    break;
                case 3:
                    plan.Add(new OrderOp(orderOffset++, a, null));
                    break;
                case 4 or 5:
                    var id = a % 4;
                    if (customers.Remove(id, out var present))
                        plan.Add(new CustomerOp(customerOffset++, present, Insert: false));
                    if (kind == 4)
                    {
                        var customer = new Customer(id, Regions[b % Regions.Length]);
                        customers[id] = customer;
                        plan.Add(new CustomerOp(customerOffset++, customer, Insert: true));
                    }
                    break;
                case 6 or 7:
                    plan.Add(new ProductOp(a % 5, new Product(a % 5, Categories[b % Categories.Length], 1 + c)));
                    break;
                case 8:
                    plan.Add(new ProductOp(a % 5, null));
                    break;
                case 9 or 10 or 11:
                    plan.Add(new StepOp(0));
                    break;
                default:
                    plan.Add(new StepOp(1 + a % 4)); // step, and kill the leader in one of four ways
                    break;
            }
        }
        plan.Add(new StepOp(0));
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

    private static async Task<ChangelogStorage> ColdCopyAsync(IStateChangelog log)
    {
        var copy = ChangelogStorage.Follow(log);
        await copy.WhenCaughtUpAsync().WaitAsync(Timeout);
        return copy;
    }

    /// <summary>How a run went, so the test can tell that the property was exercised.</summary>
    private sealed class Tally
    {
        public int Failovers, HeldDeliveries, FailedCommits, FencedCommits, ReplayedSteps;
    }

    /// <summary>
    /// Runs <paramref name="commands"/> against a leader with a standby attached, killing
    /// the leader and promoting the standby wherever the workload says, and against the
    /// uninterrupted reference; then compares the sinks.
    /// <para>
    /// The harness plays the sources. Input records carry offsets, and after a takeover it
    /// re-sends everything since the last checkpoint, cut into the steps they were first
    /// run as. Product changes are committed to the table only when the circuit has nothing
    /// uncommitted: with the steps of a replay cut the same and reading the same table
    /// version, the replay delivers what was delivered before, which is what makes keyed
    /// sinks converge exactly (durability-and-standby.md, <em>A gap in "keyed upserts
    /// converge"</em>).
    /// </para>
    /// </summary>
    private static async Task RunFailoverAsync(
        List<(int Kind, int A, int B, int C)> commands, int workers, int interval, bool compact, Tally tally)
    {
        var plan = Plan(commands);
        var cluster = new Deployment(workers, interval);
        await using var reference = new Reference(workers);
        var leader = await cluster.StartLeaderAsync();
        var standby = await cluster.StartStandbyAsync();

        var unstepped = new List<Op>();          // sent since the last step
        var uncommitted = new List<List<Op>>();  // the steps since the last checkpoint, by their inputs
        var products = new List<ProductOp>();    // waiting for a commit boundary
        long? committedStep = null;

        async Task StepLeaderAsync(Task? alreadyRunning = null)
        {
            await (alreadyRunning ?? leader.Circuit.StepAsync()).WaitAsync(Timeout);
            if (leader.Circuit.CommittedStep != committedStep)
            {
                committedStep = leader.Circuit.CommittedStep;
                uncommitted.Clear();
            }
        }

        async Task FailoverAsync(Node killed, Func<Task>? afterPromotion = null)
        {
            tally.Failovers++;
            if (compact)
            {
                // Tombstones stay while a follower may be mid-log.
                cluster.Changelog.Compact(dropTombstones: false);
            }

            await cluster.PromoteAsync(standby);
            leader = standby;

            // Start() found the last checkpoint, warm: the step and offsets it resumes from
            // are the ones the old leader last committed, and its state is the log's.
            Assert.Equal(committedStep, leader.Circuit.CommittedStep);
            await using (var copy = await ColdCopyAsync(cluster.Changelog))
                Assert.Equal(copy.Pipeline(PipelineId).DescribeState(), leader.Storage.Pipeline(PipelineId).DescribeState());

            if (afterPromotion is not null)
                await afterPromotion();

            // The old process is gone. (Disposing its circuit here is the test tidying up
            // threads; a commit it attempts on the way out is fenced.)
            killed.Kill.Cancel();
            await killed.Circuit.DisposeAsync().AsTask().WaitAsync(Timeout);
            await killed.Storage.DisposeAsync();

            // Replay: the uncommitted steps, cut as they were, then what was never stepped.
            var replay = uncommitted.ToList();
            uncommitted.Clear();
            foreach (var step in replay)
            {
                tally.ReplayedSteps++;
                foreach (var op in step)
                    await SendAsync(leader, op, stamped: true);
                uncommitted.Add(step);
                await StepLeaderAsync();
            }
            foreach (var op in unstepped)
                await SendAsync(leader, op, stamped: true);

            standby = await cluster.StartStandbyAsync();
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

            var crash = ((StepOp)op).Crash;
            if (uncommitted.Count == 0 && products.Count > 0)
            {
                await WriteProductsAsync(leader.Products, products);
                await WriteProductsAsync(reference.Products, products);
                products.Clear();
            }
            await reference.Node.Circuit.StepAsync().WaitAsync(Timeout);
            uncommitted.Add([.. unstepped]);
            unstepped.Clear();

            var old = leader;
            switch (crash)
            {
                case 0:
                    await StepLeaderAsync();
                    break;

                case 1: // killed between steps
                    await StepLeaderAsync();
                    await FailoverAsync(old);
                    break;

                case 2: // killed with a batch delivered and not acknowledged
                {
                    old.Death.AfterDelivery = true;
                    var step = old.Circuit.StepAsync();
                    if (await Task.WhenAny(step, old.Death.Delivered.Task).WaitAsync(Timeout) == step)
                    {
                        await StepLeaderAsync(step); // the step delivered nothing: it simply ran
                    }
                    else
                    {
                        tally.HeldDeliveries++;
                        _ = step.ContinueWith(t => t.Exception, TaskScheduler.Default);
                    }
                    await FailoverAsync(old);
                    break;
                }

                case 3: // killed with everything acknowledged and the commit not landed
                {
                    cluster.Changelog.OnCommitting = () => throw new IOException("the process died here");
                    try
                    {
                        await StepLeaderAsync();
                    }
                    catch (IOException)
                    {
                        tally.FailedCommits++;
                        Assert.NotNull(old.Circuit.Fault);
                    }
                    cluster.Changelog.OnCommitting = null;
                    await FailoverAsync(old);
                    break;
                }

                default: // a zombie: its commit is in flight when the standby takes over
                {
                    var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    cluster.Changelog.OnCommitting = async () =>
                    {
                        entered.TrySetResult();
                        await release.Task;
                    };
                    var step = old.Circuit.StepAsync();
                    var held = await Task.WhenAny(step, entered.Task).WaitAsync(Timeout) != step;
                    cluster.Changelog.OnCommitting = null;
                    if (!held)
                    {
                        await StepLeaderAsync(step); // not a commit step
                        await FailoverAsync(old);
                        break;
                    }

                    await FailoverAsync(old, afterPromotion: async () =>
                    {
                        // The old leader wakes up after the takeover: its commit must fail,
                        // and the circuit must fault rather than carry on.
                        tally.FencedCommits++;
                        release.SetResult();
                        await Assert.ThrowsAsync<ChangelogFencedException>(() => step).WaitAsync(Timeout);
                        Assert.IsType<ChangelogFencedException>(old.Circuit.Fault);
                        Assert.True(old.Storage.IsFenced);
                        await Assert.ThrowsAsync<CircuitFaultedException>(() => old.Circuit.StepAsync()).WaitAsync(Timeout);
                    });
                    break;
                }
            }
        }

        // A clean shutdown commits the tail and completes the outputs.
        await leader.Circuit.DisposeAsync().AsTask().WaitAsync(Timeout);
        await leader.Deliveries.WaitAsync(Timeout);
        await reference.DisposeAsync();
        cluster.Sinks.AssertEqual(reference.Sinks);
        Assert.Equal(reference.Products.Count, leader.Products.Count);

        // And what the log holds at the end is what the last leader held — compacted to
        // its live records or not.
        if (compact)
        {
            await standby.Storage.WhenCaughtUpAsync().WaitAsync(Timeout);
            cluster.Changelog.Compact();
        }
        await using (var copy = await ColdCopyAsync(cluster.Changelog))
            Assert.Equal(leader.Storage.Pipeline(PipelineId).DescribeState(), copy.Pipeline(PipelineId).DescribeState());

        await standby.Circuit.DisposeAsync().AsTask().WaitAsync(Timeout);
        await standby.Storage.DisposeAsync();
        await standby.Products.DisposeAsync();
        await leader.Storage.DisposeAsync();
    }

    private static readonly Gen<(List<(int Kind, int A, int B, int C)> Commands, int Interval, bool Compact)> GenRun =
        Gen.Select(
            Gen.Select(Gen.Int[0, 12], Gen.Int[0, 7], Gen.Int[0, 7], Gen.Int[0, 9]).List[5, 70],
            Gen.Int[1, 3],
            Gen.Bool);

    [Theory]
    [InlineData(1, 200)]
    [InlineData(4, 80)]
    public void Killing_the_leader_and_promoting_the_follower_ends_where_an_uninterrupted_run_does(int workers, int iterations)
    {
        var tally = new Tally();
        GenRun.Sample(
            run => RunFailoverAsync(run.Commands, workers, run.Interval, run.Compact, tally).GetAwaiter().GetResult(),
            iter: iterations, threads: 1);

        // The property is only worth something if the interesting things happened.
        Assert.True(tally.Failovers > iterations / 2, $"only {tally.Failovers} failovers");
        Assert.True(tally.ReplayedSteps > 0, "no step was ever replayed");
        Assert.True(tally.HeldDeliveries > 0, "no leader died with a batch delivered and unacknowledged");
        Assert.True(tally.FailedCommits > 0, "no leader died with its commit not landed");
        Assert.True(tally.FencedCommits > 0, "no zombie's commit was fenced");
    }

    // ── directed ───────────────────────────────────────────────────────────────────────

    private static List<(int Kind, int A, int B, int C)> Fixed(params (int Kind, int A, int B, int C)[] commands) => [.. commands];

    /// <summary>A small workload that fills every operator's state, with a kill of the given
    /// kind in the middle (kind 12 with A = crash − 1).</summary>
    private static List<(int Kind, int A, int B, int C)> Workload(int crash) => Fixed(
        (4, 0, 0, 0), (4, 1, 1, 0), (6, 0, 0, 4), (6, 1, 1, 2), (0, 1, 0, 0), (0, 2, 1, 1), (9, 0, 0, 0),
        (0, 3, 0, 1), (6, 0, 1, 7), (0, 1, 1, 0), (12, crash - 1, 0, 0),
        (3, 2, 0, 0), (4, 1, 2, 0), (8, 1, 0, 0), (0, 4, 1, 0), (9, 0, 0, 0));

    [Theory]
    [InlineData(1, 1)]
    [InlineData(2, 1)]
    [InlineData(3, 1)]
    [InlineData(4, 1)]
    [InlineData(2, 3)]
    [InlineData(4, 2)]
    public async Task Each_kind_of_death_fails_over_to_the_same_result(int crash, int interval)
    {
        var tally = new Tally();
        await RunFailoverAsync(Workload(crash), workers: 1, interval, compact: true, tally);
        Assert.Equal(1, tally.Failovers);
        if (interval == 1)
        {
            Assert.Equal(crash == 2 ? 1 : 0, tally.HeldDeliveries);
            Assert.Equal(crash == 3 ? 1 : 0, tally.FailedCommits);
            Assert.Equal(crash == 4 ? 1 : 0, tally.FencedCommits);
            Assert.Equal(crash == 1 ? 0 : 1, tally.ReplayedSteps);
        }
    }

    [Fact]
    public async Task Restart_from_the_log_resumes_state_offsets_and_table_version()
    {
        var cluster = new Deployment(workers: 1, interval: 1);
        var sinks = cluster.Sinks;
        var first = await cluster.StartLeaderAsync();
        await WriteProductsAsync(first.Products, [new ProductOp(1, new Product(1, "tools", 10))]);
        await first.Customers.InsertAsync(new Customer(1, "emea"), offset: 0);
        await first.Orders.UpsertAsync(new Order(7, 1, 1, 2), offset: 0);
        await first.Circuit.StepAsync().WaitAsync(Timeout);
        Assert.Equal(20, sinks.Totals.Rows["emea/tools"]);
        Assert.Equal(1, first.Circuit.CommittedStep);

        // The process stops dead; a new one opens the same logs.
        first.Kill.Cancel();
        var second = await cluster.StartLeaderAsync();
        Assert.Equal(1, second.Circuit.CommittedStep);
        Assert.Equal(0, second.Circuit.CommittedOffset("orders"));
        Assert.Equal(0, second.Circuit.CommittedOffset("customers"));
        Assert.Equal(1, second.Circuit.CommittedVersion(second.Products));
        Assert.Equal(2, second.Storage.Term);

        // Replayed records are dropped; new ones meet the restored state on every operator.
        await second.Orders.UpsertAsync(new Order(7, 1, 1, 2), offset: 0);
        await second.Orders.UpsertAsync(new Order(8, 1, 1, 1), offset: 1);
        await second.Circuit.StepAsync().WaitAsync(Timeout);
        Assert.Equal(30, sinks.Totals.Rows["emea/tools"]);
        Assert.Equal(20, sinks.Largest.Rows["emea"]);
        Assert.Equal(2, second.Circuit.CommittedStep);

        // Retracting the order the first process took in needs its join, aggregate, tree,
        // lookup and keyed-input state back.
        await second.Orders.DeleteAsync(7, offset: 2);
        await second.Circuit.StepAsync().WaitAsync(Timeout);
        Assert.Equal(10, sinks.Totals.Rows["emea/tools"]);
        Assert.Equal(10, sinks.Largest.Rows["emea"]);
        Assert.Equal([8], sinks.Priced.Rows.Keys);

        await second.Circuit.DisposeAsync().AsTask().WaitAsync(Timeout);
        await second.Deliveries.WaitAsync(Timeout);
        await second.Storage.DisposeAsync();
    }

    [Fact]
    public async Task Standby_circuit_cannot_start_until_its_storage_is_promoted()
    {
        var cluster = new Deployment(workers: 1, interval: 1);
        var leader = await cluster.StartLeaderAsync();
        await leader.Orders.UpsertAsync(new Order(1, 1, 1, 1), offset: 0);
        await leader.Circuit.StepAsync().WaitAsync(Timeout);

        var standby = await cluster.StartStandbyAsync();
        var refused = Assert.Throws<InvalidOperationException>(() => standby.Circuit.Start());
        Assert.Contains("follower", refused.Message);

        // The refusal wrote nothing and the leader is unaffected.
        await leader.Orders.UpsertAsync(new Order(2, 1, 1, 1), offset: 1);
        await leader.Circuit.StepAsync().WaitAsync(Timeout);
        Assert.Equal(1, leader.Storage.Term);
        Assert.Equal(1, cluster.Changelog.Term);

        await leader.Circuit.DisposeAsync().AsTask().WaitAsync(Timeout);
        await leader.Deliveries.WaitAsync(Timeout);
        await leader.Storage.DisposeAsync();
        await standby.Storage.DisposeAsync();
        await standby.Products.DisposeAsync();
    }

    [Fact]
    public async Task Follower_keeps_the_standby_circuits_stores_at_the_leaders_last_commit()
    {
        var cluster = new Deployment(workers: 4, interval: 2);
        var leader = await cluster.StartLeaderAsync();
        var standby = await cluster.StartStandbyAsync();
        var written = leader.Storage.Pipeline(PipelineId);
        var followed = standby.Storage.Pipeline(PipelineId);

        await WriteProductsAsync(leader.Products, [new ProductOp(1, new Product(1, "tools", 10))]);
        for (var i = 0; i < 6; i++)
        {
            await leader.Customers.InsertAsync(new Customer(i, Regions[i % 3]), offset: i);
            await leader.Orders.UpsertAsync(new Order(i, i, 1, 1 + i), offset: i);
            await leader.Circuit.StepAsync().WaitAsync(Timeout);

            if (leader.Circuit.CommittedStep == i + 1)
            {
                // At a commit the follower — once it has read it — is the leader, entry
                // for entry, in stores the standby's circuit already holds.
                await standby.Storage.WhenCaughtUpAsync().WaitAsync(Timeout);
                Assert.Equal(written.DescribeState(), followed.DescribeState());
                Assert.Equal(written.CommitSequence, followed.CommitSequence);
                Assert.NotEmpty(followed.DescribeState());
            }
            else
            {
                // Between commits the leader is ahead and the follower is not.
                Assert.True(written.UncommittedEntries > 0);
                Assert.NotEqual(written.DescribeState(), followed.DescribeState());
            }
        }

        await leader.Circuit.DisposeAsync().AsTask().WaitAsync(Timeout);
        await leader.Deliveries.WaitAsync(Timeout);
        await leader.Storage.DisposeAsync();
        await standby.Storage.DisposeAsync();
        await standby.Products.DisposeAsync();
    }

    private sealed record Item(int Id, int Val);

    private static Dictionary<int, Item> Rows(SharedTable<int, Item> table)
    {
        using var snapshot = table.Snapshot();
        var rows = new Dictionary<int, Item>();
        for (var id = 0; id < 10; id++)
            if (snapshot.TryGet(id, out var row))
                rows[id] = row;
        return rows;
    }

    [Fact]
    public async Task Circuit_that_writes_a_table_fails_over_with_its_table()
    {
        // The leader dies after a table version is durable and before the checkpoint that
        // covers it: the promoted standby must undo that version and replay.
        var changelog = new InMemoryStateChangelog(partitions: 2);
        var tableLog = new InMemoryTableLog<int, Item>();

        (CircuitBuilder Builder, KeyedInputStream<int, Item> Items) Define(ChangelogStorage storage, SharedTable<int, Item> table)
        {
            var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Storage = storage.Pipeline("writer") });
            var items = builder.KeyedInput<int, Item>("items", i => i.Id);
            items.ToTable(table);
            return (builder, items);
        }

        var leaderStorage = await ChangelogStorage.OpenAsync(changelog);
        var leaderTable = await SharedTable<int, Item>.OpenAsync("items", i => i.Id, tableLog);
        var leaderDefinition = Define(leaderStorage, leaderTable);
        var leader = leaderDefinition.Builder.Build();
        leader.Start();

        var standbyLog = tableLog.Connect();
        await using var standbyStorage = ChangelogStorage.Follow(changelog);
        await using var standbyTable = await SharedTable<int, Item>.FollowAsync("items", i => i.Id, standbyLog);
        var standbyDefinition = Define(standbyStorage, standbyTable);
        await using var standby = standbyDefinition.Builder.Build();

        var input = leader.Handle(leaderDefinition.Items);
        await input.UpsertAsync(new Item(1, 10), offset: 0);
        await input.UpsertAsync(new Item(2, 20), offset: 1);
        await leader.StepAsync().WaitAsync(Timeout);
        var checkpointed = leader.CommittedVersion(leaderTable);
        Assert.Equal(leaderTable.LatestVersion, checkpointed);

        await input.UpsertAsync(new Item(1, 11), offset: 2);
        await input.DeleteAsync(2, offset: 3);
        changelog.OnCommitting = () => throw new IOException("the process died here");
        await Assert.ThrowsAsync<IOException>(() => leader.StepAsync()).WaitAsync(Timeout);
        changelog.OnCommitting = null;
        Assert.True(leaderTable.LatestVersion > checkpointed); // the version outran the checkpoint
        Assert.Equal(new Dictionary<int, Item> { [1] = new(1, 11) }, Rows(leaderTable));

        standbyLog.TakeOver();
        await standbyTable.PromoteAsync().WaitAsync(Timeout);
        await standbyStorage.PromoteAsync().WaitAsync(Timeout);
        Assert.Equal(leaderTable.LatestVersion, standbyTable.LatestVersion);
        standby.Start();
        Assert.Equal(checkpointed, standby.CommittedVersion(standbyTable));
        Assert.Equal(1, standby.CommittedOffset("items"));

        // The replay is cut differently: only the upsert arrives before the next step. The
        // table steps back to what the circuit has computed, then forward.
        input = standby.Handle(standbyDefinition.Items);
        await input.UpsertAsync(new Item(1, 11), offset: 2);
        await standby.StepAsync().WaitAsync(Timeout);
        Assert.Equal(new Dictionary<int, Item> { [1] = new(1, 11), [2] = new(2, 20) }, Rows(standbyTable));
        Assert.Equal(standbyTable.LatestVersion, standby.CommittedVersion(standbyTable));

        await input.DeleteAsync(2, offset: 3);
        await standby.StepAsync().WaitAsync(Timeout);
        Assert.Equal(new Dictionary<int, Item> { [1] = new(1, 11) }, Rows(standbyTable));

        // The table's own log agrees with the view.
        var reopened = await SharedTable<int, Item>.OpenAsync("items", i => i.Id, tableLog.Connect());
        Assert.Equal(Rows(standbyTable), Rows(reopened));
        await leaderStorage.DisposeAsync();
    }

    [Fact]
    public async Task Circuit_that_writes_a_table_cannot_write_it_while_the_table_follows()
    {
        // Tables are promoted before circuits. A circuit started against a table that is
        // still following must not write over the log's history.
        var tableLog = new InMemoryTableLog<int, Item>();
        var active = await SharedTable<int, Item>.OpenAsync("items", i => i.Id, tableLog);
        await active.Writer.UpsertAsync(new Item(1, 10));
        await active.Writer.CommitAsync();
        await using var following = await SharedTable<int, Item>.FollowAsync("items", i => i.Id, tableLog.Connect());

        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        var items = builder.KeyedInput<int, Item>("items", i => i.Id);
        items.ToTable(following);
        await using var circuit = builder.Build();
        circuit.Start();
        await circuit.Handle(items).UpsertAsync(new Item(2, 20));

        var failure = await Assert.ThrowsAsync<InvalidOperationException>(() => circuit.StepAsync()).WaitAsync(Timeout);
        Assert.Contains("following its log", failure.Message);
        Assert.NotNull(circuit.Fault);
        Assert.Equal(1, await tableLog.LastVersionAsync());
        Assert.Equal(new Dictionary<int, Item> { [1] = new(1, 10) }, Rows(following));
    }

    [Fact]
    public void Clustered_circuit_refuses_the_changelog_provider()
    {
        var storage = ChangelogStorage.Follow(new InMemoryStateChangelog(1));
        var builder = new CircuitBuilder(new CircuitOptions
        {
            AutoStep = false,
            Storage = storage.Pipeline("p"),
            Cluster = new ClusterOptions { Transport = InMemoryExchangeTransport.CreateMesh(2)[0] },
        });
        builder.Input<int>("in").ToOutput();
        Assert.Throws<NotSupportedException>(() => builder.Build());
    }
}
