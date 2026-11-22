using System.Net;
using System.Net.Sockets;
using Dbsp.Cluster;

namespace Dbsp.Tests;

/// <summary>
/// Cross-process exchange: the same circuit definition running on several "processes" (here:
/// several Circuit instances in one test process), each hosting a contiguous slice of the
/// global worker set, with exchange operators shuffling across all of them over a transport.
/// Steps are explicit and lockstep — every process steps every step, concurrently — and
/// every wait is timeout-fenced so a regression shows up as a failed test, not a hang.
/// </summary>
public class ClusterTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(20);

    private static Task StepAll(params Circuit[] processes) =>
        Task.WhenAll(processes.Select(p => p.StepAsync())).WaitAsync(Timeout);

    private static CircuitOptions Clustered(IExchangeTransport transport, int workers) => new()
    {
        Workers = workers,
        AutoStep = false,
        Cluster = new ClusterOptions { Transport = transport },
    };

    /// <summary>Each process's output handle carries the deltas its own workers produced;
    /// the full stream is the union across processes.</summary>
    private static void Drain<T>(ZSet<T> into, params OutputHandle<T>[] outputs) where T : notnull
    {
        foreach (var output in outputs)
            while (output.Reader.TryRead(out var delta))
                into.AddAll(delta);
    }

    private static void AssertSameZSet<T>(ZSet<T> expected, ZSet<T> actual) where T : notnull
    {
        Assert.Equal(expected.DistinctCount, actual.DistinctCount);
        foreach (var (value, weight) in expected)
            Assert.Equal(weight, actual.WeightOf(value));
    }

    private sealed record RevenueCircuit(
        CircuitBuilder Builder,
        InputStream<(int Id, int CustomerId, decimal Amount)> Orders,
        InputStream<(int Id, string Region)> Customers,
        Output<KeyedValue<string, decimal>> Revenue);

    private static RevenueCircuit RevenuePipeline(CircuitOptions options)
    {
        var builder = new CircuitBuilder(options);
        var orders = builder.Input<(int Id, int CustomerId, decimal Amount)>("orders");
        var customers = builder.Input<(int Id, string Region)>("customers");
        var revenue = orders.GroupBy(o => o.CustomerId)
            .Join(customers.GroupBy(c => c.Id), (_, o, c) => (c.Region, o.Amount))
            .GroupBy(x => x.Region)
            .Sum(x => x.Amount)
            .ToOutput(bootstrap: false);
        return new RevenueCircuit(builder, orders, customers, revenue);
    }

    private static readonly ((int, int, decimal) Order, bool OnProcess0, long Weight)[] OrderChurn =
    [
        ((101, 1, 60m), true, 1), ((102, 2, 40m), true, 1), ((103, 3, 10m), true, 1),
        ((104, 1, 25m), true, 1), ((101, 1, 60m), true, -1), // cancelled — retracts through join and sum
        ((105, 2, 5m), true, 1), ((106, 3, 7m), false, 1),   // inputs can be fed on any process
    ];

    private static async Task RunRevenueChurn(
        (Circuit Circuit, RevenueCircuit Defs) process0,
        (Circuit Circuit, RevenueCircuit Defs) process1)
    {
        process0.Circuit.Start();
        process1.Circuit.Start();
        var customersIn = process1.Circuit.Handle(process1.Defs.Customers); // reference data on process 1
        foreach (var customer in new[] { (1, "emea"), (2, "apac"), (3, "amer") })
            await customersIn.InsertAsync(customer);

        // Multi-step incremental churn, orders fed on either process, one step at a time.
        foreach (var (order, onProcess0, weight) in OrderChurn)
        {
            var feeder = onProcess0 ? process0 : process1;
            var ordersIn = feeder.Circuit.Handle(feeder.Defs.Orders);
            await ordersIn.SendAsync(order, weight);
            await StepAll(process0.Circuit, process1.Circuit);
        }

        // Mid-stream reference-data amendment: apac's customer moves to emea.
        await customersIn.RemoveAsync((2, "apac"));
        await customersIn.InsertAsync((2, "emea"));
        await StepAll(process0.Circuit, process1.Circuit);
    }

    private static async Task<ZSet<KeyedValue<string, decimal>>> RevenueReference()
    {
        var defs = RevenuePipeline(new CircuitOptions { AutoStep = false });
        await using var single = defs.Builder.Build();
        single.Start();
        var orders = single.Handle(defs.Orders);
        var customers = single.Handle(defs.Customers);
        foreach (var customer in new[] { (1, "emea"), (2, "emea"), (3, "amer") }) // post-amendment state
            await customers.InsertAsync(customer);
        foreach (var (order, _, weight) in OrderChurn)
            await orders.SendAsync(order, weight);
        await single.StepAsync().WaitAsync(Timeout);
        var integrated = new ZSet<KeyedValue<string, decimal>>();
        Drain(integrated, single.Handle(defs.Revenue));
        return integrated;
    }

    [Fact]
    public async Task Join_and_aggregate_across_two_processes_match_single_process()
    {
        var mesh = InMemoryExchangeTransport.CreateMesh(2);
        var defs0 = RevenuePipeline(Clustered(mesh[0], workers: 2));
        var defs1 = RevenuePipeline(Clustered(mesh[1], workers: 2));
        await using var process0 = defs0.Builder.Build();
        await using var process1 = defs1.Builder.Build();
        await RunRevenueChurn((process0, defs0), (process1, defs1));

        var integrated = new ZSet<KeyedValue<string, decimal>>();
        Drain(integrated,
            process0.Handle(defs0.Revenue),
            process1.Handle(defs1.Revenue));

        AssertSameZSet(await RevenueReference(), integrated);
    }

    [Fact]
    public async Task Same_pipeline_over_tcp_loopback_matches_single_process()
    {
        var ports = FreePorts(2);
        string[] endpoints = [$"127.0.0.1:{ports[0]}", $"127.0.0.1:{ports[1]}"];
        var defs0 = RevenuePipeline(Clustered(new TcpExchangeTransport(0, endpoints), workers: 2));
        var defs1 = RevenuePipeline(Clustered(new TcpExchangeTransport(1, endpoints), workers: 2));
        await using var process0 = defs0.Builder.Build();
        await using var process1 = defs1.Builder.Build();
        await RunRevenueChurn((process0, defs0), (process1, defs1));

        var integrated = new ZSet<KeyedValue<string, decimal>>();
        Drain(integrated,
            process0.Handle(defs0.Revenue),
            process1.Handle(defs1.Revenue));

        AssertSameZSet(await RevenueReference(), integrated);
    }

    [Fact]
    public async Task Distinct_retractions_cross_the_wire()
    {
        static (CircuitBuilder Builder, InputStream<int> Values, Output<int> Distinct) Pipeline(CircuitOptions options)
        {
            var builder = new CircuitBuilder(options);
            var values = builder.Input<int>("values");
            var distinct = values.Distinct().ToOutput();
            return (builder, values, distinct);
        }

        var mesh = InMemoryExchangeTransport.CreateMesh(2);
        var defs0 = Pipeline(Clustered(mesh[0], workers: 2));
        var defs1 = Pipeline(Clustered(mesh[1], workers: 2));
        await using var process0 = defs0.Builder.Build();
        await using var process1 = defs1.Builder.Build();
        process0.Start();
        process1.Start();
        var in0 = process0.Handle(defs0.Values);
        var in1 = process1.Handle(defs1.Values);
        var outputs = new[] { process0.Handle(defs0.Distinct), process1.Handle(defs1.Distinct) };
        var integrated = new ZSet<int>();

        // Three copies of every value, split across processes: Distinct's shuffle-by-value
        // must bring them to one owner regardless of which process fed them.
        for (var v = 0; v < 40; v++)
        {
            await in0.InsertAsync(v);
            await in0.InsertAsync(v);
            await in1.InsertAsync(v);
        }
        await StepAll(process0, process1);
        Drain(integrated, outputs);
        Assert.Equal(40, integrated.DistinctCount);
        Assert.Equal(1, integrated.WeightOf(7));

        // Dropping one copy changes nothing; dropping the last retracts, across the wire.
        await in0.RemoveAsync(7);
        await StepAll(process0, process1);
        Drain(integrated, outputs);
        Assert.Equal(1, integrated.WeightOf(7));

        await in0.RemoveAsync(7);
        await in1.RemoveAsync(7);
        await StepAll(process0, process1);
        Drain(integrated, outputs);
        Assert.Equal(0, integrated.WeightOf(7));
        Assert.Equal(39, integrated.DistinctCount);
    }

    [Fact]
    public async Task Watermark_is_globally_min_reduced_and_eviction_stays_consistent()
    {
        var start = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        static (CircuitBuilder Builder,
            InputStream<(DateTimeOffset At, string Page)> Views,
            Output<KeyedValue<DateTimeOffset, long>> Counts) Pipeline(CircuitOptions options)
        {
            var builder = new CircuitBuilder(options);
            var views = builder.Input<(DateTimeOffset At, string Page)>("views");
            var counts = views
                .WithEventTime(v => v.At, lateness: TimeSpan.Zero)
                .GroupBy(v => v.At)
                .EvictWhen(key => key) // a bucket closes the moment the watermark passes it
                .Count()
                .ToOutput(bootstrap: false);
            return (builder, views, counts);
        }

        // Workers = 1 per process also covers the inline (no worker thread) engine.
        var mesh = InMemoryExchangeTransport.CreateMesh(2);
        var defs0 = Pipeline(Clustered(mesh[0], workers: 1));
        var defs1 = Pipeline(Clustered(mesh[1], workers: 1));
        await using var process0 = defs0.Builder.Build();
        await using var process1 = defs1.Builder.Build();
        process0.Start();
        process1.Start();
        var in0 = process0.Handle(defs0.Views);
        var in1 = process1.Handle(defs1.Views);
        var outputs = new[]
        {
            process0.Handle(defs0.Counts),
            process1.Handle(defs1.Counts),
        };
        var integrated = new ZSet<KeyedValue<DateTimeOffset, long>>();

        // Only process 0 sees events, but the frozen watermark must be identical on both:
        // the observations travel in the step-alignment frames.
        await in0.InsertAsync((start, "home"));
        await StepAll(process0, process1);
        Assert.Equal(start, process0.CurrentWatermark);
        Assert.Equal(process0.CurrentWatermark, process1.CurrentWatermark);

        var later = start.AddSeconds(10);
        await in0.InsertAsync((later, "home"));
        await StepAll(process0, process1);
        Assert.Equal(later, process0.CurrentWatermark);
        Assert.Equal(process0.CurrentWatermark, process1.CurrentWatermark);
        Drain(integrated, outputs);
        Assert.Equal(1, integrated.WeightOf(new KeyedValue<DateTimeOffset, long>(start, 1)));

        // The first bucket is closed and evicted cluster-wide: a late event for it — fed on
        // EITHER process — is dropped by that process's watermark filter, so the already
        // emitted result stays final and no delta appears anywhere.
        await in0.InsertAsync((start, "late"));
        await in1.InsertAsync((start, "late"));
        await StepAll(process0, process1);
        Drain(integrated, outputs);
        Assert.Equal(1, integrated.WeightOf(new KeyedValue<DateTimeOffset, long>(start, 1)));
        Assert.Equal(1, integrated.WeightOf(new KeyedValue<DateTimeOffset, long>(later, 1)));
    }

    [Fact]
    public async Task Empty_steps_align_without_hanging()
    {
        static (CircuitBuilder Builder, InputStream<string> Words, Output<KeyedValue<string, long>> Counts)
            Pipeline(CircuitOptions options)
        {
            var builder = new CircuitBuilder(options);
            var words = builder.Input<string>("words");
            var counts = words.GroupBy(w => w).Count().ToOutput(bootstrap: false);
            return (builder, words, counts);
        }

        var mesh = InMemoryExchangeTransport.CreateMesh(2);
        var defsA = Pipeline(Clustered(mesh[0], workers: 2));
        var defsB = Pipeline(Clustered(mesh[1], workers: 2));
        await using var a = defsA.Builder.Build();
        await using var b = defsB.Builder.Build();
        a.Start();
        b.Start();

        // No input anywhere: five steps must still exchange empty frames and complete.
        for (var i = 0; i < 5; i++)
            await StepAll(a, b);

        await a.Handle(defsA.Words).InsertAsync("hello");
        await StepAll(a, b);
        var integrated = new ZSet<KeyedValue<string, long>>();
        Drain(integrated, a.Handle(defsA.Counts), b.Handle(defsB.Counts));
        Assert.Equal(1, integrated.WeightOf(new KeyedValue<string, long>("hello", 1)));
    }

    [Fact]
    public async Task Fault_on_one_process_faults_the_peer_instead_of_hanging_it()
    {
        static (CircuitBuilder Builder, InputStream<int> Numbers) Pipeline(CircuitOptions options)
        {
            var builder = new CircuitBuilder(options);
            var numbers = builder.Input<int>("numbers");
            numbers
                .Select(n => n == 13 ? throw new InvalidOperationException("boom 13") : n)
                .GroupBy(n => n % 4)
                .Count()
                .ToOutput(bootstrap: false);
            return (builder, numbers);
        }

        var mesh = InMemoryExchangeTransport.CreateMesh(2);
        var defs0 = Pipeline(Clustered(mesh[0], workers: 2));
        var defs1 = Pipeline(Clustered(mesh[1], workers: 2));
        await using var process0 = defs0.Builder.Build();
        await using var process1 = defs1.Builder.Build();
        process0.Start();
        process1.Start();

        for (var n = 0; n < 16; n++)
            await process0.Handle(defs0.Numbers).InsertAsync(n);

        // Process 0 throws in user code before its exchange; process 1 sits waiting for
        // process 0's shards and must be faulted by the poison, not left blocked.
        var step0 = process0.StepAsync();
        var step1 = process1.StepAsync();

        var ex0 = await Assert.ThrowsAnyAsync<Exception>(() => step0).WaitAsync(Timeout);
        Assert.Contains("boom 13", ex0.Message);

        var ex1 = await Assert.ThrowsAnyAsync<Exception>(() => step1).WaitAsync(Timeout);
        Assert.Contains("process 0", ex1.Message);

        Assert.NotNull(process0.Fault);
        Assert.NotNull(process1.Fault);
        Assert.Contains("process 0", process1.Fault!.Message);

        // Both circuits are terminally faulted: further steps refuse with the fault.
        await Assert.ThrowsAsync<CircuitFaultedException>(() => process1.StepAsync()).WaitAsync(Timeout);
    }

    [Fact]
    public void Recurse_is_rejected_in_clustered_circuits_at_build_time()
    {
        var mesh = InMemoryExchangeTransport.CreateMesh(2);
        var builder = new CircuitBuilder(Clustered(mesh[0], workers: 1));
        var edges = builder.Input<(string From, string To)>("edges");
        edges.Recurse(paths => paths
                .GroupBy(p => p.To)
                .Join(edges.GroupBy(e => e.From), (_, p, e) => (p.From, e.To)))
            .ToOutput();

        var ex = Assert.Throws<NotSupportedException>(() => builder.Build());
        Assert.Contains("Recurse", ex.Message);
    }

    [Fact]
    public void AutoStep_is_rejected_in_clustered_circuits()
    {
        var mesh = InMemoryExchangeTransport.CreateMesh(2);
        var ex = Assert.Throws<NotSupportedException>(() => new CircuitBuilder(new CircuitOptions
        {
            Workers = 2,
            Cluster = new ClusterOptions { Transport = mesh[0] },
        }));
        Assert.Contains("AutoStep", ex.Message);
    }

    [Fact]
    public async Task Tcp_transport_delivers_frames_in_send_order()
    {
        var ports = FreePorts(2);
        string[] endpoints = [$"127.0.0.1:{ports[0]}", $"127.0.0.1:{ports[1]}"];
        await using var t0 = new TcpExchangeTransport(0, endpoints);
        await using var t1 = new TcpExchangeTransport(1, endpoints);

        await Task.WhenAll(t0.ConnectAsync(default), t1.ConnectAsync(default)).WaitAsync(Timeout);

        await t0.SendAsync(1, new ExchangeFrame(7, 3, [1, 2, 3]), default).AsTask().WaitAsync(Timeout);
        await t0.SendAsync(1, new ExchangeFrame(7, 5, []), default).AsTask().WaitAsync(Timeout); // empty frames still ship
        await t1.SendAsync(0, new ExchangeFrame(7, 3, [9]), default).AsTask().WaitAsync(Timeout);

        var first = await t1.ReceiveAsync(0, default).AsTask().WaitAsync(Timeout);
        var second = await t1.ReceiveAsync(0, default).AsTask().WaitAsync(Timeout);
        var back = await t0.ReceiveAsync(1, default).AsTask().WaitAsync(Timeout);

        Assert.Equal(7L, first.Step);
        Assert.Equal(3, first.NodeId);
        Assert.Equal(new byte[] { 1, 2, 3 }, first.Payload);
        Assert.Equal(5, second.NodeId);
        Assert.Empty(second.Payload);
        Assert.Equal(new byte[] { 9 }, back.Payload);
    }

    private static int[] FreePorts(int count)
    {
        var listeners = Enumerable.Range(0, count)
            .Select(_ =>
            {
                var listener = new TcpListener(IPAddress.Loopback, 0);
                listener.Start();
                return listener;
            })
            .ToArray();
        var ports = listeners.Select(l => ((IPEndPoint)l.LocalEndpoint).Port).ToArray();
        foreach (var listener in listeners)
            listener.Stop();
        return ports;
    }
}
