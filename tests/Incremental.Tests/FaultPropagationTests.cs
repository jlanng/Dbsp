using Incremental.Storage;

namespace Incremental.Tests;

/// <summary>
/// Cooperative fault propagation: when a worker throws mid-step (user delegate, storage
/// error), its peers bail out of the step via the poisoned exchange barrier instead of
/// deadlocking, and the circuit moves to a terminal faulted state within that step — the
/// faulting <see cref="Circuit.StepAsync"/> throws the original exception, consumers'
/// reads observe it after the pre-fault deltas, producers are released with a
/// <see cref="CircuitFaultedException"/>, <see cref="Circuit.Fault"/> exposes it, and
/// disposal completes quietly. Every wait is fenced with a timeout: a regression here
/// shows up as a hang, not a wrong value.
/// </summary>
public class FaultPropagationTests
{
    private static readonly TimeSpan StepTimeout = TimeSpan.FromSeconds(10);

    /// <summary>The original fault must be <paramref name="ex"/> or inside it (inner chain,
    /// aggregate branches) — never swallowed, never replaced by a timeout or a bare
    /// closed-channel error.</summary>
    private static void AssertContainsOriginal(Exception ex, string marker)
    {
        static IEnumerable<Exception> Flatten(Exception e)
        {
            yield return e;
            IEnumerable<Exception> children = e switch
            {
                AggregateException aggregate => aggregate.InnerExceptions,
                { InnerException: { } inner } => [inner],
                _ => [],
            };
            foreach (var child in children)
                foreach (var nested in Flatten(child))
                    yield return nested;
        }

        Assert.True(
            Flatten(ex).Any(e => e is InvalidOperationException && e.Message.Contains(marker)),
            $"Expected the original fault '{marker}' in: {ex}");
    }

    [Fact]
    public async Task Fault_between_exchanges_releases_peers_within_the_step()
    {
        // Distinct shuffles by value, so exactly one of the four workers owns the poison
        // value and throws in the Map — while its peers sit on the next exchange barrier
        // (IndexBy), waiting for a participant that will never arrive. The poisoned barrier
        // must release them; the timeout is the deadlock detector.
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Workers = 4 });
        builder.Input<int>("numbers")
            .Distinct()
            .Map(n => n == 13 ? throw new InvalidOperationException("boom 13") : n)
            .IndexBy(n => n % 3)
            .Count()
            .Output("counts");

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Input<int>("numbers");
        var output = circuit.Output<KeyedValue<int, long>>("counts");

        for (var n = 0; n < 32; n++)
            await input.InsertAsync(n);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => circuit.StepAsync()).WaitAsync(StepTimeout);
        Assert.Contains("boom 13", ex.Message);

        // The circuit is terminally faulted: the fault is exposed, further steps refuse,
        // consumers observe the original fault instead of hanging, and producers fail fast
        // with the fault as inner instead of filling a channel nothing will drain.
        AssertContainsOriginal(circuit.Fault!, "boom 13");
        var refused = await Assert.ThrowsAsync<CircuitFaultedException>(
            () => circuit.StepAsync()).WaitAsync(StepTimeout);
        AssertContainsOriginal(refused, "boom 13");

        var consumerEx = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await foreach (var _ in output.ReadAllAsync()) { }
        }).WaitAsync(StepTimeout);
        AssertContainsOriginal(consumerEx, "boom 13");

        var producerEx = await Assert.ThrowsAsync<CircuitFaultedException>(async () =>
        {
            for (var n = 100; ; n++)
                await input.InsertAsync(n);
        }).WaitAsync(StepTimeout);
        AssertContainsOriginal(producerEx, "boom 13");
    }

    [Fact]
    public async Task Fault_in_a_keyed_operator_downstream_of_IndexBy_faults_the_step()
    {
        // The throw happens after the exchange, in the aggregate of the worker that owns
        // the poisoned key, while its peers run their own shards of the same step.
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Workers = 4 });
        builder.Input<(int Key, int Amount)>("orders")
            .IndexBy(o => o.Key)
            .Sum(o => o.Amount < 0 ? throw new InvalidOperationException("negative amount") : o.Amount)
            .Output("sums");

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Input<(int Key, int Amount)>("orders");

        for (var k = 0; k < 16; k++)
            await input.InsertAsync((k, k == 7 ? -1 : k));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => circuit.StepAsync()).WaitAsync(StepTimeout);
        Assert.Contains("negative amount", ex.Message);
        AssertContainsOriginal(circuit.Fault!, "negative amount");
    }

    [Fact]
    public async Task Fault_under_the_pump_releases_a_blocked_producer()
    {
        // AutoStep with a tiny input channel: after the pump faults on the poison record,
        // a producer still inserting must be released with the fault — not left suspended
        // forever on a channel no step will ever drain again.
        var builder = new CircuitBuilder(new CircuitOptions { Workers = 4, InputChannelCapacity = 4 });
        builder.Input<int>("numbers")
            .Distinct()
            .Map(n => n < 0 ? throw new InvalidOperationException("boom pump") : n)
            .Output("distinct");

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Input<int>("numbers");
        var output = circuit.Output<int>("distinct");

        await input.InsertAsync(-1);
        var producerEx = await Assert.ThrowsAsync<CircuitFaultedException>(async () =>
        {
            for (var n = 0; ; n++)
                await input.InsertAsync(n);
        }).WaitAsync(StepTimeout);
        AssertContainsOriginal(producerEx, "boom pump");
        Assert.IsType<InvalidOperationException>(producerEx.InnerException);

        var consumerEx = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await foreach (var _ in output.ReadAllAsync()) { }
        }).WaitAsync(StepTimeout);
        AssertContainsOriginal(consumerEx, "boom pump");
        AssertContainsOriginal(circuit.Fault!, "boom pump");
    }

    [Fact]
    public async Task Fault_inside_a_recursion_loop_iteration_does_not_deadlock()
    {
        // The poison path (a, c) is derived at iteration 1 on the single worker that owns
        // join key "b"; its peers are mid-loop — convergence flags and the loop's exchanges
        // synchronize every iteration — so the poison must unwind them out of the loop.
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Workers = 4 });
        var edges = builder.Input<(string From, string To)>("edges");
        edges.Recurse(paths => paths
                .IndexBy(p => p.To)
                .Join(edges.IndexBy(e => e.From), (_, p, e) => (p.From, e.To))
                .Map(p => p == ("a", "c") ? throw new InvalidOperationException("boom loop") : p))
            .Output("closure");

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Input<(string From, string To)>("edges");

        await input.InsertAsync(("a", "b"));
        await input.InsertAsync(("b", "c"));
        for (var i = 0; i < 20; i++) // filler chains so every worker has loop work
            await input.InsertAsync(($"x{i}", $"x{i + 1}"));

        var ex = await Assert.ThrowsAnyAsync<Exception>(
            () => circuit.StepAsync()).WaitAsync(StepTimeout);
        AssertContainsOriginal(ex, "boom loop");
        AssertContainsOriginal(circuit.Fault!, "boom loop");
    }

    [Fact]
    public async Task Simultaneous_worker_faults_aggregate_and_do_not_hang()
    {
        // The maxIterations guard fires on every worker in the same iteration (the
        // convergence decision is global), so several workers fault genuinely at once:
        // whoever faults before observing a peer's poison is recorded, and the step
        // surfaces one of them or their aggregate — never a hang, never a lost fault.
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Workers = 4 });
        var edges = builder.Input<(string From, string To)>("edges");
        edges.Recurse(paths => paths
                .IndexBy(p => p.To)
                .Join(edges.IndexBy(e => e.From), (_, p, e) => (p.From, e.To)),
                maxIterations: 3)
            .Output("closure");

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Input<(string From, string To)>("edges");
        for (var i = 0; i < 7; i++)
            await input.InsertAsync(($"n{i}", $"n{i + 1}"));

        var ex = await Assert.ThrowsAnyAsync<Exception>(
            () => circuit.StepAsync()).WaitAsync(StepTimeout);
        Assert.True(ex is InvalidOperationException or AggregateException, ex.ToString());
        AssertContainsOriginal(ex, "fixed point");
        if (ex is AggregateException aggregate)
            Assert.All(aggregate.InnerExceptions, e => Assert.IsType<InvalidOperationException>(e));
    }

    [Fact]
    public async Task Committed_checkpoint_reflects_only_pre_fault_steps()
    {
        var storage = new InMemoryCheckpointStorageProvider();
        static CircuitBuilder Pipeline(IStorageProvider storage)
        {
            var builder = new CircuitBuilder(new CircuitOptions
            {
                AutoStep = false,
                Workers = 4,
                Storage = storage,
            });
            builder.Input<(int Id, string Region)>("orders")
                .Map(o => o.Id < 0 ? throw new InvalidOperationException("poison record") : o)
                .IndexBy(o => o.Region)
                .Count()
                .Output("counts");
            return builder;
        }
        static (int Id, string Region) Record(int i) => (i, $"r{i % 3}");

        var integrated = new ZSet<KeyedValue<string, long>>();
        {
            await using var crashed = Pipeline(storage).Build();
            crashed.Start();
            var input = crashed.Input<(int Id, string Region)>("orders");
            var output = crashed.Output<KeyedValue<string, long>>("counts");

            for (var i = 0; i < 20; i++)
                await input.SendAsync(Record(i), 1, offset: i);
            await crashed.StepAsync().WaitAsync(StepTimeout); // commits offsets 0..19

            for (var i = 20; i < 30; i++)
                await input.SendAsync(i == 25 ? (-1, "r1") : Record(i), 1, offset: i);
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(
                () => crashed.StepAsync()).WaitAsync(StepTimeout);
            Assert.Contains("poison record", ex.Message);

            // The faulting step never commits: the checkpoint still says 19, and the only
            // published deltas are the pre-fault step's.
            Assert.Equal(19L, crashed.CommittedOffset("orders"));
            while (output.Reader.TryRead(out var delta))
                integrated.AddAll(delta);
        } // disposal after a fault is quiet — and must not commit the half-staged step

        // Rebuild against the same provider: recovery rolls the staged step back, the
        // producer resumes past the checkpoint (with the poison fixed), and the deltas of
        // both runs integrate to exactly the records committed once each.
        await using var resumed = Pipeline(storage).Build();
        resumed.Start();
        Assert.Equal(19L, resumed.CommittedOffset("orders"));

        var replay = resumed.Input<(int Id, string Region)>("orders");
        var counts = resumed.Output<KeyedValue<string, long>>("counts");
        for (var i = 20; i < 30; i++)
            await replay.SendAsync(Record(i), 1, offset: i);
        await resumed.StepAsync().WaitAsync(StepTimeout);
        while (counts.Reader.TryRead(out var delta))
            integrated.AddAll(delta);

        var expected = new Dictionary<string, long>();
        for (var i = 0; i < 30; i++)
            expected[Record(i).Region] = expected.GetValueOrDefault(Record(i).Region) + 1;
        Assert.Equal(expected.Count, integrated.DistinctCount);
        foreach (var (region, count) in expected)
            Assert.Equal(1, integrated.WeightOf(new KeyedValue<string, long>(region, count)));
    }

    [Fact]
    public async Task Clean_dispose_while_idle_is_not_a_fault()
    {
        var builder = new CircuitBuilder(new CircuitOptions { Workers = 4 });
        builder.Input<int>("numbers").Map(n => n * 2).Output("doubled");

        var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Input<int>("numbers");
        var output = circuit.Output<int>("doubled");

        await input.InsertAsync(21);
        var delta = await output.Reader.ReadAsync().AsTask().WaitAsync(StepTimeout);
        Assert.Equal(1, delta.WeightOf(42));

        await circuit.DisposeAsync().AsTask().WaitAsync(StepTimeout); // idle: workers parked between steps
        Assert.Null(circuit.Fault);
        await foreach (var _ in output.ReadAllAsync()) { } // completes cleanly, no error
    }

    [Fact]
    public async Task Clean_dispose_under_load_is_not_a_fault()
    {
        var builder = new CircuitBuilder(new CircuitOptions { Workers = 4 });
        builder.Input<int>("numbers")
            .IndexBy(n => n % 97)
            .Count()
            .Output("counts");

        var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Input<int>("numbers");
        var output = circuit.Output<KeyedValue<int, long>>("counts");

        var integrated = new ZSet<KeyedValue<int, long>>();
        var consumer = Task.Run(async () =>
        {
            await foreach (var delta in output.ReadAllAsync())
                integrated.AddAll(delta);
        });

        for (var n = 0; n < 10_000; n++)
            await input.InsertAsync(n);

        // Dispose mid-flight, without completing the input: everything buffered must still
        // be drained, the outputs completed without error, and no fault raced in.
        await circuit.DisposeAsync().AsTask().WaitAsync(StepTimeout);
        await consumer.WaitAsync(StepTimeout);

        Assert.Null(circuit.Fault);
        Assert.Equal(97, integrated.DistinctCount);
    }
}
