namespace Incremental.Tests;

/// <summary>
/// Auto-stepping circuits overlap each step's output publish with the next step (staged
/// flush, depth one). These tests pin the semantics that overlap must not change: every
/// step's delta arrives, exactly once, in step order, and a slow consumer on a bounded
/// channel still backpressures the circuit instead of deltas piling up unboundedly.
/// </summary>
public class StagedPublishTests
{
    [Theory]
    [InlineData(1, false)]
    [InlineData(4, false)]
    [InlineData(4, true)]
    public async Task Slow_consumer_receives_every_delta_exactly_once_in_step_order(
        int workers, bool pointToPoint)
    {
        const int values = 200;
        var builder = new CircuitBuilder(new CircuitOptions
        {
            Workers = workers,
            PointToPointExchange = pointToPoint,
            // Small enough that the staged flush must actually block on the consumer.
            OutputChannelCapacity = 2,
        });
        var numbers = builder.Input<int>("numbers");
        // An exchange in the graph so the multi-worker variants shuffle before publishing.
        var result = numbers.GroupBy(n => n % 7).Count().ToOutput(bootstrap: false);

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(numbers);
        var output = circuit.Handle(result);

        var seen = new List<ZSet<KeyedValue<int, long>>>();
        var consumer = Task.Run(async () =>
        {
            await foreach (var delta in output.Reader.ReadAllAsync())
            {
                seen.Add(delta);
                await Task.Delay(1); // slow consumer: force the bounded channel to fill
            }
        });

        for (var i = 0; i < values; i++)
            await input.InsertAsync(i);
        await circuit.DisposeAsync(); // drains, flushes the last staged step, completes outputs
        await consumer;

        // Exactly once: integrating every delta yields each group's final count, nothing
        // lost to an unflushed stage and nothing duplicated by a double publish.
        var integrated = new ZSet<KeyedValue<int, long>>();
        foreach (var delta in seen)
            integrated.AddAll(delta);
        Assert.Equal(7, integrated.DistinctCount);
        for (var group = 0; group < 7; group++)
        {
            var count = values / 7 + (group < values % 7 ? 1 : 0);
            Assert.Equal(1, integrated.WeightOf(new KeyedValue<int, long>(group, count)));
        }

        // Step order: counts only grow, so each delta's retractions must be values some
        // earlier delta asserted — an out-of-order flush would retract a count that was
        // never published or leave a stale one asserted.
        var live = new ZSet<KeyedValue<int, long>>();
        foreach (var delta in seen)
        {
            foreach (var (entry, weight) in delta)
                if (weight < 0)
                    Assert.Equal(1, live.WeightOf(entry));
            live.AddAll(delta);
            foreach (var (entry, weight) in live)
                Assert.Equal(1, weight);
        }
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task Fault_while_auto_stepping_surfaces_on_outputs_and_dispose_completes(int workers)
    {
        var builder = new CircuitBuilder(new CircuitOptions { Workers = workers });
        var numbers = builder.Input<int>("numbers");
        var result = numbers
            .Select(n => n == 13 ? throw new InvalidOperationException("poison record") : n)
            .ToOutput();

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(numbers);
        var output = circuit.Handle(result);

        var consumer = Task.Run(async () =>
        {
            await foreach (var _ in output.Reader.ReadAllAsync()) { }
        });

        // The faulting record may land while the previous step's staged flush is still in
        // flight; the fault must still surface on the consumer and none of the teardown
        // paths may hang on the never-flushed stage.
        var feeding = await Record.ExceptionAsync(async () =>
        {
            for (var i = 0; i < 50; i++)
                await input.InsertAsync(i);
        });

        var consumed = await Record.ExceptionAsync(() => consumer);
        Assert.Contains("poison record", consumed!.ToString());
        // Feeding may or may not have outrun the fault; if it did fail, it failed loudly.
        if (feeding is not null)
            Assert.IsType<CircuitFaultedException>(feeding);

        await circuit.DisposeAsync(); // must not hang; fault already surfaced
        Assert.NotNull(circuit.Fault);
    }
}
