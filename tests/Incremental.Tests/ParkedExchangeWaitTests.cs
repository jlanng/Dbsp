namespace Incremental.Tests;

/// <summary>
/// Point-to-point exchange waits spin briefly and then park on the exchange gate until a
/// publication (or the poison) wakes them. A lost wake-up is a deadlock, so these tests
/// route every slow wait through the park path (spin budget zero) under both the plain
/// point-to-point path and fragment streaming, check results against a recompute, and use
/// the step timeout as the deadlock detector. One test keeps the default budget and stalls
/// a single worker long enough that its peers must park for real.
/// </summary>
public class ParkedExchangeWaitTests
{
    private static readonly TimeSpan StepTimeout = TimeSpan.FromSeconds(20);

    private static CircuitOptions Options(bool streaming, int? spinBudget) => new()
    {
        AutoStep = false, Workers = 4, PointToPointExchange = true,
        FragmentStreaming = streaming, StreamChunkRows = streaming ? 2 : 256,
        ExchangeSpinBudget = spinBudget ?? new CircuitOptions().ExchangeSpinBudget,
    };

    [Theory, InlineData(false), InlineData(true)]
    public async Task Every_slow_wait_parks_and_the_join_pipeline_still_matches(bool streaming)
    {
        var builder = new CircuitBuilder(Options(streaming, spinBudget: 0));
        var left = builder.Input<(int Key, int Tag)>("left");
        var right = builder.Input<(int Key, int Value)>("right");
        var sumsDef = left.GroupBy(l => l.Key)
            .Join(right.GroupBy(r => r.Key), (key, l, r) => (Bucket: (l.Tag + r.Value) % 5, Amount: (long)r.Value))
            .GroupBy(x => x.Bucket)
            .Sum(x => x.Amount)
            .ToOutput(bootstrap: false);

        await using var circuit = builder.Build();
        circuit.Start();
        var leftIn = circuit.Handle(left);
        var rightIn = circuit.Handle(right);
        var output = circuit.Handle(sumsDef);

        var leftRows = new ZSet<(int Key, int Tag)>();
        var rightRows = new ZSet<(int Key, int Value)>();
        var integrated = new ZSet<KeyedValue<int, long>>();

        async Task StepAndCheckAsync()
        {
            await circuit.StepAsync().WaitAsync(StepTimeout);
            while (output.Reader.TryRead(out var delta))
                integrated.AddAll(delta);
            var expected = new Dictionary<int, long>();
            foreach (var (l, lw) in leftRows)
                foreach (var (r, rw) in rightRows)
                    if (l.Key == r.Key)
                        expected[(l.Tag + r.Value) % 5] = expected.GetValueOrDefault((l.Tag + r.Value) % 5) + r.Value * lw * rw;
            Assert.Equal(expected.Count, integrated.DistinctCount);
            foreach (var (bucket, sum) in expected)
                Assert.Equal(1, integrated.WeightOf(new KeyedValue<int, long>(bucket, sum)));
        }

        for (var step = 0; step < 6; step++)
        {
            for (var i = 0; i < 300; i++)
            {
                var l = ((i * 7 + step) % 50, i + step * 1000);
                var r = ((i * 3 + step) % 50, i * 3 + step);
                var retractLeft = step % 2 == 1 && i % 3 == 0 && leftRows.WeightOf((l.Item1, l.Item2 - 1000)) > 0;
                if (retractLeft)
                {
                    await leftIn.SendAsync((l.Item1, l.Item2 - 1000), -1);
                    leftRows.Add((l.Item1, l.Item2 - 1000), -1);
                }
                else
                {
                    await leftIn.InsertAsync(l);
                    leftRows.Add(l, 1);
                }
                await rightIn.InsertAsync(r);
                rightRows.Add(r, 1);
            }
            await StepAndCheckAsync();
        }
        await StepAndCheckAsync(); // an empty step: zero-fragment completions wake parked peers too

        Assert.True(circuit.ExchangeParks > 0, "with a zero spin budget every slow wait must have parked");
    }

    [Theory, InlineData(false), InlineData(true)]
    public async Task A_stalled_worker_parks_its_peers_under_the_default_budget(bool streaming)
    {
        // Distinct routes each value to one worker; the Select downstream stalls on one value,
        // so exactly one worker is late at the next exchange while three peers wait for it —
        // far past the spin budget. They must park (not burn) and wake on its publication.
        var builder = new CircuitBuilder(Options(streaming, spinBudget: null));
        var numbers = builder.Input<int>("numbers");
        var countsDef = numbers
            .Distinct()
            .Select(n =>
            {
                if (n == 17)
                    Thread.Sleep(50);
                return n % 3;
            })
            .GroupBy(v => v)
            .Count()
            .ToOutput(bootstrap: false);

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(numbers);
        var output = circuit.Handle(countsDef);

        for (var n = 0; n < 60; n++)
            await input.InsertAsync(n);
        await circuit.StepAsync().WaitAsync(StepTimeout);

        var integrated = new ZSet<KeyedValue<int, long>>();
        while (output.Reader.TryRead(out var delta))
            integrated.AddAll(delta);
        Assert.Equal(1, integrated.WeightOf(new KeyedValue<int, long>(0, 20)));
        Assert.Equal(1, integrated.WeightOf(new KeyedValue<int, long>(1, 20)));
        Assert.Equal(1, integrated.WeightOf(new KeyedValue<int, long>(2, 20)));
        Assert.True(circuit.ExchangeParks > 0, "a 50 ms stall must outlive the spin budget on at least one peer");
    }

    [Theory, InlineData(false), InlineData(true)]
    public async Task A_fault_wakes_parked_peers(bool streaming)
    {
        // Every wait parks immediately; the throwing worker's peers are asleep on the gate
        // when it faults, and the poison must pulse them awake to unwind within the step.
        var builder = new CircuitBuilder(Options(streaming, spinBudget: 0));
        var numbers = builder.Input<int>("numbers");
        var countsDef = numbers
            .Distinct()
            .Select(n => n == 13 ? throw new InvalidOperationException("boom 13") : n)
            .GroupBy(n => n % 3)
            .Count()
            .ToOutput(bootstrap: false);

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(numbers);
        for (var n = 0; n < 32; n++)
            await input.InsertAsync(n);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => circuit.StepAsync()).WaitAsync(StepTimeout);
        Assert.Contains("boom 13", ex.Message);
        await Assert.ThrowsAsync<CircuitFaultedException>(() => circuit.StepAsync()).WaitAsync(StepTimeout);
    }
}
