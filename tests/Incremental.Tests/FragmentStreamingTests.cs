namespace Incremental.Tests;

/// <summary>
/// Fragment streaming (docs/design/fragment-streaming.md): chunked publication through
/// fused linear chains, arrival-order partial merges, and the fragment sinks (join, linear
/// aggregate). The differential properties already run every operator under a streaming
/// engine at one row per fragment; these tests pin the structural edges — chunk clamping on
/// large batches, a fused Where filtering whole chunks away, the fan-out grouping that must
/// fall back to the partial merge, faults raised inside the produce loop, and the
/// all-exchanges-empty step that must still deliver watermark-driven work.
/// </summary>
public class FragmentStreamingTests
{
    private static readonly TimeSpan StepTimeout = TimeSpan.FromSeconds(10);

    private static CircuitOptions Options(int chunkRows = 4, bool autoStep = false) => new()
    {
        AutoStep = autoStep, Workers = 4, PointToPointExchange = true,
        FragmentStreaming = true, StreamChunkRows = chunkRows,
    };

    [Fact]
    public void FragmentStreaming_requires_point_to_point()
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            new CircuitBuilder(new CircuitOptions { Workers = 4, FragmentStreaming = true }));
        Assert.Contains("PointToPointExchange", ex.Message);
    }

    [Fact]
    public async Task Chunked_fused_chain_sums_match_over_large_batches_and_retractions()
    {
        // Select and Where fuse into the GroupBy exchange (linear, single-consumer chain);
        // batches far above StreamChunkRows force multi-fragment publication clamped by the
        // ring, and the Where drops a residue class so some chunks publish partially or not
        // at all. The Sum is a linear-aggregate sink fed per fragment.
        var builder = new CircuitBuilder(Options());
        var numbers = builder.Input<int>("numbers");
        var sumsDef = numbers
            .Select(n => (Key: n % 7, Val: n))
            .Where(x => x.Val % 5 != 0)
            .GroupBy(x => x.Key)
            .Sum(x => (long)x.Val)
            .ToOutput(bootstrap: false);

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(numbers);
        var output = circuit.Handle(sumsDef);

        var expected = new Dictionary<int, long>();
        void Track(int n, long weight)
        {
            if (n % 5 != 0)
                expected[n % 7] = expected.GetValueOrDefault(n % 7) + n * weight;
        }

        var integrated = new ZSet<KeyedValue<int, long>>();
        async Task StepAndIntegrateAsync()
        {
            await circuit.StepAsync().WaitAsync(StepTimeout);
            while (output.Reader.TryRead(out var delta))
                integrated.AddAll(delta);
        }

        // One large insert step, then a step retracting half of it, then a mixed step.
        for (var n = 0; n < 1000; n++)
        {
            await input.InsertAsync(n);
            Track(n, +1);
        }
        await StepAndIntegrateAsync();

        for (var n = 0; n < 1000; n += 2)
        {
            await input.SendAsync(n, -1);
            Track(n, -1);
        }
        await StepAndIntegrateAsync();

        for (var n = 500; n < 700; n++)
        {
            await input.InsertAsync(n);
            Track(n, +1);
        }
        await StepAndIntegrateAsync();

        foreach (var (key, sum) in expected)
            Assert.Equal(1, integrated.WeightOf(new KeyedValue<int, long>(key, sum)));
        Assert.Equal(expected.Count, integrated.DistinctCount);
    }

    [Fact]
    public async Task Shared_grouping_fans_out_and_still_matches()
    {
        // The right grouping feeds both the join and an aggregate, so its exchange has two
        // consumers and must take the partial-merge path (no sink); the left grouping's
        // exchange keeps the sink path into the join. Both must agree with a recompute.
        var builder = new CircuitBuilder(Options());
        var left = builder.Input<(int Key, string Tag)>("left");
        var right = builder.Input<(int Key, int Value)>("right");

        var rightGrouped = right.GroupBy(r => r.Key);
        var joinedDef = left.GroupBy(l => l.Key)
            .Join(rightGrouped, (key, l, r) => (key, l.Tag, r.Value))
            .ToOutput();
        var countsDef = rightGrouped.Count().ToOutput(bootstrap: false);

        await using var circuit = builder.Build();
        circuit.Start();
        var leftIn = circuit.Handle(left);
        var rightIn = circuit.Handle(right);
        var joined = circuit.Handle(joinedDef);
        var counts = circuit.Handle(countsDef);

        const int keys = 200;
        for (var k = 0; k < keys; k++)
        {
            await leftIn.InsertAsync((k, $"tag-{k}"));
            await rightIn.InsertAsync((k, k * 2));
            await rightIn.InsertAsync((k, k * 2 + 1));
        }
        await circuit.StepAsync().WaitAsync(StepTimeout);

        var joinedRows = new ZSet<(int, string, int)>();
        while (joined.Reader.TryRead(out var delta))
            joinedRows.AddAll(delta);
        var countRows = new ZSet<KeyedValue<int, long>>();
        while (counts.Reader.TryRead(out var delta))
            countRows.AddAll(delta);

        Assert.Equal(keys * 2, joinedRows.DistinctCount);
        for (var k = 0; k < keys; k++)
        {
            Assert.Equal(1, joinedRows.WeightOf((k, $"tag-{k}", k * 2)));
            Assert.Equal(1, joinedRows.WeightOf((k, $"tag-{k}", k * 2 + 1)));
            Assert.Equal(1, countRows.WeightOf(new KeyedValue<int, long>(k, 2)));
        }
    }

    [Fact]
    public async Task Fault_inside_the_fused_chain_releases_peers_within_the_step()
    {
        // The throwing Select fuses into the GroupBy exchange, so the exception fires from
        // the produce loop of a streamed exchange — after Distinct has routed the poison
        // value to exactly one worker. Its peers sit in consume loops waiting for fragments
        // and a completion that will never come; the poison flag must release them. The
        // timeout is the deadlock detector.
        var builder = new CircuitBuilder(Options());
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
        Assert.NotNull(circuit.Fault);
        await Assert.ThrowsAsync<CircuitFaultedException>(() => circuit.StepAsync()).WaitAsync(StepTimeout);
    }

    [Fact]
    public async Task Window_closes_on_a_step_where_every_streamed_exchange_is_empty()
    {
        // The zero-fragment execution must still publish its completion seq and yield the
        // canonical empty — and the watermark-driven close downstream (EmptyStepResult ==
        // null) must still run on a step where no exchange moves any data.
        var t0 = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var builder = new CircuitBuilder(Options());
        var views = builder.Input<(string Page, DateTimeOffset At)>("views");
        var closesDef = views
            .WithEventTime(v => v.At, lateness: TimeSpan.Zero)
            .Tumble(TimeSpan.FromSeconds(5), v => v.At)
            .GroupBy(w => w.WindowStart)
            .WindowCloses(windowStart => windowStart.AddSeconds(5))
            .ToOutput(bootstrap: false);

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(views);
        var output = circuit.Handle(closesDef);

        await input.InsertAsync(("home", t0.AddSeconds(1)));
        await circuit.StepAsync().WaitAsync(StepTimeout);
        await input.InsertAsync(("home", t0.AddSeconds(7)));
        await circuit.StepAsync().WaitAsync(StepTimeout);
        Assert.False(output.Reader.TryRead(out _));

        await circuit.StepAsync().WaitAsync(StepTimeout);
        var delta = await output.Reader.ReadAsync().AsTask().WaitAsync(StepTimeout);
        Assert.Equal(1, delta.WeightOf(new KeyedValue<DateTimeOffset, DateTimeOffset>(t0, t0.AddSeconds(5))));
        Assert.Equal(1, delta.DistinctCount);
    }

    [Fact]
    public async Task Auto_stepping_pump_streams_correctly()
    {
        // The pump path: staged publishes overlap the next step while exchanges stream.
        var builder = new CircuitBuilder(Options(chunkRows: 16, autoStep: true));
        var words = builder.Input<string>("words");
        var countsDef = words.GroupBy(w => w).Count().ToOutput(bootstrap: false);

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(words);
        var output = circuit.Handle(countsDef);

        var expected = new Dictionary<string, long>();
        for (var i = 0; i < 5000; i++)
        {
            var word = $"word-{i % 37}";
            expected[word] = expected.GetValueOrDefault(word) + 1;
            await input.InsertAsync(word);
        }
        input.Complete();

        var integrated = new ZSet<KeyedValue<string, long>>();
        await foreach (var delta in output.Changes())
            integrated.AddAll(delta);

        Assert.Equal(expected.Count, integrated.DistinctCount);
        foreach (var (word, count) in expected)
            Assert.Equal(1, integrated.WeightOf(new KeyedValue<string, long>(word, count)));
    }
}
