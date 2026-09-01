namespace Incremental.Tests;

/// <summary>
/// Fragment streaming (docs/design/fragment-streaming.md): chunked publication through
/// fused linear chains, arrival-order partial merges, and the fragment sinks (join, linear
/// aggregate). The differential properties already run every operator under a streaming
/// engine at one row per fragment; these tests pin the structural edges — chunk clamping on
/// large batches, a fused Where filtering whole chunks away, the fan-out grouping that must
/// fall back to the partial merge, faults raised inside the produce loop, and the
/// all-exchanges-empty step that must still deliver watermark-driven work — and, for v2,
/// the forwarding paths: an emitting join feeding the next exchange per fragment (with and
/// without a non-streamed side), an exchange forwarding through a linear chain into another,
/// a fault raised mid-forward, and a loop-headed exchange producing at its own position.
/// </summary>
public class FragmentStreamingTests
{
    private static readonly TimeSpan StepTimeout = TimeSpan.FromSeconds(10);

    private static CircuitOptions Options(int chunkRows = 4, bool autoStep = false, bool streaming = true) => new()
    {
        AutoStep = autoStep, Workers = 4, PointToPointExchange = true,
        FragmentStreaming = streaming, StreamChunkRows = chunkRows,
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
        // Both exchanges are headed by an input and produce right after it runs; only the
        // left one feeds the join as a sink.
        var plans = circuit.DescribeStreamPlans();
        Assert.Equal(2, plans.Count(p => p.Contains("AfterHead")));
        Assert.Single(plans, p => p.Contains("sink"));
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

    // ------------------------------------------------------------------ v2: forwarding

    [Theory, InlineData(true), InlineData(false)]
    public async Task Join_emits_per_fragment_into_the_downstream_exchange(bool streaming)
    {
        // join -> Select -> GroupBy: the join's only reader is the linear chain into the
        // second exchange, so it is an emitting sink — each fragment's probe results are
        // forwarded straight into the downstream ring, and the downstream exchange completes
        // after the join's own Step. One row per fragment forces many small forwarded
        // fragments to coalesce and, on the large batches, to hit the ring-depth cap. Both
        // sides retract, so weights pass through zero on both stores.
        var builder = new CircuitBuilder(Options(chunkRows: 1, streaming: streaming));
        var left = builder.Input<(int Key, int Tag)>("left");
        var right = builder.Input<(int Key, int Value)>("right");
        var sumsDef = left.GroupBy(l => l.Key)
            .Join(right.GroupBy(r => r.Key), (key, l, r) => (Bucket: (l.Tag + r.Value) % 5, Amount: r.Value))
            .Select(x => (x.Bucket, Amount: (long)x.Amount))
            .GroupBy(x => x.Bucket)
            .Sum(x => x.Amount)
            .ToOutput(bootstrap: false);

        await using var circuit = builder.Build();
        circuit.Start();
        var plans = circuit.DescribeStreamPlans();
        Assert.Equal(streaming ? 3 : 0, plans.Count(p => p.Contains("sink"))); // join x2, then the Sum
        Assert.Equal(streaming ? 1 : 0, plans.Count(p => p.Contains("Forwarded")));

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
            Assert.True(expected.Count == integrated.DistinctCount,
                $"expected {string.Join(",", expected.OrderBy(kv => kv.Key))} got {string.Join(",", integrated.OrderBy(e => e.Value.Key).Select(e => $"{e.Value.Key}:{e.Value.Value}x{e.Weight}"))}");
            foreach (var (bucket, sum) in expected)
                Assert.Equal(1, integrated.WeightOf(new KeyedValue<int, long>(bucket, sum)));
        }

        for (var i = 0; i < 600; i++)
        {
            await leftIn.InsertAsync((i % 50, i));
            leftRows.Add((i % 50, i), 1);
            await rightIn.InsertAsync((i % 50, i * 3));
            rightRows.Add((i % 50, i * 3), 1);
        }
        await StepAndCheckAsync();

        // Right side only: every fragment probes the left store built in step one.
        for (var i = 600; i < 900; i++)
        {
            await rightIn.InsertAsync((i % 50, i));
            rightRows.Add((i % 50, i), 1);
        }
        await StepAndCheckAsync();

        // Retractions on both sides in one step, plus fresh inserts.
        for (var i = 0; i < 600; i += 3)
        {
            await leftIn.SendAsync((i % 50, i), -1);
            leftRows.Add((i % 50, i), -1);
        }
        for (var i = 0; i < 600; i += 4)
        {
            await rightIn.SendAsync((i % 50, i * 3), -1);
            rightRows.Add((i % 50, i * 3), -1);
        }
        for (var i = 900; i < 950; i++)
        {
            await leftIn.InsertAsync((i % 50, i));
            leftRows.Add((i % 50, i), 1);
        }
        await StepAndCheckAsync();

        // A step where nothing reaches the join at all: the forwarded exchange must still
        // complete with zero fragments and the sink skip must propagate.
        await StepAndCheckAsync();
    }

    [Theory, InlineData(true), InlineData(false)]
    public async Task Join_with_a_grouped_side_forwards_its_step_output_too(bool streaming)
    {
        // The right side arrives without an exchange (a keyed aggregate re-grouped in place),
        // so only the left is fed per fragment; the right delta reaches the join in Step,
        // whose output must flow into the same downstream exchange before it completes.
        var builder = new CircuitBuilder(Options(chunkRows: 1, streaming: streaming));
        var left = builder.Input<(int Key, int Tag)>("left");
        var right = builder.Input<(int Key, int Value)>("right");
        var rightCounts = right.GroupBy(r => r.Key).Count().Grouped();
        var sumsDef = left.GroupBy(l => l.Key)
            .Join(rightCounts, (key, l, count) => (Bucket: l.Tag % 3, Weight: count))
            .Select(x => (x.Bucket, x.Weight))
            .GroupBy(x => x.Bucket)
            .Sum(x => x.Weight)
            .ToOutput(bootstrap: false);

        await using var circuit = builder.Build();
        circuit.Start();
        var plans = circuit.DescribeStreamPlans();
        Assert.Equal(streaming ? 1 : 0, plans.Count(p => p.Contains("Forwarded")));

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
            var counts = new Dictionary<int, long>();
            foreach (var (r, rw) in rightRows)
                counts[r.Key] = counts.GetValueOrDefault(r.Key) + rw;
            var expected = new Dictionary<int, long>();
            foreach (var (l, lw) in leftRows)
                if (counts.TryGetValue(l.Key, out var count) && count > 0)
                    expected[l.Tag % 3] = expected.GetValueOrDefault(l.Tag % 3) + count * lw;
            foreach (var (bucket, sum) in expected.Where(kv => kv.Value != 0))
                Assert.Equal(1, integrated.WeightOf(new KeyedValue<int, long>(bucket, sum)));
            Assert.Equal(expected.Count(kv => kv.Value != 0), integrated.DistinctCount);
        }

        // Left only: fragments probe an empty right store; nothing emitted.
        for (var i = 0; i < 300; i++)
        {
            await leftIn.InsertAsync((i % 40, i));
            leftRows.Add((i % 40, i), 1);
        }
        await StepAndCheckAsync();
        Assert.True(integrated.IsEmpty);

        // Right only: the join's Step produces everything, forwarded after the join ran.
        for (var i = 0; i < 200; i++)
        {
            await rightIn.InsertAsync((i % 40, i));
            rightRows.Add((i % 40, i), 1);
        }
        await StepAndCheckAsync();
        Assert.False(integrated.IsEmpty);

        // Both sides at once, with retractions: per-fragment output plus Step output land
        // in the same downstream execution.
        for (var i = 0; i < 300; i += 2)
        {
            await leftIn.SendAsync((i % 40, i), -1);
            leftRows.Add((i % 40, i), -1);
        }
        for (var i = 0; i < 200; i += 5)
        {
            await rightIn.SendAsync((i % 40, i), -1);
            rightRows.Add((i % 40, i), -1);
        }
        await StepAndCheckAsync();
    }

    [Theory, InlineData(true), InlineData(false)]
    public async Task Exchange_forwards_fragments_through_a_linear_chain_into_the_next_exchange(bool streaming)
    {
        // An exchange whose only reader is a linear chain into another exchange forwards each
        // arriving fragment through the chain and on, completing the downstream exchange when
        // it completes itself. The public API never places a linear operator directly on an
        // exchange today, so the shape is built from the internal node API.
        var builder = new CircuitBuilder(Options(chunkRows: 1, streaming: streaming));
        var numbers = builder.Input<int>("numbers");
        var shuffled = builder.AddNode(
            "shuffle", [numbers.NodeId], _ => new Operators.ExchangeOperator<int>(n => (uint)(n * 2654435761u)), isExchange: true);
        var sumsDef = new ZStream<int>(builder, shuffled)
            .Select(n => (Key: n % 7, Val: n))
            .Where(x => x.Val % 5 != 0)
            .GroupBy(x => x.Key)
            .Sum(x => (long)x.Val)
            .ToOutput(bootstrap: false);

        await using var circuit = builder.Build();
        circuit.Start();
        var plans = circuit.DescribeStreamPlans();
        Assert.Equal(streaming ? 1 : 0, plans.Count(p => p.Contains("forward #")));
        Assert.Equal(streaming ? 1 : 0, plans.Count(p => p.Contains("Forwarded")));

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

        for (var n = 1; n <= 1000; n++)
        {
            await input.InsertAsync(n);
            Track(n, +1);
        }
        await StepAndIntegrateAsync();
        for (var n = 1; n <= 1000; n += 2)
        {
            await input.SendAsync(n, -1);
            Track(n, -1);
        }
        await StepAndIntegrateAsync();
        await StepAndIntegrateAsync(); // nothing moves: zero-fragment completions all the way down

        foreach (var (key, sum) in expected)
            Assert.Equal(1, integrated.WeightOf(new KeyedValue<int, long>(key, sum)));
        Assert.Equal(expected.Count, integrated.DistinctCount);
    }

    [Fact]
    public async Task Fault_while_forwarding_an_emitting_joins_output_releases_peers()
    {
        // The throwing Select sits between the join and the second exchange, so it runs from
        // the first exchange's consume loop as fragments are forwarded — on exactly one worker.
        // Its peers wait for that worker's fragments and completions; the poison flag must
        // release them within the step.
        var builder = new CircuitBuilder(Options(chunkRows: 1));
        var left = builder.Input<(int Key, int Tag)>("left");
        var right = builder.Input<(int Key, int Value)>("right");
        var countsDef = left.GroupBy(l => l.Key)
            .Join(right.GroupBy(r => r.Key), (key, l, r) => (key, r.Value))
            .Select(x => x.Value == 13 ? throw new InvalidOperationException("boom 13") : x.Value % 3)
            .GroupBy(v => v)
            .Count()
            .ToOutput(bootstrap: false);

        await using var circuit = builder.Build();
        circuit.Start();
        var leftIn = circuit.Handle(left);
        var rightIn = circuit.Handle(right);

        for (var i = 0; i < 40; i++)
        {
            await leftIn.InsertAsync((i, i));
            await rightIn.InsertAsync((i, i));
        }
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => circuit.StepAsync()).WaitAsync(StepTimeout);
        Assert.Contains("boom 13", ex.Message);
        await Assert.ThrowsAsync<CircuitFaultedException>(() => circuit.StepAsync()).WaitAsync(StepTimeout);
    }

    [Fact]
    public async Task Loop_result_heading_an_exchange_is_produced_at_the_exchange()
    {
        // The GroupBy's chain head is the Recurse region's exit node, which runs once per
        // iteration — so the exchange produces from the (complete) slot at its own position
        // instead of after the head, and the closure still matches a recompute.
        var builder = new CircuitBuilder(Options(chunkRows: 1));
        var edges = builder.Input<(int From, int To)>("edges");
        var outDegreeDef = edges
            .Recurse(paths => paths
                .GroupBy(p => p.To)
                .Join(edges.GroupBy(e => e.From), (_, p, e) => (p.From, e.To)))
            .GroupBy(p => p.From)
            .Count()
            .ToOutput(bootstrap: false);

        await using var circuit = builder.Build();
        circuit.Start();
        Assert.Single(circuit.DescribeStreamPlans(), p => p.Contains("AtSelf"));

        var input = circuit.Handle(edges);
        var output = circuit.Handle(outDegreeDef);

        // A chain 0 -> 1 -> ... -> 30 plus a back edge: reach counts per source.
        const int n = 30;
        for (var i = 0; i < n; i++)
            await input.InsertAsync((i, i + 1));
        await input.InsertAsync((n, 0));
        await circuit.StepAsync().WaitAsync(StepTimeout);

        var integrated = new ZSet<KeyedValue<int, long>>();
        while (output.Reader.TryRead(out var delta))
            integrated.AddAll(delta);
        // Every node reaches every node (including itself) around the cycle.
        Assert.Equal(n + 1, integrated.DistinctCount);
        for (var i = 0; i <= n; i++)
            Assert.Equal(1, integrated.WeightOf(new KeyedValue<int, long>(i, n + 1)));

        // Breaking the cycle retracts most of the closure — through the same path.
        await input.SendAsync((n, 0), -1);
        await circuit.StepAsync().WaitAsync(StepTimeout);
        while (output.Reader.TryRead(out var delta))
            integrated.AddAll(delta);
        Assert.Equal(n, integrated.DistinctCount);
        for (var i = 0; i < n; i++)
            Assert.Equal(1, integrated.WeightOf(new KeyedValue<int, long>(i, n - i)));
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
