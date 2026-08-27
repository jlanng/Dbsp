namespace Incremental.Tests;

/// <summary>
/// Recurse: fixed-point subcircuits (DBSP recursion). Most tests drive a transitive-closure
/// query with AutoStep = false and assert the exact per-step delta the loop emits — the net
/// change of the new fixed point versus the previous step's. The churn tests integrate the
/// output and compare it against a from-scratch reachability computation after every step,
/// which pins the semantics down for arbitrary insert/delete interleavings, cycles and
/// self-loops included.
/// </summary>
public class RecursionTests
{
    private static CircuitBuilder ManualBuilder(int workers = 1) =>
        new(new CircuitOptions { AutoStep = false, Workers = workers });

    /// <summary>closure = fix X. Distinct(edges ∪ {(p.From, e.To) | p ∈ X, e ∈ edges, p.To = e.From})</summary>
    private static void DefineClosure(CircuitBuilder builder, int maxIterations = 10_000)
    {
        var edges = builder.Input<(string From, string To)>("edges");
        edges.Recurse(paths => paths
                .IndexBy(p => p.To)
                .Join(edges.IndexBy(e => e.From), (_, p, e) => (p.From, e.To)),
                maxIterations)
            .Output("closure");
    }

    [Fact]
    public async Task Transitive_closure_grows_incrementally()
    {
        var builder = ManualBuilder();
        DefineClosure(builder);

        await using var circuit = builder.Build();
        circuit.Start();
        var edges = circuit.Input<(string From, string To)>("edges");
        var closure = circuit.Output<(string From, string To)>("closure");

        await edges.InsertAsync(("a", "b"));
        await circuit.StepAsync();
        var delta = await closure.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(("a", "b")));
        Assert.Equal(1, delta.DistinctCount);

        // A new edge extends existing paths: the loop derives (a, c) from integrated state.
        await edges.InsertAsync(("b", "c"));
        await circuit.StepAsync();
        delta = await closure.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(("b", "c")));
        Assert.Equal(1, delta.WeightOf(("a", "c")));
        Assert.Equal(2, delta.DistinctCount);

        await edges.InsertAsync(("c", "d"));
        await circuit.StepAsync();
        delta = await closure.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(("c", "d")));
        Assert.Equal(1, delta.WeightOf(("b", "d")));
        Assert.Equal(1, delta.WeightOf(("a", "d")));
        Assert.Equal(3, delta.DistinctCount);
    }

    [Fact]
    public async Task Edge_deletion_retracts_exactly_the_lost_paths()
    {
        var builder = ManualBuilder();
        DefineClosure(builder);

        await using var circuit = builder.Build();
        circuit.Start();
        var edges = circuit.Input<(string From, string To)>("edges");
        var closure = circuit.Output<(string From, string To)>("closure");

        // Diamond: a→b→d and a→c→d. The path (a, d) has two derivations.
        await edges.InsertAsync(("a", "b"));
        await edges.InsertAsync(("a", "c"));
        await edges.InsertAsync(("b", "d"));
        await edges.InsertAsync(("c", "d"));
        await circuit.StepAsync();
        var delta = await closure.Reader.ReadAsync();
        Assert.Equal(5, delta.DistinctCount); // (a,b) (a,c) (b,d) (c,d) (a,d) — (a,d) once, despite two routes
        Assert.Equal(1, delta.WeightOf(("a", "d")));

        // Deleting b→d loses (b, d) but not (a, d): the route via c still exists.
        await edges.RemoveAsync(("b", "d"));
        await circuit.StepAsync();
        delta = await closure.Reader.ReadAsync();
        Assert.Equal(-1, delta.WeightOf(("b", "d")));
        Assert.Equal(1, delta.DistinctCount);

        // Deleting c→d loses the last route: (c, d) and (a, d) both retract.
        await edges.RemoveAsync(("c", "d"));
        await circuit.StepAsync();
        delta = await closure.Reader.ReadAsync();
        Assert.Equal(-1, delta.WeightOf(("c", "d")));
        Assert.Equal(-1, delta.WeightOf(("a", "d")));
        Assert.Equal(2, delta.DistinctCount);
    }

    [Fact]
    public async Task Cyclic_graph_converges_and_unwinds()
    {
        var builder = ManualBuilder();
        DefineClosure(builder);

        await using var circuit = builder.Build();
        circuit.Start();
        var edges = circuit.Input<(string From, string To)>("edges");
        var closure = circuit.Output<(string From, string To)>("closure");

        // A 3-cycle: every node reaches every node, including itself.
        await edges.InsertAsync(("a", "b"));
        await edges.InsertAsync(("b", "c"));
        await edges.InsertAsync(("c", "a"));
        await circuit.StepAsync();
        var delta = await closure.Reader.ReadAsync();
        Assert.Equal(9, delta.DistinctCount);
        foreach (var from in new[] { "a", "b", "c" })
            foreach (var to in new[] { "a", "b", "c" })
                Assert.Equal(1, delta.WeightOf((from, to)));

        // Breaking the cycle: only the chain a→b→c remains reachable.
        await edges.RemoveAsync(("c", "a"));
        await circuit.StepAsync();
        delta = await closure.Reader.ReadAsync();
        Assert.Equal(6, delta.DistinctCount);
        Assert.Equal(-1, delta.WeightOf(("c", "a")));
        Assert.Equal(-1, delta.WeightOf(("c", "b")));
        Assert.Equal(-1, delta.WeightOf(("b", "a")));
        Assert.Equal(-1, delta.WeightOf(("a", "a")));
        Assert.Equal(-1, delta.WeightOf(("b", "b")));
        Assert.Equal(-1, delta.WeightOf(("c", "c")));
    }

    [Fact]
    public async Task Self_supporting_derivation_unwinds_on_deletion()
    {
        // The well-foundedness case: with a self-loop a→a, the path (b, a) derives itself
        // ((b, a) ⋈ a→a → (b, a)). Deleting the edge b→a must retract the path even though
        // the cyclic derivation still "supports" it — support that is not well-founded.
        var builder = ManualBuilder();
        DefineClosure(builder);

        await using var circuit = builder.Build();
        circuit.Start();
        var edges = circuit.Input<(string From, string To)>("edges");
        var closure = circuit.Output<(string From, string To)>("closure");

        await edges.InsertAsync(("a", "a"));
        await edges.InsertAsync(("b", "a"));
        await circuit.StepAsync();
        var delta = await closure.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(("a", "a")));
        Assert.Equal(1, delta.WeightOf(("b", "a")));
        Assert.Equal(2, delta.DistinctCount);

        await edges.RemoveAsync(("b", "a"));
        await circuit.StepAsync();
        delta = await closure.Reader.ReadAsync();
        Assert.Equal(-1, delta.WeightOf(("b", "a")));
        Assert.Equal(1, delta.DistinctCount);
    }

    [Fact]
    public async Task Duplicate_edges_keep_paths_until_the_last_copy_is_removed()
    {
        var builder = ManualBuilder();
        DefineClosure(builder);

        await using var circuit = builder.Build();
        circuit.Start();
        var edges = circuit.Input<(string From, string To)>("edges");
        var closure = circuit.Output<(string From, string To)>("closure");

        await edges.InsertAsync(("a", "b"));
        await edges.InsertAsync(("a", "b"));
        await edges.InsertAsync(("b", "c"));
        await circuit.StepAsync();
        var delta = await closure.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(("a", "b")));
        Assert.Equal(1, delta.WeightOf(("a", "c")));
        Assert.Equal(3, delta.DistinctCount);

        // One copy remains: the closure is unchanged, so no delta is published at all.
        await edges.RemoveAsync(("a", "b"));
        await circuit.StepAsync();
        Assert.False(closure.Reader.TryRead(out _));

        await edges.RemoveAsync(("a", "b"));
        await circuit.StepAsync();
        delta = await closure.Reader.ReadAsync();
        Assert.Equal(-1, delta.WeightOf(("a", "b")));
        Assert.Equal(-1, delta.WeightOf(("a", "c")));
        Assert.Equal(2, delta.DistinctCount);
    }

    [Fact]
    public async Task Ancestor_query_follows_a_reorganization()
    {
        // The same fixed point, phrased as a hierarchy: ancestors = reports ∪ (ancestors ∘ reports).
        var builder = ManualBuilder();
        var reports = builder.Input<(string Person, string Manager)>("reports");
        reports.Recurse(ancestors => ancestors
                .IndexBy(a => a.Manager)
                .Join(reports.IndexBy(r => r.Person), (_, a, r) => (a.Person, r.Manager)))
            .Output("ancestors");

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Input<(string Person, string Manager)>("reports");
        var output = circuit.Output<(string Person, string Manager)>("ancestors");

        await input.InsertAsync(("dave", "carol"));
        await input.InsertAsync(("carol", "bob"));
        await input.InsertAsync(("bob", "alice"));
        await circuit.StepAsync();
        var delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(("dave", "carol")));
        Assert.Equal(1, delta.WeightOf(("dave", "bob")));
        Assert.Equal(1, delta.WeightOf(("dave", "alice")));
        Assert.Equal(1, delta.WeightOf(("carol", "alice")));
        Assert.Equal(6, delta.DistinctCount);

        // Carol's subtree moves directly under Alice in one step: everyone below Carol
        // loses Bob as an ancestor, and nothing else changes.
        await input.RemoveAsync(("carol", "bob"));
        await input.InsertAsync(("carol", "alice"));
        await circuit.StepAsync();
        delta = await output.Reader.ReadAsync();
        Assert.Equal(-1, delta.WeightOf(("carol", "bob")));
        Assert.Equal(-1, delta.WeightOf(("dave", "bob")));
        Assert.Equal(2, delta.DistinctCount);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task Random_churn_matches_a_from_scratch_reference(int workers)
    {
        // Property-style: apply a random insert/delete churn (duplicates, cycles, and
        // self-loops all arise) and after every step compare the integrated closure against
        // reachability recomputed from scratch on the current edge multiset.
        var builder = ManualBuilder(workers);
        DefineClosure(builder);

        await using var circuit = builder.Build();
        circuit.Start();
        var edgesIn = circuit.Input<(string From, string To)>("edges");
        var closure = circuit.Output<(string From, string To)>("closure");

        var nodes = new[] { "a", "b", "c", "d", "e", "f" };
        var random = new Random(20260828);
        var multiset = new Dictionary<(string, string), int>();
        var integrated = new ZSet<(string From, string To)>();

        for (var step = 0; step < 40; step++)
        {
            var mutations = 1 + random.Next(3);
            for (var m = 0; m < mutations; m++)
            {
                var edge = (nodes[random.Next(nodes.Length)], nodes[random.Next(nodes.Length)]);
                var present = multiset.GetValueOrDefault(edge);
                if (present > 0 && random.Next(2) == 0)
                {
                    multiset[edge] = present - 1;
                    await edgesIn.RemoveAsync(edge);
                }
                else
                {
                    multiset[edge] = present + 1;
                    await edgesIn.InsertAsync(edge);
                }
            }

            await circuit.StepAsync();
            while (closure.Reader.TryRead(out var delta))
                integrated.AddAll(delta);

            var expected = ReferenceClosure(multiset);
            Assert.Equal(expected.Count, integrated.DistinctCount);
            foreach (var pair in expected)
                Assert.Equal(1, integrated.WeightOf(pair));
        }
    }

    private static HashSet<(string From, string To)> ReferenceClosure(Dictionary<(string, string), int> multiset)
    {
        var edges = multiset.Where(kv => kv.Value > 0).Select(kv => kv.Key).ToList();
        var closure = new HashSet<(string, string)>(edges);
        bool grew;
        do
        {
            grew = false;
            foreach (var (from, mid) in closure.ToList())
                foreach (var (mid2, to) in edges)
                    if (mid == mid2 && closure.Add((from, to)))
                        grew = true;
        } while (grew);
        return closure;
    }

    [Fact]
    public async Task Multi_worker_deltas_match_single_worker()
    {
        // The same scripted churn through a 1-worker and a 4-worker circuit must integrate
        // to identical per-step states — exchanges inside the loop (the body's IndexBy and
        // the stabilizer's shuffle) reshard every iteration, and convergence is global.
        var script = new (string From, string To, bool Insert)[]
        {
            ("a", "b", true), ("b", "c", true),
            ("c", "a", true), ("c", "d", true),
            ("b", "c", false),
            ("b", "c", true), ("d", "b", true),
            ("c", "a", false), ("a", "b", false),
        };

        var single = new ZSet<(string From, string To)>();
        var multi = new ZSet<(string From, string To)>();
        foreach (var workers in new[] { 1, 4 })
        {
            var builder = ManualBuilder(workers);
            DefineClosure(builder);
            await using var circuit = builder.Build();
            circuit.Start();
            var edges = circuit.Input<(string From, string To)>("edges");
            var closure = circuit.Output<(string From, string To)>("closure");
            var integrated = workers == 1 ? single : multi;

            foreach (var (from, to, insert) in script)
            {
                if (insert)
                    await edges.InsertAsync((from, to));
                else
                    await edges.RemoveAsync((from, to));
                await circuit.StepAsync();
                while (closure.Reader.TryRead(out var delta))
                    integrated.AddAll(delta);
            }
        }

        Assert.Equal(single.DistinctCount, multi.DistinctCount);
        foreach (var (pair, weight) in single)
            Assert.Equal(weight, multi.WeightOf(pair));
    }

    [Fact]
    public async Task Closure_runs_under_the_autostepping_pump()
    {
        // AutoStep batches the inserts into arbitrary steps, so the loop's cross-step
        // iteration profiles get exercised under nondeterministic step boundaries.
        var builder = new CircuitBuilder(new CircuitOptions { Workers = 4 });
        DefineClosure(builder);

        await using var circuit = builder.Build();
        circuit.Start();
        var edges = circuit.Input<(string From, string To)>("edges");
        var closure = circuit.Output<(string From, string To)>("closure");

        for (var i = 0; i < 20; i++)
            await edges.InsertAsync(($"n{i}", $"n{i + 1}"));
        edges.Complete();

        var integrated = new ZSet<(string From, string To)>();
        await foreach (var delta in closure.ReadAllAsync())
            integrated.AddAll(delta);

        // A 21-node chain closes to every ordered pair i < j.
        Assert.Equal(21 * 20 / 2, integrated.DistinctCount);
        for (var i = 0; i < 21; i++)
            for (var j = i + 1; j < 21; j++)
                Assert.Equal(1, integrated.WeightOf(($"n{i}", $"n{j}")));
    }

    [Fact]
    public async Task Max_iterations_guard_throws_when_the_loop_runs_too_deep()
    {
        // A 7-hop chain needs more than 3 iterations to close; the guard faults the step.
        var builder = ManualBuilder();
        DefineClosure(builder, maxIterations: 3);

        await using var circuit = builder.Build();
        circuit.Start();
        var edges = circuit.Input<(string From, string To)>("edges");

        for (var i = 0; i < 7; i++)
            await edges.InsertAsync(($"n{i}", $"n{i + 1}"));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => circuit.StepAsync());
        Assert.Contains("fixed point", ex.Message);
    }

    [Fact]
    public void Streams_defined_inside_a_loop_are_contained()
    {
        var builder = ManualBuilder();
        var edges = builder.Input<(string From, string To)>("edges");

        ZStream<(string From, string To)>? leaked = null;
        edges.Recurse(paths =>
        {
            leaked = paths;
            return paths.IndexBy(p => p.To)
                .Join(edges.IndexBy(e => e.From), (_, p, e) => (p.From, e.To));
        });

        // Per-iteration deltas are meaningless outside the loop; only Recurse's result escapes.
        Assert.Throws<InvalidOperationException>(() => leaked!.Map(p => p.From));
        Assert.Throws<InvalidOperationException>(() => leaked!.Output("boom"));
    }

    [Fact]
    public void Stateful_operators_without_loop_support_are_rejected_inside_a_body()
    {
        var builder = ManualBuilder();
        var edges = builder.Input<(string From, string To)>("edges");

        Assert.Throws<NotSupportedException>(() =>
            edges.Recurse(paths => paths.IndexBy(p => p.From).Count().Values().Map(_ => ("", ""))));
    }
}
