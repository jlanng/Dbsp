using CsCheck;

namespace Incremental.Tests;

/// <summary>
/// Property-based differential tests of the library's defining invariant: integrating the
/// deltas an operator emits must equal recomputing the query from scratch over the
/// integrated input — after every step, under arbitrary insert/retract interleavings.
/// Inputs are generated over deliberately small domains so values collide, pass through
/// zero, and retract often, and every property runs the same batches at Workers = 1 and
/// Workers = 4 — the sharded exchange path, in every synchronization mode: barrier,
/// point-to-point, and point-to-point with fragment streaming (chunked to one row per
/// fragment, so multi-fragment publication, partial merges, and the fragment sinks all
/// engage even on these tiny deltas). CsCheck shrinks failures and prints the seed to
/// reproduce them; the
/// op-list → batch folding below is deterministic, so a seed replays the exact circuit run.
/// </summary>
public class PropertyDifferentialTests
{
    private static readonly (int Workers, bool P2p, bool Streaming)[] Engines =
        [(1, false, false), (4, false, false), (4, true, false), (4, true, true)];

    // ---------------------------------------------------------------- generators

    /// <summary>One generated primitive: which value slot to touch, an alternate small
    /// payload dimension, whether to prefer retracting, and whether to start a new batch
    /// (step) before this op.</summary>
    private static Gen<List<(int Slot, int Alt, bool Retract, bool NewBatch)>> GenOps(int slots, int alts) =>
        Gen.Select(Gen.Int[0, slots - 1], Gen.Int[0, alts - 1], Gen.Bool, Gen.Bool).List[1, 50];

    /// <summary>Folds generated ops into per-step batches. By default the integrated input
    /// stays a well-formed multiset (a retract of an absent value becomes an insert), which
    /// is the contract the aggregate oracles need; <paramref name="allowNegative"/> lets
    /// weights go below zero for operators (Distinct) whose Z-set semantics cover it.</summary>
    private static List<List<WeightedValue<T>>> ToBatches<T>(
        List<(int Slot, int Alt, bool Retract, bool NewBatch)> ops,
        Func<int, int, T> value,
        bool allowNegative = false)
        where T : notnull
    {
        var counts = new Dictionary<T, long>();
        var batches = new List<List<WeightedValue<T>>> { new() };
        foreach (var op in ops)
        {
            if (op.NewBatch && batches[^1].Count > 0)
                batches.Add(new List<WeightedValue<T>>());
            var v = value(op.Slot, op.Alt);
            var weight = op.Retract && (allowNegative || counts.GetValueOrDefault(v) > 0) ? -1L : 1L;
            counts[v] = counts.GetValueOrDefault(v) + weight;
            batches[^1].Add(new WeightedValue<T>(v, weight));
        }
        return batches;
    }

    // ---------------------------------------------------------------- harness

    /// <summary>Runs the batches through the query step by step and, after every step,
    /// asserts integrated output ≡ oracle(integrated input).</summary>
    private static void AssertDifferential<TIn, TOut>(
        List<List<WeightedValue<TIn>>> batches,
        Func<ZStream<TIn>, ZStream<TOut>> query,
        Func<ZSet<TIn>, IEnumerable<WeightedValue<TOut>>> oracle)
        where TIn : notnull
        where TOut : notnull
    {
        RunAsync().GetAwaiter().GetResult();
        return;

        async Task RunAsync()
        {
            foreach (var (workers, p2p, streaming) in Engines)
            {
                var builder = new CircuitBuilder(new CircuitOptions
                {
                    AutoStep = false, Workers = workers, PointToPointExchange = p2p,
                    FragmentStreaming = streaming, StreamChunkRows = streaming ? 1 : 256,
                });
                var inputDef = builder.Input<TIn>("in");
                var outputDef = query(inputDef).ToOutput();
                await using var circuit = builder.Build();
                circuit.Start();
                var input = circuit.Handle(inputDef);
                var output = circuit.Handle(outputDef);

                var integratedIn = new ZSet<TIn>();
                var integratedOut = new ZSet<TOut>();
                for (var step = 0; step < batches.Count; step++)
                {
                    foreach (var (v, w) in batches[step])
                        await input.SendAsync(v, w);
                    await circuit.StepAsync();
                    while (output.Reader.TryRead(out var delta))
                        integratedOut.AddAll(delta);
                    foreach (var (v, w) in batches[step])
                        integratedIn.Add(v, w);
                    AssertZSetsEqual(oracle(integratedIn), integratedOut, workers, p2p, streaming, step);
                }
            }
        }
    }

    /// <summary>Two-input variant for joins: batches carry a side flag.</summary>
    private static void AssertDifferential2<TIn, TOut>(
        List<List<(bool Left, WeightedValue<TIn> Entry)>> batches,
        Func<ZStream<TIn>, ZStream<TIn>, ZStream<TOut>> query,
        Func<ZSet<TIn>, ZSet<TIn>, IEnumerable<WeightedValue<TOut>>> oracle)
        where TIn : notnull
        where TOut : notnull
    {
        RunAsync().GetAwaiter().GetResult();
        return;

        async Task RunAsync()
        {
            foreach (var (workers, p2p, streaming) in Engines)
            {
                var builder = new CircuitBuilder(new CircuitOptions
                {
                    AutoStep = false, Workers = workers, PointToPointExchange = p2p,
                    FragmentStreaming = streaming, StreamChunkRows = streaming ? 1 : 256,
                });
                var leftDef = builder.Input<TIn>("left");
                var rightDef = builder.Input<TIn>("right");
                var outputDef = query(leftDef, rightDef).ToOutput();
                await using var circuit = builder.Build();
                circuit.Start();
                var left = circuit.Handle(leftDef);
                var right = circuit.Handle(rightDef);
                var output = circuit.Handle(outputDef);

                var integratedL = new ZSet<TIn>();
                var integratedR = new ZSet<TIn>();
                var integratedOut = new ZSet<TOut>();
                for (var step = 0; step < batches.Count; step++)
                {
                    foreach (var (isLeft, entry) in batches[step])
                        await (isLeft ? left : right).SendAsync(entry.Value, entry.Weight);
                    await circuit.StepAsync();
                    while (output.Reader.TryRead(out var delta))
                        integratedOut.AddAll(delta);
                    foreach (var (isLeft, entry) in batches[step])
                        (isLeft ? integratedL : integratedR).Add(entry.Value, entry.Weight);
                    AssertZSetsEqual(oracle(integratedL, integratedR), integratedOut, workers, p2p, streaming, step);
                }
            }
        }
    }

    private static void AssertZSetsEqual<T>(
        IEnumerable<WeightedValue<T>> expectedEntries, ZSet<T> actual, int workers, bool p2p, bool streaming, int step)
        where T : notnull
    {
        var expected = new ZSet<T>(expectedEntries);
        var equal = expected.DistinctCount == actual.DistinctCount
            && expected.All(e => actual.WeightOf(e.Value) == e.Weight);
        if (!equal)
            throw new Exception(
                $"differential mismatch (workers={workers}, " +
                $"{(streaming ? "fragment-streaming" : p2p ? "point-to-point" : "barrier")}, after step {step}): " +
                $"expected {expected} but integrated deltas give {actual}");
    }

    /// <summary>Integrated (key → positive-weight rows) view of a keyed input multiset.</summary>
    private static Dictionary<int, List<(int Val, long Weight)>> Groups(ZSet<(int Key, int Val)> input)
    {
        var groups = new Dictionary<int, List<(int, long)>>();
        foreach (var (v, w) in input)
        {
            if (!groups.TryGetValue(v.Key, out var list))
                groups[v.Key] = list = new List<(int, long)>();
            list.Add((v.Val, w));
        }
        return groups;
    }

    // ---------------------------------------------------------------- properties

    [Fact]
    public void Distinct_matches_presence_recompute() =>
        GenOps(slots: 6, alts: 1).Sample(ops =>
        {
            // allowNegative: Distinct is defined for any Z-set — present means weight > 0.
            var batches = ToBatches(ops, (slot, _) => slot, allowNegative: true);
            AssertDifferential(
                batches,
                s => s.Distinct(),
                input => input.Where(e => e.Weight > 0).Select(e => new WeightedValue<int>(e.Value, 1)));
        }, iter: 50);

    [Fact]
    public void Count_matches_group_recompute() =>
        GenOps(slots: 4, alts: 5).Sample(ops =>
        {
            var batches = ToBatches(ops, (k, v) => (Key: k, Val: v));
            AssertDifferential(
                batches,
                s => s.GroupBy(x => x.Key).Count(),
                input => Groups(input).Where(g => g.Value.Sum(r => r.Weight) > 0)
                    .Select(g => new WeightedValue<KeyedValue<int, long>>(
                        new KeyedValue<int, long>(g.Key, g.Value.Sum(r => r.Weight)), 1)));
        }, iter: 40);

    [Fact]
    public void Sum_decimal_matches_group_recompute() =>
        GenOps(slots: 4, alts: 5).Sample(ops =>
        {
            var batches = ToBatches(ops, (k, v) => (Key: k, Val: v));
            AssertDifferential(
                batches,
                // Val - 2 makes group sums hit zero while rows remain — the interesting case.
                s => s.GroupBy(x => x.Key).Sum(x => (decimal)(x.Val - 2)),
                input => Groups(input).Where(g => g.Value.Sum(r => r.Weight) > 0)
                    .Select(g => new WeightedValue<KeyedValue<int, decimal>>(
                        new KeyedValue<int, decimal>(g.Key, g.Value.Sum(r => (r.Val - 2) * r.Weight)), 1)));
        }, iter: 40);

    [Fact]
    public void NonInvertible_aggregate_matches_group_recompute() =>
        GenOps(slots: 3, alts: 4).Sample(ops =>
        {
            var batches = ToBatches(ops, (k, v) => (Key: k, Val: v));
            AssertDifferential(
                batches,
                s => s.GroupBy(x => x.Key).Aggregate(new SortedConcat()),
                input => Groups(input).Where(g => g.Value.Any(r => r.Weight > 0))
                    .Select(g => new WeightedValue<KeyedValue<int, string>>(
                        new KeyedValue<int, string>(g.Key, SortedConcat.Compute(
                            g.Value.Where(r => r.Weight > 0).Select(r => (r.Val, r.Weight)))), 1)));
        }, iter: 30);

    /// <summary>Deliberately non-invertible: sorted "value×multiplicity" concatenation —
    /// nothing about a retraction can be undone without the full group.</summary>
    private sealed class SortedConcat : IAggregateFunction<(int Key, int Val), string>
    {
        public string Aggregate(IReadOnlyList<WeightedValue<(int Key, int Val)>> group) =>
            Compute(group.Select(r => (r.Value.Val, r.Weight)));

        public static string Compute(IEnumerable<(int Val, long Weight)> rows) =>
            string.Join("|", rows
                .GroupBy(r => r.Val, r => r.Weight)
                .Select(g => (Val: g.Key, Count: g.Sum()))
                .Where(r => r.Count != 0)
                .OrderBy(r => r.Val)
                .Select(r => $"{r.Val}x{r.Count}"));
    }

    [Fact]
    public void Min_and_max_match_group_recompute() =>
        GenOps(slots: 3, alts: 6).Sample(ops =>
        {
            var batches = ToBatches(ops, (k, v) => (Key: k, Val: v));
            AssertDifferential(
                batches,
                s =>
                {
                    var keyed = s.GroupBy(x => x.Key);
                    // Tag min 0 / max 1 and merge, so one circuit exercises both trees —
                    // including deleting the current extremum, the tournament tree's hot spot.
                    return keyed.Min(x => x.Val).MapValues(v => (Tag: 0, V: v))
                        .Concat(keyed.Max(x => x.Val).MapValues(v => (Tag: 1, V: v)));
                },
                input => Groups(input)
                    .Select(g => (g.Key, Vals: g.Value.Where(r => r.Weight > 0).Select(r => r.Val).ToList()))
                    .Where(g => g.Vals.Count > 0)
                    .SelectMany(g => new[]
                    {
                        new WeightedValue<KeyedValue<int, (int Tag, int V)>>(
                            new KeyedValue<int, (int, int)>(g.Key, (0, g.Vals.Min())), 1),
                        new WeightedValue<KeyedValue<int, (int Tag, int V)>>(
                            new KeyedValue<int, (int, int)>(g.Key, (1, g.Vals.Max())), 1),
                    }));
        }, iter: 30);

    [Fact]
    public void TopK_matches_ranked_recompute() =>
        GenOps(slots: 3, alts: 5).Sample(ops =>
        {
            var batches = ToBatches(ops, (k, v) => (Key: k, Val: v));
            AssertDifferential(
                batches,
                // Ranked by the value itself: ties are identical rows, so multiplicities —
                // which count toward K — make the top-2 recompute exact.
                s => s.GroupBy(x => x.Key).TopK(2, x => x.Val),
                input => Groups(input).SelectMany(g => g.Value
                    .GroupBy(r => r.Val, r => r.Weight)
                    .Select(v => (Val: v.Key, Count: v.Sum()))
                    .Where(v => v.Count > 0)
                    .OrderByDescending(v => v.Val)
                    .SelectMany(v => Enumerable.Repeat(v.Val, (int)v.Count))
                    .Take(2)
                    .GroupBy(v => v)
                    .Select(v => new WeightedValue<KeyedValue<int, (int Key, int Val)>>(
                        new KeyedValue<int, (int Key, int Val)>(g.Key, (g.Key, v.Key)), v.Count()))));
        }, iter: 30);

    private static Gen<List<(int Slot, int Alt, bool Retract, bool NewBatch, bool Left)>> GenOps2(int slots, int alts) =>
        Gen.Select(Gen.Int[0, slots - 1], Gen.Int[0, alts - 1], Gen.Bool, Gen.Bool, Gen.Bool).List[1, 50];

    private static List<List<(bool Left, WeightedValue<(int Key, int Val)> Entry)>> ToBatches2(
        List<(int Slot, int Alt, bool Retract, bool NewBatch, bool Left)> ops)
    {
        var counts = new Dictionary<(bool, (int, int)), long>();
        var batches = new List<List<(bool, WeightedValue<(int Key, int Val)>)>> { new() };
        foreach (var op in ops)
        {
            if (op.NewBatch && batches[^1].Count > 0)
                batches.Add(new List<(bool, WeightedValue<(int Key, int Val)>)>());
            var v = (Key: op.Slot, Val: op.Alt);
            var key = (op.Left, (v.Key, v.Val));
            var weight = op.Retract && counts.GetValueOrDefault(key) > 0 ? -1L : 1L;
            counts[key] = counts.GetValueOrDefault(key) + weight;
            batches[^1].Add((op.Left, new WeightedValue<(int Key, int Val)>(v, weight)));
        }
        return batches;
    }

    [Fact]
    public void Inner_join_matches_product_recompute() =>
        GenOps2(slots: 3, alts: 3).Sample(ops =>
        {
            AssertDifferential2(
                ToBatches2(ops),
                (l, r) => l.GroupBy(x => x.Key)
                    .Join(r.GroupBy(x => x.Key), (k, lv, rv) => (k, lv.Val, rv.Val)),
                (left, right) => JoinOracle(left, right));
        }, iter: 25);

    private static IEnumerable<WeightedValue<(int, int, int)>> JoinOracle(
        ZSet<(int Key, int Val)> left, ZSet<(int Key, int Val)> right) =>
        from l in left
        from r in right
        where l.Value.Key == r.Value.Key
        group l.Weight * r.Weight by (l.Value.Key, l.Value.Val, r.Value.Val) into g
        let w = g.Sum()
        where w != 0
        select new WeightedValue<(int, int, int)>(g.Key, w);

    [Fact]
    public void AntiJoin_matches_no_match_recompute() =>
        GenOps2(slots: 3, alts: 3).Sample(ops =>
        {
            AssertDifferential2(
                ToBatches2(ops),
                (l, r) => l.GroupBy(x => x.Key).AntiJoin(r.GroupBy(x => x.Key)),
                (left, right) =>
                {
                    var matchedKeys = right.Where(e => e.Weight > 0).Select(e => e.Value.Key).ToHashSet();
                    return left.Where(e => !matchedKeys.Contains(e.Value.Key))
                        .Select(e => new WeightedValue<KeyedValue<int, (int Key, int Val)>>(
                            new KeyedValue<int, (int Key, int Val)>(e.Value.Key, e.Value), e.Weight));
                });
        }, iter: 25);

    [Fact]
    public void LeftJoin_matches_recompute() =>
        GenOps2(slots: 3, alts: 3).Sample(ops =>
        {
            AssertDifferential2(
                ToBatches2(ops),
                (l, r) => l.GroupBy(x => x.Key).LeftJoin(
                    r.GroupBy(x => x.Key),
                    (k, lv, rv) => (k, lv.Val, rv.Val),
                    (k, lv) => (k, lv.Val, -1)),
                (left, right) =>
                {
                    var matchedKeys = right.Where(e => e.Weight > 0).Select(e => e.Value.Key).ToHashSet();
                    var unmatched = left.Where(e => !matchedKeys.Contains(e.Value.Key))
                        .Select(e => new WeightedValue<(int, int, int)>((e.Value.Key, e.Value.Val, -1), e.Weight));
                    return JoinOracle(left, right).Concat(unmatched);
                });
        }, iter: 20);

    [Fact]
    public void SessionWindow_matches_sessionization_recompute() =>
        GenOps(slots: 3, alts: 8).Sample(ops =>
        {
            var gap = TimeSpan.FromMinutes(2);
            var batches = ToBatches(ops, (k, t) => (Key: k, T: t));
            AssertDifferential(
                batches,
                // Generous lateness so nothing is dropped: this property is about
                // sessionization, not the watermark.
                s => s.WithEventTime(e => Time(e.T), lateness: TimeSpan.FromDays(1))
                    .GroupBy(e => e.Key)
                    .SessionWindow(gap)
                    .Count(),
                input =>
                {
                    var rows = input.Where(e => e.Weight > 0).ToList();
                    return rows.GroupBy(e => e.Value.Key).SelectMany(g =>
                    {
                        // Recompute sessionization from scratch: sorted distinct times,
                        // split where the inactivity gap is reached (>= gap per the docs),
                        // then one count row per session over the events it contains.
                        var times = g.Select(e => e.Value.T).Distinct().OrderBy(t => t).ToList();
                        var bounds = new Dictionary<int, (DateTimeOffset Start, DateTimeOffset End)>();
                        for (var i = 0; i < times.Count;)
                        {
                            var j = i;
                            while (j + 1 < times.Count && Time(times[j + 1]) - Time(times[j]) < gap)
                                j++;
                            var span = (Start: Time(times[i]), End: Time(times[j]) + gap);
                            for (var t = i; t <= j; t++)
                                bounds[times[t]] = span;
                            i = j + 1;
                        }
                        return g.GroupBy(e => bounds[e.Value.T])
                            .Select(session => new WeightedValue<WindowedValue<int, long>>(
                                new WindowedValue<int, long>(
                                    session.Key.Start, session.Key.End, g.Key, session.Sum(e => e.Weight)),
                                1));
                    });
                });
        }, iter: 20);

    private static DateTimeOffset Time(int minutes) =>
        DateTimeOffset.UnixEpoch + TimeSpan.FromMinutes(minutes);

    [Fact]
    public void Recurse_transitive_closure_matches_reachability_recompute() =>
        GenOps(slots: 4, alts: 4).Sample(ops =>
        {
            // Edges over 4 nodes (self-loops included — cyclic self-support is the hard case).
            var batches = ToBatches(ops, (from, to) => (From: from, To: to));
            AssertDifferential(
                batches,
                edges => edges.Recurse(paths => paths
                    .GroupBy(p => p.To)
                    .Join(edges.GroupBy(e => e.From), (_, p, e) => (p.From, e.To))),
                input =>
                {
                    var present = input.Where(e => e.Weight > 0).Select(e => e.Value).ToHashSet();
                    var reach = new HashSet<(int From, int To)>(present);
                    // Floyd–Warshall-style closure over the tiny node domain.
                    for (var changed = true; changed;)
                    {
                        changed = false;
                        foreach (var a in reach.ToList())
                            foreach (var b in present)
                                if (a.To == b.From && reach.Add((a.From, b.To)))
                                    changed = true;
                    }
                    return reach.Select(p => new WeightedValue<(int From, int To)>(p, 1));
                });
        }, iter: 15);

    // ---------------------------------------------------------------- Z-set algebra

    [Fact]
    public void ZSet_addition_is_pointwise_and_zero_weights_vanish() =>
        Gen.Select(Gen.Int[0, 5], Gen.Long[-3, 3]).List[0, 40].Sample(entries =>
        {
            var zset = new ZSet<int>();
            var reference = new Dictionary<int, long>();
            foreach (var (value, weight) in entries)
            {
                zset.Add(value, weight);
                reference[value] = reference.GetValueOrDefault(value) + weight;
            }
            foreach (var (value, weight) in reference)
                if (zset.WeightOf(value) != weight)
                    throw new Exception($"weight of {value}: expected {weight}, got {zset.WeightOf(value)}");
            if (zset.DistinctCount != reference.Count(kv => kv.Value != 0))
                throw new Exception("zero-weight entries must vanish");

            var cancelled = new ZSet<int>(zset);
            cancelled.AddAll(zset.Negated());
            if (!cancelled.IsEmpty)
                throw new Exception($"x + (-x) must be empty, got {cancelled}");
        }, iter: 100);
}
