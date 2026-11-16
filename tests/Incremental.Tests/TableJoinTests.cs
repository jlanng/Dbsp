using CsCheck;
using Incremental.Storage;
using Incremental.Tables;

namespace Incremental.Tests;

/// <summary>
/// The retroactive join of a grouped stream against a shared table
/// (<c>docs/design/shared-tables.md</c>, <em>The two joins</em>): facts are enriched from the
/// table's row for their key, and a later change to that row corrects every fact already
/// joined to it. The differential properties pin it to the existing stream join over a keyed
/// input fed the same commands; the directed tests cover what only a table can do — one
/// table under several circuits, a circuit attached late, a restart from a committed version.
/// </summary>
public class TableJoinTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(20);
    private static readonly DateTimeOffset T0 = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
    private const string None = "(none)";

    private sealed record Instrument(int Id, string Symbol);

    private sealed record Trade(int Id, int InstrumentId);

    private static SharedTable<int, Instrument> NewTable(SharedTableOptions? options = null) =>
        new("instruments", i => i.Id, options);

    private static async Task<long> Commit(SharedTable<int, Instrument> table, params Instrument[] rows)
    {
        foreach (var row in rows)
            await table.Writer.UpsertAsync(row);
        return await table.Writer.CommitAsync().AsTask().WaitAsync(Timeout);
    }

    private static async Task<long> Delete(SharedTable<int, Instrument> table, params int[] keys)
    {
        foreach (var key in keys)
            await table.Writer.DeleteAsync(key);
        return await table.Writer.CommitAsync().AsTask().WaitAsync(Timeout);
    }

    /// <summary>Trades enriched with their instrument's symbol, as (trade id, symbol).</summary>
    private sealed record Pipeline(
        CircuitBuilder Builder,
        InputStream<Trade> Trades,
        Output<(int Trade, string Symbol)> Enriched);

    private static Pipeline Build(
        SharedTable<int, Instrument> table,
        bool left = false,
        int workers = 1,
        bool autoStep = false,
        IStorageProvider? storage = null)
    {
        var builder = new CircuitBuilder(new CircuitOptions
        {
            AutoStep = autoStep,
            Workers = workers,
            Storage = storage ?? new InMemoryStorageProvider(),
        });
        var instruments = builder.Table(table);
        var trades = builder.Input<Trade>("trades");
        var byInstrument = trades.GroupBy(t => t.InstrumentId);
        var enriched = left
            ? byInstrument.LeftJoin(instruments, (_, trade, instrument) => (trade.Id, instrument.Symbol), (_, trade) => (trade.Id, None))
            : byInstrument.Join(instruments, (_, trade, instrument) => (trade.Id, instrument.Symbol));
        return new Pipeline(builder, trades, enriched.Named("enrich").ToOutput());
    }

    /// <summary>Everything buffered on <paramref name="output"/>, summed into one delta.</summary>
    private static ZSet<T> Drain<T>(OutputHandle<T> output) where T : notnull
    {
        var drained = new ZSet<T>();
        while (output.Reader.TryRead(out var delta))
            drained.AddAll(delta);
        return drained;
    }

    private static void AssertDelta<T>(ZSet<T> actual, params (T Value, long Weight)[] expected) where T : notnull
    {
        var want = new ZSet<T>();
        foreach (var (value, weight) in expected)
            want.Add(value, weight);
        Assert.True(
            want.DistinctCount == actual.DistinctCount && want.All(e => actual.WeightOf(e.Value) == e.Weight),
            $"expected {want} but got {actual}");
    }

    // ── differential: the table join against the stream join it replaces ────────────────

    private sealed record Row(int Key, int Val);

    private enum Kind { InsertFact, RetractFact, UpsertRow, DeleteRow }

    /// <summary>One generated command: what to do, to which key, with which payload, whether
    /// to step the circuit first, and — for a table command — whether to commit a version
    /// right after it, so that a step sees anything from no version to several.</summary>
    private static readonly Gen<List<(int Kind, int Key, int Alt, bool NewStep, bool Commit)>> GenCommands =
        Gen.Select(Gen.Int[0, 3], Gen.Int[0, 3], Gen.Int[0, 2], Gen.Bool, Gen.Bool).List[1, 60];

    /// <summary>Runs one circuit that joins the same facts twice — against the table, and
    /// against a keyed input fed the table's commands — and asserts after every step that
    /// the two integrated outputs are equal. Fact weights are left free to go negative: both
    /// joins are linear in the facts.</summary>
    private static void AssertMatchesStreamJoin(
        List<(int Kind, int Key, int Alt, bool NewStep, bool Commit)> commands, bool left)
    {
        RunAsync().GetAwaiter().GetResult();
        return;

        async Task RunAsync()
        {
            foreach (var workers in new[] { 1, 4 })
            {
                var table = new SharedTable<int, Row>("reference", r => r.Key);
                var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Workers = workers });
                var reference = builder.Table(table);
                var factsDef = builder.Input<(int Key, int Val)>("facts");
                var rowsDef = builder.KeyedInput<int, Row>("rows", r => r.Key);
                var viaTableDef = (left
                    ? factsDef.GroupBy(f => f.Key).LeftJoin(reference, (k, f, r) => (k, f.Val, r.Val), (k, f) => (k, f.Val, -1))
                    : factsDef.GroupBy(f => f.Key).Join(reference, (k, f, r) => (k, f.Val, r.Val))).ToOutput();
                var rowsByKey = rowsDef.GroupBy(r => r.Key);
                var viaStreamDef = (left
                    ? factsDef.GroupBy(f => f.Key).LeftJoin(rowsByKey, (k, f, r) => (k, f.Val, r.Val), (k, f) => (k, f.Val, -1))
                    : factsDef.GroupBy(f => f.Key).Join(rowsByKey, (k, f, r) => (k, f.Val, r.Val))).ToOutput();

                await using var circuit = builder.Build();
                circuit.Start();
                var facts = circuit.Handle(factsDef);
                var rows = circuit.Handle(rowsDef);
                var viaTable = new ZSet<(int, int, int)>();
                var viaStream = new ZSet<(int, int, int)>();
                var step = 0;

                async Task StepAndCompare()
                {
                    await table.Writer.CommitAsync();
                    await circuit.StepAsync();
                    viaTable.AddAll(Drain(circuit.Handle(viaTableDef)));
                    viaStream.AddAll(Drain(circuit.Handle(viaStreamDef)));
                    var equal = viaTable.DistinctCount == viaStream.DistinctCount
                        && viaStream.All(e => viaTable.WeightOf(e.Value) == e.Weight);
                    if (!equal)
                        throw new Exception(
                            $"differential mismatch (workers={workers}, left={left}, after step {step}): " +
                            $"the stream join gives {viaStream} but the table join gives {viaTable}");
                    step++;
                }

                var pending = false;
                foreach (var command in commands)
                {
                    if (command.NewStep && pending)
                    {
                        await StepAndCompare();
                        pending = false;
                    }
                    switch ((Kind)command.Kind)
                    {
                        case Kind.InsertFact:
                            await facts.SendAsync((command.Key, command.Alt), 1);
                            break;
                        case Kind.RetractFact:
                            await facts.SendAsync((command.Key, command.Alt), -1);
                            break;
                        case Kind.UpsertRow:
                            await table.Writer.UpsertAsync(new Row(command.Key, command.Alt));
                            await rows.UpsertAsync(new Row(command.Key, command.Alt));
                            break;
                        default:
                            await table.Writer.DeleteAsync(command.Key);
                            await rows.DeleteAsync(command.Key);
                            break;
                    }
                    if (command.Commit && (Kind)command.Kind is Kind.UpsertRow or Kind.DeleteRow)
                        await table.Writer.CommitAsync();
                    pending = true;
                }
                await StepAndCompare();
            }
        }
    }

    [Fact]
    public void A_table_join_matches_the_stream_join_over_a_keyed_input_fed_the_same_commands() =>
        GenCommands.Sample(commands => AssertMatchesStreamJoin(commands, left: false));

    [Fact]
    public void A_table_left_join_matches_the_stream_left_join_over_a_keyed_input_fed_the_same_commands() =>
        GenCommands.Sample(commands => AssertMatchesStreamJoin(commands, left: true));

    // ── directed ───────────────────────────────────────────────────────────────────────

    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    public async Task A_reference_row_arriving_after_its_facts_enriches_them(int workers)
    {
        var table = NewTable();
        var pipeline = Build(table, workers: workers);
        await using var circuit = pipeline.Builder.Build();
        circuit.Start();
        var trades = circuit.Handle(pipeline.Trades);
        var enriched = circuit.Handle(pipeline.Enriched);

        await trades.InsertAsync(new Trade(1, 7));
        await trades.InsertAsync(new Trade(2, 7));
        await trades.InsertAsync(new Trade(3, 8));
        await circuit.StepAsync();
        AssertDelta(Drain(enriched));

        await Commit(table, new Instrument(7, "SEVEN"));
        await circuit.StepAsync();
        AssertDelta(Drain(enriched), ((1, "SEVEN"), 1), ((2, "SEVEN"), 1));

        // A fact and its row in the same step join once, not twice.
        await Commit(table, new Instrument(9, "NINE"));
        await trades.InsertAsync(new Trade(4, 9));
        await circuit.StepAsync();
        AssertDelta(Drain(enriched), ((4, "NINE"), 1));
    }

    [Fact]
    public async Task A_change_to_a_hot_row_re_enriches_every_fact_under_its_key_and_no_other()
    {
        const int hot = 500;
        var table = NewTable();
        await Commit(table, new Instrument(7, "OLD"), new Instrument(8, "OTHER"));
        var pipeline = Build(table, workers: 3);
        await using var circuit = pipeline.Builder.Build();
        circuit.Start();
        var trades = circuit.Handle(pipeline.Trades);
        var enriched = circuit.Handle(pipeline.Enriched);

        for (var id = 0; id < hot; id++)
            await trades.InsertAsync(new Trade(id, 7));
        await trades.InsertAsync(new Trade(hot, 8));
        await circuit.StepAsync();
        Assert.Equal(hot + 1, Drain(enriched).DistinctCount);

        await Commit(table, new Instrument(7, "NEW"));
        await circuit.StepAsync();
        var delta = Drain(enriched);
        Assert.Equal(2 * hot, delta.DistinctCount);
        for (var id = 0; id < hot; id++)
        {
            Assert.Equal(-1, delta.WeightOf((id, "OLD")));
            Assert.Equal(1, delta.WeightOf((id, "NEW")));
        }

        // A retracted fact takes its current enrichment with it, not the one it arrived with.
        await trades.RemoveAsync(new Trade(0, 7));
        await circuit.StepAsync();
        AssertDelta(Drain(enriched), ((0, "NEW"), -1));
    }

    [Fact]
    public async Task Deleting_a_reference_row_retracts_the_facts_joined_to_it()
    {
        var table = NewTable();
        await Commit(table, new Instrument(7, "SEVEN"), new Instrument(8, "EIGHT"));
        var pipeline = Build(table, workers: 2);
        await using var circuit = pipeline.Builder.Build();
        circuit.Start();
        var trades = circuit.Handle(pipeline.Trades);
        var enriched = circuit.Handle(pipeline.Enriched);

        await trades.InsertAsync(new Trade(1, 7));
        await trades.InsertAsync(new Trade(2, 8));
        await circuit.StepAsync();
        AssertDelta(Drain(enriched), ((1, "SEVEN"), 1), ((2, "EIGHT"), 1));

        await Delete(table, 7);
        await circuit.StepAsync();
        AssertDelta(Drain(enriched), ((1, "SEVEN"), -1));

        // While the row is gone a new fact for the key joins nothing; both come back with it.
        await trades.InsertAsync(new Trade(3, 7));
        await circuit.StepAsync();
        AssertDelta(Drain(enriched));

        await Commit(table, new Instrument(7, "BACK"));
        await circuit.StepAsync();
        AssertDelta(Drain(enriched), ((1, "BACK"), 1), ((3, "BACK"), 1));
    }

    [Fact]
    public async Task A_left_join_flips_its_facts_between_unmatched_and_matched_as_the_row_comes_and_goes()
    {
        var table = NewTable();
        var pipeline = Build(table, left: true, workers: 2);
        await using var circuit = pipeline.Builder.Build();
        circuit.Start();
        var trades = circuit.Handle(pipeline.Trades);
        var enriched = circuit.Handle(pipeline.Enriched);

        await trades.InsertAsync(new Trade(1, 7));
        await trades.InsertAsync(new Trade(2, 7));
        await circuit.StepAsync();
        AssertDelta(Drain(enriched), ((1, None), 1), ((2, None), 1));

        await Commit(table, new Instrument(7, "SEVEN"));
        await circuit.StepAsync();
        AssertDelta(Drain(enriched), ((1, None), -1), ((2, None), -1), ((1, "SEVEN"), 1), ((2, "SEVEN"), 1));

        await Commit(table, new Instrument(7, "SEVEN'"));
        await circuit.StepAsync();
        AssertDelta(Drain(enriched), ((1, "SEVEN"), -1), ((2, "SEVEN"), -1), ((1, "SEVEN'"), 1), ((2, "SEVEN'"), 1));

        await Delete(table, 7);
        await trades.InsertAsync(new Trade(3, 7));
        await circuit.StepAsync();
        AssertDelta(Drain(enriched), ((1, "SEVEN'"), -1), ((2, "SEVEN'"), -1), ((1, None), 1), ((2, None), 1), ((3, None), 1));

        // A row inserted and deleted between two steps never existed as far as the join can tell.
        await Commit(table, new Instrument(7, "BRIEFLY"));
        await Delete(table, 7);
        await circuit.StepAsync();
        AssertDelta(Drain(enriched));
    }

    [Fact]
    public async Task A_restarted_circuit_corrects_the_facts_in_its_checkpoint_for_the_versions_it_missed()
    {
        var table = NewTable();
        var storage = new InMemoryCheckpointStorageProvider();
        var integrated = new ZSet<(int Trade, string Symbol)>();
        await Commit(table, new Instrument(7, "SEVEN"), new Instrument(8, "EIGHT"), new Instrument(9, "NINE"));

        var before = Build(table, left: true, workers: 2, storage: storage);
        await using (var first = before.Builder.Build())
        {
            first.Start();
            var trades = first.Handle(before.Trades);
            await trades.InsertAsync(new Trade(1, 7));
            await trades.InsertAsync(new Trade(2, 8));
            await trades.InsertAsync(new Trade(3, 9));
            await trades.InsertAsync(new Trade(4, 10));
            await first.StepAsync();
            integrated.AddAll(Drain(first.Handle(before.Enriched)));
        }

        // The table moves on while no circuit is running: a change, a delete, an insert.
        await Commit(table, new Instrument(7, "SEVEN'"));
        await Delete(table, 8);
        await Commit(table, new Instrument(10, "TEN"));

        var after = Build(table, left: true, workers: 2, storage: storage);
        await using var resumed = after.Builder.Build();
        resumed.Start();
        await resumed.Handle(after.Trades).InsertAsync(new Trade(5, 7));
        await resumed.StepAsync();

        // Exactly the corrections for what it held, plus the new fact — trade 3 untouched.
        var delta = Drain(resumed.Handle(after.Enriched));
        AssertDelta(delta,
            ((1, "SEVEN"), -1), ((1, "SEVEN'"), 1),
            ((2, "EIGHT"), -1), ((2, None), 1),
            ((4, None), -1), ((4, "TEN"), 1),
            ((5, "SEVEN'"), 1));

        // Which is where a circuit that never stopped would be.
        integrated.AddAll(delta);
        AssertDelta(integrated, ((1, "SEVEN'"), 1), ((2, None), 1), ((3, "NINE"), 1), ((4, "TEN"), 1), ((5, "SEVEN'"), 1));
    }

    [Fact]
    public async Task A_circuit_attached_after_the_table_is_loaded_enriches_new_facts_with_no_backfill()
    {
        const int rows = 1000;
        var table = NewTable();
        for (var id = 0; id < rows; id++)
            await table.Writer.UpsertAsync(new Instrument(id, "v1-" + id));
        await table.Writer.CommitAsync();

        var early = Build(table);
        await using var first = early.Builder.Build();
        first.Start();
        await first.Handle(early.Trades).InsertAsync(new Trade(1, 500));
        await first.StepAsync();
        AssertDelta(Drain(first.Handle(early.Enriched)), ((1, "v1-500"), 1));

        // The second pipeline is added to a service whose table is already full.
        var late = Build(table, workers: 3);
        await using var second = late.Builder.Build();
        second.Start();
        await second.StepAsync();
        AssertDelta(Drain(second.Handle(late.Enriched)));

        await second.Handle(late.Trades).InsertAsync(new Trade(2, 500));
        await second.Handle(late.Trades).InsertAsync(new Trade(3, 999));
        await second.StepAsync();
        AssertDelta(Drain(second.Handle(late.Enriched)), ((2, "v1-500"), 1), ((3, "v1-999"), 1));

        // From here both are ordinary readers of the same table.
        await Commit(table, new Instrument(500, "v2-500"));
        await first.StepAsync();
        await second.StepAsync();
        AssertDelta(Drain(first.Handle(early.Enriched)), ((1, "v1-500"), -1), ((1, "v2-500"), 1));
        AssertDelta(Drain(second.Handle(late.Enriched)), ((2, "v1-500"), -1), ((2, "v2-500"), 1));
    }

    /// <summary>Three auto-stepping circuits — different worker counts, inner and left —
    /// join one table while its writer churns and each circuit's facts come and go. Nothing
    /// coordinates them: every circuit pins its own versions and coalesces its own changes.
    /// Each must still converge on the join of its own live facts with the table's final
    /// rows, which it cannot do if it misses a change, applies one twice, or routes one to a
    /// worker that does not hold the key's facts.</summary>
    [Fact]
    public async Task Several_circuits_joining_one_table_under_a_churning_writer_each_converge_on_the_final_table()
    {
        const int keys = 64;
        const int versions = 400;
        var table = NewTable(new SharedTableOptions { TailVersions = 8 });

        (Circuit Circuit, bool Left, Task<List<Trade>> Feeder, Task Consumer, ZSet<(int Trade, string Symbol)> Integrated)
            RunCircuit(int workers, bool left, CancellationToken stop)
        {
            var pipeline = Build(table, left, workers, autoStep: true);
            var circuit = pipeline.Builder.Build();
            circuit.Start();
            var input = circuit.Handle(pipeline.Trades);
            var integrated = new ZSet<(int Trade, string Symbol)>();

            var feeder = Task.Run(async () =>
            {
                var random = new Random(workers);
                var live = new List<Trade>();
                for (var id = 0; !stop.IsCancellationRequested; id++)
                {
                    if (live.Count > 0 && random.Next(4) == 0)
                    {
                        var at = random.Next(live.Count);
                        await input.RemoveAsync(live[at]);
                        live.RemoveAt(at);
                    }
                    var trade = new Trade(id, random.Next(keys));
                    await input.InsertAsync(trade);
                    live.Add(trade);
                    if (id % 16 == 0)
                        await Task.Yield();
                }
                return live;
            });
            var consumer = Task.Run(async () =>
            {
                await foreach (var delta in circuit.Handle(pipeline.Enriched).Reader.ReadAllAsync())
                    lock (integrated)
                        integrated.AddAll(delta);
            });
            return (circuit, left, feeder, consumer, integrated);
        }

        using var stop = new CancellationTokenSource();
        var circuits = new[]
        {
            RunCircuit(workers: 1, left: false, stop.Token),
            RunCircuit(workers: 3, left: true, stop.Token),
            RunCircuit(workers: 2, left: false, stop.Token),
        };

        var churn = new Random(42);
        for (var v = 1; v <= versions; v++)
        {
            for (var i = 0; i < 6; i++)
            {
                var key = churn.Next(keys);
                if (churn.Next(4) == 0)
                    await table.Writer.DeleteAsync(key);
                else
                    await table.Writer.UpsertAsync(new Instrument(key, $"v{v}"));
            }
            await table.Writer.CommitAsync().AsTask().WaitAsync(Timeout);
        }
        stop.Cancel();

        using var final = table.Snapshot();
        foreach (var run in circuits)
        {
            var live = await run.Feeder.WaitAsync(Timeout);
            var expected = new ZSet<(int Trade, string Symbol)>();
            foreach (var trade in live)
            {
                if (final.TryGet(trade.InstrumentId, out var instrument))
                    expected.Add((trade.Id, instrument.Symbol), 1);
                else if (run.Left)
                    expected.Add((trade.Id, None), 1);
            }

            bool Converged()
            {
                lock (run.Integrated)
                    return expected.DistinctCount == run.Integrated.DistinctCount
                        && expected.All(e => run.Integrated.WeightOf(e.Value) == e.Weight);
            }

            var deadline = DateTime.UtcNow + Timeout;
            while (!Converged() && DateTime.UtcNow < deadline)
                await Task.Delay(10);
            Assert.True(live.Count > keys, "the feeder barely ran");
            Assert.True(Converged(), $"a circuit (left={run.Left}) did not converge on the join with the final table");
        }

        foreach (var run in circuits)
        {
            await run.Circuit.DisposeAsync();
            await run.Consumer.WaitAsync(Timeout);
        }
    }

    // ── routing, eviction, composition ─────────────────────────────────────────────────

    [Fact]
    public async Task Table_changes_follow_a_grouping_routed_by_a_caller_supplied_hash()
    {
        const int keys = 40;
        var table = NewTable();
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Workers = 4 });
        var instruments = builder.Table(table);
        var tradesDef = builder.Input<Trade>("trades");
        // Nothing like the built-in hash: changes routed by that one would mostly miss their facts.
        var enrichedDef = tradesDef
            .GroupBy(t => t.InstrumentId, id => (uint)(id / 5))
            .Join(instruments, (_, trade, instrument) => (trade.Id, instrument.Symbol))
            .ToOutput();
        await using var circuit = builder.Build();
        circuit.Start();

        for (var key = 0; key < keys; key++)
            await circuit.Handle(tradesDef).InsertAsync(new Trade(key, key));
        await circuit.StepAsync();
        AssertDelta(Drain(circuit.Handle(enrichedDef)));

        await Commit(table, Enumerable.Range(0, keys).Select(key => new Instrument(key, "S" + key)).ToArray());
        await circuit.StepAsync();
        AssertDelta(Drain(circuit.Handle(enrichedDef)), Enumerable.Range(0, keys).Select(key => ((key, "S" + key), 1L)).ToArray());
    }

    [Fact]
    public async Task An_aggregate_regrouped_on_its_key_joins_a_table_without_a_reshuffle()
    {
        const int keys = 40;
        var table = NewTable();
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Workers = 4 });
        var instruments = builder.Table(table);
        var tradesDef = builder.Input<Trade>("trades");
        var countsDef = tradesDef
            .GroupBy(t => t.InstrumentId)
            .Count()
            .Grouped()
            .Join(instruments, (_, count, instrument) => (instrument.Symbol, count))
            .ToOutput();
        await using var circuit = builder.Build();
        circuit.Start();

        for (var key = 0; key < keys; key++)
            await circuit.Handle(tradesDef).InsertAsync(new Trade(key, key));
        await circuit.StepAsync();
        await Commit(table, Enumerable.Range(0, keys).Select(key => new Instrument(key, "S" + key)).ToArray());
        await circuit.StepAsync();
        AssertDelta(Drain(circuit.Handle(countsDef)), Enumerable.Range(0, keys).Select(key => (("S" + key, 1L), 1L)).ToArray());
    }

    [Fact]
    public async Task Completing_every_input_ends_an_auto_stepping_circuit_though_its_table_never_completes()
    {
        var table = NewTable(new SharedTableOptions { TailVersions = 2 });
        await Commit(table, new Instrument(1, "ONE"));
        var pipeline = Build(table, autoStep: true);
        await using var circuit = pipeline.Builder.Build();
        circuit.Start();
        var trades = circuit.Handle(pipeline.Trades);
        var enriched = circuit.Handle(pipeline.Enriched);

        await trades.InsertAsync(new Trade(1, 1));
        trades.Complete();

        // The usual shutdown idiom: read the output to its end, then dispose. A table has no
        // end, so it must not be what the circuit waits for.
        var all = new ZSet<(int Trade, string Symbol)>();
        var consumer = Task.Run(async () =>
        {
            await foreach (var delta in enriched.Reader.ReadAllAsync())
                all.AddAll(delta);
        });
        await consumer.WaitAsync(Timeout);
        AssertDelta(all, ((1, "ONE"), 1));

        // Nor does the finished circuit go on holding the table's history back.
        for (var v = 0; v < 6; v++)
            await Commit(table, new Instrument(1, "ONE-" + v));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    public async Task Facts_of_a_closed_key_are_evicted_and_their_results_stay_final(int workers)
    {
        var table = NewTable();
        await Commit(table, new Instrument(1, "ONE"), new Instrument(2, "TWO"));
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Workers = workers });
        var instruments = builder.Table(table);
        var tradesDef = builder.Input<(int Id, int InstrumentId, DateTimeOffset At)>("trades");
        var enrichedDef = tradesDef
            .WithEventTime(t => t.At, lateness: TimeSpan.Zero)
            .GroupBy(t => t.InstrumentId)
            .EvictWhen(id => id == 1 ? T0.AddSeconds(5) : T0.AddHours(1))
            .Join(instruments, (_, trade, instrument) => (trade.Id, instrument.Symbol))
            .ToOutput();
        await using var circuit = builder.Build();
        circuit.Start();
        var trades = circuit.Handle(tradesDef);
        var enriched = circuit.Handle(enrichedDef);

        await trades.InsertAsync((1, 1, T0.AddSeconds(1)));
        await trades.InsertAsync((2, 2, T0.AddSeconds(1)));
        await circuit.StepAsync();
        AssertDelta(Drain(enriched), ((1, "ONE"), 1), ((2, "TWO"), 1));

        // The watermark passes key 1's deadline; the step after sees it and drops key 1's facts.
        await trades.InsertAsync((3, 2, T0.AddSeconds(7)));
        await circuit.StepAsync();
        await circuit.StepAsync();
        AssertDelta(Drain(enriched), ((3, "TWO"), 1));

        // Both rows change: the open key is corrected, the closed one has nothing left to correct.
        await Commit(table, new Instrument(1, "ONE'"), new Instrument(2, "TWO'"));
        await circuit.StepAsync();
        AssertDelta(Drain(enriched), ((2, "TWO"), -1), ((2, "TWO'"), 1), ((3, "TWO"), -1), ((3, "TWO'"), 1));
    }

    [Fact]
    public void A_table_join_is_rejected_inside_a_recursion_and_across_builders()
    {
        var table = NewTable();
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        var instruments = builder.Table(table);
        var keys = builder.Input<int>("keys");

        Assert.Throws<NotSupportedException>(() => keys.Recurse(loop => loop
            .GroupBy(key => key)
            .Join(instruments, (key, _, _) => key + 1)));
        Assert.Throws<NotSupportedException>(() => keys.Recurse(loop => loop
            .GroupBy(key => key)
            .LeftJoin(instruments, (key, _, _) => key + 1, (key, _) => key)));

        var other = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        Assert.Throws<ArgumentException>(() => other.Input<int>("keys")
            .GroupBy(key => key)
            .Join(instruments, (key, _, _) => key));
    }
}
