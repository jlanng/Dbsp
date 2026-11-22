using Dbsp.Storage;
using Dbsp.Tables;

namespace Dbsp.Tests;

/// <summary>
/// <c>EvictWhenFinal</c>: a retroactive join, a left join, an index join and a remembered
/// lookup against a table over a stream of facts whose key never closes, each forgetting a
/// fact once the watermark has passed its event time — and a keyed input that forgets its
/// final rows. What is checked: a table change corrects exactly the open facts, whether or
/// not the final ones have been evicted yet; amendments of an open fact retract, of a final
/// one are dropped as late; the state returns to the facts still open, across a restart
/// from a checkpoint too, and with several workers.
/// </summary>
public class EvictWhenFinalTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(20);
    private static readonly DateTimeOffset T0 = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
    private static readonly TimeSpan Lateness = TimeSpan.FromSeconds(10);

    private sealed record Instrument(int Id, string Symbol);

    private sealed record Trade(int Id, int InstrumentId, DateTimeOffset At);

    private static DateTimeOffset At(int seconds) => T0.AddSeconds(seconds);

    private static SharedTable<int, Instrument> NewTable() => new("instruments", i => i.Id);

    private static async Task Commit(SharedTable<int, Instrument> table, params Instrument[] rows)
    {
        foreach (var row in rows)
            await table.Writer.UpsertAsync(row);
        await table.Writer.CommitAsync().AsTask().WaitAsync(Timeout);
    }

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

    /// <summary>Entries the committed state holds in stores whose name contains
    /// <paramref name="part"/>, every store when it is null.</summary>
    private static long Entries(InMemoryCheckpointStorageProvider storage, string? part = null) =>
        storage.CommittedEntriesByStore().Where(s => part is null || s.Key.Contains(part, StringComparison.Ordinal)).Sum(s => s.Value);

    private sealed class Enrichment : IAsyncDisposable
    {
        public required Circuit Circuit { get; init; }
        public required KeyedInputHandle<int, Trade> Trades { get; init; }
        public required OutputHandle<(int Id, string Symbol)> Enriched { get; init; }

        public async Task Upsert(params Trade[] trades)
        {
            foreach (var trade in trades)
                await Trades.UpsertAsync(trade);
            await Circuit.StepAsync();
        }

        public ValueTask DisposeAsync() => Circuit.DisposeAsync();
    }

    /// <summary>The reference shape: a keyed input of trades, forgetting final rows, joined
    /// retroactively to instruments, forgetting final facts.</summary>
    private static Enrichment Start(SharedTable<int, Instrument> table, InMemoryCheckpointStorageProvider storage, int workers = 1)
    {
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Workers = workers, Storage = storage });
        var instruments = builder.Table(table);
        var trades = builder.KeyedInput<int, Trade>("trades", t => t.Id);
        var enriched = trades
            .WithEventTime(t => t.At, Lateness, evictWhenFinal: true)
            .GroupBy(t => t.InstrumentId)
            .EvictWhenFinal()
            .Join(instruments, (_, t, i) => (t.Id, i.Symbol)).Named("priced")
            .ToOutput();
        var circuit = builder.Build();
        circuit.Start();
        return new Enrichment { Circuit = circuit, Trades = circuit.Handle(trades), Enriched = circuit.Handle(enriched) };
    }

    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    public async Task A_table_change_corrects_the_open_facts_and_leaves_the_final_ones_as_they_were(int workers)
    {
        var table = NewTable();
        await Commit(table, new Instrument(1, "ONE"), new Instrument(2, "TWO"));
        var storage = new InMemoryCheckpointStorageProvider();
        await using var p = Start(table, storage, workers);

        await p.Upsert(new Trade(1, 1, At(1)), new Trade(2, 2, At(2)));
        AssertDelta(Drain(p.Enriched), ((1, "ONE"), 1), ((2, "TWO"), 1));

        // The watermark moves to 15 − 10 = 5: trades 1 and 2 are final from the next step.
        await p.Upsert(new Trade(3, 2, At(15)));
        AssertDelta(Drain(p.Enriched), ((3, "TWO"), 1));
        Assert.Equal(At(5), p.Circuit.CurrentWatermark);

        // Both rows change in the very step that first sees the new watermark — before the
        // join has evicted anything: still, only the open trade is corrected.
        await Commit(table, new Instrument(1, "ONE'"), new Instrument(2, "TWO'"));
        await p.Circuit.StepAsync();
        AssertDelta(Drain(p.Enriched), ((3, "TWO"), -1), ((3, "TWO'"), 1));

        // What is left: trade 3's row and deadline in the input, its fact and deadline in the join.
        Assert.Equal(1, Entries(storage, "input.trades.upsert.rows"));
        Assert.Equal(1, Entries(storage, "input.trades.upsert.deadlines"));
        Assert.Equal(1, Entries(storage, "named.priced.tableJoin.facts"));
        Assert.Equal(1, Entries(storage, "named.priced.tableJoin.deadlines"));

        // An open trade amended: retracted and asserted as ever.
        await p.Upsert(new Trade(3, 1, At(15)));
        AssertDelta(Drain(p.Enriched), ((3, "TWO'"), -1), ((3, "ONE'"), 1));

        // A final trade amended at its own time: behind the watermark, dropped.
        await p.Upsert(new Trade(1, 2, At(1)));
        AssertDelta(Drain(p.Enriched));

        // A forgotten id with a later time: a new trade, and nothing retracted for the old one.
        await p.Upsert(new Trade(2, 1, At(16)));
        AssertDelta(Drain(p.Enriched), ((2, "ONE'"), 1));

        // Far ahead: everything before is final and gone after the next step.
        await p.Upsert(new Trade(9, 1, At(100)));
        await p.Circuit.StepAsync();
        AssertDelta(Drain(p.Enriched), ((9, "ONE'"), 1));
        Assert.Equal(4, Entries(storage));
    }

    [Fact]
    public async Task The_state_of_an_endless_feed_returns_to_what_the_lateness_holds()
    {
        var table = NewTable();
        await Commit(table, Enumerable.Range(1, 10).Select(i => new Instrument(i, $"S{i}")).ToArray());
        var storage = new InMemoryCheckpointStorageProvider();
        await using var p = Start(table, storage, workers: 2);

        // One trade a second of event time, each with an id of its own, ten a step. The
        // lateness holds ten seconds: about ten trades open, each with a row, a deadline, a
        // fact and a deadline, plus the step's own until the next one evicts.
        var peaks = new List<long>();
        var id = 0;
        for (var step = 0; step < 50; step++)
        {
            for (var i = 0; i < 10; i++, id++)
                await p.Trades.UpsertAsync(new Trade(id, 1 + id % 10, At(id)));
            await p.Circuit.StepAsync();
            peaks.Add(Entries(storage));
        }
        Assert.All(peaks.Skip(5), entries => Assert.InRange(entries, 1, 4 * 21));
        Assert.Equal(500, Drain(p.Enriched).Count());
    }

    [Fact]
    public async Task A_restart_from_the_checkpoint_still_forgets_the_facts_that_become_final()
    {
        var table = NewTable();
        await Commit(table, new Instrument(1, "ONE"), new Instrument(2, "TWO"));
        var storage = new InMemoryCheckpointStorageProvider();
        await using (var first = Start(table, storage))
        {
            await first.Upsert(new Trade(1, 1, At(1)), new Trade(2, 2, At(2)), new Trade(3, 2, At(8)));
            // Nothing final yet (watermark 8 − 10): a row, a deadline, a fact and a deadline each.
            Assert.Equal(12, Entries(storage));
        }

        // Restored: the deadlines came back with the facts, so nothing touched again is
        // still evicted when the watermark passes it.
        await using var second = Start(table, storage);
        await second.Upsert(new Trade(9, 1, At(30)));
        await second.Circuit.StepAsync();
        AssertDelta(Drain(second.Enriched), ((9, "ONE"), 1));
        Assert.Equal(4, Entries(storage));

        await Commit(table, new Instrument(2, "TWO'"));
        await second.Circuit.StepAsync();
        AssertDelta(Drain(second.Enriched));
    }

    [Fact]
    public async Task A_left_join_switches_only_the_open_facts_when_the_row_appears()
    {
        var table = NewTable();
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Storage = new InMemoryCheckpointStorageProvider() });
        var instruments = builder.Table(table);
        var tradesDef = builder.Input<Trade>("trades");
        var enrichedDef = tradesDef
            .WithEventTime(t => t.At, Lateness)
            .GroupBy(t => t.InstrumentId)
            .EvictWhenFinal()
            .LeftJoin(instruments, (_, t, i) => (t.Id, i.Symbol), (_, t) => (t.Id, "?"))
            .ToOutput();
        await using var circuit = builder.Build();
        circuit.Start();
        var trades = circuit.Handle(tradesDef);
        var enriched = circuit.Handle(enrichedDef);

        await trades.InsertAsync(new Trade(1, 7, At(1)));
        await trades.InsertAsync(new Trade(2, 7, At(20)));
        await circuit.StepAsync();
        AssertDelta(Drain(enriched), ((1, "?"), 1), ((2, "?"), 1));
        await trades.InsertAsync(new Trade(3, 8, At(21)));
        await circuit.StepAsync();
        Drain(enriched);

        // Watermark 11: trade 1 is final and stays unmatched; trade 2 switches.
        await Commit(table, new Instrument(7, "SEVEN"));
        await circuit.StepAsync();
        AssertDelta(Drain(enriched), ((2, "?"), -1), ((2, "SEVEN"), 1));
    }

    [Fact]
    public async Task An_index_join_corrects_only_the_open_facts_and_forgets_the_final_ones()
    {
        var table = NewTable();
        await Commit(table, new Instrument(1, "A"), new Instrument(2, "B"));
        var storage = new InMemoryCheckpointStorageProvider();
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Storage = storage });
        var bySymbol = builder.Table(table).By("symbol", i => i.Symbol);
        var tradesDef = builder.Input<(int Id, string Symbol, DateTimeOffset At)>("trades");
        var enrichedDef = tradesDef
            .WithEventTime(t => t.At, Lateness)
            .GroupBy(t => t.Symbol)
            .EvictWhenFinal()
            .Join(bySymbol, (_, t, i) => (t.Id, i.Id))
            .ToOutput();
        await using var circuit = builder.Build();
        circuit.Start();
        var trades = circuit.Handle(tradesDef);
        var enriched = circuit.Handle(enrichedDef);

        await trades.InsertAsync((1, "A", At(1)));
        await trades.InsertAsync((2, "A", At(30)));
        await circuit.StepAsync();
        AssertDelta(Drain(enriched), ((1, 1), 1), ((2, 1), 1));

        // Watermark 20: a second row under "A" reaches trade 2 only.
        await Commit(table, new Instrument(3, "A"));
        await circuit.StepAsync();
        AssertDelta(Drain(enriched), ((2, 3), 1));
        Assert.Equal(1, Entries(storage, "tableIndexJoin.facts"));
    }

    [Fact]
    public async Task A_remembered_lookup_forgets_what_it_emitted_for_a_final_fact()
    {
        var table = NewTable();
        await Commit(table, new Instrument(1, "ONE"));
        var storage = new InMemoryCheckpointStorageProvider();
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Storage = storage });
        var instruments = builder.Table(table);
        var tradesDef = builder.KeyedInput<int, Trade>("trades", t => t.Id);
        var enrichedDef = tradesDef
            .WithEventTime(t => t.At, Lateness, evictWhenFinal: true)
            .GroupBy(t => t.InstrumentId)
            .EvictWhenFinal()
            .Lookup(instruments, (_, t, i) => (t.Id, i.Symbol), (_, t) => (t.Id, "?")).Named("booked")
            .ToOutput();
        await using var circuit = builder.Build();
        circuit.Start();
        var trades = circuit.Handle(tradesDef);
        var enriched = circuit.Handle(enrichedDef);

        for (var i = 0; i < 40; i++)
        {
            await trades.UpsertAsync(new Trade(i, 1, At(i)));
            await circuit.StepAsync();
        }
        Assert.Equal(40, Drain(enriched).Count());
        // Eleven open (watermark 29, times 29 to 39) less the one evicted at the end of the
        // step that froze it: a remembered row and a deadline each in the lookup, a row and
        // a deadline each in the input.
        Assert.InRange(Entries(storage, "named.booked"), 2 * 10, 2 * 12);
        Assert.InRange(Entries(storage, "input.trades"), 2 * 10, 2 * 12);

        // An open trade amended is retracted from what was remembered for it.
        await trades.UpsertAsync(new Trade(39, 1, At(39)) with { InstrumentId = 2 });
        await circuit.StepAsync();
        AssertDelta(Drain(enriched), ((39, "ONE"), -1), ((39, "?"), 1));
    }

    [Fact]
    public void A_keyed_input_that_forgets_final_rows_must_be_read_only_through_its_event_time_stream()
    {
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        var trades = builder.KeyedInput<int, Trade>("trades", t => t.Id);
        var timed = trades.WithEventTime(t => t.At, Lateness, evictWhenFinal: true);
        timed.ToOutput();
        trades.Select(t => t.Id).ToOutput();
        var refused = Assert.Throws<InvalidOperationException>(builder.Build);
        Assert.Contains("'trades'", refused.Message);

        var twice = new CircuitBuilder(new CircuitOptions { AutoStep = false }).KeyedInput<int, Trade>("trades", t => t.Id);
        twice.WithEventTime(t => t.At, Lateness, evictWhenFinal: true);
        Assert.Throws<InvalidOperationException>(() => twice.WithEventTime(t => t.At, Lateness, evictWhenFinal: true));
    }

    /// <summary>A deadline is set when a row or a fact is written, so what a checkpoint
    /// holds from before the definition forgot anything has none and would never go.
    /// Adopting the eviction on such a checkpoint is refused — beforehand by
    /// <see cref="CircuitBuilder.CheckState"/> and at <c>Start()</c> — and a circuit that
    /// only gains state it had none of (a new operator) is not.</summary>
    [Theory]
    [InlineData(true, true)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    public async Task Adopting_the_eviction_on_a_checkpoint_that_holds_state_without_deadlines_is_refused(bool input, bool join)
    {
        var table = NewTable();
        await Commit(table, new Instrument(1, "ONE"));
        var storage = new InMemoryCheckpointStorageProvider();

        (CircuitBuilder Builder, KeyedInputStream<int, Trade> Trades) Define(bool forgetRows, bool forgetFacts, bool extra = false)
        {
            var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Storage = storage });
            var instruments = builder.Table(table);
            var trades = builder.KeyedInput<int, Trade>("trades", t => t.Id);
            var grouped = (forgetRows ? trades.WithEventTime(t => t.At, Lateness, evictWhenFinal: true) : trades.WithEventTime(t => t.At, Lateness))
                .GroupBy(t => t.InstrumentId);
            var priced = forgetFacts
                ? grouped.EvictWhenFinal().Join(instruments, (_, t, i) => (t.Id, i.Symbol)).Named("priced")
                : grouped.Join(instruments, (_, t, i) => (t.Id, i.Symbol)).Named("priced");
            priced.ToOutput();
            if (extra)
                priced.WithEventTime(p => T0).GroupBy(p => p.Id).EvictWhenFinal()
                    .Lookup(instruments, (_, p, i) => p.Symbol + i.Symbol).Named("again").ToOutput();
            return (builder, trades);
        }

        // Version one keeps every row and every fact.
        var (v1, v1Trades) = Define(false, false);
        Assert.Empty(v1.CheckState(storage));
        await using (var circuit = v1.Build())
        {
            circuit.Start();
            for (var i = 0; i < 20; i++)
                await circuit.Handle(v1Trades).UpsertAsync(new Trade(i, 1, At(i)));
            await circuit.StepAsync();
        }
        Assert.Equal(20, Entries(storage, "input.trades.upsert.rows"));
        Assert.Equal(20, Entries(storage, "named.priced.tableJoin.facts"));

        // Version two forgets, on the state version one left.
        var (v2, _) = Define(input, join);
        var problems = v2.CheckState(storage);
        Assert.Equal((input ? 1 : 0) + (join ? 1 : 0), problems.Count);
        if (input)
            Assert.Contains(problems, p => p.StartsWith("'input.trades.upsert' now forgets", StringComparison.Ordinal));
        if (join)
            Assert.Contains(problems, p => p.StartsWith("'named.priced.tableJoin' now forgets", StringComparison.Ordinal));
        Assert.All(problems, p => Assert.Contains("no deadlines", p));
        var refused = Assert.Throws<CircuitStateMismatchException>(() => v2.Build().Start());
        Assert.Equal(problems, refused.Problems);

        // The same definition as version one, with an operator it did not have: new state,
        // deadlines included, beside state that stays as it was.
        var (v1b, _) = Define(false, false, extra: true);
        Assert.Empty(v1b.CheckState(storage));

        // And on an empty store the forgetting definition starts, and goes on from its own checkpoint.
        storage = new InMemoryCheckpointStorageProvider();
        var (fresh, freshTrades) = Define(input, join);
        Assert.Empty(fresh.CheckState(storage));
        await using (var circuit = fresh.Build())
        {
            circuit.Start();
            await circuit.Handle(freshTrades).UpsertAsync(new Trade(1, 1, At(1)));
            await circuit.StepAsync();
        }
        var (again, _) = Define(input, join);
        Assert.Empty(again.CheckState(storage));
    }

    private sealed record Stamp(DateTimeOffset At);

    private sealed record Stamped(int Id, Stamp? Stamp);

    /// <summary>The input's own state runs the event-time selector too (a row's deadline is
    /// its event time), upstream of the declaration that dead-letters a record whose time
    /// cannot be read. Under the dead-letter policy it must not be what faults: the row is
    /// handed on to be dead-lettered and nothing is held for it.</summary>
    [Fact]
    public async Task A_row_whose_event_time_cannot_be_read_is_dead_lettered_and_not_held_by_an_input_that_forgets_final_rows()
    {
        var storage = new InMemoryCheckpointStorageProvider();
        var builder = new CircuitBuilder(new CircuitOptions
        {
            AutoStep = false, Storage = storage, ErrorPolicy = RecordErrorPolicy.DeadLetter,
        });
        var inputDef = builder.KeyedInput<int, Stamped>("stamped", s => s.Id);
        var outDef = inputDef.WithEventTime(s => s.Stamp!.At, Lateness, evictWhenFinal: true).Select(s => s.Id).ToOutput();
        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(inputDef);
        var output = circuit.Handle(outDef);

        List<DeadLetter> DeadLetters()
        {
            var letters = new List<DeadLetter>();
            while (circuit.DeadLetters.TryRead(out var letter))
                letters.Add(letter);
            return letters;
        }

        await input.UpsertAsync(new Stamped(1, new Stamp(At(1))));
        await input.UpsertAsync(new Stamped(2, null));
        await circuit.StepAsync();
        AssertDelta(Drain(output), (1, 1));
        var letter = Assert.Single(DeadLetters());
        Assert.Equal(new Stamped(2, null), letter.Record);
        Assert.Equal(1, letter.Weight);
        Assert.IsType<NullReferenceException>(letter.Exception);
        // Only the row that has a time is held, with its deadline.
        Assert.Equal(1, Entries(storage, "input.stamped.upsert.rows"));
        Assert.Equal(1, Entries(storage, "input.stamped.upsert.deadlines"));

        // The key again, readable now: a new row, with nothing owed for the one never held.
        await input.UpsertAsync(new Stamped(2, new Stamp(At(2))));
        await circuit.StepAsync();
        AssertDelta(Drain(output), (2, 1));
        Assert.Empty(DeadLetters());
        Assert.Equal(2, Entries(storage, "input.stamped.upsert.rows"));

        // A held row replaced by one with no time: the held row is taken back and
        // forgotten, the new one dead-lettered, and the key holds nothing.
        await input.UpsertAsync(new Stamped(1, null));
        await circuit.StepAsync();
        AssertDelta(Drain(output), (1, -1));
        letter = Assert.Single(DeadLetters());
        Assert.Equal(new Stamped(1, null), letter.Record);
        Assert.Equal(1, Entries(storage, "input.stamped.upsert.rows"));
        Assert.Equal(1, Entries(storage, "input.stamped.upsert.deadlines"));

        // Under the default policy the same row faults the step, as any delegate's exception does.
        var faulting = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        var faultingDef = faulting.KeyedInput<int, Stamped>("stamped", s => s.Id);
        faultingDef.WithEventTime(s => s.Stamp!.At, Lateness, evictWhenFinal: true).ToOutput();
        var strict = faulting.Build();
        strict.Start();
        await strict.Handle(faultingDef).UpsertAsync(new Stamped(2, null));
        await Assert.ThrowsAnyAsync<Exception>(() => strict.StepAsync());
        try { await strict.DisposeAsync(); } catch { /* it faulted, as asserted */ }
    }

    /// <summary>A record is late by its time alone: one the circuit has never seen is
    /// dropped like an amendment of one that is final, and one record stamped far ahead
    /// makes every record behind it late. Nothing reports the drop but the count.</summary>
    [Theory]
    [InlineData(RecordErrorPolicy.Fault, 1)]
    [InlineData(RecordErrorPolicy.DeadLetter, 1)]
    [InlineData(RecordErrorPolicy.Fault, 3)]
    public async Task A_record_first_seen_behind_the_watermark_is_dropped_and_counted_as_late(RecordErrorPolicy policy, int workers)
    {
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, ErrorPolicy = policy, Workers = workers });
        var tradesDef = builder.Input<Trade>("trades");
        var outDef = tradesDef.WithEventTime(t => t.At, Lateness)
            .Select(t => t with { At = t.At.AddSeconds(-5) })
            .WithEventTime(t => t.At)                               // a re-declaration drops, and counts, too
            .Select(t => t.Id).ToOutput();
        await using var circuit = builder.Build();
        circuit.Start();
        var trades = circuit.Handle(tradesDef);
        var output = circuit.Handle(outDef);
        Assert.Equal(0, circuit.LateRecords);

        await trades.InsertAsync(new Trade(1, 1, At(1)));
        await circuit.StepAsync();
        // One trade stamped far ahead: the watermark is at 90 from the next step on.
        await trades.InsertAsync(new Trade(2, 1, At(100)));
        await circuit.StepAsync();
        AssertDelta(Drain(output), (1, 1), (2, 1));
        Assert.Equal(0, circuit.LateRecords);

        // Three trades never seen before: two behind the watermark at the declaration, one
        // that passes it and is behind once its time has been reshaped; and one that is not late.
        await trades.InsertAsync(new Trade(3, 1, At(50)));
        await trades.InsertAsync(new Trade(4, 2, At(60)));
        await trades.InsertAsync(new Trade(5, 3, At(92)));
        await trades.InsertAsync(new Trade(6, 1, At(96)));
        await circuit.StepAsync();
        AssertDelta(Drain(output), (6, 1));
        Assert.Equal(3, circuit.LateRecords);

        // The retraction of a record that is final is dropped and is not another late record.
        await trades.RemoveAsync(new Trade(1, 1, At(1)));
        await circuit.StepAsync();
        Assert.True(Drain(output).IsEmpty);
        Assert.Equal(3, circuit.LateRecords);
    }

    [Fact]
    public async Task Event_time_declared_again_after_a_filter_does_not_hold_the_watermark_back()
    {
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        Assert.Throws<InvalidOperationException>(() => builder.Input<Trade>("early").WithEventTime(t => t.At));

        var tradesDef = builder.Input<Trade>("trades");
        var outDef = tradesDef
            .WithEventTime(t => t.At, Lateness)
            .Where(t => t.InstrumentId == 1)
            .Select(t => t with { At = t.At.AddSeconds(-10) })     // a time of its own, ten seconds earlier
            .WithEventTime(t => t.At)
            .ToOutput();
        await using var circuit = builder.Build();
        circuit.Start();
        var trades = circuit.Handle(tradesDef);
        var output = circuit.Handle(outDef);

        await trades.InsertAsync(new Trade(1, 1, At(1)));
        await trades.InsertAsync(new Trade(2, 2, At(50)));
        await circuit.StepAsync();
        // Only the input's declaration feeds the clock: 50 − 10, not 1 − 10.
        Assert.Equal(At(40), circuit.CurrentWatermark);

        // And the re-declaration drops what is behind the watermark by its own time: 45
        // passes the input's declaration, but 35 is behind 40.
        await trades.InsertAsync(new Trade(3, 1, At(45)));
        await trades.InsertAsync(new Trade(4, 1, At(55)));
        await circuit.StepAsync();
        AssertDelta(Drain(output), (new Trade(1, 1, At(-9)), 1), (new Trade(4, 1, At(45)), 1));
    }

    [Fact]
    public async Task A_fact_at_the_watermark_itself_is_still_open_and_can_be_retracted()
    {
        var table = NewTable();
        await Commit(table, new Instrument(1, "ONE"));
        var storage = new InMemoryCheckpointStorageProvider();
        await using var p = Start(table, storage);

        await p.Upsert(new Trade(1, 1, At(5)));
        await p.Upsert(new Trade(2, 1, At(15)));                 // watermark 5: trade 1 is exactly at it
        await p.Circuit.StepAsync();                              // a step that would evict it, were it final
        Drain(p.Enriched);

        // Still open: its amendment is not late, and its old result is taken back.
        await Commit(table, new Instrument(1, "ONE'"));
        await p.Circuit.StepAsync();
        AssertDelta(Drain(p.Enriched), ((1, "ONE"), -1), ((1, "ONE'"), 1), ((2, "ONE"), -1), ((2, "ONE'"), 1));
        await p.Trades.DeleteAsync(1);
        await p.Circuit.StepAsync();
        AssertDelta(Drain(p.Enriched), ((1, "ONE'"), -1));
        Assert.Equal(1, Entries(storage, "named.priced.tableJoin.facts"));
    }

    private sealed record Row(int Id, string Text);

    [Fact]
    public async Task Removing_entries_drops_exactly_those_in_every_provider_and_durably()
    {
        static void Fill(IIndexedWeightStore<int, Row> store)
        {
            store.Update(1, new Row(1, "a"), 2);
            store.Update(1, new Row(2, "b"), 1);
            store.Update(2, new Row(3, "c"), 1);
        }
        static void Remove(IIndexedWeightStore<int, Row> store) =>
            store.RemoveEntries([new(1, new Row(1, "a")), new(2, new Row(3, "c")), new(9, new Row(9, "absent"))]);
        static void AssertLeft(IIndexedWeightStore<int, Row> store)
        {
            Assert.Equal([new WeightedValue<Row>(new Row(2, "b"), 1)], store.GetGroup(1));
            Assert.Empty(store.GetGroup(2));
            Assert.Empty(store.GetGroup(9));
        }

        var plain = new InMemoryStorageProvider().CreateIndexedWeightStore<int, Row>("s");
        Fill(plain);
        Remove(plain);
        AssertLeft(plain);

        // Staged over committed: the removal of a committed entry commits, and rolls back.
        var checkpoint = new InMemoryCheckpointStorageProvider();
        var staged = checkpoint.CreateIndexedWeightStore<int, Row>("s");
        Fill(staged);
        checkpoint.Commit(new Dictionary<string, long>());
        Remove(staged);
        AssertLeft(staged);
        checkpoint.Recover();
        Assert.Equal(2, staged.GetGroup(1).Count);
        staged.Update(1, new Row(1, "a"), 1);                    // staged weight 3 over the committed 2: all of it goes
        Remove(staged);
        AssertLeft(staged);
        checkpoint.Commit(new Dictionary<string, long>());
        Assert.Equal(1, checkpoint.CommittedEntriesByStore()["s"]);

        // The changelog: a tombstone per removed entry, so a copy read from the log agrees.
        var log = new InMemoryStateChangelog(partitions: 1);
        await using var storage = await ChangelogStorage.OpenAsync(log);
        var provider = storage.Pipeline("p");
        var logged = provider.CreateIndexedWeightStore<int, Row>("s");
        provider.Recover();
        Fill(logged);
        await provider.CommitAsync(new Dictionary<string, long> { ["in"] = 1 });
        Remove(logged);
        await provider.CommitAsync(new Dictionary<string, long> { ["in"] = 2 });
        AssertLeft(logged);
        await using var copy = ChangelogStorage.Follow(log);
        await copy.WhenCaughtUpAsync().WaitAsync(Timeout);
        AssertLeft(copy.Pipeline("p").CreateIndexedWeightStore<int, Row>("s"));
    }
}
