using CsCheck;
using Dbsp.Storage;
using Dbsp.Tables;

namespace Dbsp.Tests;

/// <summary>
/// Enrich-on-arrival against a shared table (<c>docs/design/shared-tables.md</c>,
/// <em>Lookup</em>): a record is enriched from the table once and never corrected. The
/// remembered form keeps what it emitted so that a retraction takes back exactly that; the
/// append-only form keeps nothing and refuses retractions. The differential property pins
/// the remembered form to a direct model of its invariant; the directed tests cover the
/// behaviours a join does not have.
/// </summary>
public class TableLookupTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(20);
    private static readonly DateTimeOffset T0 = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
    private const string None = "(none)";

    private sealed record Instrument(int Id, string Symbol);

    private sealed record Trade(int Id, int InstrumentId);

    private static SharedTable<int, Instrument> NewTable() => new("instruments", i => i.Id);

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

    /// <summary>Trades enriched on arrival with their instrument's symbol, as (trade id, symbol).</summary>
    private sealed record Pipeline(
        CircuitBuilder Builder,
        InputStream<Trade> Trades,
        Output<(int Trade, string Symbol)> Enriched);

    private static Pipeline Build(
        SharedTable<int, Instrument> table, bool missing = false, int workers = 1, IStorageProvider? storage = null)
    {
        var builder = new CircuitBuilder(new CircuitOptions
        {
            AutoStep = false,
            Workers = workers,
            Storage = storage ?? new InMemoryStorageProvider(),
        });
        var instruments = builder.Table(table);
        var trades = builder.Input<Trade>("trades");
        var enriched = trades.Lookup(
            instruments,
            t => t.InstrumentId,
            (trade, instrument) => (trade.Id, instrument.Symbol),
            missing ? trade => (trade.Id, None) : null);
        return new Pipeline(builder, trades, enriched.Named("enrich").ToOutput());
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
        Assert.True(Same(want, actual), $"expected {want} but got {actual}");
    }

    private static bool Same<T>(ZSet<T> a, ZSet<T> b) where T : notnull =>
        a.DistinctCount == b.DistinctCount && a.All(e => b.WeightOf(e.Value) == e.Weight);

    // ── differential: the remembered lookup against a model of its invariant ────────────

    private sealed record Row(int Key, int Group, int Val);

    private enum Kind { InsertFact, RetractFact, UpsertRow, DeleteRow }

    private static readonly Gen<List<(int Kind, int Key, int Alt, bool NewStep)>> GenCommands =
        Gen.Select(Gen.Int[0, 3], Gen.Int[0, 3], Gen.Int[0, 2], Gen.Bool).List[1, 60];

    /// <summary>What the operator promises, written down directly: per fact, a weight and
    /// the enriched rows it is remembered as. A step's net change to a fact re-enriches it
    /// from the table as of that step when it is an assertion, or when nothing is held for
    /// the fact; otherwise it only scales what is remembered. The output must integrate to
    /// the sum, over facts, of weight × remembered rows.</summary>
    private sealed class Model<TOut>(Func<(int Key, int Val), IEnumerable<TOut>> enrich) where TOut : notnull
    {
        private readonly Dictionary<(int Key, int Val), (long Weight, List<TOut> Rows)> _held = [];

        public void Apply(ZSet<(int Key, int Val)> delta)
        {
            foreach (var (fact, change) in delta)
            {
                var held = _held.TryGetValue(fact, out var entry);
                var rows = change > 0 || !held ? enrich(fact).ToList() : entry.Rows;
                var weight = (held ? entry.Weight : 0) + change;
                if (weight == 0)
                    _held.Remove(fact);
                else
                    _held[fact] = (weight, rows);
            }
        }

        public ZSet<TOut> Integrated()
        {
            var total = new ZSet<TOut>();
            foreach (var (weight, rows) in _held.Values)
                foreach (var row in rows)
                    total.Add(row, weight);
            return total;
        }
    }

    /// <summary>Runs one circuit with three lookups over the same facts — on the table's
    /// key dropping misses, on its key with a row for misses, and through a non-unique
    /// index — and compares each integrated output with its model after every step. Fact
    /// weights are free to go negative and to exceed one.</summary>
    private static void AssertMatchesModel(List<(int Kind, int Key, int Alt, bool NewStep)> commands)
    {
        RunAsync().GetAwaiter().GetResult();
        return;

        async Task RunAsync()
        {
            foreach (var workers in new[] { 1, 4 })
            {
                var table = new SharedTable<int, Row>("reference", r => r.Key);
                var current = new Dictionary<int, Row>();
                var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Workers = workers });
                var reference = builder.Table(table);
                var factsDef = builder.Input<(int Key, int Val)>("facts");
                var innerDef = factsDef
                    .Lookup(reference, f => f.Key, (f, r) => (f.Key, f.Val, r.Val))
                    .ToOutput();
                var missingDef = factsDef
                    .Lookup(reference, f => f.Key, (f, r) => (f.Key, f.Val, r.Val), f => (f.Key, f.Val, -1))
                    .ToOutput();
                // Two rows in a group can carry the same Val, so equal results under one fact are covered.
                var indexDef = factsDef
                    .Lookup(reference.By("group", r => r.Group), f => f.Key, (f, r) => (f.Key, f.Val, r.Val), f => (f.Key, f.Val, -1))
                    .ToOutput();

                var inner = new Model<(int, int, int)>(f =>
                    current.TryGetValue(f.Key, out var row) ? [(f.Key, f.Val, row.Val)] : []);
                var missing = new Model<(int, int, int)>(f =>
                    current.TryGetValue(f.Key, out var row) ? [(f.Key, f.Val, row.Val)] : [(f.Key, f.Val, -1)]);
                var index = new Model<(int, int, int)>(f =>
                {
                    var rows = current.Values.Where(r => r.Group == f.Key).Select(r => (f.Key, f.Val, r.Val)).ToList();
                    return rows.Count > 0 ? rows : [(f.Key, f.Val, -1)];
                });

                await using var circuit = builder.Build();
                circuit.Start();
                var facts = circuit.Handle(factsDef);
                var outputs = new[] { new ZSet<(int, int, int)>(), new ZSet<(int, int, int)>(), new ZSet<(int, int, int)>() };
                var pendingFacts = new ZSet<(int Key, int Val)>();
                var step = 0;

                async Task StepAndCompare()
                {
                    await table.Writer.CommitAsync();
                    await circuit.StepAsync();
                    outputs[0].AddAll(Drain(circuit.Handle(innerDef)));
                    outputs[1].AddAll(Drain(circuit.Handle(missingDef)));
                    outputs[2].AddAll(Drain(circuit.Handle(indexDef)));
                    inner.Apply(pendingFacts);
                    missing.Apply(pendingFacts);
                    index.Apply(pendingFacts);
                    pendingFacts.Clear();

                    var models = new[] { ("inner", inner), ("missing", missing), ("index", index) };
                    for (var i = 0; i < models.Length; i++)
                        if (!Same(models[i].Item2.Integrated(), outputs[i]))
                            throw new Exception(
                                $"differential mismatch ({models[i].Item1}, workers={workers}, after step {step}): " +
                                $"the model gives {models[i].Item2.Integrated()} but the lookup gives {outputs[i]}");
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
                            pendingFacts.Add((command.Key, command.Alt), 1);
                            break;
                        case Kind.RetractFact:
                            await facts.SendAsync((command.Key, command.Alt), -1);
                            pendingFacts.Add((command.Key, command.Alt), -1);
                            break;
                        case Kind.UpsertRow:
                            // Rows 0..3 fall into groups 0..1, with Val drawn from 0..2.
                            var row = new Row(command.Key, command.Key % 2 + command.Alt % 2, command.Alt);
                            await table.Writer.UpsertAsync(row);
                            current[row.Key] = row;
                            break;
                        default:
                            await table.Writer.DeleteAsync(command.Key);
                            current.Remove(command.Key);
                            break;
                    }
                    pending = true;
                }
                await StepAndCompare();
            }
        }
    }

    [Fact]
    public void A_remembered_lookup_integrates_to_each_held_fact_enriched_as_of_its_last_assertion() =>
        GenCommands.Sample(AssertMatchesModel);

    // ── directed: remembered ───────────────────────────────────────────────────────────

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task A_retracted_fact_takes_back_the_row_it_was_emitted_as_not_the_row_the_table_has_now(int workers)
    {
        const int trades = 201;
        var table = NewTable();
        await Commit(table, new Instrument(7, "OLD"), new Instrument(8, "EIGHT"));
        var pipeline = Build(table, workers: workers);
        await using var circuit = pipeline.Builder.Build();
        circuit.Start();
        var input = circuit.Handle(pipeline.Trades);
        var enriched = circuit.Handle(pipeline.Enriched);

        for (var id = 0; id < trades; id++)
            await input.InsertAsync(new Trade(id, id % 2 == 0 ? 7 : 8));
        await circuit.StepAsync();
        Assert.Equal(trades, Drain(enriched).DistinctCount);

        // A hot row changes and another is deleted: nothing already emitted moves.
        await Commit(table, new Instrument(7, "NEW"));
        await Delete(table, 8);
        await circuit.StepAsync();
        AssertDelta(Drain(enriched));

        // Every retraction cancels what was emitted. They are sent in the opposite order, so
        // each is staged on a different worker from its assertion and only the exchange
        // brings it back to the one that remembers; a worker that did not would probe the
        // table and retract "NEW", or nothing at all.
        for (var id = trades - 1; id >= 0; id--)
            await input.RemoveAsync(new Trade(id, id % 2 == 0 ? 7 : 8));
        await circuit.StepAsync();
        AssertDelta(
            Drain(enriched),
            Enumerable.Range(0, trades).Select(id => ((id, id % 2 == 0 ? "OLD" : "EIGHT"), -1L)).ToArray());

        // Arriving now, the same trades see the table as it is now.
        await input.InsertAsync(new Trade(0, 7));
        await input.InsertAsync(new Trade(1, 8));
        await circuit.StepAsync();
        AssertDelta(Drain(enriched), ((0, "NEW"), 1));
    }

    [Fact]
    public async Task A_fact_with_no_row_is_dropped_or_given_the_missing_row_and_stays_that_way()
    {
        var table = NewTable();
        var dropping = Build(table, workers: 2);
        var defaulting = Build(table, missing: true, workers: 2);
        await using var inner = dropping.Builder.Build();
        await using var outer = defaulting.Builder.Build();
        inner.Start();
        outer.Start();

        await inner.Handle(dropping.Trades).InsertAsync(new Trade(1, 7));
        await outer.Handle(defaulting.Trades).InsertAsync(new Trade(1, 7));
        await inner.StepAsync();
        await outer.StepAsync();
        AssertDelta(Drain(inner.Handle(dropping.Enriched)));
        AssertDelta(Drain(outer.Handle(defaulting.Enriched)), ((1, None), 1));

        // The row arrives late. A join would enrich the trade now; a lookup has moved on.
        await Commit(table, new Instrument(7, "SEVEN"));
        await inner.StepAsync();
        await outer.StepAsync();
        AssertDelta(Drain(inner.Handle(dropping.Enriched)));
        AssertDelta(Drain(outer.Handle(defaulting.Enriched)));

        // And the retraction takes back what was emitted: nothing, and the missing row.
        await inner.Handle(dropping.Trades).RemoveAsync(new Trade(1, 7));
        await outer.Handle(defaulting.Trades).RemoveAsync(new Trade(1, 7));
        await inner.StepAsync();
        await outer.StepAsync();
        AssertDelta(Drain(inner.Handle(dropping.Enriched)));
        AssertDelta(Drain(outer.Handle(defaulting.Enriched)), ((1, None), -1));
    }

    [Fact]
    public async Task An_upserted_record_is_enriched_afresh_and_its_previous_enrichment_retracted()
    {
        var table = NewTable();
        await Commit(table, new Instrument(7, "SEVEN"), new Instrument(8, "EIGHT"));
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Workers = 3 });
        var instruments = builder.Table(table);
        var ordersDef = builder.KeyedInput<int, (int Id, int InstrumentId, int Qty)>("orders", o => o.Id);
        var enrichedDef = ordersDef
            .Lookup(instruments, o => o.InstrumentId, (order, instrument) => (order.Id, order.Qty, instrument.Symbol))
            .ToOutput();
        await using var circuit = builder.Build();
        circuit.Start();
        var orders = circuit.Handle(ordersDef);
        var enriched = circuit.Handle(enrichedDef);

        await orders.UpsertAsync((1, 7, 10));
        await circuit.StepAsync();
        AssertDelta(Drain(enriched), ((1, 10, "SEVEN"), 1));

        // The order is amended after its instrument was renamed: the old enrichment goes
        // exactly as it was emitted, and the amended order picks up the new name.
        await Commit(table, new Instrument(7, "SEVEN'"));
        await orders.UpsertAsync((1, 7, 12));
        await circuit.StepAsync();
        AssertDelta(Drain(enriched), ((1, 10, "SEVEN"), -1), ((1, 12, "SEVEN'"), 1));

        // Amended onto another instrument: the two halves of the upsert have different
        // lookup keys and may be handled by different workers.
        await orders.UpsertAsync((1, 8, 12));
        await circuit.StepAsync();
        AssertDelta(Drain(enriched), ((1, 12, "SEVEN'"), -1), ((1, 12, "EIGHT"), 1));

        await orders.DeleteAsync(1);
        await circuit.StepAsync();
        AssertDelta(Drain(enriched), ((1, 12, "EIGHT"), -1));
    }

    [Fact]
    public async Task What_a_lookup_remembers_survives_a_restart()
    {
        var table = NewTable();
        var storage = new InMemoryCheckpointStorageProvider();
        await Commit(table, new Instrument(7, "SEVEN"));

        var before = Build(table, workers: 2, storage: storage);
        await using (var first = before.Builder.Build())
        {
            first.Start();
            await first.Handle(before.Trades).InsertAsync(new Trade(1, 7));
            await first.Handle(before.Trades).InsertAsync(new Trade(2, 7));
            await first.StepAsync();
            AssertDelta(Drain(first.Handle(before.Enriched)), ((1, "SEVEN"), 1), ((2, "SEVEN"), 1));
        }

        await Commit(table, new Instrument(7, "SEVEN'"));

        var after = Build(table, workers: 2, storage: storage);
        await using var resumed = after.Builder.Build();
        resumed.Start();
        await resumed.StepAsync();
        AssertDelta(Drain(resumed.Handle(after.Enriched)));

        await resumed.Handle(after.Trades).RemoveAsync(new Trade(1, 7));
        await resumed.Handle(after.Trades).InsertAsync(new Trade(3, 7));
        await resumed.StepAsync();
        AssertDelta(Drain(resumed.Handle(after.Enriched)), ((1, "SEVEN"), -1), ((3, "SEVEN'"), 1));
    }

    [Fact]
    public async Task A_lookup_through_an_index_emits_one_row_per_match_and_takes_them_all_back()
    {
        var table = new SharedTable<int, Listing>("listings", l => l.Id);
        await table.Writer.UpsertAsync(new Listing(1, "ACME", "NYSE"));
        await table.Writer.UpsertAsync(new Listing(2, "ACME", "LSE"));
        await table.Writer.UpsertAsync(new Listing(3, "BOLT", "NYSE"));
        await table.Writer.CommitAsync();

        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Workers = 3 });
        var bySymbol = builder.Table(table).By("symbol", l => l.Symbol);
        var quotesDef = builder.Input<(int Id, string Symbol)>("quotes");
        var listedDef = quotesDef
            .Lookup(bySymbol, q => q.Symbol, (quote, listing) => (quote.Id, listing.Venue), quote => (quote.Id, None))
            .ToOutput();
        await using var circuit = builder.Build();
        circuit.Start();
        var quotes = circuit.Handle(quotesDef);
        var listed = circuit.Handle(listedDef);

        await quotes.InsertAsync((1, "ACME"));
        await quotes.InsertAsync((2, "BOLT"));
        await quotes.InsertAsync((3, "CRUX"));
        await circuit.StepAsync();
        AssertDelta(Drain(listed), ((1, "NYSE"), 1), ((1, "LSE"), 1), ((2, "NYSE"), 1), ((3, None), 1));

        // ACME delists from one venue and lists on another; CRUX appears. Nothing moves.
        await table.Writer.DeleteAsync(2);
        await table.Writer.UpsertAsync(new Listing(4, "ACME", "XETRA"));
        await table.Writer.UpsertAsync(new Listing(5, "CRUX", "LSE"));
        await table.Writer.CommitAsync();
        await circuit.StepAsync();
        AssertDelta(Drain(listed));

        await quotes.RemoveAsync((1, "ACME"));
        await quotes.RemoveAsync((3, "CRUX"));
        await quotes.InsertAsync((4, "ACME"));
        await circuit.StepAsync();
        AssertDelta(
            Drain(listed),
            ((1, "NYSE"), -1), ((1, "LSE"), -1), ((3, None), -1), ((4, "NYSE"), 1), ((4, "XETRA"), 1));
    }

    private sealed record Listing(int Id, string Symbol, string Venue);

    [Fact]
    public async Task Lookup_keys_follow_a_caller_supplied_hash()
    {
        const int trades = 61;
        var table = NewTable();
        await Commit(table, Enumerable.Range(0, 10).Select(id => new Instrument(id, "v1-" + id)).ToArray());
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Workers = 4 });
        var instruments = builder.Table(table);
        var tradesDef = builder.Input<Trade>("trades");
        var enrichedDef = tradesDef
            .Lookup(instruments, t => t.InstrumentId, id => (uint)(id / 3), (trade, instrument) => (trade.Id, instrument.Symbol))
            .ToOutput();
        await using var circuit = builder.Build();
        circuit.Start();

        for (var id = 0; id < trades; id++)
            await circuit.Handle(tradesDef).InsertAsync(new Trade(id, id % 10));
        await circuit.StepAsync();
        await Commit(table, Enumerable.Range(0, 10).Select(id => new Instrument(id, "v2-" + id)).ToArray());
        for (var id = trades - 1; id >= 0; id--)
            await circuit.Handle(tradesDef).RemoveAsync(new Trade(id, id % 10));
        await circuit.StepAsync();
        Assert.True(Drain(circuit.Handle(enrichedDef)).IsEmpty, "every retraction should have cancelled its assertion");
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task What_is_remembered_for_a_closed_key_is_evicted_and_its_results_stay_final(int workers)
    {
        var table = NewTable();
        await Commit(table, new Instrument(1, "ONE"), new Instrument(2, "TWO"));
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Workers = workers });
        var instruments = builder.Table(table);
        var tradesDef = builder.Input<(int Id, int InstrumentId)>("trades");
        var clockDef = builder.Input<DateTimeOffset>("clock");
        // The clock only drives the watermark, so the trades themselves are never late.
        clockDef.WithEventTime(at => at, lateness: TimeSpan.Zero);
        var enrichedDef = tradesDef
            .GroupBy(t => t.InstrumentId)
            .EvictWhen(id => id == 1 ? T0.AddSeconds(5) : T0.AddHours(1))
            .Lookup(instruments, (_, trade, instrument) => (trade.Id, instrument.Symbol))
            .ToOutput();
        await using var circuit = builder.Build();
        circuit.Start();
        var trades = circuit.Handle(tradesDef);
        var enriched = circuit.Handle(enrichedDef);

        await trades.InsertAsync((1, 1));
        await trades.InsertAsync((2, 2));
        await circuit.Handle(clockDef).InsertAsync(T0.AddSeconds(1));
        await circuit.StepAsync();
        AssertDelta(Drain(enriched), ((1, "ONE"), 1), ((2, "TWO"), 1));

        // The watermark passes key 1's deadline; the step after sees it and forgets key 1.
        await circuit.Handle(clockDef).InsertAsync(T0.AddSeconds(7));
        await circuit.StepAsync();
        await circuit.StepAsync();
        await Commit(table, new Instrument(1, "ONE'"), new Instrument(2, "TWO'"));

        // The open key still retracts what it emitted. The closed key has nothing held, so
        // its retraction is treated as a fact arriving now — which shows the memory is gone.
        await trades.RemoveAsync((1, 1));
        await trades.RemoveAsync((2, 2));
        await circuit.StepAsync();
        AssertDelta(Drain(enriched), ((1, "ONE'"), -1), ((2, "TWO"), -1));
    }

    // ── append-only ────────────────────────────────────────────────────────────────────

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task An_append_only_lookup_enriches_each_record_once_from_the_table_as_it_is(int workers)
    {
        const int trades = 100;
        var table = NewTable();
        await Commit(table, new Instrument(0, "ZERO"), new Instrument(1, "ONE"));
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Workers = workers });
        var instruments = builder.Table(table);
        var tradesDef = builder.Input<Trade>("trades");
        var innerDef = tradesDef
            .LookupAppendOnly(instruments, t => t.InstrumentId, (trade, instrument) => (trade.Id, instrument.Symbol))
            .ToOutput();
        var outerDef = tradesDef
            .LookupAppendOnly(instruments, t => t.InstrumentId, (trade, instrument) => (trade.Id, instrument.Symbol), trade => (trade.Id, None))
            .ToOutput();
        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(tradesDef);

        for (var id = 0; id < trades; id++)
            await input.InsertAsync(new Trade(id, id % 3));
        await circuit.StepAsync();
        string? Symbol(int id) => (id % 3) switch { 0 => "ZERO", 1 => "ONE", _ => null };
        AssertDelta(
            Drain(circuit.Handle(innerDef)),
            Enumerable.Range(0, trades).Where(id => Symbol(id) is not null).Select(id => ((id, Symbol(id)!), 1L)).ToArray());
        AssertDelta(
            Drain(circuit.Handle(outerDef)),
            Enumerable.Range(0, trades).Select(id => ((id, Symbol(id) ?? None), 1L)).ToArray());

        // Later changes to the table correct nothing; later records see them.
        await Commit(table, new Instrument(0, "ZERO'"), new Instrument(2, "TWO"));
        await circuit.StepAsync();
        AssertDelta(Drain(circuit.Handle(innerDef)));
        await input.SendAsync(new Trade(1000, 0), 2);
        await input.InsertAsync(new Trade(1001, 2));
        await circuit.StepAsync();
        AssertDelta(Drain(circuit.Handle(innerDef)), ((1000, "ZERO'"), 2), ((1001, "TWO"), 1));
    }

    [Fact]
    public async Task A_retraction_reaching_an_append_only_lookup_faults_the_circuit_and_says_why()
    {
        var table = NewTable();
        await Commit(table, new Instrument(7, "SEVEN"));
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Workers = 2 });
        var instruments = builder.Table(table);
        var tradesDef = builder.Input<Trade>("trades");
        var enrichedDef = tradesDef
            .LookupAppendOnly(instruments, t => t.InstrumentId, (trade, instrument) => (trade.Id, instrument.Symbol))
            .ToOutput();
        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(tradesDef);

        await input.InsertAsync(new Trade(1, 7));
        await circuit.StepAsync();
        AssertDelta(Drain(circuit.Handle(enrichedDef)), ((1, "SEVEN"), 1));

        await input.RemoveAsync(new Trade(1, 7));
        var fault = await Assert.ThrowsAsync<InvalidOperationException>(() => circuit.StepAsync()).WaitAsync(Timeout);
        Assert.Contains("LookupAppendOnly against table 'instruments' received a retraction", fault.Message);
        Assert.Contains("Use Lookup", fault.Message);
        Assert.NotNull(circuit.Fault);
        await Assert.ThrowsAsync<CircuitFaultedException>(() => circuit.StepAsync()).WaitAsync(Timeout);

        // A faulted circuit must not go on holding the table's history back.
        Assert.Equal(0, table.PinCount);
    }

    [Fact]
    public void Lookups_are_rejected_inside_a_recursion_and_across_builders()
    {
        var table = NewTable();
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        var instruments = builder.Table(table);
        var keys = builder.Input<int>("keys");

        Assert.Throws<NotSupportedException>(() => keys.Recurse(loop => loop
            .Lookup(instruments, key => key, (key, _) => key + 1)));
        Assert.Throws<NotSupportedException>(() => keys.Recurse(loop => loop
            .LookupAppendOnly(instruments, key => key, (key, _) => key + 1)));

        var other = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        Assert.Throws<ArgumentException>(() => other.Input<int>("keys").Lookup(instruments, key => key, (key, _) => key));
        Assert.Throws<ArgumentException>(() => other.Input<int>("keys").LookupAppendOnly(instruments, key => key, (key, _) => key));
    }
}
