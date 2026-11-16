using System.Diagnostics;
using Incremental;
using Incremental.Tables;

// Trade enrichment on shared tables: reference data held once per process, joined by
// circuits that keep none of it.
//
//   traders ──Changes()──▶ ⋈ desks ──ToTable──▶ trader-desks              (a small circuit)
//
//   trades ──Lookup(instruments)──▶ ⋈ trader-desks ──▶ ⋈ counterparties ──▶ blotter sink
//            on arrival             retroactive        retroactive      └─▶ Σ per desk ──▶ sink
//
// The join kind is chosen per column. A trade's symbol is looked up when the trade arrives
// and then left alone; its desk and its counterparty's rating are joined, so a trader who
// moves desk takes every trade they ever did with them. Both sinks are fed through the
// acknowledged-output path (DeliverToAsync), as a database or a topic would be.
//
// The trade circuit's state is the live book: every trade not yet cancelled is held by both
// joins and the lookup, so it is bounded by what the source deletes — a cancellation, a
// settlement — and by nothing else. That suits a book of positions. A feed of trades that
// are never deleted needs event time and EvictWhenFinal instead (the enrichment service's
// trade-enrichment definition, and docs/enrichment-service.md, "Bounding a join over a
// stream of facts"). Act 7 settles the whole book and the state goes with it.
//
// The program narrates what it does and checks every claim against a model kept beside the
// circuits; a claim that does not hold ends the run with exit code 1.
//
//   dotnet run --project examples/Incremental.TradeEnrichment [-- --trades 5000]

var tradeCount = 2000;
if (Array.IndexOf(args, "--trades") is >= 0 and var at && at + 1 < args.Length)
    tradeCount = int.Parse(args[at + 1]);

// ── reference data: four shared tables, loaded before any circuit exists ────────────────

var desks = new SharedTable<int, Desk>("desks", d => d.Id);
var traders = new SharedTable<int, Trader>("traders", t => t.Id);
var instruments = new SharedTable<int, Instrument>("instruments", i => i.Id);
var counterparties = new SharedTable<int, Counterparty>("counterparties", c => c.Id);
var traderDesks = new SharedTable<int, TraderDesk>("trader-desks", td => td.TraderId);   // derived, below

// The model: plain dictionaries of what the tables should hold.
var deskRows = new Dictionary<int, Desk>();
var traderRows = new Dictionary<int, Trader>();
var instrumentRows = new Dictionary<int, Instrument>();
var counterpartyRows = new Dictionary<int, Counterparty>();

(string Name, string Region)[] deskSeed =
[
    ("Equity Cash", "EMEA"), ("Equity Derivs", "AMER"), ("Macro", "EMEA"), ("Credit", "AMER"),
    ("Rates", "APAC"), ("FX Spot", "EMEA"), ("Commodities", "AMER"), ("Convertibles", "APAC"),
];
string[] firstNames = ["Ada", "Grace", "Alan", "Edsger", "Barbara", "Donald", "Tony", "Leslie", "Radia", "Ken", "Dennis", "Margaret"];
(string Symbol, string AssetClass, decimal Price)[] instrumentSeed =
[
    ("AAPL", "Equity", 230m), ("MSFT", "Equity", 420m), ("VOD.L", "Equity", 0.70m), ("SIE.DE", "Equity", 180m),
    ("SPY", "ETF", 560m), ("US10Y", "Govt Bond", 98.50m), ("DE10Y", "Govt Bond", 97.20m), ("XS-CRD1", "Corp Bond", 101.20m),
    ("EURUSD", "FX", 1.08m), ("GBPUSD", "FX", 1.27m), ("BRN-F", "Commodity", 82m), ("GLD-F", "Commodity", 2350m),
];
string[] ratings = ["AAA", "AA", "A", "BBB", "BB"];
string[] counterpartyNames = ["Meridian AM", "Blue Harbor", "Northlake Capital", "Osprey Funds", "Cinder Rock", "Halcyon Partners", "Verdant Global", "Quarry Point"];

for (var i = 0; i < deskSeed.Length; i++)
    deskRows[i + 1] = new Desk(i + 1, deskSeed[i].Name, deskSeed[i].Region);
for (var i = 1; i <= 24; i++)
    traderRows[i] = new Trader(i, $"{firstNames[i % firstNames.Length]} {(char)('A' + i % 26)}.", 1 + i % deskSeed.Length);
for (var i = 0; i < instrumentSeed.Length; i++)
    instrumentRows[i + 1] = new Instrument(i + 1, instrumentSeed[i].Symbol, instrumentSeed[i].AssetClass, instrumentSeed[i].Price);
for (var i = 0; i < counterpartyNames.Length; i++)
    counterpartyRows[i + 1] = new Counterparty(i + 1, counterpartyNames[i], ratings[i % ratings.Length]);

// One writer per table; ReplaceAllAsync is a snapshot load, committed as one version.
await desks.Writer.ReplaceAllAsync(deskRows.Values);
await traders.Writer.ReplaceAllAsync(traderRows.Values);
await instruments.Writer.ReplaceAllAsync(instrumentRows.Values);
await counterparties.Writer.ReplaceAllAsync(counterpartyRows.Values);

// ── a derived table: traders ⋈ desks, computed once, written with ToTable ───────────────

var deriving = new CircuitBuilder(new CircuitOptions { Workers = 1 });
deriving.Table(traders).Changes()                       // the table's rows as a stream
    .GroupBy(t => t.DeskId)
    .Join(deriving.Table(desks), (_, trader, desk) => new TraderDesk(trader.Id, trader.Name, desk.Name, desk.Region))
    .ToTable(traderDesks);                              // this circuit is now the table's one writer
await using var derivingCircuit = deriving.Build();
derivingCircuit.Start();

// ── the trade circuit: it stores trades, and no reference row ───────────────────────────

// A checkpointing store, for one reason: it can say how many entries the state holds.
var storage = new Incremental.Storage.InMemoryCheckpointStorageProvider();
var builder = new CircuitBuilder(new CircuitOptions { Workers = 1, Storage = storage });
var tradeStream = builder.KeyedInput<long, Trade>("trades", t => t.Id);

var enriched = tradeStream
    // Enrich on arrival: the symbol a trade was done under is a fact about the trade.
    .Lookup(
        builder.Table(instruments), t => t.InstrumentId,
        (t, i) => new Priced(t, i.Symbol, i.AssetClass),
        t => new Priced(t, "?", "?"))
    // Retroactive: the desk is where the trader sits now, for every trade they ever did.
    .GroupBy(p => p.Trade.TraderId)
    .Join(builder.Table(traderDesks), (_, p, td) => (Priced: p, Desk: td))
    // Retroactive: a re-rating applies to the counterparty's whole book.
    .GroupBy(x => x.Priced.Trade.CounterpartyId)
    .Join(builder.Table(counterparties), (_, x, c) => new EnrichedTrade(
        x.Priced.Trade.Id, x.Priced.Symbol, x.Priced.AssetClass, x.Desk.Trader, x.Desk.Desk, x.Desk.Region,
        c.Name, c.Rating, x.Priced.Trade.Quantity * x.Priced.Trade.Price));               // a computed column

var blotterOutput = enriched.ToAcknowledgedOutput();
var deskOutput = enriched.GroupBy(e => e.Desk).Sum(e => e.Notional).ToAcknowledgedOutput();

await using var circuit = builder.Build();
circuit.Start();
var trades = circuit.Handle(tradeStream);

// Sinks on the acknowledged-output path: a step is committed only after its batch is
// written, and a batch is at most one upsert or delete per key.
var blotter = new InMemoryUpsertSink<long, EnrichedTrade>();
var deskTotals = new InMemoryUpsertSink<string, decimal>();
var blotterDelivery = circuit.Handle(blotterOutput).DeliverToAsync(blotter, e => e.Id);
var deskDelivery = circuit.Handle(deskOutput).DeliverToAsync(deskTotals);

// ── the model of the trade side ─────────────────────────────────────────────────────────

var book = new Dictionary<long, Trade>();
var arrival = new Dictionary<long, Instrument>();        // the instrument row each trade met when it arrived
var failures = 0;
var claims = 0;

EnrichedTrade Expected(Trade t)
{
    var trader = traderRows[t.TraderId];
    var desk = deskRows[trader.DeskId];
    var counterparty = counterpartyRows[t.CounterpartyId];
    var instrument = arrival[t.Id];
    return new EnrichedTrade(t.Id, instrument.Symbol, instrument.AssetClass, trader.Name, desk.Name, desk.Region,
        counterparty.Name, counterparty.Rating, t.Quantity * t.Price);
}

bool BlotterMatches()
{
    var rows = blotter.Rows;
    return rows.Count == book.Count && book.Values.All(t => rows.TryGetValue(t.Id, out var row) && row == Expected(t));
}

bool DeskTotalsMatch()
{
    var expected = book.Values.Select(Expected).GroupBy(e => e.Desk).ToDictionary(g => g.Key, g => g.Sum(e => e.Notional));
    var rows = deskTotals.Rows;
    return rows.Count == expected.Count && expected.All(e => rows.TryGetValue(e.Key, out var total) && total == e.Value);
}

// The circuits step by themselves, so a claim is checked by waiting — with a bound — for
// the sinks to say what the model says.
async Task Claim(string text, Func<bool> holds)
{
    claims++;
    var waited = Stopwatch.StartNew();
    while (!holds() && waited.Elapsed < TimeSpan.FromSeconds(15))
        await Task.Delay(5);
    if (holds())
        Console.WriteLine($"  ok    {text}");
    else
    {
        failures++;
        Console.WriteLine($"  WRONG {text}");
    }
}

async Task Send(Trade trade)
{
    book[trade.Id] = trade;
    arrival[trade.Id] = instrumentRows[trade.InstrumentId];
    await trades.UpsertAsync(trade);
}

// ── act 1: the reference data is there before the trades ────────────────────────────────

Console.WriteLine("Trade enrichment on shared tables");
Console.WriteLine();
Console.WriteLine($"1. Four reference tables are loaded ({deskRows.Count} desks, {traderRows.Count} traders, {instrumentRows.Count} instruments, " +
                  $"{counterpartyRows.Count} counterparties); a small circuit derives trader-desks from two of them.");
await Claim("the derived table holds one row per trader, each with its desk's name and region", () =>
{
    using var snapshot = traderDesks.Snapshot();
    return traderDesks.Count == traderRows.Count && traderRows.Values.All(t =>
        snapshot.TryGet(t.Id, out var row) && row == new TraderDesk(t.Id, t.Name, deskRows[t.DeskId].Name, deskRows[t.DeskId].Region));
});

// ── act 2: trades, amendments, cancellations ────────────────────────────────────────────

var random = new Random(11);
Trade RandomTrade(long id)
{
    var instrument = instrumentRows[1 + random.Next(instrumentRows.Count)];
    return new Trade(id, instrument.Id, 1 + random.Next(traderRows.Count), 1 + random.Next(counterpartyRows.Count),
        10 * (1 + random.Next(500)), Math.Round(instrument.BasePrice * (0.99m + random.Next(200) / 10_000m), 2));
}

int amended = 0, cancelled = 0;
for (long id = 1; id <= tradeCount; id++)
{
    await Send(RandomTrade(id));
    if (id % 10 == 0)
    {
        // An amendment is an upsert of the same id; the circuit retracts the old row itself.
        var victim = book.Values.ElementAt(random.Next(book.Count));
        await Send(victim with { Quantity = victim.Quantity + 10 });
        amended++;
    }
    if (id % 25 == 0)
    {
        var victim = book.Keys.ElementAt(random.Next(book.Count));
        book.Remove(victim);
        arrival.Remove(victim);
        await trades.DeleteAsync(victim);
        cancelled++;
    }
}
Console.WriteLine();
Console.WriteLine($"2. {tradeCount:N0} trades arrive, {amended:N0} are amended and {cancelled:N0} cancelled.");
await Claim($"the blotter sink holds exactly the {book.Count:N0} live trades, each enriched from all four tables", BlotterMatches);
await Claim("the per-desk totals in the second sink equal the sums over the model", DeskTotalsMatch);

// ── act 3: a retroactive join — a trader moves desk ─────────────────────────────────────

var mover = traderRows.Values.OrderByDescending(t => book.Values.Count(b => b.TraderId == t.Id)).First();
var fromDesk = deskRows[mover.DeskId];
var toDesk = deskRows[1 + mover.DeskId % deskRows.Count];
var moved = book.Values.Count(t => t.TraderId == mover.Id);
var batchesBefore = blotter.Batches;
traderRows[mover.Id] = mover with { DeskId = toDesk.Id };
await traders.Writer.UpsertAsync(traderRows[mover.Id]);
await traders.Writer.CommitAsync();
Console.WriteLine();
Console.WriteLine($"3. {mover.Name} moves from {fromDesk.Name} to {toDesk.Name}: one row of one table changes.");
await Claim($"all {moved:N0} of their historical trades now carry {toDesk.Name} — through the derived table and the join, with no trade re-sent",
    () => blotter.Rows.Values.Count(e => e.Trader == mover.Name && e.Desk == toDesk.Name) == moved && BlotterMatches());
await Claim($"both desks' totals moved by the same amount, and every other total is unchanged", DeskTotalsMatch);
Console.WriteLine($"        ({blotter.Batches - batchesBefore} batch(es) reached the blotter sink for it)");

// ── act 4: a lookup — an instrument is renamed ──────────────────────────────────────────

var renamed = instrumentRows.Values.OrderByDescending(i => book.Values.Count(b => b.InstrumentId == i.Id)).First();
var oldSymbol = renamed.Symbol;
var newSymbol = oldSymbol + ".N";
var before = book.Values.Where(t => t.InstrumentId == renamed.Id).Select(t => t.Id).ToHashSet();
instrumentRows[renamed.Id] = renamed with { Symbol = newSymbol };
await instruments.Writer.UpsertAsync(instrumentRows[renamed.Id]);
await instruments.Writer.CommitAsync();
var after = RandomTrade(tradeCount + 1) with { InstrumentId = renamed.Id };
await Send(after);
Console.WriteLine();
Console.WriteLine($"4. {oldSymbol} is renamed {newSymbol}, and one more trade in it arrives.");
await Claim($"the new trade carries {newSymbol}", () => blotter.Rows.TryGetValue(after.Id, out var row) && row.Symbol == newSymbol);
await Claim($"the {before.Count:N0} trades done before the rename still say {oldSymbol}: a lookup enriches once",
    () => BlotterMatches() && before.All(id => blotter.Rows[id].Symbol == oldSymbol));

// ── act 5: an amendment is a new arrival ────────────────────────────────────────────────

var amendedId = before.First();
await Send(book[amendedId] with { Quantity = book[amendedId].Quantity + 100 });
Console.WriteLine();
Console.WriteLine($"5. Trade {amendedId}, one of those, is amended.");
await Claim($"the row it produced is taken back and the amended trade is enriched afresh, with {newSymbol}",
    () => blotter.Rows.TryGetValue(amendedId, out var row) && row.Symbol == newSymbol && row == Expected(book[amendedId]) && BlotterMatches());

// ── act 6: a re-rating, and a cancellation ──────────────────────────────────────────────

var rerated = counterpartyRows[1];
var newRating = rerated.Rating == "BB" ? "AAA" : "BB";
var book1 = book.Values.Count(t => t.CounterpartyId == rerated.Id);
counterpartyRows[rerated.Id] = rerated with { Rating = newRating };
await counterparties.Writer.UpsertAsync(counterpartyRows[rerated.Id]);
await counterparties.Writer.CommitAsync();
var gone = book.Keys.Max();
book.Remove(gone);
arrival.Remove(gone);
await trades.DeleteAsync(gone);
Console.WriteLine();
Console.WriteLine($"6. {rerated.Name} is re-rated {rerated.Rating} → {newRating}, and trade {gone} is cancelled.");
await Claim($"every one of its {book.Values.Count(t => t.CounterpartyId == rerated.Id):N0} trades carries {newRating}",
    () => BlotterMatches() && blotter.Rows.Values.Where(e => e.Counterparty == rerated.Name).All(e => e.Rating == newRating));
await Claim($"trade {gone} is gone from the blotter and from its desk's total", () => !blotter.Rows.ContainsKey(gone) && BlotterMatches() && DeskTotalsMatch());

// ── act 7: the book is settled ──────────────────────────────────────────────────────────

long StateEntries() => storage.CommittedEntriesByStore().Values.Sum();
var held = StateEntries();
var live = book.Count;
foreach (var id in book.Keys.ToList())
{
    book.Remove(id);
    arrival.Remove(id);
    await trades.DeleteAsync(id);
}
Console.WriteLine();
Console.WriteLine($"7. Every one of the {live:N0} live trades is settled: deleted at the source.");
await Claim($"the circuit's state, {held:N0} entries for {live:N0} live trades, returns to nothing with the book: what bounds it is the source's deletes",
    () => StateEntries() == 0 && blotter.Rows.Count == 0 && DeskTotalsMatch());

// ── the end: drain, and look once more ──────────────────────────────────────────────────

await circuit.DisposeAsync();
await Task.WhenAll(blotterDelivery, deskDelivery);
await derivingCircuit.DisposeAsync();
Console.WriteLine();
Console.WriteLine("8. Both circuits are drained and disposed.");
await Claim("the sinks, read after the last acknowledgement, still equal the model", () => BlotterMatches() && DeskTotalsMatch());
await Claim("no batch was delivered twice", () => blotter.Redeliveries == 0 && deskTotals.Redeliveries == 0);

Console.WriteLine();
Console.WriteLine(failures == 0
    ? $"{claims} claims checked, all hold. The trade circuit kept {live:N0} trades at most and not one reference row."
    : $"{failures} of {claims} claims did NOT hold.");
return failures == 0 ? 0 : 1;

// Rows of shared tables are immutable records: every circuit is handed the same objects.
record Desk(int Id, string Name, string Region);

record Trader(int Id, string Name, int DeskId);

record TraderDesk(int TraderId, string Trader, string Desk, string Region);

record Instrument(int Id, string Symbol, string AssetClass, decimal BasePrice);

record Counterparty(int Id, string Name, string Rating);

record Trade(long Id, int InstrumentId, int TraderId, int CounterpartyId, long Quantity, decimal Price);

record Priced(Trade Trade, string Symbol, string AssetClass);

record EnrichedTrade(
    long Id, string Symbol, string AssetClass, string Trader, string Desk, string Region,
    string Counterparty, string Rating, decimal Notional);
