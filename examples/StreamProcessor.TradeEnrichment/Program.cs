using System.Diagnostics;
using Spectre.Console;
using Spectre.Console.Rendering;
using StreamProcessor;

// Trade enrichment: a stream of trades joined against reference ("master") data — the
// classic finance pipeline. You pick which enrichments to join and which dimensions to
// group by; the circuit is composed from those choices at runtime.
//
//   trades ⋈ instruments ⋈ traders ⋈ desks ⋈ counterparties ─▶ group by <your dims> ─▶ top N
//
// The part most streaming systems get wrong: reference data is a stream too. When a trader
// moves desks mid-run, every one of their historical trades is re-attributed instantly —
// the join retracts the old enrichment and asserts the new one, and the grouped totals
// follow. Watch for the master-data notices while it runs.

const int MaxGroupRows = 14;
const int MaxBlotterRows = 12;

var interactive = !Console.IsInputRedirected && AnsiConsole.Profile.Capabilities.Interactive;

AnsiConsole.Write(new Panel(new Rows(
        new Markup("A live trade stream is enriched by joining reference tables you choose, then grouped by dimensions you choose — the circuit is composed from your selections."),
        new Markup("[green]▪[/] Trades are amended and cancelled while it runs; every view corrects itself by retraction."),
        new Markup("[green]▪[/] Reference data is a stream too: desk moves and rating changes re-attribute [italic]historical[/] trades instantly — watch the notices."),
        new Markup("[green]▪[/] With no dimensions selected you get a live top-trades blotter instead.")))
    .Header("[bold] trade enrichment [/]")
    .Padding(1, 0)
    .BorderColor(Color.Grey));

// ── master data ─────────────────────────────────────────────────────────────────

var desks = new List<Desk>
{
    new(1, "Equity Cash", "EMEA"), new(2, "Equity Derivs", "AMER"), new(3, "Macro", "EMEA"),
    new(4, "Credit", "AMER"), new(5, "Rates", "APAC"), new(6, "FX Spot", "EMEA"),
    new(7, "Commodities", "AMER"), new(8, "Convertibles", "APAC"),
};
string[] firstNames = ["Ada", "Grace", "Alan", "Edsger", "Barbara", "Donald", "Tony", "Leslie", "Radia", "Ken", "Dennis", "Margaret"];
var traders = Enumerable.Range(1, 24)
    .Select(i => new TraderRow(i, $"{firstNames[i % firstNames.Length]} {(char)('A' + i % 26)}.", desks[i % desks.Count].Id))
    .ToList();
(string Symbol, string AssetClass, string Currency, double Price)[] instrumentSeed =
[
    ("AAPL", "Equity", "USD", 230), ("MSFT", "Equity", "USD", 420), ("VOD.L", "Equity", "GBP", 0.7),
    ("SIE.DE", "Equity", "EUR", 180), ("7203.T", "Equity", "JPY", 2800), ("SPY", "ETF", "USD", 560),
    ("US10Y", "Govt Bond", "USD", 98.5), ("DE10Y", "Govt Bond", "EUR", 97.2), ("GB10Y", "Govt Bond", "GBP", 96.8),
    ("XS-CRD1", "Corp Bond", "USD", 101.2), ("XS-CRD2", "Corp Bond", "EUR", 99.4),
    ("EURUSD", "FX", "USD", 1.08), ("GBPUSD", "FX", "USD", 1.27), ("USDJPY", "FX", "JPY", 148),
    ("BRN-F", "Commodity", "USD", 82), ("GLD-F", "Commodity", "USD", 2350),
];
var instruments = instrumentSeed.Select((s, i) => new Instrument(i + 1, s.Symbol, s.AssetClass, s.Currency, s.Price)).ToList();
string[] ratings = ["AAA", "AA", "A", "BBB", "BB"];
string[] cptyNames = ["Meridian AM", "Blue Harbor", "Northlake Capital", "Osprey Funds", "Cinder Rock", "Halcyon Partners", "Verdant Global", "Quarry Point", "Silvergate Macro", "Aldgate Securities"];
var counterparties = cptyNames.Select((n, i) => new Counterparty(i + 1, n, ratings[i % ratings.Length])).ToList();

// ── setup: choose enrichments and dimensions ────────────────────────────────────

const string EnrichInstrument = "Instrument  (symbol, asset class, currency)";
const string EnrichDesk = "Desk        (trader ⋈ desk: name, region — a two-hop join)";
const string EnrichCpty = "Counterparty (name, credit rating)";

List<string> chosenEnrichments;
if (interactive)
{
    chosenEnrichments = AnsiConsole.Prompt(new MultiSelectionPrompt<string>()
        .Title("Which [bold]enrichments[/] should be joined onto the trade stream?")
        .InstructionsText("[grey](space to toggle, enter to accept — none is allowed)[/]")
        .NotRequired()
        .AddChoices(EnrichInstrument, EnrichDesk, EnrichCpty));
}
else
{
    chosenEnrichments = [EnrichInstrument, EnrichDesk, EnrichCpty];
    AnsiConsole.MarkupLine("[grey]non-interactive input: enabling all enrichments[/]");
}
var useInstrument = chosenEnrichments.Contains(EnrichInstrument);
var useDesk = chosenEnrichments.Contains(EnrichDesk);
var useCpty = chosenEnrichments.Contains(EnrichCpty);

var dimensionCatalog = new List<(string Name, Func<EnrichedTrade, string> Select)> { ("Side", e => e.Side) };
if (useInstrument)
{
    dimensionCatalog.Add(("Asset class", e => e.AssetClass));
    dimensionCatalog.Add(("Currency", e => e.Currency));
}
if (useDesk)
{
    dimensionCatalog.Add(("Desk", e => e.Desk));
    dimensionCatalog.Add(("Region", e => e.Region));
    dimensionCatalog.Add(("Trader", e => e.Trader));
}
if (useCpty)
{
    dimensionCatalog.Add(("Counterparty", e => e.Counterparty));
    dimensionCatalog.Add(("Rating", e => e.Rating));
}

List<string> chosenDims;
if (interactive)
{
    chosenDims = AnsiConsole.Prompt(new MultiSelectionPrompt<string>()
        .Title($"Group by which [bold]dimensions[/]? [grey](pick none for a top-{MaxBlotterRows} trade blotter)[/]")
        .InstructionsText("[grey](space to toggle, enter to accept)[/]")
        .NotRequired()
        .AddChoices(dimensionCatalog.Select(d => d.Name)));
}
else
{
    chosenDims = [useDesk ? "Desk" : "Side", useInstrument ? "Asset class" : "Side"];
    chosenDims = chosenDims.Distinct().ToList();
    AnsiConsole.MarkupLine($"[grey]non-interactive input: grouping by {string.Join(" · ", chosenDims)}[/]");
}
var dims = dimensionCatalog.Where(d => chosenDims.Contains(d.Name)).ToList();
var blotterMode = dims.Count == 0;

AnsiConsole.MarkupLine(
    $"[grey]circuit:[/] trades{(useInstrument ? " ⋈ instruments" : "")}{(useDesk ? " ⋈ traders ⋈ desks" : "")}{(useCpty ? " ⋈ counterparties" : "")} " +
    (blotterMode
        ? $"→ top {MaxBlotterRows} trades by notional"
        : $"→ group by [bold]{string.Join(" · ", dims.Select(d => d.Name))}[/] → count + notional → top {MaxGroupRows} groups"));
AnsiConsole.WriteLine();

// ── build the circuit from the selections ───────────────────────────────────────

var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Workers = 2 });

var tradesIn = builder.Input<Trade>("trades");
var enriched = tradesIn.Map(t => new EnrichedTrade(
    t.Id, t.Side, t.InstrumentId, t.TraderId, t.CounterpartyId, t.Quantity, t.Price, t.Quantity * t.Price,
    "—", "—", "—", "—", "—", "—", "—", "—"));

if (useInstrument)
{
    var table = builder.Input<Instrument>("instruments");
    enriched = enriched.IndexBy(e => e.InstrumentId)
        .Join(table.IndexBy(i => i.Id),
            (_, e, i) => e with { Symbol = i.Symbol, AssetClass = i.AssetClass, Currency = i.Currency });
}
if (useDesk)
{
    var traderTable = builder.Input<TraderRow>("traders");
    var deskTable = builder.Input<Desk>("desks");
    enriched = enriched.IndexBy(e => e.TraderId)
        .Join(traderTable.IndexBy(t => t.Id), (_, e, t) => (Trade: e, t.DeskId, t.Name))
        .IndexBy(x => x.DeskId)
        .Join(deskTable.IndexBy(d => d.Id),
            (_, x, d) => x.Trade with { Trader = x.Name, Desk = d.Name, Region = d.Region });
}
if (useCpty)
{
    var table = builder.Input<Counterparty>("counterparties");
    enriched = enriched.IndexBy(e => e.CounterpartyId)
        .Join(table.IndexBy(c => c.Id), (_, e, c) => e with { Counterparty = c.Name, Rating = c.Rating });
}

const char KeySep = '\u001f'; // unit separator: cannot occur in any dimension value
if (blotterMode)
{
    enriched.IndexBy(_ => 0).TopK(MaxBlotterRows, e => e.Notional).Output("blotter");
}
else
{
    var selectors = dims.Select(d => d.Select).ToArray();
    enriched
        .IndexBy(e => string.Join(KeySep, selectors.Select(s => s(e))))
        .AggregateLinear(new TradeStatsAggregate())
        .IndexBy(_ => 0)
        .TopK(MaxGroupRows, s => s.Value.Notional)
        .Output("groups");
}

await using var circuit = builder.Build();
circuit.Start();

var tradeInput = circuit.Input<Trade>("trades");
if (useInstrument)
    await circuit.Input<Instrument>("instruments").SendAsync(instruments.Select(i => new WeightedValue<Instrument>(i, 1)));
if (useDesk)
{
    await circuit.Input<TraderRow>("traders").SendAsync(traders.Select(t => new WeightedValue<TraderRow>(t, 1)));
    await circuit.Input<Desk>("desks").SendAsync(desks.Select(d => new WeightedValue<Desk>(d, 1)));
}
if (useCpty)
    await circuit.Input<Counterparty>("counterparties").SendAsync(counterparties.Select(c => new WeightedValue<Counterparty>(c, 1)));
await circuit.StepAsync();

var blotterOut = blotterMode ? circuit.Output<KeyedValue<int, EnrichedTrade>>("blotter") : null;
var groupsOut = blotterMode ? null : circuit.Output<KeyedValue<int, KeyedValue<string, (long Trades, double Notional)>>>("groups");
var blotterView = new ZSet<KeyedValue<int, EnrichedTrade>>();
var groupsView = new ZSet<KeyedValue<int, KeyedValue<string, (long Trades, double Notional)>>>();

long DrainOutputs()
{
    long rows = 0;
    if (blotterOut is not null)
        while (blotterOut.Reader.TryRead(out var d)) { blotterView.AddAll(d); rows += d.DistinctCount; }
    if (groupsOut is not null)
        while (groupsOut.Reader.TryRead(out var d)) { groupsView.AddAll(d); rows += d.DistinctCount; }
    return rows;
}

// ── run: trades churn until keypress, master data shifts underneath ─────────────

AnsiConsole.MarkupLine(interactive
    ? "[bold]Streaming — press any key to stop.[/]"
    : "[grey]non-interactive input: stopping after 120 ticks[/]");

var rng = new Random(11);
long nextTradeId = 1;
var livePool = new List<Trade>();
long tradesInserted = 0, tradesCancelled = 0, tradesAmended = 0;
long stepCount = 0, stepTicks = 0;
double lastStepMicros = 0;
var notices = new Queue<string>();
void Notice(string text)
{
    notices.Enqueue(text);
    while (notices.Count > 3)
        notices.Dequeue();
}

async Task<(double Micros, long DeltaRows)> StepTimedAsync()
{
    var timer = Stopwatch.StartNew();
    await circuit.StepAsync();
    timer.Stop();
    var deltaRows = DrainOutputs();
    stepCount++;
    stepTicks += timer.Elapsed.Ticks;
    lastStepMicros = timer.Elapsed.TotalMicroseconds;
    return (timer.Elapsed.TotalMicroseconds, deltaRows);
}

Trade RandomTrade()
{
    var instrument = instruments[rng.Next(instruments.Count)];
    var qty = (long)Math.Pow(10, 2 + 3 * rng.NextDouble());
    var price = instrument.BasePrice * (1 + (rng.NextDouble() - 0.5) * 0.02);
    return new Trade(nextTradeId++, rng.NextDouble() < 0.5 ? "BUY" : "SELL",
        instrument.Id, traders[rng.Next(traders.Count)].Id, counterparties[rng.Next(counterparties.Count)].Id,
        qty, Math.Round(price, 4));
}

var ticks = 0;
await AnsiConsole.Live(BuildLiveView()).AutoClear(false).StartAsync(async ctx =>
{
    while (interactive || ticks < 120)
    {
        if (interactive && Console.KeyAvailable)
        {
            Console.ReadKey(intercept: true);
            break;
        }
        ticks++;

        // A burst of new trades, the odd amendment, the odd cancellation.
        for (var i = 0; i < 8; i++)
        {
            var trade = RandomTrade();
            livePool.Add(trade);
            tradesInserted++;
            await tradeInput.InsertAsync(trade);
        }
        if (livePool.Count > 50 && rng.NextDouble() < 0.35)
        {
            var victim = livePool[rng.Next(livePool.Count)];
            await tradeInput.RemoveAsync(victim);
            if (rng.NextDouble() < 0.5)
            {
                livePool.Remove(victim);
                tradesCancelled++;
            }
            else
            {
                var amended = victim with { Quantity = Math.Max(1, victim.Quantity / 2) };
                livePool[livePool.IndexOf(victim)] = amended;
                await tradeInput.InsertAsync(amended);
                tradesAmended++;
            }
        }
        await StepTimedAsync();

        // Roughly every 8 seconds, reference data itself changes.
        if (ticks % 40 == 0 && (useDesk || useCpty))
        {
            if (useDesk && (!useCpty || rng.NextDouble() < 0.6))
            {
                var trader = traders[rng.Next(traders.Count)];
                var newDesk = desks[rng.Next(desks.Count)];
                if (newDesk.Id != trader.DeskId)
                {
                    var oldDesk = desks.First(d => d.Id == trader.DeskId).Name;
                    var moved = trader with { DeskId = newDesk.Id };
                    traders[traders.IndexOf(trader)] = moved;
                    var input = circuit.Input<TraderRow>("traders");
                    await input.RemoveAsync(trader);
                    await input.InsertAsync(moved);
                    var (micros, deltaRows) = await StepTimedAsync();
                    Notice($"[yellow]master data:[/] {trader.Name} moved {oldDesk} → {newDesk.Name} — " +
                           $"historical trades re-attributed in [bold]{micros:N0} µs[/] ({deltaRows} delta rows)");
                }
            }
            else if (useCpty)
            {
                var cpty = counterparties[rng.Next(counterparties.Count)];
                var newRating = ratings[rng.Next(ratings.Length)];
                if (newRating != cpty.Rating)
                {
                    var changed = cpty with { Rating = newRating };
                    counterparties[counterparties.IndexOf(cpty)] = changed;
                    var input = circuit.Input<Counterparty>("counterparties");
                    await input.RemoveAsync(cpty);
                    await input.InsertAsync(changed);
                    var (micros, deltaRows) = await StepTimedAsync();
                    Notice($"[yellow]master data:[/] {cpty.Name} re-rated {cpty.Rating} → {newRating} — " +
                           $"trades re-attributed in [bold]{micros:N0} µs[/] ({deltaRows} delta rows)");
                }
            }
        }

        ctx.UpdateTarget(BuildLiveView());
        await Task.Delay(100);
    }
});

await circuit.DisposeAsync();
AnsiConsole.MarkupLine(
    $"[grey]processed[/] {tradesInserted:N0} trades ({tradesAmended:N0} amended, {tradesCancelled:N0} cancelled) " +
    $"in {stepCount:N0} steps, avg [bold]{TimeSpan.FromTicks(stepTicks / Math.Max(1, stepCount)).TotalMicroseconds:N0} µs[/] per step");
return;

// ────────────────────────────────────────────────────────────────────────────────

IRenderable BuildLiveView()
{
    var avgMicros = stepCount == 0 ? 0 : TimeSpan.FromTicks(stepTicks / stepCount).TotalMicroseconds;
    var renderables = new List<IRenderable>
    {
        new Markup(
            $"[grey]trades[/] [bold]{tradesInserted,8:N0}[/]   [grey]live[/] {livePool.Count,7:N0}   " +
            $"[grey]amended[/] {tradesAmended,5:N0}   [grey]cancelled[/] {tradesCancelled,5:N0}   " +
            $"[grey]step[/] [bold green]{lastStepMicros,7:N0} µs[/]   [grey]avg[/] {avgMicros,7:N0} µs"),
    };
    foreach (var notice in notices.Reverse())
        renderables.Add(new Markup(notice));
    renderables.Add(blotterMode ? BuildBlotterTable() : BuildGroupTable());
    return new Rows(renderables);
}

IRenderable BuildGroupTable()
{
    var table = new Table().Border(TableBorder.Rounded);
    foreach (var (name, _) in dims)
        table.AddColumn(new TableColumn(name.ToLowerInvariant()).Width(16));
    table.AddColumn(new TableColumn("trades").RightAligned().Width(8));
    table.AddColumn(new TableColumn("notional").RightAligned().Width(10));
    table.AddColumn(new TableColumn("avg size").RightAligned().Width(10));

    foreach (var entry in groupsView.OrderByDescending(e => e.Value.Value.Value.Notional).Take(MaxGroupRows))
    {
        var (trades, notional) = entry.Value.Value.Value;
        var parts = entry.Value.Value.Key.Split(KeySep);
        var cells = parts.Select(p => Markup.Escape(Truncate(p, 16))).ToList();
        cells.Add($"{trades:N0}");
        cells.Add(Money(notional));
        cells.Add(Money(trades == 0 ? 0 : notional / trades));
        table.AddRow(cells.ToArray());
    }
    return table;
}

IRenderable BuildBlotterTable()
{
    var table = new Table().Border(TableBorder.Rounded).Title($"[bold]top {MaxBlotterRows} trades by notional[/]");
    table.AddColumn(new TableColumn("id").RightAligned().Width(7));
    table.AddColumn(new TableColumn("side").Width(4));
    if (useInstrument) table.AddColumn(new TableColumn("symbol").Width(8));
    table.AddColumn(new TableColumn("qty").RightAligned().Width(9));
    table.AddColumn(new TableColumn("price").RightAligned().Width(10));
    table.AddColumn(new TableColumn("notional").RightAligned().Width(10));
    if (useDesk) table.AddColumn(new TableColumn("desk").Width(14));
    if (useCpty) table.AddColumn(new TableColumn("counterparty").Width(17));

    foreach (var entry in blotterView.OrderByDescending(e => e.Value.Value.Notional).Take(MaxBlotterRows))
    {
        var t = entry.Value.Value;
        var cells = new List<string> { $"{t.Id:N0}", t.Side };
        if (useInstrument) cells.Add(Markup.Escape(t.Symbol));
        cells.Add($"{t.Quantity:N0}");
        cells.Add($"{t.Price:N4}");
        cells.Add(Money(t.Notional));
        if (useDesk) cells.Add(Markup.Escape(Truncate(t.Desk, 14)));
        if (useCpty) cells.Add(Markup.Escape(Truncate($"{t.Counterparty} ({t.Rating})", 17)));
        table.AddRow(cells.ToArray());
    }
    return table;
}

static string Money(double value) => Math.Abs(value) switch
{
    >= 1e9 => $"${value / 1e9:N1}B",
    >= 1e6 => $"${value / 1e6:N1}M",
    >= 1e3 => $"${value / 1e3:N1}K",
    _ => $"${value:N0}",
};

static string Truncate(string value, int width) =>
    value.Length <= width ? value : value[..(width - 1)] + "…";

/// <summary>Custom linear aggregate: trade count and notional maintained together in one
/// accumulator per group — folded from deltas, never recomputed.</summary>
sealed class TradeStatsAggregate : ILinearAggregate<EnrichedTrade, (long Trades, double Notional)>
{
    public (long Trades, double Notional) Zero => (0, 0);

    public (long Trades, double Notional) Map(EnrichedTrade trade, long weight) =>
        (weight, trade.Notional * weight);

    public (long Trades, double Notional) Combine((long Trades, double Notional) a, (long Trades, double Notional) b) =>
        (a.Trades + b.Trades, a.Notional + b.Notional);
}

record Trade(long Id, string Side, int InstrumentId, int TraderId, int CounterpartyId, long Quantity, double Price);

record Instrument(int Id, string Symbol, string AssetClass, string Currency, double BasePrice);

record TraderRow(int Id, string Name, int DeskId);

record Desk(int Id, string Name, string Region);

record Counterparty(int Id, string Name, string Rating);

record EnrichedTrade(
    long Id, string Side, int InstrumentId, int TraderId, int CounterpartyId,
    long Quantity, double Price, double Notional,
    string Symbol, string AssetClass, string Currency,
    string Trader, string Desk, string Region,
    string Counterparty, string Rating);
