using System.Text.Json;
using System.Text.Json.Nodes;
using Enrichment.Contracts;

namespace Incremental.EnrichmentService;

/// <summary>
/// What the pipelines should have delivered, computed without them: the smoke test tells
/// the model everything it does to the world — trades published, reference rows changed,
/// versions deployed, state reset — and compares each sink with what the model expects.
/// It is the specification of the five definitions, written as plain loops.
/// </summary>
public sealed class Model(WorldShape shape)
{
    public Dictionary<int, Instrument> Instruments { get; } = [];
    public Dictionary<string, Counterparty> Counterparties { get; } = new(StringComparer.Ordinal);
    public Dictionary<int, Trader> Traders { get; } = [];
    public Dictionary<int, Desk> Desks { get; } = [];
    public Dictionary<string, EnrichmentModel> Enrichments { get; } = new(StringComparer.Ordinal);
    public ExposureModel? Exposure { get; set; }
    public QualityModel? Quality { get; set; }
    public Dictionary<string, AlertsModel> Alerts { get; } = new(StringComparer.Ordinal);
    public long Trades { get; private set; }

    /// <summary>How many intervals of quotes have been published.</summary>
    public long QuoteIntervals { get; set; }

    public WorldShape Shape => shape;

    /// <summary>The published trades no desk could have booked: no quantity.</summary>
    public IEnumerable<Trade> Poison => Replay().Where(t => t.Quantity <= 0);

    public TraderDesk TraderDesk(int traderId)
    {
        var trader = Traders[traderId];
        var desk = Desks[trader.DeskId];
        return new TraderDesk(trader.Id, trader.Name, desk.Id, desk.Name, desk.Region);
    }

    /// <summary>Trades <c>Trades … Trades + count - 1</c> of the feed arrive, in order.</summary>
    public void Arrive(long count)
    {
        for (var offset = Trades; offset < Trades + count; offset++)
        {
            var trade = shape.TradeAt(offset);
            foreach (var enrichment in Enrichments.Values)
                enrichment.Arrive(this, trade);
            // A straggler is behind the watermark of the pipelines that keep event time:
            // they drop it. The others carry it like any execution.
            if (!shape.Stragglers.Contains(offset))
                Exposure?.Arrive(this, trade);
            foreach (var alerts in Alerts.Values)
                alerts.Arrive(trade);
        }
        Trades += count;
    }

    /// <summary>A retroactive join: every open row the instrument prices is emitted again.</summary>
    public void Change(Instrument instrument)
    {
        var changed = !Instruments.TryGetValue(instrument.Id, out var old) || old.Multiplier != instrument.Multiplier || old.Symbol != instrument.Symbol;
        Instruments[instrument.Id] = instrument;
        if (changed)
        {
            foreach (var enrichment in Enrichments.Values)
                enrichment.Reemit(this, row => row.Trade.InstrumentId == instrument.Id);
        }
    }

    /// <summary>A retroactive join, through the derived table: the trader's open rows move desk.</summary>
    public void Change(Trader trader)
    {
        Traders[trader.Id] = trader;
        foreach (var enrichment in Enrichments.Values)
            enrichment.Reemit(this, row => row.Trade.TraderId == trader.Id);
    }

    /// <summary>A lookup: nothing already delivered changes.</summary>
    public void Change(Counterparty counterparty) => Counterparties[counterparty.Id] = counterparty;

    /// <summary>The published trades an event-time pipeline counts: all but the stragglers,
    /// which arrived behind its watermark. (True of a pipeline that read the feed as it was
    /// published. One reset afterwards reads the whole feed in a few steps, with no
    /// watermark yet to be behind — so the smoke test publishes its straggler last.)</summary>
    public IEnumerable<Trade> OnTime()
    {
        for (long offset = 0; offset < Trades; offset++)
            if (!shape.Stragglers.Contains(offset))
                yield return shape.TradeAt(offset);
    }

    /// <summary>A pipeline reset reads its input again from the start, against the tables as they are now.</summary>
    public IEnumerable<Trade> Replay()
    {
        for (long offset = 0; offset < Trades; offset++)
            yield return shape.TradeAt(offset);
    }

    public Dictionary<string, JsonObject> ExpectedTraderDesks() =>
        Traders.Keys.ToDictionary(id => id.ToString(), id => (JsonObject)JsonSerializer.SerializeToNode(TraderDesk(id), JsonSerializerOptions.Web)!);

    // ── comparing JSON ─────────────────────────────────────────────────────────────────

    /// <summary>Null when <paramref name="actual"/> holds exactly the expected rows;
    /// otherwise the first difference, in words.</summary>
    public static string? Difference(Dictionary<string, JsonObject> expected, IReadOnlyDictionary<string, string> actual)
    {
        foreach (var (key, row) in expected)
        {
            if (!actual.TryGetValue(key, out var json))
                return $"key {key} is missing ({actual.Count} rows, {expected.Count} expected)";
            if (!Same(JsonNode.Parse(row.ToJsonString()), JsonNode.Parse(json)))   // both as parsed text
                return $"key {key} is {json}, expected {row.ToJsonString()}";
        }
        if (actual.Count != expected.Count)
            return $"key {actual.Keys.First(k => !expected.ContainsKey(k))} should not be there ({actual.Count} rows, {expected.Count} expected)";
        return null;
    }

    // Numbers are compared as numbers (1.50 is 1.5) and instants as instants; a member that
    // is absent is not the same as one that is null.
    private static bool Same(JsonNode? a, JsonNode? b)
    {
        if (a is null || b is null)
            return a is null && b is null;
        if (a is JsonObject x && b is JsonObject y)
            return x.Count == y.Count && x.All(p => y.TryGetPropertyValue(p.Key, out var other) && Same(p.Value, other));
        if (a is JsonArray s && b is JsonArray t)
            return s.Count == t.Count && s.Zip(t).All(p => Same(p.First, p.Second));
        if (a is not JsonValue || b is not JsonValue)
            return false;
        var (left, right) = (a.GetValueKind(), b.GetValueKind());
        if (left != right)
            return false;
        if (left == JsonValueKind.Number)
            return decimal.Parse(a.ToJsonString(), System.Globalization.CultureInfo.InvariantCulture)
                   == decimal.Parse(b.ToJsonString(), System.Globalization.CultureInfo.InvariantCulture);
        if (left == JsonValueKind.String)
        {
            var (p, q) = (a.GetValue<string>(), b.GetValue<string>());
            return p == q || (p.Length > 18 && DateTimeOffset.TryParse(p, out var m) && DateTimeOffset.TryParse(q, out var n) && m == n);
        }
        return true;
    }
}

/// <summary>
/// One deployment of the trade-enrichment definition. A trade is open while its execution
/// time is at or after the pipeline's watermark — the newest execution time it has read,
/// less <c>openSeconds</c> — and final after: a change of reference data re-emits only the
/// open trades, a final trade's row stays as it was last emitted, and a trade that arrives
/// behind the watermark (a straggler) is dropped. The smoke test changes reference data
/// only once the pipeline has read everything published, so the watermark is the model's.
/// <para>
/// The sink is an upsert by trade id, and it is modelled as one: a later trade under an id
/// whose trade is final retracts nothing, so if the new trade is filtered out the old row
/// stays. (The smoke test never publishes two trades of one id within one step: it
/// publishes fewer than the 1,500 ids at a time and waits.)
/// </para>
/// </summary>
public sealed class EnrichmentModel(decimal minNotional, TimeSpan open)
{
    /// <summary>What the pipeline last emitted per trade id: the trade, the counterparty it
    /// met on arrival, the instrument and the trader's desk it was last joined to, and which
    /// version of the definition emitted it.</summary>
    public sealed record Row(Trade Trade, Counterparty? AtArrival, bool EmittedByV2, Instrument Instrument, TraderDesk Desk);

    private readonly Dictionary<long, Row> _rows = [];
    private readonly Dictionary<string, JsonObject> _sink = new(StringComparer.Ordinal);
    private long _newest = long.MinValue;

    /// <summary>Whether version 2 of the definition is running.</summary>
    public bool V2 { get; set; }

    /// <summary>The watermark: trades executed before it are final.</summary>
    public DateTimeOffset? Watermark => _newest == long.MinValue ? null : new DateTimeOffset(_newest, TimeSpan.Zero) - open;

    public bool IsOpen(Row row) => Watermark is not { } watermark || row.Trade.ExecutedAt >= watermark;

    public void Arrive(Model model, Trade trade)
    {
        if (Watermark is { } watermark && trade.ExecutedAt < watermark)
            return;                                                     // late: dropped, and a final trade under its id stays
        // An open trade amended: its row is taken back — if the filter let it through.
        var amends = _rows.TryGetValue(trade.Id, out var before) && IsOpen(before) && Json(before) is not null;
        var row = _rows[trade.Id] = new Row(trade, model.Counterparties.GetValueOrDefault(trade.CounterpartyId), V2,
            model.Instruments[trade.InstrumentId], model.TraderDesk(trade.TraderId));
        Emit(row, retracts: amends);
        _newest = Math.Max(_newest, trade.ExecutedAt.UtcTicks);
    }

    /// <summary>A retroactive join: the open rows <paramref name="where"/> selects are
    /// emitted again from the tables as they are now.</summary>
    public void Reemit(Model model, Func<Row, bool> where)
    {
        foreach (var (id, row) in _rows.Where(r => where(r.Value) && IsOpen(r.Value)).ToList())
            Emit(_rows[id] = row with
            {
                EmittedByV2 = V2, Instrument = model.Instruments[row.Trade.InstrumentId], Desk = model.TraderDesk(row.Trade.TraderId),
            }, retracts: Json(row) is not null);
    }

    /// <summary>What the sink does with a row the pipeline emits: holds it under its id if
    /// it passes the filter; if it does not, deletes the id only when the row it replaces
    /// was delivered and is taken back (an open trade amended or corrected).</summary>
    private void Emit(Row row, bool retracts)
    {
        var key = row.Trade.Id.ToString();
        if (Json(row) is { } json)
            _sink[key] = json;
        else if (retracts)
            _sink.Remove(key);
    }

    /// <summary>The rows <paramref name="where"/> selects that would be delivered, open and final.</summary>
    public (int Open, int Final) Count(Func<Row, bool> where)
    {
        var rows = _rows.Values.Where(r => where(r) && r.Trade.Quantity * r.Trade.Price * r.Instrument.Multiplier >= minNotional).ToList();
        return (rows.Count(IsOpen), rows.Count(r => !IsOpen(r)));
    }

    /// <summary>The instrument of the newest trade: one with an open trade in it.</summary>
    public int OpenInstrument() => _rows.Values.MaxBy(r => r.Trade.ExecutedAt)!.Trade.InstrumentId;

    /// <summary>Trades the pipeline holds state for: those still open.</summary>
    public int OpenTrades => _rows.Values.Count(IsOpen);

    public Dictionary<string, JsonObject> Expected() => new(_sink, StringComparer.Ordinal);

    /// <summary>The row the pipeline emits for a trade, or null when the filter drops it.</summary>
    private JsonObject? Json(Row row)
    {
        var notional = row.Trade.Quantity * row.Trade.Price * row.Instrument.Multiplier;
        if (notional < minNotional)
            return null;                                                // the filter
        var json = new JsonObject
        {
            ["tradeId"] = row.Trade.Id,
            ["symbol"] = row.Instrument.Symbol,
            ["counterparty"] = row.AtArrival?.Name ?? "(unknown)",
            ["rating"] = row.AtArrival?.Rating ?? "NR",
            ["trader"] = row.Desk.Trader,
            ["desk"] = row.Desk.Desk,
            ["region"] = row.Desk.Region,
            ["notional"] = notional,
        };
        if (row.EmittedByV2)
            json["size"] = notional >= 1_000_000m ? "block" : "regular";
        return json;
    }
}

/// <summary>The desk-exposure definition: every execution counted once, in the window of
/// its execution time, under the group its trader's desk gave it on arrival.</summary>
public sealed class ExposureModel(TimeSpan window)
{
    private readonly List<(string Group, DateTimeOffset At, decimal Notional)> _executions = [];

    /// <summary>Version 2 groups by region.</summary>
    public bool ByRegion { get; set; }

    public void Arrive(Model model, Trade trade)
    {
        var desk = model.TraderDesk(trade.TraderId);
        _executions.Add((ByRegion ? desk.Region : desk.Desk, trade.ExecutedAt, trade.Quantity * trade.Price));
    }

    public void Reset(Model model)
    {
        _executions.Clear();
        foreach (var trade in model.OnTime())
            Arrive(model, trade);
    }

    public Dictionary<string, JsonObject> Expected()
    {
        var expected = new Dictionary<string, JsonObject>(StringComparer.Ordinal);
        foreach (var group in _executions.GroupBy(e => (e.Group, Start: new DateTimeOffset(e.At.UtcTicks - e.At.UtcTicks % window.Ticks, TimeSpan.Zero))))
        {
            var key = $"{group.Key.Group}@{group.Key.Start.UtcDateTime:yyyy-MM-ddTHH:mm:ss}Z";
            expected[JsonSerializer.Serialize(key, JsonSerializerOptions.Web)] = new JsonObject
            {
                ["group"] = group.Key.Group,
                ["windowStart"] = group.Key.Start,
                ["windowEnd"] = group.Key.Start + window,
                ["notional"] = group.Sum(e => e.Notional),
            };
        }
        return expected;
    }
}

/// <summary>
/// The execution-quality definition: every execution is priced by the quote of its
/// instrument and interval if there is one, and counted — priced or unpriced — in every
/// sliding window that covers it. A function of what has been published and nothing else:
/// which input the pipeline read first, and whether it has been restarted, do not enter.
/// </summary>
public sealed class QualityModel(TimeSpan window, TimeSpan hop)
{
    public (long Priced, long Unpriced) Counts(Model model)
    {
        long priced = 0, unpriced = 0;
        foreach (var trade in model.OnTime())
            _ = Slippage(model, trade) is null ? unpriced++ : priced++;
        return (priced, unpriced);
    }

    private static decimal? Slippage(Model model, Trade trade)
    {
        var interval = model.Shape.IntervalOf(trade.ExecutedAt);
        if (interval >= model.QuoteIntervals || !model.Shape.Quoted(trade.InstrumentId, interval))
            return null;                                                // the left join's unmatched side
        var mid = model.Shape.Mid(trade.InstrumentId, interval);
        return Math.Round((trade.Price - mid) / mid * 10_000m, 2);
    }

    public Dictionary<string, JsonObject> Expected(Model model)
    {
        var totals = new Dictionary<(string AssetClass, long Start), (long Priced, long Unpriced, decimal Bps)>();
        foreach (var trade in model.OnTime())
        {
            var assetClass = model.Instruments[trade.InstrumentId].AssetClass;
            var slippage = Slippage(model, trade);
            var at = trade.ExecutedAt.UtcTicks;
            // Windows start at multiples of the hop; those that cover the execution.
            for (var start = at - at % hop.Ticks; start > at - window.Ticks; start -= hop.Ticks)
            {
                var held = totals.GetValueOrDefault((assetClass, start));
                totals[(assetClass, start)] = slippage is { } bps
                    ? (held.Priced + 1, held.Unpriced, held.Bps + bps)
                    : (held.Priced, held.Unpriced + 1, held.Bps);
            }
        }
        var expected = new Dictionary<string, JsonObject>(StringComparer.Ordinal);
        foreach (var ((assetClass, startTicks), total) in totals)
        {
            var start = new DateTimeOffset(startTicks, TimeSpan.Zero);
            var key = $"{assetClass}@{start.UtcDateTime:yyyy-MM-ddTHH:mm:ss}Z";
            expected[JsonSerializer.Serialize(key, JsonSerializerOptions.Web)] = new JsonObject
            {
                ["assetClass"] = assetClass,
                ["windowStart"] = start,
                ["windowEnd"] = start + window,
                ["priced"] = total.Priced,
                ["unpriced"] = total.Unpriced,
                ["slippageBps"] = total.Bps,
            };
        }
        return expected;
    }
}

/// <summary>One deployment of the watchlist-alerts definition: an alert for each sizeable
/// execution whose counterparty was on its list when the execution arrived.</summary>
public sealed class AlertsModel(Dictionary<string, WatchlistEntry> watchlist, decimal minNotional, string severity)
{
    private readonly Dictionary<string, JsonObject> _alerts = new(StringComparer.Ordinal);

    public Dictionary<string, WatchlistEntry> Watchlist => watchlist;

    public static string Key(long tradeId, DateTimeOffset executedAt) => $"{tradeId}@{executedAt.UtcTicks}";

    public void Arrive(Trade trade)
    {
        if (trade.Quantity <= 0)
            return;                                                     // the definition throws: a dead letter
        var notional = trade.Quantity * trade.Price;
        if (notional < minNotional || !watchlist.TryGetValue(trade.CounterpartyId, out var entry))
            return;
        _alerts[Key(trade.Id, trade.ExecutedAt)] = new JsonObject
        {
            ["tradeId"] = trade.Id,
            ["executedAt"] = trade.ExecutedAt,
            ["counterpartyId"] = entry.CounterpartyId,
            ["reason"] = entry.Reason,
            ["notional"] = notional,
            ["severity"] = severity,
        };
    }

    public Dictionary<string, JsonObject> Expected() => _alerts;
}
