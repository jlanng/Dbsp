using System.Text.Json;
using System.Text.Json.Nodes;
using Enrichment.Contracts;

namespace Incremental.EnrichmentService;

/// <summary>
/// What the pipelines should have delivered, computed without them: the smoke test tells
/// the model everything it does to the world — trades published, reference rows changed,
/// versions deployed, state reset — and compares each sink with what the model expects.
/// It is the specification of the three definitions, written as plain loops.
/// </summary>
public sealed class Model(WorldShape shape)
{
    public Dictionary<int, Instrument> Instruments { get; } = [];
    public Dictionary<string, Counterparty> Counterparties { get; } = new(StringComparer.Ordinal);
    public Dictionary<int, Trader> Traders { get; } = [];
    public Dictionary<int, Desk> Desks { get; } = [];
    public Dictionary<string, EnrichmentModel> Enrichments { get; } = new(StringComparer.Ordinal);
    public ExposureModel? Exposure { get; set; }
    public long Trades { get; private set; }

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
            Exposure?.Arrive(this, trade);
        }
        Trades += count;
    }

    /// <summary>A retroactive join: every row the instrument prices is emitted again.</summary>
    public void Change(Instrument instrument)
    {
        var changed = !Instruments.TryGetValue(instrument.Id, out var old) || old.Multiplier != instrument.Multiplier || old.Symbol != instrument.Symbol;
        Instruments[instrument.Id] = instrument;
        if (changed)
        {
            foreach (var enrichment in Enrichments.Values)
                enrichment.Reemit(row => row.Trade.InstrumentId == instrument.Id);
        }
    }

    /// <summary>A retroactive join, through the derived table: the trader's rows move desk.</summary>
    public void Change(Trader trader)
    {
        Traders[trader.Id] = trader;
        foreach (var enrichment in Enrichments.Values)
            enrichment.Reemit(row => row.Trade.TraderId == trader.Id);
    }

    /// <summary>A lookup: nothing already delivered changes.</summary>
    public void Change(Counterparty counterparty) => Counterparties[counterparty.Id] = counterparty;

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

/// <summary>One deployment of the trade-enrichment definition.</summary>
public sealed class EnrichmentModel(decimal minNotional)
{
    /// <summary>What the pipeline holds per trade id: the trade, the counterparty it met on
    /// arrival, and which version of the definition last emitted its row.</summary>
    public sealed record Row(Trade Trade, Counterparty? AtArrival, bool EmittedByV2);

    private readonly Dictionary<long, Row> _rows = [];

    /// <summary>Whether version 2 of the definition is running.</summary>
    public bool V2 { get; set; }

    public void Arrive(Model model, Trade trade) =>
        _rows[trade.Id] = new Row(trade, model.Counterparties.GetValueOrDefault(trade.CounterpartyId), V2);

    public void Reemit(Func<Row, bool> where)
    {
        foreach (var (id, row) in _rows.Where(r => where(r.Value)).ToList())
            _rows[id] = row with { EmittedByV2 = V2 };
    }

    public void Reset(Model model)
    {
        _rows.Clear();
        foreach (var trade in model.Replay())
            Arrive(model, trade);
    }

    public Dictionary<string, JsonObject> Expected(Model model)
    {
        var expected = new Dictionary<string, JsonObject>(StringComparer.Ordinal);
        foreach (var (id, row) in _rows)
        {
            var instrument = model.Instruments[row.Trade.InstrumentId];
            var notional = row.Trade.Quantity * row.Trade.Price * instrument.Multiplier;
            if (notional < minNotional)
                continue;                                               // the filter
            var desk = model.TraderDesk(row.Trade.TraderId);
            var json = new JsonObject
            {
                ["tradeId"] = id,
                ["symbol"] = instrument.Symbol,
                ["counterparty"] = row.AtArrival?.Name ?? "(unknown)",
                ["rating"] = row.AtArrival?.Rating ?? "NR",
                ["trader"] = desk.Trader,
                ["desk"] = desk.Desk,
                ["region"] = desk.Region,
                ["notional"] = notional,
            };
            if (row.EmittedByV2)
                json["size"] = notional >= 1_000_000m ? "block" : "regular";
            expected[id.ToString()] = json;
        }
        return expected;
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
        foreach (var trade in model.Replay())
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
