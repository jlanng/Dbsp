using Acme.Contracts;
using AcmeTables = Acme.Contracts.Tables;

namespace Incremental.Service.Tests;

// Pipeline definitions compiled into the test host (AddPlugin): the supervisor and
// registry tests use these; the plugin tests load real assemblies instead.

[Pipeline("enrich", Description = "Trades joined to the instruments table.")]
public sealed class Enrich : IPipelineDefinition<Enrich.Settings>
{
    public sealed record Settings
    {
        [System.ComponentModel.DataAnnotations.Range(0, 1e9)]
        public decimal MinQuantity { get; init; }

        [StateNeutral]
        public string? Label { get; init; }
    }

    public void Build(PipelineBuilder p, Settings s)
    {
        var instruments = p.Table(AcmeTables.Instruments);
        var enriched = p.Input<Trade>("trades")
            .Where(t => t.Quantity >= s.MinQuantity)
            .GroupBy(t => t.InstrumentId)
            .Join(instruments, (_, t, i) => new EnrichedTrade(t.Id, i.Symbol, t.Quantity * i.Multiplier)).Named("joined");
        p.UpsertSink("out", enriched, e => e.TradeId);
    }

    public static Dictionary<long, EnrichedTrade> Model(IEnumerable<Trade> trades, IEnumerable<Instrument> instruments, decimal minQuantity = 0)
    {
        var byId = instruments.ToDictionary(i => i.Id);
        return trades.Where(t => t.Quantity >= minQuantity && byId.ContainsKey(t.InstrumentId))
            .ToDictionary(t => t.Id, t => new EnrichedTrade(t.Id, byId[t.InstrumentId].Symbol, t.Quantity * byId[t.InstrumentId].Multiplier));
    }
}

[Pipeline("counts")]
public sealed class Counts : IPipelineDefinition
{
    public void Build(PipelineBuilder p) =>
        p.UpsertSink("out", p.Input<Trade>("trades").GroupBy(t => t.Trader).Count().Named("count"));

    public static Dictionary<string, long> Model(IEnumerable<Trade> trades) =>
        trades.GroupBy(t => t.Trader).ToDictionary(g => g.Key, g => g.LongCount());
}

/// <summary>Writes a derived table: the summed quantity per instrument.</summary>
[Pipeline("positions")]
public sealed class Positions : IPipelineDefinition
{
    public void Build(PipelineBuilder p) =>
        p.ToTable(
            AcmeTables.Positions,
            p.Input<Trade>("trades").GroupBy(t => t.InstrumentId).Sum(t => t.Quantity).Named("sum"),
            (instrument, quantity) => new Position(instrument, quantity));
}

/// <summary>Reads the derived table.</summary>
[Pipeline("position-report")]
public sealed class PositionReport : IPipelineDefinition
{
    public void Build(PipelineBuilder p) =>
        p.UpsertSink("out", p.Table(AcmeTables.Positions).Changes(), position => position.InstrumentId);
}

/// <summary>Throws on a trade with a negative quantity: a fault under the Fault policy, a
/// dead letter under DeadLetter.</summary>
[Pipeline("fragile")]
public sealed class Fragile : IPipelineDefinition
{
    public void Build(PipelineBuilder p) =>
        p.UpsertSink("out", p.Input<Trade>("trades")
            .Select(t => t.Quantity < 0 ? throw new InvalidOperationException($"negative quantity on trade {t.Id}") : t)
            .GroupBy(t => t.Trader).Count().Named("count"));
}

/// <summary>A delegate that does not return until the gate opens.</summary>
[Pipeline("wedge")]
public sealed class Wedge : IPipelineDefinition
{
    public static readonly ManualResetEventSlim Gate = new(initialState: false);

    public void Build(PipelineBuilder p) =>
        p.AppendSink("out", p.Input<Trade>("trades").Select(t =>
        {
            Gate.Wait();
            return t.Id;
        }));
}

/// <summary>Looks instruments up by a declared secondary index.</summary>
[Pipeline("by-isin")]
public sealed class ByIsin : IPipelineDefinition
{
    public void Build(PipelineBuilder p) =>
        p.UpsertSink("out",
            p.KeyedInput<string, IsinRequest>("requests", r => r.Isin)
                .Lookup(p.Table(AcmeTables.Instruments).By(InstrumentsTable.ByIsin), r => r.Isin, (r, i) => new IsinAnswer(r.Isin, i.Symbol)),
            a => a.Isin);
}

/// <summary>A definition whose constructor throws.</summary>
[Pipeline("broken-constructor")]
public sealed class BrokenConstructor : IPipelineDefinition
{
    public BrokenConstructor() => throw new NotSupportedException("the constructor's own message");

    public void Build(PipelineBuilder p) => p.AppendSink("out", p.Input<Trade>("trades"));
}

public sealed record IsinRequest(string Isin);

public sealed record IsinAnswer(string Isin, string Symbol);

/// <summary><c>counts</c> grouped by another key under the same operator name: the store
/// names are the same, so the state layout matches, and the committed keys (trader names)
/// cannot be read as the keys this declares (instrument ids). What no comparison of
/// layouts can see.</summary>
[Pipeline("counts-by-instrument")]
public sealed class CountsByInstrument : IPipelineDefinition
{
    public void Build(PipelineBuilder p) =>
        p.UpsertSink("out", p.Input<Trade>("trades").GroupBy(t => t.InstrumentId).Count().Named("count"));
}

/// <summary><c>counts</c> with its stateful operator under another name: the committed
/// store has no home in this circuit, which a comparison of layouts does see.</summary>
[Pipeline("counts-renamed")]
public sealed class CountsRenamed : IPipelineDefinition
{
    public void Build(PipelineBuilder p) =>
        p.UpsertSink("out", p.Input<Trade>("trades").GroupBy(t => t.Trader).Count().Named("count-v2"));
}

/// <summary>Trades above a quantity, as an append-only stream.</summary>
[Pipeline("big-trades")]
public sealed class BigTrades : IPipelineDefinition
{
    public const decimal Threshold = 4;

    public void Build(PipelineBuilder p) =>
        p.AppendSink("out", p.Input<Trade>("trades")
            .Select(t => t.Quantity < 0 ? throw new InvalidOperationException($"negative quantity on trade {t.Id}") : t)
            .Where(t => t.Quantity >= Threshold));

    public static List<Trade> Model(IEnumerable<Trade> trades) => trades.Where(t => t.Quantity >= Threshold).ToList();
}

/// <summary>The latest trade per id: a keyed input straight to an upsert sink.</summary>
[Pipeline("latest-trades")]
public sealed class LatestTrades : IPipelineDefinition
{
    public void Build(PipelineBuilder p) =>
        p.UpsertSink("out", p.KeyedInput<long, Trade>("trades", t => t.Id), t => t.Id);
}

public sealed record Fill(long Sequence, long TradeId, decimal Price);

public sealed record FilledTrade(long Sequence, long TradeId, decimal Notional);

/// <summary>Two inputs joined to each other, and a count over one of them that a record
/// read twice would show in.</summary>
[Pipeline("trade-fills")]
public sealed class TradeFills : IPipelineDefinition
{
    public void Build(PipelineBuilder p)
    {
        var trades = p.Input<Trade>("trades");
        var fills = p.Input<Fill>("fills");
        p.UpsertSink("filled",
            trades.GroupBy(t => t.Id).Join(fills.GroupBy(f => f.TradeId), (_, t, f) => new FilledTrade(f.Sequence, t.Id, t.Quantity * f.Price)).Named("filled"),
            f => f.Sequence);
        p.UpsertSink("fill-counts", fills.GroupBy(f => f.TradeId).Count().Named("fill-count"));
    }

    public static Dictionary<long, FilledTrade> Filled(IEnumerable<Trade> trades, IEnumerable<Fill> fills)
    {
        var byId = trades.ToDictionary(t => t.Id);
        return fills.Where(f => byId.ContainsKey(f.TradeId)).ToDictionary(f => f.Sequence, f => new FilledTrade(f.Sequence, f.TradeId, byId[f.TradeId].Quantity * f.Price));
    }

    public static Dictionary<long, long> Counts(IEnumerable<Fill> fills) => fills.GroupBy(f => f.TradeId).ToDictionary(g => g.Key, g => g.LongCount());
}

/// <summary>An event with a time of its own, in milliseconds after <see cref="Epoch"/>.</summary>
public sealed record Tick(long Id, string Trader, long AtMs, decimal Quantity)
{
    public static readonly DateTimeOffset Epoch = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    public DateTimeOffset At() => Epoch.AddMilliseconds(AtMs);
}

public sealed record WindowTotal(string Trader, long WindowStartMs, decimal Quantity);

/// <summary>Event-time state: a quantity per trader in tumbling windows, live and — once
/// the watermark closes a window — as a final. A tick with a negative quantity throws.</summary>
[Pipeline("windowed")]
public sealed class WindowedTotals : IPipelineDefinition
{
    public const int WindowMs = 10_000;
    public const int LatenessMs = 2_000;

    public void Build(PipelineBuilder p)
    {
        var totals = p.Input<Tick>("ticks")
            .Where(t => t.Quantity >= 0 ? true : throw new InvalidOperationException($"negative quantity on tick {t.Id}"))
            .WithEventTime(t => t.At(), TimeSpan.FromMilliseconds(LatenessMs))
            .GroupBy(t => t.Trader)
            .TumblingWindow(TimeSpan.FromMilliseconds(WindowMs))
            .Sum(t => t.Quantity).Named("totals");
        p.UpsertSink("live", totals.Rows().Select(Row), Key);
        p.UpsertSink("final", totals.OnClose().Named("finals").Rows().Select(Row), Key);
    }

    private static WindowTotal Row(WindowedValue<string, decimal> w) =>
        new(w.Key, (long)(w.WindowStart - Tick.Epoch).TotalMilliseconds, w.Value);

    public static string Key(WindowTotal total) => $"{total.Trader}@{total.WindowStartMs}";

    /// <summary>What the live sink holds for <paramref name="ticks"/>, none of them late.</summary>
    public static Dictionary<string, WindowTotal> Model(IEnumerable<Tick> ticks) =>
        ticks.GroupBy(t => (t.Trader, Start: t.AtMs - t.AtMs % WindowMs))
            .Select(g => new WindowTotal(g.Key.Trader, g.Key.Start, g.Sum(t => t.Quantity)))
            .ToDictionary(Key);
}

/// <summary>The windowed totals with event time declared on the input itself, so the clock
/// keeps a time per partition of the source and the checkpoint holds them — the state
/// that format level 2 added.</summary>
[Pipeline("windowed-by-partition")]
public sealed class WindowedByPartition : IPipelineDefinition
{
    public void Build(PipelineBuilder p)
    {
        var totals = p.Input<Tick>("ticks")
            .WithEventTime(t => t.At(), TimeSpan.FromMilliseconds(WindowedTotals.LatenessMs))
            .GroupBy(t => t.Trader)
            .TumblingWindow(TimeSpan.FromMilliseconds(WindowedTotals.WindowMs))
            .Sum(t => t.Quantity).Named("totals");
        p.UpsertSink("live", totals.Rows().Select(Row), WindowedTotals.Key);
        p.UpsertSink("final", totals.OnClose().Named("finals").Rows().Select(Row), WindowedTotals.Key);
    }

    private static WindowTotal Row(WindowedValue<string, decimal> w) =>
        new(w.Key, (long)(w.WindowStart - Tick.Epoch).TotalMilliseconds, w.Value);
}

/// <summary>The windowed totals again, over an input whose name starts as the clock's own
/// entries in the checkpoint do. It is an input like any other.</summary>
[Pipeline("windowed-oddly-named")]
public sealed class OddlyNamedInput : IPipelineDefinition
{
    public const string Input = "$watermark-feed";

    public void Build(PipelineBuilder p)
    {
        var totals = p.Input<Tick>(Input)
            .Where(t => t.Quantity >= 0 ? true : throw new InvalidOperationException($"negative quantity on tick {t.Id}"))
            .WithEventTime(t => t.At(), TimeSpan.FromMilliseconds(WindowedTotals.LatenessMs))
            .GroupBy(t => t.Trader)
            .TumblingWindow(TimeSpan.FromMilliseconds(WindowedTotals.WindowMs))
            .Sum(t => t.Quantity);
        p.UpsertSink("live", totals.Rows().Select(w => new WindowTotal(w.Key, (long)(w.WindowStart - Tick.Epoch).TotalMilliseconds, w.Value)), WindowedTotals.Key);
    }
}

/// <summary>A trade with a time of its own, for definitions that keep it open by event time.</summary>
public sealed record TimedTrade(long Id, int InstrumentId, string Trader, DateTimeOffset At);

/// <summary>The reference shape: a keyed input that forgets final rows and a retroactive
/// join that forgets final facts, both open for <c>OpenSeconds</c> of event time. With
/// <c>CountPerTrade</c> it also keeps a count per trade id, which nothing ever closes.</summary>
[Pipeline("open-trades")]
public sealed class OpenTrades : IPipelineDefinition<OpenTrades.Settings>
{
    public sealed record Settings
    {
        public int OpenSeconds { get; init; } = 3600;
        public bool CountPerTrade { get; init; }
    }

    public void Build(PipelineBuilder p, Settings s)
    {
        var instruments = p.Table(AcmeTables.Instruments);
        var trades = p.KeyedInput<long, TimedTrade>("trades", t => t.Id)
            .WithEventTime(t => t.At, TimeSpan.FromSeconds(s.OpenSeconds), evictWhenFinal: true);
        p.UpsertSink("out",
            trades.GroupBy(t => t.InstrumentId).EvictWhenFinal()
                .Join(instruments, (_, t, i) => new EnrichedTrade(t.Id, i.Symbol, i.Multiplier)).Named("priced"),
            e => e.TradeId);
        if (s.CountPerTrade)
            p.UpsertSink("seen", trades.GroupBy(t => t.Id).Count().Named("seen"));
    }
}
