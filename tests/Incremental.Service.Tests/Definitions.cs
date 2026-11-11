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
