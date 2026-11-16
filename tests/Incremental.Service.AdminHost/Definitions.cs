using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using Acme.Contracts;
using AcmeTables = Acme.Contracts.Tables;

namespace Incremental.Service.AdminHost;

// The pipelines compiled into the admin host. Their inputs are keyed by order id over a
// sliding id space, so a host left running holds a bounded amount of state.

public sealed record Order(long Id, int InstrumentId, string Counterparty, decimal Quantity, decimal Price, string Trader, string Desk);

public sealed record PricedOrder(long OrderId, string Symbol, string Counterparty, decimal Notional, string Trader, string Desk);

public sealed record EnrichedOrder(long OrderId, string Symbol, string CounterpartyName, string Country, string Tier, decimal Notional, string Trader);

public sealed record DeskExposure(string Desk, string Key, decimal Notional);

[Pipeline("enrich-orders", Description = "Orders joined to instruments (retroactive) and enriched with their counterparty on arrival.")]
public sealed class EnrichOrders : IPipelineDefinition<EnrichOrders.Settings>
{
    public sealed record Settings
    {
        [Range(0, 1e12), Description("Orders with a notional below this are dropped.")]
        public decimal MinNotional { get; init; }

        [StateNeutral, Description("A label for the operator; it changes no stored row.")]
        public string? Label { get; init; }
    }

    public void Build(PipelineBuilder p, Settings s)
    {
        var instruments = p.Table(AcmeTables.Instruments);
        var counterparties = p.Table(AcmeTables.Counterparties);
        var priced = p.KeyedInput<long, Order>("orders", o => o.Id)
            .GroupBy(o => o.InstrumentId)
            .Join(instruments, (_, o, i) => new PricedOrder(o.Id, i.Symbol, o.Counterparty, o.Quantity * o.Price * i.Multiplier, o.Trader, o.Desk))
            .Named("priced")
            .Where(o => o.Notional >= s.MinNotional);
        var enriched = priced.Lookup(
            counterparties, o => o.Counterparty,
            (o, c) => new EnrichedOrder(o.OrderId, o.Symbol, c.Name, c.Country, c.Tier, o.Notional, o.Trader),
            o => new EnrichedOrder(o.OrderId, o.Symbol, "(unknown)", "??", "none", o.Notional, o.Trader));
        p.UpsertSink("enriched", enriched, e => e.OrderId);
    }
}

[Pipeline("trader-activity", Description = "Orders and total quantity per trader.")]
public sealed class TraderActivity : IPipelineDefinition
{
    public void Build(PipelineBuilder p)
    {
        var orders = p.KeyedInput<long, Order>("orders", o => o.Id);
        p.UpsertSink("counts", orders.GroupBy(o => o.Trader).Count().Named("count"));
        p.UpsertSink("quantities", orders.GroupBy(o => o.Trader).Sum(o => o.Quantity).Named("quantity"));
    }
}

[Pipeline("positions", Description = "Net quantity per instrument, written to the shared positions table.")]
public sealed class PositionsPipeline : IPipelineDefinition
{
    public void Build(PipelineBuilder p) =>
        p.ToTable(
            AcmeTables.Positions,
            p.KeyedInput<long, Order>("orders", o => o.Id).GroupBy(o => o.InstrumentId).Sum(o => o.Quantity).Named("net"),
            (instrument, quantity) => new Position(instrument, quantity));
}

[Pipeline("position-report", Description = "The positions table joined back to instruments.")]
public sealed class PositionReport : IPipelineDefinition
{
    public sealed record Line(int InstrumentId, string Symbol, decimal Quantity);

    public void Build(PipelineBuilder p)
    {
        var lines = p.Table(AcmeTables.Positions).Changes()
            .Lookup(p.Table(AcmeTables.Instruments), x => x.InstrumentId, (x, i) => new Line(x.InstrumentId, i.Symbol, x.Quantity));
        p.UpsertSink("report", lines, l => l.InstrumentId);
    }
}

/// <summary>A wide circuit: one branch per desk, each priced, enriched and rolled up three
/// ways, then merged. Thirty-odd operators — what the topology view has to stay readable at.</summary>
[Pipeline("desk-risk", Description = "Exposure per desk by country, tier and symbol, and each desk's largest orders.")]
public sealed class DeskRisk : IPipelineDefinition<DeskRisk.Settings>
{
    public sealed record Settings
    {
        [Description("The desks to roll up; one branch of the circuit each.")]
        public string[] Desks { get; init; } = ["rates", "credit", "fx", "equities"];

        [Range(1, 100), Description("How many of each desk's largest orders to keep.")]
        public int Top { get; init; } = 5;
    }

    public void Build(PipelineBuilder p, Settings s)
    {
        var instruments = p.Table(AcmeTables.Instruments);
        var counterparties = p.Table(AcmeTables.Counterparties);
        var orders = p.KeyedInput<long, Order>("orders", o => o.Id);

        ZStream<DeskExposure>? byCountry = null, byTier = null, bySymbol = null;
        ZStream<EnrichedOrder>? largest = null;
        foreach (var desk in s.Desks)
        {
            var enriched = orders.Where(o => o.Desk == desk)
                .GroupBy(o => o.InstrumentId)
                .Join(instruments, (_, o, i) => new PricedOrder(o.Id, i.Symbol, o.Counterparty, o.Quantity * o.Price * i.Multiplier, o.Trader, o.Desk))
                .Named($"priced-{desk}")
                .Lookup(counterparties, o => o.Counterparty,
                    (o, c) => new EnrichedOrder(o.OrderId, o.Symbol, c.Name, c.Country, c.Tier, o.Notional, o.Trader));

            ZStream<DeskExposure> Roll(Func<EnrichedOrder, string> key, string name) =>
                enriched.GroupBy(key).Sum(e => e.Notional).Named($"{name}-{desk}").Select(kv => new DeskExposure(desk, kv.Key, kv.Value));

            var country = Roll(e => e.Country, "country");
            var tier = Roll(e => e.Tier, "tier");
            var symbol = Roll(e => e.Symbol, "symbol");
            var top = enriched.GroupBy(_ => desk).TopK(s.Top, e => e.Notional).Named($"top-{desk}").Select(kv => kv.Value);
            byCountry = byCountry is null ? country : byCountry.Concat(country);
            byTier = byTier is null ? tier : byTier.Concat(tier);
            bySymbol = bySymbol is null ? symbol : bySymbol.Concat(symbol);
            largest = largest is null ? top : largest.Concat(top);
        }
        if (byCountry is null || byTier is null || bySymbol is null || largest is null)
            throw new ValidationException("desks must name at least one desk.");

        p.UpsertSink("by-country", byCountry, e => e.Desk + "/" + e.Key);
        p.UpsertSink("by-tier", byTier, e => e.Desk + "/" + e.Key);
        p.UpsertSink("by-symbol", bySymbol, e => e.Desk + "/" + e.Key);
        p.UpsertSink("largest", largest, e => e.OrderId);
    }
}

/// <summary>Throws on an order with a negative quantity. Under the Fault policy that
/// faults the pipeline — and, the record being replayed, faults it again until it is
/// Failed; under DeadLetter the record is dropped and kept in the dead-letter ring.</summary>
[Pipeline("checked-orders", Description = "Order counts per trader, refusing negative quantities.")]
public sealed class CheckedOrders : IPipelineDefinition
{
    public void Build(PipelineBuilder p) =>
        p.UpsertSink("counts", p.KeyedInput<long, Order>("orders", o => o.Id)
            .Select(o => o.Quantity < 0
                ? throw new InvalidOperationException($"order {o.Id} from {o.Trader} has a negative quantity ({o.Quantity})")
                : o)
            .GroupBy(o => o.Trader).Count().Named("count"));
}
