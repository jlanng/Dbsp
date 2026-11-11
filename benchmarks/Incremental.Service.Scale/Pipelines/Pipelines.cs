using Incremental;
using Incremental.Service;
using Scale.Contracts;

namespace Scale.Pipelines;

/// <summary>
/// Large state, the retroactive way: orders are keyed upserts (the input keeps one entry
/// per order), joined to the party (the join keeps every order under its party), so a
/// pipeline that has seen S orders holds about 2·S state entries.
/// </summary>
[Pipeline("order-enrichment", Description = "Orders (keyed) joined to parties; state per order.")]
public sealed class OrderEnrichment : IPipelineDefinition
{
    public void Build(PipelineBuilder p)
    {
        var enriched = p.KeyedInput<long, Order>("orders", o => o.Id)
            .GroupBy(o => o.PartyId)
            .Join(p.Table(Reference.Parties), (_, o, party) => new EnrichedOrder(
                o.Id, o.PartyId, party.Name, party.Country, party.Rating, o.Symbol, o.Quantity * o.Price)).Named("joined");
        p.UpsertSink("enriched", enriched, e => e.OrderId);
    }
}

/// <summary>Large state in an aggregate: one running total per party that has had an order.</summary>
[Pipeline("party-exposure", Description = "Notional per party; state per party seen.")]
public sealed class PartyExposure : IPipelineDefinition
{
    public void Build(PipelineBuilder p)
    {
        var exposure = p.Input<Order>("orders")
            .GroupBy(o => o.PartyId)
            .Sum(o => o.Quantity * o.Price).Named("exposure")
            .Select(kv => new Exposure(kv.Key, kv.Value));
        p.UpsertSink("exposure", exposure, e => e.PartyId);
    }
}

/// <summary>No state at all: each order takes the party as it is when the order arrives.</summary>
[Pipeline("order-lookup", Description = "Orders enriched on arrival; no state.")]
public sealed class OrderLookup : IPipelineDefinition
{
    public void Build(PipelineBuilder p)
    {
        var tagged = p.Input<Order>("orders")
            .LookupAppendOnly(p.Table(Reference.Parties), o => o.PartyId,
                (o, party) => new EnrichedOrder(o.Id, o.PartyId, party.Name, party.Country, party.Rating, o.Symbol, o.Quantity * o.Price));
        p.AppendSink("tagged", tagged);
    }
}

public sealed record EnrichedOrder(long OrderId, long PartyId, string Party, string Country, string Rating, string Symbol, decimal Notional);

public sealed record Exposure(long PartyId, decimal Notional);
