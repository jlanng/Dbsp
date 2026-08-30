using Incremental;

namespace Incremental.Showcase;

/// <summary>
/// The whole dataflow, declared once. Act 4 rebuilds the circuit by calling
/// <see cref="Declare"/> again on a fresh builder — declaration order is identity for
/// checkpointed state (store names are positional unless pinned with .Named), so this
/// function is the single source of truth for the graph.
/// </summary>
public sealed record ShowcaseDefs(
    InputStream<Order> WebOrders,
    InputStream<Order> MobileOrders,
    InputStream<Payment> Payments,
    KeyedInputStream<int, Merchant> Merchants,
    KeyedInputStream<int, Customer> Customers,
    KeyedInputStream<int, FeeRate> FeeRates,
    KeyedInputStream<(int A, int B), LinkEdge> Edges,
    // Act 1 — living data
    Output<KeyedValue<string, FeeStats>> CategoryStats,
    Output<KeyedValue<string, decimal>> CategoryMin,
    Output<KeyedValue<string, decimal>> CategoryMax,
    Output<KeyedValue<string, EnrichedOrder>> TopOrders,
    Output<KeyedValue<int, long>> MerchantOrders,
    Output<KeyedValue<int, decimal>> MerchantRevenue,
    Output<KeyedValue<int, long>> MerchantBuyers,
    Output<KeyedValue<int, long>> MerchantItems,
    Output<KeyedValue<string, decimal>> RegionRevenue,
    Output<KeyedValue<int, Order>> Compliance,
    Output<KeyedValue<string, long>> Recon,
    Output<EnrichedOrder> EnrichedFeed,
    // Act 2 — time
    Output<WindowedValue<int, decimal>> LiveLeaders,
    Output<WindowedValue<int, decimal>> FinalLeaders,
    Output<WindowedValue<string, Acceptance>> AcceptanceRate,
    Output<WindowedValue<int, Spend>> Sessions,
    // Act 3 — the graph trick
    Output<KeyedValue<int, long>> RingSizes,
    Output<KeyedValue<int, decimal>> RingExposure,
    Output<KeyedValue<int, int>> RingMembership);

public static class ShowcasePipeline
{
    public static ShowcaseDefs Declare(CircuitBuilder builder)
    {
        var web = builder.Input<Order>("orders-web");
        var mobile = builder.Input<Order>("orders-mobile");
        var payments = builder.Input<Payment>("payments");
        var merchants = builder.KeyedInput<int, Merchant>("merchants", m => m.Id);
        var customers = builder.KeyedInput<int, Customer>("customers", c => c.Id);
        var feeRates = builder.KeyedInput<int, FeeRate>("fee-rates", f => f.Tier);
        var edges = builder.KeyedInput<(int A, int B), LinkEdge>("link-edges", e => (e.A, e.B));

        var orders = web.Concat(mobile);

        // ── Act 1: enrichment (orders ⋈ merchants ⋈ fee schedule, left-⋈ customers) ──
        var enriched = orders
            .GroupBy(o => o.MerchantId)
            .Join(merchants.GroupBy(m => m.Id),
                (_, o, m) => new OrderWithMerchant(o.Id, o.CustomerId, o.MerchantId, m.Name, m.Category, m.FeeTier, o.Amount))
            .GroupBy(x => x.FeeTier)
            .Join(feeRates.GroupBy(f => f.Tier),
                (_, x, f) => new EnrichedOrder(x.OrderId, x.MerchantId, x.Merchant, x.Category, x.CustomerId,
                    "guest", x.Amount, Math.Round(x.Amount * f.Rate, 2)))
            .GroupBy(e => e.CustomerId)
            .LeftJoin(customers.GroupBy(c => c.Id),
                (_, e, c) => e with { Region = c.Region },
                (_, e) => e)                                       // unknown account: stays "guest"
            .Named("enrichment");

        var byCategory = enriched.GroupBy(e => e.Category);
        var categoryStats = byCategory.AggregateLinear(new FeeTakeAggregate()).Named("category-stats").ToOutput();
        var categoryMin = byCategory.Min(e => e.Amount).ToOutput();
        var categoryMax = byCategory.Max(e => e.Amount).ToOutput();
        var topOrders = byCategory.TopK(3, e => e.Amount).ToOutput();

        var byMerchant = enriched.GroupBy(e => e.MerchantId);
        var merchantOrders = byMerchant.Count().ToOutput();
        var merchantRevenue = byMerchant.Sum(e => e.Amount).ToOutput();
        var merchantBuyers = enriched
            .Select(e => new BuyerRow(e.MerchantId, e.CustomerId))
            .GroupBy(b => b.MerchantId)
            .Distinct().Grouped().Count()                          // distinct buyers per merchant
            .ToOutput();
        var merchantItems = orders.SelectMany(SplitItems).GroupBy(i => i.MerchantId).Count().ToOutput();
        var regionRevenue = enriched.GroupBy(e => e.Region).Sum(e => e.Amount).ToOutput();

        // Orders whose merchant is not in master data: the compliance queue. When the
        // merchant is later onboarded, these rows retract out of the anti-join live.
        var compliance = orders.GroupBy(o => o.MerchantId).AntiJoin(merchants.GroupBy(m => m.Id)).ToOutput();

        var recon = orders.GroupBy(o => o.Id)
            .FullJoin(payments.GroupBy(p => p.OrderId),
                (_, o, p) => new ReconRow(o.Id, p.Status == "approved" ? "matched" : "declined"),
                (_, o) => new ReconRow(o.Id, "awaiting payment"),
                (id, p) => new ReconRow(id, "orphan payment"))
            .GroupBy(r => r.Bucket).Count().ToOutput();

        // Raw enriched changefeed, drained synchronously after each step for delta counts.
        var enrichedFeed = enriched.ToOutput();

        // ── Act 2: event time ────────────────────────────────────────────────────
        var ordersTimed = orders.WithEventTime(o => o.At, lateness: TimeSpan.FromSeconds(5));

        var leaders = ordersTimed
            .GroupBy(o => o.MerchantId)
            .TumblingWindow(TimeSpan.FromSeconds(10))
            .Sum(o => o.Amount)
            .TopK(3, w => w.Value);                                // ranks within each window
        var liveLeaders = leaders.ToOutput();                      // flapping, corrected live
        var finalLeaders = leaders.OnClose().ToOutput(bootstrap: false);   // one final row per window

        var acceptanceRate = payments
            .WithEventTime(p => p.At, lateness: TimeSpan.FromSeconds(5))
            .GroupBy(_ => "gateway")                               // global rate: one group
            .HoppingWindow(TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(5))
            .AggregateLinear(new AcceptanceAggregate())
            .ToOutput();

        var sessions = ordersTimed
            .GroupBy(o => o.CustomerId)
            .SessionWindow(TimeSpan.FromSeconds(20))
            .AggregateLinear(new SpendAggregate())
            .ToOutput();

        // ── Act 3: transitive closure over account linkage ───────────────────────
        var links = edges.SelectMany(e => new[] { new Link(e.A, e.B), new Link(e.B, e.A) });
        var reach = links.Recurse(paths => paths
            .GroupBy(p => p.To)
            .Join(links.GroupBy(l => l.From), (_, p, l) => new Link(p.From, l.To)));
        var membership = reach
            .Concat(links.Select(l => new Link(l.From, l.From)))
            .GroupBy(l => l.From)
            .Min(l => l.To);                                       // ring id = smallest linked account

        var ringSizes = membership
            .Select(m => new RingMember(m.Value, m.Key))
            .GroupBy(m => m.Ring).Count()
            .ToOutput();
        var ringExposure = orders
            .GroupBy(o => o.CustomerId)
            .Join(membership.Grouped(), (account, o, ring) => new FlaggedOrder(ring, account, o.Id, o.Amount))
            .GroupBy(f => f.Ring).Sum(f => f.Amount)
            .ToOutput();
        var ringMembership = membership.ToOutput(bootstrap: false);    // drained for beat notices

        return new ShowcaseDefs(
            web, mobile, payments, merchants, customers, feeRates, edges,
            categoryStats, categoryMin, categoryMax, topOrders,
            merchantOrders, merchantRevenue, merchantBuyers, merchantItems, regionRevenue,
            compliance, recon, enrichedFeed,
            liveLeaders, finalLeaders, acceptanceRate, sessions,
            ringSizes, ringExposure, ringMembership);
    }

    // Pure function of the order (equal ItemSale rows collapse to one z-set entry with
    // weight = Items, which is exactly what Count() counts).
    static IEnumerable<ItemSale> SplitItems(Order o)
    {
        var unit = Math.Round(o.Amount / o.Items, 2);
        for (var i = 0; i < o.Items; i++)
            yield return new ItemSale(o.Id, o.MerchantId, unit);
    }
}
