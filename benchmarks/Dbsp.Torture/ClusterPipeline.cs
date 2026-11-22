using Dbsp.Showcase;

namespace Dbsp.Torture;

/// <summary>
/// The cluster-mode graph: the showcase's enrichment-and-aggregation spine, minus the
/// recursive fixed point (<c>Recurse</c> is rejected in clustered circuits) and minus the
/// session windows (whose state is worker-local anyway). Every operator here sits behind an
/// exchange, so in a cluster every one of them ships shards across the process boundary —
/// which is the point: this measures the wire, not the operators.
/// </summary>
internal sealed record ClusterDefs(
    InputStream<Order> Orders,
    KeyedInputStream<int, Merchant> Merchants,
    KeyedInputStream<int, FeeRate> FeeRates,
    Output<KeyedValue<string, FeeStats>> CategoryStats,
    Output<KeyedValue<int, long>> MerchantOrders,
    Output<KeyedValue<int, decimal>> MerchantRevenue,
    Output<KeyedValue<string, decimal>> CategoryMax,
    Output<KeyedValue<string, EnrichedOrder>> TopOrders,
    Output<EnrichedOrder> EnrichedFeed);

internal static class ClusterPipeline
{
    public static ClusterDefs Declare(CircuitBuilder builder)
    {
        var orders = builder.Input<Order>("orders");
        var merchants = builder.KeyedInput<int, Merchant>("merchants", m => m.Id);
        var feeRates = builder.KeyedInput<int, FeeRate>("fee-rates", f => f.Tier);

        var enriched = orders
            .GroupBy(o => o.MerchantId)
            .Join(merchants.GroupBy(m => m.Id),
                (_, o, m) => new OrderWithMerchant(o.Id, o.CustomerId, o.MerchantId, m.Name, m.Category, m.FeeTier, o.Amount))
            .GroupBy(x => x.FeeTier)
            .Join(feeRates.GroupBy(f => f.Tier),
                (_, x, f) => new EnrichedOrder(x.OrderId, x.MerchantId, x.Merchant, x.Category, x.CustomerId,
                    "guest", x.Amount, Math.Round(x.Amount * f.Rate, 2)));

        var byCategory = enriched.GroupBy(e => e.Category);
        var byMerchant = enriched.GroupBy(e => e.MerchantId);

        return new ClusterDefs(
            orders, merchants, feeRates,
            byCategory.AggregateLinear(new FeeTakeAggregate()).ToOutput(bootstrap: false),
            byMerchant.Count().ToOutput(bootstrap: false),
            byMerchant.Sum(e => e.Amount).ToOutput(bootstrap: false),
            byCategory.Max(e => e.Amount).ToOutput(bootstrap: false),
            byCategory.TopK(3, e => e.Amount).ToOutput(bootstrap: false),
            enriched.ToOutput());
    }
}
