using Dbsp;

namespace Dbsp.Showcase;

// The JSON shapes the browser renders. Nothing here computes a result — every number is
// read straight off a materialized view; projection is formatting, not aggregation.

public record CategoryRow(string Category, long Orders, decimal Gross, decimal Fees, decimal? Min, decimal? Max);
public record MerchantRow(string Merchant, long Orders, long Items, long Buyers, decimal Revenue);
public record TopRow(string Category, decimal[] Amounts);
public record ReconBucket(string Bucket, long Count);
public record RegionRow(string Region, decimal Revenue);
public record ComplianceState(int Held, string[] Recent);
public record Act1View(
    CategoryRow[] Categories, MerchantRow[] Merchants, TopRow[] Top,
    ReconBucket[] Recon, RegionRow[] Regions, ComplianceState Compliance);

public record LeaderRow(string Window, string Merchant, decimal Revenue);
public record AcceptanceRow(string Window, long Approved, long Total, double Rate);
public record SessionRow(string Customer, long Orders, decimal Amount, string Span, bool Open);
public record Act2View(LeaderRow[] Live, LeaderRow[] Finals, AcceptanceRow[] Acceptance, SessionRow[] Sessions);

public record RingRow(int Ring, string[] Accounts, long Size, decimal Exposure);
public record EdgeRow(int A, int B, string Reason);
public record Act3View(RingRow[] Rings, EdgeRow[] Edges);

/// <summary>One row of the enriched changefeed, exactly as the output emitted it —
/// weight &lt; 0 is a retraction. This is the only panel that shows raw deltas.</summary>
public record FeedRow(long Seq, long OrderId, string Merchant, string Category, decimal Amount, decimal Fee, long Weight);

public record RecoveryView(
    int Generation, Dictionary<string, long?> Committed, long Replayed, long Deduped, long ReferenceRows,
    bool Matched, int Rows, decimal Gross, decimal ExpectedGross);

public record ScalingView(
    ScalingAct.Sample[] Samples, int Cores, int LiveBatch, int BulkBatch, int CurrentWorkers,
    bool DebugBuild);

public record ClusterProcessRow(string Process, string Workers, string Merchants, string Frames);
public record ClusterView(
    ClusterProcessRow[] Processes, int Nodes, int WorkersPerNode, int Orders, int Steps, double AvgStepMs,
    bool Correct, int LocalRows, int RemoteRows);

public static class Views
{
    public static Dictionary<TKey, TValue> Snapshot<TKey, TValue>(InMemoryView<KeyedValue<TKey, TValue>> view)
        where TKey : notnull where TValue : notnull
    {
        var result = new Dictionary<TKey, TValue>();
        foreach (var entry in view.Current())
            if (entry.Weight > 0)
                result[entry.Value.Key] = entry.Value.Value;
        return result;
    }

    public static List<WindowedValue<TKey, TValue>> Windows<TKey, TValue>(InMemoryView<WindowedValue<TKey, TValue>> view)
        where TKey : notnull where TValue : notnull
        => view.Current().Where(e => e.Weight > 0).Select(e => e.Value).ToList();

    // ── Act 1 ──────────────────────────────────────────────────────────────────

    /// <summary>The category rollup on its own — tab 4 shows it as the state being
    /// verified against the pre-crash snapshot.</summary>
    public static CategoryRow[] Categories(ShowcaseCircuit c)
    {
        var stats = Snapshot(c.CategoryStats);
        var mins = Snapshot(c.CategoryMin);
        var maxs = Snapshot(c.CategoryMax);
        return stats.OrderByDescending(kv => kv.Value.Gross)
            .Select(kv => new CategoryRow(kv.Key, kv.Value.Orders, kv.Value.Gross, kv.Value.Fees,
                mins.TryGetValue(kv.Key, out var mn) ? mn : null,
                maxs.TryGetValue(kv.Key, out var mx) ? mx : null))
            .ToArray();
    }

    public static Act1View Act1(ShowcaseCircuit c, Director d)
    {
        var names = d.Merchants.ToDictionary(m => m.Id, m => m.Name);
        var revenue = Snapshot(c.MerchantRevenue);
        var orders = Snapshot(c.MerchantOrders);
        var buyers = Snapshot(c.MerchantBuyers);
        var items = Snapshot(c.MerchantItems);
        var merchants = revenue.OrderByDescending(kv => kv.Value).Take(8)
            .Select(kv => new MerchantRow(
                names.GetValueOrDefault(kv.Key, $"#{kv.Key}"),
                orders.GetValueOrDefault(kv.Key), items.GetValueOrDefault(kv.Key),
                buyers.GetValueOrDefault(kv.Key), kv.Value))
            .ToArray();

        var top = c.TopOrders.Current().Where(e => e.Weight > 0).Select(e => e.Value)
            .GroupBy(kv => kv.Key).OrderBy(g => g.Key)
            .Select(g => new TopRow(g.Key, g.Select(kv => kv.Value.Amount).OrderByDescending(a => a).ToArray()))
            .ToArray();

        var recon = Snapshot(c.Recon);
        var buckets = new[] { "matched", "declined", "awaiting payment", "orphan payment" }
            .Select(b => new ReconBucket(b, recon.GetValueOrDefault(b))).ToArray();

        var regions = Snapshot(c.RegionRevenue).OrderByDescending(kv => kv.Value)
            .Select(kv => new RegionRow(kv.Key, kv.Value)).ToArray();

        var quarantined = c.Compliance.Current().Where(e => e.Weight > 0).Select(e => e.Value).ToList();
        var compliance = new ComplianceState(quarantined.Count,
            quarantined.TakeLast(4)
                .Select(kv => $"order #{kv.Value.Id} → unknown merchant #{kv.Key} ({kv.Value.Amount:N2})")
                .ToArray());

        return new Act1View(Categories(c), merchants, top, buckets, regions, compliance);
    }

    // ── Act 2 ──────────────────────────────────────────────────────────────────

    public static Act2View Act2(ShowcaseCircuit c, Director d)
    {
        var names = d.Merchants.ToDictionary(m => m.Id, m => m.Name);
        string MerchantName(int id) => names.GetValueOrDefault(id, $"#{id}");
        static string Span(DateTimeOffset from, DateTimeOffset to) => $"{from:HH:mm:ss}–{to:HH:mm:ss}";

        var liveWindows = Windows(c.LiveLeaders);
        var live = liveWindows.Select(w => w.WindowStart).Distinct().OrderByDescending(s => s).Take(2)
            .SelectMany(start => liveWindows.Where(w => w.WindowStart == start).OrderByDescending(w => w.Value)
                .Select(w => new LeaderRow(Span(w.WindowStart, w.WindowEnd), MerchantName(w.Key), w.Value)))
            .ToArray();

        var finals = c.Finals.Select(w => w.WindowStart).Distinct().OrderByDescending(s => s).Take(3)
            .SelectMany(start => c.Finals.Where(w => w.WindowStart == start).OrderByDescending(w => w.Value)
                .Select(w => new LeaderRow(Span(w.WindowStart, w.WindowEnd), MerchantName(w.Key), w.Value)))
            .ToArray();

        var acceptance = Windows(c.AcceptanceRate).OrderByDescending(w => w.WindowStart).Take(6)
            .Select(w => new AcceptanceRow($"{w.WindowStart:HH:mm:ss}", w.Value.Approved, w.Value.Total,
                w.Value.Total == 0 ? 0 : (double)w.Value.Approved / w.Value.Total))
            .ToArray();

        var customerNames = d.Customers.ToDictionary(cu => cu.Id, cu => cu.Name);
        var wm = c.Circuit.CurrentWatermark;
        var sessions = Windows(c.Sessions).OrderByDescending(w => w.WindowEnd).Take(7)
            .Select(s => new SessionRow(
                customerNames.GetValueOrDefault(s.Key, $"#{s.Key} (guest)"),
                s.Value.Orders, s.Value.Amount, Span(s.WindowStart, s.WindowEnd),
                !(wm is { } w && w >= s.WindowEnd)))
            .ToArray();

        return new Act2View(live, finals, acceptance, sessions);
    }

    // ── Act 3 ──────────────────────────────────────────────────────────────────

    public static Act3View Act3(ShowcaseCircuit c, Director d)
    {
        var sizes = Snapshot(c.RingSizes);
        var exposure = Snapshot(c.RingExposure);
        var customerNames = d.Customers.ToDictionary(cu => cu.Id, cu => cu.Name);

        var rings = c.Rings.GroupBy(kv => kv.Value).OrderBy(g => g.Key)
            .Select(g => new RingRow(g.Key,
                g.Select(kv => kv.Key).OrderBy(a => a)
                    .Select(a => $"{a} ({customerNames.GetValueOrDefault(a, "?")})").ToArray(),
                sizes.GetValueOrDefault(g.Key),
                exposure.GetValueOrDefault(g.Key)))
            .ToArray();

        var edges = d.Edges.Values.Select(e => new EdgeRow(e.A, e.B, e.Reason)).ToArray();
        return new Act3View(rings, edges);
    }
}
