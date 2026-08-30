namespace Incremental.Showcase;

/// <summary>
/// The unattended path (redirected stdin, or <c>--headless</c>): drives every tab and every
/// trigger with no browser, prints what the panels would have shown, and fails the process
/// if a verified act does not verify. This is the CI smoke test.
/// </summary>
public static class Headless
{
    const int LeadTicks = 10;
    const int SettleTicks = 8;

    public static async Task<int> RunAsync(Show show, bool withCluster)
    {
        show.PaceMs = 3;
        var stopping = new CancellationTokenSource();
        Snapshot? last = null;
        var driver = show.RunAsync(snapshot => last = snapshot, stopping.Token);
        var failures = new List<string>();

        try
        {
            await show.SetRunningAsync(false);   // headless steps explicitly, never free-runs
            var seen = 0;

            async Task Ticks(int count)
            {
                for (var i = 0; i < count; i++) await show.StepOnceAsync();
            }

            void PrintNewNotices()
            {
                if (last is null) return;
                // Notices arrive newest-first; print the ones added since the last drain.
                var fresh = last.Notices.Count - seen;
                foreach (var notice in last.Notices.Take(Math.Max(fresh, 0)).Reverse())
                    Console.WriteLine($"    · {notice.Text}");
                seen = last.Notices.Count;
            }

            foreach (var tab in show.Tabs)
            {
                Console.WriteLine();
                Console.WriteLine($"── {tab.Title} {new string('─', Math.Max(0, 66 - tab.Title.Length))}");
                Console.WriteLine(Indent(tab.Explainer));
                await Ticks(LeadTicks);
                PrintNewNotices();

                foreach (var id in tab.Beats)
                {
                    if (!show.TryGetBeat(id, out var beat)) continue;
                    if (last?.Beats.GetValueOrDefault(tab.Id)?.FirstOrDefault(b => b.Id == id) is not { Armed: true })
                    {
                        Console.WriteLine($"  → {beat.Label}  [skipped: not armed]");
                        continue;
                    }
                    Console.WriteLine($"  → {beat.Label}");
                    await show.FireAsync(beat);
                    await Ticks(SettleTicks);
                    PrintNewNotices();
                }

                Report(tab.Id, last, failures, withCluster);
            }
        }
        finally
        {
            await stopping.CancelAsync();
            await driver;
        }

        Console.WriteLine();
        Console.WriteLine(Indent("""
            One declared dataflow, kept correct by deltas: joins (inner/left/full/anti), Count/Sum/
            Min/Max, custom linear aggregates, distinct counts, grouped and per-window TopK,
            tumbling/hopping/session windows with OnClose finals and watermark eviction, recursive
            fixed points with retraction unwinding, keyed upsert inputs correcting history, crash
            recovery with exactly-once replay and self-bootstrapping consumers, and (with --cluster)
            the same code sharded across two OS processes.
            """));

        if (failures.Count == 0)
        {
            Console.WriteLine("\nsmoke run OK — every act ran and every verified act verified.");
            return 0;
        }
        Console.WriteLine();
        foreach (var failure in failures) Console.Error.WriteLine($"FAILED: {failure}");
        return 1;
    }

    static void Report(string tabId, Snapshot? s, List<string> failures, bool withCluster)
    {
        if (s is null) { failures.Add($"{tabId}: no snapshot was produced"); return; }
        switch (tabId)
        {
            case "living":
                Table("revenue by category · AggregateLinear + Min/Max",
                    ["category", "orders", "gross", "fee take", "min", "max"],
                    s.Act1.Categories.Select(c => new[]
                    {
                        c.Category, $"{c.Orders:N0}", $"{c.Gross:N2}", $"{c.Fees:N2}",
                        c.Min?.ToString("N2") ?? "—", c.Max?.ToString("N2") ?? "—",
                    }));
                Table("top merchants · Distinct().Grouped().Count()",
                    ["merchant", "orders", "items", "buyers", "revenue"],
                    s.Act1.Merchants.Select(m => new[]
                    {
                        Ui.Truncate(m.Merchant, 20), $"{m.Orders:N0}", $"{m.Items:N0}", $"{m.Buyers:N0}", $"{m.Revenue:N2}",
                    }));
                Line("recon · FullJoin", string.Join("  ", s.Act1.Recon.Select(r => $"{r.Bucket} {r.Count:N0}")));
                Line("regions · LeftJoin", string.Join("  ", s.Act1.Regions.Select(r => $"{r.Region} {r.Revenue:N2}")));
                Line("compliance · AntiJoin", $"{s.Act1.Compliance.Held:N0} orders held");
                if (s.Act1.Categories.Length == 0) failures.Add("living: no category rollup rows");
                break;

            case "time":
                Table("live leaderboard · 10s tumbling",
                    ["window", "merchant", "revenue"],
                    s.Act2.Live.Select(l => new[] { l.Window, Ui.Truncate(l.Merchant, 20), $"{l.Revenue:N2}" }));
                Table("finalized · OnClose() · exactly once",
                    ["window", "merchant", "revenue"],
                    s.Act2.Finals.Select(l => new[] { l.Window, Ui.Truncate(l.Merchant, 20), $"{l.Revenue:N2}" }));
                Table("acceptance · HoppingWindow(30s, 5s)",
                    ["window", "approved", "total", "rate"],
                    s.Act2.Acceptance.Select(a => new[] { a.Window, $"{a.Approved:N0}", $"{a.Total:N0}", $"{a.Rate:P1}" }));
                Table("shopper sessions · SessionWindow(gap 20s)",
                    ["customer", "orders", "spend", "span", "state"],
                    s.Act2.Sessions.Select(x => new[]
                    {
                        Ui.Truncate(x.Customer, 18), $"{x.Orders:N0}", $"{x.Amount:N2}", x.Span, x.Open ? "open" : "closing",
                    }));
                if (s.Act2.Finals.Length == 0) failures.Add("time: no window ever finalized via OnClose()");
                break;

            case "graph":
                Table("connected fraud rings · Recurse",
                    ["ring", "size", "exposure", "accounts"],
                    s.Act3.Rings.Select(r => new[]
                    {
                        $"#{r.Ring}", $"{r.Size:N0}", $"{r.Exposure:N2}", string.Join(", ", r.Accounts),
                    }));
                if (s.Act3.Rings.Length == 0) failures.Add("graph: the transitive closure produced no rings");
                break;

            case "kill":
                if (s.Recovery is not { } r) { failures.Add("kill: the crash trigger never ran"); break; }
                Line("generation", $"{r.Generation}");
                Line("committed offsets", string.Join("  ", r.Committed.Select(kv => $"{kv.Key} {kv.Value?.ToString("N0") ?? "—"}")));
                Line("replay", $"{r.Replayed:N0} records → {r.Deduped:N0} deduplicated, {r.Replayed - r.Deduped:N0} reapplied");
                Line("verified", r.Matched
                    ? $"rebuilt state matches the pre-crash snapshot ({r.Rows} rows, gross {r.Gross:N2})"
                    : $"MISMATCH: gross {r.Gross:N2} vs expected {r.ExpectedGross:N2}");
                if (!r.Matched) failures.Add("kill: rebuilt state does not match the pre-crash snapshot");
                break;

            case "cluster":
                if (s.Scaling is { } sc)
                {
                    Table($"threads · same declaration, {sc.Cores} logical cores{(sc.DebugBuild ? " · DEBUG BUILD, throughput is a lower bound" : "")}",
                        ["topology", "p50", "p99", "bulk throughput"],
                        sc.Samples.Select(x => new[]
                        {
                            x.Label + (x.Highlight ? "  ← running" : ""),
                            $"{x.LatencyP50:N1} µs", $"{x.LatencyP99:N1} µs", $"{x.BulkRowsPerSec:N0} rows/s",
                        }));
                    var quickest = sc.Samples.MinBy(x => x.LatencyP50)!;
                    var fastest = sc.Samples.MaxBy(x => x.BulkRowsPerSec)!;
                    Line("verified", $"lowest latency at {quickest.Workers} worker(s), highest throughput at {fastest.Workers}");
                    if (quickest.LatencyP50 <= 0) failures.Add("scaling: measured a non-positive latency");
                }
                else failures.Add("cluster: the worker measurement never ran");

                if (!withCluster)
                {
                    Console.WriteLine("  (two processes skipped — run with --cluster)");
                    break;
                }
                if (s.Cluster is not { } c) { failures.Add("cluster: the peer process never ran"); break; }
                Table("one circuit, two OS processes, TCP exchange",
                    ["process", "workers", "merchants on shard", "frames"],
                    c.Processes.Select(x => new[] { x.Process, x.Workers, x.Merchants, x.Frames }));
                Line("verified", c.Correct
                    ? $"shards are disjoint ({c.LocalRows:N0} + {c.RemoteRows:N0} merchants) and their union matches the single-process answer"
                    : "merged shards did NOT match the expected totals");
                if (!c.Correct) failures.Add("cluster: merged shards do not match the single-process reference");
                break;
        }
    }

    static void Line(string label, string value) => Console.WriteLine($"  {label,-20} {value}");

    static void Table(string title, string[] headers, IEnumerable<string[]> rows)
    {
        var body = rows.ToList();
        Console.WriteLine($"\n  {title}");
        if (body.Count == 0) { Console.WriteLine("    (empty)"); return; }
        var widths = headers.Select((h, i) => Math.Max(h.Length, body.Max(r => r[i].Length))).ToArray();
        Console.WriteLine("    " + string.Join("  ", headers.Select((h, i) => h.PadRight(widths[i]))));
        foreach (var row in body)
            Console.WriteLine("    " + string.Join("  ", row.Select((cell, i) => cell.PadRight(widths[i]))));
    }

    static string Indent(string text) =>
        string.Join('\n', text.Split('\n').Select(l => "  " + l));
}
