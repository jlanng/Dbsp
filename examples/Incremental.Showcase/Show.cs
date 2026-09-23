using System.Threading.Channels;
using Incremental.Storage;
using Microsoft.Extensions.Logging;

namespace Incremental.Showcase;

/// <summary>A scripted moment, fired from the browser by id.</summary>
public sealed class Beat
{
    public required string Id;
    public required string Label;
    public required Func<Task> Fire;
    public bool Once;                    // fires at most once (e.g. onboarding)
    /// <summary>Extra guard: returns why the trigger cannot fire, or null when it can.</summary>
    public Func<string?>? Unavailable;
    public bool Fired;
}

public sealed class Tab
{
    public required string Id;
    public required string Title;
    public required string Explainer;
    public string? Code;
    public string[] Beats = [];
}

// What the browser receives, whole, on every frame. Bounded by construction (under ~100
// rows), so a snapshot per frame is cheaper than any delta protocol would be to maintain.
public record BeatState(string Id, string Label, bool Armed, string? Reason);
public record HeaderState(
    int Tick, int Generation, string VirtualNow, string? Watermark, double? WatermarkBehind,
    string Storage, int Workers, int Nodes, string Build,
    bool Running, string Circuit, double LastStepMicros, int LastEnrichedDelta,
    double StepP50, double StepP95, int StepWindow, double SyncWaitPct, double SyncWaitSeconds);
public record Snapshot(
    HeaderState Header, MeterSnapshot Meter, List<Notice> Notices,
    Act1View Act1, Act2View Act2, Act3View Act3,
    CategoryRow[] Act4Categories, RecoveryView? Recovery,
    ScalingView? Scaling, ClusterView? Cluster,
    FeedRow[] Feed, Dictionary<string, BeatState[]> Beats);

/// <summary>
/// The showcase driver. Owns the circuit, the director and the simulated clock, and runs
/// them on a single loop — the circuit is stepped from exactly one thread, so HTTP handlers
/// never touch it directly: they queue a command and await its turn.
/// </summary>
public sealed class Show : IAsyncDisposable
{
    readonly record struct Command(Func<Task> Run, TaskCompletionSource Done);

    readonly Channel<Command> commands = Channel.CreateUnbounded<Command>(
        new UnboundedChannelOptions { SingleReader = true });

    readonly IStorageProvider storage;
    readonly ILoggerFactory loggerFactory;

    /// <summary>The configuration this process is running: what the header shows and what
    /// the browser's "apply" is diffed against.</summary>
    public Topology Topology { get; }

    public NoticeLog Notices { get; }
    public IncrementalMetrics Metrics { get; }
    public string StorageLabel { get; }
    public Tab[] Tabs { get; }
    public bool Running { get; private set; } = true;

    /// <summary>Wall-clock pacing of the driver loop. The browser wants a watchable ~6
    /// frames a second; the headless smoke run turns it right down.</summary>
    public int PaceMs { get; set; } = 170;

    readonly Dictionary<string, Beat> beats = [];
    readonly Director director = new(DateTimeOffset.UtcNow);
    ShowcaseCircuit live;
    int globalTick;
    int generation = 1;
    RecoveryView? recovery;
    ScalingView? scaling;
    ClusterView? cluster;

    // Exchange-wait share of the live pipeline over a trailing window of frames: each frame
    // samples the circuit's summed per-worker barrier wait (cumulative seconds, from the
    // library's gauge) against its summed step time; the share is Δwait / (workers ×
    // Δstep time) between the oldest and newest sample. Tagged by generation so a crash
    // rebuild (new circuit, new gauge series) starts the window over.
    const int WaitWindow = 120;              // frames ≈ 20 s at the browser's pace
    readonly (int Generation, double WaitSeconds, double StepSeconds)[] waitSamples = new (int, double, double)[WaitWindow];
    int waitSampleCount;

    (double Pct, double Seconds) SyncWaitShare()
    {
        (int Generation, double WaitSeconds, double StepSeconds) sample =
            (generation, Metrics.Gauge(live.Circuit.Name, "incremental.circuit.barrier_wait"), live.TotalStepMicros / 1e6);
        waitSamples[waitSampleCount++ % WaitWindow] = sample;
        var oldest = sample;
        for (var i = Math.Max(0, waitSampleCount - WaitWindow); i < waitSampleCount; i++)
        {
            var s = waitSamples[i % WaitWindow];
            if (s.Generation == generation) { oldest = s; break; }
        }
        var stepSeconds = sample.StepSeconds - oldest.StepSeconds;
        var waitSeconds = sample.WaitSeconds - oldest.WaitSeconds;
        return stepSeconds <= 0 ? (0, 0) : (100 * waitSeconds / (live.Workers * stepSeconds), waitSeconds);
    }

    public static async Task<Show> StartAsync(IStorageProvider storage, string storageLabel, Topology topology,
        NoticeLog notices, IncrementalMetrics metrics, ILoggerFactory loggerFactory) =>
        new(storage, storageLabel, topology, notices, metrics, loggerFactory,
            await ShowcaseCircuit.StartAsync("showcase", storage, loggerFactory, topology.Workers));

    Show(IStorageProvider storage, string storageLabel, Topology topology,
        NoticeLog notices, IncrementalMetrics metrics, ILoggerFactory loggerFactory, ShowcaseCircuit first)
    {
        this.storage = storage;
        this.loggerFactory = loggerFactory;
        Topology = topology;
        StorageLabel = storageLabel;
        Notices = notices;
        Metrics = metrics;
        live = first;
        Tabs = BuildTabs();
    }

    // ── the loop ───────────────────────────────────────────────────────────────

    /// <summary>Runs until cancelled. <paramref name="onFrame"/> is called with a fresh
    /// snapshot after every tick — that is the only publication point.</summary>
    public async Task RunAsync(Action<Snapshot> onFrame, CancellationToken ct)
    {
        await live.ReplayMasterDataAsync(director);      // initial load of the keyed inputs
        await live.StepTimedAsync();
        Notices.Push(NoticeLevel.Info,
            $"reference data loaded: {director.Merchants.Count} merchants, {director.Customers.Count} customers, {director.FeeRates.Count} fee tiers");

        while (!ct.IsCancellationRequested)
        {
            while (commands.Reader.TryRead(out var command))
            {
                try
                {
                    await command.Run();
                    command.Done.TrySetResult();
                }
                catch (Exception ex)
                {
                    Notices.Push(NoticeLevel.Bad, $"trigger failed: {ex.Message}");
                    command.Done.TrySetException(ex);
                }
            }

            if (Running)
            {
                await TrafficTickAsync();
                await RingTrafficTickAsync();
            }
            Metrics.Poll();
            onFrame(Compose());
            try { await Task.Delay(Running ? PaceMs : Math.Max(1, PaceMs / 3), ct); }
            catch (OperationCanceledException) { break; }
        }
    }

    /// <summary>Queues work onto the driver loop and completes when it has run there.</summary>
    public Task EnqueueAsync(Func<Task> action)
    {
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        if (!commands.Writer.TryWrite(new Command(action, done)))
            done.TrySetException(new InvalidOperationException("the showcase is shutting down"));
        return done.Task;
    }

    public Task SetRunningAsync(bool running) => EnqueueAsync(() => { Running = running; return Task.CompletedTask; });

    /// <summary>One tick of traffic while paused.</summary>
    public Task StepOnceAsync() => EnqueueAsync(async () =>
    {
        await TrafficTickAsync();
        await RingTrafficTickAsync();
    });

    public int CurrentWorkers => live.Workers;

    public static string Build =>
#if DEBUG
        "Debug";
#else
        "Release";
#endif

    public bool TryGetBeat(string id, out Beat beat) => beats.TryGetValue(id, out beat!);

    public Task FireAsync(Beat beat) => EnqueueAsync(async () =>
    {
        if (Blocked(beat) is { } reason) throw new InvalidOperationException(reason);
        await beat.Fire();
        beat.Fired = true;
    });

    /// <summary>Why this trigger cannot fire right now, or null when it can — shown on the
    /// disabled button, so a greyed-out trigger always explains itself.</summary>
    string? Blocked(Beat beat) =>
        beat.Once && beat.Fired ? "already fired — this one only happens once"
        : beat.Unavailable?.Invoke();

    async Task TrafficTickAsync()
    {
        director.AdvanceClock();
        await live.FeedAsync(director.EmitOrders(globalTick), director.EmitPayments(globalTick));
        await live.StepTimedAsync();
        globalTick++;
    }

    // Ring accounts keep spending so their flagged exposure stays live. An earlier TUI ran
    // this only on the graph tab; with several browsers on different tabs it always runs.
    static readonly int[] RingAccounts = [.. Director.RingA, .. Director.RingB];
    async Task RingTrafficTickAsync()
    {
        if (globalTick % 2 != 0) return;
        await live.FeedAsync([director.EmitOrderFor(globalTick, RingAccounts[globalTick % RingAccounts.Length])], []);
        await live.StepTimedAsync();
    }

    // ── the snapshot ───────────────────────────────────────────────────────────

    Snapshot Compose()
    {
        var wm = live.Circuit.CurrentWatermark;
        var (p50, p95, windowSteps) = live.StepPercentiles();
        var (waitPct, waitSeconds) = SyncWaitShare();
        var header = new HeaderState(
            globalTick, generation, director.VirtualNow.ToString("HH:mm:ss"),
            wm?.ToString("HH:mm:ss"), wm is null ? null : (director.VirtualNow - wm.Value).TotalSeconds,
            StorageLabel, live.Workers, Topology.Nodes, Build,
            Running, live.Circuit.Name, live.LastStepMicros, live.LastEnrichedDelta,
            p50, p95, windowSteps, waitPct, waitSeconds);

        return new Snapshot(
            header, Metrics.Footer(live.Circuit.Name), Notices.Snapshot(),
            Views.Act1(live, director), Views.Act2(live, director), Views.Act3(live, director),
            Views.Categories(live), recovery, scaling, cluster,
            live.Feed.AsEnumerable().Reverse().ToArray(),
            Tabs.ToDictionary(t => t.Id, t => t.Beats
                .Select(id => beats[id])
                .Select(b => new BeatState(b.Id, b.Label, Blocked(b) is null, Blocked(b)))
                .ToArray()));
    }

    // ── beats ──────────────────────────────────────────────────────────────────

    async Task OrphanBeat()
    {
        await live.FeedAsync([], [director.EmitOrphanPayment()]);
        await live.StepTimedAsync();
        Notices.Push(NoticeLevel.Warn, "the gateway posted a payment for an order we never saw → recon bucket 'orphan payment'");
    }

    // A correction, deliberately not a re-rate: changing a fee going forward must NOT
    // rewrite history, but fixing wrong master data must — that is what retraction is for.
    async Task CorrectionBeat()
    {
        var wrong = director.Merchants.First(m => m.Id == 100).Category == "fashion";
        var (old, updated) = director.Recategorize(100, wrong ? "electronics" : "fashion");
        await live.Merchants.UpsertAsync(updated);
        var (micros, delta) = await live.StepTimedAsync();
        Notices.Push(NoticeLevel.Warn, wrong
            ? $"master-data correction: {updated.Name} was onboarded under '{old.Category}' — one UpsertAsync re-filed it under '{updated.Category}': {delta:N0} changefeed rows (retract old + assert new) moved its entire history between rollups in {micros:N0} µs"
            : $"error restored: {updated.Name} filed back under '{updated.Category}' so you can watch its history move again — {delta:N0} rows in {micros:N0} µs");
    }

    async Task OnboardBeat()
    {
        var held = live.Compliance.Current().Count(e => e.Weight > 0);
        var merchant = director.OnboardMissingMerchant();
        await live.Merchants.UpsertAsync(merchant);
        var (micros, _) = await live.StepTimedAsync();
        Notices.Push(NoticeLevel.Warn, $"merchant onboarded: '{merchant.Name}' — {held:N0} held orders left the anti-join and joined revenue, fees and leaderboards in {micros:N0} µs");
    }

    async Task LateBeat()
    {
        var late = director.EmitLateOrder(globalTick, TimeSpan.FromSeconds(4));
        await live.FeedAsync([late], []);
        var (micros, _) = await live.StepTimedAsync();
        Notices.Push(NoticeLevel.Warn, $"late event: an order from {late.Order.At:HH:mm:ss} arrived 4 s late (within the 5 s lateness) — its still-open window corrected itself in {micros:N0} µs");
    }

    Task FlashBeat()
    {
        director.SurgeTicksRemaining = 12;
        Notices.Push(NoticeLevel.Warn, "flash sale! 3× order volume and a straining gateway — watch the leaderboard churn and the acceptance rate dip");
        return Task.CompletedTask;
    }

    async Task DropBeat()
    {
        await live.FeedAsync([director.EmitLateOrder(globalTick, TimeSpan.FromSeconds(45))], []);
        await live.StepTimedAsync();
        Notices.Push(NoticeLevel.Bad, "too late: an event 45 s late (> 5 s lateness) was dropped at ingest — closed windows stay final, evicted state stays evicted");
    }

    async Task LinkABeat()
    {
        await live.Edges.UpsertAsync(director.AddEdge(21, 22, "shared card"));
        await live.Edges.UpsertAsync(director.AddEdge(22, 23, "shared device"));
        await live.StepTimedAsync();
        Notices.Push(NoticeLevel.Warn, $"linkage evidence: accounts 21·22·23 connected → ring #21 ({live.LastRingDelta} membership rows from the fixed point)");
    }

    async Task LinkBBeat()
    {
        await live.Edges.UpsertAsync(director.AddEdge(31, 32, "shared device"));
        await live.Edges.UpsertAsync(director.AddEdge(32, 33, "shared card"));
        await live.StepTimedAsync();
        Notices.Push(NoticeLevel.Warn, "linkage evidence: accounts 31·32·33 connected → ring #31");
    }

    async Task BridgeBeat()
    {
        await live.Edges.UpsertAsync(director.AddEdge(23, 31, "shared card"));
        var (micros, _) = await live.StepTimedAsync();
        Notices.Push(NoticeLevel.Bad, $"bridge edge 23↔31: the two rings merged into one 6-account ring — the closure re-converged, {live.LastRingDelta} membership rows rewritten in {micros:N0} µs");
    }

    async Task WithdrawBeat()
    {
        var edge = director.RemoveEdge(23, 31);
        await live.Edges.DeleteAsync((edge.A, edge.B));
        var (micros, _) = await live.StepTimedAsync();
        Notices.Push(NoticeLevel.Warn, $"evidence withdrawn (one DeleteAsync): the closure unwound in well-founded order — rings split back apart, exposure re-attributed, in {micros:N0} µs");
    }

    async Task CrashBeat()
    {
        var before = Views.Snapshot(live.CategoryStats);
        var beforeGross = before.Values.Sum(s => s.Gross);
        // The crash itself: every reference dropped, nothing disposed, nothing flushed.
        Notices.Push(NoticeLevel.Bad, "💥 crash: the circuit's host abandoned without a stop — views and consumers died with it; rebuilding from the last committed checkpoint");
        generation++;
        // The same topology as before the crash — a checkpoint restores into the worker
        // count that wrote it.
        live = await ShowcaseCircuit.StartAsync($"showcase-g{generation}", storage, loggerFactory, Topology.Workers);
        var committed = new Dictionary<string, long?>
        {
            ["orders-web"] = live.Circuit.CommittedOffset("orders-web"),
            ["orders-mobile"] = live.Circuit.CommittedOffset("orders-mobile"),
            ["payments"] = live.Circuit.CommittedOffset("payments"),
        };
        long total = 0, deduped = 0;
        foreach (var o in director.OrderLog)
        {
            total++;
            if (o.Offset <= committed[o.Channel == "web" ? "orders-web" : "orders-mobile"]) deduped++;
        }
        foreach (var p in director.PaymentLog)
        {
            total++;
            if (p.Offset <= committed["payments"]) deduped++;
        }
        // Reference data first: the keyed inputs re-feed their whole tables, unstamped —
        // the recovered upsert state absorbs every row it already holds.
        var referenceRows = await live.ReplayMasterDataAsync(director);
        await live.FeedAsync(director.OrderLog, director.PaymentLog);   // full replay, from offset 0
        await live.StepTimedAsync();
        await live.StepTimedAsync();

        var after = Views.Snapshot(live.CategoryStats);
        for (var attempt = 0; attempt < 40 && !SnapshotsEqual(before, after); attempt++)
        {
            await Task.Delay(50);           // the view integrates its bootstrap + replay batches asynchronously
            after = Views.Snapshot(live.CategoryStats);
        }
        var recovered = SnapshotsEqual(before, after);
        recovery = new RecoveryView(generation, committed, total, deduped, referenceRows, recovered,
            after.Count, after.Values.Sum(s => s.Gross), beforeGross);
        Notices.Push(recovered ? NoticeLevel.Good : NoticeLevel.Bad, recovered
            ? $"recovered exactly-once: state + offsets from the checkpoint, replay deduplicated, {referenceRows:N0} reference rows re-fed without double-counting, consumers bootstrapped"
            : "recovery verification FAILED");
    }

    async Task MeasureBeat()
    {
        Running = false;         // the measurement must not compete with the live circuit
        Notices.Push(NoticeLevel.Info, "measuring 1 / 2 / 4 workers on this machine; traffic paused, the view holds briefly");
        scaling = await ScalingAct.MeasureAllAsync(Topology);
        var best = scaling.Samples.MinBy(s => s.LatencyP50)!;
        var fastest = scaling.Samples.MaxBy(s => s.BulkRowsPerSec)!;
        Notices.Push(NoticeLevel.Good,
            $"measured: lowest live-batch latency at {best.Workers} worker{(best.Workers == 1 ? "" : "s")} " +
            $"({best.LatencyP50:N0} µs p50); highest bulk throughput at {fastest.Workers} workers " +
            $"({fastest.BulkRowsPerSec / 1_000_000:N2} M rows/s) — sharding buys throughput, not latency");
    }

    async Task ClusterBeat()
    {
        Notices.Push(NoticeLevel.Info,
            $"spawning {Topology.Nodes - 1} peer process{(Topology.Nodes == 2 ? "" : "es")} — TCP mesh, lockstep steps; the view freezes for a few seconds");
        cluster = await ClusterAct.RunParentAsync(Notices, Metrics, Topology.Nodes, Topology.Workers);
    }

    static bool SnapshotsEqual(Dictionary<string, FeeStats> a, Dictionary<string, FeeStats> b) =>
        a.Count == b.Count && a.All(kv => b.TryGetValue(kv.Key, out var v) && v == kv.Value);

    // ── tabs ───────────────────────────────────────────────────────────────────

    Tab[] BuildTabs()
    {
        void Add(Beat beat) => beats[beat.Id] = beat;

        Add(new Beat { Id = "correct", Label = "correct the miscategorized merchant (again toggles it back)", Fire = CorrectionBeat });
        Add(new Beat { Id = "orphan", Label = "post an orphan payment", Fire = OrphanBeat });
        Add(new Beat { Id = "onboard", Label = "onboard the unknown merchant", Fire = OnboardBeat, Once = true });
        Add(new Beat { Id = "late", Label = "send an order 4 s late — inside lateness, corrects its window", Fire = LateBeat });
        Add(new Beat { Id = "flash", Label = "start a flash sale (12 ticks of 3× volume)", Fire = FlashBeat });
        Add(new Beat { Id = "drop", Label = "send an order 45 s late — beyond lateness, dropped", Fire = DropBeat });
        Add(new Beat { Id = "link-a", Label = "link accounts 21·22·23 (ring A)", Fire = LinkABeat, Once = true });
        Add(new Beat { Id = "link-b", Label = "link accounts 31·32·33 (ring B)", Fire = LinkBBeat, Once = true });
        Add(new Beat
        {
            Id = "bridge", Label = "add the bridge edge 23↔31 — the rings merge", Fire = BridgeBeat,
            // Both rings must exist first, and the bridge must not already be there.
            Unavailable = () =>
                !director.Edges.ContainsKey((22, 23)) || !director.Edges.ContainsKey((32, 33))
                    ? "link both rings first"
                    : director.Edges.ContainsKey((23, 31)) ? "the bridge is already in place" : null,
        });
        Add(new Beat
        {
            Id = "withdraw", Label = "withdraw the bridge — the closure unwinds", Fire = WithdrawBeat,
            Unavailable = () => director.Edges.ContainsKey((23, 31)) ? null : "add the bridge first",
        });
        Add(new Beat { Id = "crash", Label = "kill -9 the circuit and rebuild from the checkpoint", Fire = CrashBeat });
        Add(new Beat { Id = "measure", Label = "measure 1 / 2 / 4 workers on this machine", Fire = MeasureBeat });
        Add(new Beat
        {
            Id = "cluster",
            Label = $"run {Topology.Nodes} OS processes × {Topology.Workers} workers over TCP and verify the shards",
            Fire = ClusterBeat,
        });

        return
        [
            new Tab
            {
                Id = "living", Title = "living data",
                Explainer = """
                    Orders stream in on two channels (web + mobile); merchants, customers and the fee
                    schedule are keyed upsert inputs. Everything below is a join or an aggregate over
                    the same enriched stream. Master data can be wrong — the busiest merchant was
                    onboarded under the wrong category — and fixing it is one upsert: every historical
                    rollup that depended on the bad row is retracted and re-asserted, never recomputed.
                    (A correction rewrites history by design; a fee change would not — that applies
                    going forward only, so it is deliberately not a trigger here.)
                    """,
                Code = """
                    var enriched = orders                                    // = web.Concat(mobile)
                        .GroupBy(o => o.MerchantId).Join(merchants.GroupBy(m => m.Id), …)
                        .GroupBy(x => x.FeeTier)   .Join(feeRates.GroupBy(f => f.Tier), …)
                        .GroupBy(e => e.CustomerId).LeftJoin(customers.GroupBy(c => c.Id), …);
                    var stats  = enriched.GroupBy(e => e.Category).AggregateLinear(new FeeTakeAggregate());
                    var buyers = enriched.Select(e => new BuyerRow(e.MerchantId, e.CustomerId))
                                         .GroupBy(b => b.MerchantId).Distinct().Grouped().Count();
                    var held   = orders.GroupBy(o => o.MerchantId).AntiJoin(merchants.GroupBy(m => m.Id));
                    var recon  = orders.GroupBy(o => o.Id).FullJoin(payments.GroupBy(p => p.OrderId), …);
                    """,
                Beats = ["correct", "orphan", "onboard"],
            },
            new Tab
            {
                Id = "time", Title = "time",
                Explainer = """
                    The same order stream, now with declared event time. The live leaderboard flaps by
                    design (every update is retract + assert); its OnClose() twin emits each window
                    once, final. Closed windows evict their state, so this view runs forever in
                    constant memory. The watermark (max event time − lateness, min across sources) is
                    in the header. Triggers: a late-but-in-time event correcting an open window, a
                    flash-sale surge, and a 45 s-late event that is dropped at ingest.
                    """,
                Code = """
                    var timed   = orders.WithEventTime(o => o.At, lateness: 5s);
                    var leaders = timed.GroupBy(o => o.MerchantId)
                                       .TumblingWindow(10s).Sum(o => o.Amount).TopK(3, w => w.Value);
                    var liveBoard  = leaders.ToOutput();               // corrected live — flaps
                    var finalBoard = leaders.OnClose().ToOutput(…);    // exactly once, at window close
                    var rate = payments.WithEventTime(p => p.At, 5s).GroupBy(_ => "gateway")
                                       .HoppingWindow(30s, hop: 5s).AggregateLinear(new AcceptanceAggregate());
                    var sessions = timed.GroupBy(o => o.CustomerId).SessionWindow(gap: 20s)
                                        .AggregateLinear(new SpendAggregate());
                    """,
                Beats = ["late", "flash", "drop"],
            },
            new Tab
            {
                Id = "graph", Title = "recursion",
                Explainer = """
                    Why a graph? A fraudster does not open one account, they open twenty, and the
                    accounts give themselves away by sharing physical things: a card, a device
                    fingerprint, a shipping address. Any single shared card is unremarkable — people
                    share cards. What matters is the whole connected group, because that is the unit
                    that gets blocked, charged back and reported. Finding it is not a join: a join
                    reaches one hop, and you do not know in advance whether the account funding this
                    order is one hop from a known-bad account or five. You need "everything reachable
                    from here", which is recursion.

                    Recurse computes a least fixed point: apply a rule to what you have, add
                    whatever is new, repeat until a pass adds nothing. Here the rule is "if A links
                    to B and B links to C, then A links to C" — the transitive closure of the
                    linkage graph. Rings are its connected components, and an account's ring id is
                    the smallest account it can reach.

                    Nothing about the operator is fraud-specific. The same fixed point derives any
                    rule that feeds on its own output: bill-of-materials explosion (a part contains
                    parts that contain parts), org-chart and category rollups to any depth,
                    package or task dependency resolution, ancestry and reachability, "who can see
                    this document" through nested groups. What is unusual is that it stays
                    incremental: the fixed point is maintained under deltas, so evidence can arrive
                    and — the hard half — be withdrawn. Link ring A, link ring B, bridge them into one
                    six-account ring, then withdraw the bridge — one DeleteAsync unwinds every derived
                    row that depended on it, in well-founded order, including orders flagged long ago.
                    Recursive derivations that cannot retract are usually recomputed from scratch;
                    this one is not.
                    """,
                Code = """
                    var links = edges.SelectMany(e => new[] { new Link(e.A, e.B), new Link(e.B, e.A) });
                    var reach = links.Recurse(paths => paths.GroupBy(p => p.To)
                                     .Join(links.GroupBy(l => l.From), (_, p, l) => new Link(p.From, l.To)));
                    var ring  = reach.Concat(links.Select(l => new Link(l.From, l.From)))
                                     .GroupBy(l => l.From).Min(l => l.To);       // ring id per account
                    var exposure = orders.GroupBy(o => o.CustomerId).Join(ring.Grouped(), …)
                                         .GroupBy(f => f.Ring).Sum(f => f.Amount);
                    """,
                Beats = ["link-a", "link-b", "bridge", "withdraw"],
            },
            new Tab
            {
                Id = "kill", Title = "kill it",
                Explainer = $"""
                    The trigger abandons the circuit object mid-flight — no dispose, no flush: a
                    simulated kill -9. Its checkpoints (every 4 steps: all operator state + consumed
                    input offsets, one atomic commit) live in {StorageLabel}. The rebuild starts
                    against the same provider, the source logs replay from the beginning, and the
                    circuit drops everything at-or-below the committed offsets on arrival. The keyed
                    reference inputs are simply re-fed whole: their current rows are checkpointed
                    state too, so re-upserting a row the circuit already holds changes nothing. The
                    panel below verifies the rebuilt state against a pre-crash snapshot. Fire it as
                    often as you like — each crash starts a new generation.
                    """,
                Code = """
                    services.AddIncremental<ShowcaseDefs>()
                        .Configure(o => { o.Storage = provider; o.CheckpointInterval = 4; … });
                    await host.StartAsync();                                // restores the latest checkpoint
                    var committed = circuit.CommittedOffset("orders-web");
                    await web.InsertAsync(order, offset: o.Offset);         // ≤ committed ⇒ dropped on arrival
                    await merchants.UpsertAsync(merchant);                  // already held ⇒ no-op
                    """,
                Beats = ["crash"],
            },
            new Tab
            {
                Id = "cluster", Title = "scaling out",
                Explainer = """
                    One declaration, two knobs: threads in this process and processes. Both are CircuitOptions — the
                    pipeline code never mentions any of them. The running-configuration card shows
                    what this process is running; compose another one and apply it, and the
                    showcase relaunches into it in one go.

                    Threads first, because sharding is not free. Above one worker every GroupBy
                    inserts an exchange, and an exchange costs a hash, a per-worker queue and a
                    barrier on every step, whether or not there is enough work to amortize it. At
                    the showcase's live batch size — a handful of orders a tick — that overhead is
                    most of the step, so one worker has the lowest latency; feed the same pipeline a
                    bulk batch and the exchange is what lets the extra workers earn their keep. The
                    trigger measures both on this machine, at 1, 2 and 4 workers, rather than asking
                    you to take the claim on trust. Multi-worker rows rendezvous every worker at each
                    exchange barrier. The relaunch is what gets the GC heap count sized to the new worker count; the rows measured
                    in one process all share this process's heap count, so treat them as comparing
                    exchange cost, not as tuned per-topology throughput.

                    Processes second. Nodes above one respawn this binary as peers; every process
                    builds the identical circuit and only CircuitOptions.Cluster tells each its
                    place. Exchanges then hash-partition by StableHash across the nodes × workers
                    global workers and ship remote shards over TCP (unauthenticated cleartext —
                    loopback only here), with steps in lockstep everywhere. The cluster shuffle is
                    barrier-based, and Recurse is
                    rejected in clustered circuits, so that run uses its own small revenue pipeline.
                    """,
                Code = """
                    // threads: the only change is a number. No pipeline code moves.
                    new CircuitOptions { Workers = 4 };

                    // processes: same declaration on every peer, each told its own index.
                    new CircuitOptions {
                        Workers = 2, AutoStep = false,               // lockstep StepAsync on every process
                        Cluster = new ClusterOptions { Transport = new TcpExchangeTransport(index, endpoints) },
                    };
                    """,
                Beats = Topology.Nodes > 1 ? ["measure", "cluster"] : ["measure"],
            },
        ];
    }

    public async ValueTask DisposeAsync()
    {
        commands.Writer.TryComplete();
        await live.ShutdownAsync();
    }
}
