using System.Diagnostics;
using Incremental;
using Incremental.Hosting;
using Incremental.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Incremental.Showcase;

/// <summary>
/// One live generation of the showcase circuit: a generic host around
/// <c>AddIncremental&lt;ShowcaseDefs&gt;()</c>, every redeemed handle, the host's shared views,
/// and the synchronously drained readers. A generation is a host of its own, rather than a
/// service of the web app's, because act 4 kills it: the crash abandons one host without
/// stopping it and starts the next against the same storage provider — which is also why
/// the headless run, with no web app at all, hosts the circuit exactly the same way.
/// </summary>
public sealed class ShowcaseCircuit
{
    readonly IHost host;
    public readonly Circuit Circuit;

    public readonly InputHandle<Order> Web;
    public readonly InputHandle<Order> Mobile;
    public readonly InputHandle<Payment> Payments;
    public readonly KeyedInputHandle<int, Merchant> Merchants;
    public readonly KeyedInputHandle<int, Customer> Customers;
    public readonly KeyedInputHandle<int, FeeRate> FeeRates;
    public readonly KeyedInputHandle<(int A, int B), LinkEdge> Edges;

    // Table-shaped outputs consumed as live in-memory views (each is that output's single
    // consumer; batch zero after a rebuild is the automatic bootstrap).
    public readonly InMemoryView<KeyedValue<string, FeeStats>> CategoryStats;
    public readonly InMemoryView<KeyedValue<string, decimal>> CategoryMin;
    public readonly InMemoryView<KeyedValue<string, decimal>> CategoryMax;
    public readonly InMemoryView<KeyedValue<string, EnrichedOrder>> TopOrders;
    public readonly InMemoryView<KeyedValue<int, long>> MerchantOrders;
    public readonly InMemoryView<KeyedValue<int, decimal>> MerchantRevenue;
    public readonly InMemoryView<KeyedValue<int, long>> MerchantBuyers;
    public readonly InMemoryView<KeyedValue<int, long>> MerchantItems;
    public readonly InMemoryView<KeyedValue<string, decimal>> RegionRevenue;
    public readonly InMemoryView<KeyedValue<int, Order>> Compliance;
    public readonly InMemoryView<KeyedValue<string, long>> Recon;
    public readonly InMemoryView<WindowedValue<int, decimal>> LiveLeaders;
    public readonly InMemoryView<WindowedValue<string, Acceptance>> AcceptanceRate;
    public readonly InMemoryView<WindowedValue<int, Spend>> Sessions;
    public readonly InMemoryView<KeyedValue<int, long>> RingSizes;
    public readonly InMemoryView<KeyedValue<int, decimal>> RingExposure;

    // Step-timed feeds, drained synchronously right after StepAsync (publish completes
    // inside the step), for per-step delta counts and append-only lists.
    public readonly OutputHandle<EnrichedOrder> EnrichedFeed;
    public readonly OutputHandle<WindowedValue<int, decimal>> FinalLeaders;
    public readonly OutputHandle<KeyedValue<int, int>> RingMembership;

    public readonly List<WindowedValue<int, decimal>> Finals = [];
    public readonly Dictionary<int, int> Rings = [];

    /// <summary>The tail of the raw enriched changefeed, weights included — the one panel
    /// that shows deltas rather than an integrated view.</summary>
    public readonly List<FeedRow> Feed = [];
    const int FeedCap = 14;
    long feedSeq;

    public double LastStepMicros;
    public int LastEnrichedDelta;
    public int LastRingDelta;

    /// <summary>Every step's wall time in microseconds, cumulative, plus a rolling window of
    /// the most recent ones — the header's p50 / p95 and the exchange-wait share (wait per
    /// worker over summed step time) are read off these, so the effect of a topology shows
    /// on the live pipeline itself, not only in the measurement table.</summary>
    public double TotalStepMicros;
    public long StepCount;
    const int WindowSize = 300;
    readonly double[] window = new double[WindowSize];

    /// <summary>p50 and p95 of the last <see cref="WindowSize"/> steps (fewer while warming
    /// up), and how many steps that is.</summary>
    public (double P50, double P95, int Steps) StepPercentiles()
    {
        var n = (int)Math.Min(StepCount, WindowSize);
        if (n == 0) return (0, 0, 0);
        var sorted = new double[n];
        Array.Copy(window, sorted, n);
        Array.Sort(sorted);
        return (sorted[n / 2], sorted[Math.Min(n - 1, (int)(n * 0.95))], n);
    }

    public readonly int Workers;

    public static async Task<ShowcaseCircuit> StartAsync(
        string name, IStorageProvider storage, ILoggerFactory loggerFactory, int workers = 2)
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());
        // A guest of this process: the web host (or the headless run) owns Ctrl-C.
        builder.Services.AddSingleton<IHostLifetime, GuestLifetime>();
        builder.Services
            .AddIncremental<ShowcaseDefs>()
            .Configure(o =>
            {
                o.Name = name;
                o.Workers = workers;         // >1: exchanges really shuffle, state is sharded per worker
                o.AutoStep = false;          // the app steps explicitly: exact boundaries for timing and beats
                o.Storage = storage;
                o.CheckpointInterval = 4;    // a crash loses up to 3 steps — replay covers the gap
                o.LoggerFactory = loggerFactory;   // circuit logs go to the notice feed
            });
        var host = builder.Build();
        await host.StartAsync();             // restores from the latest checkpoint if one exists
        return new ShowcaseCircuit(host, workers);
    }

    ShowcaseCircuit(IHost host, int workers)
    {
        this.host = host;
        Workers = workers;
        var circuit = host.Services.GetRequiredService<CircuitHost<ShowcaseDefs>>();
        Circuit = circuit.Circuit;

        Web = circuit.Handle(d => d.WebOrders);
        Mobile = circuit.Handle(d => d.MobileOrders);
        Payments = circuit.Handle(d => d.Payments);
        Merchants = circuit.Handle(d => d.Merchants);
        Customers = circuit.Handle(d => d.Customers);
        FeeRates = circuit.Handle(d => d.FeeRates);
        Edges = circuit.Handle(d => d.Edges);

        CategoryStats = circuit.View(d => d.CategoryStats);
        CategoryMin = circuit.View(d => d.CategoryMin);
        CategoryMax = circuit.View(d => d.CategoryMax);
        TopOrders = circuit.View(d => d.TopOrders);
        MerchantOrders = circuit.View(d => d.MerchantOrders);
        MerchantRevenue = circuit.View(d => d.MerchantRevenue);
        MerchantBuyers = circuit.View(d => d.MerchantBuyers);
        MerchantItems = circuit.View(d => d.MerchantItems);
        RegionRevenue = circuit.View(d => d.RegionRevenue);
        Compliance = circuit.View(d => d.Compliance);
        Recon = circuit.View(d => d.Recon);
        LiveLeaders = circuit.View(d => d.LiveLeaders);
        AcceptanceRate = circuit.View(d => d.AcceptanceRate);
        Sessions = circuit.View(d => d.Sessions);
        RingSizes = circuit.View(d => d.RingSizes);
        RingExposure = circuit.View(d => d.RingExposure);

        EnrichedFeed = circuit.Handle(d => d.EnrichedFeed);
        FinalLeaders = circuit.Handle(d => d.FinalLeaders);
        RingMembership = circuit.Handle(d => d.RingMembership);
    }

    public async ValueTask FeedAsync(IEnumerable<StampedOrder> orders, IEnumerable<StampedPayment> payments)
    {
        foreach (var o in orders)
            await (o.Channel == "web" ? Web : Mobile).InsertAsync(o.Order, offset: o.Offset);
        foreach (var p in payments)
            await Payments.InsertAsync(p.Payment, offset: p.Offset);
    }

    public async ValueTask ReplayMasterDataAsync(Director director)
    {
        foreach (var m in director.Merchants) await Merchants.UpsertAsync(m);
        foreach (var c in director.Customers) await Customers.UpsertAsync(c);
        foreach (var f in director.FeeRates) await FeeRates.UpsertAsync(f);
        foreach (var e in director.Edges.Values) await Edges.UpsertAsync(e);
    }

    /// <summary>One step, timed, with all step-level readers drained.</summary>
    public async Task<(double Micros, int EnrichedDelta)> StepTimedAsync()
    {
        var timer = Stopwatch.StartNew();
        await Circuit.StepAsync();
        timer.Stop();
        LastStepMicros = timer.Elapsed.TotalMicroseconds;
        TotalStepMicros += LastStepMicros;
        window[StepCount++ % WindowSize] = LastStepMicros;

        var enrichedDelta = 0;
        var stepRows = new List<FeedRow>();
        while (EnrichedFeed.Reader.TryRead(out var batch))
        {
            enrichedDelta += batch.DistinctCount;
            foreach (var (row, weight) in batch)
                stepRows.Add(new FeedRow(feedSeq++, row.OrderId, row.Merchant, row.Category, row.Amount, row.Fee, weight));
        }
        // A correction emits hundreds of rows in one step. Sample both signs rather than
        // the tail, so the retract-then-assert pair stays visible — that is the point of
        // the panel. Ordinary steps are far under the cap and pass through whole.
        var retracts = stepRows.Where(r => r.Weight < 0).Take(FeedCap / 2).ToList();
        Feed.AddRange(retracts);
        Feed.AddRange(stepRows.Where(r => r.Weight > 0).Take(FeedCap - retracts.Count));
        if (Feed.Count > FeedCap) Feed.RemoveRange(0, Feed.Count - FeedCap);
        LastEnrichedDelta = enrichedDelta;

        while (FinalLeaders.Reader.TryRead(out var batch))
            foreach (var (row, weight) in batch)
                if (weight > 0)
                    Finals.Add(row);
        if (Finals.Count > 30) Finals.RemoveRange(0, Finals.Count - 30);

        var ringDelta = 0;
        var asserts = new List<KeyedValue<int, int>>();
        while (RingMembership.Reader.TryRead(out var batch))
            foreach (var (row, weight) in batch)
            {
                ringDelta++;
                if (weight > 0) asserts.Add(row);
                else if (Rings.TryGetValue(row.Key, out var ring) && ring == row.Value) Rings.Remove(row.Key);
            }
        foreach (var row in asserts) Rings[row.Key] = row.Value;   // asserts win over same-step retracts
        LastRingDelta = ringDelta;

        return (LastStepMicros, enrichedDelta);
    }

    /// <summary>Graceful shutdown — the crash path never calls this. Stopping the host
    /// completes the inputs, drains the circuit while the views keep reading, then disposes
    /// them.</summary>
    public async ValueTask ShutdownAsync()
    {
        await host.StopAsync();
        host.Dispose();
    }

    sealed class GuestLifetime : IHostLifetime
    {
        public Task WaitForStartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
