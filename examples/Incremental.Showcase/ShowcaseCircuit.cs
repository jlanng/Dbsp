using System.Diagnostics;
using Incremental;
using Incremental.Storage;
using Microsoft.Extensions.Logging;

namespace Incremental.Showcase;

/// <summary>
/// One live generation of the showcase circuit: the built circuit, every redeemed handle,
/// the materialized views, and the synchronously drained readers. Act 4 abandons one
/// instance (the crash) and constructs the next against the same storage provider.
/// </summary>
public sealed class ShowcaseCircuit
{
    public readonly Circuit Circuit;
    public readonly ShowcaseDefs Defs;

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

    public readonly int Workers;

    public ShowcaseCircuit(string name, IStorageProvider storage, ILoggerFactory loggerFactory, int workers = 2)
    {
        Workers = workers;
        var builder = new CircuitBuilder(new CircuitOptions
        {
            Name = name,
            Workers = workers,           // >1: exchanges really shuffle, state is sharded per worker
            AutoStep = false,            // explicit steps: exact boundaries for timing and beats
            Storage = storage,
            CheckpointInterval = 4,      // a crash loses up to 3 steps — replay covers the gap
            LoggerFactory = loggerFactory,
        });
        Defs = ShowcasePipeline.Declare(builder);
        Circuit = builder.Build();
        Circuit.Start();                 // restores from the latest checkpoint if one exists

        Web = Circuit.Handle(Defs.WebOrders);
        Mobile = Circuit.Handle(Defs.MobileOrders);
        Payments = Circuit.Handle(Defs.Payments);
        Merchants = Circuit.Handle(Defs.Merchants);
        Customers = Circuit.Handle(Defs.Customers);
        FeeRates = Circuit.Handle(Defs.FeeRates);
        Edges = Circuit.Handle(Defs.Edges);

        CategoryStats = Circuit.Handle(Defs.CategoryStats).MaterializeInMemory();
        CategoryMin = Circuit.Handle(Defs.CategoryMin).MaterializeInMemory();
        CategoryMax = Circuit.Handle(Defs.CategoryMax).MaterializeInMemory();
        TopOrders = Circuit.Handle(Defs.TopOrders).MaterializeInMemory();
        MerchantOrders = Circuit.Handle(Defs.MerchantOrders).MaterializeInMemory();
        MerchantRevenue = Circuit.Handle(Defs.MerchantRevenue).MaterializeInMemory();
        MerchantBuyers = Circuit.Handle(Defs.MerchantBuyers).MaterializeInMemory();
        MerchantItems = Circuit.Handle(Defs.MerchantItems).MaterializeInMemory();
        RegionRevenue = Circuit.Handle(Defs.RegionRevenue).MaterializeInMemory();
        Compliance = Circuit.Handle(Defs.Compliance).MaterializeInMemory();
        Recon = Circuit.Handle(Defs.Recon).MaterializeInMemory();
        LiveLeaders = Circuit.Handle(Defs.LiveLeaders).MaterializeInMemory();
        AcceptanceRate = Circuit.Handle(Defs.AcceptanceRate).MaterializeInMemory();
        Sessions = Circuit.Handle(Defs.Sessions).MaterializeInMemory();
        RingSizes = Circuit.Handle(Defs.RingSizes).MaterializeInMemory();
        RingExposure = Circuit.Handle(Defs.RingExposure).MaterializeInMemory();

        EnrichedFeed = Circuit.Handle(Defs.EnrichedFeed);
        FinalLeaders = Circuit.Handle(Defs.FinalLeaders);
        RingMembership = Circuit.Handle(Defs.RingMembership);
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

    public void CompleteInputs()
    {
        Web.Complete();
        Mobile.Complete();
        Payments.Complete();
        Merchants.Complete();
        Customers.Complete();
        FeeRates.Complete();
        Edges.Complete();
    }

    /// <summary>Graceful shutdown — the crash path never calls this.</summary>
    public async ValueTask ShutdownAsync()
    {
        CompleteInputs();
        await Circuit.DisposeAsync();
        foreach (var view in new IAsyncDisposable[]
        {
            CategoryStats, CategoryMin, CategoryMax, TopOrders, MerchantOrders, MerchantRevenue,
            MerchantBuyers, MerchantItems, RegionRevenue, Compliance, Recon, LiveLeaders,
            AcceptanceRate, Sessions, RingSizes, RingExposure,
        })
            await view.DisposeAsync();
    }
}
