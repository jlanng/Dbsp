namespace Incremental.Showcase;

/// <summary>
/// The deterministic "director": seeded background traffic plus the scripted beats
/// (master-data corrections, late events, fraud edges, the crash) fired from the browser.
/// It owns the simulated event-time clock and keeps the current master data and
/// offset-stamped source logs so Act 4 can replay everything after the crash.
/// </summary>
public sealed class Director
{
    public static readonly TimeSpan Tick = TimeSpan.FromSeconds(1);

    readonly Random rng = new(42);

    public DateTimeOffset VirtualNow { get; private set; }

    // Master data: current truth, upserted into keyed inputs (and re-upserted wholesale on
    // rebuild — keyed inputs are full-replay sources after a restart).
    public List<Merchant> Merchants { get; } = [];
    public List<Customer> Customers { get; } = [];
    public List<FeeRate> FeeRates { get; } = [];
    public Dictionary<(int A, int B), LinkEdge> Edges { get; } = [];

    // Offset-stamped source logs for Act 4 replay.
    public List<StampedOrder> OrderLog { get; } = [];
    public List<StampedPayment> PaymentLog { get; } = [];

    long nextOrderId = 5000;
    long nextWebOffset;
    long nextMobileOffset;
    long nextPaymentOffset;
    readonly Queue<(int DueTick, Payment Payment)> duePayments = new();
    readonly List<(int CustomerId, int Remaining)> bursts = [];

    /// <summary>Ticks remaining of flash-sale surge traffic (3× volume, lower acceptance).</summary>
    public int SurgeTicksRemaining { get; set; }

    /// <summary>While true, a slice of orders reference merchants that are not in master data yet.</summary>
    public bool UnknownMerchantsActive { get; set; } = true;

    static readonly string[] Categories = ["electronics", "fashion", "grocery", "home", "toys", "books"];
    static readonly string[] Regions = ["emea", "amer", "apac"];
    static readonly string[] First = ["Alba", "Bram", "Cleo", "Dara", "Ezra", "Faye", "Gil", "Hana", "Iris", "Joss", "Kai", "Lena", "Miro", "Nia", "Otto", "Pia", "Quin", "Rue", "Sol", "Tess"];
    static readonly string[] Shops = ["Atlas", "Beacon", "Cinder", "Drift", "Ember", "Fable", "Grove", "Harbor", "Ivory", "Juniper", "Kestrel", "Lumen", "Meadow", "Nimbus", "Orchard", "Pines", "Quarry", "Rustic", "Summit", "Tidal", "Umber", "Vale", "Willow", "Yarrow"];
    static readonly string[] Wares = ["Audio", "Threads", "Pantry", "Living", "Playhouse", "Pages"];

    public const int MissingMerchantId = 901;   // referenced by orders before it is onboarded
    public static readonly int[] RingA = [21, 22, 23];
    public static readonly int[] RingB = [31, 32, 33];

    public Director(DateTimeOffset start)
    {
        VirtualNow = start;
        for (var i = 0; i < 24; i++)
        {
            var category = Categories[i % Categories.Length];
            Merchants.Add(new Merchant(100 + i, $"{Shops[i]} {Wares[i % Wares.Length]}", category, FeeTierFor(i)));
        }
        for (var i = 0; i < 80; i++)
            Customers.Add(new Customer(1 + i, $"{First[i % First.Length]} {(char)('A' + i / First.Length)}.", Regions[i % Regions.Length]));
        FeeRates.Add(new FeeRate(1, 0.015m));
        FeeRates.Add(new FeeRate(2, 0.022m));
        FeeRates.Add(new FeeRate(3, 0.031m));
        // The busiest merchant is onboarded under the wrong category — tab 1's
        // master-data-correction beat fixes it live (a case where retroactive
        // re-attribution is the *correct* behavior, unlike, say, a fee change).
        Merchants[0] = Merchants[0] with { Category = "fashion" };
    }

    static int FeeTierFor(int i) => 1 + i % 3;

    public void AdvanceClock() => VirtualNow += Tick;

    /// <summary>Background order traffic for one tick, offset-stamped and logged.</summary>
    public List<StampedOrder> EmitOrders(int tick)
    {
        var emitted = new List<StampedOrder>();
        var surge = SurgeTicksRemaining > 0;
        if (surge) SurgeTicksRemaining--;
        var count = (surge ? 9 : 3) + rng.Next(surge ? 6 : 3);

        // Session bursts: a shopper placing several orders inside one inactivity gap.
        if (rng.NextDouble() < 0.25)
            bursts.Add((PickCustomer(), 2 + rng.Next(4)));
        for (var i = bursts.Count - 1; i >= 0; i--)
        {
            var (customer, remaining) = bursts[i];
            emitted.Add(MakeOrder(tick, customer, PickMerchant()));
            if (remaining <= 1) bursts.RemoveAt(i); else bursts[i] = (customer, remaining - 1);
        }

        for (var i = 0; i < count; i++)
        {
            var merchant = UnknownMerchantsActive && rng.NextDouble() < 0.06
                ? MissingMerchantId
                : PickMerchant();
            emitted.Add(MakeOrder(tick, PickCustomer(), merchant));
        }
        return emitted;
    }

    StampedOrder MakeOrder(int tick, int customerId, int merchantId, TimeSpan lateBy = default)
    {
        var items = 1 + rng.Next(4);
        var amount = items * (5m + Math.Round((decimal)rng.NextDouble() * 120m, 2));
        var channel = rng.NextDouble() < 0.6 ? "web" : "mobile";
        var jitter = TimeSpan.FromMilliseconds(rng.Next(900));
        var order = new Order(nextOrderId++, customerId, merchantId, channel, items, amount, VirtualNow - lateBy + jitter);
        var stamped = new StampedOrder(channel, order,
            channel == "web" ? nextWebOffset++ : nextMobileOffset++);
        OrderLog.Add(stamped);

        if (rng.NextDouble() < 0.97)     // a few orders never get a payment: recon "awaiting"
        {
            var declineRate = SurgeTicksRemaining > 0 ? 0.22 : 0.06;
            var status = rng.NextDouble() < declineRate ? "declined" : "approved";
            duePayments.Enqueue((tick + 1 + rng.Next(3),
                new Payment(order.Id, status, order.Amount, order.At + TimeSpan.FromSeconds(1 + rng.Next(2)))));
        }
        return stamped;
    }

    /// <summary>Payments that fall due this tick, offset-stamped and logged.</summary>
    public List<StampedPayment> EmitPayments(int tick)
    {
        var emitted = new List<StampedPayment>();
        while (duePayments.Count > 0 && duePayments.Peek().DueTick <= tick)
        {
            var payment = duePayments.Dequeue().Payment;
            // keep payment event time from stalling the watermark behind the order feed
            var stamped = new StampedPayment(payment with { At = Later(payment.At) }, nextPaymentOffset++);
            PaymentLog.Add(stamped);
            emitted.Add(stamped);
        }
        return emitted;
    }

    DateTimeOffset Later(DateTimeOffset at) => at > VirtualNow ? at : VirtualNow;

    // ── scripted beats ──────────────────────────────────────────────────────────

    public StampedOrder EmitLateOrder(int tick, TimeSpan lateBy) =>
        MakeOrder(tick, PickCustomer(), PickMerchant(), lateBy);

    long nextOrphanOrderId = 999_001;   // ids no order will ever use — the beat is re-triggerable

    public StampedPayment EmitOrphanPayment()
    {
        var payment = new Payment(nextOrphanOrderId++, "approved", 74.50m, VirtualNow);
        var stamped = new StampedPayment(payment, nextPaymentOffset++);
        PaymentLog.Add(stamped);
        return stamped;
    }

    public (Merchant Old, Merchant New) Recategorize(int merchantId, string category)
    {
        var index = Merchants.FindIndex(m => m.Id == merchantId);
        var old = Merchants[index];
        var updated = old with { Category = category };
        Merchants[index] = updated;
        return (old, updated);
    }

    public Merchant OnboardMissingMerchant()
    {
        var merchant = new Merchant(MissingMerchantId, "Vagrant Vendors", "electronics", 2);
        Merchants.Add(merchant);
        UnknownMerchantsActive = false;
        return merchant;
    }

    public LinkEdge AddEdge(int a, int b, string reason)
    {
        var edge = new LinkEdge(a, b, reason);
        Edges[(a, b)] = edge;
        return edge;
    }

    public LinkEdge RemoveEdge(int a, int b)
    {
        var edge = Edges[(a, b)];
        Edges.Remove((a, b));
        return edge;
    }

    /// <summary>An order from a specific customer (used to give ring accounts live exposure).</summary>
    public StampedOrder EmitOrderFor(int tick, int customerId) => MakeOrder(tick, customerId, PickMerchant());

    // ~4% of orders come from accounts that are not in customer master data — they take
    // the LeftJoin's unmatched branch and land in the "guest" region.
    int PickCustomer() => rng.NextDouble() < 0.04 ? 900 + rng.Next(5) : Customers[Skewed(Customers.Count)].Id;
    int PickMerchant() => Merchants[Skewed(Merchants.Count)].Id;

    // Squared uniform: concentrates traffic on low indices so leaderboards have shape.
    int Skewed(int count) => (int)(rng.NextDouble() * rng.NextDouble() * count);
}
