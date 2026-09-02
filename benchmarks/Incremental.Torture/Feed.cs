using Incremental.Showcase;

namespace Incremental.Torture;

/// <summary>
/// Deterministic traffic generator for the torture run. Master data is seeded once; order
/// traffic is a <em>sliding</em> population — inserting order N retracts order N−Live — so
/// operator state settles at a fixed size instead of growing for as long as the run lasts.
/// That is what makes a long run measurable: throughput is not confounded by dictionaries
/// that keep getting bigger, and the retraction half of every step exercises the
/// negative-weight paths that a pure-insert benchmark never touches.
/// </summary>
internal sealed class Feed
{
    private readonly Options _options;
    private ulong _rng;

    // The live population, as a ring: slot i holds the order (and its payment, if any)
    // that must be retracted when the ring wraps back around to i.
    private readonly Order?[] _liveOrders;
    private readonly Payment?[] _livePayments;
    private long _issued;

    private long _nextOrderId = 1;
    private DateTimeOffset _eventTime;

    public Feed(Options options)
    {
        _options = options;
        _rng = 0x9E3779B97F4A7C15ul ^ (ulong)options.Seed;
        _liveOrders = new Order?[options.LiveOrders];
        _livePayments = new Payment?[options.LiveOrders];
        _eventTime = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

        Merchants = new Merchant[options.Merchants];
        for (var i = 0; i < options.Merchants; i++)
            Merchants[i] = new Merchant(
                1000 + i,
                $"Merchant {i}",
                $"cat-{i % options.Categories}",
                1 + i % options.FeeTiers);

        Customers = new Customer[options.Customers];
        for (var i = 0; i < options.Customers; i++)
            Customers[i] = new Customer(1 + i, $"Customer {i}", $"region-{i % options.Regions}");

        FeeRates = new FeeRate[options.FeeTiers];
        for (var i = 0; i < options.FeeTiers; i++)
            FeeRates[i] = new FeeRate(1 + i, 0.010m + i % 20 * 0.001m);

        // Linkage evidence, kept to small disjoint rings: the transitive closure is a
        // fixed-point loop, so a dense graph would make it the whole benchmark.
        var edges = new List<LinkEdge>();
        for (var ring = 0; ring < options.Rings; ring++)
        {
            var first = 1 + ring * 7 % Math.Max(1, options.Customers - 4);
            for (var k = 0; k < 3; k++)
                edges.Add(new LinkEdge(first + k, first + k + 1, "shared-card"));
        }
        Edges = [.. edges];
    }

    public Merchant[] Merchants { get; }
    public Customer[] Customers { get; }
    public FeeRate[] FeeRates { get; }
    public LinkEdge[] Edges { get; }

    /// <summary>The event-time clock, advanced one virtual second per
    /// <see cref="Options.OrdersPerVirtualSecond"/> orders so the windowed operators open and
    /// close windows at a steady, rate-independent cadence.</summary>
    public DateTimeOffset EventTime => _eventTime;

    private uint Next()
    {
        _rng ^= _rng << 13;
        _rng ^= _rng >> 7;
        _rng ^= _rng << 17;
        return (uint)(_rng >> 32);
    }

    /// <summary>One unit of traffic: the next order (and its payment, when one is generated),
    /// plus whatever the ring evicts. Weights are the caller's business — this only says what
    /// rows are involved.</summary>
    internal readonly record struct Unit(
        Order Insert, Payment? InsertPayment, Order? Retract, Payment? RetractPayment, bool Web);

    public Unit NextUnit()
    {
        var slot = (int)(_issued % _liveOrders.Length);
        var retract = _liveOrders[slot];
        var retractPayment = _livePayments[slot];

        if (_issued % _options.OrdersPerVirtualSecond == 0)
            _eventTime += TimeSpan.FromSeconds(1);

        var merchant = Merchants[Next() % (uint)Merchants.Length];
        var customer = 1 + (int)(Next() % (uint)Customers.Length);
        var web = (Next() & 1) == 0;
        var order = new Order(
            _nextOrderId++,
            customer,
            merchant.Id,
            web ? "web" : "mobile",
            1 + (int)(Next() % 3),
            10m + Next() % 50000 / 100m,
            _eventTime);

        // ~90% of orders settle; the rest stay unmatched, which is what keeps the
        // reconciliation full-join's "awaiting payment" bucket populated.
        Payment? payment = null;
        if (Next() % 10 != 0)
            payment = new Payment(order.Id, (Next() % 100) < 92 ? "approved" : "declined", order.Amount, _eventTime);

        _liveOrders[slot] = order;
        _livePayments[slot] = payment;
        _issued++;
        return new Unit(order, payment, retract, retractPayment, web);
    }
}
