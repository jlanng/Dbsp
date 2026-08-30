namespace Incremental.ClusterHost;

/// <summary>
/// The scenario shared verbatim — via a source link — by the multi-process cluster tests
/// and the child host executable: same dataset, same pipeline shape, so the single-process
/// reference run and the clustered runs can never drift apart. String region keys are the
/// point of the whole exercise: the processes launched by the test are real OS processes
/// with different randomized string-hash seeds, so exchange routing only agrees across
/// them because it is structural (<see cref="StableHash"/>), and any regression back to
/// seeded hashing splits a region's state across workers and fails the test.
/// </summary>
public static class ClusterScenario
{
    /// <summary>Reference data, fed on process 0 before the first step. Eight distinct
    /// string regions so both processes' worker shards own some of them.</summary>
    public static readonly (int Id, string Region)[] Customers =
    [
        (1, "emea"), (2, "apac"), (3, "amer"), (4, "latam"),
        (5, "nordics"), (6, "dach"), (7, "anz"), (8, "mea"),
    ];

    /// <summary>Order churn, fed one row per lockstep step by <see cref="FeederOf"/>'s
    /// process. Indexes ≥ <see cref="LoadPhaseOrders"/> form the checkpoint-restart
    /// scenario's second phase; the two retractions there cancel orders fed in the first
    /// phase — from the <em>other</em> process — so they only produce correct deltas if the
    /// restored join and sum state is routed and read back correctly.</summary>
    public static readonly ((int Id, int CustomerId, decimal Amount) Order, long Weight)[] Orders =
    [
        ((101, 1, 60m), 1),
        ((102, 2, 40m), 1),
        ((103, 3, 10m), 1),
        ((104, 4, 25m), 1),
        ((105, 5, 15m), 1),
        ((106, 6, 35m), 1),
        ((107, 7, 55m), 1),
        ((108, 8, 5m), 1),
        // -- load phase ends here (checkpoint-restart scenario) --
        ((109, 1, 20m), 1),
        ((101, 1, 60m), -1), // cancelled: fed by process 0 in phase 1, retracted by process 1 in phase 2
        ((110, 2, 30m), 1),
        ((111, 3, 45m), 1),
        ((105, 5, 15m), -1), // cancelled: empties the nordics group entirely
        ((112, 4, 50m), 1),
        ((113, 6, 65m), 1),
        ((114, 8, 90m), 1),
    ];

    /// <summary>Which process feeds the order at <paramref name="orderIndex"/> (both
    /// processes step every step regardless — the lockstep contract).</summary>
    public static int FeederOf(int orderIndex) => orderIndex % 2;

    /// <summary>How many orders the checkpoint-restart scenario feeds before exiting.</summary>
    public const int LoadPhaseOrders = 8;

    /// <summary>orders ⋈ customers (int key) → IndexBy(Region) (string key) → Sum(Amount):
    /// two exchange hops, the second on the hash-seed-sensitive string key.</summary>
    public static CircuitBuilder RevenuePipeline(CircuitOptions options)
    {
        var builder = new CircuitBuilder(options);
        var orders = builder.Input<(int Id, int CustomerId, decimal Amount)>("orders");
        var customers = builder.Input<(int Id, string Region)>("customers");
        orders.IndexBy(o => o.CustomerId)
            .Join(customers.IndexBy(c => c.Id), (_, o, c) => (c.Region, o.Amount))
            .IndexBy(x => x.Region)
            .Sum(x => x.Amount)
            .Output("revenue");
        return builder;
    }
}
