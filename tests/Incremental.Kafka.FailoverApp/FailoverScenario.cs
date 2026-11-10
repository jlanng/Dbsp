using System.Text.Json;

namespace Incremental.Kafka.FailoverApp;

/// <summary>
/// What the failover app computes, stated twice: as the input it generates, and as the
/// answer a correct run must leave in the sink. Source-shared with the test assembly, which
/// uses the second to judge a run of real processes that were killed, paused and replaced.
/// <para>
/// Everything is a pure function of position. The orders input is record <c>i</c> for
/// <c>i = 0, 1, 2 …</c>, and <c>i</c> is the input offset the circuit checkpoints. A step
/// is the next <see cref="Batch"/> records, so a replay cuts the same steps the first run
/// did. The products table changes only at a step that begins a checkpoint interval, to a
/// row that depends on the step alone, so a replayed step reads the table it read the first
/// time. Those two conditions are what make keyed sinks converge exactly rather than
/// at-least-once (durability-and-standby.md, <i>A gap in "keyed upserts converge"</i>).
/// </para>
/// </summary>
public static class FailoverScenario
{
    public const string Pipeline = "failover";
    public const string OrdersInput = "orders";
    public const string EventsInput = "events";
    public const string ProductsTable = "products";

    /// <summary>Records per step.</summary>
    public const int Batch = 20;

    /// <summary>Steps per checkpoint.</summary>
    public const int CheckpointInterval = 4;

    /// <summary>The products table changes before every step that is a multiple of this.
    /// A multiple of <see cref="CheckpointInterval"/>, so the circuit has nothing
    /// uncommitted when it does.</summary>
    public const int TableEvery = 8;

    public const int OrderIds = 400;
    public const int Customers = 25;
    public const int Products = 12;
    public const int Buckets = 16;

    /// <summary>The sink names, which prefix the keys in the sink topic.</summary>
    public const string EnrichedSink = "enriched";
    public const string TotalsSink = "totals";
    public const string CountsSink = "counts";

    public sealed record Order(int Id, int Customer, int Product, int Qty);

    public sealed record Product(int Id, int Price);

    public sealed record Enriched(int OrderId, int Customer, long Value);

    /// <summary>Record <c>Offset</c> of the input once more, as a fact that is never
    /// overwritten: the orders are upserts, so a record lost in the middle of a run would
    /// be papered over by the next upsert of its key. The count of these per bucket is
    /// right only if every record was processed exactly once.</summary>
    public sealed record Event(long Offset, int Bucket);

    public static Event EventAt(long offset) => new(offset, OrderAt(offset).Id % Buckets);

    public static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    private static ulong Mix(ulong x)
    {
        // splitmix64: the same everywhere, unlike Random or GetHashCode.
        x += 0x9E3779B97F4A7C15;
        x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9;
        x = (x ^ (x >> 27)) * 0x94D049BB133111EB;
        return x ^ (x >> 31);
    }

    /// <summary>Record <paramref name="offset"/> of the orders input: an upsert of the
    /// order, or (one time in eight) its deletion.</summary>
    public static (int Id, Order? Row) OrderAt(long offset)
    {
        var h = Mix((ulong)offset);
        var id = (int)(h % OrderIds);
        if ((h >> 16) % 8 == 0)
            return (id, null);
        return (id, new Order(id, (int)((h >> 20) % Customers), (int)((h >> 28) % Products), 1 + (int)((h >> 36) % 9)));
    }

    /// <summary>The products rows written before step <paramref name="step"/>, as
    /// (id, row-or-deleted). Step 0 loads the table; later multiples of
    /// <see cref="TableEvery"/> reprice one product, or (every fifth time) delete it —
    /// which retracts every order joined to it until it is repriced again.</summary>
    public static IEnumerable<(int Id, Product? Row)> ProductsBefore(long step)
    {
        if (step % TableEvery != 0)
            yield break;
        if (step == 0)
        {
            for (var p = 0; p < Products; p++)
                yield return (p, new Product(p, 10 + p));
            yield break;
        }
        var write = step / TableEvery;
        var id = (int)(write % Products);
        yield return write % 5 == 4 ? (id, null) : (id, new Product(id, 10 + id + (int)(write % 97)));
    }

    /// <summary>What the three sinks must hold once <paramref name="steps"/> steps have run.</summary>
    public static (Dictionary<int, Enriched> Enriched, Dictionary<int, long> Totals, Dictionary<int, long> Counts) Expected(long steps)
    {
        var orders = new Dictionary<int, Order>();
        var counts = new Dictionary<int, long>();
        for (long offset = 0; offset < steps * Batch; offset++)
        {
            var (id, row) = OrderAt(offset);
            counts[id % Buckets] = counts.GetValueOrDefault(id % Buckets) + 1;
            if (row is null)
                orders.Remove(id);
            else
                orders[id] = row;
        }

        var products = new Dictionary<int, Product>();
        for (long step = 0; step < steps; step += TableEvery)
        {
            foreach (var (id, row) in ProductsBefore(step))
            {
                if (row is null)
                    products.Remove(id);
                else
                    products[id] = row;
            }
        }

        var enriched = new Dictionary<int, Enriched>();
        var totals = new Dictionary<int, long>();
        foreach (var order in orders.Values)
        {
            if (!products.TryGetValue(order.Product, out var product))
                continue;
            var row = new Enriched(order.Id, order.Customer, (long)order.Qty * product.Price);
            enriched[order.Id] = row;
            totals[order.Customer] = totals.GetValueOrDefault(order.Customer) + row.Value;
        }
        return (enriched, totals, counts);
    }
}
