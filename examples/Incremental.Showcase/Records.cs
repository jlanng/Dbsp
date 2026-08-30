namespace Incremental.Showcase;

// Rows that flow through the circuit. Records only: z-set membership needs value equality,
// and with --redis every stateful operator round-trips these through JSON.

public record Order(long Id, int CustomerId, int MerchantId, string Channel, int Items, decimal Amount, DateTimeOffset At);

public record ItemSale(long OrderId, int MerchantId, decimal Amount);

public record Merchant(int Id, string Name, string Category, int FeeTier);

public record Customer(int Id, string Name, string Region);

public record FeeRate(int Tier, decimal Rate);

public record Payment(long OrderId, string Status, decimal Amount, DateTimeOffset At);

// Account-linkage evidence (shared card / device) between two customer accounts. Undirected;
// the pipeline expands each edge into both directions.
public record LinkEdge(int A, int B, string Reason);

public record Link(int From, int To);

// Intermediate and derived rows — these are join/aggregate state, so they stay flat records.
public record OrderWithMerchant(long OrderId, int CustomerId, int MerchantId, string Merchant, string Category, int FeeTier, decimal Amount);

public record EnrichedOrder(long OrderId, int MerchantId, string Merchant, string Category, int CustomerId, string Region, decimal Amount, decimal Fee);

public record BuyerRow(int MerchantId, int CustomerId);

public record ReconRow(long OrderId, string Bucket);

public record RingMember(int Ring, int Account);

public record FlaggedOrder(int Ring, int Account, long OrderId, decimal Amount);

// Offset-stamped log entries the director keeps so Act 4 can replay the source feeds
// after the crash (exactly-once dedup happens inside the circuit, not here).
public record StampedOrder(string Channel, Order Order, long Offset);

public record StampedPayment(Payment Payment, long Offset);
