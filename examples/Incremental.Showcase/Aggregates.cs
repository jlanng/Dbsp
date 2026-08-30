using Incremental;

namespace Incremental.Showcase;

// Linear-aggregate accumulators are operator state: records, not tuples, so they
// round-trip JSON under --redis.

public record FeeStats(long Orders, decimal Gross, decimal Fees);

public record Acceptance(long Approved, long Total);

public record Spend(long Orders, decimal Amount);

// A linear aggregate is a homomorphism over weights: no per-key group contents are
// stored, just one accumulator — retractions arrive as negative weights and fold in.
public sealed class FeeTakeAggregate : ILinearAggregate<EnrichedOrder, FeeStats>
{
    public FeeStats Zero { get; } = new(0, 0m, 0m);
    public FeeStats Map(EnrichedOrder value, long weight) => new(weight, value.Amount * weight, value.Fee * weight);
    public FeeStats Combine(FeeStats left, FeeStats right) =>
        new(left.Orders + right.Orders, left.Gross + right.Gross, left.Fees + right.Fees);
}

public sealed class AcceptanceAggregate : ILinearAggregate<Payment, Acceptance>
{
    public Acceptance Zero { get; } = new(0, 0);
    public Acceptance Map(Payment value, long weight) => new(value.Status == "approved" ? weight : 0, weight);
    public Acceptance Combine(Acceptance left, Acceptance right) =>
        new(left.Approved + right.Approved, left.Total + right.Total);
}

public sealed class SpendAggregate : ILinearAggregate<Order, Spend>
{
    public Spend Zero { get; } = new(0, 0m);
    public Spend Map(Order value, long weight) => new(weight, value.Amount * weight);
    public Spend Combine(Spend left, Spend right) =>
        new(left.Orders + right.Orders, left.Amount + right.Amount);
}
