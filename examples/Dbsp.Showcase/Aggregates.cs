using Dbsp;

namespace Dbsp.Showcase;

// Linear-aggregate accumulators are operator state: records, not tuples, so they
// round-trip JSON under --redis. Record *structs*, because every Combine constructs a
// fresh accumulator — as reference records these three were the largest allocations in
// the whole pipeline (~220 B per input record on the torture rig); as structs they live
// inline in the operator's state and Z-set entries and never touch the heap.

public readonly record struct FeeStats(long Orders, decimal Gross, decimal Fees);

public readonly record struct Acceptance(long Approved, long Total);

public readonly record struct Spend(long Orders, decimal Amount);

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
