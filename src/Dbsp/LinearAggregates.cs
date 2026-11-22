using System.Numerics;

namespace Dbsp;

/// <summary>
/// A linear aggregate: one whose result is Σ Map(v)·w over the group, where results form a
/// commutative group under <see cref="Combine"/>. Linearity is what makes the aggregate
/// maintainable from deltas alone — no group state, no rescans: updating a key costs O(delta)
/// and the state per key is a single accumulator, regardless of how many rows the group holds.
/// COUNT and SUM are linear; MIN/MAX are not (deleting the minimum needs the group) and use
/// <see cref="IAggregateFunction{TValue,TResult}"/> instead.
/// </summary>
public interface ILinearAggregate<TValue, TResult> where TValue : notnull
{
    TResult Zero { get; }

    /// <summary>The contribution of <paramref name="value"/> with multiplicity
    /// <paramref name="weight"/> (which may be negative — a retraction).</summary>
    TResult Map(TValue value, long weight);

    TResult Combine(TResult left, TResult right);
}

/// <summary>Built-in linear aggregates for <c>AggregateLinear</c>.</summary>
public static class LinearAggregates
{
    /// <summary>COUNT: the number of rows in the group (sum of weights — duplicates count).</summary>
    public static ILinearAggregate<TValue, long> Count<TValue>() where TValue : notnull =>
        new CountAggregate<TValue>();

    /// <summary>
    /// SUM of <paramref name="selector"/> over the group. Integer accumulation is checked:
    /// an overflow throws <see cref="OverflowException"/> (faulting the circuit) instead of
    /// silently wrapping. Prefer <see cref="decimal"/> or <see cref="long"/> accumulators —
    /// <see cref="float"/>/<see cref="double"/> sums are not exact under retraction: adding
    /// x and later adding -x does not restore the previous accumulator (rounding), so a
    /// long-lived floating-point sum drifts permanently as rows are amended or deleted.
    /// </summary>
    public static ILinearAggregate<TValue, TNumber> Sum<TValue, TNumber>(Func<TValue, TNumber> selector)
        where TValue : notnull
        where TNumber : INumber<TNumber> =>
        new SumAggregate<TValue, TNumber>(selector);

    private sealed class CountAggregate<TValue> : ILinearAggregate<TValue, long> where TValue : notnull
    {
        public long Zero => 0;
        public long Map(TValue value, long weight) => weight;
        public long Combine(long left, long right) => left + right;
    }

    private sealed class SumAggregate<TValue, TNumber>(Func<TValue, TNumber> selector)
        : ILinearAggregate<TValue, TNumber>
        where TValue : notnull
        where TNumber : INumber<TNumber>
    {
        public TNumber Zero => TNumber.Zero;

        // `checked` binds the generic-math checked operators (op_CheckedMultiply/Add), so
        // integer accumulators overflow loudly, like Z-set weight arithmetic does;
        // float/double/decimal keep their own semantics (their checked ops are the same).
        public TNumber Map(TValue value, long weight) => checked(selector(value) * TNumber.CreateChecked(weight));
        public TNumber Combine(TNumber left, TNumber right) => checked(left + right);
    }
}
