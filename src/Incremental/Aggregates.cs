using System.Numerics;

namespace Incremental;

/// <summary>
/// An aggregate over one group of an indexed Z-set. <paramref name="group"/> is the full
/// integrated contents of the group (never empty; weights are positive when the input
/// describes a well-formed multiset). Implementations must be pure.
/// </summary>
public interface IAggregateFunction<TValue, out TResult> where TValue : notnull
{
    TResult Aggregate(IReadOnlyList<WeightedValue<TValue>> group);
}

/// <summary>Built-in aggregate functions for <see cref="KeyedZStream{TKey,TValue}.Aggregate{TResult}"/>.</summary>
public static class Aggregates
{
    /// <summary>Number of rows in the group (sum of weights — duplicates count).</summary>
    public static IAggregateFunction<TValue, long> Count<TValue>() where TValue : notnull =>
        new DelegateAggregate<TValue, long>(group =>
        {
            long count = 0;
            foreach (var (_, weight) in group)
                count += weight;
            return count;
        });

    /// <summary>Number of distinct values in the group.</summary>
    public static IAggregateFunction<TValue, long> CountDistinct<TValue>() where TValue : notnull =>
        new DelegateAggregate<TValue, long>(group =>
        {
            long count = 0;
            foreach (var (_, weight) in group)
                if (weight > 0)
                    count++;
            return count;
        });

    public static IAggregateFunction<TValue, TNumber> Sum<TValue, TNumber>(Func<TValue, TNumber> selector)
        where TValue : notnull
        where TNumber : INumber<TNumber> =>
        new DelegateAggregate<TValue, TNumber>(group =>
        {
            var sum = TNumber.Zero;
            foreach (var (value, weight) in group)
                sum += selector(value) * TNumber.CreateChecked(weight);
            return sum;
        });

    public static IAggregateFunction<TValue, TProjected> Min<TValue, TProjected>(Func<TValue, TProjected> selector)
        where TValue : notnull
        where TProjected : notnull =>
        new ExtremumAggregate<TValue, TProjected>(selector, sign: -1);

    public static IAggregateFunction<TValue, TProjected> Max<TValue, TProjected>(Func<TValue, TProjected> selector)
        where TValue : notnull
        where TProjected : notnull =>
        new ExtremumAggregate<TValue, TProjected>(selector, sign: +1);

    /// <summary>General fold over the group; the folder receives each value with its weight.</summary>
    public static IAggregateFunction<TValue, TResult> Fold<TValue, TResult>(
        TResult seed,
        Func<TResult, TValue, long, TResult> folder)
        where TValue : notnull =>
        new DelegateAggregate<TValue, TResult>(group =>
        {
            var acc = seed;
            foreach (var (value, weight) in group)
                acc = folder(acc, value, weight);
            return acc;
        });

    private sealed class DelegateAggregate<TValue, TResult>(
        Func<IReadOnlyList<WeightedValue<TValue>>, TResult> compute) : IAggregateFunction<TValue, TResult>
        where TValue : notnull
    {
        public TResult Aggregate(IReadOnlyList<WeightedValue<TValue>> group) => compute(group);
    }

    private sealed class ExtremumAggregate<TValue, TProjected>(
        Func<TValue, TProjected> selector,
        int sign) : IAggregateFunction<TValue, TProjected>
        where TValue : notnull
        where TProjected : notnull
    {
        public TProjected Aggregate(IReadOnlyList<WeightedValue<TValue>> group)
        {
            var comparer = Comparer<TProjected>.Default;
            var found = false;
            TProjected best = default!;
            foreach (var (value, weight) in group)
            {
                if (weight <= 0)
                    continue;
                var candidate = selector(value);
                if (!found || Math.Sign(comparer.Compare(candidate, best)) == sign)
                {
                    best = candidate;
                    found = true;
                }
            }
            return found
                ? best
                : throw new InvalidOperationException(
                    "MIN/MAX over a group with no positively-weighted values; the input stream is not a well-formed multiset.");
        }
    }
}
