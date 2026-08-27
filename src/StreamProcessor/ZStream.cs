using System.Numerics;
using StreamProcessor.Operators;

namespace StreamProcessor;

/// <summary>
/// A handle to a node in a circuit under construction: a stream of Z-set deltas of
/// <typeparamref name="T"/>. Every operator is fully incremental — it processes only the
/// changes flowing through the stream, and deletions (negative weights) propagate through
/// the whole pipeline, including joins and aggregations.
/// </summary>
public class ZStream<T> where T : notnull
{
    internal ZStream(CircuitBuilder builder, int nodeId)
    {
        Builder = builder;
        NodeId = nodeId;
    }

    internal CircuitBuilder Builder { get; }
    internal int NodeId { get; }

    public ZStream<TOut> Map<TOut>(Func<T, TOut> selector) where TOut : notnull =>
        new(Builder, Builder.AddNode("map", [NodeId], _ => new MapOperator<T, TOut>(selector)));

    public ZStream<T> Filter(Func<T, bool> predicate) =>
        new(Builder, Builder.AddNode("filter", [NodeId], _ => new FilterOperator<T>(predicate)));

    public ZStream<TOut> FlatMap<TOut>(Func<T, IEnumerable<TOut>> selector) where TOut : notnull =>
        new(Builder, Builder.AddNode("flatMap", [NodeId], _ => new FlatMapOperator<T, TOut>(selector)));

    /// <summary>Negates all weights (turns this stream's insertions into retractions and vice versa).</summary>
    public ZStream<T> Negate() =>
        new(Builder, Builder.AddNode("negate", [NodeId], _ => new NegateOperator<T>()));

    /// <summary>Z-set addition (bag union) with another stream.</summary>
    public ZStream<T> Plus(ZStream<T> other)
    {
        RequireSameBuilder(other);
        return new ZStream<T>(Builder, Builder.AddNode("plus", [NodeId, other.NodeId], _ => new PlusOperator<T>()));
    }

    /// <summary>Z-set difference: this stream minus <paramref name="other"/>.</summary>
    public ZStream<T> Minus(ZStream<T> other) => Plus(other.Negate());

    /// <summary>Collapses the multiset to a set: every present value gets weight exactly 1.</summary>
    public ZStream<T> Distinct()
    {
        var upstream = NodeId;
        if (Builder.Options.Workers > 1)
        {
            // Distinct state is per-value; route equal values to the same worker.
            upstream = Builder.AddNode(
                "shuffle",
                [upstream],
                _ => new ExchangeOperator<T>(static v => EqualityComparer<T>.Default.GetHashCode(v)),
                isExchange: true);
        }
        return new ZStream<T>(Builder, Builder.AddNode("distinct", [upstream], ctx => new DistinctOperator<T>(ctx)));
    }

    /// <summary>
    /// Keys the stream by <paramref name="keySelector"/>, inserting an exchange so that all
    /// records with equal keys are processed by the same worker. This is the entry point to
    /// joins and grouped aggregation, and the DBSP equivalent of a shuffle/repartition.
    /// </summary>
    public KeyedZStream<TKey, T> IndexBy<TKey>(Func<T, TKey> keySelector) where TKey : notnull
    {
        var keyed = Builder.AddNode(
            "index",
            [NodeId],
            _ => new MapOperator<T, KeyedValue<TKey, T>>(v => new KeyedValue<TKey, T>(keySelector(v), v)));

        var node = keyed;
        if (Builder.Options.Workers > 1)
        {
            node = Builder.AddNode(
                "exchange",
                [keyed],
                _ => new ExchangeOperator<KeyedValue<TKey, T>>(
                    static kv => EqualityComparer<TKey>.Default.GetHashCode(kv.Key)),
                isExchange: true);
        }
        return new KeyedZStream<TKey, T>(Builder, node);
    }

    /// <summary>Registers this stream as a named output, readable after <c>Build</c> via <see cref="Circuit.Output{T}"/>.</summary>
    public ZStream<T> Output(string name)
    {
        Builder.AddOutput<T>(name, NodeId);
        return this;
    }

    private protected void RequireSameBuilder<TOther>(ZStream<TOther> other) where TOther : notnull
    {
        if (!ReferenceEquals(Builder, other.Builder))
            throw new ArgumentException("Streams from different circuit builders cannot be combined.", nameof(other));
    }
}

/// <summary>
/// A stream of key/value pairs partitioned by key across workers. Joins and aggregations
/// operate per key on worker-local state, so they scale with the worker count.
/// </summary>
public sealed class KeyedZStream<TKey, TValue> : ZStream<KeyedValue<TKey, TValue>>
    where TKey : notnull
    where TValue : notnull
{
    internal KeyedZStream(CircuitBuilder builder, int nodeId) : base(builder, nodeId)
    {
    }

    /// <summary>Incremental equi-join on the shared key.</summary>
    public ZStream<TOut> Join<TRight, TOut>(
        KeyedZStream<TKey, TRight> right,
        Func<TKey, TValue, TRight, TOut> resultSelector)
        where TRight : notnull
        where TOut : notnull
    {
        RequireSameBuilder(right);
        return new ZStream<TOut>(
            Builder,
            Builder.AddNode(
                "join",
                [NodeId, right.NodeId],
                ctx => new JoinOperator<TKey, TValue, TRight, TOut>(ctx, resultSelector)));
    }

    /// <summary>Grouped aggregation. The result stream carries one (key, result) row per key,
    /// with the previous result retracted whenever it changes.</summary>
    public KeyedZStream<TKey, TResult> Aggregate<TResult>(IAggregateFunction<TValue, TResult> aggregate)
        where TResult : notnull =>
        new(Builder, Builder.AddNode("aggregate", [NodeId], ctx => new AggregateOperator<TKey, TValue, TResult>(ctx, aggregate)));

    public KeyedZStream<TKey, long> Count() => Aggregate(Aggregates.Count<TValue>());

    public KeyedZStream<TKey, TNumber> Sum<TNumber>(Func<TValue, TNumber> selector)
        where TNumber : INumber<TNumber> =>
        Aggregate(Aggregates.Sum(selector));

    public KeyedZStream<TKey, TProjected> Min<TProjected>(Func<TValue, TProjected> selector)
        where TProjected : notnull =>
        Aggregate(Aggregates.Min(selector));

    public KeyedZStream<TKey, TProjected> Max<TProjected>(Func<TValue, TProjected> selector)
        where TProjected : notnull =>
        Aggregate(Aggregates.Max(selector));

    /// <summary>
    /// Keeps only the top <paramref name="count"/> rows per key, ranked by
    /// <paramref name="orderBy"/> (largest first by default). The output carries the rows
    /// themselves: when membership changes, displaced rows are retracted and newly ranked
    /// rows asserted. Multiplicities count toward K; ties are broken arbitrarily.
    /// </summary>
    public KeyedZStream<TKey, TValue> TopK<TOrder>(int count, Func<TValue, TOrder> orderBy, bool descending = true)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(count, 1, nameof(count));
        return new KeyedZStream<TKey, TValue>(
            Builder,
            Builder.AddNode("topK", [NodeId], ctx => new TopKOperator<TKey, TValue, TOrder>(ctx, count, orderBy, descending)));
    }

    /// <summary>Transforms values while preserving the key and the partitioning.</summary>
    public KeyedZStream<TKey, TOut> MapValues<TOut>(Func<TValue, TOut> selector) where TOut : notnull =>
        new(Builder, Builder.AddNode(
            "mapValues",
            [NodeId],
            _ => new MapOperator<KeyedValue<TKey, TValue>, KeyedValue<TKey, TOut>>(
                kv => new KeyedValue<TKey, TOut>(kv.Key, selector(kv.Value)))));

    /// <summary>Drops the keys. The result is no longer partitioned by them.</summary>
    public ZStream<TValue> Values() => Map(static kv => kv.Value);
}
