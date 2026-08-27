using System.Numerics;
using Incremental.Operators;

namespace Incremental;

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
    private readonly KeyEviction<TKey>? _eviction;

    internal KeyedZStream(CircuitBuilder builder, int nodeId, KeyEviction<TKey>? eviction = null)
        : base(builder, nodeId)
    {
        _eviction = eviction;
    }

    /// <summary>
    /// Attaches a watermark-driven eviction policy: once the circuit's watermark passes
    /// <paramref name="closesAt"/>(key), stateful operators downstream of this stream
    /// (aggregations, TopK) delete everything they hold for that key. Already-emitted
    /// results for the key remain final. Typical use with windowed keys:
    /// <c>.WithEviction(k =&gt; k.Window + windowSize)</c>. Requires <c>WithWatermark</c> on
    /// an upstream input. The policy is inherited by keyed streams derived from this one
    /// with the same key type.
    /// </summary>
    public KeyedZStream<TKey, TValue> WithEviction(Func<TKey, DateTimeOffset> closesAt)
    {
        var clock = Builder.WatermarkClock
            ?? throw new InvalidOperationException(
                "WithEviction requires an event-time watermark; call WithWatermark on an input stream first.");
        return new KeyedZStream<TKey, TValue>(Builder, NodeId, new KeyEviction<TKey>(clock, closesAt));
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

    /// <summary>
    /// Incremental anti-join: the rows of this stream whose key has no match in
    /// <paramref name="right"/>. When a key gains its first right-side row its left rows are
    /// retracted; when it loses its last they are re-asserted.
    /// </summary>
    public KeyedZStream<TKey, TValue> AntiJoin<TRight>(KeyedZStream<TKey, TRight> right)
        where TRight : notnull
    {
        RequireSameBuilder(right);
        return new KeyedZStream<TKey, TValue>(
            Builder,
            Builder.AddNode("antiJoin", [NodeId, right.NodeId], ctx => new AntiJoinOperator<TKey, TValue, TRight>(ctx)),
            _eviction);
    }

    /// <summary>Left outer join: <paramref name="matched"/> per matching right row, or one
    /// <paramref name="unmatched"/> row while the key has no match. Composed from the inner
    /// join plus the anti-join — the Z-set algebra makes the union incremental for free.</summary>
    public ZStream<TOut> LeftJoin<TRight, TOut>(
        KeyedZStream<TKey, TRight> right,
        Func<TKey, TValue, TRight, TOut> matched,
        Func<TKey, TValue, TOut> unmatched)
        where TRight : notnull
        where TOut : notnull =>
        Join(right, matched)
            .Plus(AntiJoin(right).Map(kv => unmatched(kv.Key, kv.Value)));

    /// <summary>Full outer join, composed like <see cref="LeftJoin"/> with an anti-join per side.</summary>
    public ZStream<TOut> FullJoin<TRight, TOut>(
        KeyedZStream<TKey, TRight> right,
        Func<TKey, TValue, TRight, TOut> matched,
        Func<TKey, TValue, TOut> leftOnly,
        Func<TKey, TRight, TOut> rightOnly)
        where TRight : notnull
        where TOut : notnull =>
        Join(right, matched)
            .Plus(AntiJoin(right).Map(kv => leftOnly(kv.Key, kv.Value)))
            .Plus(right.AntiJoin(this).Map(kv => rightOnly(kv.Key, kv.Value)));

    /// <summary>Grouped aggregation. The result stream carries one (key, result) row per key,
    /// with the previous result retracted whenever it changes.</summary>
    public KeyedZStream<TKey, TResult> Aggregate<TResult>(IAggregateFunction<TValue, TResult> aggregate)
        where TResult : notnull =>
        new(Builder,
            Builder.AddNode("aggregate", [NodeId], ctx => new AggregateOperator<TKey, TValue, TResult>(ctx, aggregate, _eviction)),
            _eviction);

    /// <summary>Grouped aggregation for a linear aggregate (COUNT, SUM, …): maintained from
    /// deltas alone with O(1) state per key — no group contents are stored, so update cost
    /// is independent of group size.</summary>
    public KeyedZStream<TKey, TResult> AggregateLinear<TResult>(ILinearAggregate<TValue, TResult> aggregate)
        where TResult : notnull =>
        new(Builder,
            Builder.AddNode("aggregateLinear", [NodeId], ctx => new LinearAggregateOperator<TKey, TValue, TResult>(ctx, aggregate, _eviction)),
            _eviction);

    public KeyedZStream<TKey, long> Count() => AggregateLinear(LinearAggregates.Count<TValue>());

    public KeyedZStream<TKey, TNumber> Sum<TNumber>(Func<TValue, TNumber> selector)
        where TNumber : INumber<TNumber> =>
        AggregateLinear(LinearAggregates.Sum(selector));

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
            Builder.AddNode("topK", [NodeId], ctx => new TopKOperator<TKey, TValue, TOrder>(ctx, count, orderBy, descending, _eviction)),
            _eviction);
    }

    /// <summary>Transforms values while preserving the key and the partitioning.</summary>
    public KeyedZStream<TKey, TOut> MapValues<TOut>(Func<TValue, TOut> selector) where TOut : notnull =>
        new(Builder, Builder.AddNode(
            "mapValues",
            [NodeId],
            _ => new MapOperator<KeyedValue<TKey, TValue>, KeyedValue<TKey, TOut>>(
                kv => new KeyedValue<TKey, TOut>(kv.Key, selector(kv.Value)))),
            _eviction);

    /// <summary>Drops the keys. The result is no longer partitioned by them.</summary>
    public ZStream<TValue> Values() => Map(static kv => kv.Value);
}
