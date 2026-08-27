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
        // Inside a Recurse body the operator must be incremental in the iteration dimension
        // too; see RecursionOperators.cs.
        var inLoop = Builder.InRecursionScope;
        return new ZStream<T>(Builder, Builder.AddNode("distinct", [upstream],
            ctx => inLoop ? new LoopDistinctOperator<T>(ctx) : (IStepOperator)new DistinctOperator<T>(ctx)));
    }

    /// <summary>
    /// Closes the stream over itself and computes the least fixed point
    /// <c>X = Distinct(this ∪ body(X))</c> — the DBSP recursive circuit, with this stream as
    /// the seed and <c>Distinct</c> as the built-in stabilizer. <paramref name="body"/>
    /// receives the recursive stream and may use the linear operators plus
    /// <c>IndexBy</c>/<c>Join</c>/<c>Distinct</c>; it may also reference other streams of
    /// the circuit (they are imported into the loop automatically). Within every outer step
    /// the subcircuit iterates until no delta circulates on any worker — workers iterate in
    /// lockstep, so exchanges inside the body stay barrier-aligned — and the result stream
    /// carries the net change of the new fixed point versus the previous step's, so
    /// downstream operators see ordinary incremental deltas. Retractions flow through like
    /// any other delta: deleting a seed or imported row retracts exactly the derived rows
    /// that no longer have a well-founded derivation, cyclic derivations included. The
    /// classic use is transitive closure:
    /// <code>
    /// var closure = edges.Map(e =&gt; (e.From, e.To))
    ///     .Recurse(paths =&gt; paths
    ///         .IndexBy(p =&gt; p.To)
    ///         .Join(edges.IndexBy(e =&gt; e.From), (_, p, e) =&gt; (p.From, e.To)));
    /// </code>
    /// </summary>
    /// <param name="body">The monotone step of the recursion, applied to the recursive stream.</param>
    /// <param name="maxIterations">Iterations allowed per outer step before the circuit
    /// faults with <see cref="InvalidOperationException"/> — a guard against bodies that
    /// never converge (e.g. non-monotone ones that oscillate).</param>
    public ZStream<T> Recurse(Func<ZStream<T>, ZStream<T>> body, int maxIterations = 10_000)
    {
        ArgumentNullException.ThrowIfNull(body);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxIterations, 1, nameof(maxIterations));

        var scope = Builder.OpenRecursionScope();
        var feedback = new ZStream<T>(Builder, Builder.AddNode("loopFeedback", [], _ => new LoopFeedbackOperator<T>()));
        var bodyResult = body(feedback);
        ArgumentNullException.ThrowIfNull(bodyResult);
        RequireSameBuilder(bodyResult);
        var stabilized = Plus(bodyResult).Distinct();
        var exit = Builder.AddNode("loopExit", [stabilized.NodeId], _ => new LoopExitOperator<T>());
        Builder.CloseRecursionScope(scope, stabilized.NodeId, exit, maxIterations);
        return new ZStream<T>(Builder, exit);
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
    /// (aggregations, TopK, joins, anti-joins, keyed <see cref="Distinct"/>) delete
    /// everything they hold for that key. Already-emitted results for the key remain final.
    /// Typical use with windowed keys: <c>.WithEviction(k =&gt; k.Window + windowSize)</c>.
    /// Requires <c>WithWatermark</c> on an upstream input. The policy is inherited by keyed
    /// streams derived from this one with the same key type; a two-input operator (join,
    /// anti-join) uses the left side's policy if it has one, otherwise the right's.
    /// </summary>
    public KeyedZStream<TKey, TValue> WithEviction(Func<TKey, DateTimeOffset> closesAt)
    {
        Builder.RequireOutsideRecursion("WithEviction");
        var clock = Builder.WatermarkClock
            ?? throw new InvalidOperationException(
                "WithEviction requires an event-time watermark; call WithWatermark on an input stream first.");
        return new KeyedZStream<TKey, TValue>(Builder, NodeId, new KeyEviction<TKey>(clock, closesAt));
    }

    /// <summary>
    /// A stream of window-close notifications: emits each key of this stream exactly once,
    /// with weight +1, in the first step whose watermark has passed
    /// <paramref name="closesAt"/>(key). Closed means what it means for eviction: the
    /// watermark admits no further events for the key, so its results are final —
    /// notifications are never retracted, and a window whose contents were fully retracted
    /// still closes. The value carried is the key's close deadline. The result stays
    /// partitioned by key (each key is announced by the worker owning it), so it can be
    /// joined with the window's aggregate to publish finalized results — the join emits
    /// them in the same step its state for the key is evicted. Requires
    /// <c>WithWatermark</c> on an upstream input. With <c>AutoStep</c>, a notification
    /// whose deadline passes on the final batch is flushed by one last step before the
    /// outputs complete.
    /// </summary>
    public KeyedZStream<TKey, DateTimeOffset> WindowCloses(Func<TKey, DateTimeOffset> closesAt)
    {
        var clock = Builder.WatermarkClock
            ?? throw new InvalidOperationException(
                "WindowCloses requires an event-time watermark; call WithWatermark on an input stream first.");
        return WindowCloses(new KeyEviction<TKey>(clock, closesAt));
    }

    /// <summary>Window-close notifications at the deadline of the eviction policy attached
    /// by <see cref="WithEviction"/>: keys are announced final at the same watermark the
    /// stateful operators drop their state.</summary>
    public KeyedZStream<TKey, DateTimeOffset> WindowCloses()
    {
        var policy = _eviction
            ?? throw new InvalidOperationException(
                "WindowCloses without a deadline requires an eviction policy; call WithEviction first or pass closesAt explicitly.");
        return WindowCloses(policy);
    }

    private KeyedZStream<TKey, DateTimeOffset> WindowCloses(KeyEviction<TKey> policy)
    {
        Builder.RequireOutsideRecursion("WindowCloses");
        return new(Builder,
            Builder.AddNode("windowClose", [NodeId], _ => new WindowCloseOperator<TKey, TValue>(policy)),
            policy);
    }

    /// <summary>Incremental equi-join on the shared key.</summary>
    public ZStream<TOut> Join<TRight, TOut>(
        KeyedZStream<TKey, TRight> right,
        Func<TKey, TValue, TRight, TOut> resultSelector)
        where TRight : notnull
        where TOut : notnull
    {
        RequireSameBuilder(right);
        var eviction = _eviction ?? right._eviction;
        if (Builder.InRecursionScope)
        {
            // Inside a Recurse body the join must be incremental in the iteration dimension
            // too; see RecursionOperators.cs. Eviction has no meaning there.
            if (eviction is not null)
                Builder.RequireOutsideRecursion("Join on a stream with WithEviction");
            return new ZStream<TOut>(
                Builder,
                Builder.AddNode(
                    "loopJoin",
                    [NodeId, right.NodeId],
                    ctx => new LoopJoinOperator<TKey, TValue, TRight, TOut>(ctx, resultSelector)));
        }
        return new ZStream<TOut>(
            Builder,
            Builder.AddNode(
                "join",
                [NodeId, right.NodeId],
                ctx => new JoinOperator<TKey, TValue, TRight, TOut>(ctx, resultSelector, eviction)));
    }

    /// <summary>
    /// Incremental anti-join: the rows of this stream whose key has no match in
    /// <paramref name="right"/>. When a key gains its first right-side row its left rows are
    /// retracted; when it loses its last they are re-asserted.
    /// </summary>
    public KeyedZStream<TKey, TValue> AntiJoin<TRight>(KeyedZStream<TKey, TRight> right)
        where TRight : notnull
    {
        Builder.RequireOutsideRecursion("AntiJoin");
        RequireSameBuilder(right);
        var eviction = _eviction ?? right._eviction;
        return new KeyedZStream<TKey, TValue>(
            Builder,
            Builder.AddNode("antiJoin", [NodeId, right.NodeId], ctx => new AntiJoinOperator<TKey, TValue, TRight>(ctx, eviction)),
            eviction);
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
        where TResult : notnull
    {
        Builder.RequireOutsideRecursion("Aggregate");
        return new(Builder,
            Builder.AddNode("aggregate", [NodeId], ctx => new AggregateOperator<TKey, TValue, TResult>(ctx, aggregate, _eviction)),
            _eviction);
    }

    /// <summary>Grouped aggregation for a linear aggregate (COUNT, SUM, …): maintained from
    /// deltas alone with O(1) state per key — no group contents are stored, so update cost
    /// is independent of group size.</summary>
    public KeyedZStream<TKey, TResult> AggregateLinear<TResult>(ILinearAggregate<TValue, TResult> aggregate)
        where TResult : notnull
    {
        Builder.RequireOutsideRecursion("AggregateLinear");
        return new(Builder,
            Builder.AddNode("aggregateLinear", [NodeId], ctx => new LinearAggregateOperator<TKey, TValue, TResult>(ctx, aggregate, _eviction)),
            _eviction);
    }

    public KeyedZStream<TKey, long> Count() => AggregateLinear(LinearAggregates.Count<TValue>());

    public KeyedZStream<TKey, TNumber> Sum<TNumber>(Func<TValue, TNumber> selector)
        where TNumber : INumber<TNumber> =>
        AggregateLinear(LinearAggregates.Sum(selector));

    /// <summary>Grouped MIN maintained by a tournament tree: an update — including deleting
    /// the current minimum — touches a fixed number of small buckets instead of rescanning
    /// the group, so cost is independent of group size. The general recompute path remains
    /// available as <c>Aggregate(Aggregates.Min(...))</c>.</summary>
    public KeyedZStream<TKey, TProjected> Min<TProjected>(Func<TValue, TProjected> selector)
        where TProjected : notnull
    {
        Builder.RequireOutsideRecursion("Min");
        return new(Builder,
            Builder.AddNode("min", [NodeId], ctx => new ExtremumOperator<TKey, TValue, TProjected>(ctx, selector, sign: -1, _eviction)),
            _eviction);
    }

    /// <summary>Grouped MAX maintained by a tournament tree; see <see cref="Min{TProjected}"/>.</summary>
    public KeyedZStream<TKey, TProjected> Max<TProjected>(Func<TValue, TProjected> selector)
        where TProjected : notnull
    {
        Builder.RequireOutsideRecursion("Max");
        return new(Builder,
            Builder.AddNode("max", [NodeId], ctx => new ExtremumOperator<TKey, TValue, TProjected>(ctx, selector, sign: +1, _eviction)),
            _eviction);
    }

    /// <summary>
    /// Keeps only the top <paramref name="count"/> rows per key, ranked by
    /// <paramref name="orderBy"/> (largest first by default). The output carries the rows
    /// themselves: when membership changes, displaced rows are retracted and newly ranked
    /// rows asserted. Multiplicities count toward K; ties are broken arbitrarily.
    /// </summary>
    public KeyedZStream<TKey, TValue> TopK<TOrder>(int count, Func<TValue, TOrder> orderBy, bool descending = true)
    {
        Builder.RequireOutsideRecursion("TopK");
        ArgumentOutOfRangeException.ThrowIfLessThan(count, 1, nameof(count));
        return new KeyedZStream<TKey, TValue>(
            Builder,
            Builder.AddNode("topK", [NodeId], ctx => new TopKOperator<TKey, TValue, TOrder>(ctx, count, orderBy, descending, _eviction)),
            _eviction);
    }

    /// <summary>Collapses the multiset to a set, like the unkeyed <see cref="ZStream{T}.Distinct"/>,
    /// but keeps the result keyed and its state grouped by key so an eviction policy applies.
    /// The stream is already partitioned by key, so no extra exchange is needed.</summary>
    public new KeyedZStream<TKey, TValue> Distinct()
    {
        if (Builder.InRecursionScope)
        {
            // Iteration-aware distinct over the (key, value) pairs; the key partitioning
            // already routes equal pairs to the same worker. Eviction has no meaning here.
            if (_eviction is not null)
                Builder.RequireOutsideRecursion("Distinct on a stream with WithEviction");
            return new(Builder,
                Builder.AddNode("distinct", [NodeId], ctx => new LoopDistinctOperator<KeyedValue<TKey, TValue>>(ctx)),
                eviction: null);
        }
        return new(Builder,
            Builder.AddNode("distinct", [NodeId], ctx => new KeyedDistinctOperator<TKey, TValue>(ctx, _eviction)),
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
