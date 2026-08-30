using System.Numerics;
using Incremental.Operators;

namespace Incremental;

/// <summary>
/// A grouped stream: records keyed by <typeparamref name="TKey"/> and partitioned across
/// workers by a stable hash of the key. This is a staging type, not a stream — grouping
/// only means something as a prelude to stateful work, so it offers aggregates, joins, and
/// (via <see cref="GroupedEventTimeStream{TKey,TValue}"/>) windows, but no projection or
/// output. Aggregation results come back as <see cref="KeyedZStream{TKey,TValue}"/> rows.
/// </summary>
public class GroupedZStream<TKey, TValue>
    where TKey : notnull
    where TValue : notnull
{
    internal GroupedZStream(CircuitBuilder builder, int nodeId, KeyEviction<TKey>? eviction)
    {
        Builder = builder;
        NodeId = nodeId;
        Eviction = eviction;
    }

    internal CircuitBuilder Builder { get; }
    internal int NodeId { get; }
    internal KeyEviction<TKey>? Eviction { get; }

    /// <summary>
    /// Attaches a watermark-driven eviction policy: once the circuit's watermark passes
    /// <paramref name="closesAt"/>(key), stateful operators downstream of this grouping
    /// (aggregations, TopK, joins, anti-joins, keyed <see cref="Distinct"/>) delete
    /// everything they hold for that key. Already-emitted results for the key remain final.
    /// Advanced: windowed groupings derive this automatically from the window — use this
    /// for keyed state with a lifecycle that isn't window-derived. Requires
    /// <c>WithEventTime</c> on an upstream input. The policy is inherited by keyed streams
    /// derived from this grouping; a two-input operator (join, anti-join) uses the left
    /// side's policy if it has one, otherwise the right's.
    /// </summary>
    public GroupedZStream<TKey, TValue> EvictWhen(Func<TKey, DateTimeOffset> closesAt)
    {
        Builder.RequireOutsideRecursion("EvictWhen");
        var clock = Builder.WatermarkClock
            ?? throw new InvalidOperationException(
                "EvictWhen requires an event-time watermark; call WithEventTime on an input stream first.");
        return new GroupedZStream<TKey, TValue>(Builder, NodeId, new KeyEviction<TKey>(clock, closesAt));
    }

    /// <summary>Incremental equi-join on the shared key.</summary>
    public ZStream<TOut> Join<TRight, TOut>(
        GroupedZStream<TKey, TRight> right,
        Func<TKey, TValue, TRight, TOut> resultSelector)
        where TRight : notnull
        where TOut : notnull
    {
        RequireSameBuilder(right);
        var eviction = Eviction ?? right.Eviction;
        if (Builder.InRecursionScope)
        {
            // Inside a Recurse body the join must be incremental in the iteration dimension
            // too; see RecursionOperators.cs. Eviction has no meaning there.
            if (eviction is not null)
                Builder.RequireOutsideRecursion("Join on a grouping with EvictWhen");
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
    /// Incremental anti-join: the rows of this grouping whose key has no match in
    /// <paramref name="right"/>. When a key gains its first right-side row its left rows are
    /// retracted; when it loses its last they are re-asserted.
    /// </summary>
    public KeyedZStream<TKey, TValue> AntiJoin<TRight>(GroupedZStream<TKey, TRight> right)
        where TRight : notnull
    {
        Builder.RequireOutsideRecursion("AntiJoin");
        RequireSameBuilder(right);
        var eviction = Eviction ?? right.Eviction;
        return new KeyedZStream<TKey, TValue>(
            Builder,
            Builder.AddNode("antiJoin", [NodeId, right.NodeId], ctx => new AntiJoinOperator<TKey, TValue, TRight>(ctx, eviction)),
            eviction);
    }

    /// <summary>Left outer join: <paramref name="matched"/> per matching right row, or one
    /// <paramref name="unmatched"/> row while the key has no match. Composed from the inner
    /// join plus the anti-join — the Z-set algebra makes the union incremental for free.</summary>
    public ZStream<TOut> LeftJoin<TRight, TOut>(
        GroupedZStream<TKey, TRight> right,
        Func<TKey, TValue, TRight, TOut> matched,
        Func<TKey, TValue, TOut> unmatched)
        where TRight : notnull
        where TOut : notnull =>
        Join(right, matched)
            .Concat(AntiJoin(right).Select(kv => unmatched(kv.Key, kv.Value)));

    /// <summary>Full outer join, composed like <see cref="LeftJoin"/> with an anti-join per side.</summary>
    public ZStream<TOut> FullJoin<TRight, TOut>(
        GroupedZStream<TKey, TRight> right,
        Func<TKey, TValue, TRight, TOut> matched,
        Func<TKey, TValue, TOut> leftOnly,
        Func<TKey, TRight, TOut> rightOnly)
        where TRight : notnull
        where TOut : notnull =>
        Join(right, matched)
            .Concat(AntiJoin(right).Select(kv => leftOnly(kv.Key, kv.Value)))
            .Concat(right.AntiJoin(this).Select(kv => rightOnly(kv.Key, kv.Value)));

    /// <summary>Grouped aggregation. The result stream carries one (key, result) row per key,
    /// with the previous result retracted whenever it changes.</summary>
    public KeyedZStream<TKey, TResult> Aggregate<TResult>(IAggregateFunction<TValue, TResult> aggregate)
        where TResult : notnull
    {
        Builder.RequireOutsideRecursion("Aggregate");
        return new(Builder,
            Builder.AddNode("aggregate", [NodeId], ctx => new AggregateOperator<TKey, TValue, TResult>(ctx, aggregate, Eviction)),
            Eviction);
    }

    /// <summary>Grouped aggregation for a linear aggregate (COUNT, SUM, …): maintained from
    /// deltas alone with O(1) state per key — no group contents are stored, so update cost
    /// is independent of group size.</summary>
    public KeyedZStream<TKey, TResult> AggregateLinear<TResult>(ILinearAggregate<TValue, TResult> aggregate)
        where TResult : notnull
    {
        Builder.RequireOutsideRecursion("AggregateLinear");
        return new(Builder,
            Builder.AddNode("aggregateLinear", [NodeId], ctx => new LinearAggregateOperator<TKey, TValue, TResult>(ctx, aggregate, Eviction)),
            Eviction);
    }

    /// <summary>Live count per key. A fully-retracted key's row is retracted, never emitted
    /// as zero — weight zero means gone.</summary>
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
            Builder.AddNode("min", [NodeId], ctx => new ExtremumOperator<TKey, TValue, TProjected>(ctx, selector, sign: -1, Eviction)),
            Eviction);
    }

    /// <summary>Grouped MAX maintained by a tournament tree; see <see cref="Min{TProjected}"/>.</summary>
    public KeyedZStream<TKey, TProjected> Max<TProjected>(Func<TValue, TProjected> selector)
        where TProjected : notnull
    {
        Builder.RequireOutsideRecursion("Max");
        return new(Builder,
            Builder.AddNode("max", [NodeId], ctx => new ExtremumOperator<TKey, TValue, TProjected>(ctx, selector, sign: +1, Eviction)),
            Eviction);
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
            Builder.AddNode("topK", [NodeId], ctx => new TopKOperator<TKey, TValue, TOrder>(ctx, count, orderBy, descending, Eviction)),
            Eviction);
    }

    /// <summary>Collapses each group's multiset of values to a set (weight exactly 1 per
    /// present (key, value) pair). The result stays partitioned by key: continue with
    /// <c>Grouped()</c> for a distinct-count (<c>.Distinct().Grouped().Count()</c>).</summary>
    public KeyedZStream<TKey, TValue> Distinct()
    {
        if (Builder.InRecursionScope)
        {
            // Iteration-aware distinct over the (key, value) pairs; the key partitioning
            // already routes equal pairs to the same worker. Eviction has no meaning here.
            if (Eviction is not null)
                Builder.RequireOutsideRecursion("Distinct on a grouping with EvictWhen");
            return new(Builder,
                Builder.AddNode("distinct", [NodeId], ctx => new LoopDistinctOperator<KeyedValue<TKey, TValue>>(ctx)),
                eviction: null);
        }
        return new(Builder,
            Builder.AddNode("distinct", [NodeId], ctx => new KeyedDistinctOperator<TKey, TValue>(ctx, Eviction)),
            Eviction);
    }

    /// <summary>
    /// A stream of window-close notifications: emits each key of this grouping exactly
    /// once, with weight +1, in the first step whose watermark has passed
    /// <paramref name="closesAt"/>(key). Closed means what it means for eviction: the
    /// watermark admits no further events for the key, so its results are final —
    /// notifications are never retracted, and a key whose contents were fully retracted
    /// still closes. The value carried is the key's close deadline. The result stays
    /// partitioned by key (each key is announced by the worker owning it), so it can be
    /// joined with the key's aggregate to publish finalized results — the join emits them
    /// in the same step its state for the key is evicted. Advanced: windowed results get
    /// this wired up for free via <c>OnClose()</c>. Requires <c>WithEventTime</c> on an
    /// upstream input.
    /// </summary>
    public KeyedZStream<TKey, DateTimeOffset> WindowCloses(Func<TKey, DateTimeOffset> closesAt)
    {
        Builder.RequireOutsideRecursion("WindowCloses");
        var clock = Builder.WatermarkClock
            ?? throw new InvalidOperationException(
                "WindowCloses requires an event-time watermark; call WithEventTime on an input stream first.");
        return WindowClosesCore(new KeyEviction<TKey>(clock, closesAt));
    }

    /// <summary>Window-close notifications at the deadline of the eviction policy attached
    /// by <see cref="EvictWhen"/>: keys are announced final at the same watermark the
    /// stateful operators drop their state.</summary>
    public KeyedZStream<TKey, DateTimeOffset> WindowCloses()
    {
        Builder.RequireOutsideRecursion("WindowCloses");
        var policy = Eviction
            ?? throw new InvalidOperationException(
                "WindowCloses without a deadline requires an eviction policy; call EvictWhen first or pass closesAt explicitly.");
        return WindowClosesCore(policy);
    }

    private KeyedZStream<TKey, DateTimeOffset> WindowClosesCore(KeyEviction<TKey> policy) =>
        new(Builder,
            Builder.AddNode("windowClose", [NodeId], _ => new WindowCloseOperator<TKey, TValue>(policy)),
            policy);

    private protected void RequireSameBuilder<TOtherKey, TOther>(GroupedZStream<TOtherKey, TOther> other)
        where TOtherKey : notnull
        where TOther : notnull
    {
        if (!ReferenceEquals(Builder, other.Builder))
            throw new ArgumentException("Streams from different circuit builders cannot be combined.", nameof(other));
    }
}

/// <summary>
/// A grouping of an event-timed stream: everything a <see cref="GroupedZStream{TKey,TValue}"/>
/// offers, plus windows. Windows attach to groupings — the window becomes part of each
/// group's identity automatically, and state for a window is evicted once the watermark
/// passes its end, with no bookkeeping on the caller's side.
/// </summary>
public sealed class GroupedEventTimeStream<TKey, TValue> : GroupedZStream<TKey, TValue>
    where TKey : notnull
    where TValue : notnull
{
    private readonly Func<TValue, DateTimeOffset> _eventTime;

    internal GroupedEventTimeStream(CircuitBuilder builder, int nodeId, Func<TValue, DateTimeOffset> eventTime)
        : base(builder, nodeId, eviction: null)
    {
        _eventTime = eventTime;
    }

    /// <summary>Windows each group into fixed, non-overlapping windows of
    /// <paramref name="size"/> (epoch-aligned, UTC) by event time.</summary>
    public WindowedGroupedZStream<TKey, TValue> TumblingWindow(TimeSpan size) => HoppingWindow(size, size);

    /// <summary>Windows each group into windows of <paramref name="size"/> whose starts are
    /// multiples of <paramref name="hop"/> (epoch-aligned, UTC): each record joins every
    /// window covering its event time.</summary>
    public WindowedGroupedZStream<TKey, TValue> HoppingWindow(TimeSpan size, TimeSpan hop)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(size, TimeSpan.Zero, nameof(size));
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(hop, TimeSpan.Zero, nameof(hop));
        Builder.RequireOutsideRecursion("HoppingWindow");
        var eventTime = _eventTime;
        var sizeTicks = size.Ticks;
        var hopTicks = hop.Ticks;
        var node = Builder.AddNode(
            "window",
            [NodeId],
            _ => new WindowAssignOperator<TKey, TValue>(
                value => eventTime(value).UtcTicks, sizeTicks, hopTicks));
        return new WindowedGroupedZStream<TKey, TValue>(Builder, node, WindowEviction());
    }

    /// <summary>
    /// Groups each key's events into sessions separated by <paramref name="gap"/> of
    /// event-time inactivity: consecutive events of a key belong to the same session while
    /// they are strictly less than the gap apart, and a session spans
    /// [first event, last event + gap) — an event exactly at a session's end starts a new
    /// one. Session assignment is not a pure projection: an insertion can extend a session
    /// or merge two (a bridging or late event), and a retraction can shrink or split one —
    /// the rows whose session bounds changed are retracted and re-asserted, recomputing
    /// touched keys only.
    /// </summary>
    public WindowedGroupedZStream<TKey, TValue> SessionWindow(TimeSpan gap)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(gap, TimeSpan.Zero, nameof(gap));
        Builder.RequireOutsideRecursion("SessionWindow");
        var builder = Builder;
        var eventTime = _eventTime;
        var gapTicks = gap.Ticks;
        var sessions = builder.AddNode(
            "sessionWindow",
            [NodeId],
            ctx => new SessionWindowOperator<TKey, TValue>(ctx, gapTicks, eventTime, builder.WatermarkClock));
        // Fold the session bounds into the key. Same user key → same worker, so no exchange.
        var node = builder.AddNode(
            "windowKey",
            [sessions],
            _ => new MapOperator<KeyedValue<TKey, Windowed<TValue>>, KeyedValue<WindowKey<TKey>, TValue>>(
                kv => new KeyedValue<WindowKey<TKey>, TValue>(
                    new WindowKey<TKey>(kv.Value.WindowStart, kv.Value.WindowEnd, kv.Key), kv.Value.Value)));
        return new WindowedGroupedZStream<TKey, TValue>(Builder, node, WindowEviction());
    }

    /// <summary>A window's state closes when the watermark passes its end — the lateness
    /// margin is already inside the watermark itself.</summary>
    private KeyEviction<WindowKey<TKey>> WindowEviction() =>
        new(Builder.WatermarkClock!, static wk => wk.WindowEnd);
}
