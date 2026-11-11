using System.Numerics;
using Incremental.Operators;
using Incremental.Tables;

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
    internal GroupedZStream(CircuitBuilder builder, int nodeId, KeyEviction<TKey>? eviction, Func<TKey, uint>? routingHash)
    {
        Builder = builder;
        NodeId = nodeId;
        Eviction = eviction;
        RoutingHash = routingHash;
    }

    internal CircuitBuilder Builder { get; }
    internal int NodeId { get; }
    internal KeyEviction<TKey>? Eviction { get; }

    /// <summary>The hash this grouping's exchange routed its keys by, when that is known:
    /// set by <c>GroupBy</c> on a sharded circuit and carried through the operators that
    /// keep the key. Null on an unsharded circuit, and where the partitioning comes from
    /// somewhere else (a windowed result is placed by its user key, not its window key). A
    /// table join needs it to send the table's changes to the worker holding each key's facts.</summary>
    internal Func<TKey, uint>? RoutingHash { get; }

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
        return new GroupedZStream<TKey, TValue>(Builder, NodeId, new KeyEviction<TKey>(clock, closesAt), RoutingHash);
    }

    /// <summary>Sets what the operator behind this grouping — <c>GroupBy</c>'s key
    /// selector — does with a record it cannot key: a selector that throws, or a null key.
    /// See <see cref="ZStream{T}.OnError"/>.</summary>
    public GroupedZStream<TKey, TValue> OnError(RecordErrorPolicy policy)
    {
        Builder.SetErrorPolicy(NodeId, policy);
        return this;
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
    /// Retroactive join against a shared table: each record is joined to the table's rows
    /// for its key, and when those rows later change, every record already joined to them
    /// is corrected — the old result retracted, the new one asserted. The key is the
    /// table's own (one row at most) or, through <c>By</c>, a secondary index (one result
    /// per row under it). The circuit keeps this grouping's records and nothing of the
    /// table, which lives once per process however many circuits join it. A hot table row
    /// therefore re-emits every record under its key when it changes; <see cref="Lookup"/>
    /// is the join that does not. Honours <see cref="EvictWhen"/>: a closed key's records
    /// are dropped and its results stay final.
    /// </summary>
    public ZStream<TOut> Join<TRow, TOut>(
        TableView<TKey, TRow> table,
        Func<TKey, TValue, TRow, TOut> resultSelector)
        where TRow : class
        where TOut : notnull
    {
        ArgumentNullException.ThrowIfNull(table);
        ArgumentNullException.ThrowIfNull(resultSelector);
        table.RequireUsableFrom(Builder, "Join");
        return table.Join(this, "Join", resultSelector, unmatched: null);
    }

    /// <summary>
    /// Retroactive left join against a shared table: <paramref name="matched"/> per row the
    /// table has for the record's key, <paramref name="unmatched"/> while it has none,
    /// switching every record under the key from one to the other when the first row
    /// appears or the last is deleted. One operator and one copy of the records: unlike the
    /// left join of two streams, no anti-join is involved.
    /// </summary>
    public ZStream<TOut> LeftJoin<TRow, TOut>(
        TableView<TKey, TRow> table,
        Func<TKey, TValue, TRow, TOut> matched,
        Func<TKey, TValue, TOut> unmatched)
        where TRow : class
        where TOut : notnull
    {
        ArgumentNullException.ThrowIfNull(table);
        ArgumentNullException.ThrowIfNull(matched);
        ArgumentNullException.ThrowIfNull(unmatched);
        table.RequireUsableFrom(Builder, "LeftJoin");
        return table.Join(this, "LeftJoin", matched, unmatched);
    }

    /// <summary>
    /// Enriches each record from a shared table once, as of the step that processes it, and
    /// never corrects it: a later change to the table's rows touches nothing already
    /// emitted, so a hot row causes no fan-out and reference updates no churn. With
    /// <paramref name="missing"/> a record whose key has no row yields that instead;
    /// without it the record is dropped.
    /// <para>
    /// The operator remembers what it emitted for each record, so retracting a record
    /// retracts exactly that — whatever the table says by then — and an upsert (the old
    /// record retracted, the new one asserted) is enriched afresh. A record asserted again
    /// while it is still held is re-enriched as of the later assertion, all of its weight.
    /// That memory is one entry per live record; <see cref="EvictWhen"/> bounds it, dropping
    /// what is remembered for a key once the key closes. After a crash, records replayed
    /// from the source may be enriched from newer rows than they were the first time.
    /// </para>
    /// </summary>
    public ZStream<TOut> Lookup<TRow, TOut>(
        TableView<TKey, TRow> table,
        Func<TKey, TValue, TRow, TOut> matched,
        Func<TKey, TValue, TOut>? missing = null)
        where TRow : class
        where TOut : notnull
    {
        ArgumentNullException.ThrowIfNull(table);
        ArgumentNullException.ThrowIfNull(matched);
        table.RequireUsableFrom(Builder, "Lookup");
        var probe = table.GroupProbe;
        var eviction = Eviction;
        return new ZStream<TOut>(
            Builder,
            Builder.SetProbes(
                Builder.AddNode(
                    "tableLookup",
                    [NodeId],
                    ctx => new TableLookupOperator<TKey, TValue, TRow, TOut>(ctx, probe, matched, missing, eviction)),
                table.TableNodeId));
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
            eviction,
            RoutingHash);
    }

    /// <summary>Left outer join: <paramref name="matched"/> per matching right row, or one
    /// <paramref name="unmatched"/> row while the key has no match. Composed from the inner
    /// join plus the anti-join — the Z-set algebra makes the union incremental for free.</summary>
    public ZStream<TOut> LeftJoin<TRight, TOut>(
        GroupedZStream<TKey, TRight> right,
        Func<TKey, TValue, TRight, TOut> matched,
        Func<TKey, TValue, TOut> unmatched)
        where TRight : notnull
        where TOut : notnull
    {
        var inner = Join(right, matched);
        var outer = AntiJoin(right).Select(kv => unmatched(kv.Key, kv.Value));
        var joined = inner.Concat(outer);
        Builder.SetErrorPolicyTargets(joined.NodeId, inner.NodeId, outer.NodeId);
        return joined;
    }

    /// <summary>Full outer join, composed like the stream <c>LeftJoin</c> with an anti-join per side.</summary>
    public ZStream<TOut> FullJoin<TRight, TOut>(
        GroupedZStream<TKey, TRight> right,
        Func<TKey, TValue, TRight, TOut> matched,
        Func<TKey, TValue, TOut> leftOnly,
        Func<TKey, TRight, TOut> rightOnly)
        where TRight : notnull
        where TOut : notnull
    {
        var inner = Join(right, matched);
        var left = AntiJoin(right).Select(kv => leftOnly(kv.Key, kv.Value));
        var rightRows = right.AntiJoin(this).Select(kv => rightOnly(kv.Key, kv.Value));
        var joined = inner.Concat(left).Concat(rightRows);
        Builder.SetErrorPolicyTargets(joined.NodeId, inner.NodeId, left.NodeId, rightRows.NodeId);
        return joined;
    }

    /// <summary>Grouped aggregation. The result stream carries one (key, result) row per key,
    /// with the previous result retracted whenever it changes.</summary>
    public KeyedZStream<TKey, TResult> Aggregate<TResult>(IAggregateFunction<TValue, TResult> aggregate)
        where TResult : notnull
    {
        Builder.RequireOutsideRecursion("Aggregate");
        return new(Builder,
            Builder.AddNode("aggregate", [NodeId], ctx => new AggregateOperator<TKey, TValue, TResult>(ctx, aggregate, Eviction)),
            Eviction,
            RoutingHash);
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
            Eviction,
            RoutingHash);
    }

    /// <summary>Live count per key. A fully retracted key's row is retracted, never emitted
    /// as zero — weight zero means gone.</summary>
    public KeyedZStream<TKey, long> Count() => AggregateLinear(LinearAggregates.Count<TValue>());

    /// <summary>Live sum of <paramref name="selector"/> per key; see
    /// <see cref="LinearAggregates.Sum{TValue,TNumber}"/> for the overflow and floating-point caveats.</summary>
    public KeyedZStream<TKey, TNumber> Sum<TNumber>(Func<TValue, TNumber> selector)
        where TNumber : INumber<TNumber> =>
        AggregateLinear(LinearAggregates.Sum(selector));

    /// <summary>Grouped MIN maintained by a tournament tree: an update — including deleting
    /// the current minimum — touches a fixed number of small buckets instead of rescanning
    /// the group, so cost is independent of group size. The full-recompute path remains
    /// available as <c>Aggregate(Aggregates.Min(...))</c>.</summary>
    public KeyedZStream<TKey, TProjected> Min<TProjected>(Func<TValue, TProjected> selector)
        where TProjected : notnull
    {
        Builder.RequireOutsideRecursion("Min");
        return new(Builder,
            Builder.AddNode("min", [NodeId], ctx => new ExtremumOperator<TKey, TValue, TProjected>(ctx, selector, sign: -1, Eviction)),
            Eviction,
            RoutingHash);
    }

    /// <summary>Grouped MAX maintained by a tournament tree; see <see cref="Min{TProjected}"/>.</summary>
    public KeyedZStream<TKey, TProjected> Max<TProjected>(Func<TValue, TProjected> selector)
        where TProjected : notnull
    {
        Builder.RequireOutsideRecursion("Max");
        return new(Builder,
            Builder.AddNode("max", [NodeId], ctx => new ExtremumOperator<TKey, TValue, TProjected>(ctx, selector, sign: +1, Eviction)),
            Eviction,
            RoutingHash);
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
            Eviction,
            RoutingHash);
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
                eviction: null,
                RoutingHash);
        }
        return new(Builder,
            Builder.AddNode("distinct", [NodeId], ctx => new KeyedDistinctOperator<TKey, TValue>(ctx, Eviction)),
            Eviction,
            RoutingHash);
    }

    /// <summary>
    /// A stream of window-close notifications: emits each key of this grouping exactly
    /// once, with weight +1, in the first step whose watermark has passed
    /// <paramref name="closesAt"/>(key). Closed means the same as for eviction: the
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
            policy,
            RoutingHash);

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

    internal GroupedEventTimeStream(
        CircuitBuilder builder, int nodeId, Func<TValue, DateTimeOffset> eventTime, Func<TKey, uint>? routingHash)
        : base(builder, nodeId, eviction: null, routingHash)
    {
        _eventTime = eventTime;
    }

    /// <summary>Sets the key selector's error policy, keeping the grouping event-timed; see
    /// <see cref="GroupedZStream{TKey,TValue}.OnError"/>.</summary>
    public new GroupedEventTimeStream<TKey, TValue> OnError(RecordErrorPolicy policy)
    {
        Builder.SetErrorPolicy(NodeId, policy);
        return this;
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
