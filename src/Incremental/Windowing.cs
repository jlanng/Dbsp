using System.Numerics;
using Incremental.Operators;

namespace Incremental;

/// <summary>A value assigned to one event-time window — the window-as-data form used by the
/// session operator internally and by the manual-windowing escape hatch (assign windows
/// with <c>SelectMany</c>, group by a composite key, wire eviction with
/// <c>EvictWhen</c>).</summary>
public readonly record struct Windowed<T>(DateTimeOffset WindowStart, DateTimeOffset WindowEnd, T Value)
    where T : notnull;

/// <summary>The composite key of a windowed grouping: the window bounds plus the user key.
/// The library composes and routes this for you — user code mostly meets it inside
/// checkpointed state, never in lambdas.</summary>
public readonly record struct WindowKey<TKey>(DateTimeOffset WindowStart, DateTimeOffset WindowEnd, TKey Key)
    where TKey : notnull;

/// <summary>One row of a windowed result: the window bounds, the group key, and the
/// aggregated value — flat, so consumer lambdas read <c>row.Key</c> and <c>row.Value</c>
/// without unwrapping.</summary>
public readonly record struct WindowedValue<TKey, TValue>(
    DateTimeOffset WindowStart, DateTimeOffset WindowEnd, TKey Key, TValue Value)
    where TKey : notnull
    where TValue : notnull;

/// <summary>A window's bounds alone — the routing key of window-collapsing operations
/// (per-window <c>TopK</c>), public because it names checkpointed state.</summary>
public readonly record struct WindowBounds(DateTimeOffset WindowStart, DateTimeOffset WindowEnd);

/// <summary>
/// A windowed grouping: records keyed by (window, key) and partitioned by the user key, so
/// every window of a key lives on the worker that owns the key and closed-window state is
/// evicted worker-locally. Aggregates only — the aggregation is what a windowed grouping is
/// for; its results come back as <see cref="WindowedZStream{TKey,TValue}"/> rows with
/// eviction already wired to the window lifecycle.
/// </summary>
public sealed class WindowedGroupedZStream<TKey, TValue>
    where TKey : notnull
    where TValue : notnull
{
    internal WindowedGroupedZStream(CircuitBuilder builder, int nodeId, KeyEviction<WindowKey<TKey>> eviction)
    {
        Builder = builder;
        NodeId = nodeId;
        Eviction = eviction;
    }

    internal CircuitBuilder Builder { get; }
    internal int NodeId { get; }
    internal KeyEviction<WindowKey<TKey>> Eviction { get; }

    /// <summary>Windowed grouped aggregation: one (window, key, result) row per group, with
    /// the previous result retracted whenever it changes, and the group's state evicted when
    /// the watermark closes its window.</summary>
    public WindowedZStream<TKey, TResult> Aggregate<TResult>(IAggregateFunction<TValue, TResult> aggregate)
        where TResult : notnull
    {
        Builder.RequireOutsideRecursion("Aggregate");
        return new(Builder,
            Builder.AddNode("aggregate", [NodeId],
                ctx => new AggregateOperator<WindowKey<TKey>, TValue, TResult>(ctx, aggregate, Eviction)),
            Eviction);
    }

    /// <summary>Windowed aggregation for a linear aggregate (COUNT, SUM, …): maintained from
    /// deltas alone with O(1) state per (window, key).</summary>
    public WindowedZStream<TKey, TResult> AggregateLinear<TResult>(ILinearAggregate<TValue, TResult> aggregate)
        where TResult : notnull
    {
        Builder.RequireOutsideRecursion("AggregateLinear");
        return new(Builder,
            Builder.AddNode("aggregateLinear", [NodeId],
                ctx => new LinearAggregateOperator<WindowKey<TKey>, TValue, TResult>(ctx, aggregate, Eviction)),
            Eviction);
    }

    /// <summary>Live count per (window, key). A fully retracted group's row is retracted,
    /// never emitted as zero — weight zero means gone.</summary>
    public WindowedZStream<TKey, long> Count() => AggregateLinear(LinearAggregates.Count<TValue>());

    /// <summary>Live sum of <paramref name="selector"/> per (window, key); see
    /// <see cref="LinearAggregates.Sum{TValue,TNumber}"/> for the overflow and floating-point caveats.</summary>
    public WindowedZStream<TKey, TNumber> Sum<TNumber>(Func<TValue, TNumber> selector)
        where TNumber : INumber<TNumber> =>
        AggregateLinear(LinearAggregates.Sum(selector));

    /// <summary>Windowed grouped MIN; see <c>GroupedZStream.Min</c> for the mechanism.</summary>
    public WindowedZStream<TKey, TProjected> Min<TProjected>(Func<TValue, TProjected> selector)
        where TProjected : notnull
    {
        Builder.RequireOutsideRecursion("Min");
        return new(Builder,
            Builder.AddNode("min", [NodeId],
                ctx => new ExtremumOperator<WindowKey<TKey>, TValue, TProjected>(ctx, selector, sign: -1, Eviction)),
            Eviction);
    }

    /// <summary>Windowed grouped MAX; see <c>GroupedZStream.Min</c> for the mechanism.</summary>
    public WindowedZStream<TKey, TProjected> Max<TProjected>(Func<TValue, TProjected> selector)
        where TProjected : notnull
    {
        Builder.RequireOutsideRecursion("Max");
        return new(Builder,
            Builder.AddNode("max", [NodeId],
                ctx => new ExtremumOperator<WindowKey<TKey>, TValue, TProjected>(ctx, selector, sign: +1, Eviction)),
            Eviction);
    }
}

/// <summary>
/// A windowed result: a stream of (window, key, value) rows in which everything is
/// per-window by construction.
/// <see cref="GroupBy{TNewKey}(Func{WindowedValue{TKey,TValue},TNewKey})"/> is window-sticky
/// (the new key composes within the window, eviction re-derived), <see cref="TopK"/> ranks within each window,
/// <see cref="OnClose"/> emits each row exactly once when the watermark closes its window,
/// and <see cref="Rows"/> is the explicit exit that drops window semantics. Live results
/// flap by design — each update retracts the stale row and asserts the new one; use
/// <see cref="OnClose"/> for finals only.
/// </summary>
public sealed class WindowedZStream<TKey, TValue> : ZStream<WindowedValue<TKey, TValue>>
    where TKey : notnull
    where TValue : notnull
{
    private readonly int _keyedNodeId;
    private readonly KeyEviction<WindowKey<TKey>> _eviction;

    internal WindowedZStream(CircuitBuilder builder, int keyedNodeId, KeyEviction<WindowKey<TKey>> eviction)
        : base(builder, AddRowsNode(builder, keyedNodeId))
    {
        _keyedNodeId = keyedNodeId;
        _eviction = eviction;
    }

    private static int AddRowsNode(CircuitBuilder builder, int keyedNodeId) =>
        builder.AddNode("rows", [keyedNodeId],
            _ => new MapOperator<KeyedValue<WindowKey<TKey>, TValue>, WindowedValue<TKey, TValue>>(Row));

    private static WindowedValue<TKey, TValue> Row(KeyedValue<WindowKey<TKey>, TValue> kv) =>
        new(kv.Key.WindowStart, kv.Key.WindowEnd, kv.Key.Key, kv.Value);

    /// <summary>Names the operator this windowed result carries; see <see cref="ZStream{T}.Named"/>.</summary>
    public new WindowedZStream<TKey, TValue> Named(string name)
    {
        Builder.SetStateId(_keyedNodeId, name);
        return this;
    }

    /// <summary>Transforms values while preserving the window, the key, and the partitioning.</summary>
    public WindowedZStream<TKey, TOut> MapValues<TOut>(Func<TValue, TOut> selector) where TOut : notnull =>
        new(Builder, Builder.AddNode(
                "mapValues",
                [_keyedNodeId],
                ctx => new MapOperator<KeyedValue<WindowKey<TKey>, TValue>, KeyedValue<WindowKey<TKey>, TOut>>(
                    KeySelectors.MapValues<WindowKey<TKey>, TValue, TOut>(selector, guarded: ctx.Errors is not null), ctx.Errors)),
            _eviction);

    /// <summary>Keeps the rows matching <paramref name="predicate"/>, staying windowed.</summary>
    public new WindowedZStream<TKey, TValue> Where(Func<WindowedValue<TKey, TValue>, bool> predicate) =>
        new(Builder, Builder.AddNode(
                "where",
                [_keyedNodeId],
                ctx => new FilterOperator<KeyedValue<WindowKey<TKey>, TValue>>(kv => predicate(Row(kv)), ctx.Errors)),
            _eviction);

    /// <summary>
    /// Regroups the windowed rows by a new key, <em>within</em> each window: the new
    /// grouping key composes as (window, <paramref name="keySelector"/>(row)) and eviction
    /// is re-derived from the window — you cannot accidentally aggregate across windows.
    /// Crossing windows on purpose is <see cref="Rows"/>. Inserts an exchange on the
    /// composite key (window bounds route through the stable hash).
    /// </summary>
    public new WindowedGroupedZStream<TNewKey, WindowedValue<TKey, TValue>> GroupBy<TNewKey>(
        Func<WindowedValue<TKey, TValue>, TNewKey> keySelector)
        where TNewKey : notnull =>
        GroupByCore(keySelector,
            Builder.Options.IsSharded ? StableHash.RoutingHasherFor<WindowKey<TNewKey>>("GroupBy") : null);

    /// <summary>Window-sticky regrouping with a caller-supplied routing hash over the
    /// composite (window, key); see <see cref="GroupBy{TNewKey}(Func{WindowedValue{TKey,TValue},TNewKey})"/>.</summary>
    public WindowedGroupedZStream<TNewKey, WindowedValue<TKey, TValue>> GroupBy<TNewKey>(
        Func<WindowedValue<TKey, TValue>, TNewKey> keySelector,
        Func<WindowKey<TNewKey>, uint> stableHash)
        where TNewKey : notnull
    {
        ArgumentNullException.ThrowIfNull(stableHash);
        return GroupByCore(keySelector, stableHash);
    }

    private WindowedGroupedZStream<TNewKey, WindowedValue<TKey, TValue>> GroupByCore<TNewKey>(
        Func<WindowedValue<TKey, TValue>, TNewKey> keySelector, Func<WindowKey<TNewKey>, uint>? keyHash)
        where TNewKey : notnull
    {
        var keyed = Builder.AddNode(
            "groupBy",
            [_keyedNodeId],
            ctx => new MapOperator<KeyedValue<WindowKey<TKey>, TValue>, KeyedValue<WindowKey<TNewKey>, WindowedValue<TKey, TValue>>>(
                ctx.Errors is null
                    ? kv =>
                    {
                        var row = Row(kv);
                        return new KeyedValue<WindowKey<TNewKey>, WindowedValue<TKey, TValue>>(
                            new WindowKey<TNewKey>(row.WindowStart, row.WindowEnd, keySelector(row)), row);
                    }
                    : kv =>
                    {
                        // As KeySelectors.Keyed: a null key becomes an exception the
                        // dead-letter policy catches.
                        var row = Row(kv);
                        var key = RecordErrors.Key(keySelector(row));
                        return new KeyedValue<WindowKey<TNewKey>, WindowedValue<TKey, TValue>>(
                            new WindowKey<TNewKey>(row.WindowStart, row.WindowEnd, key), row);
                    },
                ctx.Errors));

        var node = keyed;
        if (Builder.Options.IsSharded)
        {
            node = Builder.AddNode(
                "exchange",
                [keyed],
                _ => new ExchangeOperator<KeyedValue<WindowKey<TNewKey>, WindowedValue<TKey, TValue>>>(kv => keyHash!(kv.Key)),
                isExchange: true);
        }
        return new WindowedGroupedZStream<TNewKey, WindowedValue<TKey, TValue>>(
            Builder, node, new KeyEviction<WindowKey<TNewKey>>(_eviction.Clock, static wk => wk.WindowEnd));
    }

    /// <summary>
    /// Keeps the top <paramref name="count"/> rows <em>within each window</em>, ranked
    /// across all keys by <paramref name="orderBy"/> (largest first by default). The output
    /// carries the rows themselves: when membership changes, displaced rows are retracted
    /// and newly ranked rows asserted. A global per-window ranking requires the window's
    /// rows on one worker, so this inserts an exchange routed by the window bounds — each
    /// window's ranking work lands on a single worker.
    /// </summary>
    public WindowedZStream<TKey, TValue> TopK<TOrder>(
        int count, Func<WindowedValue<TKey, TValue>, TOrder> orderBy, bool descending = true)
    {
        Builder.RequireOutsideRecursion("TopK");
        ArgumentOutOfRangeException.ThrowIfLessThan(count, 1, nameof(count));

        var byWindow = Builder.AddNode(
            "groupByWindow",
            [_keyedNodeId],
            _ => new MapOperator<KeyedValue<WindowKey<TKey>, TValue>, KeyedValue<WindowBounds, WindowedValue<TKey, TValue>>>(
                kv => new KeyedValue<WindowBounds, WindowedValue<TKey, TValue>>(
                    new WindowBounds(kv.Key.WindowStart, kv.Key.WindowEnd), Row(kv))));

        var node = byWindow;
        if (Builder.Options.IsSharded)
        {
            var hash = StableHash.RoutingHasherFor<WindowBounds>("TopK");
            node = Builder.AddNode(
                "exchange",
                [byWindow],
                _ => new ExchangeOperator<KeyedValue<WindowBounds, WindowedValue<TKey, TValue>>>(kv => hash(kv.Key)),
                isExchange: true);
        }

        var windowEviction = new KeyEviction<WindowBounds>(_eviction.Clock, static b => b.WindowEnd);
        var top = Builder.AddNode(
            "topK",
            [node],
            ctx => new TopKOperator<WindowBounds, WindowedValue<TKey, TValue>, TOrder>(ctx, count, orderBy, descending, windowEviction));

        // Back to the canonical (window, key) → value shape. Rows of one window are already
        // co-located, so the re-key is worker-local.
        var rekeyed = Builder.AddNode(
            "windowKey",
            [top],
            _ => new MapOperator<KeyedValue<WindowBounds, WindowedValue<TKey, TValue>>, KeyedValue<WindowKey<TKey>, TValue>>(
                kv => new KeyedValue<WindowKey<TKey>, TValue>(
                    new WindowKey<TKey>(kv.Value.WindowStart, kv.Value.WindowEnd, kv.Value.Key), kv.Value.Value)));
        return new WindowedZStream<TKey, TValue>(Builder, rekeyed, _eviction);
    }

    /// <summary>
    /// Emits each row exactly once, when the watermark closes its window — the incremental
    /// analogue of emit-on-close/suppression: no flapping, results are final. Rows are
    /// published in the same step the window's state is evicted; a window whose contents
    /// were fully retracted closes without emitting rows (it has none).
    /// </summary>
    public WindowedZStream<TKey, TValue> OnClose()
    {
        Builder.RequireOutsideRecursion("OnClose");
        var eviction = _eviction;
        var closes = Builder.AddNode(
            "windowClose",
            [_keyedNodeId],
            _ => new WindowCloseOperator<WindowKey<TKey>, TValue>(eviction));
        var final = Builder.AddNode(
            "onClose",
            [closes, _keyedNodeId],
            ctx => new JoinOperator<WindowKey<TKey>, DateTimeOffset, TValue, KeyedValue<WindowKey<TKey>, TValue>>(
                ctx, static (key, _, value) => new KeyedValue<WindowKey<TKey>, TValue>(key, value), eviction));
        return new WindowedZStream<TKey, TValue>(Builder, final, eviction);
    }

    /// <summary>The explicit exit from window semantics: the same rows as a plain stream.
    /// Downstream of here you own key composition and state lifecycle yourself (group with
    /// <c>GroupBy</c>, bound state with <c>EvictWhen</c>).</summary>
    public ZStream<WindowedValue<TKey, TValue>> Rows() => new(Builder, NodeId);

    /// <summary>
    /// Marks this windowed result as a circuit output; redeem the returned definition with
    /// <see cref="Circuit.Handle{T}(Output{T})"/> after <c>Build()</c>. Table-shaped
    /// outputs resync consumers automatically: with <c>bootstrap</c> (the
    /// default) the output maintains checkpoint-backed snapshot state, and the handle's
    /// <c>Changes()</c> begins with one batch asserting the current integrated state —
    /// correct across restarts and mid-run subscription. Pass <c>false</c> to opt out when
    /// the consumer is stateless and the O(live rows) state cost isn't wanted.
    /// </summary>
    public new Output<WindowedValue<TKey, TValue>> ToOutput() => ToOutput(bootstrap: true);

    /// <inheritdoc cref="ToOutput()" path="/summary"/>
    public Output<WindowedValue<TKey, TValue>> ToOutput(bool bootstrap)
    {
        var nodeId = NodeId;
        if (bootstrap)
        {
            Builder.RequireOutsideRecursion("ToOutput");
            nodeId = Builder.AddNode("snapshot", [nodeId], ctx => new SnapshotOperator<WindowedValue<TKey, TValue>>(ctx));
        }
        return new Output<WindowedValue<TKey, TValue>>(
            Builder, Builder.AddOutput<WindowedValue<TKey, TValue>>(nodeId, bootstrap));
    }
}
