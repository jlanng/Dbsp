using Dbsp.Operators;
using Dbsp.Tables;

namespace Dbsp;

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

    /// <summary>
    /// Gives the operator this stream carries a stable state identity. By default an
    /// operator's checkpointed state is keyed by its position in the circuit, so inserting,
    /// removing, or reordering upstream operators renumbers it and orphans its committed
    /// state (a mismatch the circuit detects at restart and refuses to run on). A named
    /// operator keys its state by the name instead: the circuit can evolve around it and a
    /// rebuilt circuit still finds its state. Name the streams returned by stateful
    /// operators — joins, aggregations, <c>Distinct</c>, <c>TopK</c>, session windows;
    /// names must be unique within the circuit. The name pins identity, not meaning: state
    /// is resumed as-is, so an operator whose computation changes (say, a different
    /// aggregate behind the same name) silently reinterprets old state — rename it when
    /// the meaning changes.
    /// </summary>
    public ZStream<T> Named(string name)
    {
        Builder.SetStateId(NodeId, name);
        return this;
    }

    /// <summary>
    /// Sets what the operator this stream carries does with a record it cannot process — a
    /// delegate that throws, a null key or result — overriding
    /// <see cref="CircuitOptions.ErrorPolicy"/> for that one operator. It applies to the
    /// operator that produced this stream and to nothing upstream of it; where one call
    /// builds several operators (<c>stream.Lookup(table, keySelector, …)</c> is a grouping
    /// and a lookup) it reaches the last — except the stream <c>LeftJoin</c> and
    /// <c>FullJoin</c>, where it reaches every selector of the join. See <see cref="RecordErrorPolicy"/> for which
    /// operators can dead-letter; on the others the setting changes nothing and a failure
    /// still faults the circuit.
    /// </summary>
    public ZStream<T> OnError(RecordErrorPolicy policy)
    {
        Builder.SetErrorPolicy(NodeId, policy);
        return this;
    }

    /// <summary>Projects each record. Like LINQ's <c>Select</c>, but over a stream of
    /// changes: weights pass through unchanged, so retracting an input record retracts
    /// its projection.</summary>
    public ZStream<TOut> Select<TOut>(Func<T, TOut> selector) where TOut : notnull =>
        new(Builder, Builder.AddNode("select", [NodeId], ctx => new MapOperator<T, TOut>(selector, ctx.Errors)));

    /// <summary>Keeps the records matching <paramref name="predicate"/>.</summary>
    public ZStream<T> Where(Func<T, bool> predicate) =>
        new(Builder, Builder.AddNode("where", [NodeId], ctx => new FilterOperator<T>(predicate, ctx.Errors)));

    /// <summary>Projects each record to zero or more records; every produced record carries
    /// the source record's weight.</summary>
    public ZStream<TOut> SelectMany<TOut>(Func<T, IEnumerable<TOut>> selector) where TOut : notnull =>
        new(Builder, Builder.AddNode("selectMany", [NodeId], ctx => new FlatMapOperator<T, TOut>(selector, ctx.Errors)));

    /// <summary>Negates all weights (turns this stream's insertions into retractions and
    /// vice versa). Advanced: pair with <see cref="Concat"/> for Z-set arithmetic.</summary>
    public ZStream<T> Negate() =>
        new(Builder, Builder.AddNode("negate", [NodeId], _ => new NegateOperator<T>()));

    /// <summary>Merges this stream with <paramref name="other"/> (Z-set addition — a
    /// duplicate-preserving union, like LINQ's <c>Concat</c>).</summary>
    public ZStream<T> Concat(ZStream<T> other)
    {
        RequireSameBuilder(other);
        return new ZStream<T>(Builder, Builder.AddNode("concat", [NodeId, other.NodeId], _ => new PlusOperator<T>()));
    }

    /// <summary>Z-set subtraction: this stream minus <paramref name="other"/>, pointwise on
    /// weights. Advanced: the result can carry negative weights — deliberately not LINQ's
    /// <c>Except</c>, which is a distinct set difference.</summary>
    public ZStream<T> Minus(ZStream<T> other) => Concat(other.Negate());

    /// <summary>Collapses the multiset to a set: every present value gets weight exactly 1.</summary>
    public ZStream<T> Distinct() =>
        DistinctCore(Builder.Options.IsSharded ? StableHash.RoutingHasherFor<T>("Distinct") : null);

    /// <summary>Collapses the multiset to a set, routing values across workers by
    /// <paramref name="stableHash"/> instead of the built-in structural hash — for value
    /// types the structural hash cannot derive (or types whose equality diverges from their
    /// public structure). The hash must return the same value for equal records in every
    /// process, on every machine: exchange routing and checkpoint restore depend on it.</summary>
    public ZStream<T> Distinct(Func<T, uint> stableHash)
    {
        ArgumentNullException.ThrowIfNull(stableHash);
        return DistinctCore(stableHash);
    }

    private protected ZStream<T> DistinctCore(Func<T, uint>? routingHash)
    {
        var upstream = NodeId;
        if (Builder.Options.IsSharded)
        {
            // Distinct state is per-value; route equal values to the same worker.
            upstream = Builder.AddNode(
                "shuffle",
                [upstream],
                _ => new ExchangeOperator<T>(routingHash!),
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
    /// <c>GroupBy</c>/<c>Join</c>/<c>Distinct</c>; it may also reference other streams of
    /// the circuit (they are imported into the loop automatically). Within every outer step
    /// the subcircuit iterates until no delta circulates on any worker — workers iterate in
    /// lockstep, so exchanges inside the body stay barrier-aligned — and the result stream
    /// carries the net change of the new fixed point versus the previous step's, so
    /// downstream operators see ordinary incremental deltas. Retractions flow through like
    /// any other delta: deleting a seed or imported row retracts exactly the derived rows
    /// that no longer have a well-founded derivation, cyclic derivations included. The
    /// classic use is transitive closure:
    /// <code>
    /// var closure = edges.Select(e =&gt; (e.From, e.To))
    ///     .Recurse(paths =&gt; paths
    ///         .GroupBy(p =&gt; p.To)
    ///         .Join(edges.GroupBy(e =&gt; e.From), (_, p, e) =&gt; (p.From, e.To)));
    /// </code>
    /// </summary>
    /// <param name="body">The monotone step of the recursion, applied to the recursive stream.</param>
    /// <param name="maxIterations">Iterations allowed per outer step before the circuit
    /// faults with <see cref="InvalidOperationException"/> — a guard against bodies that
    /// never converge (a non-monotone body can oscillate forever).</param>
    public ZStream<T> Recurse(Func<ZStream<T>, ZStream<T>> body, int maxIterations = 10_000)
    {
        ArgumentNullException.ThrowIfNull(body);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxIterations, 1, nameof(maxIterations));

        var scope = Builder.OpenRecursionScope();
        var feedback = new ZStream<T>(Builder, Builder.AddNode("loopFeedback", [], _ => new LoopFeedbackOperator<T>()));
        var bodyResult = body(feedback);
        ArgumentNullException.ThrowIfNull(bodyResult);
        RequireSameBuilder(bodyResult);
        var stabilized = Concat(bodyResult).Distinct();
        var exit = Builder.AddNode("loopExit", [stabilized.NodeId], _ => new LoopExitOperator<T>());
        Builder.CloseRecursionScope(scope, stabilized.NodeId, exit, maxIterations);
        return new ZStream<T>(Builder, exit);
    }

    /// <summary>
    /// Groups the stream by <paramref name="keySelector"/>, inserting an exchange so that
    /// all records with equal keys are processed by the same worker (the DBSP equivalent of
    /// a shuffle/repartition). The result is a staging type: grouping only means something
    /// as a prelude to stateful work — aggregate it, join it, or (on an event-timed stream)
    /// window it. Sharded circuits route by <see cref="StableHash"/>, so a key type it
    /// cannot derive a hash for is rejected here, at definition time — supply one via the
    /// overload.
    /// </summary>
    public GroupedZStream<TKey, T> GroupBy<TKey>(Func<T, TKey> keySelector) where TKey : notnull =>
        GroupByCore(keySelector, Builder.Options.IsSharded ? StableHash.RoutingHasherFor<TKey>("GroupBy") : null);

    /// <summary>
    /// Groups the stream by <paramref name="keySelector"/>, routing keys across workers by
    /// <paramref name="stableHash"/> instead of the built-in structural hash — for key types
    /// the structural hash cannot derive (or whose equality diverges from their public
    /// structure). The hash must return the same value for equal keys in every process, on
    /// every machine: exchange routing and checkpoint restore depend on it.
    /// </summary>
    public GroupedZStream<TKey, T> GroupBy<TKey>(Func<T, TKey> keySelector, Func<TKey, uint> stableHash)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(stableHash);
        return GroupByCore(keySelector, stableHash);
    }

    private GroupedZStream<TKey, T> GroupByCore<TKey>(Func<T, TKey> keySelector, Func<TKey, uint>? keyHash)
        where TKey : notnull
    {
        var keyed = Builder.AddNode(
            "groupBy",
            [NodeId],
            ctx => new MapOperator<T, KeyedValue<TKey, T>>(KeySelectors.Keyed(keySelector, guarded: ctx.Errors is not null), ctx.Errors));

        var node = keyed;
        if (Builder.Options.IsSharded)
        {
            node = Builder.AddNode(
                "exchange",
                [keyed],
                _ => new ExchangeOperator<KeyedValue<TKey, T>>(kv => keyHash!(kv.Key)),
                isExchange: true);
        }
        return new GroupedZStream<TKey, T>(Builder, node, eviction: null, keyHash);
    }

    /// <summary>
    /// Enriches each record from a shared table once, as of the step that processes it:
    /// <paramref name="matched"/> per row the table has under
    /// <paramref name="keySelector"/>'s value, <paramref name="missing"/> when it has none
    /// (the record is dropped if that is not given). Unlike a table <c>Join</c>, a later
    /// change to those rows corrects nothing already emitted — no fan-out from a hot row,
    /// no churn from reference updates. Retractions are still exact: the operator remembers
    /// what it emitted per record, so a retracted or upserted record takes back precisely
    /// the row it produced, whatever the table says by then.
    /// <para>
    /// That memory is state keyed by the record, so the stream is first partitioned by the
    /// lookup key — this is <c>GroupBy(keySelector).Lookup(...)</c>, and going through the
    /// grouping yourself is how to attach <c>EvictWhen</c> to bound it. A stream that only
    /// ever inserts can skip the state altogether with <see cref="LookupAppendOnly"/>.
    /// </para>
    /// </summary>
    public ZStream<TOut> Lookup<TKey, TRow, TOut>(
        TableView<TKey, TRow> table,
        Func<T, TKey> keySelector,
        Func<T, TRow, TOut> matched,
        Func<T, TOut>? missing = null)
        where TKey : notnull
        where TRow : class
        where TOut : notnull
    {
        ArgumentNullException.ThrowIfNull(matched);
        return GroupBy(keySelector).Lookup(table, WithoutKey<TKey, TRow, TOut>(matched), WithoutKey<TKey, TOut>(missing));
    }

    /// <summary><see cref="Lookup{TKey,TRow,TOut}(TableView{TKey,TRow}, Func{T,TKey}, Func{T,TRow,TOut}, Func{T,TOut}?)"/>,
    /// routing lookup keys across workers by <paramref name="stableHash"/> instead of the
    /// built-in structural hash — for key types it cannot derive one for. The hash must
    /// return the same value for equal keys in every process: checkpoint restore depends
    /// on it.</summary>
    public ZStream<TOut> Lookup<TKey, TRow, TOut>(
        TableView<TKey, TRow> table,
        Func<T, TKey> keySelector,
        Func<TKey, uint> stableHash,
        Func<T, TRow, TOut> matched,
        Func<T, TOut>? missing = null)
        where TKey : notnull
        where TRow : class
        where TOut : notnull
    {
        ArgumentNullException.ThrowIfNull(matched);
        return GroupBy(keySelector, stableHash)
            .Lookup(table, WithoutKey<TKey, TRow, TOut>(matched), WithoutKey<TKey, TOut>(missing));
    }

    private static Func<TKey, T, TRow, TOut> WithoutKey<TKey, TRow, TOut>(Func<T, TRow, TOut> matched) =>
        (_, record, row) => matched(record, row);

    private static Func<TKey, T, TOut>? WithoutKey<TKey, TOut>(Func<T, TOut>? missing) =>
        missing is null ? null : (_, record) => missing(record);

    /// <summary>
    /// <see cref="Lookup{TKey,TRow,TOut}(TableView{TKey,TRow}, Func{T,TKey}, Func{T,TRow,TOut}, Func{T,TOut}?)"/>
    /// for a stream that only ever inserts: no state, no partitioning, one probe per
    /// record. Nothing is remembered, so nothing can be retracted — a record arriving with
    /// a negative weight faults the circuit rather than emit a retraction that would not
    /// cancel what was emitted before. Events, ticks and log lines are append-only; an
    /// upserted or keyed input is not.
    /// </summary>
    public ZStream<TOut> LookupAppendOnly<TKey, TRow, TOut>(
        TableView<TKey, TRow> table,
        Func<T, TKey> keySelector,
        Func<T, TRow, TOut> matched,
        Func<T, TOut>? missing = null)
        where TKey : notnull
        where TRow : class
        where TOut : notnull
    {
        ArgumentNullException.ThrowIfNull(table);
        ArgumentNullException.ThrowIfNull(keySelector);
        ArgumentNullException.ThrowIfNull(matched);
        table.RequireUsableFrom(Builder, "LookupAppendOnly");
        var probe = table.GroupProbe;
        var tableName = table.TableName;
        return new ZStream<TOut>(
            Builder,
            Builder.SetProbes(
                Builder.AddNode(
                    "tableLookupAppendOnly",
                    [NodeId],
                    ctx => new TableLookupAppendOnlyOperator<T, TKey, TRow, TOut>(probe, tableName, keySelector, matched, missing, ctx.Errors)),
                table.TableNodeId));
    }

    /// <summary>
    /// Declares which field carries event time and how much disorder to tolerate. The
    /// result carries the timestamp with it in the type: windows only exist on groupings of
    /// an <see cref="EventTimeStream{T}"/>, and reshaping the record (<see cref="Select"/>)
    /// drops back to a plain stream — re-declare event time on the new shape. The circuit's
    /// watermark advances to (max event time seen − <paramref name="lateness"/>), taking the
    /// minimum across all declared sources, and never regresses; records older than the
    /// watermark are dropped here — the trade-off for being able to evict closed-window
    /// state downstream. An idle event-time source stalls the watermark (and therefore
    /// window closes and eviction) until it advances.
    /// <para>
    /// Declare it on the input itself where the input is read from a source with
    /// partitions: only there is the watermark kept per partition (the least over the
    /// partitions the source names, <see cref="InputHandle{T}.ExpectPartitionAsync"/>), so
    /// that a partition behind the others — a backlog, a broker that was away — does not
    /// have its records dropped as late; and only there does
    /// <see cref="CircuitOptions.MaxEventTimeAhead"/> apply. A declaration after a
    /// <c>Select</c> or a lookup sees records, not where they came from: declare on the
    /// input and re-declare (<see cref="WithEventTime(Func{T, DateTimeOffset})"/>) on the
    /// new shape.
    /// </para>
    /// </summary>
    public EventTimeStream<T> WithEventTime(Func<T, DateTimeOffset> eventTime, TimeSpan lateness)
    {
        ArgumentNullException.ThrowIfNull(eventTime);
        ArgumentOutOfRangeException.ThrowIfLessThan(lateness, TimeSpan.Zero, nameof(lateness));
        Builder.RequireOutsideRecursion("WithEventTime");
        var clock = Builder.GetOrCreateWatermarkClock();
        var tracker = clock.RegisterTracker(lateness.Ticks);
        // Declared on an input itself, the input feeds the clock as it drains — it knows
        // each record's partition — and the operator only filters.
        var observes = InputNameOrNull is { } input && Builder.BindInputEventTime(input, clock, tracker, eventTime) ? -1 : tracker;
        return new EventTimeStream<T>(
            Builder,
            Builder.AddNode("eventTime", [NodeId], ctx => new WatermarkOperator<T>(ctx, clock, observes, eventTime)),
            eventTime);
    }

    /// <summary>The input this stream is, for a stream that is one; null for every stream
    /// derived from anything.</summary>
    private protected virtual string? InputNameOrNull => null;

    /// <summary>
    /// Re-declares event time on a stream derived from one that already declared it —
    /// after a <see cref="Select"/>, a join or a lookup has reshaped the record — so that
    /// it can be grouped as an <see cref="EventTimeStream{T}"/> again (to window it, or for
    /// <c>EvictWhenFinal</c>). Records behind the watermark are dropped here as at the
    /// declaration, but the clock gains no source: the watermark stays what the upstream
    /// declarations make it. That matters after a filter, which would otherwise hold the
    /// watermark back to the newest record it lets through. Use the overload with a
    /// lateness for a feed of its own, whose time nothing else tracks.
    /// </summary>
    public EventTimeStream<T> WithEventTime(Func<T, DateTimeOffset> eventTime)
    {
        ArgumentNullException.ThrowIfNull(eventTime);
        Builder.RequireOutsideRecursion("WithEventTime");
        var clock = Builder.WatermarkClock
            ?? throw new InvalidOperationException(
                "WithEventTime without a lateness re-declares event time that a stream upstream declared, and none has: " +
                "call WithEventTime(eventTime, lateness) on the input first.");
        return new EventTimeStream<T>(
            Builder,
            Builder.AddNode("eventTime", [NodeId], ctx => new WatermarkOperator<T>(ctx, clock, tracker: -1, eventTime)),
            eventTime);
    }

    /// <summary>
    /// Marks this stream as a circuit output; redeem the returned definition with
    /// <see cref="Circuit.Handle{T}(Output{T})"/> after <c>Build()</c> for the live handle.
    /// A plain stream output is a pure changefeed: it has no meaningful "current state" to
    /// resync from, so across a crash its delivery is at-most-once — exactly-once is the
    /// downstream consumer's offset/idempotency concern. (Keyed and windowed results
    /// bootstrap automatically; see their <c>ToOutput</c> overloads.)
    /// </summary>
    public Output<T> ToOutput() =>
        new(Builder, Builder.AddOutput<T>(NodeId, snapshot: false));

    /// <summary>
    /// Marks this stream as an acknowledged circuit output — the at-least-once counterpart
    /// of <see cref="ToOutput"/>; redeem the returned definition with
    /// <see cref="Circuit.Handle{T}(AcknowledgedOutput{T})"/>. Each batch carries the
    /// sequence number of its step and is confirmed by the consumer once delivered, and a
    /// checkpoint is committed only after every batch published up to it has been
    /// confirmed. A crash therefore never loses output: the restarted circuit re-runs the
    /// steps since the checkpoint and delivers them again. See
    /// <see cref="AcknowledgedOutputHandle{T}"/> for what that asks of the consumer. Works
    /// on any stream, keyed and windowed results included; it keeps no snapshot state and
    /// has no bootstrap batch. Not available on clustered circuits.
    /// </summary>
    public AcknowledgedOutput<T> ToAcknowledgedOutput() =>
        new(Builder, Builder.AddAcknowledgedOutput<T>(NodeId));

    private protected void RequireSameBuilder<TOther>(ZStream<TOther> other) where TOther : notnull
    {
        if (!ReferenceEquals(Builder, other.Builder))
            throw new ArgumentException("Streams from different circuit builders cannot be combined.", nameof(other));
    }
}

/// <summary>
/// A stream of key/value rows partitioned by key across workers — the result shape of
/// grouped aggregation (and of <c>AntiJoin</c>). Regroup it (<c>GroupBy</c>), continue on
/// the same key without a reshuffle (<see cref="Grouped"/>), or output it — as a
/// table-shaped result its output bootstraps consumers automatically.
/// </summary>
public sealed class KeyedZStream<TKey, TValue> : ZStream<KeyedValue<TKey, TValue>>
    where TKey : notnull
    where TValue : notnull
{
    internal KeyedZStream(
        CircuitBuilder builder, int nodeId, KeyEviction<TKey>? eviction = null, Func<TKey, uint>? routingHash = null)
        : base(builder, nodeId)
    {
        Eviction = eviction;
        RoutingHash = routingHash;
    }

    internal KeyEviction<TKey>? Eviction { get; }

    /// <summary>See <see cref="GroupedZStream{TKey,TValue}.RoutingHash"/>.</summary>
    internal Func<TKey, uint>? RoutingHash { get; }

    /// <summary>Names the operator this stream carries, keeping the result keyed; see
    /// <see cref="ZStream{T}.Named"/>.</summary>
    public new KeyedZStream<TKey, TValue> Named(string name)
    {
        Builder.SetStateId(NodeId, name);
        return this;
    }

    /// <summary>Sets the error policy of the operator this stream carries, keeping the
    /// result keyed; see <see cref="ZStream{T}.OnError"/>.</summary>
    public new KeyedZStream<TKey, TValue> OnError(RecordErrorPolicy policy)
    {
        Builder.SetErrorPolicy(NodeId, policy);
        return this;
    }

    /// <summary>Transforms values while preserving the key and the partitioning.</summary>
    public KeyedZStream<TKey, TOut> MapValues<TOut>(Func<TValue, TOut> selector) where TOut : notnull =>
        new(Builder, Builder.AddNode(
            "mapValues",
            [NodeId],
            ctx => new MapOperator<KeyedValue<TKey, TValue>, KeyedValue<TKey, TOut>>(
                KeySelectors.MapValues<TKey, TValue, TOut>(selector, guarded: ctx.Errors is not null), ctx.Errors)),
            Eviction,
            RoutingHash);

    /// <summary>Drops the keys. The result is no longer partitioned by them.</summary>
    public ZStream<TValue> Values() => Select(static kv => kv.Value);

    /// <summary>Continues stateful work on the same key without a reshuffle: the stream is
    /// already partitioned by <typeparamref name="TKey"/>, so the returned grouping reuses
    /// that partitioning (and inherits this stream's eviction policy) — join it or
    /// aggregate it again directly.</summary>
    public GroupedZStream<TKey, TValue> Grouped() => new(Builder, NodeId, Eviction, RoutingHash);

    /// <summary>
    /// Marks this keyed result as a circuit output; redeem the returned definition with
    /// <see cref="Circuit.Handle{T}(Output{T})"/> after <c>Build()</c>. Table-shaped
    /// outputs resync consumers automatically: with <c>bootstrap</c> (the
    /// default) the output maintains checkpoint-backed snapshot state, and the handle's
    /// <c>Changes()</c> begins with one batch asserting the current integrated state —
    /// correct across restarts and mid-run subscription. Pass <c>false</c> to opt out when
    /// the consumer is stateless and the O(live rows) state cost isn't wanted.
    /// </summary>
    public new Output<KeyedValue<TKey, TValue>> ToOutput() => ToOutput(bootstrap: true);

    /// <inheritdoc cref="ToOutput()" path="/summary"/>
    public Output<KeyedValue<TKey, TValue>> ToOutput(bool bootstrap)
    {
        var nodeId = NodeId;
        if (bootstrap)
        {
            Builder.RequireOutsideRecursion("ToOutput");
            nodeId = Builder.AddNode("snapshot", [nodeId], ctx => new SnapshotOperator<KeyedValue<TKey, TValue>>(ctx));
        }
        return new Output<KeyedValue<TKey, TValue>>(Builder, Builder.AddOutput<KeyedValue<TKey, TValue>>(nodeId, bootstrap));
    }
}
