using Incremental.Operators;

namespace Incremental;

/// <summary>A value assigned to one event-time window.</summary>
public readonly record struct Windowed<T>(DateTimeOffset WindowStart, DateTimeOffset WindowEnd, T Value)
    where T : notnull;

/// <summary>
/// Event-time windowing. In the Z-set model window assignment is pure data transformation
/// (a flat-map): a record joins every window covering its event time, and downstream keyed
/// aggregates over (window, ...) keys become windowed aggregates for free. Late or
/// out-of-order events need no special machinery — they simply produce deltas that update
/// the affected window's results, and deleted events retract from their windows. Pair with
/// <c>WithWatermark</c> + <c>WithEviction</c> so closed windows' state is garbage collected.
/// </summary>
public static class WindowExtensions
{
    /// <summary>Assigns each record to the single fixed window of <paramref name="size"/>
    /// (epoch-aligned, UTC) containing its event time.</summary>
    public static ZStream<Windowed<T>> TumblingWindow<T>(
        this ZStream<T> stream,
        TimeSpan size,
        Func<T, DateTimeOffset> eventTime)
        where T : notnull =>
        stream.HoppingWindow(size, size, eventTime);

    /// <summary>Assigns each record to every window of <paramref name="size"/> whose start is a
    /// multiple of <paramref name="hop"/> (epoch-aligned, UTC) and which contains its event time.</summary>
    public static ZStream<Windowed<T>> HoppingWindow<T>(
        this ZStream<T> stream,
        TimeSpan size,
        TimeSpan hop,
        Func<T, DateTimeOffset> eventTime)
        where T : notnull
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(size, TimeSpan.Zero, nameof(size));
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(hop, TimeSpan.Zero, nameof(hop));
        var sizeTicks = size.Ticks;
        var hopTicks = hop.Ticks;
        return stream.FlatMap(value => AssignWindows(value, eventTime(value).UtcTicks, sizeTicks, hopTicks));
    }

    /// <summary>
    /// Groups events into per-key sessions separated by <paramref name="gap"/> of event-time
    /// inactivity: consecutive events of a key belong to the same session while they are
    /// strictly less than the gap apart, and a session spans [first event, last event + gap)
    /// — an event exactly at a session's end starts a new one. Inserts the exchange on
    /// <paramref name="sessionKey"/> (like <c>IndexBy</c>), so the stateful sessionization
    /// runs on the worker owning each key. Unlike tumbling/hopping windows session assignment
    /// is not a pure flat-map: an insertion can extend a session or merge two (a bridging or
    /// late event), and a retraction can shrink or split one — the rows whose session bounds
    /// changed are retracted and re-asserted, recomputing touched keys only. Key the result by
    /// <c>(Key, WindowStart, WindowEnd)</c> to turn any aggregate into a per-session one.
    /// Pair with <c>WithWatermark</c> upstream: a key's event state is then dropped once the
    /// watermark passes its (last event + gap).
    /// </summary>
    public static KeyedZStream<TKey, Windowed<T>> SessionWindow<T, TKey>(
        this ZStream<T> stream,
        TimeSpan gap,
        Func<T, DateTimeOffset> eventTime,
        Func<T, TKey> sessionKey)
        where T : notnull
        where TKey : notnull =>
        stream.IndexBy(sessionKey).SessionWindow(gap, eventTime);

    /// <summary>Session windows over an already key-partitioned stream; see
    /// <see cref="SessionWindow{T,TKey}(ZStream{T},TimeSpan,Func{T,DateTimeOffset},Func{T,TKey})"/>.
    /// The stream's existing partitioning routes each key to one worker, so no extra
    /// exchange is needed.</summary>
    public static KeyedZStream<TKey, Windowed<TValue>> SessionWindow<TKey, TValue>(
        this KeyedZStream<TKey, TValue> stream,
        TimeSpan gap,
        Func<TValue, DateTimeOffset> eventTime)
        where TKey : notnull
        where TValue : notnull
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(gap, TimeSpan.Zero, nameof(gap));
        var builder = stream.Builder;
        var gapTicks = gap.Ticks;
        return new KeyedZStream<TKey, Windowed<TValue>>(
            builder,
            builder.AddNode(
                "sessionWindow",
                [stream.NodeId],
                // The clock is read inside the factory (i.e. at Build), so a watermark
                // declared on any input after this call still drives eviction.
                ctx => new SessionWindowOperator<TKey, TValue>(ctx, gapTicks, eventTime, builder.WatermarkClock)));
    }

    private static IEnumerable<Windowed<T>> AssignWindows<T>(T value, long eventTicks, long sizeTicks, long hopTicks)
        where T : notnull
    {
        var latestStart = eventTicks / hopTicks * hopTicks;
        for (var start = latestStart; start > eventTicks - sizeTicks; start -= hopTicks)
            yield return new Windowed<T>(
                new DateTimeOffset(start, TimeSpan.Zero),
                new DateTimeOffset(start + sizeTicks, TimeSpan.Zero),
                value);
    }
}
