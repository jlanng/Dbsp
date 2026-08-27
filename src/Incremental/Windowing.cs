namespace Incremental;

/// <summary>A value assigned to one event-time window.</summary>
public readonly record struct Windowed<T>(DateTimeOffset WindowStart, DateTimeOffset WindowEnd, T Value)
    where T : notnull;

/// <summary>
/// Event-time windowing. In the Z-set model window assignment is pure data transformation
/// (a flat-map): a record joins every window covering its event time, and downstream keyed
/// aggregates over (window, ...) keys become windowed aggregates for free. Late or
/// out-of-order events need no special machinery — they simply produce deltas that update
/// the affected window's results, and deleted events retract from their windows.
/// TODO: watermark-driven state eviction so closed windows can be garbage collected.
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
