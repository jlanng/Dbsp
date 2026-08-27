using StreamProcessor.Operators;

namespace StreamProcessor;

/// <summary>
/// Watermarks: the circuit's claim that "no more events older than T are expected". In the
/// Z-set model watermarks are not needed for correctness — late events simply produce
/// deltas that correct their windows — so here they serve exactly one purpose: bounding
/// state. <c>WithWatermark</c> drops events that are later than the allowed lateness, and
/// <see cref="KeyedZStream{TKey,TValue}.WithEviction"/> lets downstream aggregations drop
/// per-key state once the watermark passes the key's deadline, which is what keeps an
/// indefinitely running windowed pipeline's memory bounded.
/// </summary>
public static class WatermarkExtensions
{
    /// <summary>
    /// Declares this stream as an event-time source for the circuit's watermark. The
    /// watermark advances to (max event time seen − <paramref name="allowedLateness"/>),
    /// taking the minimum across all declared sources, and never regresses. Records whose
    /// event time is below the watermark are dropped here — the trade-off for being able to
    /// evict closed-key state downstream. Results already emitted for such keys simply
    /// remain final.
    /// </summary>
    public static ZStream<T> WithWatermark<T>(
        this ZStream<T> stream,
        Func<T, DateTimeOffset> eventTime,
        TimeSpan allowedLateness)
        where T : notnull
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(allowedLateness, TimeSpan.Zero, nameof(allowedLateness));
        var clock = stream.Builder.GetOrCreateWatermarkClock();
        var tracker = clock.RegisterTracker(allowedLateness.Ticks);
        return new ZStream<T>(
            stream.Builder,
            stream.Builder.AddNode(
                "watermark",
                [stream.NodeId],
                ctx => new WatermarkOperator<T>(ctx, clock, tracker, eventTime)));
    }
}
