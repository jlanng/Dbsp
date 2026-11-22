using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Tracing;

namespace Dbsp.Torture;

/// <summary>
/// Count and wall time of every collection in the measured window, bucketed by generation
/// and — for gen2 — by whether it was blocking or background, plus the trigger reason of
/// each gen2. Together with the thread-name CPU split in <see cref="ThreadCpu"/> this
/// answers the question the aggregate GC share cannot: is the collector's time driven by
/// per-step allocation (gen0/1 churn) or by the size of the live set (gen2 marking)?
///
/// Wall time is only meaningful for background collections: a managed listener cannot run
/// while the world is stopped, so for blocking GCs the start event is delivered after the
/// EE has already resumed and the measured span collapses to ~zero. Blocking time is
/// reported from <see cref="GC.GetTotalPauseDuration"/> instead; the buckets here still
/// carry the counts.
/// </summary>
internal sealed class GcProfiler : EventListener
{
    private const int GCStart = 1;
    private const int GCEnd = 2;

    // GCType payload of GCStart: 0 = blocking, 1 = background, 2 = foreground (a blocking
    // gen0/1 that runs while a background gen2 is in flight — churn, so bucketed by depth).
    private readonly ConcurrentDictionary<long, (long Ticks, int Depth, int Type)> _inFlight = new();

    private sealed class Bucket
    {
        public long Count;
        public long Ticks;
    }

    private readonly Bucket[] _blocking = [new(), new(), new()];
    private readonly Bucket _background = new();
    private readonly ConcurrentDictionary<int, long> _gen2Reasons = new();
    private bool _enabled;

    public void Begin()
    {
        foreach (var bucket in (Bucket[])[.. _blocking, _background])
        {
            Interlocked.Exchange(ref bucket.Count, 0);
            Interlocked.Exchange(ref bucket.Ticks, 0);
        }
        _gen2Reasons.Clear();
        _enabled = true;
    }

    private static readonly string[] ReasonNames =
    [
        "AllocSmall", "Induced", "LowMemory", "Empty", "AllocLarge", "OutOfSpaceSOH",
        "OutOfSpaceLOH", "InducedNotForced", "Internal", "InducedLowMemory", "InducedCompacting",
    ];

    /// <summary>Why the gen2 collections of the window were triggered — the datum that says
    /// what to fix. <c>AllocLarge</c> means LOH churn is exhausting the budget.</summary>
    public string Gen2ReasonSummary() => _gen2Reasons.IsEmpty
        ? "none"
        : string.Join("  ", _gen2Reasons.OrderByDescending(kv => kv.Value).Select(kv =>
            $"{(kv.Key < ReasonNames.Length ? ReasonNames[kv.Key] : $"reason{kv.Key}")} x{kv.Value}"));

    public readonly record struct Summary(
        long Gen0, long Gen1, long Gen2Blocking, long Background, double BackgroundSeconds);

    public Summary End()
    {
        _enabled = false;
        return new Summary(
            _blocking[0].Count, _blocking[1].Count, _blocking[2].Count, _background.Count,
            Interlocked.Read(ref _background.Ticks) / (double)Stopwatch.Frequency);
    }

    protected override void OnEventSourceCreated(EventSource eventSource)
    {
        if (eventSource.Name == "Microsoft-Windows-DotNETRuntime")
            EnableEvents(eventSource, EventLevel.Informational, (EventKeywords)0x1 /* GC */);
    }

    protected override void OnEventWritten(EventWrittenEventArgs data)
    {
        if (data.Payload is null || data.EventId is not (GCStart or GCEnd))
            return;

        long number = -1;
        var depth = 0;
        var type = 0;
        var reason = -1;
        for (var i = 0; i < data.PayloadNames!.Count; i++)
        {
            switch (data.PayloadNames[i])
            {
                case "Count":
                    number = Convert.ToInt64(data.Payload[i]);
                    break;
                case "Depth":
                    depth = Convert.ToInt32(data.Payload[i]);
                    break;
                case "Type":
                    type = Convert.ToInt32(data.Payload[i]);
                    break;
                case "Reason":
                    reason = Convert.ToInt32(data.Payload[i]);
                    break;
            }
        }
        if (number < 0)
            return;

        if (data.EventId == GCStart)
        {
            _inFlight[number] = (Stopwatch.GetTimestamp(), depth, type);
            if (_enabled && depth >= 2 && reason >= 0)
                _gen2Reasons.AddOrUpdate(reason, 1, (_, n) => n + 1);
        }
        else if (_inFlight.TryRemove(number, out var start) && _enabled)
        {
            var bucket = start.Type == 1 ? _background : _blocking[Math.Clamp(start.Depth, 0, 2)];
            Interlocked.Increment(ref bucket.Count);
            Interlocked.Add(ref bucket.Ticks, Stopwatch.GetTimestamp() - start.Ticks);
        }
    }
}
