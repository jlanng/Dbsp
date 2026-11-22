using System.Collections.Concurrent;
using System.Diagnostics.Tracing;

namespace Dbsp.Torture;

/// <summary>
/// A byte-weighted histogram of what the run allocates, by type. The runtime raises
/// <c>GCAllocationTick</c> roughly every 100 KB of allocation and names the type that
/// tripped it, which makes this a sampling allocation profiler that costs nothing to run and
/// needs no external tooling — the one thing a CPU profile of a GC-bound workload cannot tell
/// you is <em>which</em> allocations are feeding the collector.
/// </summary>
internal sealed class AllocationProfiler : EventListener
{
    private const int GCAllocationTick = 10;
    private readonly ConcurrentDictionary<string, long> _bytes = new();
    private bool _enabled;

    /// <summary>Starts counting. Ticks raised before this are ignored, so the seeding and
    /// warmup phases do not pollute the measured window.</summary>
    public void Begin()
    {
        _bytes.Clear();
        _enabled = true;
    }

    public IEnumerable<(string Type, long Bytes)> End()
    {
        _enabled = false;
        return _bytes.Select(kv => (kv.Key, kv.Value)).OrderByDescending(e => e.Value);
    }

    protected override void OnEventSourceCreated(EventSource eventSource)
    {
        if (eventSource.Name == "Microsoft-Windows-DotNETRuntime")
            EnableEvents(eventSource, EventLevel.Verbose, (EventKeywords)0x1 /* GC */);
    }

    protected override void OnEventWritten(EventWrittenEventArgs data)
    {
        if (!_enabled || data.EventId != GCAllocationTick || data.Payload is null)
            return;

        string? type = null;
        long amount = 0;
        for (var i = 0; i < data.PayloadNames!.Count; i++)
        {
            switch (data.PayloadNames[i])
            {
                case "TypeName":
                    type = data.Payload[i] as string;
                    break;
                case "AllocationAmount64":
                    amount = Convert.ToInt64(data.Payload[i]);
                    break;
            }
        }
        if (type is not null && amount > 0)
            _bytes.AddOrUpdate(Shorten(type), amount, (_, existing) => existing + amount);
    }

    /// <summary>Generic type names arrive fully qualified and are unreadable at width; keep
    /// the shape (which generic, over what) and drop the namespaces.</summary>
    private static string Shorten(string type)
    {
        var text = type.Replace("Dbsp.Showcase.", "").Replace("Dbsp.Storage.", "")
                       .Replace("Dbsp.Operators.", "").Replace("Dbsp.", "")
                       .Replace("System.Collections.Generic.", "").Replace("System.", "");
        return text.Length > 110 ? text[..110] + "…" : text;
    }
}
