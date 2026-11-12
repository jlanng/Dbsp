using System.Globalization;
using System.Runtime;

namespace Incremental.Service;

/// <summary>The collector's settings as the runtime reports them, which is what decides
/// how far a node's working set stands above what it holds (the guide, <i>Sizing a
/// node</i>). <c>Heaps</c> is the configured heap count; 0 means the runtime chose (one
/// per core under server GC, one under workstation). <c>ConserveMemory</c> is 0 to 9;
/// <c>HeapHardLimitBytes</c> 0 when there is no limit.</summary>
public sealed record GcSettingsInfo(bool Server, bool Concurrent, int Heaps, int ConserveMemory, long HeapHardLimitBytes, bool Dynamic)
{
    public static GcSettingsInfo Current { get; } = ReadCurrent();

    private static GcSettingsInfo ReadCurrent()
    {
        var variables = GC.GetConfigurationVariables();
        long Number(string name) => variables.TryGetValue(name, out var value) ? Convert.ToInt64(value, CultureInfo.InvariantCulture) : 0;
        bool Flag(string name) => variables.TryGetValue(name, out var value) && value is bool set && set;
        return new GcSettingsInfo(
            GCSettings.IsServerGC, Flag("ConcurrentGC"), (int)Number("HeapCount"), (int)Number("GCConserveMem"), Number("GCHeapHardLimit"),
            Number("GCDynamicAdaptationMode") == 1);
    }

    /// <summary>One line for a person: <c>server, 4 heaps, concurrent, conserve 5</c>.</summary>
    public override string ToString()
    {
        var parts = new List<string> { Server ? "server" : "workstation" };
        if (Server)
            parts.Add(Dynamic ? "heaps adapt" : Heaps > 0 ? $"{Heaps} heaps" : $"{Environment.ProcessorCount} heaps (one per core)");
        parts.Add(Concurrent ? "concurrent" : "not concurrent");
        if (ConserveMemory > 0)
            parts.Add($"conserve {ConserveMemory}");
        if (HeapHardLimitBytes > 0)
            parts.Add(string.Create(CultureInfo.InvariantCulture, $"limit {HeapHardLimitBytes / 1048576.0:0} MB"));
        return string.Join(", ", parts);
    }
}

/// <summary>What a node's process holds, read from the runtime when asked.
/// <c>WorkingSetBytes</c> is the operating system's figure for the process;
/// <c>GcHeapBytes</c> the managed heap in use as of the last collection plus what was
/// allocated since (<see cref="GC.GetTotalMemory"/>, not forcing a collection);
/// <c>GcCommittedBytes</c> what the collector has committed for it;
/// <c>GcPausePercent</c> the share of the process's life spent paused in collections
/// (<see cref="GCMemoryInfo.PauseTimePercentage"/>); <c>Gen2Collections</c> how many full
/// collections there have been. Two in-memory nodes share a process and report the same.</summary>
/// <remarks>
/// <c>GcLiveBytes</c>: what the heap held at the end of the last collection, less the
/// free space inside it (<see cref="GCMemoryInfo.HeapSizeBytes"/> −
/// <see cref="GCMemoryInfo.FragmentedBytes"/>) — the nearest figure to "what the node
/// holds" that costs nothing to read; it is as old as the last collection and counts what
/// a younger generation's collection did not look at. <c>GcFragmentedBytes</c>: that free
/// space. <c>OtherBytes</c>: the process's anonymous resident memory less what the
/// collector has committed — the Kafka client's buffers, the runtime's own memory, thread
/// stacks and whatever the C allocator has not given back; null where the operating
/// system does not say (it is read from <c>/proc/self/status</c>).
/// <c>GcPauseRecentPercent</c>: the share of the last minute (less, on a process younger
/// than that) spent paused; null until two readings a second apart exist.
/// <c>Gc</c>: the collector's settings.
/// </remarks>
public sealed record ProcessMemory(
    long WorkingSetBytes, long GcHeapBytes, long GcCommittedBytes, double GcPausePercent, int Gen2Collections,
    long GcLiveBytes = 0, long GcFragmentedBytes = 0, long? OtherBytes = null, double? GcPauseRecentPercent = null,
    GcSettingsInfo? Gc = null)
{
    public static ProcessMemory Read()
    {
        var info = GC.GetGCMemoryInfo();
        long? other = AnonymousResidentBytes() is { } anonymous ? Math.Max(0, anonymous - info.TotalCommittedBytes) : null;
        return new ProcessMemory(
            Environment.WorkingSet, GC.GetTotalMemory(false), info.TotalCommittedBytes, info.PauseTimePercentage, GC.CollectionCount(2),
            Math.Max(0, info.HeapSizeBytes - info.FragmentedBytes), info.FragmentedBytes, other, PauseClock.Recent(),
            GcSettingsInfo.Current);
    }

    /// <summary><c>RssAnon</c> of this process, on Linux; null elsewhere.</summary>
    internal static long? AnonymousResidentBytes()
    {
        if (!OperatingSystem.IsLinux())
            return null;
        try
        {
            foreach (var line in File.ReadLines("/proc/self/status"))
            {
                if (!line.StartsWith("RssAnon:", StringComparison.Ordinal))
                    continue;
                var parts = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                return parts.Length >= 2 && long.TryParse(parts[1], NumberStyles.Integer, CultureInfo.InvariantCulture, out var kilobytes)
                    ? kilobytes * 1024
                    : null;
            }
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
        }
        return null;
    }

    /// <summary>One token for the probe line: numbers, comma-separated. The first five are
    /// what a node from before the rest sent; a reader takes what it knows.</summary>
    internal string Encode()
    {
        var culture = CultureInfo.InvariantCulture;
        var text = string.Create(culture, $"{WorkingSetBytes},{GcHeapBytes},{GcCommittedBytes},{GcPausePercent:0.##},{Gen2Collections}")
            + string.Create(culture, $",{GcLiveBytes},{GcFragmentedBytes},{OtherBytes ?? -1},{GcPauseRecentPercent ?? -1:0.##}");
        return Gc is null
            ? text
            : text + string.Create(culture, $",{(Gc.Server ? 1 : 0)},{(Gc.Concurrent ? 1 : 0)},{Gc.Heaps},{Gc.ConserveMemory},{Gc.HeapHardLimitBytes},{(Gc.Dynamic ? 1 : 0)}");
    }

    internal static ProcessMemory? Decode(string text)
    {
        var parts = text.Split(',');
        var culture = CultureInfo.InvariantCulture;
        if (!(parts.Length >= 5
              && long.TryParse(parts[0], NumberStyles.Integer, culture, out var workingSet)
              && long.TryParse(parts[1], NumberStyles.Integer, culture, out var heap)
              && long.TryParse(parts[2], NumberStyles.Integer, culture, out var committed)
              && double.TryParse(parts[3], NumberStyles.Float, culture, out var pause)
              && int.TryParse(parts[4], NumberStyles.Integer, culture, out var gen2)))
            return null;
        var memory = new ProcessMemory(workingSet, heap, committed, pause, gen2);
        if (!(parts.Length >= 9
              && long.TryParse(parts[5], NumberStyles.Integer, culture, out var live)
              && long.TryParse(parts[6], NumberStyles.Integer, culture, out var fragmented)
              && long.TryParse(parts[7], NumberStyles.Integer, culture, out var other)
              && double.TryParse(parts[8], NumberStyles.Float, culture, out var recent)))
            return memory;
        memory = memory with
        {
            GcLiveBytes = live, GcFragmentedBytes = fragmented, OtherBytes = other < 0 ? null : other, GcPauseRecentPercent = recent < 0 ? null : recent,
        };
        if (!(parts.Length >= 15
              && int.TryParse(parts[11], NumberStyles.Integer, culture, out var heaps)
              && int.TryParse(parts[12], NumberStyles.Integer, culture, out var conserve)
              && long.TryParse(parts[13], NumberStyles.Integer, culture, out var limit)))
            return memory;
        return memory with { Gc = new GcSettingsInfo(parts[9] == "1", parts[10] == "1", heaps, conserve, limit, parts[14] == "1") };
    }

    /// <summary>The process's collection pauses over the last minute: a ring of readings
    /// of <see cref="GC.GetTotalPauseDuration"/>, one a second at most, taken as
    /// <see cref="Read"/> is called. A node reads once a second for its gauges, so the
    /// ring is full on any node that has run for a minute.</summary>
    private static class PauseClock
    {
        private const int Slots = 61;
        private static readonly (long At, long PauseTicks)[] Ring = new (long, long)[Slots];
        private static readonly object Lock = new();
        private static int _next, _count;

        public static double? Recent()
        {
            var now = Environment.TickCount64;
            var pause = GC.GetTotalPauseDuration().Ticks;
            lock (Lock)
            {
                var newest = _count == 0 ? default : Ring[(_next + Slots - 1) % Slots];
                if (_count == 0 || now - newest.At >= 1000)
                {
                    Ring[_next] = (now, pause);
                    _next = (_next + 1) % Slots;
                    _count = Math.Min(_count + 1, Slots);
                }

                // The oldest reading still within the minute; with gaps between readings
                // (nobody asked for a while), the newest one older than it.
                (long At, long PauseTicks)? from = null;
                for (var i = 0; i < _count; i++)
                {
                    var reading = Ring[(_next + Slots - _count + i) % Slots];
                    if (now - reading.At > 60_000)
                        from = reading;
                    else
                    {
                        from ??= reading;
                        break;
                    }
                }
                if (from is not { } start || now - start.At < 1000)
                    return null;
                return Math.Round(100.0 * (pause - start.PauseTicks) / TimeSpan.TicksPerMillisecond / (now - start.At), 2);
            }
        }
    }
}
