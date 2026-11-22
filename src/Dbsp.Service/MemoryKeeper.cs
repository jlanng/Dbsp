using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging;

namespace Dbsp.Service;

/// <summary>What a node last did to give memory back (<see cref="ServiceOptions.ReleaseUnusedMemoryEvery"/>).
/// <c>NativeReturnedBytes</c>: what the C allocator returned to the operating system that
/// time, by the process's anonymous resident memory before and after; the totals count
/// from the process's start. <c>CollectionsInduced</c>: young-generation collections the
/// node asked for that time, so that the collector would let go of what a burst had left
/// it holding.</summary>
public sealed record MemoryRelease(
    DateTimeOffset At, long NativeReturnedBytes, int CollectionsInduced, long NativeReturnedBytesTotal, long CollectionsInducedTotal);

/// <summary>
/// Gives back what the process holds and does not use. Two things hold memory a node has
/// finished with (docs/enrichment-service.md, <i>Sizing a node</i>; both measured with the
/// scale rig), and neither lets go by itself:
/// <list type="bullet">
/// <item><b>The C allocator.</b> The Kafka client allocates from glibc's <c>malloc</c>,
/// which keeps freed memory in per-thread arenas: after a table load or a state restore
/// a node held 2 GB there with 35 MB of it in use. <c>malloc_trim</c> returns it; it
/// takes about 0.1 s for 2 GB and next to nothing when there is nothing to return.</item>
/// <item><b>The collector.</b> After a burst of allocation — a restore, a bulk load, a
/// replay — the collector keeps the regions the burst needed and releases one only after
/// it has gone unused through some twenty collections. A node that has gone quiet (a
/// standby that has become warm, a leader whose inputs are idle) collects a few times an
/// hour, so it would hold them for hours. While the collector has committed well over
/// what was live at its last collection and is collecting seldom, the keeper asks for
/// young-generation collections — a few tens of milliseconds each on a heap of
/// gigabytes — until it has let go, and stops asking when asking does not help.</item>
/// </list>
/// It never asks for a full blocking collection: on a leader that is a pause of seconds.
/// </summary>
internal sealed class MemoryKeeper
{
    /// <summary>The process's one keeper: the memory is the process's, however many
    /// nodes share it (two, on the in-memory backend).</summary>
    public static MemoryKeeper Shared { get; } = new();

    private int _running;
    private long _lastRunTicks;

    /// <summary>Runs unless a run is under way or one ended less than
    /// <paramref name="every"/> ago. Never throws but for cancellation.</summary>
    public async Task RunIfDueAsync(TimeSpan every, ILogger logger, string node, CancellationToken stop)
    {
        if (every <= TimeSpan.Zero || Environment.TickCount64 - Interlocked.Read(ref _lastRunTicks) < every.TotalMilliseconds
            || Interlocked.Exchange(ref _running, 1) == 1)
            return;
        try
        {
            await RunAsync(logger, node, stop).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogDebug(ex, "Service node {Node}: giving back unused memory failed", node);
        }
        finally
        {
            Interlocked.Exchange(ref _lastRunTicks, Environment.TickCount64);
            Volatile.Write(ref _running, 0);
        }
    }

    // Not worth a system call's attention below this.
    private const long NativeWorthReturning = 64L << 20;

    private static readonly TimeSpan BetweenCollections = TimeSpan.FromMilliseconds(250);

    private bool _nativeUnavailable;
    private bool _infoUnavailable;
    private int _collectionsSeen = GC.CollectionCount(0);
    private readonly CollectorAgeing _ageing = new();
    private long _nativeTotal;
    private long _inducedTotal;

    /// <summary>What the last run did; null before the first.</summary>
    public MemoryRelease? Last { get; private set; }

    internal async Task RunAsync(ILogger logger, string node, CancellationToken stop)
    {
        var native = ReturnNative();
        var induced = await AgeCollectorAsync(stop).ConfigureAwait(false);
        _nativeTotal += native;
        _inducedTotal += induced;
        Last = new MemoryRelease(DateTimeOffset.UtcNow, native, induced, _nativeTotal, _inducedTotal);
        if (native > 0 || induced > 0)
            logger.LogDebug(
                "Service node {Node}: returned {Native} MB of the C allocator's free memory to the operating system and asked for {Induced} young collections",
                node, native >> 20, induced);
    }

    /// <summary>Has glibc return its free memory; the bytes the process's anonymous
    /// resident memory fell by. Nothing on a platform whose allocator is another.</summary>
    private long ReturnNative()
    {
        if (_nativeUnavailable || !OperatingSystem.IsLinux())
            return 0;
        try
        {
            if (!_infoUnavailable)
            {
                try
                {
                    if ((long)Glibc.mallinfo2().fordblks < NativeWorthReturning)
                        return 0;
                }
                catch (EntryPointNotFoundException)
                {
                    // glibc before 2.33 cannot say how much is free: trim regardless.
                    _infoUnavailable = true;
                }
            }
            var before = ProcessMemory.AnonymousResidentBytes() ?? 0;
            Glibc.malloc_trim(0);
            return Math.Max(0, before - (ProcessMemory.AnonymousResidentBytes() ?? before));
        }
        catch (Exception ex) when (ex is DllNotFoundException or EntryPointNotFoundException)
        {
            // Another C library (musl has no malloc_trim): nothing to do here, ever.
            _nativeUnavailable = true;
            return 0;
        }
    }

    private async Task<int> AgeCollectorAsync(CancellationToken stop)
    {
        var info = GC.GetGCMemoryInfo();
        var collections = GC.CollectionCount(0);
        var since = collections - _collectionsSeen;
        _collectionsSeen = collections;

        var ask = _ageing.Decide(info.TotalCommittedBytes, info.HeapSizeBytes, since, DateTimeOffset.UtcNow);
        var asked = 0;
        for (; asked < ask && !stop.IsCancellationRequested; asked++)
        {
            GC.Collect(0, GCCollectionMode.Forced, blocking: true);
            try
            {
                await Task.Delay(BetweenCollections, stop).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
        _collectionsSeen = GC.CollectionCount(0);
        return asked;
    }

    /// <summary>
    /// When to ask the collector for collections, and when to stop asking: the decision
    /// alone, given what the collector reports, so that it can be read and tested without
    /// a heap. It asks while the collector has committed well over what was live at its
    /// last collection and the process has collected seldom since the last look; and if
    /// asking did not bring the committed figure down by a tenth of that excess, what the
    /// collector holds is what it means to keep — so it rests, a minute, then two, up to
    /// ten, and starts afresh once the excess is gone.
    /// </summary>
    internal sealed class CollectorAgeing
    {
        // The collector is left alone unless it has committed this much more than was
        // live at its last collection, and at least this share of it.
        public const long SlackWorthReleasing = 256L << 20;
        public const double SlackShare = 0.25;

        // A region is released once it has gone unused through twenty collections; a few
        // more, because the count starts at the collection after the one that emptied it.
        public const int CollectionsToRelease = 25;
        public static readonly TimeSpan FirstRest = TimeSpan.FromMinutes(1);
        public static readonly TimeSpan LongestRest = TimeSpan.FromMinutes(10);

        private long _committedWhenAsked = -1;
        private TimeSpan _rest;
        private DateTimeOffset _restUntil;

        /// <summary>How many young-generation collections to ask for now: none, or
        /// <see cref="CollectionsToRelease"/>.</summary>
        public int Decide(long committedBytes, long heapBytes, int collectionsSince, DateTimeOffset now)
        {
            var slack = committedBytes - heapBytes;
            if (slack < Math.Max(SlackWorthReleasing, (long)(heapBytes * SlackShare)))
            {
                // Nothing to let go of: whatever was asked for worked, or was never needed.
                (_rest, _restUntil, _committedWhenAsked) = (TimeSpan.Zero, default, -1);
                return 0;
            }

            // Asked last time and the collector holds as much as it did: ask again later,
            // and later still each time.
            if (_committedWhenAsked >= 0 && _committedWhenAsked - committedBytes < slack / 10)
            {
                _rest = _rest == TimeSpan.Zero ? FirstRest : _rest * 2 > LongestRest ? LongestRest : _rest * 2;
                _restUntil = now + _rest;
            }
            _committedWhenAsked = -1;

            // Resting, or the process collects often enough by itself.
            if (now < _restUntil || collectionsSince >= CollectionsToRelease)
                return 0;
            _committedWhenAsked = committedBytes;
            return CollectionsToRelease;
        }
    }

    private static class Glibc
    {
        [StructLayout(LayoutKind.Sequential)]
        public struct Mallinfo2
        {
            public nuint arena, ordblks, smblks, hblks, hblkhd, usmblks, fsmblks, uordblks, fordblks, keepcost;
        }

        [DllImport("libc.so.6")]
        public static extern int malloc_trim(nuint pad);

        [DllImport("libc.so.6")]
        public static extern Mallinfo2 mallinfo2();
    }
}
