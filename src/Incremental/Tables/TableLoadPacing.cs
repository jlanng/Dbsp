using System.Diagnostics;

namespace Incremental.Tables;

/// <summary>
/// How a table's writer paces a large load (<see cref="SharedTableOptions.LoadPacing"/>).
/// <para>
/// A load keeps nearly everything it writes: the rows. Left to itself the collector lets
/// its young generations fill with them, gigabytes on a server collector, and then copies
/// them into the old generation in one collection that stops the process for as long as
/// the copy takes — measured at 1.4 and 2.4 s while four and five million rows were loaded,
/// next to a standby that takes over after 3. Once a run of commits has written
/// <see cref="LargeAfterChanges"/> changes, the writer asks for a young collection every
/// <see cref="StepBytes"/> the process allocates, so each copy is a step's worth and each
/// stop a fraction of a second. A run is a load, not a table that keeps changing: it ends
/// at a pause and at a pace under <see cref="MinChangesPerSecond"/>, and the tables of a
/// process that load at once share one pace.
/// </para>
/// <para>
/// What it does not do: a process whose memory is close to the collector's high-load
/// threshold gets a full, compacting collection whatever the pace, and one asked for there
/// is the collector's cue to make it then. Above <see cref="MemoryLoadCeiling"/> the writer
/// asks for nothing (<see cref="TableLoadPacingStats.Skipped"/>). That case is the node's
/// size, not the load's speed.
/// </para>
/// </summary>
public sealed class TableLoadPacing
{
    /// <summary>The pacing every table has unless its options say otherwise.</summary>
    public static TableLoadPacing Default { get; } = new();

    /// <summary>Changes committed in one run before the run counts as a large load. A run
    /// ends when the writer has gone <see cref="QuietGap"/> without a commit, and when
    /// its pace over a stretch of that length has been under
    /// <see cref="MinChangesPerSecond"/>.</summary>
    public int LargeAfterChanges { get; init; } = 100_000;

    /// <summary>A pause between commits that ends a run, and the stretch a run's pace is
    /// measured over.</summary>
    public TimeSpan QuietGap { get; init; } = TimeSpan.FromSeconds(2);

    /// <summary>The pace below which commits are not a load: a table that changes
    /// steadily — a CDC table on a busy topic, a pipeline's output — never pauses for
    /// <see cref="QuietGap"/>, and its changes would otherwise add up to a large load
    /// that never ends, with a collection asked for at every <see cref="StepBytes"/>
    /// the whole process allocates, for good. The loads measured ran at 80,000 changes a
    /// second; a load slower than this is left to the collector.</summary>
    public int MinChangesPerSecond { get; init; } = 5_000;

    /// <summary>Bytes the process allocates between two young collections the writer asks
    /// for. Most of a load's allocation survives, so this is about what one collection
    /// copies: on four cores a collection copied about 1 MB a millisecond.</summary>
    public long StepBytes { get; init; } = 128L << 20;

    /// <summary>The share of the collector's high memory-load threshold above which the
    /// writer asks for nothing.</summary>
    public double MemoryLoadCeiling { get; init; } = 0.85;
}

/// <summary>What a writer's pacing has done since the table was opened.</summary>
/// <param name="Steps">Young collections the writer asked for.</param>
/// <param name="Skipped">Steps it did not take because memory was above the ceiling.</param>
/// <param name="Collecting">Time spent in the collections it asked for.</param>
public readonly record struct TableLoadPacingStats(long Steps, long Skipped, TimeSpan Collecting);

/// <summary>One writer's pacing state. Called with the commit gate held, so never
/// concurrently — but there is a writer per table, and what they pace by is the
/// process's allocation: the mark of the last collection asked for is shared, so tables
/// that load at once ask for one collection a step between them, not one each.</summary>
internal sealed class LoadPacer(TableLoadPacing pacing)
{
    // Process-wide: the allocation count at the last young collection any writer asked
    // for (or passed over for memory). A number; nothing of a table's is reachable here.
    private static long s_allocatedAtStep;

    private long _lastCommit;
    private long _runChanges;
    private long _stretchStart;
    private long _stretchChanges;
    private long _allocatedWhenLarge;
    private long _steps;
    private long _skipped;
    private long _collectingTicks;

    public TableLoadPacingStats Stats => new(
        Interlocked.Read(ref _steps), Interlocked.Read(ref _skipped),
        Stopwatch.GetElapsedTime(0, Interlocked.Read(ref _collectingTicks)));

    public void Committed(int changes)
    {
        var now = Stopwatch.GetTimestamp();
        if (_lastCommit == 0 || Stopwatch.GetElapsedTime(_lastCommit, now) > pacing.QuietGap)
        {
            _runChanges = 0;
            _stretchStart = now;
            _stretchChanges = 0;
        }
        else if (Stopwatch.GetElapsedTime(_stretchStart, now) is var stretch && stretch >= pacing.QuietGap)
        {
            // A stretch of the run is over: at a trickle's pace the run is over too.
            // This commit's changes belong to the next stretch.
            if (_stretchChanges < pacing.MinChangesPerSecond * stretch.TotalSeconds)
                _runChanges = 0;
            _stretchStart = now;
            _stretchChanges = 0;
        }
        _lastCommit = now;
        _stretchChanges += changes;
        var wasLarge = _runChanges >= pacing.LargeAfterChanges;
        _runChanges += changes;

        if (_runChanges < pacing.LargeAfterChanges)
            return;
        var allocated = GC.GetTotalAllocatedBytes(precise: false);
        if (!wasLarge)
        {
            // It has just become a large load: the first step is a step after this.
            _allocatedWhenLarge = allocated;
            return;
        }
        var last = Volatile.Read(ref s_allocatedAtStep);
        if (allocated - Math.Max(last, _allocatedWhenLarge) < pacing.StepBytes)
            return;

        // One writer takes the step; another that comes to the same count at the same
        // moment has had it taken for it.
        if (Interlocked.CompareExchange(ref s_allocatedAtStep, allocated, last) != last)
            return;

        var memory = GC.GetGCMemoryInfo();
        if (memory.HighMemoryLoadThresholdBytes > 0 &&
            memory.MemoryLoadBytes >= pacing.MemoryLoadCeiling * memory.HighMemoryLoadThresholdBytes)
        {
            Interlocked.Increment(ref _skipped);
            return;
        }

        var started = Stopwatch.GetTimestamp();
        GC.Collect(1, GCCollectionMode.Forced, blocking: true, compacting: false);
        Interlocked.Add(ref _collectingTicks, Stopwatch.GetTimestamp() - started);
        Interlocked.Increment(ref _steps);
    }
}
