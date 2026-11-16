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
/// stop a fraction of a second.
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
    /// ends when the writer has gone <see cref="QuietGap"/> without a commit.</summary>
    public int LargeAfterChanges { get; init; } = 100_000;

    /// <summary>A pause between commits that ends a run.</summary>
    public TimeSpan QuietGap { get; init; } = TimeSpan.FromSeconds(2);

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
/// concurrently.</summary>
internal sealed class LoadPacer(TableLoadPacing pacing)
{
    private long _lastCommit;
    private long _runChanges;
    private long _allocatedAtStep;
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
            _runChanges = 0;
        _lastCommit = now;
        _runChanges += changes;

        var allocated = GC.GetTotalAllocatedBytes(precise: false);
        if (_runChanges < pacing.LargeAfterChanges)
        {
            // Not a large load (yet): the first step is a step after it becomes one.
            _allocatedAtStep = allocated;
            return;
        }
        if (allocated - _allocatedAtStep < pacing.StepBytes)
            return;
        _allocatedAtStep = allocated;

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
