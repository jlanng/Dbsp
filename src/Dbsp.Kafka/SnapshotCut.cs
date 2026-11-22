namespace Dbsp.Kafka;

/// <summary>
/// Decides when a read of a table's rows topic, partition by partition, has arrived at one
/// version in all of them — the logic of <c>KafkaTableLog.RestoreAsync</c>, apart from Kafka
/// so that it can be tested against every interleaving.
/// <para>
/// Each partition is read on its own clock, so "to the end of each" could mix versions
/// while the writer is busy. What makes a consistent cut possible is that every version
/// ends, in every partition, with a head record carrying its number, and that a version's
/// records in a partition are contiguous. The rule: a partition is first read to the end it
/// had when the restore began. From then on it is paused whenever it stands right after a
/// head at the highest version seen in any partition, and resumed when another partition
/// shows a higher one. The cut is done when every partition has passed its end and stands
/// right after the head of the same version.
/// </para>
/// <para>
/// A paused partition may still deliver records the client had already fetched. They are
/// applied like any other — the reader's position has moved past them — and a row among
/// them means the partition is inside a later version and must run on to that version's
/// head.
/// </para>
/// </summary>
internal sealed class SnapshotCut
{
    private readonly long[] _ends;
    private readonly long[] _versions;
    private readonly bool[] _atHead;
    private readonly bool[] _reachedEnd;
    private readonly bool[] _paused;

    /// <param name="ends">Each partition's end of committed data (its last stable offset)
    /// when the restore began.</param>
    public SnapshotCut(long[] ends)
    {
        _ends = ends;
        _versions = new long[ends.Length];
        Array.Fill(_versions, -1);
        _atHead = new bool[ends.Length];
        _reachedEnd = new bool[ends.Length];
        _paused = new bool[ends.Length];
    }

    /// <summary>The highest version any partition's head has shown; -1 before the first.</summary>
    public long Target { get; private set; } = -1;

    /// <summary>Whether <paramref name="partition"/> should be paused now. The caller
    /// applies the difference after every event.</summary>
    public bool ShouldPause(int partition) => _paused[partition];

    /// <summary>Every partition has read past its end and stands right after the head of
    /// <see cref="Target"/>: what has been applied is the table at that version.</summary>
    public bool Done
    {
        get
        {
            if (Target < 0)
                return false;
            for (var p = 0; p < _versions.Length; p++)
            {
                if (!(_reachedEnd[p] && _atHead[p] && _versions[p] == Target))
                    return false;
            }
            return true;
        }
    }

    /// <summary>A row record (or a row's tombstone) was applied.</summary>
    public void Row(int partition, long offset)
    {
        Passed(partition, offset);
        _atHead[partition] = false;
        _paused[partition] = false;
    }

    /// <summary>A head record was read.</summary>
    public void Head(int partition, long offset, long version)
    {
        Passed(partition, offset);
        _versions[partition] = version;
        _atHead[partition] = true;
        if (version > Target)
        {
            Target = version;
            for (var other = 0; other < _paused.Length; other++)
            {
                if (_versions[other] < Target)
                    _paused[other] = false;
            }
        }
        Settle(partition);
    }

    /// <summary>The client reported the end of the partition.</summary>
    public void End(int partition)
    {
        _reachedEnd[partition] = true;
        Settle(partition);
    }

    /// <summary>"Has read everything committed below the end." The offset just below a last
    /// stable offset is always a transaction marker (every write here is transactional), so
    /// the last record that can be seen is two below it. When that one belongs to an
    /// aborted transaction it is never seen, and a later record or the end-of-partition
    /// event settles it instead. The event alone would not do: a busy writer keeps every
    /// fetch non-empty, and the client reports the end only on an empty one.</summary>
    private void Passed(int partition, long offset) => _reachedEnd[partition] |= offset >= _ends[partition] - 2;

    private void Settle(int partition)
    {
        if (_reachedEnd[partition] && _atHead[partition] && _versions[partition] >= Target)
            _paused[partition] = true;
    }
}
