namespace Incremental.Storage;

/// <summary>
/// The levels of the persisted formats. A level names everything a build writes that
/// another build must be able to read: the state log's record values and commit records,
/// the state manifest and the checkpoint's marker keys, and the table logs' records. A
/// build reads every level up to its own and writes any of them; which one it writes is
/// decided for the deployment, not by the build (<see cref="PersistedFormat"/>).
/// <para>
/// A change to any persisted format is a new level: add a constant, raise
/// <see cref="Current"/>, write the new format only where
/// <see cref="PersistedFormat.Writes"/> has reached the level, and keep the reader of
/// everything below it. Levels are never renumbered or removed.
/// </para>
/// </summary>
public static class FormatLevel
{
    /// <summary>What every build since layouts (round two) writes: state-log values of
    /// format bytes 1 and 2, table-log records of format byte 1, state manifest format 7.
    /// A build that says nothing of levels is at this one.</summary>
    public const int Base = 1;

    /// <summary>State manifest format 8: an input's partitions have event times of their
    /// own in the checkpoint's marker (<c>$watermark#s{n}#m{p}</c>, <c>#i{p}</c>).</summary>
    public const int PartitionTimes = 2;

    /// <summary>The newest level this build reads and can write.</summary>
    public const int Current = PartitionTimes;

    /// <summary>
    /// A level no released build writes: under it every state-log value and every
    /// table-log record is the level below's with <see cref="EnvelopeByte"/> in front.
    /// It exists so that the path a real change of a format byte will take — a node that
    /// cannot read it, a leader that holds it back, the raise, the takeover — is run by
    /// tests on every build, on formats a node below the level does refuse. Only a test
    /// can ask for it (<see cref="PersistedFormat.ForTesting"/>).
    /// </summary>
    internal const int Proving = 1_000_000;

    /// <summary>The format byte of the proving level, in the state log and the table
    /// logs. Reserved: no real format takes it.</summary>
    internal const byte EnvelopeByte = 0x7E;

    /// <summary>The state manifest's format at a level.</summary>
    internal static int ManifestFormat(int level) => level >= PartitionTimes ? 8 : 7;
}

/// <summary>
/// What one node reads and writes of the persisted formats. One instance belongs to a node
/// and is shared by everything of it that reads or writes a log.
/// <para>
/// <see cref="Reads"/> is fixed: the build's level, or a lower one the node was pinned to,
/// in which case it behaves as a build of that level in every respect — it refuses what
/// such a build would refuse. <see cref="Writes"/> is the deployment's: it starts at
/// <see cref="FormatLevel.Base"/>, is set to what the logs are written at when the node
/// becomes the writer, and is raised by the leader, never lowered, once every node of the
/// deployment reads the higher level. A raise is made durable (in the leader topic on
/// Kafka) before anything is written under it, so a node never meets a format it was not
/// told of first.
/// </para>
/// <para>
/// Used without a deployment — a circuit on a storage provider of its own —
/// <see cref="Latest"/> writes the build's level from the start.
/// </para>
/// </summary>
public sealed class PersistedFormat
{
    private int _writes;

    /// <summary>Reads and writes <see cref="FormatLevel.Current"/>, and nothing decides
    /// otherwise: what a storage or a log uses when it is given no other.</summary>
    public static PersistedFormat Latest { get; } = new(FormatLevel.Current, FormatLevel.Current, check: false);

    /// <summary>A node's formats: it reads up to <paramref name="reads"/> (the build's
    /// level when null) and writes <see cref="FormatLevel.Base"/> until told what the
    /// deployment's logs are written at.</summary>
    public PersistedFormat(int? reads = null)
        : this(reads ?? FormatLevel.Current, FormatLevel.Base, check: true)
    {
    }

    private PersistedFormat(int reads, int writes, bool check)
    {
        if (check && reads is < FormatLevel.Base or > FormatLevel.Current)
            throw new ArgumentOutOfRangeException(
                nameof(reads), reads,
                $"A format level is between {FormatLevel.Base} and {FormatLevel.Current}, the level of this build.");
        Reads = reads;
        _writes = writes;
    }

    /// <summary>A node of the proving level, or one pinned below it, for tests.</summary>
    internal static PersistedFormat ForTesting(int reads, int? writes = null) => new(reads, writes ?? FormatLevel.Base, check: false);

    /// <summary>The highest level this node reads.</summary>
    public int Reads { get; }

    /// <summary>The level this node writes now. Read it once per transaction: everything
    /// of one commit is of one level.</summary>
    public int Writes => Volatile.Read(ref _writes);

    /// <summary>Raises <see cref="Writes"/> to <paramref name="level"/>; a level at or
    /// below it changes nothing. Called by whoever has made the raise durable.</summary>
    /// <exception cref="FormatLevelException">The level is above <see cref="Reads"/>.</exception>
    public void Raise(int level)
    {
        if (level > Reads)
            throw new FormatLevelException(level, Reads);
        int seen;
        while ((seen = Volatile.Read(ref _writes)) < level)
            Interlocked.CompareExchange(ref _writes, level, seen);
    }

    /// <summary>The highest level this node has met in a log it read; 0 before any. A
    /// node that becomes the writer never writes below it.</summary>
    public int Seen => Volatile.Read(ref _seen);

    private int _seen;

    /// <summary>Records that a log this node reads holds something of
    /// <paramref name="level"/>; throws if the node does not read it.</summary>
    public void NoteSeen(int level)
    {
        RequireReads(level);
        int seen;
        while ((seen = Volatile.Read(ref _seen)) < level)
            Interlocked.CompareExchange(ref _seen, level, seen);
    }

    /// <summary>Throws unless this node reads <paramref name="level"/>.</summary>
    public void RequireReads(int level)
    {
        if (level > Reads)
            throw new FormatLevelException(level, Reads);
    }

    /// <summary>Whether a state-log value or a table-log record is of the proving level,
    /// which a node reads only at that level; throws if this node does not.</summary>
    internal void RequireReadable(ReadOnlySpan<byte> value)
    {
        if (value.Length > 0 && value[0] == FormatLevel.EnvelopeByte && Seen < FormatLevel.Proving)
            NoteSeen(FormatLevel.Proving);
    }

    /// <summary>A value without the proving level's envelope, if it has one.</summary>
    internal static ReadOnlySpan<byte> Unwrapped(ReadOnlySpan<byte> value) =>
        value.Length > 0 && value[0] == FormatLevel.EnvelopeByte ? value[1..] : value;

    /// <inheritdoc cref="Unwrapped(ReadOnlySpan{byte})"/>
    internal static ReadOnlyMemory<byte> Unwrapped(ReadOnlyMemory<byte> value) =>
        value.Length > 0 && value.Span[0] == FormatLevel.EnvelopeByte ? value[1..] : value;

    /// <summary>Whether this node writes the proving level's envelope now.</summary>
    internal bool WritesEnvelope => Writes >= FormatLevel.Proving;
}

/// <summary>
/// A log is written at a format level this node does not read: it was built before the
/// level, or pinned below it. Nothing was applied. The node cannot follow or lead until it
/// is replaced by a build of the level; a level, once written, is not taken back.
/// </summary>
public sealed class FormatLevelException(int level, int reads) : InvalidOperationException(
    $"The log is written at format level {level} and this node reads up to level {reads}: it was built before that level " +
    "(or is pinned below it), and cannot follow or lead. Replace it with the build the other node runs. A format level " +
    "that has been written is not taken back.")
{
    public int Level { get; } = level;
    public int Reads { get; } = reads;
}

/// <summary>Implemented by a checkpointing provider that belongs to a node: the formats
/// its circuit writes its checkpoint in.</summary>
internal interface IPersistedFormatSource
{
    PersistedFormat Formats { get; }
}
