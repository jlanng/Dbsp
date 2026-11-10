namespace Incremental.Storage;

/// <summary>
/// The log behind <see cref="ChangelogStorage"/>: the seam between checkpointed operator
/// state and whatever makes it durable and replicated (see
/// <c>docs/design/durability-and-standby.md</c>, <em>Changelog-backed checkpoint
/// provider</em>). One changelog serves every circuit in the process.
/// <para>
/// It is shaped after one compacted Kafka topic with a fixed number of partitions, written
/// through one transactional producer:
/// </para>
/// <list type="bullet">
/// <item>Records are opaque key and value bytes, appended to a partition. For one key, a
/// later record supersedes an earlier one and a tombstone removes it; the log may drop
/// superseded records at any time (compaction), so offsets are not dense.</item>
/// <item>Writes happen only inside a transaction: every record of a transaction becomes
/// readable at once, or none does.</item>
/// <item>There is one writer at a time. Acquiring the writer fences every earlier one: none
/// of their transactions can commit afterwards.</item>
/// <item>Readers see committed records only, in offset order per partition.</item>
/// </list>
/// </summary>
public interface IStateChangelog
{
    /// <summary>Fixed for the life of the log: a key's partition is derived from it.</summary>
    int PartitionCount { get; }

    /// <summary>
    /// Makes the caller the log's one writer, under a term higher than any before it. When
    /// this returns, the outcome of every earlier writer's transactions is decided — each
    /// is committed in full or will never be — and any commit they attempt from here on
    /// throws <see cref="ChangelogFencedException"/>. (Kafka: <c>InitTransactions</c> on the
    /// shared <c>transactional.id</c>.) A process calls this once, when it becomes active.
    /// </summary>
    ValueTask<IStateChangelogWriter> AcquireWriterAsync(CancellationToken cancellationToken = default);

    /// <summary>The offset one past the last committed record of
    /// <paramref name="partition"/> at the time of the call. A reader whose
    /// <see cref="ChangelogBatch.NextOffset"/> has reached it has seen everything committed
    /// before the call. Asked after <see cref="AcquireWriterAsync"/>, that is everything the
    /// previous writer will ever have committed.</summary>
    ValueTask<long> EndOffsetAsync(int partition, CancellationToken cancellationToken = default);

    /// <summary>
    /// The committed records of <paramref name="partition"/> at or after
    /// <paramref name="fromOffset"/>, in offset order, in batches. It does not end at the
    /// end of the log: it waits for more, which is how a standby follows. It ends only by
    /// cancellation. Every transaction's records are delivered whole in the sense that
    /// matters — none is delivered unless all are committed — but a batch boundary may fall
    /// inside one. Whenever the reader reaches the end of committed data it must have
    /// yielded a batch whose <see cref="ChangelogBatch.NextOffset"/> is at least that end,
    /// even if that batch has no records (the tail of the partition may be offsets with no
    /// record behind them).
    /// </summary>
    IAsyncEnumerable<ChangelogBatch> ReadAsync(int partition, long fromOffset, CancellationToken cancellationToken = default);
}

/// <summary>
/// The changelog's writer for one term. Thread-safe: many circuits commit through it
/// concurrently, each with its own transaction.
/// </summary>
public interface IStateChangelogWriter : IAsyncDisposable
{
    /// <summary>The leader term this writer holds. Stamped on every checkpoint's commit
    /// record.</summary>
    long Term { get; }

    /// <summary>
    /// Starts a transaction: a unit of atomicity. An implementation is free to carry
    /// several of them in one physical transaction (the Kafka log writer's group commit
    /// does), as long as each still lands whole or not at all. Dispose it when done;
    /// disposing without committing aborts it.
    /// </summary>
    IStateChangelogTransaction BeginTransaction();
}

/// <summary>One atomic set of appends. Not thread-safe; used by one committer.</summary>
public interface IStateChangelogTransaction : IDisposable
{
    /// <summary>Adds a record. The bytes are copied before this returns.</summary>
    void Append(int partition, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value);

    /// <summary>Adds a tombstone: <paramref name="key"/> no longer has a record.</summary>
    void AppendTombstone(int partition, ReadOnlySpan<byte> key);

    /// <summary>
    /// Makes every appended record durable and readable, atomically, and completes once
    /// that is so. Records of one transaction keep their append order within a partition.
    /// <para>
    /// A failure has one of two meanings, and the implementation must make sure it is one
    /// of them. <see cref="ChangelogFencedException"/>: this writer is finished — another
    /// has taken over, or the log has failed in a way that leaves the writer unusable — and
    /// nothing of this transaction is or will be readable unless it had already committed
    /// when the new writer was acquired. Any other exception: the transaction was aborted,
    /// nothing of it is readable, and the writer can carry on. An implementation that
    /// cannot tell whether a commit landed must resolve that itself (retry, or treat the
    /// writer as finished) before it reports.
    /// </para>
    /// </summary>
    ValueTask CommitAsync(CancellationToken cancellationToken = default);
}

/// <summary>One record read from an <see cref="IStateChangelog"/>.</summary>
public readonly struct ChangelogRecord(long offset, ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, bool isTombstone)
{
    public long Offset { get; } = offset;
    public ReadOnlyMemory<byte> Key { get; } = key;

    /// <summary>Empty for a tombstone.</summary>
    public ReadOnlyMemory<byte> Value { get; } = value;

    /// <summary>The key was removed (Kafka: a null value).</summary>
    public bool IsTombstone { get; } = isTombstone;
}

/// <summary>Consecutive records of one partition, and where the reader now stands. The
/// records' memory is valid until the next batch is requested.</summary>
public sealed class ChangelogBatch(IReadOnlyList<ChangelogRecord> records, long nextOffset)
{
    public IReadOnlyList<ChangelogRecord> Records { get; } = records;

    /// <summary>The offset the next batch starts at: past every record in this one, and
    /// past any offsets after them that hold no record.</summary>
    public long NextOffset { get; } = nextOffset;
}

/// <summary>
/// Thrown when a changelog transaction cannot commit because its writer is finished:
/// another process has acquired the writer (a newer term), or the log failed fatally.
/// A checkpoint that hits this did not commit; its circuit faults, and so does every other
/// circuit on the same <see cref="ChangelogStorage"/> at its next commit. The process is no
/// longer the active one and should exit.
/// </summary>
public sealed class ChangelogFencedException(string message, Exception? innerException = null)
    : Exception(message, innerException);
