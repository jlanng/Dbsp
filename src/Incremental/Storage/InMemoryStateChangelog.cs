using System.Buffers;
using System.Runtime.CompilerServices;

namespace Incremental.Storage;

/// <summary>
/// A heap-backed <see cref="IStateChangelog"/>: it stands in for the Kafka topic in tests
/// and lets one process play active and standby. It behaves like the real thing where the
/// difference could hide a bug: offsets are not dense (every transaction ends in an offset
/// with no record behind it, as a Kafka commit marker does), <see cref="Compact"/> drops
/// superseded records as the log cleaner would, and acquiring the writer fences the
/// previous one. It keeps every live record for good, so it is not a store to run on.
/// <para>
/// With a <c>compactAbove</c> it also compacts by itself, so that what it holds stays
/// proportional to the keys that are live rather than to everything ever committed: a
/// partition is compacted inside the commit that takes it past its threshold, and the
/// threshold then moves to twice what survived (never below <c>compactAbove</c>), so the
/// work is amortised over the records appended. A tombstone is dropped only once every
/// reader that is enumerating the partition has passed it — a reader that had seen the
/// record a tombstone removes and not the tombstone would keep the record. That protects
/// a reader for as long as it enumerates; a reader that stops and later resumes from the
/// offset it had reached is not protected, and must read from 0 again
/// (<see cref="ChangelogStorage"/> always does).
/// </para>
/// </summary>
public sealed class InMemoryStateChangelog : IStateChangelog
{
    private const int MaxBatchRecords = 256;

    private readonly object _lock = new();
    private readonly Partition[] _partitions;
    private long _term;

    // Completed and replaced by every commit: what a reader that has caught up waits on.
    private TaskCompletionSource _committed = new(TaskCreationOptions.RunContinuationsAsynchronously);

    private long _recordsAppended;
    private long _bytesAppended;
    private long _transactions;

    private sealed class Partition
    {
        public List<ChangelogRecord> Records = [];
        public long NextOffset;
        public bool Touched; // by the transaction being applied

        // Self-compaction: the record count that triggers the next one, and where each
        // reader now enumerating this partition stands.
        public long CompactAt;
        public readonly List<ReaderPosition> Readers = [];
    }

    private sealed class ReaderPosition
    {
        public long Next;
    }

    private readonly int _compactAbove;
    private long _compactions;

    /// <param name="partitions">The fixed partition count (the design's <c>P</c>).</param>
    /// <param name="compactAbove">Records a partition may hold before it compacts itself;
    /// 0 (the default) leaves compaction to <see cref="Compact"/>.</param>
    public InMemoryStateChangelog(int partitions = 12, int compactAbove = 0)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(partitions, 1);
        ArgumentOutOfRangeException.ThrowIfNegative(compactAbove);
        _compactAbove = compactAbove;
        _partitions = new Partition[partitions];
        for (var p = 0; p < partitions; p++)
            _partitions[p] = new Partition { CompactAt = compactAbove };
    }

    /// <summary>Times a partition has compacted itself (see the constructor's
    /// <c>compactAbove</c>).</summary>
    public long Compactions => Interlocked.Read(ref _compactions);

    public int PartitionCount => _partitions.Length;

    /// <summary>The term of the current writer; 0 before any was acquired.</summary>
    public long Term
    {
        get
        {
            lock (_lock)
                return _term;
        }
    }

    /// <summary>Records appended by committed transactions since the log was created,
    /// tombstones included. Compaction does not lower it.</summary>
    public long RecordsAppended => Interlocked.Read(ref _recordsAppended);

    /// <summary>Key and value bytes appended by committed transactions.</summary>
    public long BytesAppended => Interlocked.Read(ref _bytesAppended);

    /// <summary>Transactions committed.</summary>
    public long TransactionsCommitted => Interlocked.Read(ref _transactions);

    /// <summary>Records currently held, across all partitions.</summary>
    public long RecordCount
    {
        get
        {
            lock (_lock)
            {
                var count = 0L;
                foreach (var partition in _partitions)
                    count += partition.Records.Count;
                return count;
            }
        }
    }

    /// <summary>
    /// A test hook, awaited at the start of every commit, before the transaction is checked
    /// against the current term and applied. Returning late holds the commit in flight;
    /// throwing fails it (the transaction is aborted). Null by default.
    /// </summary>
    public Func<ValueTask>? OnCommitting { get; set; }

    public ValueTask<IStateChangelogWriter> AcquireWriterAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_lock)
            return new ValueTask<IStateChangelogWriter>(new Writer(this, ++_term));
    }

    public ValueTask<long> EndOffsetAsync(int partition, CancellationToken cancellationToken = default)
    {
        lock (_lock)
            return new ValueTask<long>(_partitions[partition].NextOffset);
    }

    public async IAsyncEnumerable<ChangelogBatch> ReadAsync(
        int partition, long fromOffset, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(fromOffset);
        var source = _partitions[partition];
        var position = fromOffset;

        // Registered for as long as the enumeration lasts: no tombstone at or after
        // where this reader stands is dropped by a self-compaction.
        var reader = new ReaderPosition { Next = fromOffset };
        lock (_lock)
            source.Readers.Add(reader);
        try
        {
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                ChangelogBatch? batch = null;
                Task committed;
                lock (_lock)
                {
                    committed = _committed.Task;
                    if (position < source.NextOffset)
                    {
                        var records = source.Records;
                        var index = FirstAtOrAfter(records, position);
                        var count = Math.Min(MaxBatchRecords, records.Count - index);
                        var copy = new ChangelogRecord[count];
                        records.CopyTo(index, copy, 0, count);
                        position = index + count == records.Count ? source.NextOffset : copy[^1].Offset + 1;
                        reader.Next = position;
                        batch = new ChangelogBatch(copy, position);
                    }
                }

                if (batch is not null)
                    yield return batch;
                else
                    await committed.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            lock (_lock)
                source.Readers.Remove(reader);
        }
    }

    private static int FirstAtOrAfter(List<ChangelogRecord> records, long offset)
    {
        int low = 0, high = records.Count;
        while (low < high)
        {
            var middle = (low + high) >>> 1;
            if (records[middle].Offset < offset)
                low = middle + 1;
            else
                high = middle;
        }
        return low;
    }

    /// <summary>
    /// Compacts every partition as the log cleaner would: of the records sharing a key only
    /// the last survives. With <paramref name="dropTombstones"/> a key whose last record is
    /// a tombstone disappears altogether, as it does once Kafka's
    /// <c>delete.retention.ms</c> has passed — which a reader part-way through the
    /// partition must not straddle (it would keep the row the tombstone removed), so pass
    /// false while a follower may be behind. Offsets of surviving records do not change.
    /// </summary>
    public void Compact(bool dropTombstones = true)
    {
        lock (_lock)
        {
            foreach (var partition in _partitions)
                CompactPartition(partition, dropTombstones ? long.MaxValue : 0);
        }
    }

    /// <summary>Keeps the last record of every key, and of those drops the tombstones
    /// below <paramref name="dropTombstonesBelow"/>. Caller holds the lock.</summary>
    private static void CompactPartition(Partition partition, long dropTombstonesBelow)
    {
        var records = partition.Records;
        var last = new Dictionary<ReadOnlyMemory<byte>, int>(ByteKeyComparer.Instance);
        for (var i = 0; i < records.Count; i++)
            last[records[i].Key] = i;

        var kept = new List<ChangelogRecord>(last.Count);
        for (var i = 0; i < records.Count; i++)
        {
            var record = records[i];
            if (last[record.Key] == i && !(record.IsTombstone && record.Offset < dropTombstonesBelow))
                kept.Add(record);
        }
        partition.Records = kept;
    }

    /// <summary>A partition compacting itself, inside the commit that took it past its
    /// threshold. Caller holds the lock.</summary>
    private void CompactIfDue(Partition partition)
    {
        if (_compactAbove == 0 || partition.Records.Count < partition.CompactAt)
            return;

        // Every reader enumerating the partition has been handed the records below where
        // it stands; with none, a later reader starts from 0 and never sees what a
        // dropped tombstone removed.
        var horizon = long.MaxValue;
        foreach (var reader in partition.Readers)
            horizon = Math.Min(horizon, reader.Next);
        CompactPartition(partition, horizon);
        partition.CompactAt = Math.Max(_compactAbove, 2L * partition.Records.Count);
        _compactions++;
    }

    private sealed class ByteKeyComparer : IEqualityComparer<ReadOnlyMemory<byte>>
    {
        public static readonly ByteKeyComparer Instance = new();

        public bool Equals(ReadOnlyMemory<byte> x, ReadOnlyMemory<byte> y) => x.Span.SequenceEqual(y.Span);

        public int GetHashCode(ReadOnlyMemory<byte> key)
        {
            var hash = new HashCode();
            hash.AddBytes(key.Span);
            return hash.ToHashCode();
        }
    }

    private async ValueTask CommitAsync(Writer writer, Transaction transaction)
    {
        if (OnCommitting is { } hook)
            await hook().ConfigureAwait(false);

        TaskCompletionSource committed;
        lock (_lock)
        {
            if (writer.Term != _term)
                throw new ChangelogFencedException(
                    $"The changelog writer of term {writer.Term} has been fenced: term {_term} has taken the log over.");

            var bytes = 0L;
            var buffer = transaction.Buffer.WrittenSpan;
            foreach (var entry in transaction.Entries)
            {
                var partition = _partitions[entry.Partition];
                var key = buffer.Slice(entry.KeyStart, entry.KeyLength).ToArray();
                var value = entry.ValueLength < 0 ? [] : buffer.Slice(entry.ValueStart, entry.ValueLength).ToArray();
                partition.Records.Add(new ChangelogRecord(partition.NextOffset++, key, value, entry.ValueLength < 0));
                partition.Touched = true;
                bytes += key.Length + value.Length;
            }

            // The commit marker: an offset in each written partition with no record behind
            // it, so nothing here can come to depend on offsets being dense.
            foreach (var partition in _partitions)
            {
                if (!partition.Touched)
                    continue;
                partition.Touched = false;
                partition.NextOffset++;
                CompactIfDue(partition);
            }

            _recordsAppended += transaction.Entries.Count;
            _bytesAppended += bytes;
            _transactions++;
            committed = _committed;
            _committed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }
        committed.TrySetResult();
    }

    private sealed class Writer(InMemoryStateChangelog log, long term) : IStateChangelogWriter
    {
        // Transactions are reused: their buffers keep the capacity of the largest commit
        // they have carried, so a steady stream of commits allocates no buffers.
        private readonly Stack<Transaction> _spare = new();

        public long Term { get; } = term;

        public IStateChangelogTransaction BeginTransaction()
        {
            lock (_spare)
            {
                if (_spare.TryPop(out var spare))
                {
                    spare.Open = true;
                    return spare;
                }
            }
            return new Transaction(log, this);
        }

        public void Return(Transaction transaction)
        {
            transaction.Reset();
            lock (_spare)
            {
                if (_spare.Count < 16)
                    _spare.Push(transaction);
            }
        }

        public ValueTask DisposeAsync() => default;
    }

    private readonly record struct Entry(int Partition, int KeyStart, int KeyLength, int ValueStart, int ValueLength);

    private sealed class Transaction(InMemoryStateChangelog log, Writer writer) : IStateChangelogTransaction
    {
        // Scratch released rather than kept once it has grown past this: one very large
        // commit (a bootstrap) should not pin its buffer for the life of the writer.
        private const int MaxRetainedBytes = 1 << 20;

        public ArrayBufferWriter<byte> Buffer = new();
        public readonly List<Entry> Entries = [];
        public bool Open = true;

        public void Append(int partition, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            CheckOpen(partition);
            var start = Buffer.WrittenCount;
            Buffer.Write(key);
            Buffer.Write(value);
            Entries.Add(new Entry(partition, start, key.Length, start + key.Length, value.Length));
        }

        public void AppendTombstone(int partition, ReadOnlySpan<byte> key)
        {
            CheckOpen(partition);
            var start = Buffer.WrittenCount;
            Buffer.Write(key);
            Entries.Add(new Entry(partition, start, key.Length, 0, -1));
        }

        private void CheckOpen(int partition)
        {
            if (!Open)
                throw new InvalidOperationException("This changelog transaction has been committed or disposed.");
            ArgumentOutOfRangeException.ThrowIfNegative(partition);
            ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(partition, log.PartitionCount);
        }

        public ValueTask CommitAsync(CancellationToken cancellationToken = default)
        {
            if (!Open)
                throw new InvalidOperationException("This changelog transaction has been committed or disposed.");
            cancellationToken.ThrowIfCancellationRequested();
            return log.CommitAsync(writer, this);
        }

        public void Reset()
        {
            Entries.Clear();
            if (Buffer.Capacity > MaxRetainedBytes)
                Buffer = new ArrayBufferWriter<byte>();
            else
                Buffer.ResetWrittenCount();
            if (Entries.Capacity > MaxRetainedBytes / 16)
                Entries.Capacity = 0;
        }

        public void Dispose()
        {
            if (!Open)
                return;
            Open = false;
            writer.Return(this);
        }
    }
}
