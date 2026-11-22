using System.Runtime.CompilerServices;
using Confluent.Kafka;
using Dbsp.Storage;

namespace Dbsp.Kafka;

/// <summary>
/// The state changelog on Kafka: <see cref="IStateChangelog"/> over the one compacted topic
/// <c>{prefix}.state</c>, written through its <see cref="KafkaLog"/>'s transactional
/// producer. Get it from <see cref="KafkaLog.State"/>.
/// <list type="bullet">
/// <item><see cref="AcquireWriterAsync"/> is <see cref="KafkaLog.AcquireWriterAsync"/>:
/// <c>InitTransactions</c> on the shared <c>transactional.id</c>, then a new leader term.</item>
/// <item>A transaction here is one commit handed to the log's writer thread. Several ride
/// in one Kafka transaction; each still lands whole or not at all.</item>
/// <item><see cref="IStateChangelogTransaction.CommitAsync"/> fails as
/// <see cref="ChangelogFencedException"/> when the writer is finished (fenced, or failed
/// fatally) and as <see cref="KafkaLogException"/> when the transaction was aborted and the
/// writer carries on. A commit whose outcome is not yet known is asked about again until it
/// is one or the other.</item>
/// <item>Readers use <c>isolation.level=read_committed</c>. <see cref="EndOffsetAsync"/> is
/// the partition's last stable offset, and a reader's position moves past the transaction
/// markers at the end of committed data.</item>
/// <item>A removed entry is a record with a null value, which compaction drops after the
/// topic's <c>delete.retention.ms</c>.</item>
/// </list>
/// </summary>
public sealed class KafkaStateChangelog : IStateChangelog
{
    private readonly KafkaLog _log;
    private readonly TopicPartition[] _partitions;

    internal KafkaStateChangelog(KafkaLog log)
    {
        _log = log;
        Topic = log.Options.TopicPrefix + ".state";
        _partitions = new TopicPartition[log.Options.StatePartitions];
        for (var p = 0; p < _partitions.Length; p++)
            _partitions[p] = new TopicPartition(Topic, p);
    }

    /// <summary>The topic's name: <c>{prefix}.state</c>.</summary>
    public string Topic { get; }

    public int PartitionCount => _partitions.Length;

    public async ValueTask<IStateChangelogWriter> AcquireWriterAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await _log.AcquireWriterAsync(cancellationToken).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (KafkaLogFencedException ex)
        {
            throw new ChangelogFencedException(ex.Message, ex);
        }
        return new Writer(this, _log.Term);
    }

    /// <summary>
    /// The partition's last stable offset. One caveat that the in-memory changelog does not
    /// have: Kafka writes a transaction's commit markers a moment after the commit call
    /// returns, so asked immediately after a commit this can still be the offset before it.
    /// Asked after <see cref="AcquireWriterAsync"/> it is exact — initialising the
    /// transactional id waits for the previous writer's markers — and that is the case a
    /// takeover depends on.
    /// </summary>
    public async ValueTask<long> EndOffsetAsync(int partition, CancellationToken cancellationToken = default) =>
        await _log.EndOffsetAsync(_partitions[partition], cancellationToken).ConfigureAwait(false);

    public async IAsyncEnumerable<ChangelogBatch> ReadAsync(
        int partition, long fromOffset, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(fromOffset);
        await foreach (var batch in _log.ReadBatchesAsync(_partitions[partition], fromOffset, cancellationToken).ConfigureAwait(false))
        {
            var records = new ChangelogRecord[batch.Records.Count];
            for (var i = 0; i < records.Length; i++)
            {
                var message = batch.Records[i];
                records[i] = message.Message.Value is { } value
                    ? new ChangelogRecord(message.Offset.Value, message.Message.Key, value, isTombstone: false)
                    : new ChangelogRecord(message.Offset.Value, message.Message.Key, ReadOnlyMemory<byte>.Empty, isTombstone: true);
            }
            yield return new ChangelogBatch(records, batch.NextOffset);
        }
    }

    private sealed class Writer(KafkaStateChangelog changelog, long term) : IStateChangelogWriter
    {
        public long Term { get; } = term;

        public IStateChangelogTransaction BeginTransaction() => new Transaction(changelog);

        // The producer belongs to the KafkaLog, which tables write through as well.
        public ValueTask DisposeAsync() => default;
    }

    private sealed class Transaction(KafkaStateChangelog changelog) : IStateChangelogTransaction
    {
        private LogCommit? _commit = new();

        public void Append(int partition, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) =>
            Open().Add(changelog._partitions[partition], key.ToArray(), value.ToArray());

        public void AppendTombstone(int partition, ReadOnlySpan<byte> key) =>
            Open().Add(changelog._partitions[partition], key.ToArray(), null);

        private LogCommit Open() =>
            _commit ?? throw new InvalidOperationException("This changelog transaction has been committed or disposed.");

        public async ValueTask CommitAsync(CancellationToken cancellationToken = default)
        {
            var commit = Open();
            cancellationToken.ThrowIfCancellationRequested();
            _commit = null;
            if (commit.Records.Count == 0)
                return;
            try
            {
                // Not cancellable from here: once the records are with the writer the
                // transaction lands or fails, and the caller must hear which.
                await changelog._log.CommitAsync(commit).ConfigureAwait(false);
            }
            catch (KafkaLogFencedException ex)
            {
                throw new ChangelogFencedException(ex.Message, ex);
            }
        }

        // Nothing reaches Kafka before CommitAsync, so an uncommitted transaction is
        // aborted by forgetting it.
        public void Dispose() => _commit = null;
    }
}
