using Confluent.Kafka;
using Dbsp.Storage;
using Microsoft.Extensions.Logging;

namespace Dbsp.Kafka;

/// <summary>Settings of a <see cref="KafkaLog"/>. The active and its standby must agree on
/// <see cref="TopicPrefix"/>, <see cref="TransactionalId"/>, <see cref="StatePartitions"/>
/// and <see cref="Serializer"/>.</summary>
public sealed class KafkaLogOptions
{
    /// <summary>The cluster to connect to (<c>bootstrap.servers</c>).</summary>
    public required string BootstrapServers { get; init; }

    /// <summary>Prefix of every topic this log owns: <c>{prefix}.state</c>,
    /// <c>{prefix}.leader</c>, <c>{prefix}.table.{name}.changes</c> and
    /// <c>{prefix}.table.{name}.rows</c>. One prefix is one deployment.</summary>
    public string TopicPrefix { get; init; } = "dbsp";

    /// <summary>The <c>transactional.id</c> of the one producer everything is written
    /// through. Whoever initialises it last is the only process that can commit, so the
    /// active and the standby share it. Defaults to <c>{prefix}.writer</c>.</summary>
    public string? TransactionalId { get; init; }

    /// <summary>This process's name in the leader topic: it is written with the term and
    /// with every heartbeat, and it is how a status page says who leads. The active and
    /// its standby must differ. Defaults to the machine name and the process id.</summary>
    public string? NodeId { get; init; }

    /// <summary>How often an idle writer commits a transaction carrying only a heartbeat to
    /// <c>{prefix}.leader</c>. Every other transaction carries one too, so this is the
    /// longest a healthy writer is silent — and the longest a writer that was fenced while
    /// idle takes to find out. A standby's takeover timeout
    /// (<see cref="KafkaLeadershipOptions.TakeoverAfter"/>) must be a few of these. Default
    /// 1 s.</summary>
    public TimeSpan HeartbeatInterval { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>Partitions of <c>{prefix}.state</c> (the design's <c>P</c>). Fixed for the
    /// life of the topic: a pipeline's partition is derived from it, so an existing topic
    /// with a different count is refused.</summary>
    public int StatePartitions { get; init; } = 12;

    /// <summary>Partitions of each table's <c>rows</c> topic when it is created (the
    /// design's <c>R</c>). An existing topic keeps the count it was created with.</summary>
    public int TableRowPartitions { get; init; } = 4;

    /// <summary>Replication factor of topics this log creates. 3 suits a three-broker MSK
    /// cluster; a single local broker needs 1.</summary>
    public short ReplicationFactor { get; init; } = 3;

    /// <summary>Producer compression (<c>compression.type</c>). Records are JSON, which
    /// compresses well; the default is <c>zstd</c>.</summary>
    public CompressionType Compression { get; init; } = CompressionType.Zstd;

    /// <summary>Serializes table keys and rows. The state changelog's records are
    /// serialized by <see cref="ChangelogStorageOptions.Serializer"/> instead. The output is
    /// a persisted format. Equal keys must serialize to the same text, because the rows
    /// topic compacts by key bytes.</summary>
    public IStateSerializer Serializer { get; init; } = JsonStateSerializer.Instance;

    /// <summary><c>delete.retention.ms</c> of the compacted topics: how long a tombstone
    /// stays readable. A restore or a follower that takes longer than this to read a
    /// partition can miss a tombstone and keep a deleted entry.</summary>
    public TimeSpan DeleteRetention { get; init; } = TimeSpan.FromHours(24);

    /// <summary>
    /// <c>transaction.timeout.ms</c>: how long one transaction of the writer may stay open.
    /// Two clocks run on it. The client gives a transaction's records this long to be
    /// taken by the brokers and then fails the transaction; the transaction coordinator
    /// aborts a transaction it has registered once it is this old and raises the
    /// producer's epoch, which fences the writer. Either way a writer whose brokers cannot
    /// commit for longer than this — a cluster without a quorum — usually ends
    /// (<see cref="KafkaLogFencedException"/>; the process exits and comes back as the
    /// standby), so this is how long an outage of the brokers a leader rides out, less
    /// the time the brokers take to commit again once they are back.
    /// <para>
    /// What a long timeout costs is small here. A writer that dies inside a transaction
    /// leaves it open until somebody initialises the <c>transactional.id</c> — which the
    /// standby's takeover and the node's own restart both do, within seconds — or until
    /// this timeout if nobody does; until then readers at <c>read_committed</c> see
    /// nothing past the transaction's first record in the partitions it wrote, which
    /// are this log's own topics and the topics of sinks that write through the log.
    /// </para>
    /// <para>
    /// The brokers refuse a value above their <c>transaction.max.timeout.ms</c> (fifteen
    /// minutes unless the cluster says otherwise) when the writer is acquired. Default
    /// five minutes (the Kafka clients' own default is one).
    /// </para>
    /// </summary>
    public TimeSpan TransactionTimeout { get; init; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// The persisted formats of the node this log belongs to
    /// (<see cref="PersistedFormat"/>): what it reads, and what it writes now. The writer
    /// announces the level it writes in every record of the leader topic, a new writer
    /// goes on at the level the last one announced, and <see cref="KafkaLog.RaiseFormatAsync"/>
    /// raises it. Give the same instance to the <see cref="ChangelogStorage"/> over this
    /// log (<see cref="ChangelogStorageOptions.Formats"/>). The default reads and writes
    /// this build's level from the start, which suits a deployment whose processes are
    /// all replaced together; a node of the enrichment service passes one that is raised
    /// only once both nodes read the level.
    /// </summary>
    public PersistedFormat Formats { get; set; } = PersistedFormat.Latest;

    /// <summary>How long the log writer waits for more commits before it begins a
    /// transaction for the ones it has. Zero (the default) adds no latency: commits that
    /// arrive while a transaction is in flight ride in the next one, which is where the
    /// batching comes from under load.</summary>
    public TimeSpan GroupCommitLinger { get; init; } = TimeSpan.Zero;

    /// <summary>The most records one Kafka transaction carries before the writer commits it
    /// and starts another. One commit is never split, so a single larger commit still goes
    /// in one transaction.</summary>
    public int MaxTransactionRecords { get; init; } = 100_000;

    /// <summary>The size at which a table version is split into another chunk record. Must
    /// stay under the topic's <c>max.message.bytes</c> (1 MiB by default on the broker).</summary>
    public int MaxRecordBytes { get; init; } = 900_000;

    /// <summary>How many times a transaction that the broker aborted is retried before the
    /// commits in it fail.</summary>
    public int MaxAbortRetries { get; init; } = 5;

    /// <summary>The most each of this log's consumers may hold of records fetched and not yet
    /// applied (<c>queued.max.messages.kbytes</c>), in kilobytes — for each partition it
    /// reads: the client keeps a queue per partition and fetches for one until that
    /// queue is this full. A follower or a restore runs one consumer per state partition
    /// and one per followed table (which has several), so while it is behind it holds
    /// about this much native memory per partition, and the client's allocator keeps it
    /// after the records are gone unless it is asked to give it back (the service asks).
    /// Default 8 MB (the client's own default is 64). At 2 MB a restore was if anything
    /// slower, for a few hundred megabytes that a node holds only while it restores
    /// (docs/design/enrichment-service.md, S7, <i>Memory</i>).</summary>
    public int FetchBufferKilobytes { get; init; } = 8 * 1024;

    /// <summary>Timeout of admin calls, and of a read that makes no progress.</summary>
    public TimeSpan OperationTimeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>Extra client settings applied to the producer, every consumer and the admin
    /// client, after this class's own — for example MSK's
    /// <c>security.protocol=SASL_SSL</c>, <c>sasl.mechanism=OAUTHBEARER</c> and the TLS
    /// settings. Keys are librdkafka configuration names.</summary>
    public IReadOnlyDictionary<string, string> ClientConfig { get; init; } = new Dictionary<string, string>();

    /// <summary>Called on the producer's builder after <see cref="ClientConfig"/> is applied:
    /// the place to attach what a dictionary cannot carry, such as an OAUTHBEARER token
    /// refresh handler for MSK IAM.</summary>
    public Action<ProducerBuilder<byte[], byte[]>>? ConfigureProducer { get; init; }

    /// <summary>As <see cref="ConfigureProducer"/>, for every consumer.</summary>
    public Action<ConsumerBuilder<byte[], byte[]>>? ConfigureConsumer { get; init; }

    /// <summary>As <see cref="ConfigureProducer"/>, for the admin client.</summary>
    public Action<AdminClientBuilder>? ConfigureAdmin { get; init; }

    /// <summary>Extra or overriding topic settings for <c>{prefix}.state</c> when it is
    /// created (for example <c>min.insync.replicas</c>, or small segments in a test).</summary>
    public IReadOnlyDictionary<string, string> StateTopicConfig { get; init; } = new Dictionary<string, string>();

    /// <summary>As <see cref="StateTopicConfig"/>, for each table's <c>rows</c> topic.</summary>
    public IReadOnlyDictionary<string, string> TableRowsTopicConfig { get; init; } = new Dictionary<string, string>();

    /// <summary>As <see cref="StateTopicConfig"/>, for each table's <c>changes</c> topic.</summary>
    public IReadOnlyDictionary<string, string> TableChangesTopicConfig { get; init; } = new Dictionary<string, string>();

    /// <summary>Receives the Kafka clients' log lines and errors. Null discards them.</summary>
    public ILogger? Logger { get; set; }

    internal string EffectiveTransactionalId => TransactionalId ?? TopicPrefix + ".writer";

    private static readonly string DefaultNodeId = $"{Environment.MachineName}-{Environment.ProcessId}";

    internal string EffectiveNodeId => NodeId ?? DefaultNodeId;

    internal void Validate()
    {
        ArgumentException.ThrowIfNullOrEmpty(BootstrapServers);
        ArgumentException.ThrowIfNullOrEmpty(TopicPrefix);
        if (!KafkaLog.IsTopicSafe(TopicPrefix))
            throw new ArgumentException(
                $"Topic prefix '{TopicPrefix}' is not usable in a topic name: use letters, digits, '.', '_' and '-'.", nameof(TopicPrefix));
        if (TransactionalId is { Length: 0 })
            throw new ArgumentException("The transactional id cannot be empty.", nameof(TransactionalId));
        if (NodeId is { Length: 0 })
            throw new ArgumentException("The node id cannot be empty.", nameof(NodeId));
        ArgumentOutOfRangeException.ThrowIfLessThan(HeartbeatInterval, TimeSpan.FromMilliseconds(10));
        ArgumentOutOfRangeException.ThrowIfLessThan(StatePartitions, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(TableRowPartitions, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(ReplicationFactor, (short)1);
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxTransactionRecords, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxRecordBytes, 1024);
        ArgumentOutOfRangeException.ThrowIfLessThan(FetchBufferKilobytes, 1);
        ArgumentOutOfRangeException.ThrowIfNegative(MaxAbortRetries);
        ArgumentOutOfRangeException.ThrowIfLessThan(GroupCommitLinger, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(TransactionTimeout, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(OperationTimeout, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(DeleteRetention, TimeSpan.Zero);
        ArgumentNullException.ThrowIfNull(Serializer);
    }
}

/// <summary>
/// A write through a <see cref="KafkaLog"/> failed and was aborted: nothing of it is
/// readable, and the log's writer carries on. Also thrown for a topic that does not have
/// the shape this log needs.
/// </summary>
public class KafkaLogException(string message, Exception? innerException = null)
    : Exception(message, innerException);

/// <summary>
/// The <see cref="KafkaLog"/>'s writer is finished: another process initialised the same
/// <c>transactional.id</c> (it has taken over), or the producer failed fatally. Nothing can
/// be written through this log again; the process should stop being the active. The state
/// changelog reports it as <see cref="ChangelogFencedException"/> and a table log as
/// <see cref="Tables.TableLogFencedException"/>, with this as the inner exception.
/// </summary>
public sealed class KafkaLogFencedException(string message, Exception? innerException = null)
    : KafkaLogException(message, innerException);

/// <summary>
/// <see cref="KafkaLog.AcquireWriterAsync"/> did not make this log the writer, and the log
/// is a reader again: another process initialised the <c>transactional.id</c> before this
/// one had committed its term, or the attempt was cancelled. Nothing of this process's was
/// committed. Unlike <see cref="KafkaLogFencedException"/> it is not final: the call can be
/// made again.
/// </summary>
public sealed class KafkaLogNotAcquiredException(string message, Exception? innerException = null)
    : KafkaLogException(message, innerException);
