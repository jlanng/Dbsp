using System.Buffers.Binary;
using System.Globalization;
using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Incremental.Kafka;

/// <summary>
/// Settings of a <see cref="KafkaUpsertSink{TKey,TRow}"/> or a
/// <see cref="KafkaAppendSink{T}"/>: the topic, and which producer writes it. Set exactly
/// one of <see cref="BootstrapServers"/> and <see cref="Log"/>.
/// <para>
/// <b><see cref="BootstrapServers"/>: a producer of the sink's own</b>, idempotent
/// (<c>enable.idempotence</c>, <c>acks=all</c>) and not transactional. This is the default
/// choice. A sink topic belongs to its readers: it may be on another cluster than the
/// logs, and written this way they read it with no isolation level to think about. The
/// guarantee is the engine's, at-least-once: a batch is acknowledged when every record's
/// delivery report is in, the checkpoint commits after that, and a crash in between
/// delivers the batch again. A transaction would add nothing to it, because the write and
/// the checkpoint are two transactions either way (exactly-once to sinks is out of scope).
/// What this producer does not give is fencing: a deposed leader that wakes up can still
/// write, so wrap the sink in <c>leadership.Leased(...)</c> and set <see cref="Term"/>.
/// </para>
/// <para>
/// <b><see cref="Log"/>: the process's one transactional producer.</b> Each batch rides a
/// transaction of the log's group commit, so a deposed leader cannot write the topic at
/// all — fencing closes the sink gap that the lease only narrows. The price: the topic
/// must be on the log's cluster, a delivery costs one transaction (which the checkpoint
/// behind it costs anyway), and readers should use <c>isolation.level=read_committed</c>.
/// </para>
/// </summary>
public sealed class KafkaSinkOptions
{
    /// <summary>The topic written. It must exist: a sink does not create its topic, since
    /// only its owner knows the partition count and, for an upsert sink, that it should be
    /// compacted.</summary>
    public required string Topic { get; init; }

    /// <summary>The cluster to write to with a producer of the sink's own.</summary>
    public string? BootstrapServers { get; init; }

    /// <summary>The log whose transactional producer writes the topic. The log must be the
    /// writer (the node leads) when a batch arrives.</summary>
    public KafkaLog? Log { get; init; }

    /// <summary>The leader term to stamp on every record as the header
    /// <c>incremental.term</c> — pass <c>() =&gt; leadership.Term</c>. A reader that keeps the
    /// term beside each row can refuse a row from an older term than the one it holds,
    /// which is how a late write by a deposed leader is told from a current one.</summary>
    public Func<long>? Term { get; init; }

    /// <summary>Compression of the sink's own producer; ignored with <see cref="Log"/>,
    /// whose producer has its own setting.</summary>
    public CompressionType Compression { get; init; } = CompressionType.Zstd;

    /// <summary>How long the sink's own producer keeps trying to deliver a record
    /// (<c>message.timeout.ms</c>) before the batch fails and the delivery's retry policy
    /// takes over. Default 30 s.</summary>
    public TimeSpan DeliveryTimeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>Extra librdkafka settings for the sink's own producer, applied last.</summary>
    public IReadOnlyDictionary<string, string> ClientConfig { get; init; } = new Dictionary<string, string>();

    /// <summary>Access to the builder of the sink's own producer.</summary>
    public Action<ProducerBuilder<byte[], byte[]>>? ConfigureProducer { get; init; }

    public ILogger? Logger { get; init; }

    internal void Validate()
    {
        ArgumentException.ThrowIfNullOrEmpty(Topic);
        if (Log is null == BootstrapServers is null)
            throw new ArgumentException(
                "Set exactly one of BootstrapServers (the sink gets an idempotent producer of its own) and Log (the sink " +
                "writes through the log's transactional producer).");
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(DeliveryTimeout, TimeSpan.Zero);
    }
}

/// <summary>A batch could not be written to a sink topic. Thrown to the delivery, which
/// retries the batch (<see cref="SinkDeliveryOptions"/>).</summary>
public sealed class KafkaSinkException(string message, Exception? innerException = null)
    : Exception(message, innerException);

/// <summary>The names of the headers the Kafka sinks write.</summary>
public static class KafkaSinkHeaders
{
    /// <summary>The sequence of the step that produced the record, as decimal text
    /// (<see cref="OutputBatch{T}.Sequence"/>).</summary>
    public const string Step = "incremental.step";

    /// <summary>The leader term the record was written under, as decimal text; present when
    /// <see cref="KafkaSinkOptions.Term"/> is set.</summary>
    public const string Term = "incremental.term";

    /// <summary>Reads a header written by a sink; null when the record has none.</summary>
    public static long? Read(Headers? headers, string name) =>
        headers is not null && headers.TryGetLastBytes(name, out var bytes)
        && long.TryParse(Encoding.ASCII.GetString(bytes), NumberStyles.None, CultureInfo.InvariantCulture, out var value)
            ? value
            : null;
}

/// <summary>
/// An <see cref="IUpsertSink{TKey,TRow}"/> over a Kafka topic, in the shape a compacted
/// topic wants: an upsert is a record <c>key → row</c>, a delete is a tombstone (the key
/// with no value). Keys and rows are JSON unless told otherwise. Every record carries the
/// step sequence, and the leader term if asked (<see cref="KafkaSinkHeaders"/>). Records
/// are placed by the Java client's rule (murmur2 of the key bytes), so a key's records
/// stay in one partition, in order. Delivery is at-least-once; see
/// <see cref="KafkaSinkOptions"/> for the two producers and what each guarantees.
/// <para>
/// A second circuit reads the topic back with a keyed <see cref="KafkaSource"/> and the
/// default decoder, given the same key and row formats.
/// </para>
/// </summary>
public sealed class KafkaUpsertSink<TKey, TRow> : IUpsertSink<TKey, TRow>, IAsyncDisposable
    where TKey : notnull
{
    private readonly SinkWriter _writer;
    private readonly KafkaSerializer<TKey> _key;
    private readonly KafkaSerializer<TRow> _row;
    private readonly List<LogRecord> _records = [];

    public KafkaUpsertSink(KafkaSinkOptions options, KafkaSerializer<TKey>? key = null, KafkaSerializer<TRow>? row = null)
    {
        _writer = new SinkWriter(options);
        _key = key ?? KafkaSerializers.Json<TKey>();
        _row = row ?? KafkaSerializers.Json<TRow>();
    }

    public string Topic => _writer.Topic;

    /// <summary>Records written and confirmed, tombstones included.</summary>
    public long RecordsWritten => _writer.RecordsWritten;

    public async ValueTask WriteAsync(UpsertBatch<TKey, TRow> batch, CancellationToken cancellationToken)
    {
        var partitions = await _writer.PartitionsAsync(cancellationToken).ConfigureAwait(false);
        var headers = _writer.Headers(batch.Sequence);
        _records.Clear();
        foreach (var (key, row) in batch.Upserts)
        {
            var keyBytes = _key(key);
            _records.Add(new LogRecord(new TopicPartition(Topic, Murmur2.Partition(keyBytes, partitions)), keyBytes, _row(row), headers));
        }
        foreach (var key in batch.Deletes)
        {
            var keyBytes = _key(key);
            _records.Add(new LogRecord(new TopicPartition(Topic, Murmur2.Partition(keyBytes, partitions)), keyBytes, null, headers));
        }
        await _writer.WriteAsync(_records, cancellationToken).ConfigureAwait(false);
    }

    public ValueTask DisposeAsync() => _writer.DisposeAsync();
}

/// <summary>
/// An <see cref="IAppendSink{T}"/> over a Kafka topic: one record per appended record,
/// each with the header <c>incremental.step</c> — the sequence of the step that produced
/// it — because that is all a topic can offer against duplicates: it cannot take records
/// back.
/// <para>
/// <b>What a reader does with the header.</b> Along a partition the step never falls,
/// except where the circuit restarted: the steps after its last checkpoint are then
/// delivered again, so the sequence steps back to the first of them. A reader that sees a
/// step at or below one it has already read therefore drops what it had read at or above
/// that step and takes the new records instead — or, when the producer steps manually and
/// replays the same step boundaries, simply skips records whose step it has seen. Records
/// of one step can be spread over partitions (by key), so the rule is per partition.
/// </para>
/// With a key serializer a record is placed by its key; without one, all of a step's
/// records go to the partition <c>step mod partitions</c>.
/// </summary>
public sealed class KafkaAppendSink<T> : IAppendSink<T>, IAsyncDisposable
{
    private readonly SinkWriter _writer;
    private readonly KafkaSerializer<T> _value;
    private readonly KafkaSerializer<T>? _key;
    private readonly List<LogRecord> _records = [];

    /// <param name="options">The topic and the producer.</param>
    /// <param name="value">The record's bytes; JSON by default.</param>
    /// <param name="key">The record's key bytes, or null for records with no key.</param>
    public KafkaAppendSink(KafkaSinkOptions options, KafkaSerializer<T>? value = null, KafkaSerializer<T>? key = null)
    {
        _writer = new SinkWriter(options);
        _value = value ?? KafkaSerializers.Json<T>();
        _key = key;
    }

    public string Topic => _writer.Topic;

    /// <summary>Records written and confirmed.</summary>
    public long RecordsWritten => _writer.RecordsWritten;

    public async ValueTask WriteAsync(AppendBatch<T> batch, CancellationToken cancellationToken)
    {
        var partitions = await _writer.PartitionsAsync(cancellationToken).ConfigureAwait(false);
        var headers = _writer.Headers(batch.Sequence);
        var unkeyed = (int)(batch.Sequence % partitions);
        _records.Clear();
        foreach (var record in batch.Records)
        {
            var keyBytes = _key?.Invoke(record);
            var partition = keyBytes is null ? unkeyed : Murmur2.Partition(keyBytes, partitions);
            _records.Add(new LogRecord(new TopicPartition(Topic, partition), keyBytes, _value(record), headers));
        }
        await _writer.WriteAsync(_records, cancellationToken).ConfigureAwait(false);
    }

    public ValueTask DisposeAsync() => _writer.DisposeAsync();
}

/// <summary>What the two sinks share: the producer (the sink's own, or the log's), the
/// topic's partition count, and "write these records and return when they are durable".</summary>
internal sealed class SinkWriter : IAsyncDisposable
{
    private readonly KafkaSinkOptions _options;
    private readonly IProducer<byte[], byte[]>? _producer;
    private int _partitions;
    private long _written;

    public SinkWriter(KafkaSinkOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        _options = options;
        if (options.Log is not null)
            return;

        var config = new ProducerConfig
        {
            BootstrapServers = options.BootstrapServers,
            EnableIdempotence = true,
            Acks = Acks.All,
            CompressionType = options.Compression,
            MessageTimeoutMs = (int)options.DeliveryTimeout.TotalMilliseconds,
            LingerMs = 1,
        };
        // A producer may ask the broker to create a topic it names; a sink must not — a
        // topic made that way has whatever partitions and cleanup policy the broker defaults to.
        config.Set("allow.auto.create.topics", "false");
        KafkaClientDefaults.Apply(config);
        foreach (var (key, value) in options.ClientConfig)
            config.Set(key, value);
        var builder = new ProducerBuilder<byte[], byte[]>(config)
            .SetLogHandler((_, message) => options.Logger?.LogDebug("{Client} {Facility}: {Message}", message.Name, message.Facility, message.Message))
            .SetErrorHandler((_, error) => options.Logger?.Log(
                error.IsFatal ? LogLevel.Error : LogLevel.Warning, "Kafka sink {Topic} error {Code}: {Reason}", options.Topic, error.Code, error.Reason));
        options.ConfigureProducer?.Invoke(builder);
        _producer = builder.Build();
    }

    public string Topic => _options.Topic;

    public long RecordsWritten => Interlocked.Read(ref _written);

    public Headers Headers(long sequence)
    {
        var headers = new Headers { { KafkaSinkHeaders.Step, Ascii(sequence) } };
        if (_options.Term is { } term)
            headers.Add(KafkaSinkHeaders.Term, Ascii(term()));
        return headers;
    }

    private static byte[] Ascii(long value) => Encoding.ASCII.GetBytes(value.ToString(CultureInfo.InvariantCulture));

    /// <summary>The topic's partition count, asked once: records are placed by it.</summary>
    public async ValueTask<int> PartitionsAsync(CancellationToken cancellationToken)
    {
        if (_partitions > 0)
            return _partitions;
        try
        {
            if (_options.Log is { } log)
                return _partitions = await log.PartitionCountAsync(Topic, cancellationToken).ConfigureAwait(false);

            return _partitions = await Task.Run(() =>
            {
                using var admin = new DependentAdminClientBuilder(_producer!.Handle).Build();
                var topic = admin.GetMetadata(Topic, TimeSpan.FromSeconds(30)).Topics.FirstOrDefault(t => t.Topic == Topic);
                if (topic is null || topic.Error.IsError || topic.Partitions.Count == 0)
                    throw new KafkaSinkException(
                        $"Sink topic '{Topic}' cannot be written: {(topic is null || !topic.Error.IsError || topic.Error.Code == ErrorCode.UnknownTopicOrPart ? "it does not exist" : topic.Error.Reason)}. " +
                        "A sink does not create its topic.");
                return topic.Partitions.Count;
            }, cancellationToken).ConfigureAwait(false);
        }
        catch (KafkaException ex)
        {
            throw new KafkaSinkException($"Sink topic '{Topic}' cannot be written: {ex.Error.Reason}", ex);
        }
    }

    /// <summary>Writes the records and returns once every one is durable in the topic.</summary>
    public async ValueTask WriteAsync(List<LogRecord> records, CancellationToken cancellationToken)
    {
        if (records.Count == 0)
            return;

        if (_options.Log is { } log)
        {
            var commit = new LogCommit();
            commit.Records.AddRange(records);
            await log.CommitAsync(commit).ConfigureAwait(false);
            Interlocked.Add(ref _written, records.Count);
            return;
        }

        var pending = new Pending(records.Count);
        foreach (var record in records)
        {
            var message = new Message<byte[], byte[]> { Key = record.Key!, Value = record.Value!, Headers = record.Headers };
            while (true)
            {
                try
                {
                    _producer!.Produce(record.Target, message, pending.Report);
                    break;
                }
                catch (ProduceException<byte[], byte[]> ex) when (ex.Error.Code == ErrorCode.Local_QueueFull)
                {
                    // The client's queue is full of this batch: let it drain.
                    await Task.Delay(5, cancellationToken).ConfigureAwait(false);
                }
                catch (ProduceException<byte[], byte[]> ex)
                {
                    throw new KafkaSinkException($"A record could not be written to sink topic '{Topic}': {ex.Error.Reason}", ex);
                }
            }
        }
        // Not cancellable from here: the records are with the client, and the delivery
        // reports say what became of them within the delivery timeout.
        await pending.Completion.Task.ConfigureAwait(false);
        Interlocked.Add(ref _written, records.Count);
    }

    private sealed class Pending(int count)
    {
        private int _remaining = count;
        private Error? _error;

        public readonly TaskCompletionSource Completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Report(DeliveryReport<byte[], byte[]> report)
        {
            if (report.Error.IsError)
                Interlocked.CompareExchange(ref _error, report.Error, null);
            if (Interlocked.Decrement(ref _remaining) != 0)
                return;
            if (_error is { } error)
                Completion.TrySetException(new KafkaSinkException(
                    $"A batch was not delivered to sink topic '{report.Topic}': {error.Reason}", new KafkaException(error)));
            else
                Completion.TrySetResult();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_producer is { } producer)
            await Task.Run(() =>
            {
                producer.Flush(TimeSpan.FromSeconds(5));
                producer.Dispose();
            }).ConfigureAwait(false);
    }
}

/// <summary>The Java client's default partitioner hash, so that a sink places a key where
/// any other producer of the topic would.</summary>
internal static class Murmur2
{
    public static int Partition(ReadOnlySpan<byte> key, int partitions) => (int)((Hash(key) & 0x7fffffff) % (uint)partitions);

    public static uint Hash(ReadOnlySpan<byte> data)
    {
        const uint m = 0x5bd1e995;
        var h = 0x9747b28c ^ (uint)data.Length;
        while (data.Length >= 4)
        {
            var k = BinaryPrimitives.ReadUInt32LittleEndian(data);
            k *= m;
            k ^= k >> 24;
            k *= m;
            h *= m;
            h ^= k;
            data = data[4..];
        }
        switch (data.Length)
        {
            case 3:
                h ^= (uint)data[2] << 16;
                goto case 2;
            case 2:
                h ^= (uint)data[1] << 8;
                goto case 1;
            case 1:
                h ^= data[0];
                h *= m;
                break;
        }
        h ^= h >> 13;
        h *= m;
        h ^= h >> 15;
        return h;
    }
}
