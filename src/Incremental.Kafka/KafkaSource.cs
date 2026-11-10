using System.Threading.Channels;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Incremental.Tables;
using Microsoft.Extensions.Logging;

namespace Incremental.Kafka;

/// <summary>Where a source starts reading a topic it has never read.</summary>
public enum KafkaSourceStart
{
    /// <summary>At the first record each partition still holds.</summary>
    Earliest,

    /// <summary>At the end: only records produced after the source first started. "First"
    /// is the start at which nothing was committed for any partition. The ends found then
    /// are recorded as positions, so a later start resumes from them like any other; and a
    /// partition that still has no position at a later start — it was empty at the first,
    /// or was added since — is read from its beginning, because everything in it is new.</summary>
    Latest,
}

/// <summary>Settings of a <see cref="KafkaSource"/>.</summary>
public sealed class KafkaSourceOptions
{
    /// <summary>The cluster the topic is on (<c>bootstrap.servers</c>). It need not be the
    /// cluster a <see cref="KafkaLog"/> writes to.</summary>
    public required string BootstrapServers { get; init; }

    /// <summary>The topic to read. Every partition it has when the source starts is read;
    /// partitions added later are picked up at the next start.</summary>
    public required string Topic { get; init; }

    /// <summary>Where the source starts in a topic it has never read: see
    /// <see cref="KafkaSourceStart"/>. A partition with a committed position always resumes
    /// just after it.</summary>
    public KafkaSourceStart StartFrom { get; init; } = KafkaSourceStart.Earliest;

    /// <summary><c>isolation.level</c>. The default reads committed records only, which is
    /// what a topic written by a transactional producer (or by Debezium with exactly-once
    /// on) needs and costs nothing on any other.</summary>
    public IsolationLevel IsolationLevel { get; init; } = IsolationLevel.ReadCommitted;

    /// <summary>The most the client may hold of records fetched and not yet handed on
    /// (<c>queued.max.messages.kbytes</c>), in kilobytes. With the input's bounded channel
    /// this is the source's whole memory: when the circuit is slow the channel fills, the
    /// source stops polling, and the client stops fetching at this limit. Default 16 MB.</summary>
    public int FetchBufferKilobytes { get; init; } = 16 * 1024;

    /// <summary>How long a write to a full input (or a table commit) may wait before the
    /// source pauses its partitions, so that the client asks the brokers for nothing until
    /// the write goes through. Memory is bounded without it — a client that is not polled
    /// stops fetching at <see cref="FetchBufferKilobytes"/> — and a pause has a price: the
    /// client drops what it had fetched ahead when it is next polled and fetches it again.
    /// So short stalls are ridden out and only a long one pauses. Default 1 s.</summary>
    public TimeSpan PauseAfter { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>Table loader only: the most records staged before a version is committed.
    /// A version is also committed whenever the loader has read everything fetched so far,
    /// so this bounds a version's size during a bulk load and nothing else. Default 4096.</summary>
    public int MaxTableBatch { get; init; } = 4096;

    /// <summary>Table loader only: called for a record that cannot be decoded, which is
    /// then passed over. With none set (the default) such a record stops the loader with a
    /// <see cref="KafkaSourceException"/>. A circuit input needs no such hook: its
    /// unreadable records go through the circuit's <see cref="RecordErrorPolicy"/>.</summary>
    public Action<KafkaRecord, Exception>? OnUnreadableTableRecord { get; init; }

    /// <summary>Table loader only: what to do when the table's committed position is no
    /// longer in the topic (retention removed it). False (the default) fails with
    /// <see cref="KafkaSourcePositionLostException"/>. True re-reads the topic from its
    /// start and replaces the table's rows with the result
    /// (<see cref="TableWriter{TKey,TRow}.ReplaceAllAsync(IEnumerable{TRow}, IReadOnlyDictionary{int, long}?, CancellationToken)"/>),
    /// which is right for a compacted reference topic and wrong for anything else.</summary>
    public bool ReloadTableWhenPositionLost { get; init; }

    /// <summary>Table loader only: how long the loader may stand past the table's committed
    /// position, with nothing more to read, before it records where it stands
    /// (<see cref="TableWriter{TKey,TRow}.AdvanceAsync"/>, which costs a version with no
    /// changes). A table's offsets ride with the versions that change something, so
    /// offsets that changed nothing — a transaction's marker, a record passed over, an
    /// upsert of the row already there — are otherwise not recorded, and once retention
    /// has removed them a restart would find the committed position gone. A circuit input
    /// needs no such delay: its position is recorded as soon as the source reaches the end.
    /// Default 30 s.</summary>
    public TimeSpan CommitIdlePositionAfter { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>How long start-up requests (metadata, offsets) may take, retries of a
    /// partition without a leader included.</summary>
    public TimeSpan OperationTimeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>Extra librdkafka settings for the consumer, applied last: MSK's
    /// <c>security.protocol</c>, <c>sasl.mechanism</c> and so on.</summary>
    public IReadOnlyDictionary<string, string> ClientConfig { get; init; } = new Dictionary<string, string>();

    /// <summary>Access to the consumer's builder, for what a setting cannot express (an
    /// OAUTHBEARER token refresh handler for MSK IAM).</summary>
    public Action<ConsumerBuilder<byte[], byte[]>>? ConfigureConsumer { get; init; }

    public ILogger? Logger { get; init; }

    internal void Validate()
    {
        ArgumentException.ThrowIfNullOrEmpty(BootstrapServers);
        ArgumentException.ThrowIfNullOrEmpty(Topic);
        ArgumentOutOfRangeException.ThrowIfLessThan(FetchBufferKilobytes, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxTableBatch, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(PauseAfter, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThan(CommitIdlePositionAfter, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(OperationTimeout, TimeSpan.Zero);
    }
}

/// <summary>A <see cref="KafkaSource"/> failed: the topic is missing, a read failed, or a
/// table record could not be decoded.</summary>
public class KafkaSourceException(string message, Exception? innerException = null)
    : Exception(message, innerException);

/// <summary>The position a source must resume from is not in the topic: retention has
/// removed it, or the topic is not the one the position was committed against. For a fact
/// topic that is lost data and needs an operator.</summary>
public sealed class KafkaSourcePositionLostException(string message) : KafkaSourceException(message);

/// <summary>How far a source is behind its topic. <see cref="Positions"/> is the next
/// offset the source will hand on, by partition; <see cref="EndOffsets"/> is where the
/// topic's readable data ended when asked.</summary>
public sealed record KafkaSourceLag(IReadOnlyDictionary<int, long> Positions, IReadOnlyDictionary<int, long> EndOffsets)
{
    /// <summary>Offsets still to read, over all partitions. Offsets, not records: a
    /// compacted or transactional topic has offsets with no record behind them.</summary>
    public long Total => EndOffsets.Sum(end => Math.Max(0, end.Value - Positions.GetValueOrDefault(end.Key)));

    public bool IsCaughtUp => Total == 0;
}

/// <summary>
/// Reads one Kafka topic into a circuit input, a keyed circuit input, or a shared table.
/// <para>
/// <b>The position lives in the checkpoint, not in a consumer group.</b> Every record is
/// handed on stamped with its partition and offset. A circuit commits those with its state
/// (<see cref="Circuit.CommittedOffsets"/>); a table records them with each version
/// (<see cref="SharedTable{TKey,TRow}.CommittedOffsets"/>). A source started against either
/// assigns every partition itself and resumes each one record after its committed position —
/// so a restart or a takeover re-reads exactly what the checkpoint does not cover, and
/// anything re-read below it would be dropped anyway. The consumer joins no group and
/// commits no offsets, and a position that is no longer in the topic is an error
/// (<see cref="KafkaSourcePositionLostException"/>), never a silent jump. Offsets that
/// yield nothing — a transaction's marker, a tombstone on a stream of facts, a record the
/// decoder passes over — are recorded as read too
/// (<see cref="InputHandle{T}.AdvanceAsync"/>), so that the committed position follows
/// the source to the end of an idle topic and retention cannot overtake it there.
/// </para>
/// <para>
/// <b>Backpressure.</b> Records are written into the input's bounded channel one by one;
/// when the circuit is slow the write waits, the source stops polling, and the client stops
/// fetching once it holds <see cref="KafkaSourceOptions.FetchBufferKilobytes"/>. A wait
/// longer than <see cref="KafkaSourceOptions.PauseAfter"/> pauses the partitions until it
/// ends. Nothing is buffered beyond those two bounds.
/// </para>
/// <para>
/// <b>Unreadable records.</b> A record whose deserializer throws is reported through
/// <see cref="InputHandle{T}.RejectAsync"/> as a <see cref="KafkaRecord"/> — topic,
/// partition, offset, raw bytes — in the record's place: under
/// <see cref="RecordErrorPolicy.DeadLetter"/> it appears on
/// <see cref="Circuit.DeadLetters"/> and its offset is checkpointed like any other; under
/// <see cref="RecordErrorPolicy.Fault"/> it faults the circuit.
/// </para>
/// A source runs one consumer on one thread of its own. Start it after
/// <see cref="Circuit.Start"/> (the committed offsets are read then) and on the leader only.
/// </summary>
public sealed class KafkaSource : IAsyncDisposable
{
    private readonly KafkaSourceOptions _options;
    private readonly Target _target;
    private readonly CancellationTokenSource _stop = new();
    private readonly TaskCompletionSource _started = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly Thread _thread;
    private readonly object _clientGate = new();
    private IAdminClient? _admin;

    // Next offset to hand on, by partition. Written by the source's thread only.
    private long[] _next = [];
    private long _read;
    private long _unreadable;
    private long _pauses;
    private volatile bool _paused;

    private KafkaSource(KafkaSourceOptions options, Target target)
    {
        options.Validate();
        _options = options;
        _target = target;
        _thread = new Thread(Run) { IsBackground = true, Name = $"incremental-kafka-source {options.Topic}" };
    }

    /// <summary>Reads <paramref name="options"/>' topic into a circuit input: each record
    /// with a value is one insertion of the deserialized value (JSON unless
    /// <paramref name="deserializer"/> says otherwise); a record with no value is passed
    /// over. Completes once the partitions are assigned at their committed positions.</summary>
    public static Task<KafkaSource> StartAsync<T>(
        Circuit circuit, InputStream<T> input, KafkaSourceOptions options,
        KafkaDeserializer<T>? deserializer = null, CancellationToken cancellationToken = default)
        where T : notnull
    {
        ArgumentNullException.ThrowIfNull(circuit);
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(options);
        var name = input.InputName;
        return StartAsync(options, new InputTarget<T>(
            circuit.Handle(input), deserializer ?? KafkaDeserializers.Json<T>(), () => circuit.CommittedOffsets(name)), cancellationToken);
    }

    /// <summary>Reads <paramref name="options"/>' topic into a keyed circuit input: each
    /// record is an upsert, a delete of its key, or nothing, as <paramref name="decoder"/>
    /// reads it (by default <see cref="KafkaDeserializers.Keyed{TKey,TValue}"/>: a value is
    /// an upsert, a tombstone a delete, both JSON). Completes once the partitions are
    /// assigned at their committed positions.</summary>
    public static Task<KafkaSource> StartAsync<TKey, TValue>(
        Circuit circuit, KeyedInputStream<TKey, TValue> input, KafkaSourceOptions options,
        KafkaChangeDecoder<TKey, TValue>? decoder = null, CancellationToken cancellationToken = default)
        where TKey : notnull
        where TValue : notnull
    {
        ArgumentNullException.ThrowIfNull(circuit);
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(options);
        var name = input.InputName;
        return StartAsync(options, new KeyedTarget<TKey, TValue>(
            circuit.Handle(input), decoder ?? KafkaDeserializers.Keyed<TKey, TValue>(), () => circuit.CommittedOffsets(name)), cancellationToken);
    }

    /// <summary>
    /// The table loader: reads <paramref name="options"/>' topic into
    /// <paramref name="table"/> as its writer. Each change is staged with its partition and
    /// offset, so the table's own replay check drops anything read twice, and a version is
    /// committed whenever the loader has read all that has been fetched (or
    /// <see cref="KafkaSourceOptions.MaxTableBatch"/> records) — with a log behind the
    /// table, whatever arrives during one commit becomes the next version. It resumes from
    /// <see cref="SharedTable{TKey,TRow}.CommittedOffsets"/>. For the leader only: a
    /// standby's table follows its log instead, and the loader starts when the table has
    /// been promoted. Completes once the partitions are assigned.
    /// </summary>
    public static Task<KafkaSource> StartTableLoaderAsync<TKey, TRow>(
        SharedTable<TKey, TRow> table, KafkaSourceOptions options,
        KafkaChangeDecoder<TKey, TRow>? decoder = null, CancellationToken cancellationToken = default)
        where TKey : notnull
        where TRow : class
    {
        ArgumentNullException.ThrowIfNull(table);
        ArgumentNullException.ThrowIfNull(options);
        if (table.IsFollowing)
            throw new InvalidOperationException(
                $"Table '{table.Name}' is following its log (a standby): it has no writer for a loader to be. Promote the " +
                "table first; a standby's rows come from the log.");
        return StartAsync(options, new TableTarget<TKey, TRow>(
            table, decoder ?? KafkaDeserializers.Keyed<TKey, TRow>(), options.OnUnreadableTableRecord), cancellationToken);
    }

    private static async Task<KafkaSource> StartAsync(KafkaSourceOptions options, Target target, CancellationToken cancellationToken)
    {
        var source = new KafkaSource(options, target);
        source._thread.Start();
        try
        {
            await source._started.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            await source.DisposeAsync().ConfigureAwait(false);
            throw;
        }
        return source;
    }

    public string Topic => _options.Topic;

    /// <summary>Completes when the source has stopped: normally after
    /// <see cref="DisposeAsync"/> or when the circuit it feeds has been disposed, with the
    /// failure when the source failed or the circuit faulted.</summary>
    public Task Completion => _completion.Task;

    /// <summary>The next offset the source will hand on, by partition: everything below has
    /// been written to the input (or, for a table loader, committed to the table).</summary>
    public IReadOnlyDictionary<int, long> Positions
    {
        get
        {
            var next = Volatile.Read(ref _next);
            var positions = new Dictionary<int, long>(next.Length);
            for (var p = 0; p < next.Length; p++)
                positions[p] = Volatile.Read(ref next[p]);
            return positions;
        }
    }

    /// <summary>Records handed on so far, unreadable ones included.</summary>
    public long RecordsRead => Interlocked.Read(ref _read);

    /// <summary>Records that could not be deserialized.</summary>
    public long RecordsUnreadable => Interlocked.Read(ref _unreadable);

    /// <summary>How many times the source has paused its partitions because the circuit
    /// (or the table's log) could not keep up.</summary>
    public long Pauses => Interlocked.Read(ref _pauses);

    /// <summary>True while the partitions are paused.</summary>
    public bool IsPaused => _paused;

    /// <summary>Asks the cluster where the topic ends and compares that with
    /// <see cref="Positions"/>.</summary>
    public async Task<KafkaSourceLag> MeasureLagAsync(CancellationToken cancellationToken = default)
    {
        IAdminClient admin;
        lock (_clientGate)
            admin = _admin ?? throw new ObjectDisposedException(nameof(KafkaSource), "The source has stopped.");
        var count = Volatile.Read(ref _next).Length;
        var ends = await ListOffsetsAsync(admin, count, OffsetSpec.Latest(), cancellationToken).ConfigureAwait(false);
        return new KafkaSourceLag(Positions, Enumerable.Range(0, count).ToDictionary(p => p, p => ends[p]));
    }

    /// <summary>Completes when the source has handed on everything the topic held when this
    /// was called. For a circuit input that means written to the input, not yet stepped
    /// through the circuit; for a table loader it means committed to the table.</summary>
    public async Task WhenCaughtUpAsync(CancellationToken cancellationToken = default)
    {
        var ends = (await MeasureLagAsync(cancellationToken).ConfigureAwait(false)).EndOffsets;
        while (true)
        {
            var next = Volatile.Read(ref _next);
            var behind = false;
            foreach (var (partition, end) in ends)
                behind |= Volatile.Read(ref next[partition]) < end;
            if (!behind)
                return;
            if (_completion.Task.IsCompleted)
            {
                await _completion.Task.ConfigureAwait(false);
                throw new ObjectDisposedException(nameof(KafkaSource), "The source stopped before it had caught up.");
            }
            await Task.Delay(10, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Stops reading. Records already written to the input stay there; the
    /// position is wherever the circuit's checkpoint says.</summary>
    public async ValueTask DisposeAsync()
    {
        _stop.Cancel();
        if (_thread.IsAlive)
            await Task.Run(_thread.Join).ConfigureAwait(false);
        await _completion.Task.ContinueWith(static _ => { }, TaskScheduler.Default).ConfigureAwait(false);
    }

    // ── the source's thread ────────────────────────────────────────────────────────────

    private void Run()
    {
        IConsumer<byte[], byte[]>? consumer = null;
        try
        {
            consumer = CreateConsumer();
            var admin = new DependentAdminClientBuilder(consumer.Handle).Build();
            lock (_clientGate)
                _admin = admin;
            var pins = Assign(consumer, admin);

            // A table records where a first start at the end began before the loader says
            // it has started; an input takes the same through its channel, which may have
            // to wait for the circuit.
            if (_target.Batches)
                Pin(consumer, pins);
            _started.TrySetResult();
            if (!_target.Batches)
                Pin(consumer, pins);
            Loop(consumer);
            _completion.TrySetResult();
        }
        catch (Exception ex) when (ex is OperationCanceledException && _stop.IsCancellationRequested || ex is ChannelClosedException)
        {
            // Stopped, or the circuit was disposed under the source: both are an ordinary end.
            _started.TrySetException(new OperationCanceledException("The source was stopped before it had started."));
            _completion.TrySetResult();
        }
        catch (Exception ex)
        {
            var failure = ex switch
            {
                KafkaSourceException or CircuitFaultedException => ex,
                ConsumeException consume when consume.Error.Code is ErrorCode.Local_AutoOffsetReset or ErrorCode.OffsetOutOfRange =>
                    new KafkaSourcePositionLostException(
                        $"Topic '{_options.Topic}' no longer holds the position the source was reading at: {consume.Error.Reason}"),
                KafkaException kafka => new KafkaSourceException($"Reading topic '{_options.Topic}' failed: {kafka.Error.Reason}", kafka),
                _ => new KafkaSourceException($"The source of topic '{_options.Topic}' failed: {ex.Message}", ex),
            };
            _options.Logger?.LogError(failure, "Kafka source {Topic} stopped", _options.Topic);
            _started.TrySetException(failure);
            _completion.TrySetException(failure);
        }
        finally
        {
            lock (_clientGate)
            {
                _admin?.Dispose();
                _admin = null;
            }
            try
            {
                consumer?.Close();
                consumer?.Dispose();
            }
            catch (KafkaException)
            {
            }
        }
    }

    private IConsumer<byte[], byte[]> CreateConsumer()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            GroupId = "incremental.source", // required by the client; never joined
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            IsolationLevel = _options.IsolationLevel,
            EnablePartitionEof = true,
            // A position that no longer exists is an error, never a silent jump.
            AutoOffsetReset = AutoOffsetReset.Error,
            QueuedMaxMessagesKbytes = _options.FetchBufferKilobytes,
            // When the fetch buffer is full the client postpones its next fetch by this
            // long, and its default is a whole second: a source that drains its buffer
            // faster than once a second would be held to one buffer a second.
            FetchQueueBackoffMs = 10,
        };
        foreach (var (key, value) in _options.ClientConfig)
            config.Set(key, value);
        var builder = new ConsumerBuilder<byte[], byte[]>(config)
            .SetLogHandler((_, message) => _options.Logger?.LogDebug("{Client} {Facility}: {Message}", message.Name, message.Facility, message.Message))
            .SetErrorHandler((_, error) => _options.Logger?.Log(
                error.IsFatal ? LogLevel.Error : LogLevel.Warning, "Kafka source {Topic} error {Code}: {Reason}", _options.Topic, error.Code, error.Reason));
        _options.ConfigureConsumer?.Invoke(builder);
        return builder.Build();
    }

    /// <summary>Works out where each partition starts and assigns them all. Every start is
    /// a number: a consumer assigned at a logical offset waits half a second before it
    /// resolves it (durability-and-standby.md, <i>A logical start offset costs half a
    /// second</i>). Returns the positions a first start at the end begins from, which are
    /// to be recorded.</summary>
    private List<(int Partition, long Offset)> Assign(IConsumer<byte[], byte[]> consumer, IAdminClient admin)
    {
        var count = PartitionCount(consumer, admin);
        var starts = Pump(consumer, ListOffsetsAsync(admin, count, OffsetSpec.Earliest(), _stop.Token));
        var ends = Pump(consumer, ListOffsetsAsync(admin, count, OffsetSpec.Latest(), _stop.Token));
        var committed = _target.CommittedOffsets();

        foreach (var partition in committed.Keys)
            if (partition >= count)
                throw new KafkaSourcePositionLostException(
                    $"A position is committed for partition {partition} of topic '{_options.Topic}', which has only {count} " +
                    "partition(s): this is not the topic the position was committed against.");

        // "The end" is taken once: at the start that finds nothing committed at all. Taken
        // again at every start it would skip whatever a quiet partition received while the
        // source was down, and whatever had been read and not yet checkpointed.
        var atEnd = _options.StartFrom == KafkaSourceStart.Latest && committed.Count == 0;
        var pins = new List<(int Partition, long Offset)>();
        var next = new long[count];
        var lost = new List<string>();
        for (var p = 0; p < count; p++)
        {
            if (!committed.TryGetValue(p, out var position))
            {
                next[p] = atEnd ? ends[p] : starts[p];
                if (atEnd && ends[p] > 0)
                    pins.Add((p, ends[p] - 1));
                continue;
            }
            next[p] = position + 1;
            if (next[p] > ends[p])
                throw new KafkaSourcePositionLostException(
                    $"Partition {p} of topic '{_options.Topic}' ends at offset {ends[p]}, below the committed position {position}: " +
                    "this is not the topic the position was committed against (it was recreated, or the source points at another cluster).");
            if (next[p] < starts[p])
                lost.Add($"partition {p}: committed {position}, first retained {starts[p]}");
        }

        if (lost.Count > 0)
        {
            if (!(_options.ReloadTableWhenPositionLost && _target.CanReload))
                throw new KafkaSourcePositionLostException(
                    $"Topic '{_options.Topic}' no longer holds the records after the committed position ({string.Join("; ", lost)}). " +
                    "For a fact topic that is lost data; a compacted reference topic can be re-read from its start " +
                    "(a table loader does that with ReloadTableWhenPositionLost).");
            _options.Logger?.LogWarning(
                "Kafka source {Topic}: the committed position is gone ({Lost}); re-reading the topic to replace the table's rows",
                _options.Topic, string.Join("; ", lost));
            Reload(consumer, starts, ends);
            next = ends;
        }

        Volatile.Write(ref _next, next);
        consumer.Assign(next.Select((offset, p) => new TopicPartitionOffset(_options.Topic, p, offset)).ToList());
        return pins;
    }

    private void Pin(IConsumer<byte[], byte[]> consumer, List<(int Partition, long Offset)> pins)
    {
        if (pins.Count > 0)
            Await(consumer, _target.AdvanceAsync(pins, _stop.Token));
    }

    /// <summary>Waits for a start-up request on the source's thread while polling the
    /// consumer, which has nothing assigned yet and so returns nothing. The polling is the
    /// point: a consumer serves its callbacks only when polled, an OAUTHBEARER token
    /// refresh among them, and without a token (MSK with IAM) no request on its
    /// connections — these included — is ever answered.</summary>
    private T Pump<T>(IConsumer<byte[], byte[]> consumer, Task<T> request)
    {
        while (!request.IsCompleted)
        {
            _stop.Token.ThrowIfCancellationRequested();
            consumer.Consume(TimeSpan.FromMilliseconds(20));
        }
        return request.GetAwaiter().GetResult();
    }

    private int PartitionCount(IConsumer<byte[], byte[]> consumer, IAdminClient admin)
    {
        // A topic created a moment ago can be unknown to the broker asked, so an unknown
        // topic is asked about again for a few seconds; one that stays unknown does not
        // exist. The source never creates its topic.
        var patience = Math.Min(5000, (long)_options.OperationTimeout.TotalMilliseconds);
        var deadline = Environment.TickCount64 + patience;
        while (true)
        {
            _stop.Token.ThrowIfCancellationRequested();
            var metadata = Pump(consumer, Task.Run(() => admin.GetMetadata(_options.Topic, _options.OperationTimeout)));
            var topic = metadata.Topics.FirstOrDefault(t => t.Topic == _options.Topic);
            if (topic is { Partitions.Count: > 0 } && !topic.Error.IsError)
                return topic.Partitions.Count;
            var unknown = topic is null || topic.Error.Code is ErrorCode.UnknownTopicOrPart or ErrorCode.LeaderNotAvailable or ErrorCode.NoError;
            if (!unknown || Environment.TickCount64 > deadline)
                throw new KafkaSourceException(
                    $"Topic '{_options.Topic}' cannot be read: {(unknown ? "it does not exist" : topic!.Error.Reason)}. A source does not create its topic.");
            Thread.Sleep(100);
        }
    }

    /// <summary>One offset per partition, asked of the cluster. A partition between
    /// leaders, or one whose leader the client does not know yet, is asked about again for
    /// up to the operation timeout: that passes, and a source that gave up on it would
    /// fail a start — or a takeover — over nothing.</summary>
    private Task<long[]> ListOffsetsAsync(IAdminClient admin, int partitions, OffsetSpec spec, CancellationToken cancellationToken) =>
        KafkaLog.RetryTransientAsync(async () =>
        {
            var result = await admin.ListOffsetsAsync(
                Enumerable.Range(0, partitions).Select(p => new TopicPartitionOffsetSpec
                {
                    TopicPartition = new TopicPartition(_options.Topic, p),
                    OffsetSpec = spec,
                }),
                new ListOffsetsOptions { IsolationLevel = _options.IsolationLevel, RequestTimeout = _options.OperationTimeout })
                .ConfigureAwait(false);
            var offsets = new long[partitions];
            foreach (var info in result.ResultInfos)
                offsets[info.TopicPartitionOffsetError.Partition.Value] = info.TopicPartitionOffsetError.Offset.Value;
            return offsets;
        }, _options.OperationTimeout, cancellationToken);

    /// <summary>Reads the whole topic as it stands and replaces the table's rows with what
    /// it says, recording the ends as the table's position.</summary>
    private void Reload(IConsumer<byte[], byte[]> consumer, long[] starts, long[] ends)
    {
        var position = (long[])starts.Clone();
        consumer.Assign(starts.Select((offset, p) => new TopicPartitionOffset(_options.Topic, p, offset)).ToList());
        _target.BeginReload();
        while (Enumerable.Range(0, ends.Length).Any(p => position[p] < ends[p]))
        {
            _stop.Token.ThrowIfCancellationRequested();
            var result = consumer.Consume(TimeSpan.FromMilliseconds(100));
            if (result is null)
                continue;
            var p = result.Partition.Value;
            if (result.IsPartitionEOF || result.Offset.Value >= ends[p])
            {
                position[p] = Math.Max(position[p], Math.Min(result.Offset.Value, ends[p]));
                if (!result.IsPartitionEOF)
                    position[p] = ends[p];
                continue;
            }
            var record = new KafkaRecord(result);
            _target.ReloadRecord(in record);
            Interlocked.Increment(ref _read);
            position[p] = result.Offset.Value + 1;
        }
        var offsets = new Dictionary<int, long>();
        for (var p = 0; p < ends.Length; p++)
            if (ends[p] > 0)
                offsets[p] = ends[p] - 1;
        Await(consumer, _target.FinishReloadAsync(offsets, _stop.Token));
    }

    private void Loop(IConsumer<byte[], byte[]> consumer)
    {
        var token = _stop.Token;
        var next = _next;
        var staged = (long[])next.Clone(); // where each partition stands once what is staged commits

        // The last offset of each partition whose position the target has been given, and
        // since when the source has stood past what the target has recorded.
        var handed = new long[next.Length];
        for (var p = 0; p < handed.Length; p++)
            handed[p] = next[p] - 1;
        var aheadSince = new long[next.Length];

        while (!token.IsCancellationRequested)
        {
            // With changes staged, do not wait for more: commit them as soon as the client
            // has nothing fetched.
            var result = consumer.Consume(_target.Staged > 0 ? TimeSpan.Zero : TimeSpan.FromMilliseconds(100));
            if (result is null || result.IsPartitionEOF)
            {
                // The end-of-partition event carries the position past whatever follows
                // the last record: its transaction's marker, say.
                if (result is not null)
                    staged[result.Partition.Value] = Math.Max(staged[result.Partition.Value], result.Offset.Value);
                Flush(consumer, next, staged);
                RecordIdlePositions(consumer, staged, handed, aheadSince);
                continue;
            }

            var record = new KafkaRecord(result);
            Await(consumer, _target.HandleAsync(in record, ref _unreadable, token));
            Interlocked.Increment(ref _read);
            staged[record.Partition] = record.Offset + 1;
            handed[record.Partition] = record.Offset;
            if (!_target.Batches)
                Volatile.Write(ref next[record.Partition], record.Offset + 1);
            else if (_target.Staged >= _options.MaxTableBatch)
                Flush(consumer, next, staged);
        }
        token.ThrowIfCancellationRequested();
    }

    private void Flush(IConsumer<byte[], byte[]> consumer, long[] next, long[] staged)
    {
        if (_target.Staged > 0)
            Await(consumer, _target.FlushAsync(_stop.Token));
        for (var p = 0; p < next.Length; p++)
            Volatile.Write(ref next[p], staged[p]);
    }

    /// <summary>With nothing left to read, tells the target how far each partition has been
    /// read wherever that is past what the target has recorded: offsets that held no
    /// record, or records that came to nothing. Without it the committed position would
    /// stay behind at the last record that counted, and once retention had removed the
    /// offsets after it a restart would report the position lost though nothing was.</summary>
    private void RecordIdlePositions(IConsumer<byte[], byte[]> consumer, long[] staged, long[] handed, long[] aheadSince)
    {
        List<(int Partition, long Offset)>? due = null;
        for (var p = 0; p < staged.Length; p++)
        {
            if (staged[p] - 1 <= _target.Recorded(p, handed[p]))
            {
                aheadSince[p] = 0;
                continue;
            }

            // A table pays for this with a version, so it waits to see that the topic has
            // really gone quiet; an input pays with a channel entry and does not wait.
            if (_target.Batches)
            {
                var now = Environment.TickCount64;
                if (aheadSince[p] == 0)
                    aheadSince[p] = now;
                if (now - aheadSince[p] < (long)_options.CommitIdlePositionAfter.TotalMilliseconds)
                    continue;
            }
            (due ??= []).Add((p, staged[p] - 1));
        }
        if (due is null)
            return;

        Await(consumer, _target.AdvanceAsync(due, _stop.Token));
        foreach (var (partition, offset) in due)
        {
            handed[partition] = offset;
            aheadSince[partition] = 0;
        }
    }

    /// <summary>Waits for a write on the source's thread. One that takes longer than
    /// <see cref="KafkaSourceOptions.PauseAfter"/> pauses the partitions meanwhile: the
    /// client fetches nothing, drops what it had fetched ahead at its next poll, and
    /// resumes after the last record handed on.</summary>
    private void Await(IConsumer<byte[], byte[]> consumer, ValueTask write)
    {
        if (write.IsCompleted)
        {
            write.GetAwaiter().GetResult();
            return;
        }

        var task = write.AsTask();
        if (((IAsyncResult)task).AsyncWaitHandle.WaitOne(_options.PauseAfter))
        {
            task.GetAwaiter().GetResult();
            return;
        }

        var assignment = consumer.Assignment;
        consumer.Pause(assignment);
        _paused = true;
        Interlocked.Increment(ref _pauses);
        try
        {
            task.GetAwaiter().GetResult();
        }
        finally
        {
            _paused = false;
        }
        consumer.Resume(assignment);
    }

    // ── where the records go ───────────────────────────────────────────────────────────

    private abstract class Target
    {
        public abstract IReadOnlyDictionary<int, long> CommittedOffsets();

        /// <summary>Decodes the record and hands it on. A record that cannot be decoded is
        /// counted in <paramref name="unreadable"/> and dealt with as the target does.</summary>
        public abstract ValueTask HandleAsync(in KafkaRecord record, ref long unreadable, CancellationToken cancellationToken);

        /// <summary>Records that the source has read up to and including each offset, with
        /// nothing (more) to hand on for it.</summary>
        public abstract ValueTask AdvanceAsync(IReadOnlyList<(int Partition, long Offset)> positions, CancellationToken cancellationToken);

        /// <summary>The last offset of <paramref name="partition"/> the target has on
        /// record, given the last one the source handed it (<paramref name="handed"/>).</summary>
        public virtual long Recorded(int partition, long handed) => handed;

        /// <summary>True for a target that stages records and commits them in batches.</summary>
        public virtual bool Batches => false;

        public virtual int Staged => 0;

        public virtual ValueTask FlushAsync(CancellationToken cancellationToken) => default;

        public virtual bool CanReload => false;

        public virtual void BeginReload() => throw new NotSupportedException();

        public virtual void ReloadRecord(in KafkaRecord record) => throw new NotSupportedException();

        public virtual ValueTask FinishReloadAsync(IReadOnlyDictionary<int, long> offsets, CancellationToken cancellationToken) =>
            throw new NotSupportedException();
    }

    private static bool IsStop(Exception exception) =>
        exception is ChannelClosedException or CircuitFaultedException or OperationCanceledException;

    private sealed class InputTarget<T>(
        InputHandle<T> handle, KafkaDeserializer<T> deserializer, Func<IReadOnlyDictionary<int, long>> committed) : Target
        where T : notnull
    {
        public override IReadOnlyDictionary<int, long> CommittedOffsets() => committed();

        public override ValueTask HandleAsync(in KafkaRecord record, ref long unreadable, CancellationToken cancellationToken)
        {
            // A tombstone means nothing on a stream of facts — but it has been read.
            if (record.Value is not { } bytes)
                return handle.AdvanceAsync(record.Partition, record.Offset, cancellationToken);

            T value;
            try
            {
                value = deserializer(bytes, in record);
                if (RecordErrors.IsNull(value))
                    throw new FormatException("The deserializer returned null.");
            }
            catch (Exception ex)
            {
                Interlocked.Increment(ref unreadable);
                return handle.RejectAsync(record, ex, record.Partition, record.Offset, cancellationToken);
            }
            return handle.InsertAsync(value, record.Partition, record.Offset, cancellationToken);
        }

        public override async ValueTask AdvanceAsync(IReadOnlyList<(int Partition, long Offset)> positions, CancellationToken cancellationToken)
        {
            foreach (var (partition, offset) in positions)
                await handle.AdvanceAsync(partition, offset, cancellationToken).ConfigureAwait(false);
        }
    }

    private sealed class KeyedTarget<TKey, TValue>(
        KeyedInputHandle<TKey, TValue> handle, KafkaChangeDecoder<TKey, TValue> decoder,
        Func<IReadOnlyDictionary<int, long>> committed) : Target
        where TKey : notnull
        where TValue : notnull
    {
        public override IReadOnlyDictionary<int, long> CommittedOffsets() => committed();

        public override ValueTask HandleAsync(in KafkaRecord record, ref long unreadable, CancellationToken cancellationToken)
        {
            try
            {
                var change = decoder(in record);
                if (change.IsSkip)
                    return handle.AdvanceAsync(record.Partition, record.Offset, cancellationToken);
                if (change.IsDelete)
                {
                    if (RecordErrors.IsNull(change.Key))
                        throw new FormatException("The decoder returned a delete with a null key.");
                    return handle.DeleteAsync(change.Key, record.Partition, record.Offset, cancellationToken);
                }
                if (RecordErrors.IsNull(change.Value))
                    throw new FormatException("The decoder returned an upsert with a null row.");
                // Under RecordErrorPolicy.Fault the input's key selector runs here and may throw.
                return handle.UpsertAsync(change.Value, record.Partition, record.Offset, cancellationToken);
            }
            catch (Exception ex) when (!IsStop(ex))
            {
                Interlocked.Increment(ref unreadable);
                return handle.RejectAsync(record, ex, record.Partition, record.Offset, cancellationToken);
            }
        }

        public override async ValueTask AdvanceAsync(IReadOnlyList<(int Partition, long Offset)> positions, CancellationToken cancellationToken)
        {
            foreach (var (partition, offset) in positions)
                await handle.AdvanceAsync(partition, offset, cancellationToken).ConfigureAwait(false);
        }
    }

    private sealed class TableTarget<TKey, TRow>(
        SharedTable<TKey, TRow> table, KafkaChangeDecoder<TKey, TRow> decoder, Action<KafkaRecord, Exception>? onUnreadable) : Target
        where TKey : notnull
        where TRow : class
    {
        private int _staged;
        private Dictionary<TKey, TRow>? _reload;

        public override IReadOnlyDictionary<int, long> CommittedOffsets() => table.CommittedOffsets;

        public override bool Batches => true;

        public override int Staged => _staged;

        // What the table has committed, not what it was handed: an upsert of the row
        // already there is handed over and recorded nowhere.
        public override long Recorded(int partition, long handed) => table.CommittedOffsets.GetValueOrDefault(partition, -1);

        public override async ValueTask AdvanceAsync(IReadOnlyList<(int Partition, long Offset)> positions, CancellationToken cancellationToken)
        {
            foreach (var (partition, offset) in positions)
                await table.Writer.AdvanceAsync(partition, offset).ConfigureAwait(false);
            await table.Writer.CommitAsync(cancellationToken).ConfigureAwait(false);
            _staged = 0;
        }

        public override ValueTask HandleAsync(in KafkaRecord record, ref long unreadable, CancellationToken cancellationToken)
        {
            if (!TryDecode(in record, ref unreadable, out var change) || change.IsSkip)
                return default;
            _staged++;
            return change.IsDelete
                ? table.Writer.DeleteAsync(change.Key, record.Partition, record.Offset)
                : table.Writer.UpsertAsync(change.Value, record.Partition, record.Offset);
        }

        private bool TryDecode(in KafkaRecord record, ref long unreadable, out KafkaChange<TKey, TRow> change)
        {
            try
            {
                change = decoder(in record);
                if (change.IsDelete && change.Key is null)
                    throw new FormatException("The decoder returned a delete with a null key.");
                if (change.IsUpsert && change.Value is null)
                    throw new FormatException("The decoder returned an upsert with a null row.");
                return true;
            }
            catch (Exception ex)
            {
                Interlocked.Increment(ref unreadable);
                if (onUnreadable is null)
                    throw new KafkaSourceException(
                        $"Table '{table.Name}' cannot be loaded past {record}: {ex.Message} Set OnUnreadableTableRecord to pass such records over.", ex);
                onUnreadable(record, ex);
                change = default;
                return false;
            }
        }

        public override async ValueTask FlushAsync(CancellationToken cancellationToken)
        {
            await table.Writer.CommitAsync(cancellationToken).ConfigureAwait(false);
            _staged = 0;
        }

        public override bool CanReload => true;

        public override void BeginReload() => _reload = [];

        public override void ReloadRecord(in KafkaRecord record)
        {
            long unreadable = 0;
            if (!TryDecode(in record, ref unreadable, out var change) || change.IsSkip)
                return;
            if (change.IsDelete)
                _reload!.Remove(change.Key);
            else
                _reload![table.KeySelector(change.Value)] = change.Value;
        }

        public override async ValueTask FinishReloadAsync(IReadOnlyDictionary<int, long> offsets, CancellationToken cancellationToken)
        {
            await table.Writer.ReplaceAllAsync(_reload!.Values, offsets, cancellationToken).ConfigureAwait(false);
            _reload = null;
        }
    }
}
