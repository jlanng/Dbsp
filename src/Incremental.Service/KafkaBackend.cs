using System.Globalization;
using System.Net;
using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Incremental.Kafka;
using Incremental.Storage;
using Incremental.Tables;
using Microsoft.Extensions.Logging;

namespace Incremental.Service;

/// <summary>Settings of a <see cref="KafkaServiceBackend"/>: the log, and how the two
/// nodes decide which of them leads.</summary>
public sealed class KafkaServiceBackendOptions
{
    /// <summary>The log every node of the deployment shares: one topic prefix, one
    /// <c>transactional.id</c>. <see cref="KafkaLogOptions.NodeId"/> is this node's name
    /// and must differ between the two nodes.</summary>
    public required KafkaLogOptions Log { get; init; }

    /// <inheritdoc cref="KafkaLeadershipOptions.TakeoverAfter"/>
    public TimeSpan TakeoverAfter { get; init; } = TimeSpan.FromSeconds(3);

    /// <inheritdoc cref="KafkaLeadershipOptions.TakeoverCeiling"/>
    public TimeSpan TakeoverCeiling { get; init; } = TimeSpan.FromSeconds(30);

    /// <inheritdoc cref="KafkaLeadershipOptions.LeaseMargin"/>
    public TimeSpan? LeaseMargin { get; init; }

    /// <inheritdoc cref="KafkaLeadershipOptions.AutomaticTakeover"/>
    public bool AutomaticTakeover { get; init; } = true;

    /// <summary>Where this node answers its peer's probe (<see cref="LeadershipProbe"/>):
    /// its role, term, name and whether it is ready to lead. Null: it does not listen.</summary>
    public IPEndPoint? ProbeListen { get; init; }

    /// <summary>Where the other node listens. With it a standby does not take over from a
    /// leader that still answers (until <see cref="TakeoverCeiling"/>), and a leader knows
    /// whether a ready standby exists. Null: the peer is unknown, and every check that asks
    /// "is there a ready standby" answers no.</summary>
    public (string Host, int Port)? PeerProbe { get; init; }

    /// <summary>How long one probe of the peer may take.</summary>
    public TimeSpan ProbeTimeout { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>Versions of history a table's log keeps whatever the retention floor says.
    /// History is disk here, so the default is large.</summary>
    public int RetainedHistoryVersions { get; init; } = 100_000;
}

/// <summary>
/// The backend on Kafka: one <see cref="KafkaLog"/> (the state changelog and every table
/// log, written through one fenced transactional producer), one
/// <see cref="KafkaLeadership"/> (heartbeats in <c>{prefix}.leader</c>, takeover on
/// silence, the delivery lease) and a compacted topic of plugin blobs,
/// <c>{prefix}.svc.blobs</c>. Two processes built with the same log options and different
/// node ids are an active and its warm standby.
/// </summary>
public sealed class KafkaServiceBackend : IServiceBackend
{
    private readonly KafkaServiceBackendOptions _options;
    private readonly ILogger _logger;
    private readonly NodeLeadership _leadership;
    private readonly KafkaBlobStore _blobs;
    private KafkaLog? _log;
    private int _disposed;

    public KafkaServiceBackend(KafkaServiceBackendOptions options, ILoggerFactory? loggerFactory = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(options.Log);
        ArgumentOutOfRangeException.ThrowIfLessThan(options.RetainedHistoryVersions, 1);
        _options = options;
        _logger = (ILogger?)loggerFactory?.CreateLogger("Incremental.Service.Kafka") ?? options.Log.Logger
            ?? Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance;

        // The log speaks too — an aborted transaction, a commit that is not coming back —
        // and a host that gave it no logger of its own would never hear it.
        options.Log.Logger ??= loggerFactory?.CreateLogger("Incremental.Kafka");
        _leadership = new NodeLeadership(this);
        _blobs = new KafkaBlobStore(options.Log, NodeId, _logger);
    }

    public KafkaServiceBackendOptions Options => _options;

    public string NodeId => _log?.NodeId ?? _options.Log.NodeId ?? $"{Environment.MachineName}-{Environment.ProcessId}";

    /// <summary>The log, once connected: a Kafka sink that should write through the fenced
    /// transactional producer asks for it (<see cref="KafkaSinkOptions.Log"/>).</summary>
    public KafkaLog Log => _log ?? throw new InvalidOperationException("The Kafka backend is not connected yet.");

    /// <summary>The leadership, once the node has started.</summary>
    public KafkaLeadership? KafkaLeadership => _leadership.Inner;

    public IStateChangelog State => Log.State;
    public int RetainedHistoryVersions => _options.RetainedHistoryVersions;
    public IBlobStore Blobs => _blobs;
    public INodeLeadership Leadership => _leadership;

    public async Task ConnectAsync(CancellationToken cancellationToken)
    {
        if (_log is not null)
            return;
        _log = await KafkaLog.ConnectAsync(_options.Log, cancellationToken).ConfigureAwait(false);
    }

    public async Task<ITableLog<TKey, TRow>> OpenTableLogAsync<TKey, TRow>(string name, CancellationToken cancellationToken)
        where TKey : notnull
        where TRow : class =>
        await Log.OpenTableLogAsync<TKey, TRow>(name, cancellationToken).ConfigureAwait(false);

    public BackendInfo Describe()
    {
        var log = _options.Log;
        var settings = new Dictionary<string, string>
        {
            ["bootstrapServers"] = log.BootstrapServers,
            ["topicPrefix"] = log.TopicPrefix,
            ["nodeId"] = NodeId,
            ["heartbeatInterval"] = log.HeartbeatInterval.ToString(),
            ["takeoverAfter"] = _options.TakeoverAfter.ToString(),
            ["takeoverCeiling"] = _options.TakeoverCeiling.ToString(),
            ["transactionTimeout"] = log.TransactionTimeout.ToString(),
            ["automaticTakeover"] = _options.AutomaticTakeover.ToString(),
            ["statePartitions"] = log.StatePartitions.ToString(CultureInfo.InvariantCulture),
            ["retainedHistoryVersions"] = RetainedHistoryVersions.ToString(CultureInfo.InvariantCulture),
            ["probeListen"] = _options.ProbeListen?.ToString() ?? "",
            ["peerProbe"] = _options.PeerProbe is { } peer ? $"{peer.Host}:{peer.Port}" : "",
        };
        var counters = new Dictionary<string, long>();
        if (_log is { } connected)
        {
            counters["transactionsCommitted"] = connected.TransactionsCommitted;
            counters["transactionsAborted"] = connected.TransactionsAborted;
            counters["commitsCompleted"] = connected.CommitsCompleted;
            counters["recordsWritten"] = connected.RecordsWritten;
            counters["idleHeartbeats"] = connected.IdleHeartbeats;
            counters["writerResets"] = connected.WriterResets;
        }
        if (_leadership.Inner is { } leadership)
        {
            counters["takeoversAbandoned"] = leadership.TakeoversAbandoned;
            counters["takeoversHeld"] = leadership.TakeoversHeld;
        }
        counters["blobsIndexed"] = _blobs.Indexed;
        return new BackendInfo("kafka", settings, counters);
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;
        await _leadership.DisposeAsync().ConfigureAwait(false);
        _blobs.Dispose();
        if (_log is { } log)
            await log.DisposeAsync().ConfigureAwait(false);
    }

    // ── leadership ─────────────────────────────────────────────────────────────────────

    /// <summary>The seam's leadership over <see cref="Incremental.Kafka.KafkaLeadership"/>.
    /// The service promotes its tables itself, in <c>PrepareAsync</c>, so only the storage
    /// is tracked.</summary>
    private sealed class NodeLeadership(KafkaServiceBackend backend) : INodeLeadership, IAsyncDisposable
    {
        private readonly List<ChangelogStorage> _storages = [];
        private readonly TaskCompletionSource _finished = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private KafkaLeadership? _inner;
        private IAsyncDisposable? _listener;

        public KafkaLeadership? Inner => Volatile.Read(ref _inner);

        public event Action<NodeLeadershipStatus>? Changed;
        public Task WhenFinished => _finished.Task;
        public long Term => backend._log?.Term ?? 0;
        public bool PeerExpected => backend._options.PeerProbe is not null;

        public NodeLeadershipStatus Status =>
            Inner is { } inner ? Map(inner.Status) : new NodeLeadershipStatus(backend.NodeId, NodeRole.Standby, 0, null, 0);

        private static NodeLeadershipStatus Map(LeadershipStatus status) => new(
            status.NodeId, status.Role, status.Term, status.LeaderNodeId, status.LeaderTerm,
            new KafkaLeadershipDetail(
                status.HeartbeatAge, status.LeaderHeartbeatAt, status.LeaderResigned, status.HoldsLease, status.LastTakeover,
                status.Fault is { } fault ? $"{fault.GetType().Name}: {fault.Message}" : null)) { TakeoverFailed = status.TakeoverFailed };

        public void Track(ChangelogStorage storage)
        {
            ArgumentNullException.ThrowIfNull(storage);
            lock (_storages)
            {
                if (_inner is not null)
                    throw new InvalidOperationException("Track the storage before the leadership is started.");
                _storages.Add(storage);
            }
        }

        public async Task StartAsync(LeadershipCallbacks callbacks, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(callbacks);
            var options = backend._options;
            var inner = new KafkaLeadership(backend.Log, new KafkaLeadershipOptions
            {
                TakeoverAfter = options.TakeoverAfter,
                TakeoverCeiling = options.TakeoverCeiling,
                LeaseMargin = options.LeaseMargin,
                AutomaticTakeover = options.AutomaticTakeover,
                ProbeLeader = options.PeerProbe is { } peer ? LeadershipProbe.Tcp(peer.Host, peer.Port) : null,
                ProbeTimeout = options.ProbeTimeout,
                LagSampleInterval = null, // the node samples what it shows
                PrepareAsync = callbacks.PrepareAsync,
                StartLeadingAsync = callbacks.StartLeadingAsync,
                StopLeadingAsync = callbacks.StopLeadingAsync,
                Logger = backend._logger,
            });
            lock (_storages)
            {
                if (_inner is not null)
                    throw new InvalidOperationException("This leadership has already been started.");
                foreach (var storage in _storages)
                    inner.Track(storage);
                Volatile.Write(ref _inner, inner);
            }
            inner.Changed += status => Changed?.Invoke(Map(status));
            _ = inner.WhenFinished.ContinueWith(_ => _finished.TrySetResult(), TaskScheduler.Default);

            if (options.ProbeListen is { } listen)
                _listener = LeadershipProbe.Listen(inner, listen, () => PeerDetail.Write(callbacks.DescribeSelf()));
            await inner.StartAsync(cancellationToken).ConfigureAwait(false);
        }

        private KafkaLeadership Started => Inner ?? throw new InvalidOperationException("The leadership has not been started.");

        public async Task TakeOverAsync(CancellationToken cancellationToken)
        {
            try
            {
                await Started.TakeOverAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (KafkaLogNotAcquiredException ex)
            {
                // Another node took the log first. This one has promoted nothing and
                // stands by again: the request failed, the node did not.
                throw new ServiceException("takeover-lost", ex.Message, 409);
            }
        }

        public Task StepDownAsync(CancellationToken cancellationToken) => Started.StepDownAsync(cancellationToken);

        public async ValueTask<long[]?> MeasureStateLagAsync(CancellationToken cancellationToken)
        {
            if (Inner is not { Role: NodeRole.Standby } inner)
                return null;
            var lag = await inner.MeasureLagAsync(cancellationToken).ConfigureAwait(false);
            return [.. lag.StateOffsets];
        }

        public async ValueTask<PeerInfo?> ProbePeerAsync(CancellationToken cancellationToken)
        {
            if (backend._options.PeerProbe is not { } peer)
                return null;
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(backend._options.ProbeTimeout);
            LeadershipProbeAnswer? answer;
            try
            {
                answer = await LeadershipProbe.QueryAsync(peer.Host, peer.Port, timeout.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                return null; // it accepts and does not answer: stopped, or hung
            }
            if (answer is null)
                return null;
            return PeerDetail.Read(answer.NodeId, answer.Role, answer.Term, answer.Detail);
        }

        public IUpsertSink<TKey, TRow> Leased<TKey, TRow>(IUpsertSink<TKey, TRow> sink) where TKey : notnull => Started.Leased(sink);

        public IAppendSink<T> Leased<T>(IAppendSink<T> sink) => Started.Leased(sink);

        public async ValueTask DisposeAsync()
        {
            if (_listener is { } listener)
                await listener.DisposeAsync().ConfigureAwait(false);
            if (Inner is { } inner)
                await inner.DisposeAsync().ConfigureAwait(false);
        }
    }
}

/// <summary>
/// Plugin files on Kafka: the compacted topic <c>{prefix}.svc.blobs</c>, one partition. A
/// blob is its bytes in 256 KB records keyed <c>{sha256}/{n}</c> and a last record
/// <c>{sha256}/end</c> carrying the count. It is written by a plain idempotent producer
/// (<c>acks=all</c>), not the log's transactional one: the key is the content's hash, so a
/// deposed leader can only write bytes that are already correct, and a partial upload is a
/// blob with no <c>end</c> record, which reads as absent. Nothing holds a blob on the heap:
/// the store keeps an index of where each record is and reads a blob record by record into
/// the caller's stream.
/// </summary>
internal sealed class KafkaBlobStore : IBlobStore, IDisposable
{
    internal const int ChunkBytes = 256 * 1024;
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(30);

    private readonly KafkaLogOptions _log;
    private readonly string _nodeId;
    private readonly ILogger _logger;
    private readonly TopicPartition _partition;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly Dictionary<string, Entry> _index = new(StringComparer.Ordinal);
    private IProducer<byte[], byte[]>? _producer;
    private IConsumer<byte[], byte[]>? _consumer;
    private long _scanned;
    private bool _ensured;
    private bool _disposed;

    private sealed class Entry
    {
        public readonly Dictionary<int, long> Chunks = [];
        public int Count = -1;

        public bool IsComplete => Count >= 0 && Enumerable.Range(0, Count).All(Chunks.ContainsKey);
    }

    public KafkaBlobStore(KafkaLogOptions log, string nodeId, ILogger logger)
    {
        _log = log;
        _nodeId = nodeId;
        _logger = logger;
        _partition = new TopicPartition(log.TopicPrefix + ".svc.blobs", 0);
    }

    public string Topic => _partition.Topic;

    public long Indexed
    {
        get
        {
            lock (_index)
                return _index.Count;
        }
    }

    private T Apply<T>(T config) where T : ClientConfig
    {
        config.BootstrapServers = _log.BootstrapServers;
        KafkaClientDefaults.Apply(config);
        foreach (var (key, value) in _log.ClientConfig)
            config.Set(key, value);
        return config;
    }

    private async Task<IDisposable> EnterAsync(CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (!_ensured)
            {
                await Task.Run(EnsureTopic, cancellationToken).ConfigureAwait(false);
                _ensured = true;
            }
            await Task.Run(() => Refresh(cancellationToken), cancellationToken).ConfigureAwait(false);
            return new Releaser(_gate);
        }
        catch
        {
            _gate.Release();
            throw;
        }
    }

    private sealed class Releaser(SemaphoreSlim gate) : IDisposable
    {
        public void Dispose() => gate.Release();
    }

    private void EnsureTopic()
    {
        var builder = new AdminClientBuilder(Apply(new AdminClientConfig()));
        _log.ConfigureAdmin?.Invoke(builder);
        using var admin = builder.Build();
        try
        {
            admin.CreateTopicsAsync(
            [
                new TopicSpecification
                {
                    Name = Topic,
                    NumPartitions = 1,
                    ReplicationFactor = _log.ReplicationFactor,
                    // Unclean election off, as for every topic the log creates
                    // (KafkaLog.EnsureTopicAsync): a blob is state too.
                    Configs = new Dictionary<string, string> { ["cleanup.policy"] = "compact", [KafkaLog.UncleanElection] = "false" },
                },
            ], new CreateTopicsOptions { RequestTimeout = Patience, OperationTimeout = Patience }).GetAwaiter().GetResult();
        }
        catch (CreateTopicsException ex) when (ex.Results.All(r => r.Error.Code is ErrorCode.TopicAlreadyExists or ErrorCode.NoError))
        {
        }
    }

    private IConsumer<byte[], byte[]> Consumer()
    {
        if (_consumer is not null)
            return _consumer;
        var builder = new ConsumerBuilder<byte[], byte[]>(Apply(new ConsumerConfig
        {
            GroupId = $"{_log.TopicPrefix}.svc.blobs.{_nodeId}", // never committed: the consumer is assigned, not subscribed
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            FetchMaxBytes = 8 << 20,
            QueuedMaxMessagesKbytes = 8 * 1024,
        }));
        _log.ConfigureConsumer?.Invoke(builder);
        return _consumer = builder.Build();
    }

    private IProducer<byte[], byte[]> Producer()
    {
        if (_producer is not null)
            return _producer;
        var builder = new ProducerBuilder<byte[], byte[]>(Apply(new ProducerConfig
        {
            EnableIdempotence = true,
            Acks = Acks.All,
            LingerMs = 0,
            CompressionType = _log.Compression,
        }));
        _log.ConfigureProducer?.Invoke(builder);
        return _producer = builder.Build();
    }

    /// <summary>Reads what the topic has gained since the last look into the index.</summary>
    private void Refresh(CancellationToken cancellationToken)
    {
        var consumer = Consumer();

        // Used now and then, and asked for watermarks before it is ever polled: its
        // callbacks (a token refresh) are served first, or it has no token to connect with.
        consumer.ServeCallbacks();
        var end = Watermark(consumer, cancellationToken);
        if (_scanned >= end)
            return;
        consumer.Assign(new TopicPartitionOffset(_partition, _scanned));
        var idleSince = Environment.TickCount64;
        while (_scanned < end)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var result = consumer.ConsumeAssigned(TimeSpan.FromMilliseconds(200));
            if (result?.Message is null)
            {
                // Compaction leaves gaps at the end of what was asked for; the position says so.
                var position = consumer.Position(_partition);
                if (position != Offset.Unset && position.Value >= end)
                    _scanned = position.Value;
                else if (Environment.TickCount64 - idleSince > Patience.TotalMilliseconds)
                    throw new TimeoutException($"Reading the blob topic '{Topic}' made no progress for {Patience}.");
                continue;
            }
            idleSince = Environment.TickCount64;
            Note(result);
            _scanned = result.Offset.Value + 1;
        }
        consumer.Unassign();
    }

    /// <summary>The end of the blob topic. A topic created a moment ago is not known to
    /// every broker of a cluster at once, and one that is between leaders has no end to
    /// give: both pass, and are asked about again.</summary>
    private long Watermark(IConsumer<byte[], byte[]> consumer, CancellationToken cancellationToken)
    {
        var deadline = Environment.TickCount64 + (long)Patience.TotalMilliseconds;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                return consumer.QueryWatermarkOffsets(_partition, TimeSpan.FromSeconds(5)).High.Value;
            }
            catch (KafkaException ex) when (Environment.TickCount64 < deadline && ex.Error.Code is
                ErrorCode.Local_UnknownPartition or ErrorCode.Local_UnknownTopic or ErrorCode.UnknownTopicOrPart or ErrorCode.LeaderNotAvailable
                or ErrorCode.NotLeaderForPartition or ErrorCode.Local_TimedOut or ErrorCode.RequestTimedOut or ErrorCode.Local_Transport
                or ErrorCode.Local_AllBrokersDown or ErrorCode.Local_WaitCoord or ErrorCode.NetworkException or ErrorCode.Local_NoEnt)
            {
                Thread.Sleep(100);
            }
        }
    }

    private void Note(ConsumeResult<byte[], byte[]> record)
    {
        if (record.Message.Key is not { } keyBytes)
            return;
        var key = Encoding.UTF8.GetString(keyBytes);
        var slash = key.IndexOf('/');
        if (slash <= 0)
            return;
        var sha = key[..slash];
        var part = key[(slash + 1)..];
        lock (_index)
        {
            if (!_index.TryGetValue(sha, out var entry))
            {
                if (record.Message.Value is null)
                    return;
                _index[sha] = entry = new Entry();
            }
            if (part == "end")
                entry.Count = record.Message.Value is { } value && int.TryParse(Encoding.ASCII.GetString(value), out var count) ? count : -1;
            else if (int.TryParse(part, NumberStyles.None, CultureInfo.InvariantCulture, out var chunk))
            {
                if (record.Message.Value is null)
                    entry.Chunks.Remove(chunk);
                else
                    entry.Chunks[chunk] = record.Offset.Value;
            }
            if (entry.Count < 0 && entry.Chunks.Count == 0)
                _index.Remove(sha);
        }
    }

    private Entry? Complete(string sha256)
    {
        lock (_index)
            return _index.TryGetValue(sha256, out var entry) && entry.IsComplete ? entry : null;
    }

    public async ValueTask<bool> ExistsAsync(string sha256, CancellationToken cancellationToken)
    {
        using var entered = await EnterAsync(cancellationToken).ConfigureAwait(false);
        return Complete(sha256) is not null;
    }

    public async Task PutAsync(string sha256, Stream content, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(content);
        using var entered = await EnterAsync(cancellationToken).ConfigureAwait(false);
        if (Complete(sha256) is not null)
            return;
        var producer = Producer();
        var buffer = new byte[ChunkBytes];
        var chunks = 0;
        while (true)
        {
            var read = await content.ReadAtLeastAsync(buffer, buffer.Length, throwOnEndOfStream: false, cancellationToken).ConfigureAwait(false);
            if (read == 0)
                break;
            await ProduceAsync(producer, $"{sha256}/{chunks++}", buffer.AsSpan(0, read).ToArray(), cancellationToken).ConfigureAwait(false);
            if (read < buffer.Length)
                break;
        }
        await ProduceAsync(producer, $"{sha256}/end", Encoding.ASCII.GetBytes(chunks.ToString(CultureInfo.InvariantCulture)), cancellationToken).ConfigureAwait(false);
    }

    private async Task ProduceAsync(IProducer<byte[], byte[]> producer, string key, byte[]? value, CancellationToken cancellationToken)
    {
        try
        {
            await producer.ProduceAsync(
                _partition, new Message<byte[], byte[]> { Key = Encoding.UTF8.GetBytes(key), Value = value! }, cancellationToken).ConfigureAwait(false);
        }
        catch (ProduceException<byte[], byte[]> ex)
        {
            throw new IOException($"The blob record '{key}' could not be written to '{Topic}': {ex.Error.Reason}", ex);
        }
    }

    public async Task<bool> CopyToAsync(string sha256, Stream destination, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(destination);
        using var entered = await EnterAsync(cancellationToken).ConfigureAwait(false);
        for (var attempt = 0; attempt < 2; attempt++)
        {
            if (Complete(sha256) is not { } entry)
                return false;
            long[] offsets;
            lock (_index)
                offsets = [.. Enumerable.Range(0, entry.Count).Select(i => entry.Chunks[i])];
            var chunks = await Task.Run(() => ReadChunks(sha256, offsets, cancellationToken), cancellationToken).ConfigureAwait(false);
            if (chunks is not null)
            {
                foreach (var chunk in chunks)
                    await destination.WriteAsync(chunk, cancellationToken).ConfigureAwait(false);
                return true;
            }

            // A record was not where the index said: the blob was written again and the
            // cleaner removed the older copy. Index the topic afresh, once.
            _logger.LogInformation("Blob {Blob}: the blob topic was compacted under the index; reading it again", sha256);
            lock (_index)
                _index.Clear();
            _scanned = 0;
            await Task.Run(() => Refresh(cancellationToken), cancellationToken).ConfigureAwait(false);
        }
        return false;
    }

    /// <summary>The blob's records, each read at its offset; null if one is not there. A
    /// plugin file is a few records, so they are gathered before anything is written to the
    /// caller's stream — a half-written file would otherwise need undoing.</summary>
    private List<byte[]>? ReadChunks(string sha256, long[] offsets, CancellationToken cancellationToken)
    {
        var consumer = Consumer();
        var chunks = new List<byte[]>(offsets.Length);
        try
        {
            for (var i = 0; i < offsets.Length; i++)
            {
                consumer.Assign(new TopicPartitionOffset(_partition, offsets[i]));
                var deadline = Environment.TickCount64 + (long)Patience.TotalMilliseconds;
                ConsumeResult<byte[], byte[]>? result = null;
                while (result?.Message is null)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    if (Environment.TickCount64 > deadline)
                        throw new TimeoutException($"Reading blob {sha256} from '{Topic}' at offset {offsets[i]} timed out.");
                    result = consumer.ConsumeAssigned(TimeSpan.FromMilliseconds(200));
                }
                if (result.Offset.Value != offsets[i] || result.Message.Value is null
                    || Encoding.UTF8.GetString(result.Message.Key ?? []) != $"{sha256}/{i}")
                    return null;
                chunks.Add(result.Message.Value);
            }
            return chunks;
        }
        finally
        {
            consumer.Unassign();
        }
    }

    public async Task DeleteAsync(string sha256, CancellationToken cancellationToken)
    {
        using var entered = await EnterAsync(cancellationToken).ConfigureAwait(false);
        int[] chunks;
        lock (_index)
        {
            if (!_index.TryGetValue(sha256, out var entry))
                return;
            chunks = [.. entry.Chunks.Keys];
        }
        var producer = Producer();

        // The end record first: from then on the blob reads as absent, whatever happens next.
        await ProduceAsync(producer, $"{sha256}/end", null, cancellationToken).ConfigureAwait(false);
        foreach (var chunk in chunks)
            await ProduceAsync(producer, $"{sha256}/{chunk}", null, cancellationToken).ConfigureAwait(false);
    }

    public void Dispose()
    {
        _gate.Wait();
        try
        {
            if (_disposed)
                return;
            _disposed = true;
            try
            {
                _producer?.Flush(TimeSpan.FromSeconds(5));
            }
            catch (KafkaException)
            {
            }
            _producer?.Dispose();
            _consumer?.Close();
            _consumer?.Dispose();
        }
        finally
        {
            _gate.Release();
        }
    }
}

/// <summary>What a node says of itself in the leadership probe's answer, and how the
/// other node reads it.</summary>
internal static class PeerDetail
{
    // The probe line's fourth token: no spaces, so the reason and the address are
    // escaped. "ready" or "notready:{reason}", then ";admin={url}" when there is one
    // and ";mem={five numbers}". A reader skips the parts it does not know.
    public static string Write(PeerInfo self) =>
        (self.Ready ? "ready" : "notready:" + Uri.EscapeDataString(self.NotReadyReason ?? ""))
        + (self.AdminUrl is { Length: > 0 } url ? ";admin=" + Uri.EscapeDataString(url) : "")
        + (self.Memory is { } memory ? ";mem=" + memory.Encode() : "");

    public static PeerInfo Read(string nodeId, NodeRole role, long term, string? detail)
    {
        var parts = (detail ?? "").Split(';');
        var ready = parts[0] == "ready";
        string? reason = null;
        if (!ready)
            reason = parts[0].StartsWith("notready:", StringComparison.Ordinal)
                ? Uri.UnescapeDataString(parts[0]["notready:".Length..])
                : "the node gave no readiness";
        var admin = parts.Skip(1).FirstOrDefault(p => p.StartsWith("admin=", StringComparison.Ordinal));
        var memory = parts.Skip(1).FirstOrDefault(p => p.StartsWith("mem=", StringComparison.Ordinal));
        return new PeerInfo(
            nodeId, role, term, ready, reason, admin is null ? null : Uri.UnescapeDataString(admin["admin=".Length..]),
            memory is null ? null : ProcessMemory.Decode(memory["mem=".Length..]));
    }
}
