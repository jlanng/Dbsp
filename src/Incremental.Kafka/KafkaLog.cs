using System.Collections.Concurrent;
using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Incremental.Storage;
using Microsoft.Extensions.Logging;

namespace Incremental.Kafka;

/// <summary>
/// One process's connection to the Kafka topics that hold its durable state: the state
/// changelog (<see cref="State"/>) and the logs of its shared tables
/// (<see cref="OpenTableLogAsync"/>). See <c>docs/design/durability-and-standby.md</c>.
/// <para>
/// Everything is written through <b>one transactional producer</b>. Checkpoint commits and
/// table versions hand their records to a single writer thread and wait; the thread puts
/// whatever is waiting into one Kafka transaction (group commit), so each commit lands
/// whole or not at all and many concurrent commits cost one transaction.
/// </para>
/// <para>
/// A log starts as a reader: it can restore and follow, which is all a standby does.
/// <see cref="AcquireWriterAsync"/> makes it the writer by initialising the shared
/// <c>transactional.id</c>, which fences whichever process held it before — that process's
/// next write fails with <see cref="KafkaLogFencedException"/> and it can never commit
/// again. <see cref="ChangelogStorage.OpenAsync"/> and <see cref="ChangelogStorage.PromoteAsync"/>
/// acquire the writer through <see cref="State"/>; a process that opens tables first calls
/// <see cref="AcquireWriterAsync"/> itself.
/// </para>
/// </summary>
public sealed class KafkaLog : IAsyncDisposable
{
    /// <summary>The first byte of every record value this package writes: JSON, UTF-8.
    /// A later format (the dense codec) takes another value and can be introduced per
    /// topic without rewriting history.</summary>
    internal const byte JsonFormat = 1;

    private static readonly byte[] TermKey = "term"u8.ToArray();
    private static readonly byte[] HeartbeatKey = "heartbeat"u8.ToArray();

    private readonly KafkaLogOptions _options;
    private readonly IAdminClient _admin;
    private readonly IProducer<byte[], byte[]> _producer;
    private readonly TopicPartition _leaderPartition;

    // The writer thread's inbox. Unbounded as a collection, bounded by its users: every
    // committer waits for its one commit, so the depth is at most the number of committers.
    private readonly BlockingCollection<LogCommit> _queue = new(new ConcurrentQueue<LogCommit>());
    private readonly CancellationTokenSource _stop = new();
    private readonly object _gate = new();
    private Thread? _writerThread;
    private Task? _acquire;
    private bool _initialised;
    private bool _closed;
    private int _readers;
    private int _disposed;

    private volatile bool _isWriter;
    private volatile KafkaLogFencedException? _fenced;
    private readonly TaskCompletionSource _whenFenced = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private long _term;
    private long _heartbeatSequence;
    private long _idleHeartbeats;
    private long _inTransactionHeartbeats;
    private long _consumersCreated;
    private long _readerRecordsTaken;
    private long _lastTransactionTicks;
    private long _transactions;
    private long _aborted;
    private long _commits;
    private long _records;

    private KafkaLog(KafkaLogOptions options)
    {
        _options = options;
        _leaderPartition = new TopicPartition(options.TopicPrefix + ".leader", 0);

        var adminConfig = new AdminClientConfig { BootstrapServers = options.BootstrapServers };
        Apply(adminConfig);
        var adminBuilder = new AdminClientBuilder(adminConfig)
            .SetLogHandler((_, message) => Log(message))
            .SetErrorHandler((_, error) => LogError("admin", error));
        options.ConfigureAdmin?.Invoke(adminBuilder);
        _admin = adminBuilder.Build();

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = options.BootstrapServers,
            TransactionalId = options.EffectiveTransactionalId,
            TransactionTimeoutMs = (int)options.TransactionTimeout.TotalMilliseconds,
            CompressionType = options.Compression,
            // A transaction is flushed by its commit, so lingering only delays the first
            // batches of a large one; a little lets them fill.
            LingerMs = 1,
        };
        Apply(producerConfig);
        var producerBuilder = new ProducerBuilder<byte[], byte[]>(producerConfig)
            .SetLogHandler((_, message) => Log(message))
            .SetErrorHandler((_, error) =>
            {
                LogError("producer", error);
                // A fence can arrive between transactions; say so at once instead of at the
                // next commit.
                if (error.IsFatal)
                    MarkFenced(new KafkaException(error));
            });
        options.ConfigureProducer?.Invoke(producerBuilder);
        _producer = producerBuilder.Build();

        State = new KafkaStateChangelog(this);
    }

    /// <summary>
    /// Connects to the cluster and makes sure <c>{prefix}.state</c> and
    /// <c>{prefix}.leader</c> exist with the right shape, creating them if they do not. An
    /// existing state topic whose partition count differs from
    /// <see cref="KafkaLogOptions.StatePartitions"/>, or that is not compacted, is refused.
    /// The log returned is a reader; nothing is fenced yet.
    /// </summary>
    public static async Task<KafkaLog> ConnectAsync(KafkaLogOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        var log = new KafkaLog(options);
        try
        {
            await log.EnsureTopicAsync(
                log.State.Topic, options.StatePartitions, CompactedConfig(options, options.StateTopicConfig),
                fixedPartitions: true, cancellationToken).ConfigureAwait(false);
            // Two keys, rewritten every second: roll segments hourly so the cleaner keeps
            // the topic at a few megabytes instead of a week of heartbeats.
            await log.EnsureTopicAsync(
                log._leaderPartition.Topic, 1,
                CompactedConfig(options, new Dictionary<string, string> { ["segment.ms"] = "3600000" }),
                fixedPartitions: true, cancellationToken).ConfigureAwait(false);
            return log;
        }
        catch
        {
            await log.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    public KafkaLogOptions Options => _options;

    /// <summary>The state changelog over <c>{prefix}.state</c>: pass it to
    /// <see cref="ChangelogStorage.OpenAsync"/> (the active) or
    /// <see cref="ChangelogStorage.Follow"/> (the standby).</summary>
    public KafkaStateChangelog State { get; }

    /// <summary>Whether this process holds the writer and has not lost it.</summary>
    public bool IsWriter => _isWriter && _fenced is null;

    /// <summary>The leader term this log writes under: one more than the last writer's.
    /// 0 until <see cref="AcquireWriterAsync"/> has completed.</summary>
    public long Term => Interlocked.Read(ref _term);

    /// <summary>Whether the writer has been fenced or has failed fatally.</summary>
    public bool IsFenced => _fenced is not null;

    /// <summary>Why the writer is finished; null while it is not.</summary>
    internal KafkaLogFencedException? FencedBy => _fenced;

    /// <summary>The writer ended by <see cref="ResignAsync"/>, not by being fenced.</summary>
    internal bool HasResigned { get; private set; }

    /// <summary>Completes when the writer is fenced or fails fatally: the signal for the
    /// process to stop being the active. Never faults.</summary>
    public Task WhenFenced => _whenFenced.Task;

    /// <summary>This process's name in the leader topic: <see cref="KafkaLogOptions.NodeId"/>.</summary>
    public string NodeId => _options.EffectiveNodeId;

    /// <summary>The leader topic, <c>{prefix}.leader</c>: one partition holding the term
    /// (key <c>term</c>) and the writer's heartbeat (key <c>heartbeat</c>).</summary>
    public string LeaderTopic => _leaderPartition.Topic;

    internal TopicPartition LeaderPartition => _leaderPartition;

    /// <summary>How long ago the last transaction this log committed was begun — a commit,
    /// or the heartbeat an idle writer commits every
    /// <see cref="KafkaLogOptions.HeartbeatInterval"/>. Null on a reader and once fenced. Local elapsed
    /// time; it is what the delivery lease is measured from. On Linux it is read from a
    /// clock that keeps counting while the machine is suspended (<c>CLOCK_BOOTTIME</c>), so a
    /// leader that slept through its lease does not wake up still holding it.</summary>
    public TimeSpan? SinceLastTransaction =>
        IsWriter ? TimeSpan.FromMilliseconds(LeaseClock.Milliseconds - Interlocked.Read(ref _lastTransactionTicks)) : null;

    /// <summary>Transactions that carried nothing but a heartbeat, committed because the
    /// writer was idle. Not counted in <see cref="TransactionsCommitted"/>.</summary>
    public long IdleHeartbeats => Interlocked.Read(ref _idleHeartbeats);

    /// <summary>Heartbeats written inside a transaction that was still being produced or
    /// flushed when another was due: what tells a standby that a long commit is a live
    /// leader and not a dead one.</summary>
    public long InTransactionHeartbeats => Interlocked.Read(ref _inTransactionHeartbeats);

    /// <summary>Consumer clients this log has built. For tests.</summary>
    internal long ConsumersCreated => Interlocked.Read(ref _consumersCreated);

    /// <summary>Records the follower threads have taken from their clients, whether or not
    /// anything has read them yet. For tests.</summary>
    internal long ReaderRecordsTaken => Interlocked.Read(ref _readerRecordsTaken);

    /// <summary>Kafka transactions committed by this log that carried at least one commit.</summary>
    public long TransactionsCommitted => Interlocked.Read(ref _transactions);

    /// <summary>Kafka transactions this log aborted (and retried or failed).</summary>
    public long TransactionsAborted => Interlocked.Read(ref _aborted);

    /// <summary>Commits completed: checkpoint commits and table versions. Under load this
    /// exceeds <see cref="TransactionsCommitted"/>, and the ratio is the group commit.</summary>
    public long CommitsCompleted => Interlocked.Read(ref _commits);

    /// <summary>Records written by committed transactions.</summary>
    public long RecordsWritten => Interlocked.Read(ref _records);

    // ── tables ─────────────────────────────────────────────────────────────────────────

    // Table name → the open of its log, running or done. Holding the task rather than the
    // result is what makes two opens of one name share a single open (and a single epoch).
    private readonly Dictionary<string, Task> _tables = new(StringComparer.Ordinal);

    /// <summary>
    /// Opens the log of the shared table <paramref name="name"/>: the topics
    /// <c>{prefix}.table.{name}.changes</c> (one partition, the version history) and
    /// <c>{prefix}.table.{name}.rows</c> (compacted, the current rows), created if they do
    /// not exist. Pass the result to <c>SharedTable.OpenAsync</c> (the active) or
    /// <c>SharedTable.FollowAsync</c> (the standby). The same instance is returned for the
    /// same name, and concurrent calls for one name share one open.
    /// <para>
    /// A table's identity (its epoch) is written by the writer the first time the table is
    /// opened. On a reader this call therefore waits until the active has opened the table
    /// at least once.
    /// </para>
    /// </summary>
    public async Task<KafkaTableLog<TKey, TRow>> OpenTableLogAsync<TKey, TRow>(string name, CancellationToken cancellationToken = default)
        where TKey : notnull
        where TRow : class
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        if (!IsTopicSafe(name))
            throw new ArgumentException(
                $"Table name '{name}' is not usable in a topic name: use letters, digits, '.', '_' and '-'.", nameof(name));
        while (true)
        {
            Task<KafkaTableLog<TKey, TRow>> open;
            var mine = false;
            lock (_gate)
            {
                ObjectDisposedException.ThrowIf(_closed, this);
                if (_tables.TryGetValue(name, out var existing))
                {
                    open = existing as Task<KafkaTableLog<TKey, TRow>>
                        ?? throw new InvalidOperationException(
                            $"Table log '{name}' is already open in this process with different key or row types.");
                }
                else
                {
                    open = Task.Run(() => KafkaTableLog<TKey, TRow>.OpenAsync(this, name, cancellationToken), CancellationToken.None);
                    _tables[name] = open;
                    mine = true;
                }
            }

            try
            {
                return await open.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // A failed open is forgotten, so the next call tries again.
                if (open.IsCompleted)
                {
                    lock (_gate)
                    {
                        if (_tables.TryGetValue(name, out var current) && ReferenceEquals(current, open))
                            _tables.Remove(name);
                    }
                }

                // Another caller's open, ended by that caller's cancellation: not this one's.
                if (!mine && ex is OperationCanceledException && !cancellationToken.IsCancellationRequested)
                    continue;
                throw;
            }
        }
    }

    // ── becoming the writer ────────────────────────────────────────────────────────────

    /// <summary>
    /// Makes this process the one writer of every topic under the prefix. It initialises
    /// the shared <c>transactional.id</c>: when that returns, the previous writer's open
    /// transaction has been aborted or has finished committing, and nothing it does
    /// afterwards can commit. Then it reads the last leader term and commits the next one.
    /// Safe to call more than once; later calls return the first one's result. A failed or
    /// cancelled call leaves the log a reader and can be repeated.
    /// </summary>
    public Task AcquireWriterAsync(CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_closed, this);
            if (_fenced is { } fenced)
                return Task.FromException(new KafkaLogFencedException(fenced.Message, fenced.InnerException));
            if (_acquire is { IsCompleted: false } or { IsCompletedSuccessfully: true })
                return _acquire;
            return _acquire = Task.Run(() => AcquireAsync(cancellationToken), CancellationToken.None);
        }
    }

    private async Task AcquireAsync(CancellationToken cancellationToken)
    {
        ThrowIfFenced();
        if (!_initialised)
        {
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                _stop.Token.ThrowIfCancellationRequested();
                try
                {
                    // Resumable: a call that times out is retried and carries on where the
                    // last one stopped.
                    _producer.InitTransactions(TimeSpan.FromSeconds(5));
                    break;
                }
                catch (KafkaRetriableException)
                {
                }
                catch (KafkaException ex) when (IsFencing(ex))
                {
                    throw MarkFenced(ex);
                }
                catch (KafkaException ex)
                {
                    throw new KafkaLogException(
                        $"Could not initialise transactional.id '{_options.EffectiveTransactionalId}': {ex.Error.Reason}", ex);
                }
            }
            _initialised = true;
        }

        // Every earlier writer's transactions are decided now, so the leader topic's last
        // term is final.
        var term = await ReadLastTermAsync(cancellationToken).ConfigureAwait(false) + 1;
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_closed, this);
            if (_writerThread is null)
            {
                _writerThread = new Thread(WriterLoop) { IsBackground = true, Name = "incremental-kafka-writer" };
                _writerThread.Start();
            }
        }

        var commit = new LogCommit();
        commit.Add(_leaderPartition, TermKey,
            JsonSerializer.SerializeToUtf8Bytes(new LeaderRecord(term, NodeId, DateTimeOffset.UtcNow)).Prefixed(JsonFormat));
        await EnqueueAsync(commit, acquiring: true).ConfigureAwait(false);
        Interlocked.Exchange(ref _term, term);
        _isWriter = true;
    }

    /// <summary>A record of the leader topic. Under the key <c>term</c>: written once by
    /// each new writer, in its first transaction. Under the key <c>heartbeat</c>: rewritten
    /// in every transaction, and once per heartbeat interval by an idle writer. Terms never
    /// fall along the topic, so the last record carries the current one.
    /// <c>Resigned</c>: the writer stepped down after this record and will write no more.</summary>
    internal sealed record LeaderRecord(long Term, string? Node, DateTimeOffset At, long Sequence = 0, bool Resigned = false);

    /// <summary>Parses a leader-topic record; null for one this version does not know.</summary>
    internal static (LeaderRecord Record, bool IsHeartbeat)? ParseLeaderRecord(ConsumeResult<byte[], byte[]> record)
    {
        if (record.Message.Value is not { Length: > 1 } value || value[0] != JsonFormat || record.Message.Key is not { } key)
            return null;
        var heartbeat = key.AsSpan().SequenceEqual(HeartbeatKey);
        if (!heartbeat && !key.AsSpan().SequenceEqual(TermKey))
            return null;
        return JsonSerializer.Deserialize<LeaderRecord>(value.AsSpan(1)) is { } parsed ? (parsed, heartbeat) : null;
    }

    /// <summary>The last term in the leader topic, 0 when there is none. Terms never fall
    /// and every heartbeat carries one, so the tail of the topic is enough; the window
    /// widens only when the tail holds nothing readable (aborted transactions).</summary>
    private async Task<long> ReadLastTermAsync(CancellationToken cancellationToken)
    {
        var end = await EndOffsetAsync(_leaderPartition, cancellationToken).ConfigureAwait(false);
        if (end == 0)
            return 0;
        var start = await StartOffsetAsync(_leaderPartition, cancellationToken).ConfigureAwait(false);
        return await ShortReadAsync(consumer =>
        {
            for (long window = 32; ; window *= 16)
            {
                var from = Math.Max(start, end - window);
                var term = 0L;
                AssignForShortRead(consumer, new TopicPartitionOffset(_leaderPartition, from));
                ReadTo(consumer, end, record =>
                {
                    if (ParseLeaderRecord(record) is { } parsed)
                        term = Math.Max(term, parsed.Record.Term);
                }, cancellationToken);
                if (term > 0 || from == start)
                    return term;
            }
        }, cancellationToken).ConfigureAwait(false);
    }

    // ── stepping down, being deposed ───────────────────────────────────────────────────

    /// <summary>
    /// Gives the writer up for good: commits a last heartbeat marked as a resignation —
    /// which tells a standby it may take over now rather than after a silence — and closes
    /// the writer. Commit everything that should be durable first; afterwards every write
    /// through this log fails with <see cref="KafkaLogFencedException"/>, and
    /// <see cref="WhenFenced"/> completes. The process is expected to exit and return as a
    /// standby.
    /// </summary>
    public async Task ResignAsync()
    {
        if (!IsWriter)
            throw new InvalidOperationException("This Kafka log is not the writer, so it has nothing to resign.");
        var commit = new LogCommit { Resigns = true };
        commit.Add(_leaderPartition, HeartbeatKey, HeartbeatValue(resigned: true));
        await EnqueueAsync(commit, acquiring: false).ConfigureAwait(false);
    }

    /// <summary>Ends the writer because this process has learnt, from something other than
    /// a failed write, that it is no longer the active (a higher term in the leader topic,
    /// a takeover that could not be completed). Heartbeats stop at once.</summary>
    internal void Depose(string reason) => MarkFenced(new KafkaLogFencedException(reason), reason);

    private byte[] HeartbeatValue(bool resigned) => JsonSerializer.SerializeToUtf8Bytes(
        new LeaderRecord(Term, NodeId, DateTimeOffset.UtcNow, Interlocked.Increment(ref _heartbeatSequence), resigned)).Prefixed(JsonFormat);

    // ── the writer thread: group commit ────────────────────────────────────────────────

    /// <summary>Hands one atomic set of records to the writer thread and completes when the
    /// transaction carrying it has committed. It fails as
    /// <see cref="KafkaLogFencedException"/> (the writer is finished) or as
    /// <see cref="KafkaLogException"/> (aborted; nothing of it is readable; the writer
    /// carries on) and in no other way.</summary>
    internal Task CommitAsync(LogCommit commit) => EnqueueAsync(commit, acquiring: false);

    private Task EnqueueAsync(LogCommit commit, bool acquiring)
    {
        lock (_gate)
        {
            if (_fenced is { } fenced)
                return Task.FromException(new KafkaLogFencedException(fenced.Message, fenced.InnerException));
            if (_closed)
                return Task.FromException(new KafkaLogFencedException("The Kafka log has been disposed; nothing more can be written through it."));
            if (!acquiring && !_isWriter)
                return Task.FromException(new InvalidOperationException(
                    "This Kafka log is not the writer: it is a reader (a standby). Call KafkaLog.AcquireWriterAsync, or promote " +
                    "the ChangelogStorage over it, before writing."));
            _queue.Add(commit);
        }
        return commit.Completion.Task;
    }

    private void WriterLoop()
    {
        var batch = new List<LogCommit>();
        var token = _stop.Token;
        try
        {
            while (true)
            {
                batch.Clear();
                if (!_queue.TryTake(out var first, HeartbeatWait(), token))
                {
                    // Idle. A visible heartbeat means "the active can still commit", so it
                    // is a transaction like any other — and it is how a writer fenced while
                    // idle finds out.
                    if (_isWriter && _fenced is null)
                        IdleHeartbeat();
                    continue;
                }
                batch.Add(first);
                var records = batch[0].Records.Count;

                // Group commit: everything that queued while the last transaction was in
                // flight goes into this one.
                if (_options.GroupCommitLinger > TimeSpan.Zero)
                {
                    var until = Environment.TickCount64 + (long)_options.GroupCommitLinger.TotalMilliseconds;
                    while (records < _options.MaxTransactionRecords
                        && Environment.TickCount64 is var now && now < until
                        && _queue.TryTake(out var lingered, (int)(until - now), token))
                    {
                        batch.Add(lingered);
                        records += lingered.Records.Count;
                    }
                }
                while (records < _options.MaxTransactionRecords && _queue.TryTake(out var next))
                {
                    batch.Add(next);
                    records += next.Records.Count;
                }

                try
                {
                    Execute(batch);
                }
                catch (KafkaLogFencedException ex)
                {
                    foreach (var commit in batch)
                        commit.Completion.TrySetException(new KafkaLogFencedException(ex.Message, ex.InnerException));
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            MarkFenced(ex);
        }
        finally
        {
            var closed = new KafkaLogFencedException(
                _fenced?.Message ?? "The Kafka log was disposed while this write was waiting; whether it committed is unknown.",
                _fenced?.InnerException);
            foreach (var commit in batch)
                commit.Completion.TrySetException(closed);
            lock (_gate)
                _closed = true;
            while (_queue.TryTake(out var orphan))
                orphan.Completion.TrySetException(closed);
        }
    }

    /// <summary>Called on the writer thread before each transaction. For tests: blocking
    /// here is a writer that has stalled without being fenced.</summary>
    internal Action? BeforeTransaction { get; set; }

    private int HeartbeatWait()
    {
        var interval = (long)_options.HeartbeatInterval.TotalMilliseconds;
        if (!_isWriter)
            return (int)interval;
        return (int)Math.Clamp(interval - (LeaseClock.Milliseconds - Interlocked.Read(ref _lastTransactionTicks)), 1, interval);
    }

    private void IdleHeartbeat()
    {
        try
        {
            if (TryRunTransaction([], out _, out var error))
                Interlocked.Increment(ref _idleHeartbeats);
            else
                _options.Logger?.LogWarning(error, "Kafka heartbeat transaction aborted; the next one is due in {Interval}.", _options.HeartbeatInterval);
        }
        catch (KafkaLogFencedException)
        {
            // Marked; WhenFenced has completed.
        }
    }

    /// <summary>Commits <paramref name="batch"/>, completing every commit in it. Throws
    /// <see cref="KafkaLogFencedException"/> when the writer is finished, with the commits
    /// not yet completed left for the caller to fail.</summary>
    private void Execute(List<LogCommit> batch)
    {
        var attempt = 0;
        while (batch.Count > 0)
        {
            _stop.Token.ThrowIfCancellationRequested();
            if (_fenced is { } fenced)
                throw fenced;

            if (TryRunTransaction(batch, out var culprit, out var error))
            {
                Interlocked.Increment(ref _transactions);
                var resigned = false;
                foreach (var commit in batch)
                    resigned |= commit.Resigns;
                if (resigned)
                {
                    HasResigned = true;
                    MarkFenced(
                        new KafkaLogFencedException("stepped down"),
                        "This process stepped down as the active (KafkaLog.ResignAsync); nothing more can be written through this log.");
                }
                foreach (var commit in batch)
                {
                    Interlocked.Add(ref _records, commit.Records.Count);
                    Interlocked.Increment(ref _commits);
                    commit.Completion.TrySetResult();
                }
                return;
            }

            Interlocked.Increment(ref _aborted);
            if (culprit is not null)
            {
                // One commit's own record was refused by the client (too large, say). The
                // transaction is aborted for everyone; only that commit fails, the rest go
                // again.
                culprit.Completion.TrySetException(Aborted(error!));
                batch.Remove(culprit);
                continue;
            }

            if (++attempt > _options.MaxAbortRetries)
            {
                if (batch.Count == 1)
                {
                    batch[0].Completion.TrySetException(Aborted(error!));
                    return;
                }

                // The batch will not commit as a whole. Whatever is wrong may belong to one
                // commit, so each gets its own transaction and its own verdict.
                foreach (var commit in batch.ToArray())
                    Execute([commit]);
                return;
            }
            _options.Logger?.LogWarning(error, "Kafka transaction aborted (attempt {Attempt}); retrying.", attempt);
            _stop.Token.WaitHandle.WaitOne(Math.Min(50 * attempt, 1000));
        }
    }

    private static KafkaLogException Aborted(Exception error) => new(
        "The Kafka transaction carrying this write was aborted, so nothing of it is readable: " +
        ((error as KafkaException)?.Error.Reason ?? error.Message), error);

    /// <summary>One Kafka transaction. True: committed. False: aborted cleanly, with the
    /// reason in <paramref name="error"/> and, when the client refused one commit's own
    /// record, that commit in <paramref name="culprit"/>. Throws when the writer is finished.</summary>
    private bool TryRunTransaction(List<LogCommit> batch, out LogCommit? culprit, out Exception? error)
    {
        culprit = null;
        error = null;
        BeforeTransaction?.Invoke();
        var began = LeaseClock.Milliseconds;
        try
        {
            _producer.BeginTransaction();
        }
        catch (KafkaException ex)
        {
            // Fenced, fatal, or the producer is in a state this loop did not leave it in.
            throw MarkFenced(ex);
        }

        var tracked = false;
        LogCommit? producing = null;
        try
        {
            // The heartbeat goes first, so that a resignation riding in this transaction
            // is the record that stays. Before the first term is committed there is no
            // term to announce, and the term record is the sign of life.
            if (_isWriter)
                Produce(_leaderPartition, new Message<byte[], byte[]> { Key = HeartbeatKey, Value = HeartbeatValue(resigned: false) }, null);

            // A transaction's heartbeat is readable only once it commits, so a long one
            // would read as silence. While this one is being produced and flushed, another
            // heartbeat goes into it every half interval: unreadable until the commit, but
            // each moves the leader topic's end, which is what a standby checks before it
            // decides the leader is gone (KafkaLeadership.NoteProgressAsync). Not after a
            // resignation, which must stay the last heartbeat in the transaction.
            var beatEvery = Math.Max(1, (long)_options.HeartbeatInterval.TotalMilliseconds / 2);
            var lastBeat = Environment.TickCount64;
            var beats = _isWriter;
            var produced = 0;
            foreach (var commit in batch)
            {
                producing = commit;
                tracked |= commit.TrackIndex >= 0;
                beats &= !commit.Resigns;
                for (var i = 0; i < commit.Records.Count; i++)
                {
                    Produce(commit, i);
                    if ((++produced & 255) == 0)
                    {
                        WhileProducing?.Invoke();
                        if (beats)
                            BeatIfDue(beatEvery, ref lastBeat);
                    }
                }
            }
            producing = null;

            // What CommitTransaction would flush anyway, flushed here in slices so that the
            // heartbeats continue. What is left for the commit is one round trip.
            while (_fenced is null && _producer.Flush(TimeSpan.FromMilliseconds(Math.Min(beatEvery, 250))) > 0)
            {
                _stop.Token.ThrowIfCancellationRequested();
                if (beats)
                    BeatIfDue(beatEvery, ref lastBeat);
            }
        }
        catch (KafkaException ex)
        {
            if (IsFencing(ex))
                throw MarkFenced(ex);

            // Only an error that is about the record itself names a culprit. Anything else
            // — above all Local_State, which every Produce throws once an earlier record's
            // delivery has failed and put the transaction in its abortable state — says
            // nothing about the commit that happened to be producing: the batch goes
            // again, and the retries single the real cause out.
            culprit = IsRecordRefusal(ex) ? producing : null;
            error = ex;
            Abort();
            return false;
        }

        while (true)
        {
            try
            {
                _producer.CommitTransaction(TimeSpan.FromSeconds(5));
                break;
            }
            catch (KafkaRetriableException)
            {
                // Resumable, and the outcome is still open: keep asking. If the broker
                // gives up on the transaction first, the next answer is a fence or an abort.
                _stop.Token.ThrowIfCancellationRequested();
            }
            catch (KafkaTxnRequiresAbortException ex)
            {
                error = ex;
                Abort();
                return false;
            }
            catch (KafkaException ex)
            {
                if (IsFencing(ex))
                    throw MarkFenced(ex);

                // Neither retriable nor a clean "abort me". An abort that succeeds settles
                // it as not committed; one that fails ends the writer (Abort throws).
                error = ex;
                Abort();
                return false;
            }
        }

        // Delivery reports are served by the client's poll thread and may trail the commit;
        // a tracked offset has to be there before the commit completes.
        if (tracked)
            _producer.Flush(TimeSpan.FromSeconds(10));

        // Measured from before the transaction began: a standby cannot have seen this
        // heartbeat earlier than that, so a lease counted from here ends before the
        // standby's silence does.
        Interlocked.Exchange(ref _lastTransactionTicks, began);
        return true;
    }

    /// <summary>Called on the writer thread after every 256 records of a transaction. For
    /// tests: sleeping here is a commit that takes long to produce.</summary>
    internal Action? WhileProducing { get; set; }

    private void BeatIfDue(long every, ref long last)
    {
        var now = Environment.TickCount64;
        if (now - last < every)
            return;
        last = now;
        Produce(_leaderPartition, new Message<byte[], byte[]> { Key = HeartbeatKey, Value = HeartbeatValue(resigned: false) }, null);
        Interlocked.Increment(ref _inTransactionHeartbeats);
    }

    /// <summary>Whether a <c>Produce</c> failed because of the record it was given, as
    /// opposed to the state of the producer or of the transaction.</summary>
    internal static bool IsRecordRefusal(KafkaException exception) =>
        exception.Error.Code is ErrorCode.MsgSizeTooLarge
            or ErrorCode.Local_UnknownPartition or ErrorCode.Local_UnknownTopic or ErrorCode.Local_InvalidArg;

    private void Produce(LogCommit commit, int index)
    {
        var record = commit.Records[index];
        var message = new Message<byte[], byte[]> { Key = record.Key!, Value = record.Value!, Headers = record.Headers };
        Produce(record.Target, message, index == commit.TrackIndex ? commit.OnDelivered : null);
    }

    private void Produce(TopicPartition target, Message<byte[], byte[]> message, Action<DeliveryReport<byte[], byte[]>>? handler)
    {
        while (true)
        {
            try
            {
                _producer.Produce(target, message, handler);
                return;
            }
            catch (ProduceException<byte[], byte[]> ex) when (ex.Error.Code == ErrorCode.Local_QueueFull)
            {
                // The client's send queue is full of this transaction: let it drain.
                _stop.Token.ThrowIfCancellationRequested();
                _producer.Poll(TimeSpan.FromMilliseconds(20));
            }
        }
    }

    private void Abort()
    {
        while (true)
        {
            try
            {
                _producer.AbortTransaction(TimeSpan.FromSeconds(5));
                return;
            }
            catch (KafkaRetriableException)
            {
                _stop.Token.ThrowIfCancellationRequested();
            }
            catch (KafkaException ex)
            {
                // The transaction could not be aborted, so its outcome is not known here.
                throw MarkFenced(ex);
            }
        }
    }

    /// <summary>How Confluent.Kafka reports a fenced producer, as observed against Kafka
    /// 3.9 with client 2.16: <c>Local_Fenced</c>, flagged fatal, from
    /// <c>BeginTransaction</c>, <c>CommitTransaction</c> and <c>AbortTransaction</c> — but
    /// <i>not</i> flagged fatal on the <c>ProduceException</c> a later <c>Produce</c>
    /// throws, so the code is checked as well as the flag.</summary>
    internal static bool IsFencing(KafkaException exception) =>
        exception.Error.IsFatal
        || exception.Error.Code is ErrorCode.Local_Fenced or ErrorCode.ProducerFenced or ErrorCode.Local_Fatal;

    private KafkaLogFencedException MarkFenced(Exception cause, string? message = null)
    {
        lock (_gate)
        {
            if (_fenced is null)
            {
                var taken = cause is KafkaException { Error.Code: ErrorCode.Local_Fenced or ErrorCode.ProducerFenced };
                var reason = (cause as KafkaException)?.Error.Reason ?? cause.Message;
                _fenced = new KafkaLogFencedException(
                    message ?? (taken
                        ? $"Another process has initialised transactional.id '{_options.EffectiveTransactionalId}' and taken the log over ({reason})."
                        : $"The Kafka log's producer has failed and cannot write again ({reason})."),
                    cause);
            }
        }
        _whenFenced.TrySetResult();
        return _fenced!;
    }

    private void ThrowIfFenced()
    {
        if (_fenced is { } fenced)
            throw new KafkaLogFencedException(fenced.Message, fenced.InnerException);
    }

    // ── topics ─────────────────────────────────────────────────────────────────────────

    internal static bool IsTopicSafe(string text)
    {
        foreach (var c in text)
        {
            if (!(char.IsAsciiLetterOrDigit(c) || c is '.' or '_' or '-'))
                return false;
        }
        return text.Length > 0;
    }

    internal static Dictionary<string, string> CompactedConfig(KafkaLogOptions options, IReadOnlyDictionary<string, string> extra)
    {
        var config = new Dictionary<string, string>
        {
            ["cleanup.policy"] = "compact",
            ["delete.retention.ms"] = ((long)options.DeleteRetention.TotalMilliseconds).ToString(),
        };
        foreach (var (key, value) in extra)
            config[key] = value;
        return config;
    }

    /// <summary>Creates <paramref name="topic"/>, or checks the one that exists: its cleanup
    /// policy must be the one asked for, and with <paramref name="fixedPartitions"/> its
    /// partition count too. Returns the topic's partition count.</summary>
    internal async Task<int> EnsureTopicAsync(
        string topic, int partitions, Dictionary<string, string> config, bool fixedPartitions, CancellationToken cancellationToken)
    {
        try
        {
            await _admin.CreateTopicsAsync(
                [new TopicSpecification { Name = topic, NumPartitions = partitions, ReplicationFactor = _options.ReplicationFactor, Configs = config }],
                new CreateTopicsOptions { RequestTimeout = _options.OperationTimeout, OperationTimeout = _options.OperationTimeout })
                .WaitAsync(cancellationToken).ConfigureAwait(false);
            return partitions;
        }
        catch (CreateTopicsException ex) when (ex.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
        {
        }
        catch (CreateTopicsException ex)
        {
            throw new KafkaLogException($"Could not create topic '{topic}': {ex.Results[0].Error.Reason}", ex);
        }

        var actual = await RetryAsync(async () =>
        {
            var described = await _admin.DescribeTopicsAsync(
                TopicCollection.OfTopicNames([topic]), new DescribeTopicsOptions { RequestTimeout = _options.OperationTimeout })
                .ConfigureAwait(false);
            return described.TopicDescriptions[0].Partitions.Count;
        }, cancellationToken).ConfigureAwait(false);
        if (fixedPartitions && actual != partitions)
            throw new KafkaLogException(
                $"Topic '{topic}' has {actual} partition(s) and this log is configured for {partitions}. The count can never change: " +
                "records are placed by it. Configure the log with the topic's count, or use a different topic prefix.");

        var wanted = config["cleanup.policy"];
        var configs = await RetryAsync(() => _admin.DescribeConfigsAsync(
            [new ConfigResource { Type = ResourceType.Topic, Name = topic }],
            new DescribeConfigsOptions { RequestTimeout = _options.OperationTimeout }), cancellationToken).ConfigureAwait(false);
        if (configs[0].Entries.TryGetValue("cleanup.policy", out var policy) && policy.Value != wanted)
            throw new KafkaLogException(
                $"Topic '{topic}' has cleanup.policy={policy.Value}; this log needs cleanup.policy={wanted}. " +
                "It was not created by this library, or was altered since.");
        return actual;
    }

    /// <summary>The partition count of a topic this log does not own (a sink's).</summary>
    internal Task<int> PartitionCountAsync(string topic, CancellationToken cancellationToken) =>
        RetryAsync(async () =>
        {
            var described = await _admin.DescribeTopicsAsync(
                TopicCollection.OfTopicNames([topic]), new DescribeTopicsOptions { RequestTimeout = _options.OperationTimeout })
                .ConfigureAwait(false);
            return described.TopicDescriptions[0].Partitions.Count;
        }, cancellationToken);

    // ── reading ────────────────────────────────────────────────────────────────────────

    /// <summary>The end of committed data in a partition (its last stable offset).</summary>
    internal Task<long> EndOffsetAsync(TopicPartition partition, CancellationToken cancellationToken) =>
        ListOffsetAsync(partition, OffsetSpec.Latest(), cancellationToken);

    /// <summary>The end of everything written to a partition, an open transaction's records
    /// included (its high watermark).</summary>
    internal Task<long> UncommittedEndOffsetAsync(TopicPartition partition, CancellationToken cancellationToken) =>
        ListOffsetAsync(partition, OffsetSpec.Latest(), cancellationToken, IsolationLevel.ReadUncommitted);

    /// <summary>The ends of committed data of several partitions, in one request.</summary>
    internal Task<long[]> EndOffsetsAsync(IReadOnlyList<TopicPartition> partitions, CancellationToken cancellationToken) =>
        RetryAsync(async () =>
        {
            var result = await _admin.ListOffsetsAsync(
                partitions.Select(p => new TopicPartitionOffsetSpec { TopicPartition = p, OffsetSpec = OffsetSpec.Latest() }),
                new ListOffsetsOptions { IsolationLevel = IsolationLevel.ReadCommitted, RequestTimeout = _options.OperationTimeout })
                .ConfigureAwait(false);
            var ends = new long[partitions.Count];
            foreach (var info in result.ResultInfos)
            {
                var at = info.TopicPartitionOffsetError;
                for (var i = 0; i < ends.Length; i++)
                {
                    if (partitions[i].Topic == at.Topic && partitions[i].Partition == at.Partition)
                        ends[i] = at.Offset.Value;
                }
            }
            return ends;
        }, cancellationToken);

    /// <summary>The first offset a partition still holds.</summary>
    internal Task<long> StartOffsetAsync(TopicPartition partition, CancellationToken cancellationToken) =>
        ListOffsetAsync(partition, OffsetSpec.Earliest(), cancellationToken);

    private Task<long> ListOffsetAsync(
        TopicPartition partition, OffsetSpec spec, CancellationToken cancellationToken,
        IsolationLevel isolation = IsolationLevel.ReadCommitted) =>
        RetryAsync(async () =>
        {
            var result = await _admin.ListOffsetsAsync(
                [new TopicPartitionOffsetSpec { TopicPartition = partition, OffsetSpec = spec }],
                new ListOffsetsOptions { IsolationLevel = isolation, RequestTimeout = _options.OperationTimeout })
                .ConfigureAwait(false);
            return result.ResultInfos[0].TopicPartitionOffsetError.Offset.Value;
        }, cancellationToken);

    internal Task DeleteRecordsBeforeAsync(TopicPartition partition, long offset, CancellationToken cancellationToken) =>
        RetryAsync(async () =>
        {
            await _admin.DeleteRecordsAsync(
                [new TopicPartitionOffset(partition, offset)],
                new DeleteRecordsOptions { RequestTimeout = _options.OperationTimeout, OperationTimeout = _options.OperationTimeout })
                .ConfigureAwait(false);
            return true;
        }, cancellationToken);

    /// <summary>Runs an admin call, retrying the failures a cluster produces while a topic
    /// is new or a partition's leader is moving, for up to the operation timeout.</summary>
    private Task<T> RetryAsync<T>(Func<Task<T>> call, CancellationToken cancellationToken) =>
        RetryTransientAsync(call, _options.OperationTimeout, cancellationToken);

    /// <summary>Runs <paramref name="call"/>, and again every 100 ms while it fails in a way
    /// that passes (<see cref="IsTransient"/>), until <paramref name="timeout"/> has gone
    /// by; then the failure is the caller's.</summary>
    internal static async Task<T> RetryTransientAsync<T>(Func<Task<T>> call, TimeSpan timeout, CancellationToken cancellationToken)
    {
        var deadline = Environment.TickCount64 + (long)timeout.TotalMilliseconds;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                return await call().WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (KafkaException ex) when (IsTransient(ex) && Environment.TickCount64 < deadline)
            {
                await Task.Delay(100, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Whether an admin call failed for a reason that passes: a topic too new for
    /// the broker asked, a partition between leaders, a broker out of reach.
    /// <c>Local_NoEnt</c> is the client's own "Failed to query partition leaders: No leaders
    /// found" — what <c>ListOffsets</c> and <c>DeleteRecords</c> throw, as a plain
    /// <see cref="KafkaException"/>, when the client's metadata has no leader for a
    /// partition yet (seen for just-created topics on a busy broker).</summary>
    internal static bool IsTransient(KafkaException exception)
    {
        static bool Transient(ErrorCode code) => code is ErrorCode.UnknownTopicOrPart or ErrorCode.LeaderNotAvailable
            or ErrorCode.NotLeaderForPartition or ErrorCode.RequestTimedOut or ErrorCode.Local_TimedOut
            or ErrorCode.Local_Transport or ErrorCode.Local_AllBrokersDown or ErrorCode.NotEnoughReplicas
            or ErrorCode.NetworkException or ErrorCode.Local_NoEnt or ErrorCode.BrokerNotAvailable
            or ErrorCode.ReplicaNotAvailable or ErrorCode.KafkaStorageError or ErrorCode.FencedLeaderEpoch
            or ErrorCode.UnknownLeaderEpoch or ErrorCode.NotController or ErrorCode.Local_WaitCoord;

        return exception switch
        {
            ListOffsetsException list => list.Result.ResultInfos.Any(r => Transient(r.TopicPartitionOffsetError.Error.Code)),
            DescribeTopicsException topics => topics.Results.TopicDescriptions.Any(t => Transient(t.Error.Code)),
            DescribeConfigsException configs => configs.Results.Any(r => Transient(r.Error.Code)),
            DeleteRecordsException deletes => deletes.Results.Any(r => Transient(r.Error.Code)),
            _ => Transient(exception.Error.Code),
        };
    }

    /// <summary>A consumer that reads committed records only, from wherever it is assigned;
    /// it belongs to no group and commits no offsets.</summary>
    internal IConsumer<byte[], byte[]> CreateConsumer(bool forShortReads = false)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            GroupId = _options.TopicPrefix + ".reader", // required by the client; never joined
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            IsolationLevel = IsolationLevel.ReadCommitted,
            EnablePartitionEof = true,
            // A position that no longer exists is an error, never a silent jump.
            AutoOffsetReset = AutoOffsetReset.Error,
            // What a reader that is behind may hold fetched and not yet applied. The
            // client's own default is 64 MB, per consumer.
            QueuedMaxMessagesKbytes = _options.FetchBufferKilobytes,
            // With the buffer full the client postpones its next fetch by this long, and
            // its default is a second: a restore would be held to one buffer a second.
            FetchQueueBackoffMs = 10,
        };
        // A short read stops at the end of a partition, where the client has already asked
        // the broker for more: that fetch waits fetch.wait.max.ms (half a second by default)
        // for data that will not come, and the client sends a broker one fetch at a time —
        // so the NEXT short read, of another partition, would wait out the previous one's
        // long poll. With many tables that is half a second each, in a takeover. The short
        // reader therefore polls briefly; it is assigned only while a read runs.
        if (forShortReads)
            config.FetchWaitMaxMs = 5;
        Apply(config);
        var builder = new ConsumerBuilder<byte[], byte[]>(config)
            .SetLogHandler((_, message) => Log(message))
            .SetErrorHandler((_, error) => LogError("consumer", error));
        _options.ConfigureConsumer?.Invoke(builder);
        Interlocked.Increment(ref _consumersCreated);
        return builder.Build();
    }

    // One consumer for every short read (a table's head, the last term): the reads are
    // brief and a new client for each costs threads, connections and, on MSK, a TLS and
    // IAM handshake — per table, per lag sample. Guarded by the gate; rebuilt after a failure.
    private readonly SemaphoreSlim _shortReadGate = new(1, 1);
    private IConsumer<byte[], byte[]>? _shortReader;

    /// <summary>Runs <paramref name="read"/> on a thread-pool thread with the log's shared
    /// short-read consumer, which <paramref name="read"/> assigns itself. One at a time.</summary>
    internal async Task<T> ShortReadAsync<T>(Func<IConsumer<byte[], byte[]>, T> read, CancellationToken cancellationToken)
    {
        await _shortReadGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return await Task.Run(() =>
            {
                ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
                var consumer = _shortReader ??= CreateConsumer(forShortReads: true);
                try
                {
                    var result = read(consumer);
                    consumer.Unassign();
                    return result;
                }
                catch
                {
                    // Whatever state the read left it in, the next one starts clean.
                    _shortReader = null;
                    try
                    {
                        consumer.Dispose();
                    }
                    catch (KafkaException)
                    {
                    }
                    throw;
                }
            }, CancellationToken.None).ConfigureAwait(false);
        }
        finally
        {
            _shortReadGate.Release();
        }
    }

    // Topics the current short reader has been told the leaders of. Guarded by the gate.
    private readonly HashSet<string> _shortReaderTopics = new(StringComparer.Ordinal);
    private IConsumer<byte[], byte[]>? _shortReaderOfTopics;

    /// <summary>
    /// Assigns the short reader to <paramref name="position"/>, for a read inside
    /// <see cref="ShortReadAsync{T}"/>. The first time the consumer is pointed at a topic it
    /// is first asked for that topic's metadata, and waits for the answer. Without that, a
    /// consumer that is already running learns a new topic's leader only when the client's
    /// one-second housekeeping timer next looks — so each short read of a topic the reader
    /// had not seen took about a second: two per table when a node opens its tables, one
    /// after another. A new consumer's first assignment does not pay this, which is why
    /// followers and restores never showed it.
    /// </summary>
    internal void AssignForShortRead(IConsumer<byte[], byte[]> consumer, TopicPartitionOffset position)
    {
        if (!ReferenceEquals(_shortReaderOfTopics, consumer))
        {
            _shortReaderOfTopics = consumer;
            _shortReaderTopics.Clear();
        }
        if (_shortReaderTopics.Add(position.Topic))
        {
            using var admin = new DependentAdminClientBuilder(consumer.Handle).Build();
            admin.GetMetadata(position.Topic, _options.OperationTimeout);
        }
        consumer.Assign(position);
    }

    /// <summary>Reads an assigned single-partition consumer until it has seen every
    /// committed record below <paramref name="end"/> (a last stable offset), handing each to
    /// <paramref name="onRecord"/>. Blocking. Fails if nothing arrives for the operation
    /// timeout.
    /// <para>
    /// The offset just below a last stable offset is always a transaction marker — every
    /// write to these topics is transactional, and one producer's transactions do not
    /// overlap — so the read is complete once the record two below the end has been seen.
    /// Waiting for the client to report the end instead costs a whole empty fetch
    /// (<c>fetch.wait.max.ms</c>, half a second), which a takeover cannot spare. When the
    /// records before the end belong to an aborted transaction they are never seen, and the
    /// end-of-partition event settles it after all.
    /// </para></summary>
    internal void ReadTo(
        IConsumer<byte[], byte[]> consumer, long end, Action<ConsumeResult<byte[], byte[]>> onRecord, CancellationToken cancellationToken) =>
        ReadTo(consumer, end, record =>
        {
            onRecord(record);
            return false;
        }, cancellationToken);

    /// <summary>As the other overload, stopping early when <paramref name="onRecord"/>
    /// returns true.</summary>
    internal void ReadTo(
        IConsumer<byte[], byte[]> consumer, long end, Func<ConsumeResult<byte[], byte[]>, bool> onRecord, CancellationToken cancellationToken)
    {
        if (end <= 0)
            return; // nothing was ever committed here
        var assigned = consumer.Assignment.FirstOrDefault();
        var position = -1L;
        var idleSince = Environment.TickCount64;
        while (position < end - 1)
        {
            cancellationToken.ThrowIfCancellationRequested();
            _stop.Token.ThrowIfCancellationRequested();
            var result = consumer.Consume(TimeSpan.FromMilliseconds(100));
            if (result is null)
            {
                if (Environment.TickCount64 - idleSince > (long)_options.OperationTimeout.TotalMilliseconds)
                    throw new TimeoutException(
                        $"Reading {consumer.Assignment.FirstOrDefault()} stalled at offset {position} before reaching {end}.");
                continue;
            }
            // The consumer is reused between reads: nothing of an earlier assignment counts.
            if (assigned is not null && result.TopicPartition != assigned)
                continue;
            idleSince = Environment.TickCount64;
            if (result.IsPartitionEOF)
            {
                position = result.Offset.Value;
                continue;
            }
            if (result.Offset.Value >= end)
                return;
            if (onRecord(result))
                return;
            position = result.Offset.Value + 1;
        }
    }

    /// <summary>
    /// The committed records of one partition from <paramref name="from"/>, in batches, with
    /// the position after each batch. It never ends by itself: at the end of the partition
    /// it waits for more. A batch's position is past any transaction markers behind its last
    /// record once the reader has reached the end, which it reports with an empty batch if
    /// it has to. Each call runs its own consumer on its own thread, stopped when the
    /// enumeration is.
    /// </summary>
    internal async IAsyncEnumerable<LogBatch> ReadBatchesAsync(
        TopicPartition partition, Offset from,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var channel = System.Threading.Channels.Channel.CreateBounded<LogBatch>(2);
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _stop.Token);
        var token = stop.Token;
        var thread = new Thread(() => ReadLoop(partition, from, channel.Writer, token))
        {
            IsBackground = true,
            Name = $"incremental-kafka-reader {partition}",
        };
        Interlocked.Increment(ref _readers);
        thread.Start();
        try
        {
            await foreach (var batch in channel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
                yield return batch;

            // The reader thread ended without a fault: only stopping does that.
            cancellationToken.ThrowIfCancellationRequested();
            throw new ObjectDisposedException(nameof(KafkaLog));
        }
        finally
        {
            stop.Cancel();
        }
    }

    private void ReadLoop(
        TopicPartition partition, Offset from, System.Threading.Channels.ChannelWriter<LogBatch> output, CancellationToken token)
    {
        const int MaxBatch = 1024;
        try
        {
            using var consumer = CreateConsumer();
            consumer.Assign(new TopicPartitionOffset(partition, from));
            var reported = -1L;
            while (!token.IsCancellationRequested)
            {
                var result = consumer.Consume(TimeSpan.FromMilliseconds(100));
                if (result is null)
                    continue;

                var records = new List<ConsumeResult<byte[], byte[]>>();
                var next = -1L;
                while (result is not null)
                {
                    if (result.IsPartitionEOF)
                    {
                        // The end of committed data, past whatever markers follow the last
                        // record: this is the position a reader "at the end" must report.
                        next = result.Offset.Value;
                        break;
                    }
                    records.Add(result);
                    Interlocked.Increment(ref _readerRecordsTaken);
                    next = result.Offset.Value + 1;
                    if (records.Count >= MaxBatch)
                        break;
                    result = consumer.Consume(TimeSpan.Zero);
                }

                // Caught up without being told so: when the only offset between the last
                // record and the end of committed data (as the fetch that brought the
                // record reported it) is one offset, that offset is the transaction's
                // marker, and the position is the end. Otherwise the reader would stand one
                // short of the end until an empty fetch said so, half a second later.
                if (records.Count > 0 && consumer.GetWatermarkOffsets(partition).High.Value == next + 1)
                    next++;

                if (records.Count == 0 && next <= reported)
                    continue;
                reported = next;
                output.WriteAsync(new LogBatch(records, next), token).AsTask().GetAwaiter().GetResult();
            }
            output.TryComplete();
        }
        catch (OperationCanceledException)
        {
            output.TryComplete();
        }
        catch (Exception ex)
        {
            output.TryComplete(ex is ConsumeException consume
                ? new KafkaLogException($"Reading {partition} failed: {consume.Error.Reason}", consume)
                : ex);
        }
        finally
        {
            Interlocked.Decrement(ref _readers);
        }
    }

    // ── plumbing ───────────────────────────────────────────────────────────────────────

    private void Apply(ClientConfig config)
    {
        foreach (var (key, value) in _options.ClientConfig)
            config.Set(key, value);
    }

    private void Log(LogMessage message)
    {
        if (_options.Logger is not { } logger)
            return;
        var level = message.Level switch
        {
            SyslogLevel.Emergency or SyslogLevel.Alert or SyslogLevel.Critical => LogLevel.Critical,
            SyslogLevel.Error => LogLevel.Error,
            SyslogLevel.Warning => LogLevel.Warning,
            SyslogLevel.Notice or SyslogLevel.Info => LogLevel.Information,
            _ => LogLevel.Debug,
        };
        logger.Log(level, "{Client} {Facility}: {Message}", message.Name, message.Facility, message.Message);
    }

    private void LogError(string client, Error error) =>
        _options.Logger?.Log(error.IsFatal ? LogLevel.Error : LogLevel.Warning,
            "Kafka {Client} error {Code}: {Reason}", client, error.Code, error.Reason);

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;
        lock (_gate)
            _closed = true;
        _stop.Cancel();

        if (_acquire is { } acquire)
            await acquire.ContinueWith(static _ => { }, TaskScheduler.Default).ConfigureAwait(false);
        if (_writerThread is { } thread)
            await Task.Run(thread.Join).ConfigureAwait(false);
        else
        {
            var closed = new KafkaLogFencedException("The Kafka log has been disposed; nothing more can be written through it.");
            while (_queue.TryTake(out var orphan))
                orphan.Completion.TrySetException(closed);
        }

        // Readers stop on the same token; give their threads a moment to close their
        // consumers before the process moves on.
        for (var i = 0; i < 100 && Volatile.Read(ref _readers) > 0; i++)
            await Task.Delay(20).ConfigureAwait(false);

        // A short read in flight ends on the stop token; then its consumer can go.
        if (await _shortReadGate.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false))
        {
            try
            {
                _shortReader?.Dispose();
            }
            catch (KafkaException)
            {
            }
            _shortReader = null;
        }

        await Task.Run(() =>
        {
            // An open transaction is left to the broker: it is aborted by the next writer's
            // InitTransactions, or by the transaction timeout.
            _producer.Dispose();
            _admin.Dispose();
        }).ConfigureAwait(false);
    }
}

/// <summary>One atomic set of records on its way to the writer thread.</summary>
internal sealed class LogCommit
{
    public readonly List<LogRecord> Records = [];
    public readonly TaskCompletionSource Completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>The commit carries the writer's resignation: once it lands, the writer closes.</summary>
    public bool Resigns;

    /// <summary>Index of the one record whose offset the committer wants, or -1.</summary>
    public int TrackIndex = -1;

    /// <summary>The tracked record's offset once delivered; -1 until then.</summary>
    public long TrackedOffset = -1;

    public Action<DeliveryReport<byte[], byte[]>>? OnDelivered;

    public void Add(TopicPartition target, byte[]? key, byte[]? value, Headers? headers = null) =>
        Records.Add(new LogRecord(target, key, value, headers));

    /// <summary>Asks for the offset of the record added next.</summary>
    public void TrackNext()
    {
        TrackIndex = Records.Count;
        OnDelivered = report =>
        {
            if (!report.Error.IsError)
                Volatile.Write(ref TrackedOffset, report.Offset.Value);
        };
    }
}

/// <summary>A record to write. A null value is a tombstone. The log's own records always
/// have a key and no headers; a sink's records (<see cref="KafkaSinkOptions.Log"/>) may
/// have either.</summary>
internal readonly record struct LogRecord(TopicPartition Target, byte[]? Key, byte[]? Value, Headers? Headers = null);

/// <summary>Records read from one partition, and the reader's position after them.</summary>
internal readonly record struct LogBatch(List<ConsumeResult<byte[], byte[]>> Records, long NextOffset);

internal static class ByteExtensions
{
    /// <summary>The bytes with a format byte in front.</summary>
    public static byte[] Prefixed(this byte[] payload, byte format)
    {
        var value = new byte[payload.Length + 1];
        value[0] = format;
        payload.CopyTo(value, 1);
        return value;
    }
}
