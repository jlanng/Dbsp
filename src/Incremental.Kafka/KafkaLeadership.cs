using System.Diagnostics;
using Incremental.Storage;
using Incremental.Tables;
using Microsoft.Extensions.Logging;

namespace Incremental.Kafka;

/// <summary>What a node is, as its <see cref="KafkaLeadership"/> sees it.</summary>
public enum NodeRole
{
    /// <summary>Following: reads the logs, runs no steps, writes nothing, watches the
    /// leader's heartbeat.</summary>
    Standby,

    /// <summary>Taking over: it has decided to lead and is fencing the previous leader,
    /// promoting its tables and storage and starting its circuits.</summary>
    Promoting,

    /// <summary>The active: the one process that can commit.</summary>
    Leader,

    /// <summary>Was the active and no longer is: another node took over, or the writer
    /// failed, or a takeover could not be completed. Nothing more can be committed here.
    /// Final — the process should exit and come back as a standby.</summary>
    Fenced,

    /// <summary>Was the active and stepped down on request. Final, like
    /// <see cref="Fenced"/>.</summary>
    Resigned,
}

/// <summary>Settings of a <see cref="KafkaLeadership"/>.</summary>
public sealed class KafkaLeadershipOptions
{
    /// <summary>How long the leader topic must be silent before the standby considers the
    /// leader gone. With <see cref="ProbeLeader"/> set, the probe must fail as well. All
    /// timing is elapsed local time; no clocks are compared across machines. Must be at
    /// least twice <see cref="KafkaLogOptions.HeartbeatInterval"/>. Default 3 s.</summary>
    public TimeSpan TakeoverAfter { get; init; } = TimeSpan.FromSeconds(3);

    /// <summary>How long a silence is tolerated while <see cref="ProbeLeader"/> keeps
    /// saying the leader is alive. Past it the standby takes over whatever the probe says.
    /// Default 30 s.</summary>
    public TimeSpan TakeoverCeiling { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// A direct check of the other node: true when it answers as the leader. It is a hint
    /// and never a matter of safety — it stops a stall in Kafka itself (a broker election,
    /// a rolling patch), during which a healthy leader cannot heartbeat, from causing a
    /// needless failover. Throwing, or not answering within <see cref="ProbeTimeout"/>,
    /// counts as false. Null: silence alone decides. <see cref="LeadershipProbe"/> has a
    /// TCP implementation.
    /// </summary>
    public Func<CancellationToken, ValueTask<bool>>? ProbeLeader { get; init; }

    /// <summary>How long one probe may take. Default 1 s.</summary>
    public TimeSpan ProbeTimeout { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// The delivery lease is <see cref="TakeoverAfter"/> less this margin: a leader whose
    /// last committed transaction began longer ago than that must not deliver to sinks
    /// (<see cref="KafkaLeadership.HoldsLease"/>), because a standby may be about to decide
    /// it is gone. Null: a third of <see cref="TakeoverAfter"/>. The lease must stay longer
    /// than the heartbeat interval or a healthy idle leader would lose it between beats.
    /// </summary>
    public TimeSpan? LeaseMargin { get; init; }

    /// <summary>False: the standby never takes over by itself, only on
    /// <see cref="KafkaLeadership.TakeOverAsync"/>. Default true.</summary>
    public bool AutomaticTakeover { get; init; } = true;

    /// <summary>How often a standby measures its lag for <see cref="LeadershipStatus.Lag"/>.
    /// Null: never; <see cref="KafkaLeadership.MeasureLagAsync"/> still works. Default 5 s.</summary>
    public TimeSpan? LagSampleInterval { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Called during a takeover, after this node has become the writer and before anything
    /// is promoted: the place to wait for whatever the standby was still building. On a
    /// deployment's very first start no table exists until a writer opens it, so a
    /// standby's <c>OpenTableLogAsync</c> completes only now — await it here, and
    /// <see cref="KafkaLeadership.Track{TKey,TRow}"/> the tables before returning.
    /// </summary>
    public Func<CancellationToken, Task>? PrepareAsync { get; init; }

    /// <summary>
    /// Called last in a takeover, with the new term, once the tracked tables and storages
    /// have been promoted: start every circuit, connect the sources at their committed
    /// positions, start the sinks. When it returns the node is the
    /// <see cref="NodeRole.Leader"/>. If it throws, the takeover is abandoned and the node
    /// ends as <see cref="NodeRole.Fenced"/>.
    /// </summary>
    public required Func<long, CancellationToken, Task> StartLeadingAsync { get; init; }

    /// <summary>Called by <see cref="KafkaLeadership.StepDownAsync"/> before the resignation
    /// is written: stop the sources and dispose the circuits, so that their last commits
    /// land while this node can still write.</summary>
    public Func<CancellationToken, Task>? StopLeadingAsync { get; init; }

    public ILogger? Logger { get; init; }

    internal TimeSpan Lease => TakeoverAfter - (LeaseMargin ?? TakeoverAfter / 3);

    internal void Validate(KafkaLogOptions log)
    {
        ArgumentNullException.ThrowIfNull(StartLeadingAsync);
        if (TakeoverAfter < log.HeartbeatInterval * 2)
            throw new ArgumentException(
                $"TakeoverAfter ({TakeoverAfter}) must be at least twice the log's HeartbeatInterval ({log.HeartbeatInterval}), " +
                "or one late heartbeat reads as a dead leader.", nameof(TakeoverAfter));
        ArgumentOutOfRangeException.ThrowIfLessThan(TakeoverCeiling, TakeoverAfter);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(ProbeTimeout, TimeSpan.Zero);
        if (LeaseMargin is { } margin && margin < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(LeaseMargin), margin, "The lease margin cannot be negative.");
        if (Lease <= log.HeartbeatInterval)
            throw new ArgumentException(
                $"The delivery lease (TakeoverAfter less LeaseMargin = {Lease}) must be longer than the log's HeartbeatInterval " +
                $"({log.HeartbeatInterval}), or an idle leader loses it between heartbeats.", nameof(LeaseMargin));
        if (LagSampleInterval is { } sample)
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(sample, TimeSpan.Zero);
    }
}

/// <summary>A node's view of who leads, for a status page or a health check.</summary>
/// <param name="NodeId">This node.</param>
/// <param name="Role">This node's role.</param>
/// <param name="Term">The term this node leads under; 0 unless it is or was the leader.</param>
/// <param name="LeaderNodeId">The node whose record was last seen in the leader topic;
/// null when the topic is empty.</param>
/// <param name="LeaderTerm">The term in that record.</param>
/// <param name="HeartbeatAge">On a standby, how long ago the leader was last heard, in
/// local elapsed time (counted from start-up until the first record arrives). On the
/// leader, how long ago its last committed transaction began.</param>
/// <param name="LeaderHeartbeatAt">The wall-clock time the leader stamped on its last
/// record. Its clock, not this node's: for display only.</param>
/// <param name="LeaderResigned">The last record was a resignation: nobody leads.</param>
/// <param name="HoldsLease">Whether this node may deliver to sinks now.</param>
/// <param name="Lag">A standby's lag as last sampled; null on a leader or before the
/// first sample.</param>
/// <param name="LastTakeover">How this node's takeover went; null if it has not taken over.</param>
/// <param name="Fault">Why the node is <see cref="NodeRole.Fenced"/>.</param>
public sealed record LeadershipStatus(
    string NodeId,
    NodeRole Role,
    long Term,
    string? LeaderNodeId,
    long LeaderTerm,
    TimeSpan HeartbeatAge,
    DateTimeOffset? LeaderHeartbeatAt,
    bool LeaderResigned,
    bool HoldsLease,
    FollowerLag? Lag,
    TakeoverTimings? LastTakeover,
    Exception? Fault);

/// <summary>How far a standby is behind the logs it follows.</summary>
/// <param name="StateOffsets">For each partition of the state topic, the offsets between
/// the follower's position and the end of committed data. 0 when caught up.</param>
/// <param name="TableVersions">For each tracked table, the versions between the standby's
/// view and the log's last version.</param>
/// <param name="SampledAt">When it was measured.</param>
public sealed record FollowerLag(
    IReadOnlyList<long> StateOffsets, IReadOnlyDictionary<string, long> TableVersions, DateTimeOffset SampledAt)
{
    /// <summary>The sum over the state partitions.</summary>
    public long TotalStateOffsets => StateOffsets.Sum();

    /// <summary>Whether nothing is behind.</summary>
    public bool IsCaughtUp => TotalStateOffsets == 0 && TableVersions.Values.All(v => v == 0);
}

/// <summary>Where a takeover's time went.</summary>
/// <param name="Reason">Why it happened: <c>silence</c>, <c>ceiling</c>, <c>resigned</c> or <c>manual</c>.</param>
/// <param name="Silence">How long the leader had been silent when this node decided.</param>
/// <param name="Fence">Initialising the <c>transactional.id</c> and committing the term.</param>
/// <param name="Prepare">Waiting in <see cref="KafkaLeadershipOptions.PrepareAsync"/>.</param>
/// <param name="Tables">Promoting the tracked tables.</param>
/// <param name="Storage">Promoting the tracked storages.</param>
/// <param name="Start"><see cref="KafkaLeadershipOptions.StartLeadingAsync"/>.</param>
/// <param name="Total">From the decision to being the leader.</param>
public sealed record TakeoverTimings(
    string Reason, TimeSpan Silence, TimeSpan Fence, TimeSpan Prepare, TimeSpan Tables, TimeSpan Storage, TimeSpan Start, TimeSpan Total);

/// <summary>
/// Decides which of two nodes is the active, using Kafka and nothing else
/// (<c>docs/design/durability-and-standby.md</c>, <i>Leadership and fencing</i>).
/// <para>
/// Every node starts as a <see cref="NodeRole.Standby"/>: it follows the logs, builds its
/// circuits without starting them, and watches <c>{prefix}.leader</c>, where the active's
/// every transaction leaves a heartbeat. When the topic has been silent for
/// <see cref="KafkaLeadershipOptions.TakeoverAfter"/> (and the peer probe, if there is one,
/// fails), or the leader resigns, or <see cref="TakeOverAsync"/> is called, the standby
/// takes over: it acquires the log's writer — which initialises the shared
/// <c>transactional.id</c>, fencing the old leader, and commits the next term — then
/// promotes the tracked tables, then the tracked storages, then calls
/// <see cref="KafkaLeadershipOptions.StartLeadingAsync"/>, and is the
/// <see cref="NodeRole.Leader"/>.
/// </para>
/// <para>
/// Electing is not what makes this safe; fencing is. Two standbys that decide at the same
/// moment both initialise the transactional id, the later one wins, and the earlier one's
/// next write fails. A leader finds out it has been replaced from that failed write (its
/// idle heartbeat is one), or sooner from the higher term it reads in the leader topic,
/// and becomes <see cref="NodeRole.Fenced"/>. Fenced and resigned are final: the process
/// exits and comes back as a standby. Demotion in place is not built.
/// </para>
/// <para>
/// What fencing does not cover is a sink outside Kafka. <see cref="HoldsLease"/> is the
/// lease that narrows that gap, and <see cref="Leased{TKey,TRow}"/> applies it to a sink.
/// </para>
/// </summary>
public sealed class KafkaLeadership : IAsyncDisposable
{
    private readonly KafkaLog _log;
    private readonly KafkaLeadershipOptions _options;
    private readonly ILogger? _logger;
    private readonly CancellationTokenSource _stop = new();
    private readonly object _lock = new();
    private readonly List<ITrackedTable> _tables = [];
    private readonly List<ChangelogStorage> _storages = [];
    private readonly TaskCompletionSource _whenFinished = new(TaskCreationOptions.RunContinuationsAsynchronously);

    // Guarded by _lock.
    private NodeRole _role = NodeRole.Standby;
    private TaskCompletionSource _roleChanged = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private string? _leaderNode;
    private long _leaderTerm;
    private DateTimeOffset? _leaderAt;
    private bool _leaderResigned;
    private long _heardTicks;
    private long _watchPosition;
    private bool _manual;
    private FollowerLag? _lag;
    private TakeoverTimings? _lastTakeover;
    private Exception? _fault;

    private Task _run = Task.CompletedTask;
    private Task _watch = Task.CompletedTask;
    private Task _sampler = Task.CompletedTask;
    private int _started;
    private int _disposed;

    public KafkaLeadership(KafkaLog log, KafkaLeadershipOptions options)
    {
        ArgumentNullException.ThrowIfNull(log);
        ArgumentNullException.ThrowIfNull(options);
        options.Validate(log.Options);
        _log = log;
        _options = options;
        _logger = options.Logger ?? log.Options.Logger;
        _heardTicks = Environment.TickCount64;
    }

    /// <summary>This node's name: the log's <see cref="KafkaLogOptions.NodeId"/>.</summary>
    public string NodeId => _log.NodeId;

    /// <summary>This node's role.</summary>
    public NodeRole Role
    {
        get
        {
            lock (_lock)
                return _role;
        }
    }

    /// <summary>The term this node leads under; 0 unless it is or was the leader.</summary>
    public long Term => _log.Term;

    /// <summary>Raised after every change of role, and when the leader this node sees
    /// changes. Handlers run on a thread-pool thread, one at a time, in order; a handler
    /// that throws is logged and ignored.</summary>
    public event Action<LeadershipStatus>? Changed;

    /// <summary>Completes when this node reaches <see cref="NodeRole.Fenced"/> or
    /// <see cref="NodeRole.Resigned"/> — the signal for the process to exit. Never faults.</summary>
    public Task WhenFinished => _whenFinished.Task;

    /// <summary>The node's view of the leadership, now. No I/O.</summary>
    public LeadershipStatus Status
    {
        get
        {
            var sinceTransaction = _log.SinceLastTransaction;
            lock (_lock)
            {
                var age = _role is NodeRole.Leader && sinceTransaction is { } own
                    ? own
                    : TimeSpan.FromMilliseconds(Environment.TickCount64 - _heardTicks);
                return new LeadershipStatus(
                    NodeId, _role, _log.Term, _leaderNode, _leaderTerm, age, _leaderAt, _leaderResigned,
                    HoldsLease, _role is NodeRole.Standby ? _lag : null, _lastTakeover, _fault);
            }
        }
    }

    // ── what a takeover promotes, and what lag is measured on ──────────────────────────

    private interface ITrackedTable
    {
        string Name { get; }
        Task PromoteAsync(CancellationToken cancellationToken);
        ValueTask<long> LagAsync(CancellationToken cancellationToken);
    }

    private sealed class TrackedTable<TKey, TRow>(SharedTable<TKey, TRow> table, KafkaTableLog<TKey, TRow> log) : ITrackedTable
        where TKey : notnull
        where TRow : class
    {
        public string Name => table.Name;

        public Task PromoteAsync(CancellationToken cancellationToken) =>
            table.IsFollowing ? table.PromoteAsync(cancellationToken) : Task.CompletedTask;

        public async ValueTask<long> LagAsync(CancellationToken cancellationToken) =>
            Math.Max(0, await log.LastVersionAsync(cancellationToken).ConfigureAwait(false) - table.LatestVersion);
    }

    /// <summary>
    /// Registers a table this node follows (<c>SharedTable.FollowAsync</c> over
    /// <paramref name="log"/>). A takeover promotes tracked tables, in the order they were
    /// tracked, before any storage; a standby's lag is measured on them.
    /// </summary>
    public void Track<TKey, TRow>(SharedTable<TKey, TRow> table, KafkaTableLog<TKey, TRow> log)
        where TKey : notnull
        where TRow : class
    {
        ArgumentNullException.ThrowIfNull(table);
        ArgumentNullException.ThrowIfNull(log);
        lock (_lock)
            _tables.Add(new TrackedTable<TKey, TRow>(table, log));
    }

    /// <summary>Registers a storage this node follows (<see cref="ChangelogStorage.Follow"/>
    /// over <see cref="KafkaLog.State"/>). A takeover promotes it after the tracked tables;
    /// a standby's lag is measured on it.</summary>
    public void Track(ChangelogStorage storage)
    {
        ArgumentNullException.ThrowIfNull(storage);
        lock (_lock)
            _storages.Add(storage);
    }

    // ── the lease ──────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Whether this node may deliver to a sink right now: it is the log's writer, and its
    /// last committed transaction began less than the lease ago
    /// (<see cref="KafkaLeadershipOptions.TakeoverAfter"/> less
    /// <see cref="KafkaLeadershipOptions.LeaseMargin"/>). A standby cannot decide the leader
    /// is gone before <c>TakeoverAfter</c> has passed since it saw that transaction's
    /// heartbeat, so a leader that checks this before each delivery stops delivering before
    /// anyone can replace it — unless it is paused between the check and the write, which
    /// is the gap that remains.
    /// </summary>
    public bool HoldsLease => _log.IsWriter && _log.SinceLastTransaction is { } since && since < _options.Lease;

    /// <summary>Completes when <see cref="HoldsLease"/> is true. A leader that has lost the
    /// lease regains it with its next committed transaction (the idle heartbeat is one).
    /// Throws <see cref="KafkaLogFencedException"/> when the node is not, or is no longer,
    /// the writer.</summary>
    public async ValueTask WaitForLeaseAsync(CancellationToken cancellationToken = default)
    {
        while (true)
        {
            if (HoldsLease)
                return;
            if (!_log.IsWriter)
                throw new KafkaLogFencedException(
                    $"Node '{NodeId}' is not the active (it is {Role}), so it must not deliver to sinks.");
            await Task.Delay(10, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Wraps a sink so that every write first waits for the lease
    /// (<see cref="WaitForLeaseAsync"/>): a deposed or stalled leader stops delivering.</summary>
    public IUpsertSink<TKey, TRow> Leased<TKey, TRow>(IUpsertSink<TKey, TRow> sink)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(sink);
        return new LeasedUpsertSink<TKey, TRow>(this, sink);
    }

    /// <summary>As <see cref="Leased{TKey,TRow}"/>, for an append sink.</summary>
    public IAppendSink<T> Leased<T>(IAppendSink<T> sink)
    {
        ArgumentNullException.ThrowIfNull(sink);
        return new LeasedAppendSink<T>(this, sink);
    }

    private sealed class LeasedUpsertSink<TKey, TRow>(KafkaLeadership leadership, IUpsertSink<TKey, TRow> inner) : IUpsertSink<TKey, TRow>
        where TKey : notnull
    {
        public async ValueTask WriteAsync(UpsertBatch<TKey, TRow> batch, CancellationToken cancellationToken)
        {
            await leadership.WaitForLeaseAsync(cancellationToken).ConfigureAwait(false);
            await inner.WriteAsync(batch, cancellationToken).ConfigureAwait(false);
        }
    }

    private sealed class LeasedAppendSink<T>(KafkaLeadership leadership, IAppendSink<T> inner) : IAppendSink<T>
    {
        public async ValueTask WriteAsync(AppendBatch<T> batch, CancellationToken cancellationToken)
        {
            await leadership.WaitForLeaseAsync(cancellationToken).ConfigureAwait(false);
            await inner.WriteAsync(batch, cancellationToken).ConfigureAwait(false);
        }
    }

    // ── starting, commands ─────────────────────────────────────────────────────────────

    /// <summary>
    /// Starts watching the leader topic as a standby and, unless
    /// <see cref="KafkaLeadershipOptions.AutomaticTakeover"/> is off, taking over when the
    /// leader goes quiet. It returns once the watch is running. A node that should lead a
    /// deployment nobody leads yet does not need telling: it takes over after
    /// <see cref="KafkaLeadershipOptions.TakeoverAfter"/> of silence, or at once on
    /// <see cref="TakeOverAsync"/>.
    /// </summary>
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        if (Interlocked.Exchange(ref _started, 1) != 0)
            throw new InvalidOperationException("This leadership has already been started.");

        // Start near the end: only the last record matters, and the topic may hold hours
        // of heartbeats the cleaner has not reached.
        var end = await _log.EndOffsetAsync(_log.LeaderPartition, cancellationToken).ConfigureAwait(false);
        var start = await _log.StartOffsetAsync(_log.LeaderPartition, cancellationToken).ConfigureAwait(false);
        var from = Math.Max(start, end - 16);
        lock (_lock)
        {
            _heardTicks = Environment.TickCount64;
            _watchPosition = from;
        }
        _watch = Task.Run(() => WatchAsync(from));
        _run = Task.Run(RunAsync);
        if (_options.LagSampleInterval is { } interval)
            _sampler = Task.Run(() => SampleLagAsync(interval));
    }

    /// <summary>
    /// The manual "take over now": a standby stops waiting and takes over, whether or not
    /// the leader is alive. The leader, if there is one, is fenced, and whatever its
    /// circuits had not committed is replayed here. Completes when this node is the leader;
    /// throws if the takeover fails. For a planned switch with nothing to replay, ask the
    /// leader to <see cref="StepDownAsync"/> instead.
    /// </summary>
    public async Task TakeOverAsync(CancellationToken cancellationToken = default)
    {
        if (Volatile.Read(ref _started) == 0)
            throw new InvalidOperationException("Start the leadership before asking it to take over.");
        lock (_lock)
        {
            if (_role is NodeRole.Fenced or NodeRole.Resigned)
                throw new InvalidOperationException($"Node '{NodeId}' is {_role} and cannot take over; restart it as a standby.");
            _manual = true;
        }
        var role = await WhenRoleAsync(r => r is not (NodeRole.Standby or NodeRole.Promoting), cancellationToken).ConfigureAwait(false);
        if (role is not NodeRole.Leader)
            throw new KafkaLogFencedException($"Node '{NodeId}' did not become the leader: it is {role}.", Status.Fault);
    }

    /// <summary>
    /// The leader's "step down": it stops its circuits
    /// (<see cref="KafkaLeadershipOptions.StopLeadingAsync"/>), commits a resignation to the
    /// leader topic — on reading it a standby takes over at once, with no silence to wait
    /// out — and ends as <see cref="NodeRole.Resigned"/>. The process should then exit and
    /// return as a standby.
    /// </summary>
    public async Task StepDownAsync(CancellationToken cancellationToken = default)
    {
        lock (_lock)
        {
            if (_role is not NodeRole.Leader)
                throw new InvalidOperationException($"Node '{NodeId}' is {_role}; only the leader can step down.");
        }
        if (_options.StopLeadingAsync is { } stop)
            await stop(cancellationToken).ConfigureAwait(false);
        try
        {
            await _log.ResignAsync().ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is KafkaLogFencedException or InvalidOperationException)
        {
            // Replaced while stepping down: the end is the same, the name differs.
            Finish(NodeRole.Fenced, ex);
            throw;
        }
        Finish(NodeRole.Resigned, null);
    }

    /// <summary>Completes when the role satisfies <paramref name="predicate"/>, with that role.</summary>
    public async Task<NodeRole> WhenRoleAsync(Func<NodeRole, bool> predicate, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        while (true)
        {
            Task changed;
            lock (_lock)
            {
                if (predicate(_role))
                    return _role;
                changed = _roleChanged.Task;
            }
            await changed.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Completes when this node has the given role.</summary>
    public Task WhenRoleAsync(NodeRole role, CancellationToken cancellationToken = default) =>
        WhenRoleAsync(r => r == role, cancellationToken);

    // ── lag ────────────────────────────────────────────────────────────────────────────

    /// <summary>Measures how far the tracked storages and tables are behind their logs,
    /// now: one offsets request for the state topic and one short read per table.</summary>
    public async Task<FollowerLag> MeasureLagAsync(CancellationToken cancellationToken = default)
    {
        ITrackedTable[] tables;
        ChangelogStorage[] storages;
        lock (_lock)
        {
            tables = [.. _tables];
            storages = [.. _storages];
        }

        var partitions = new Confluent.Kafka.TopicPartition[_log.State.PartitionCount];
        for (var p = 0; p < partitions.Length; p++)
            partitions[p] = new Confluent.Kafka.TopicPartition(_log.State.Topic, p);
        var state = new long[partitions.Length];
        if (storages.Length > 0)
        {
            var ends = await _log.EndOffsetsAsync(partitions, cancellationToken).ConfigureAwait(false);
            foreach (var storage in storages)
            {
                var positions = storage.FollowedOffsets();
                for (var p = 0; p < state.Length; p++)
                    state[p] = Math.Max(state[p], Math.Max(0, ends[p] - positions[p]));
            }
        }

        var versions = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var table in tables)
            versions[table.Name] = await table.LagAsync(cancellationToken).ConfigureAwait(false);
        return new FollowerLag(state, versions, DateTimeOffset.UtcNow);
    }

    private async Task SampleLagAsync(TimeSpan interval)
    {
        var token = _stop.Token;
        try
        {
            while (true)
            {
                if (Role is NodeRole.Standby)
                {
                    try
                    {
                        var lag = await MeasureLagAsync(token).ConfigureAwait(false);
                        lock (_lock)
                            _lag = lag;
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        _logger?.LogWarning(ex, "Node {Node} could not measure its follower lag.", NodeId);
                    }
                }
                else if (Role is not NodeRole.Promoting)
                {
                    return;
                }
                await Task.Delay(interval, token).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    // ── watching the leader topic ──────────────────────────────────────────────────────

    private async Task WatchAsync(long from)
    {
        var token = _stop.Token;
        while (!token.IsCancellationRequested)
        {
            try
            {
                await foreach (var batch in _log.ReadBatchesAsync(_log.LeaderPartition, from, token).ConfigureAwait(false))
                {
                    foreach (var record in batch.Records)
                    {
                        if (KafkaLog.ParseLeaderRecord(record) is { } parsed)
                            Heard(parsed.Record);
                    }
                    from = batch.NextOffset;
                    lock (_lock)
                        _watchPosition = from;
                }
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                // Without the watch a standby hears nothing, and silence it cannot tell
                // from a dead leader. The takeover's last check (ConfirmSilenceAsync) asks
                // the broker directly, so this is a delay, not a false alarm.
                _logger?.LogWarning(ex, "Node {Node} lost its watch on the leader topic; reading again from offset {Offset}.", NodeId, from);
                try
                {
                    await Task.Delay(500, token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }
    }

    private void Heard(KafkaLog.LeaderRecord record)
    {
        bool leaderChanged;
        lock (_lock)
        {
            if (record.Term < _leaderTerm)
                return; // a record older than one already seen: only a restarted watch reads those
            leaderChanged = record.Term != _leaderTerm || record.Node != _leaderNode || record.Resigned != _leaderResigned;
            _leaderTerm = record.Term;
            _leaderNode = record.Node;
            _leaderAt = record.At;
            _leaderResigned = record.Resigned;
            _heardTicks = Environment.TickCount64;
        }

        // A higher term than this node's own is another node's takeover: sooner news than
        // the failed write would be, and the only news an idle sink-bound leader gets
        // before its next heartbeat.
        if (_log.IsWriter && record.Term > _log.Term)
            _log.Depose($"Node '{record.Node}' has taken the log over under term {record.Term}; this node led under term {_log.Term}.");
        if (leaderChanged)
            Raise();
    }

    // ── the state machine ──────────────────────────────────────────────────────────────

    private async Task RunAsync()
    {
        var token = _stop.Token;
        try
        {
            if (await WaitToTakeOverAsync(token).ConfigureAwait(false) is not { } decision)
                return;
            if (!await PromoteAsync(decision.Reason, decision.Silence, token).ConfigureAwait(false))
                return;

            // Leading. The only way out that this loop sees is the writer ending: fenced by
            // another node's InitTransactions, deposed by a higher term, or failed.
            await _log.WhenFenced.WaitAsync(token).ConfigureAwait(false);
            if (_log.HasResigned)
                Finish(NodeRole.Resigned, null);
            else
                Finish(NodeRole.Fenced, _log.FencedBy);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Node {Node}: the leadership loop failed.", NodeId);
            _log.Depose($"Node '{NodeId}': the leadership loop failed ({ex.Message}).");
            Finish(NodeRole.Fenced, ex);
        }
    }

    private async Task<(string Reason, TimeSpan Silence)?> WaitToTakeOverAsync(CancellationToken token)
    {
        var tick = TimeSpan.FromMilliseconds(Math.Clamp(_options.TakeoverAfter.TotalMilliseconds / 20, 10, 100));
        var nextProbe = 0L;
        while (true)
        {
            bool manual, resigned;
            TimeSpan silence;
            lock (_lock)
            {
                if (_role is not NodeRole.Standby)
                    return null;
                manual = _manual;
                resigned = _leaderResigned;
                silence = TimeSpan.FromMilliseconds(Environment.TickCount64 - _heardTicks);
            }

            if (manual)
                return ("manual", silence);
            if (_options.AutomaticTakeover)
            {
                string? reason = null;
                if (resigned)
                    reason = "resigned";
                else if (silence >= _options.TakeoverCeiling)
                    reason = "ceiling";
                else if (silence >= _options.TakeoverAfter)
                {
                    if (_options.ProbeLeader is null)
                        reason = "silence";
                    else if (Environment.TickCount64 >= nextProbe)
                    {
                        if (!await ProbeAsync(token).ConfigureAwait(false))
                            reason = "silence";
                        nextProbe = Environment.TickCount64 + 250;
                    }
                }

                if (reason is not null && await ConfirmSilenceAsync(resigned, token).ConfigureAwait(false))
                {
                    lock (_lock)
                        silence = TimeSpan.FromMilliseconds(Environment.TickCount64 - _heardTicks);
                    return (reason, silence);
                }
            }
            await Task.Delay(tick, token).ConfigureAwait(false);
        }
    }

    private async Task<bool> ProbeAsync(CancellationToken token)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(token);
        timeout.CancelAfter(_options.ProbeTimeout);
        try
        {
            return await _options.ProbeLeader!(timeout.Token).AsTask().WaitAsync(timeout.Token).ConfigureAwait(false);
        }
        catch (Exception) when (!token.IsCancellationRequested)
        {
            return false;
        }
    }

    /// <summary>The last check before a takeover: ask the broker where the leader topic
    /// ends. If it cannot be asked, this node is the one cut off from Kafka, not the
    /// leader, and taking over would stall and then depose a healthy leader when the
    /// connection returns. If the topic has records this node's watch has not delivered,
    /// the silence is the watch's, not the leader's.</summary>
    private async Task<bool> ConfirmSilenceAsync(bool resigned, CancellationToken token)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(token);
        timeout.CancelAfter(TimeSpan.FromSeconds(2));
        try
        {
            var end = await _log.EndOffsetAsync(_log.LeaderPartition, timeout.Token).ConfigureAwait(false);
            lock (_lock)
                return end <= _watchPosition && _leaderResigned == resigned;
        }
        catch (Exception ex) when (!token.IsCancellationRequested)
        {
            _logger?.LogWarning(ex, "Node {Node} hears no leader but cannot reach Kafka either; not taking over.", NodeId);
            return false;
        }
    }

    private async Task<bool> PromoteAsync(string reason, TimeSpan silence, CancellationToken token)
    {
        SetRole(NodeRole.Promoting);
        _logger?.LogInformation("Node {Node} is taking over ({Reason}; leader silent for {Silence}).", NodeId, reason, silence);
        var clock = Stopwatch.StartNew();
        try
        {
            // Fences the previous leader and settles its last transaction; commits the term.
            await _log.AcquireWriterAsync(token).ConfigureAwait(false);
            var fence = clock.Elapsed;

            if (_options.PrepareAsync is { } prepare)
                await prepare(token).ConfigureAwait(false);
            var prepared = clock.Elapsed;

            // Tables before circuits: a circuit's committed table version must exist.
            ITrackedTable[] tables;
            ChangelogStorage[] storages;
            lock (_lock)
            {
                tables = [.. _tables];
                storages = [.. _storages];
            }
            foreach (var table in tables)
                await table.PromoteAsync(token).ConfigureAwait(false);
            var tablesDone = clock.Elapsed;
            foreach (var storage in storages)
            {
                if (!storage.IsLeader)
                    await storage.PromoteAsync(token).ConfigureAwait(false);
            }
            var storageDone = clock.Elapsed;

            await _options.StartLeadingAsync(_log.Term, token).ConfigureAwait(false);
            clock.Stop();

            if (_log.FencedBy is { } fenced)
                throw fenced;
            lock (_lock)
            {
                _lastTakeover = new TakeoverTimings(
                    reason, silence, fence, prepared - fence, tablesDone - prepared, storageDone - tablesDone,
                    clock.Elapsed - storageDone, clock.Elapsed);
                _lag = null;
            }
            SetRole(NodeRole.Leader);
            _logger?.LogInformation("Node {Node} leads under term {Term} after {Elapsed}.", NodeId, _log.Term, clock.Elapsed);
            return true;
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            // Whether it lost the race for the transactional id or failed half way, this
            // node may hold promoted tables and storage and cannot follow again. It must
            // also stop heartbeating, or its peer would wait on a leader that leads nothing.
            _logger?.LogError(ex, "Node {Node} could not complete its takeover.", NodeId);
            _log.Depose($"Node '{NodeId}' could not complete its takeover ({ex.Message}).");
            Finish(NodeRole.Fenced, ex);
            return false;
        }
    }

    private void Finish(NodeRole role, Exception? fault)
    {
        lock (_lock)
        {
            if (_role is NodeRole.Fenced or NodeRole.Resigned)
                return;
            _fault = fault;
        }
        SetRole(role);
        _whenFinished.TrySetResult();
    }

    private void SetRole(NodeRole role)
    {
        TaskCompletionSource changed;
        lock (_lock)
        {
            if (_role == role || _role is NodeRole.Fenced or NodeRole.Resigned)
                return;
            _role = role;
            changed = _roleChanged;
            _roleChanged = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }
        changed.TrySetResult();
        Raise();
    }

    // Handlers are called one at a time and in order, off the threads that change state.
    private Task _raising = Task.CompletedTask;

    private void Raise()
    {
        if (Changed is null)
            return;
        var status = Status;
        lock (_lock)
        {
            _raising = _raising.ContinueWith(_ =>
            {
                try
                {
                    Changed?.Invoke(status);
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, "Node {Node}: a leadership Changed handler threw.", NodeId);
                }
            }, CancellationToken.None, TaskContinuationOptions.None, TaskScheduler.Default);
        }
    }

    /// <summary>Stops watching and electing. It does not step down and does not dispose
    /// the log: a leader stays the log's writer until the log is disposed or fenced.</summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;
        _stop.Cancel();
        await Task.WhenAll(_run, _watch, _sampler).ContinueWith(static _ => { }, TaskScheduler.Default).ConfigureAwait(false);
        Task raising;
        lock (_lock)
            raising = _raising;
        await raising.ConfigureAwait(false);
        _stop.Dispose();
    }
}
