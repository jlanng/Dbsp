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
    /// promoting its tables and storage and starting its circuits. A takeover that is
    /// given up or lost before the node is the log's writer — the leader was heard before
    /// the log was taken, or another node took it first — goes back to
    /// <see cref="Standby"/>; once it is the writer the only ways on are
    /// <see cref="Leader"/> and <see cref="Fenced"/>.</summary>
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
    /// saying the leader is alive. Past it the standby takes over whatever the probe
    /// says — unless the brokers say outright that nothing can be committed (a partition
    /// of the log without a leader or with too few replicas in sync), in which case it
    /// waits for them. Default 30 s, which leaves room for a leader's client to find a
    /// broker that has stopped answering twice over (<see cref="KafkaClientDefaults"/>).</summary>
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
    Exception? Fault)
{
    /// <summary>The node is <see cref="NodeRole.Fenced"/> by its own doing: it took the
    /// log over and then could not complete the takeover — a table, the storage or a
    /// circuit failed it — so it closed its writer. Nobody replaced it. A process that
    /// ends this way has not "lost the leadership to another node", and whatever starts
    /// it again should not start it again at once for ever: the same failure is likely
    /// waiting (the service exits with code 5 for it, not 3).</summary>
    public bool TakeoverFailed { get; init; }
}

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
/// A takeover can end before this node is the log's writer, and three things end it:
/// the leader is heard while this node has not yet taken the transactional id (it was
/// slow, not gone — its brokers were) — once the id is taken the leader is fenced, and
/// nothing heard afterwards stops the takeover; another node, or the old leader's own
/// client asking the coordinator for a new epoch, fences this one before its term is
/// committed; the attempt fails for a reason of the brokers'. Nothing has been promoted
/// then, so the node is a <see cref="NodeRole.Standby"/> again and decides afresh. And at
/// <see cref="KafkaLeadershipOptions.TakeoverCeiling"/> a standby does not take over from
/// a leader that still answers its probe while the brokers say that nobody can commit:
/// it could do no better in the leader's place. <c>docs/design/durability-and-standby.md</c>,
/// <i>As built: broker failures</i>, has what each of these was found by.
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

    // Signs of life short of a readable heartbeat: the leader topic's end as last sampled
    // with an open transaction's records counted (-1: no sample since the last record was
    // heard), and when it was last seen to move.
    private long _uncommittedEnd = -1;
    private long _progressTicks;
    private Task? _stepDown;
    private bool _manual;
    private TaskCompletionSource? _manualLost;

    // While this node is acquiring the writer: cancelled when the leader is heard after
    // all (Heard), so that a takeover that is no longer needed is given up before it
    // fences anybody. The log looks at it only until the transactional id is taken
    // (KafkaLog.AcquireWriterUnlessAsync): from then on the leader is fenced and
    // the takeover goes through, whatever is heard.
    private CancellationTokenSource? _abandon;
    private long _abandonWithin;
    private long _lateSaidTicks = long.MinValue / 2;
    private readonly HeartbeatDating _dating = new();
    private long _notBeforeTicks;
    private long _takeoversAbandoned;
    private long _takeoversHeld;
    private long _lastHeldTicks;
    private long _lastHeldLogTicks;
    private FollowerLag? _lag;
    private TakeoverTimings? _lastTakeover;
    private Exception? _fault;
    private bool _takeoverFailed;

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

    /// <summary>Takeovers this node began and gave up before it was the writer, going back
    /// to <see cref="NodeRole.Standby"/>: the leader was heard before this node had taken
    /// the transactional id, or another node initialised it first.</summary>
    public long TakeoversAbandoned => Interlocked.Read(ref _takeoversAbandoned);

    /// <summary>The earliest this node may begin another takeover by itself, as
    /// <see cref="Environment.TickCount64"/>; 0 when nothing holds it back. For tests.</summary>
    internal long RetryNotBefore => Interlocked.Read(ref _notBeforeTicks);

    /// <summary>Times this node reached <see cref="KafkaLeadershipOptions.TakeoverCeiling"/>
    /// with the leader still answering its probe and did not take over, because the
    /// brokers said nothing could be committed by anyone.</summary>
    public long TakeoversHeld => Interlocked.Read(ref _takeoversHeld);

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
                    HoldsLease, _role is NodeRole.Standby ? _lag : null, _lastTakeover, _fault) { TakeoverFailed = _takeoverFailed };
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

        // What the ceiling's question to the brokers needs and cannot ask in an outage.
        if (_options.ProbeLeader is not null)
            _ = _log.PrimeWritePathAsync(_stop.Token);
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
        TaskCompletionSource lost;
        lock (_lock)
        {
            if (_role is NodeRole.Fenced or NodeRole.Resigned)
                throw new InvalidOperationException($"Node '{NodeId}' is {_role} and cannot take over; restart it as a standby.");
            _manual = true;
            lost = _manualLost ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }
        var settled = WhenRoleAsync(r => r is not (NodeRole.Standby or NodeRole.Promoting), cancellationToken);

        // A takeover that is lost before this node is the writer leaves it a standby: the
        // request has failed, and the node can be asked again.
        await Task.WhenAny(settled, lost.Task).ConfigureAwait(false);
        if (!settled.IsCompleted)
            await lost.Task.ConfigureAwait(false);
        var role = await settled.ConfigureAwait(false);
        if (role is not NodeRole.Leader)
            throw new KafkaLogFencedException($"Node '{NodeId}' did not become the leader: it is {role}.", Status.Fault);
    }

    /// <summary>
    /// The leader's "step down": it stops its circuits
    /// (<see cref="KafkaLeadershipOptions.StopLeadingAsync"/>), commits a resignation to the
    /// leader topic — on reading it a standby takes over at once, with no silence to wait
    /// out — and ends as <see cref="NodeRole.Resigned"/>. The process should then exit and
    /// return as a standby. Calls made while one is under way, or after it, share its
    /// outcome: the circuits are stopped once and one resignation is written.
    /// </summary>
    public Task StepDownAsync(CancellationToken cancellationToken = default)
    {
        Task stepDown;
        lock (_lock)
        {
            // One step-down, however many ask: a second caller waits for the first's.
            if (_stepDown is null)
            {
                if (_role is not NodeRole.Leader)
                    return Task.FromException(new InvalidOperationException($"Node '{NodeId}' is {_role}; only the leader can step down."));
                _stepDown = Task.Run(() => StepDownCoreAsync(cancellationToken), CancellationToken.None);
            }
            stepDown = _stepDown;
        }
        return stepDown.WaitAsync(cancellationToken);
    }

    private async Task StepDownCoreAsync(CancellationToken cancellationToken)
    {
        try
        {
            if (_options.StopLeadingAsync is { } stop)
                await stop(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            // Nothing was resigned: the node still leads, and may be asked again.
            lock (_lock)
                _stepDown = null;
            throw;
        }

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
        catch (Exception ex)
        {
            // The resignation did not commit (its transaction was aborted past the retry
            // limit) and the writer is still usable — so it would go on heartbeating for
            // a node whose circuits are already stopped, and the standby would wait on a
            // leader that leads nothing. A node that cannot lead must stop heartbeating:
            // the writer is closed, as for a failed takeover, and the standby takes over
            // on the silence instead of on the resignation.
            _logger?.LogError(ex, "Node {Node} stopped its circuits but could not commit its resignation; closing its writer.", NodeId);
            _log.Depose($"Node '{NodeId}' stepped down but could not commit its resignation ({ex.Message}).");
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

    internal void Heard(KafkaLog.LeaderRecord record) => Heard(record, Environment.TickCount64);

    internal void Heard(KafkaLog.LeaderRecord record, long now)
    {
        bool leaderChanged;
        CancellationTokenSource? abandon = null;
        TimeSpan? late;
        lock (_lock)
        {
            if (record.Term < _leaderTerm)
                return; // a record older than one already seen: only a restarted watch reads those
            leaderChanged = record.Term != _leaderTerm || record.Node != _leaderNode || record.Resigned != _leaderResigned;
            _leaderTerm = record.Term;
            _log.NoteTermSeen(record.Term);
            _leaderNode = record.Node;
            _leaderAt = record.At;
            _leaderResigned = record.Resigned;

            // The leader was alive when it wrote this, which is the moment the silence
            // counts from — and not always the moment of reading: the last heartbeat of
            // a leader that died with a broker is readable only when the brokers give
            // that broker up, seconds later (HeartbeatDating). Read at once, as almost
            // every record is, the two are the same moment.
            var written = _dating.Written(record.Term, record.At.ToUnixTimeMilliseconds(), now);
            if (written > _heardTicks)
                _heardTicks = written;
            _uncommittedEnd = -1;

            // A leader that has written within the takeover timeout is a leader: the
            // brokers' word that held a takeover back is no longer needed.
            var age = now - written;
            if (age < (long)_options.TakeoverAfter.TotalMilliseconds)
                Interlocked.Exchange(ref _lastHeldTicks, 0);

            // Heard while this node is on its way to fencing it: the leader is alive and
            // committing, so the takeover is given up while that is still free. Not a
            // resignation, and not this node's own term record — and not a record so old
            // that the takeover's reason stands with it: the silence it was decided on
            // (or, at the ceiling, the ceiling) has passed since the leader wrote it.
            if (!record.Resigned && record.Node != NodeId && age < _abandonWithin)
                abandon = _abandon;
            late = age >= (long)_options.TakeoverAfter.TotalMilliseconds && record.Node != NodeId && now - _lateSaidTicks >= 5_000
                ? TimeSpan.FromMilliseconds(age)
                : null;
            if (late is not null)
                _lateSaidTicks = now;
        }

        // Worth a line: it is how a takeover that went on through "a heartbeat" is read
        // afterwards, and how brokers that sat on the leader's records show.
        if (late is { } old)
            _logger?.LogInformation(
                "Node {Node} read a record that node {Leader} wrote {Age} before: the brokers had not shown it until now. The leader's silence counts from when it was written.",
                NodeId, record.Node, old);
        try
        {
            abandon?.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // The acquisition ended a moment ago.
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
            while (true)
            {
                if (await WaitToTakeOverAsync(token).ConfigureAwait(false) is not { } decision)
                    return;
                var outcome = await PromoteAsync(decision.Reason, decision.Silence, token).ConfigureAwait(false);
                if (outcome == Promotion.Failed)
                    return;
                if (outcome == Promotion.Led)
                    break;
            }

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
        var beat = _log.Options.HeartbeatInterval;
        var sampleEvery = Math.Max((long)tick.TotalMilliseconds, (long)beat.TotalMilliseconds / 4);
        var nextProbe = 0L;
        var nextSample = 0L;
        while (true)
        {
            bool manual, resigned;
            TimeSpan silence, quiet;
            lock (_lock)
            {
                if (_role is not NodeRole.Standby)
                    return null;
                manual = _manual;
                resigned = _leaderResigned;
                (silence, quiet) = Silence();
            }

            if (manual)
                return ("manual", silence);
            if (_options.AutomaticTakeover && Environment.TickCount64 >= Interlocked.Read(ref _notBeforeTicks))
            {
                // A heartbeat is late. Before the silence is long enough to act on, start
                // watching for the other sign of life: a leader inside a long transaction.
                if (!resigned && silence > beat * 1.25 && Environment.TickCount64 >= nextSample)
                {
                    await NoteProgressAsync(token).ConfigureAwait(false);
                    nextSample = Environment.TickCount64 + sampleEvery;
                    lock (_lock)
                        (silence, quiet) = Silence();
                }

                string? reason = null;
                if (resigned)
                    reason = "resigned";
                else if (silence >= _options.TakeoverCeiling)
                    reason = "ceiling";
                else if (quiet >= _options.TakeoverAfter)
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

                // At the ceiling the leader still answers its probe (or the probe would
                // have decided long ago). If the brokers say that nobody can commit, its
                // silence is theirs and this node could do no better in its place.
                if (reason == "ceiling" && _options.ProbeLeader is not null && await HeldByBrokersAsync(silence, token).ConfigureAwait(false))
                    reason = null;

                if (reason is not null && await ConfirmSilenceAsync(resigned, token).ConfigureAwait(false))
                {
                    // The reason was found on a silence measured before the probe, the
                    // brokers and the topic's end were asked, and those answers take
                    // seconds when brokers are slow to give them — exactly when a leader
                    // that was held up is about to be heard. It is looked at again now: a
                    // leader heard meanwhile is a leader, and nothing is decided.
                    lock (_lock)
                    {
                        (silence, quiet) = Silence();
                        var stands = reason switch
                        {
                            "ceiling" => silence >= _options.TakeoverCeiling,
                            "silence" => quiet >= _options.TakeoverAfter,
                            _ => _leaderResigned,
                        };
                        if (stands)
                            return (reason, silence);
                    }
                }
            }
            await Task.Delay(tick, token).ConfigureAwait(false);
        }
    }

    /// <summary>How long since a record was read from the leader topic, and how long since
    /// any sign of life: that, or the topic's end moving under an open transaction. The
    /// caller holds the lock.</summary>
    private (TimeSpan Silence, TimeSpan Quiet) Silence()
    {
        var now = Environment.TickCount64;
        return (TimeSpan.FromMilliseconds(now - _heardTicks),
            TimeSpan.FromMilliseconds(now - Math.Max(_heardTicks, _progressTicks)));
    }

    /// <summary>
    /// Looks for a leader that is alive inside one long transaction. Its heartbeat is a
    /// record in that transaction, readable only when the transaction commits, so a commit
    /// that takes longer than <see cref="KafkaLeadershipOptions.TakeoverAfter"/> reads as
    /// silence. The log writer therefore keeps adding heartbeats to a transaction while it
    /// produces and flushes it; nobody can read them yet, but each moves the leader topic's
    /// end as an uncommitted read sees it. Only the current writer can move it — a fenced
    /// producer's records are refused and a dead one writes nothing — so an end that has
    /// moved since the last sample is a live leader, and counts against the silence like a
    /// probe that answers: until <see cref="KafkaLeadershipOptions.TakeoverCeiling"/>.
    /// </summary>
    private async Task NoteProgressAsync(CancellationToken token)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(token);
        timeout.CancelAfter(TimeSpan.FromSeconds(1));
        try
        {
            var end = await _log.UncommittedEndOffsetAsync(_log.LeaderPartition, timeout.Token).ConfigureAwait(false);
            lock (_lock)
            {
                // The first sample after a heartbeat is only a baseline: the end may hold
                // the first record of a transaction whose writer died a moment later.
                if (_uncommittedEnd >= 0 && end > _uncommittedEnd)
                    _progressTicks = Environment.TickCount64;
                _uncommittedEnd = Math.Max(_uncommittedEnd, end);
            }
        }
        catch (Exception) when (!token.IsCancellationRequested)
        {
            // Unreachable: no news either way. The last check before a takeover says so.
        }
    }

    /// <summary>Whether a takeover at the ceiling should wait: the leader answers its
    /// probe as the leader, and the brokers say outright that the log cannot be written
    /// (<see cref="KafkaLog.ProbeWritePathAsync"/>: a partition without a leader that
    /// answers, or with too few replicas in sync that answer) or said so until a moment
    /// ago. Anything short of that — a leader that no longer answers, brokers that cannot
    /// be asked or see nothing wrong — and the ceiling stands.</summary>
    private async Task<bool> HeldByBrokersAsync(TimeSpan silence, CancellationToken token)
    {
        if (!await ProbeAsync(token).ConfigureAwait(false))
            return false;
        var (path, why) = await _log.ProbeWritePathAsync(token).ConfigureAwait(false);
        var now = Environment.TickCount64;
        if (path != KafkaLog.WritePath.Unwritable)
        {
            // The brokers can commit again, or it cannot be told. A leader that was held
            // up by them needs a moment to say so — its client has to find them first —
            // and gets two takeover timeouts from the last "no" before its silence is
            // its own again.
            var last = Interlocked.Read(ref _lastHeldTicks);
            return last != 0 && now - last < (long)(_options.TakeoverAfter.TotalMilliseconds * 2);
        }
        Interlocked.Exchange(ref _lastHeldTicks, now);
        Interlocked.Increment(ref _takeoversHeld);
        if (now - Interlocked.Read(ref _lastHeldLogTicks) >= 10_000 || Interlocked.Read(ref _lastHeldLogTicks) == 0)
        {
            Interlocked.Exchange(ref _lastHeldLogTicks, now);
            _logger?.LogWarning(
                "Node {Node}: the leader has been silent for {Silence} and still answers its probe, and nothing can be committed ({Why}): not taking over, which could do no better.",
                NodeId, silence, why);
        }
        return true;
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

    private enum Promotion
    {
        /// <summary>This node is the leader.</summary>
        Led,

        /// <summary>The takeover was given up or lost before this node was the writer:
        /// it is a standby again.</summary>
        Lost,

        /// <summary>The takeover failed with this node the writer, or the node is being
        /// disposed: final.</summary>
        Failed,
    }

    private async Task<Promotion> PromoteAsync(string reason, TimeSpan silence, CancellationToken token)
    {
        SetRole(NodeRole.Promoting);
        _logger?.LogInformation("Node {Node} is taking over ({Reason}; leader silent for {Silence}).", NodeId, reason, silence);
        var clock = Stopwatch.StartNew();
        try
        {
            // Fences the previous leader and settles its last transaction; commits the term.
            // Until the transactional id is taken the takeover costs nothing to give up,
            // and it is given up when the leader is heard after all (a manual takeover is
            // the operator's word against the leader's, and is not). The log decides
            // where "until" ends: once it has the id the leader is fenced, and it
            // carries the acquisition through whatever this node hears meanwhile — a
            // takeover given up there would leave a fenced leader and a standby that had
            // just stood down.
            using (var abandon = new CancellationTokenSource())
            {
                lock (_lock)
                {
                    _abandon = reason == "manual" ? null : abandon;

                    // How lately the leader must have written for a record of its to
                    // call this off: within the silence the takeover was decided on.
                    // After a resignation any record that is not one does.
                    _abandonWithin = reason switch
                    {
                        "silence" => (long)_options.TakeoverAfter.TotalMilliseconds,
                        "ceiling" => (long)_options.TakeoverCeiling.TotalMilliseconds,
                        _ => long.MaxValue,
                    };
                }
                Exception? lost = null;
                try
                {
                    await _log.AcquireWriterUnlessAsync(abandon.Token, token).WaitAsync(token).ConfigureAwait(false);
                }
                catch (Exception ex) when (!token.IsCancellationRequested && ex is not ObjectDisposedException)
                {
                    lost = ex;
                }
                finally
                {
                    lock (_lock)
                        _abandon = null;
                }
                if (lost is not null)
                {
                    // A log that is finished is a takeover that failed. A reader that
                    // has promoted nothing stands by again.
                    if (_log.IsFenced)
                        System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(lost).Throw();
                    if (!_log.IsWriter)
                    {
                        // Which of the two it was is what ended the acquisition, not
                        // what has been heard since: a takeover lost to another node's
                        // fence hears that node's term a moment later.
                        BackToStandby(reason, lost, heard: lost is KafkaLogNotAcquiredException { InnerException: OperationCanceledException });
                        return Promotion.Lost;
                    }
                }
            }
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
            return Promotion.Led;
        }
        catch (OperationCanceledException ex) when (token.IsCancellationRequested)
        {
            // Disposed part-way. If the log is already the writer, this node has fenced
            // the previous leader and leads nothing: left alone its log would heartbeat on
            // and hold every standby back. Close the writer, as for any failed takeover.
            // The acquisition was told to stop with the same token; it is waited for,
            // because it may be committing its term this instant.
            await _log.WhenAcquisitionSettlesAsync().ConfigureAwait(false);
            if (_log.IsWriter)
            {
                _log.Depose($"Node '{NodeId}' was stopped in the middle of its takeover.");
                Finish(NodeRole.Fenced, ex);
            }
            throw;
        }
        catch (Exception ex)
        {
            // Whether it lost the race for the transactional id or failed half way, this
            // node may hold promoted tables and storage and cannot follow again. It must
            // also stop heartbeating, or its peer would wait on a leader that leads nothing.
            // Which of the two it was is kept: a node another node fenced has lost the
            // leadership, and one that failed half way has failed (TakeoverFailed).
            var own = _log.FencedBy is null && ex is not KafkaLogFencedException;
            _logger?.LogError(ex, "Node {Node} could not complete its takeover.", NodeId);
            _log.Depose($"Node '{NodeId}' could not complete its takeover ({ex.Message}).");
            lock (_lock)
                _takeoverFailed = own && _role is not (NodeRole.Fenced or NodeRole.Resigned);
            Finish(NodeRole.Fenced, ex);
            return Promotion.Failed;
        }
    }

    /// <summary>
    /// A takeover that ended before this node was the writer: nothing was promoted and
    /// nothing of this node's was committed, so it stands by again instead of ending as
    /// <see cref="NodeRole.Fenced"/>.
    /// <para>
    /// Lost to a fence (another node, or the old leader's own stale request, took the
    /// transactional id first): the wait before it may try again is the takeover timeout
    /// and a random part of it more, so that two nodes that fenced each other do not do
    /// it again in step, and the silence counts from now.
    /// </para>
    /// <para>
    /// Given up because the leader was <paramref name="heard"/>: nothing holds the next
    /// attempt back, and the silence counts from the record that was heard. That matters
    /// for the one way such a takeover can still have cost the leader its writer — a
    /// request for the transactional id that had left this node before the client was put
    /// away, and lands. The leader's next transaction then fails, it ends, and this node
    /// must take over on that silence exactly as on any dead leader's: one takeover
    /// timeout, not two or three.
    /// </para>
    /// </summary>
    private void BackToStandby(string reason, Exception why, bool heard)
    {
        TaskCompletionSource? manual;
        lock (_lock)
        {
            if (!heard)
            {
                _heardTicks = Environment.TickCount64;
                _uncommittedEnd = -1;
            }
            manual = _manualLost;
            _manualLost = null;
            _manual = false;
        }
        Interlocked.Exchange(
            ref _notBeforeTicks,
            heard ? 0 : Environment.TickCount64 + (long)(_options.TakeoverAfter.TotalMilliseconds * (1 + Random.Shared.NextDouble())));
        if (heard)
            _logger?.LogWarning(
                "Node {Node} gave its takeover ({Reason}) up: the leader was heard before this node had taken the log. It stands by again.", NodeId, reason);
        else
            _logger?.LogWarning(why, "Node {Node} lost its takeover ({Reason}) before it was the writer and stands by again.", NodeId, reason);
        SetRole(NodeRole.Standby);
        Interlocked.Increment(ref _takeoversAbandoned);     // last: whoever waits for the count finds the rest done
        manual?.TrySetException(new KafkaLogNotAcquiredException(
            $"Node '{NodeId}' did not become the leader: its takeover was lost before it was the writer ({why.Message}). It stands by again.", why));
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
    /// the log: a leader stays the log's writer until the log is disposed or fenced. A
    /// node caught in the middle of a takeover is different: it is not the leader and
    /// cannot become one now, so if it had already become the writer its writer is closed
    /// and it ends as <see cref="NodeRole.Fenced"/>.</summary>
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
