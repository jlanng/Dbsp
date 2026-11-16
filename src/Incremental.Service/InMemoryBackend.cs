using System.Text.Json;
using Incremental.Kafka;
using Incremental.Storage;
using Incremental.Tables;

namespace Incremental.Service;

public sealed class InMemoryBackendOptions
{
    /// <summary>Versions of history each table log keeps on the heap. Every retained
    /// version holds its old and new rows, so this is small.</summary>
    public int RetainedHistoryVersions { get; init; } = 1024;

    /// <summary>Row changes each table log keeps across those versions: the bound that
    /// holds when single versions are large (a first load is one version per chunk, or one
    /// for the whole table). The newest version is kept whatever its size.</summary>
    public long RetainedHistoryChanges { get; init; } = 1_000_000;

    /// <summary>
    /// Records a partition of the state changelog may hold before it compacts itself: the
    /// last record of every key is kept, and a tombstone goes once every follower has read
    /// it. After a compaction the next is due at twice what survived, so the log holds at
    /// most about twice the live entries of the pipelines' state plus this number per
    /// partition — it does not grow with the time a node has been up. 0 turns it off:
    /// every record ever committed is then kept.
    /// </summary>
    public int StateCompactAbove { get; init; } = 8192;

    /// <summary>Partitions of the in-memory state changelog (when the cluster is created
    /// by the backend rather than passed in).</summary>
    public int StatePartitions { get; init; } = 4;

    /// <summary>
    /// When set, the leader writes <c>catalog.json</c> and <c>blobs/{sha256}</c> here after
    /// each catalog commit (write-then-rename) and a first leader loads them. Tables and
    /// pipeline state are still new in every process, so this restores <em>what is
    /// deployed</em>, not its data: pipelines start empty and replay their sources.
    /// </summary>
    public string? CatalogDirectory { get; init; }

    /// <summary>
    /// How long a takeover waits for a sink write the deposed leader had begun before it
    /// was fenced. A leader that is replaced starts no write after that moment; one already
    /// inside a sink is let finish before the new leader prepares anything, so that the
    /// sink sees the old leader's last batch before the new leader's first and not after
    /// it. A write that outlasts this is waited for no longer — the takeover goes on, and
    /// the batch may then land late, as it can behind a Kafka node's lease.
    /// </summary>
    public TimeSpan DeposedWriteTimeout { get; init; } = TimeSpan.FromSeconds(5);
}

/// <summary>
/// The logs several in-memory nodes share: one state changelog, one log per table, the
/// blobs, and who leads. Two <see cref="InMemoryServiceBackend"/>s made from one cluster
/// (<see cref="Node"/>) are a leader and a standby in one process, with real fencing and
/// no broker — which is what makes the takeover order testable. A cluster that outlives
/// its nodes also stands in for durable storage: a node started on it later finds the
/// catalog, the tables and every pipeline's checkpoint.
/// </summary>
public sealed class InMemoryServiceCluster
{
    internal readonly object Gate = new();
    internal readonly Dictionary<string, object> TableLogs = new(StringComparer.Ordinal);
    internal readonly Dictionary<string, byte[]> Blobs = new(StringComparer.Ordinal);
    internal readonly List<InMemoryServiceBackend> Nodes = [];
    internal TaskCompletionSource TableCreated = new(TaskCreationOptions.RunContinuationsAsynchronously);
    internal InMemoryServiceBackend? Leader;
    internal long Term;

    /// <summary>The last leader a takeover deposed, as the term it led: what a node of that
    /// name started again reports as <see cref="NodeLeadershipStatus.Previous"/>.</summary>
    internal (string Node, PreviousTerm Previous)? LastDeposed;

    /// <summary>A leader that stepped down and has no successor yet: the next leader
    /// records it in <see cref="LastDeposed"/>, as a takeover after a resignation.</summary>
    internal (string Node, long Term)? Resigned;

    public InMemoryServiceCluster(InMemoryBackendOptions? options = null)
    {
        Options = options ?? new InMemoryBackendOptions();
        State = new InMemoryStateChangelog(Options.StatePartitions, Options.StateCompactAbove);
    }

    public InMemoryBackendOptions Options { get; }

    public InMemoryStateChangelog State { get; }

    /// <summary>A backend for one node of this cluster. The first node started leads at
    /// once; another leads on <c>TakeOverAsync</c> or when the leader resigns.</summary>
    public InMemoryServiceBackend Node(string nodeId) => new(this, nodeId);
}

/// <summary>
/// The backend with no broker: <see cref="InMemoryTableLog{TKey,TRow}"/> per table and one
/// <see cref="InMemoryStateChangelog"/>, held in an <see cref="InMemoryServiceCluster"/>.
/// There are no heartbeats and no automatic election. A leased sink writes only while its
/// node is the one the cluster calls leader: the node a takeover replaces begins no write
/// after it, and the takeover waits for the one it was in the middle of
/// (<see cref="InMemoryBackendOptions.DeposedWriteTimeout"/>).
/// </summary>
public sealed class InMemoryServiceBackend : IServiceBackend, ICatalogMirror
{
    private readonly InMemoryServiceCluster _cluster;
    private readonly NodeLeadership _leadership;
    private readonly BlobStore _blobs;

    // Each table log connection's TakeOver, called when this node acquires the writer.
    private readonly List<Action> _connections = [];

    public InMemoryServiceBackend(InMemoryBackendOptions? options = null, string nodeId = "node")
        : this(new InMemoryServiceCluster(options), nodeId)
    {
    }

    internal InMemoryServiceBackend(InMemoryServiceCluster cluster, string nodeId)
    {
        ArgumentException.ThrowIfNullOrEmpty(nodeId);
        _cluster = cluster;
        NodeId = nodeId;
        _leadership = new NodeLeadership(this);
        _blobs = new BlobStore(cluster);
    }

    public string NodeId { get; }

    public Task ConnectAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public InMemoryServiceCluster Cluster => _cluster;
    public IStateChangelog State => _cluster.State;
    public int RetainedHistoryVersions => _cluster.Options.RetainedHistoryVersions;
    public IBlobStore Blobs => _blobs;
    public INodeLeadership Leadership => _leadership;

    public bool InProcess => true;

    public BackendInfo Describe() => new(
        "in-memory",
        new Dictionary<string, string>
        {
            ["retainedHistoryVersions"] = RetainedHistoryVersions.ToString(),
            ["retainedHistoryChanges"] = _cluster.Options.RetainedHistoryChanges.ToString(),
            ["stateCompactAbove"] = _cluster.Options.StateCompactAbove.ToString(),
            ["statePartitions"] = _cluster.State.PartitionCount.ToString(),
            ["catalogDirectory"] = _cluster.Options.CatalogDirectory ?? "",
        },
        new Dictionary<string, long>
        {
            ["stateRecordsAppended"] = _cluster.State.RecordsAppended,
            ["stateTransactionsCommitted"] = _cluster.State.TransactionsCommitted,
            ["stateRecords"] = _cluster.State.RecordCount,
            ["stateCompactions"] = _cluster.State.Compactions,
        });

    public async Task<ITableLog<TKey, TRow>> OpenTableLogAsync<TKey, TRow>(string name, CancellationToken cancellationToken)
        where TKey : notnull
        where TRow : class
    {
        while (true)
        {
            Task created;
            lock (_cluster.Gate)
            {
                var writer = ReferenceEquals(_cluster.Leader, this);
                if (_cluster.TableLogs.TryGetValue(name, out var existing))
                {
                    var root = existing as InMemoryTableLog<TKey, TRow>
                        ?? throw new InvalidOperationException(
                            $"Table log '{name}' holds {existing.GetType().GetGenericArguments()[1]} by {existing.GetType().GetGenericArguments()[0]}, " +
                            $"not {typeof(TRow)} by {typeof(TKey)}. The same type name loaded twice is two types.");
                    var connection = root.Connect();
                    _connections.Add(connection.TakeOver);
                    if (writer)
                        connection.TakeOver();
                    return connection;
                }
                if (writer)
                {
                    var log = new InMemoryTableLog<TKey, TRow>(RetainedHistoryVersions, _cluster.Options.RetainedHistoryChanges);
                    _cluster.TableLogs[name] = log;
                    _connections.Add(log.TakeOver);
                    var signal = _cluster.TableCreated;
                    _cluster.TableCreated = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    signal.TrySetResult();
                    return log;
                }
                created = _cluster.TableCreated.Task;
            }
            await created.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    public ValueTask DisposeAsync()
    {
        lock (_cluster.Gate)
        {
            _cluster.Nodes.Remove(this);
            if (ReferenceEquals(_cluster.Leader, this))
                _cluster.Leader = null;
        }
        return ValueTask.CompletedTask;
    }

    // ── the catalog mirror ─────────────────────────────────────────────────────────────

    private string? CatalogFile =>
        _cluster.Options.CatalogDirectory is { } directory ? Path.Combine(directory, "catalog.json") : null;

    async Task<IReadOnlyList<CatalogEntry>?> ICatalogMirror.LoadAsync(CancellationToken cancellationToken)
    {
        if (CatalogFile is not { } file || !File.Exists(file))
            return null;
        await using var stream = File.OpenRead(file);
        var entries = await JsonSerializer.DeserializeAsync<Dictionary<string, string>>(stream, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        return entries?.Select(e => new CatalogEntry(e.Key, e.Value)).ToList();
    }

    async Task ICatalogMirror.SaveAsync(IReadOnlyCollection<CatalogEntry> entries, CancellationToken cancellationToken)
    {
        if (CatalogFile is not { } file)
            return;
        Directory.CreateDirectory(Path.GetDirectoryName(file)!);
        var temporary = file + ".tmp";
        await using (var stream = File.Create(temporary))
        {
            await JsonSerializer.SerializeAsync(
                stream, entries.OrderBy(e => e.Key, StringComparer.Ordinal).ToDictionary(e => e.Key, e => e.Value),
                new JsonSerializerOptions { WriteIndented = true }, cancellationToken).ConfigureAwait(false);
        }
        File.Move(temporary, file, overwrite: true);
    }

    // ── blobs ──────────────────────────────────────────────────────────────────────────

    private sealed class BlobStore(InMemoryServiceCluster cluster) : IBlobStore
    {
        private string? PathOf(string sha256) =>
            cluster.Options.CatalogDirectory is { } directory ? Path.Combine(directory, "blobs", sha256) : null;

        public ValueTask<bool> ExistsAsync(string sha256, CancellationToken cancellationToken)
        {
            lock (cluster.Blobs)
                if (cluster.Blobs.ContainsKey(sha256))
                    return new ValueTask<bool>(true);
            return new ValueTask<bool>(PathOf(sha256) is { } path && File.Exists(path));
        }

        public async Task PutAsync(string sha256, Stream content, CancellationToken cancellationToken)
        {
            using var buffer = new MemoryStream();
            await content.CopyToAsync(buffer, cancellationToken).ConfigureAwait(false);
            var bytes = buffer.ToArray();
            lock (cluster.Blobs)
                cluster.Blobs[sha256] = bytes;
            if (PathOf(sha256) is { } path && !File.Exists(path))
            {
                Directory.CreateDirectory(Path.GetDirectoryName(path)!);
                var temporary = path + "." + Guid.NewGuid().ToString("N") + ".tmp";
                await File.WriteAllBytesAsync(temporary, bytes, cancellationToken).ConfigureAwait(false);
                File.Move(temporary, path, overwrite: true);
            }
        }

        public async Task<bool> CopyToAsync(string sha256, Stream destination, CancellationToken cancellationToken)
        {
            byte[]? bytes;
            lock (cluster.Blobs)
                cluster.Blobs.TryGetValue(sha256, out bytes);
            if (bytes is null && PathOf(sha256) is { } path && File.Exists(path))
                bytes = await File.ReadAllBytesAsync(path, cancellationToken).ConfigureAwait(false);
            if (bytes is null)
                return false;
            await destination.WriteAsync(bytes, cancellationToken).ConfigureAwait(false);
            return true;
        }

        public Task DeleteAsync(string sha256, CancellationToken cancellationToken)
        {
            lock (cluster.Blobs)
                cluster.Blobs.Remove(sha256);
            if (PathOf(sha256) is { } path)
                File.Delete(path);
            return Task.CompletedTask;
        }
    }

    // ── leadership ─────────────────────────────────────────────────────────────────────

    private sealed class NodeLeadership(InMemoryServiceBackend backend) : INodeLeadership
    {
        private readonly InMemoryServiceCluster _cluster = backend._cluster;
        private readonly List<ChangelogStorage> _storages = [];
        private readonly TaskCompletionSource _finished = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private LeadershipCallbacks? _callbacks;
        private NodeRole _role = NodeRole.Standby;
        private long _term;
        private bool _takeoverFailed;

        public event Action<NodeLeadershipStatus>? Changed;
        public Task WhenFinished => _finished.Task;
        public long Term => Volatile.Read(ref _term);

        public NodeLeadershipStatus Status
        {
            get
            {
                lock (_cluster.Gate)
                {
                    var leader = _cluster.Leader;
                    return new NodeLeadershipStatus(
                        backend.NodeId, _role, _term, leader?.NodeId, leader is null ? 0 : _cluster.Term)
                    {
                        TakeoverFailed = _takeoverFailed,
                        Previous = _cluster.LastDeposed is { } last && last.Node == backend.NodeId ? last.Previous : null,
                    };
                }
            }
        }

        public void Track(ChangelogStorage storage)
        {
            lock (_storages)
                _storages.Add(storage);
        }

        public async Task StartAsync(LeadershipCallbacks callbacks, CancellationToken cancellationToken)
        {
            _callbacks = callbacks;
            bool lead;
            lock (_cluster.Gate)
            {
                _cluster.Nodes.Add(backend);
                lead = _cluster.Leader is null;
            }
            if (lead)
                await LeadAsync(cancellationToken).ConfigureAwait(false);
        }

        public Task TakeOverAsync(CancellationToken cancellationToken)
        {
            lock (_cluster.Gate)
                if (_role != NodeRole.Standby)
                    throw new InvalidOperationException($"Node '{backend.NodeId}' is {_role}; only a standby can take over.");
            return LeadAsync(cancellationToken);
        }

        private async Task LeadAsync(CancellationToken cancellationToken)
        {
            var callbacks = _callbacks ?? throw new InvalidOperationException("The leadership has not been started.");
            InMemoryServiceBackend? deposed;
            lock (_cluster.Gate)
            {
                if (_role != NodeRole.Standby)
                    return;
                _role = NodeRole.Promoting;

                // Acquiring the writer: from here the previous leader can append to no
                // table log, and it is told so at once (there is no heartbeat to miss).
                deposed = _cluster.Leader;
                _cluster.Leader = backend;
                _term = ++_cluster.Term;
                if (deposed is not null && deposed.NodeId != backend.NodeId)
                    _cluster.LastDeposed = (deposed.NodeId, new PreviousTerm(deposed._leadership._term, backend.NodeId, _term, "manual"));
                else if (deposed is null && _cluster.Resigned is { } resigned && resigned.Node != backend.NodeId)
                    _cluster.LastDeposed = (resigned.Node, new PreviousTerm(resigned.Term, backend.NodeId, _term, "resigned"));
                _cluster.Resigned = null;
                foreach (var takeOver in backend._connections)
                    takeOver();
            }
            deposed?._leadership.Finish(NodeRole.Fenced);
            Notify();

            try
            {
                // The deposed leader's circuits run on until its node has heard and torn
                // them down. From the lock above its leased sinks begin no write; what one
                // of them was in the middle of is let land before anything is delivered
                // from here, within a bound.
                if (deposed is not null)
                    await deposed._leadership.WritesEnded().CompletesWithin(_cluster.Options.DeposedWriteTimeout).ConfigureAwait(false);
                await callbacks.PrepareAsync(cancellationToken).ConfigureAwait(false);
                ChangelogStorage[] storages;
                lock (_storages)
                    storages = [.. _storages];
                foreach (var storage in storages)
                    await storage.PromoteAsync(cancellationToken).ConfigureAwait(false);
                await callbacks.StartLeadingAsync(_term, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                // Fenced by its own doing, unless another node took over meanwhile.
                lock (_cluster.Gate)
                    _takeoverFailed = _role == NodeRole.Promoting;
                Finish(NodeRole.Fenced);
                throw;
            }

            lock (_cluster.Gate)
                if (_role == NodeRole.Promoting)
                    _role = NodeRole.Leader;
            Notify();
        }

        public async Task StepDownAsync(CancellationToken cancellationToken)
        {
            lock (_cluster.Gate)
                if (_role != NodeRole.Leader)
                    throw new InvalidOperationException($"Node '{backend.NodeId}' is {_role}; only the leader can step down.");
            await _callbacks!.StopLeadingAsync(cancellationToken).ConfigureAwait(false);

            InMemoryServiceBackend? successor;
            lock (_cluster.Gate)
            {
                if (ReferenceEquals(_cluster.Leader, backend))
                {
                    _cluster.Leader = null;
                    _cluster.Resigned = (backend.NodeId, _term);
                }
                successor = _cluster.Nodes.FirstOrDefault(n => n != backend && n._leadership._role == NodeRole.Standby);
            }
            Finish(NodeRole.Resigned);

            // The resignation is what a standby waits for: it takes over without being asked.
            if (successor is not null)
                _ = Task.Run(async () =>
                {
                    try
                    {
                        await successor._leadership.LeadAsync(CancellationToken.None).ConfigureAwait(false);
                    }
                    catch
                    {
                        // The successor ends Fenced and reports it through its own status.
                    }
                });
        }

        private void Finish(NodeRole role)
        {
            lock (_cluster.Gate)
            {
                if (_role is NodeRole.Fenced or NodeRole.Resigned)
                    return;
                _role = role;
                if (ReferenceEquals(_cluster.Leader, backend))
                    _cluster.Leader = null;
            }
            _finished.TrySetResult();
            Notify();
        }

        private void Notify() => Changed?.Invoke(Status);

        public async ValueTask<long[]?> MeasureStateLagAsync(CancellationToken cancellationToken)
        {
            ChangelogStorage? storage;
            lock (_storages)
                storage = _storages.FirstOrDefault();
            if (storage is null || storage.IsLeader)
                return null;
            var followed = storage.FollowedOffsets();
            var lag = new long[followed.Length];
            for (var p = 0; p < lag.Length; p++)
                lag[p] = Math.Max(0, await _cluster.State.EndOffsetAsync(p, cancellationToken).ConfigureAwait(false) - followed[p]);
            return lag;
        }

        public ValueTask<PeerInfo?> ProbePeerAsync(CancellationToken cancellationToken)
        {
            InMemoryServiceBackend? peer;
            lock (_cluster.Gate)
                peer = _cluster.Nodes.FirstOrDefault(n => n != backend);
            return new ValueTask<PeerInfo?>(peer?._leadership._callbacks?.DescribeSelf());
        }

        public bool PeerExpected
        {
            get
            {
                lock (_cluster.Gate)
                    return _cluster.Nodes.Count > 1;
            }
        }

        // ── the lease: who may deliver to a sink ───────────────────────────────────────
        //
        // The cluster's leader, and nobody else. "Is it me" and "a write of mine is in
        // flight" are decided under the cluster's gate, where a takeover names the new
        // leader — so a write either began before the takeover, and the takeover waits
        // for it, or finds that its node no longer leads and reaches no sink. Without
        // this a deposed leader, which is told at once but tears its circuits down on a
        // thread of its own a moment later, could read an input, join it against a table
        // that no longer follows and put the row over the new leader's.

        private int _writing;
        private TaskCompletionSource? _writesEnded;

        private void BeginWrite()
        {
            lock (_cluster.Gate)
            {
                if (!ReferenceEquals(_cluster.Leader, backend))
                    throw new ChangelogFencedException(
                        $"Node '{backend.NodeId}' does not lead (it is {_role}), so it must not deliver to sinks.");
                _writing++;
            }
        }

        private void EndWrite()
        {
            TaskCompletionSource? ended = null;
            lock (_cluster.Gate)
            {
                if (--_writing == 0)
                    (ended, _writesEnded) = (_writesEnded, null);
            }
            ended?.TrySetResult();
        }

        /// <summary>Completes when no leased sink of this node is inside a write.</summary>
        private Task WritesEnded()
        {
            lock (_cluster.Gate)
                return _writing == 0
                    ? Task.CompletedTask
                    : (_writesEnded ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)).Task;
        }

        public IUpsertSink<TKey, TRow> Leased<TKey, TRow>(IUpsertSink<TKey, TRow> sink) where TKey : notnull
        {
            ArgumentNullException.ThrowIfNull(sink);
            return new LeasedUpsertSink<TKey, TRow>(this, sink);
        }

        public IAppendSink<T> Leased<T>(IAppendSink<T> sink)
        {
            ArgumentNullException.ThrowIfNull(sink);
            return new LeasedAppendSink<T>(this, sink);
        }

        private sealed class LeasedUpsertSink<TKey, TRow>(NodeLeadership leadership, IUpsertSink<TKey, TRow> inner) : IUpsertSink<TKey, TRow>
            where TKey : notnull
        {
            public async ValueTask WriteAsync(UpsertBatch<TKey, TRow> batch, CancellationToken cancellationToken)
            {
                leadership.BeginWrite();
                try
                {
                    await inner.WriteAsync(batch, cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    leadership.EndWrite();
                }
            }
        }

        private sealed class LeasedAppendSink<T>(NodeLeadership leadership, IAppendSink<T> inner) : IAppendSink<T>
        {
            public async ValueTask WriteAsync(AppendBatch<T> batch, CancellationToken cancellationToken)
            {
                leadership.BeginWrite();
                try
                {
                    await inner.WriteAsync(batch, cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    leadership.EndWrite();
                }
            }
        }
    }
}
