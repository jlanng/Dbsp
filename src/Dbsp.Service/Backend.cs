using Dbsp.Kafka;
using Dbsp.Storage;
using Dbsp.Tables;

namespace Dbsp.Service;

/// <summary>
/// Everything durable and everything about roles, behind one interface: the state
/// changelog, the table logs, the plugin blobs and the leadership. The service is written
/// against this and nothing else, so it runs the same on the in-memory backend
/// (<see cref="InMemoryServiceBackend"/>: one process, no broker) and on Kafka.
/// </summary>
public interface IServiceBackend : IAsyncDisposable
{
    string NodeId { get; }

    /// <summary>Connects to whatever the backend is made of. Called once, first, by the
    /// node's start; nothing else of the backend is used before it completes.</summary>
    Task ConnectAsync(CancellationToken cancellationToken);

    /// <summary>The changelog every pipeline's state is committed to. The node follows it
    /// with <see cref="ChangelogStorage.Follow"/> and the leadership promotes that storage.</summary>
    IStateChangelog State { get; }

    /// <summary>This node's connection to one table's log. On a node that holds the writer
    /// (promoting or leading) the log is created if it does not exist and the connection is
    /// its writer. On a standby the call completes only once some writer has created the
    /// log, and the connection reads and follows.</summary>
    Task<ITableLog<TKey, TRow>> OpenTableLogAsync<TKey, TRow>(string name, CancellationToken cancellationToken)
        where TKey : notnull
        where TRow : class;

    /// <summary>Versions of history a table's log keeps whatever the retention floor says:
    /// the cap that cuts a reader loose. History is heap on the in-memory backend and disk
    /// on Kafka, hence a backend's number.</summary>
    int RetainedHistoryVersions { get; }

    /// <summary>Plugin files, by content hash.</summary>
    IBlobStore Blobs { get; }

    INodeLeadership Leadership { get; }

    /// <summary>The persisted formats of this node: the level it reads, and the level the
    /// deployment's logs are written at while it is the writer. A backend whose nodes are
    /// always one build has nothing to decide and gives <see cref="PersistedFormat.Latest"/>.</summary>
    PersistedFormat Formats => PersistedFormat.Latest;

    /// <summary>The level the logs are written at as far as this node has read; 0 when
    /// nothing has said. Above <see cref="PersistedFormat.Reads"/> on a node that cannot
    /// read them.</summary>
    int FormatLevelWritten => Formats.Writes;

    /// <summary>Raises the level the leader writes, durably and before anything is
    /// written under it. Called by the leader once the other node reads the level.</summary>
    Task RaiseFormatAsync(int level, CancellationToken cancellationToken) => Task.CompletedTask;

    /// <summary>Kind, settings and counters, for an operator to read.</summary>
    BackendInfo Describe();

    /// <summary>Whether every node of the deployment is in this one process (the in-memory
    /// backend). Only there is the <c>memory</c> connector's log something a host's own
    /// code writes for the pipelines to read; on a deployment of processes a starting spec
    /// does not bind an input to it (<see cref="IServiceDiscovery.TemplateAsync"/>).</summary>
    bool InProcess => false;
}

public sealed record BackendInfo(
    string Kind, IReadOnlyDictionary<string, string> Settings, IReadOnlyDictionary<string, long> Counters);

/// <summary>Content-addressed storage for plugin files. A blob's name is the SHA-256 of
/// its bytes in lower-case hex, so a write is idempotent and needs no fencing.</summary>
public interface IBlobStore
{
    ValueTask<bool> ExistsAsync(string sha256, CancellationToken cancellationToken);
    Task PutAsync(string sha256, Stream content, CancellationToken cancellationToken);

    /// <summary>Copies the blob to <paramref name="destination"/>; false if there is none.</summary>
    Task<bool> CopyToAsync(string sha256, Stream destination, CancellationToken cancellationToken);
    Task DeleteAsync(string sha256, CancellationToken cancellationToken);
}

/// <summary>
/// Who leads. Both implementations run a takeover in one order: acquire the writer (which
/// fences the previous leader) → <see cref="LeadershipCallbacks.PrepareAsync"/> → promote
/// the tracked storages → <see cref="LeadershipCallbacks.StartLeadingAsync"/>. An
/// exception from any of these ends the node <see cref="NodeRole.Fenced"/>.
/// The leadership does not track tables: the service promotes them itself, inside
/// <c>PrepareAsync</c>, so that one table failing stays that table's failure.
/// </summary>
public interface INodeLeadership
{
    NodeLeadershipStatus Status { get; }
    event Action<NodeLeadershipStatus>? Changed;

    /// <summary>Completes when the node is <see cref="NodeRole.Fenced"/> or
    /// <see cref="NodeRole.Resigned"/>. Both are final for this process.</summary>
    Task WhenFinished { get; }
    long Term { get; }

    /// <summary>A storage to promote at takeover, after <c>PrepareAsync</c>.</summary>
    void Track(ChangelogStorage storage);
    Task StartAsync(LeadershipCallbacks callbacks, CancellationToken cancellationToken);
    Task TakeOverAsync(CancellationToken cancellationToken);

    /// <summary>Runs <see cref="LeadershipCallbacks.StopLeadingAsync"/>, then resigns.
    /// Throws unless the role is <see cref="NodeRole.Leader"/>.</summary>
    Task StepDownAsync(CancellationToken cancellationToken);

    /// <summary>How far this node's copy of the state stands behind the log, per state
    /// partition; null on a leader.</summary>
    ValueTask<long[]?> MeasureStateLagAsync(CancellationToken cancellationToken);

    /// <summary>What the other node says of itself; null when no peer is configured or
    /// none answered.</summary>
    ValueTask<PeerInfo?> ProbePeerAsync(CancellationToken cancellationToken);

    /// <summary>Whether this node has been told of another node at all. Without one a
    /// missing standby is not something to report: a lone node is a deployment too.</summary>
    bool PeerExpected { get; }

    /// <summary>Wraps a sink so that it writes only while this node holds the lease.</summary>
    IUpsertSink<TKey, TRow> Leased<TKey, TRow>(IUpsertSink<TKey, TRow> sink) where TKey : notnull;

    /// <inheritdoc cref="Leased{TKey,TRow}(IUpsertSink{TKey,TRow})"/>
    IAppendSink<T> Leased<T>(IAppendSink<T> sink);
}

/// <summary>A node's role as its leadership sees it. The service's own record, so the
/// in-memory backend invents no heartbeat or lease numbers: <see cref="Kafka"/> is null there.</summary>
public sealed record NodeLeadershipStatus(
    string NodeId, NodeRole Role, long Term, string? LeaderNodeId, long LeaderTerm,
    KafkaLeadershipDetail? Kafka = null)
{
    /// <summary>The node is <see cref="NodeRole.Fenced"/> because a takeover it began
    /// failed after it had taken the log — the catalog, the storage or the start of its
    /// pipelines failed it — and not because another node leads. Its process ends with
    /// exit code 5 ("it could not lead"), which a supervisor answers with a growing delay
    /// when it keeps happening; a node another node replaced ends with 3 and is started
    /// again at once.</summary>
    public bool TakeoverFailed { get; init; }

    /// <summary>The term this node led before another node took over from it, when the
    /// backend knows: a replaced node ends its process and is started again as a standby,
    /// and this is how that standby says it led. Null if it has not led, as far as the
    /// backend can tell.</summary>
    public PreviousTerm? Previous { get; init; }
}

/// <summary>A term a node led, and who took over from it, under which term and why
/// (<c>manual</c>: an operator asked; <c>silence</c>, <c>ceiling</c>, <c>resigned</c>;
/// null when the new leader did not say).</summary>
public sealed record PreviousTerm(long Term, string ReplacedBy, long ReplacedByTerm, string? Reason);

/// <summary>What only a Kafka-backed node knows of its leadership. <c>Fault</c> is why the
/// node is <see cref="NodeRole.Fenced"/>, as text.</summary>
public sealed record KafkaLeadershipDetail(
    TimeSpan HeartbeatAge, DateTimeOffset? LeaderHeartbeatAt, bool LeaderResigned, bool HoldsLease,
    TakeoverTimings? LastTakeover, string? Fault = null);

/// <summary>What a node answers a probe with. <c>AdminUrl</c> is its
/// <see cref="ServiceOptions.AdvertisedAdminUrl"/>: a node that has been fenced no longer
/// follows the catalog, so the probe is how it learns where the node that replaced it is.</summary>
/// <remarks><c>Memory</c> is the answering process's memory when it said (null from a
/// node that predates it). <c>FormatReads</c> is the highest format level the answering
/// node reads (<see cref="PersistedFormat.Reads"/>); 0 from a node built before levels,
/// which reads <see cref="FormatLevel.Base"/>.</remarks>
public sealed record PeerInfo(
    string NodeId, NodeRole Role, long Term, bool Ready, string? NotReadyReason, string? AdminUrl = null, ProcessMemory? Memory = null,
    int FormatReads = 0);


public sealed record LeadershipCallbacks(
    Func<CancellationToken, Task> PrepareAsync,
    Func<long, CancellationToken, Task> StartLeadingAsync,
    Func<CancellationToken, Task> StopLeadingAsync,
    Func<PeerInfo> DescribeSelf);

/// <summary>
/// Optional on a backend: a place outside its logs where the leader mirrors the catalog
/// after each commit and from which a first leader restores it. The in-memory backend
/// implements it over <see cref="InMemoryBackendOptions.CatalogDirectory"/>; a durable
/// backend has no use for it.
/// </summary>
public interface ICatalogMirror
{
    Task<IReadOnlyList<CatalogEntry>?> LoadAsync(CancellationToken cancellationToken);
    Task SaveAsync(IReadOnlyCollection<CatalogEntry> entries, CancellationToken cancellationToken);
}
