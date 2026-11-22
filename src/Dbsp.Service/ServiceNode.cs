using System.Collections.Concurrent;
using System.Reflection;
using System.Text.Json;
using Dbsp.Kafka;
using Dbsp.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Dbsp.Service;

/// <summary>What the host registered: compiled-in assemblies, seeds, and the named
/// factories specs refer to.</summary>
internal sealed class ServiceRegistrations
{
    public List<Assembly> Contracts { get; } = [];
    public List<Assembly> Plugins { get; } = [];
    public List<TableSpec> Tables { get; } = [];
    public List<PipelineSpec> Pipelines { get; } = [];
    public Dictionary<string, Func<IServiceProvider, ITableSourceFactory>> TableSources { get; } = new(StringComparer.Ordinal);
    public Dictionary<string, Func<IServiceProvider, IConnector>> Connectors { get; } = new(StringComparer.Ordinal);
    public Dictionary<string, Func<IServiceProvider, IObjectStore>> ObjectStores { get; } = new(StringComparer.Ordinal);
    public Dictionary<string, Func<IServiceProvider, KafkaConnection>> KafkaConnections { get; } = new(StringComparer.Ordinal);
    public Dictionary<string, Func<IServiceProvider, IRecordFormat>> RecordFormats { get; } = new(StringComparer.Ordinal);
    public Func<IServiceProvider, IServiceBackend>? Backend { get; set; }

    /// <summary>The topic prefix of the Kafka backend's own log, when that is the backend.</summary>
    public Func<IServiceProvider, string>? ReservedTopicPrefix { get; set; }
}

/// <summary>What the registry, the plugin catalog and the supervisor share.</summary>
internal sealed class ServiceContext
{
    private readonly ConcurrentDictionary<string, IConnector> _connectors = new(StringComparer.Ordinal);

    public required ServiceOptions Options { get; init; }
    public required IServiceBackend Backend { get; init; }
    public required ServiceRegistrations Registrations { get; init; }
    public required IServiceProvider Services { get; init; }
    public required ILoggerFactory LoggerFactory { get; init; }
    public required EventRing Events { get; init; }
    public required Catalog Catalog { get; init; }
    public required KafkaConnections KafkaConnections { get; init; }
    public ChangelogStorage Storage { get; set; } = null!;
    public PluginCatalog Plugins { get; set; } = null!;
    public TableRegistry Tables { get; set; } = null!;
    public PipelineSupervisor Supervisor { get; set; } = null!;

    /// <summary>Set when this node's takeover reaches <c>StartLeadingAsync</c>; cleared
    /// when it stops leading for any reason.</summary>
    public volatile bool Leading;

    public string NodeId => Options.NodeName ?? Backend.NodeId;
    public long Term => Backend.Leadership.Term;
    public bool IsLeader => Leading && !Storage.IsFenced;

    /// <summary>Refuses on a node that does not lead. <paramref name="what"/> says what
    /// was asked for, when it is not a mutation: a read that only the leader can answer.</summary>
    public void RequireLeader(string? what = null)
    {
        if (IsLeader)
            return;
        var status = Backend.Leadership.Status;
        throw new ServiceException(
            "not-leader",
            $"Node '{NodeId}' is {status.Role}; {what ?? "mutations go to the leader"}" + (status.LeaderNodeId is { } leader ? $" ('{leader}')." : "."), 409);
    }

    public IReadOnlyList<PipelineSpec> PipelineSpecs() => Supervisor.Specs;

    private readonly SemaphoreSlim _references = new(1, 1);

    /// <summary>
    /// The lock between naming a thing and removing it. A deploy holds it while it checks
    /// that the tables and the plugin version its spec names are there and commits the
    /// spec; removing a table and deleting or importing a plugin version hold it from their
    /// check of who names the thing to the end of the removal. Held for a check and a
    /// catalog commit (a plugin delete: its blob deletions too), never while a pipeline
    /// starts or stops; taken after a pipeline's queue, never before one.
    /// </summary>
    public async Task<IDisposable> LockReferencesAsync(CancellationToken cancellationToken)
    {
        await _references.WaitAsync(cancellationToken).ConfigureAwait(false);
        return new Unlock(_references);
    }

    private sealed class Unlock(SemaphoreSlim gate) : IDisposable
    {
        private int _released;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _released, 1) == 0)
                gate.Release();
        }
    }

    public IConnector Connector(JsonElement config)
    {
        if (config.ValueKind != JsonValueKind.Object || !config.TryGetProperty("connector", out var kind) || kind.GetString() is not { Length: > 0 } name)
            throw new InvalidOperationException("a slot's config must name its connector: { \"connector\": \"…\" }");
        if (!Registrations.Connectors.TryGetValue(name, out var factory))
            throw new InvalidOperationException(
                $"no connector named '{name}' is registered (registered: {string.Join(", ", Registrations.Connectors.Keys.Order())})");
        return _connectors.GetOrAdd(name, _ => factory(Services));
    }

    public IObjectStore ObjectStore(string? name)
    {
        if (string.IsNullOrEmpty(name))
            throw ServiceException.Invalid("The loader must name an object store connection.");
        if (!Registrations.ObjectStores.ContainsKey(name))
            throw ServiceException.Invalid(
                $"No object store named '{name}' is registered (registered: {string.Join(", ", Registrations.ObjectStores.Keys.Order())}).");
        return Services.GetRequiredService<ObjectStores>().Get(name);
    }
}

public enum HealthLevel { Healthy, Degraded, Unhealthy }

/// <summary>Computed from the status, never from whether pipelines exist: an empty leader
/// is healthy.</summary>
public sealed record NodeHealth(bool Live, bool Ready, HealthLevel Level, IReadOnlyList<string> Reasons);

/// <summary>How ready a standby is to lead, in terms an operator can act on.</summary>
/// <remarks><c>PluginsPending</c> counts plugin versions a deployed pipeline names whose files
/// this node has not yet fetched from the blob store. <c>PipelinesToBuild</c> counts
/// pipelines whose circuit this standby is still building on the state it follows
/// (<see cref="ServiceOptions.WarmStandbyPipelines"/>); one it could not build is not
/// counted — it shows as <c>Unbuilt</c> with the reason and is built at takeover.</remarks>
public sealed record StandbyReadiness(
    long CatalogVersionApplied, long CatalogVersionLatest, long StateLagRecords, int TablesBehind, int TablesFaulted,
    int PluginsPending, bool TakeoverWouldBeWarm, IReadOnlyList<string> NotWarmBecause, int PipelinesToBuild = 0);

/// <summary>Where the service's part of this node's takeover went. The leadership's own
/// record (<see cref="KafkaLeadershipDetail.LastTakeover"/>) has the stages around it:
/// its <c>Prepare</c> is <see cref="Catalog"/> plus <see cref="Tables"/>, and its
/// <c>Start</c> is <see cref="StartLeading"/>.</summary>
/// <remarks><c>Catalog</c>: opening or promoting the catalog and loading stored contracts.
/// <c>Tables</c>: promoting or opening every table, concurrently. <c>Pipelines</c>:
/// finishing pending resets and removals, then building and starting every pipeline from
/// its committed state, in parallel. <c>StartLeading</c>: all of <c>StartLeadingAsync</c>,
/// which is the five stages measured inside it and <c>Pipelines</c>, with nothing left
/// over: <c>LeaderRecord</c> (restoring the catalog from its mirror where there is one,
/// and writing <c>node/leader</c>), <c>PluginDirectory</c> (importing what the plugin
/// directory holds and the catalog lacks), <c>TableSeeds</c> (declaring the host's seed
/// tables, the first time), <c>Pipelines</c>, <c>PipelineSeeds</c> (deploying the host's
/// seed pipelines, the first time — each is built and started) and <c>Finish</c> (the
/// seed record, the plugin cache, the loaders).</remarks>
public sealed record ServiceTakeoverTimings(
    long Term, DateTimeOffset At, TimeSpan Catalog, TimeSpan Tables, int TableCount, TimeSpan Pipelines, int PipelineCount,
    TimeSpan StartLeading, TimeSpan LeaderRecord = default, TimeSpan PluginDirectory = default, TimeSpan TableSeeds = default,
    TimeSpan PipelineSeeds = default, TimeSpan Finish = default);

/// <summary>A node at a glance. <c>Peer</c> is what the other node last answered (null when
/// none is configured or none answered); <c>LastTakeover</c> is how this node's takeover
/// went (null if it has not led).</summary>
public sealed record NodeStatus(
    string NodeId, NodeLeadershipStatus Leadership, long CatalogVersion, int Tables, int Pipelines,
    IReadOnlyDictionary<PipelineState, int> PipelinesByState, StandbyReadiness? Standby, BackendInfo Backend,
    string? AdvertisedAdminUrl, NodeHealth Health, PeerInfo? Peer = null, ServiceTakeoverTimings? LastTakeover = null);

public sealed record NodeActionOptions(bool Force = false, bool DryRun = false, string? Caller = null);

/// <summary>The seeds that have been applied, under <c>node/seeds</c>. A seed is written
/// once per deployment; one that was applied and then removed stays removed.</summary>
public sealed record SeedRecord(IReadOnlyList<string> Tables, IReadOnlyList<string> Pipelines);

/// <summary>
/// The last few samples of a standby's state lag, and what they say together. A follower
/// of a leader that commits all the time is a few records behind at most instants and
/// caught up between commits; one sample a second cannot tell that from a standby that is
/// behind. So a standby is called behind only if it was behind at every one of the last
/// <see cref="Samples"/> samples — a standby that is really catching up is behind at all
/// of them, and one that keeps up is caught up at one of them.
/// </summary>
internal sealed class StateLagWindow
{
    public const int Samples = 3;
    private readonly long[] _samples = new long[Samples];
    private int _next;
    private volatile bool _behind;

    public bool Behind => _behind;

    public void Add(long lag)
    {
        _samples[_next++ % Samples] = lag;
        _behind = _samples.Min() > 0;
    }
}

/// <summary>What the leader publishes of itself under <c>node/leader</c>.</summary>
public sealed record LeaderRecord(string NodeId, long Term, string? AdminUrl, IReadOnlyDictionary<string, string> Contracts);

/// <summary>
/// One node of the service. Every node runs the same code and starts as a standby: it
/// follows the catalog and the tables the catalog names, and — when the backend's
/// leadership says so — starts writing: promotes the catalog and the tables, then runs the
/// pipelines. The catalog is the only input to what should exist; the node's role is the
/// only input to who runs it.
/// </summary>
public sealed class ServiceNode : IHostedService, IAsyncDisposable
{
    private readonly ServiceContext _context;

    /// <summary>For tests of what lies under the node.</summary>
    internal ServiceContext Context => _context;
    private readonly ILogger _logger;
    private readonly IHostApplicationLifetime? _lifetime;
    private readonly CancellationTokenSource _stopping = new();
    private readonly SemaphoreSlim _roleGate = new(1, 1);

    private CancellationTokenSource? _catalogFollow;
    private Task _catalogOpen = Task.CompletedTask;
    private Task _loop = Task.CompletedTask;
    private Task _finished = Task.CompletedTask;
    private long _catalogApplied = -1;
    private long _stateLag;

    private readonly StateLagWindow _stateLagWindow = new();
    private int _pluginsPending;
    private Task? _warming;
    private volatile PeerInfo? _peer;
    private volatile bool _peerSampled;
    private volatile ServiceTakeoverTimings? _lastTakeover;
    private (TimeSpan Catalog, TimeSpan Tables, int TableCount) _prepared;
    private DateTimeOffset _catalogReopenedAt = DateTimeOffset.MinValue;
    private int _cannotLead;
    private readonly TaskCompletionSource<int> _ended = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private volatile bool _started;
    private int _disposed;

    public ServiceNode(IServiceProvider services, ServiceOptions options)
        : this(services, options, services.GetRequiredService<ServiceRegistrations>())
    {
    }

    internal ServiceNode(IServiceProvider services, ServiceOptions options, ServiceRegistrations registrations)
    {
        var loggerFactory = services.GetService<ILoggerFactory>() ?? NullLoggerFactory.Instance;
        var backend = registrations.Backend?.Invoke(services) ?? new InMemoryServiceBackend();
        _lifetime = services.GetService<IHostApplicationLifetime>();
        _logger = loggerFactory.CreateLogger("Dbsp.Service.Node");
        var events = new EventRing(options.EventBuffer);
        _context = new ServiceContext
        {
            Options = options,
            Backend = backend,
            Registrations = registrations,
            Services = services,
            LoggerFactory = loggerFactory,
            Events = events,
            Catalog = new Catalog(backend, options),
            KafkaConnections = services.GetService<KafkaConnections>() ?? new KafkaConnections(services, registrations),
        };
        _context.Plugins = new PluginCatalog(_context);
        _context.Tables = new TableRegistry(_context);
        _context.Supervisor = new PipelineSupervisor(_context);
        _context.Tables.Reopened += _context.Supervisor.OnTableReopened;
        Metrics = new ServiceMetrics(_context.Supervisor);
        events.Published += e => Events?.Invoke(e);
    }

    public string NodeId => _context.NodeId;
    public IServiceBackend Backend => _context.Backend;
    public Catalog Catalog => _context.Catalog;
    public ITableRegistry Tables => _context.Tables;
    public IPipelineSupervisor Pipelines => _context.Supervisor;
    public IPluginCatalog Plugins => _context.Plugins;

    /// <summary>What there is to deploy with: connectors, connections, table source
    /// kinds, and a definition's slots and settings.</summary>
    public IServiceDiscovery Discovery => _discovery ??= new ServiceDiscovery(_context);

    private ServiceDiscovery? _discovery;
    public bool IsLeader => _started && _context.IsLeader;

    /// <summary>Recent per-pipeline and node-wide numbers, sampled once a second from the
    /// core's meter and the supervisor's counters.</summary>
    public ServiceMetrics Metrics { get; }

    /// <summary>The options this node runs with.</summary>
    public ServiceOptions Options => _context.Options;

    /// <summary>Role changes, pipeline and table transitions, catalog versions.</summary>
    public event Action<ServiceEvent>? Events;

    public IReadOnlyList<ServiceEvent> RecentEvents(long afterSequence = 0) => _context.Events.After(afterSequence);

    // ── start ──────────────────────────────────────────────────────────────────────────

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (_started)
            throw new InvalidOperationException("The service node has already been started.");
        var backend = _context.Backend;
        await backend.ConnectAsync(cancellationToken).ConfigureAwait(false);
        _context.Storage = ChangelogStorage.Follow(backend.State, new ChangelogStorageOptions { Formats = backend.Formats });
        backend.Leadership.Track(_context.Storage);
        backend.Leadership.Changed += status =>
            _context.Events.Publish("node", status.NodeId, $"{status.Role}, term {status.Term}");
        _context.Plugins.LoadBuiltIns();

        // On a deployment's very first start the catalog's log does not exist until some
        // node is the writer, so this completes inside the first takeover.
        _catalogFollow = new CancellationTokenSource();
        var follow = _catalogFollow.Token;
        _catalogOpen = Task.Run(() => FollowCatalogAsync(follow), CancellationToken.None);
        _catalogOpen.Forget();

        _started = true;
        Metrics.Sampled += RefreshGauges;
        ServiceMeter.Register(this);
        Metrics.Start();
        _finished = backend.Leadership.WhenFinished.ContinueWith(_ => OnFinishedAsync(), TaskScheduler.Default).Unwrap();
        await backend.Leadership.StartAsync(
            new LeadershipCallbacks(PrepareAsync, StartLeadingAsync, StopLeadingAsync, DescribeSelf), cancellationToken).ConfigureAwait(false);
        _loop = Task.Run(() => RunAsync(_stopping.Token), CancellationToken.None);
        _logger.LogInformation("Service node {Node} started as {Role}", NodeId, backend.Leadership.Status.Role);
    }

    /// <summary>A standby's catalog: opened as a follower, and tried again until it is —
    /// a node that could not follow the catalog would stand by for nothing.</summary>
    private async Task FollowCatalogAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            try
            {
                await _context.Catalog.OpenAsync(follow: true, cancellationToken).ConfigureAwait(false);
                return;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogWarning(ex, "Service node {Node} could not open the catalog to follow it; trying again", NodeId);
                await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken).ConfigureAwait(false);
            }
        }
    }

    // ── takeover ───────────────────────────────────────────────────────────────────────

    /// <summary>The writer is acquired; the storage is not promoted yet. The catalog first
    /// and alone — only it can fail the takeover — then every table concurrently, each in
    /// its own try block.</summary>
    private async Task PrepareAsync(CancellationToken cancellationToken)
    {
        await _roleGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var clock = System.Diagnostics.Stopwatch.StartNew();
            var catalog = _context.Catalog;
            if (!catalog.IsOpen)
            {
                _catalogFollow?.Cancel();
                try
                {
                    await _catalogOpen.ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is OperationCanceledException)
                {
                }
            }
            if (catalog.IsOpen)
                await catalog.PromoteAsync(cancellationToken).ConfigureAwait(false);
            else
                await catalog.OpenAsync(follow: false, cancellationToken).ConfigureAwait(false);

            var entries = catalog.Read();
            await _context.Plugins.LoadContractsAsync(entries, cancellationToken).ConfigureAwait(false);
            var catalogDone = clock.Elapsed;
            await _context.Tables.ReconcileAsync(entries, asWriter: true, cancellationToken).ConfigureAwait(false);
            _prepared = (catalogDone, clock.Elapsed - catalogDone, _context.Tables.Handles.Count());
        }
        finally
        {
            _roleGate.Release();
        }
    }

    /// <summary>The storage is promoted. Everything after <c>node/leader</c> is contained
    /// per item: a table, plugin or pipeline that fails is shown and retried, and never
    /// ends the takeover.</summary>
    private async Task StartLeadingAsync(long term, CancellationToken cancellationToken)
    {
        await _roleGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var clock = System.Diagnostics.Stopwatch.StartNew();
            _context.Leading = true;
            var catalog = _context.Catalog;

            if (_context.Backend is ICatalogMirror mirror && catalog.Version == 0
                && await mirror.LoadAsync(cancellationToken).ConfigureAwait(false) is { Count: > 0 } mirrored)
            {
                using var restore = await catalog.BeginAsync(cancellationToken).ConfigureAwait(false);
                foreach (var entry in mirrored.Where(e => e.Key != "node/leader"))
                    restore.Put(entry.Key, entry.Value);
                await restore.CommitAsync(cancellationToken).ConfigureAwait(false);
                _logger.LogInformation("Catalog restored from its mirror: {Entries} entries", mirrored.Count);
                await _context.Plugins.LoadContractsAsync(catalog.Read(), cancellationToken).ConfigureAwait(false);
                await _context.Tables.ReconcileAsync(catalog.Read(), asWriter: true, cancellationToken).ConfigureAwait(false);
            }

            using (var write = await catalog.BeginAsync(cancellationToken).ConfigureAwait(false))
            {
                write.Put("node/leader", new LeaderRecord(
                    NodeId, term, _context.Options.AdvertisedAdminUrl,
                    ContractAssemblies.All.ToDictionary(a => a.GetName().Name!, a => ContractAssemblies.VersionOf(a.GetName().Name!) ?? "")));
                await write.CommitAsync(cancellationToken).ConfigureAwait(false);
            }

            var leaderRecord = clock.Elapsed;
            await Contained("importing the plugin directory", () => _context.Plugins.ImportPluginDirectoryAsync(cancellationToken), cancellationToken).ConfigureAwait(false);
            var pluginDirectory = clock.Elapsed - leaderRecord;

            // A seed is applied once per deployment, and the catalog records that it was:
            // a key that is absent is otherwise the same for a seed never applied and for
            // one an operator removed, and the removed one would come back at every
            // takeover. An entry that is there already (a deployment older than the
            // record, or a leader that died between the two writes) counts as applied.
            var seeded = catalog.Get<SeedRecord>("node/seeds") ?? new SeedRecord([], []);
            var seededTables = seeded.Tables.ToHashSet(StringComparer.Ordinal);
            var seededPipelines = seeded.Pipelines.ToHashSet(StringComparer.Ordinal);
            foreach (var seed in _context.Registrations.Tables)
            {
                if (seededTables.Contains(seed.Name))
                    continue;
                if (catalog.Get("table/" + seed.Name) is not null
                    || await Contained($"seeding table '{seed.Name}'", () => _context.Tables.DeclareAsync(seed, cancellationToken, "seed"), cancellationToken).ConfigureAwait(false))
                    seededTables.Add(seed.Name);
            }

            var beforePipelines = clock.Elapsed;
            await _context.Supervisor.StartLeadingAsync(catalog.Read(), cancellationToken).ConfigureAwait(false);
            var pipelines = clock.Elapsed - beforePipelines;
            var pipelineCount = _context.Supervisor.Specs.Count;
            var beforePipelineSeeds = clock.Elapsed;
            foreach (var seed in _context.Registrations.Pipelines)
            {
                if (seededPipelines.Contains(seed.Id))
                    continue;
                if (catalog.Get("pipeline/" + seed.Id) is not null
                    || await Contained($"seeding pipeline '{seed.Id}'",
                        () => _context.Supervisor.DeployAsync(seed, new DeployOptions(Caller: "seed"), cancellationToken), cancellationToken).ConfigureAwait(false))
                    seededPipelines.Add(seed.Id);
            }
            var pipelineSeeds = clock.Elapsed - beforePipelineSeeds;
            if (seededTables.Count != seeded.Tables.Count || seededPipelines.Count != seeded.Pipelines.Count)
            {
                await Contained("recording the seeds", async () =>
                {
                    using var write = await catalog.BeginAsync(cancellationToken).ConfigureAwait(false);
                    write.Put("node/seeds", new SeedRecord(
                        [.. seededTables.Order(StringComparer.Ordinal)], [.. seededPipelines.Order(StringComparer.Ordinal)]));
                    await write.CommitAsync(cancellationToken).ConfigureAwait(false);
                }, cancellationToken).ConfigureAwait(false);
            }
            await Contained("pruning the plugin cache", () => _context.Plugins.PruneCacheAsync(null, cancellationToken), cancellationToken).ConfigureAwait(false);
            _context.Tables.StartLoaders();
            _catalogApplied = catalog.Version;
            // Each stage ends where the next begins, on one clock: they add up to the whole.
            var total = clock.Elapsed;
            _lastTakeover = new ServiceTakeoverTimings(
                term, DateTimeOffset.UtcNow, _prepared.Catalog, _prepared.Tables, _prepared.TableCount, pipelines, pipelineCount, total,
                leaderRecord, pluginDirectory, beforePipelines - leaderRecord - pluginDirectory, pipelineSeeds,
                total - beforePipelineSeeds - pipelineSeeds);
            ServiceMeter.Takeovers.Add(1, ServiceMeter.Tag("node", NodeId));
            ServiceMeter.RecordTakeover(NodeId, "catalog", _prepared.Catalog);
            ServiceMeter.RecordTakeover(NodeId, "tables", _prepared.Tables);
            ServiceMeter.RecordTakeover(NodeId, "pipelines", pipelines);
            ServiceMeter.RecordTakeover(NodeId, "start_leading", total);
            _logger.LogInformation(
                "Service node {Node} leads, term {Term}: catalog {Catalog} ms, {Tables} table(s) {TablesMs} ms, {Pipelines} pipeline(s) {PipelinesMs} ms",
                NodeId, term, (long)_prepared.Catalog.TotalMilliseconds, _prepared.TableCount, (long)_prepared.Tables.TotalMilliseconds,
                pipelineCount, (long)pipelines.TotalMilliseconds);
        }
        finally
        {
            _roleGate.Release();
        }
    }

    /// <summary>Runs one contained part of a takeover or a shutdown; false when it failed.
    /// Only a cancellation of <paramref name="cancellationToken"/> itself leaves this
    /// method: an <see cref="OperationCanceledException"/> from anywhere else — a timeout
    /// inside tenant code — is a failure like any other, not the end of the takeover.</summary>
    private async Task<bool> Contained(string what, Func<Task> action, CancellationToken cancellationToken = default)
    {
        try
        {
            await action().ConfigureAwait(false);
            return true;
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            _logger.LogError(ex, "Service node {Node}: {What} failed", NodeId, what);
            _context.Events.Publish("node", NodeId, $"{what} failed: {ex.Message}");
            return false;
        }
    }

    /// <summary>A step-down, before the resignation: loaders, then every pipeline in stop
    /// order and in parallel, within <see cref="ServiceOptions.StepDownTimeout"/>.</summary>
    private async Task StopLeadingAsync(CancellationToken cancellationToken)
    {
        await _context.Tables.StopLoadersAsync().ConfigureAwait(false);
        await _context.Supervisor.StopAllAsync(_stopBudget ?? _context.Options.StepDownTimeout).ConfigureAwait(false);
        _context.Leading = false;
    }

    private TimeSpan? _stopBudget;

    private PeerInfo DescribeSelf()
    {
        var status = _context.Backend.Leadership.Status;
        var admin = _context.Options.AdvertisedAdminUrl;
        if (status.Role != NodeRole.Standby)
            return new PeerInfo(
                NodeId, status.Role, status.Term, status.Role == NodeRole.Leader, status.Role == NodeRole.Leader ? null : status.Role.ToString(), admin,
                ProcessMemory.Read(), _context.Backend.Formats.Reads);
        var readiness = Readiness();
        return new PeerInfo(
            NodeId, status.Role, status.Term, readiness.TakeoverWouldBeWarm, readiness.NotWarmBecause.FirstOrDefault(), admin, ProcessMemory.Read(),
            _context.Backend.Formats.Reads);
    }

    // ── the loop: reconcile on a standby, trim and mirror on the leader ────────────────

    private async Task RunAsync(CancellationToken stop)
    {
        var options = _context.Options;
        var lastTrim = DateTimeOffset.UtcNow;
        var lastLag = DateTimeOffset.MinValue;
        var lastWatch = DateTimeOffset.MinValue;
        while (!stop.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(options.CatalogPollInterval, stop).ConfigureAwait(false);
                var catalog = _context.Catalog;
                var role = _context.Backend.Leadership.Status.Role;
                if (role == NodeRole.Standby && catalog.IsOpen && await _roleGate.WaitAsync(0, stop).ConfigureAwait(false))
                {
                    try
                    {
                        if (_context.Backend.Leadership.Status.Role == NodeRole.Standby)
                        {
                            // A catalog follower that has lost its history sees no more
                            // versions: follow again from the rows, like any other table.
                            if (catalog.Table is { FollowFault: { } catalogFault })
                            {
                                _logger.LogWarning(catalogFault, "Service node {Node}: the catalog's follower faulted; following it again", NodeId);
                                await catalog.ReopenAsync(follow: true, stop).ConfigureAwait(false);
                                _context.Events.Publish("catalog", NodeId, "follower reopened from the log");
                            }
                            var version = catalog.Version;
                            var faulted = _context.Tables.Handles.Any(h => h.FollowFault is not null);
                            if (version != _catalogApplied || faulted || Volatile.Read(ref _pluginsPending) > 0)
                            {
                                var entries = catalog.Read();
                                await _context.Plugins.LoadContractsAsync(entries, stop).ConfigureAwait(false);
                                await _context.Tables.ReconcileAsync(entries, asWriter: false, stop).ConfigureAwait(false);
                                _context.Supervisor.SyncFromCatalog(entries);

                                // The plugins' files are fetched as soon as the catalog names
                                // them: this node builds its circuits from them, and a
                                // takeover loads what it did not build from this node's disk.
                                Volatile.Write(ref _pluginsPending, await _context.Plugins.PrefetchAsync(entries, stop).ConfigureAwait(false));
                                await _context.Plugins.PruneCacheAsync(entries, stop).ConfigureAwait(false);
                                if (version != _catalogApplied)
                                    _context.Events.Publish("catalog", NodeId, $"version {version} applied");
                                _catalogApplied = version;
                            }
                        }
                    }
                    finally
                    {
                        _roleGate.Release();
                    }
                }

                // Outside the role gate: building a circuit on a large state takes seconds,
                // and a takeover that begins meanwhile waits only for the pipelines being
                // built, each on its own queue.
                if (role == NodeRole.Standby && catalog.IsOpen && _catalogApplied == catalog.Version && (_warming is null || _warming.IsCompleted))
                {
                    _warming = _context.Supervisor.WarmAsync(stop);
                    _warming.Forget();
                }

                var now = DateTimeOffset.UtcNow;
                if (role == NodeRole.Standby && now - lastLag >= TimeSpan.FromSeconds(1))
                {
                    lastLag = now;
                    var lag = (await _context.Backend.Leadership.MeasureStateLagAsync(stop).ConfigureAwait(false))?.Sum() ?? 0;
                    Interlocked.Exchange(ref _stateLag, lag);
                    _stateLagWindow.Add(lag);
                }

                if (now - lastWatch >= TimeSpan.FromSeconds(1))
                {
                    lastWatch = now;
                    // In every role: a node that has been fenced or has resigned still
                    // serves its admin surface, and what it shows of the other node must
                    // be what that node is now, not what it was when this one last led.
                    _peer = await ProbePeerAsync(stop).ConfigureAwait(false);
                    _peerSampled = true;
                    if (role == NodeRole.Leader && _context.IsLeader)
                    {
                        await WatchCatalogWriterAsync(stop).ConfigureAwait(false);
                        await RaiseFormatLevelAsync(stop).ConfigureAwait(false);
                    }
                }

                // In every role, and off this loop: a standby that has just read
                // everything holds the most it does not use.
                if (_keeping is null || _keeping.IsCompleted)
                    _keeping = MemoryKeeper.Shared.RunIfDueAsync(options.ReleaseUnusedMemoryEvery, _logger, NodeId, stop);

                if (!_context.IsLeader)
                    continue;
                if (now - lastTrim >= options.TrimInterval)
                {
                    lastTrim = now;
                    await TrimAsync(stop).ConfigureAwait(false);
                }
                await MirrorAsync(stop).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stop.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Service node {Node}: the reconcile loop failed; it carries on", NodeId);
            }
        }
    }

    private Task? _keeping;

    /// <summary>What this process last did to give memory back; null before the first
    /// time, and when <see cref="ServiceOptions.ReleaseUnusedMemoryEvery"/> is zero.</summary>
    public MemoryRelease? MemoryRelease => MemoryKeeper.Shared.Last;

    /// <summary>
    /// The leader's part of a rolling upgrade. It writes the persisted formats at the
    /// level it found the logs at, and raises that level only when the other node has
    /// just answered its probe as a standby and said it reads the higher one — to the
    /// lower of what the two read. A standby that does not say (a build from before
    /// levels) reads the base level; one that does not answer decides nothing. A node
    /// that was told of no peer has no standby to wait for and writes its own level.
    /// Never lowered: once raised, a node that reads less can neither follow nor lead.
    /// </summary>
    private async Task RaiseFormatLevelAsync(CancellationToken stop)
    {
        var backend = _context.Backend;
        var formats = backend.Formats;
        if (_context.Options.HoldFormatLevel || formats.Writes >= formats.Reads)
            return;
        int level;
        string why;
        if (!backend.Leadership.PeerExpected)
        {
            level = formats.Reads;
            why = "this node was told of no other node";
        }
        else if (_peer is { Role: NodeRole.Standby } peer)
        {
            level = Math.Min(formats.Reads, Math.Max(FormatLevel.Base, peer.FormatReads));
            why = $"the standby '{peer.NodeId}' reads level {Math.Max(FormatLevel.Base, peer.FormatReads)}";
        }
        else
            return;
        var from = formats.Writes;
        if (level <= from)
            return;
        await backend.RaiseFormatAsync(level, stop).ConfigureAwait(false);
        _logger.LogWarning(
            "Service node {Node}: persisted formats raised from level {From} to level {Level} ({Why}). A node that reads less can no longer follow or lead",
            NodeId, from, level, why);
        _context.Events.Publish("node", NodeId, $"format level raised from {from} to {level}: {why}");
    }

    private async Task<PeerInfo?> ProbePeerAsync(CancellationToken cancellationToken)
    {
        try
        {
            return await _context.Backend.Leadership.ProbePeerAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return null;
        }
    }

    /// <summary>
    /// The one condition under which this node gives leadership up by itself: the catalog's
    /// writer has faulted and the node is not fenced, so it can record nothing. Heartbeats
    /// come from the log writer, not from the catalog, so the standby would wait on a
    /// leader that can do no part of its job. The catalog is reopened once, in place; if
    /// that fails, or its writer faults again within a minute, the node steps down when a
    /// ready standby answers and otherwise exits with code 5 for its process supervisor.
    /// Pipelines failing never do this: a pipeline is tenant code.
    /// </summary>
    private async Task WatchCatalogWriterAsync(CancellationToken stop)
    {
        var catalog = _context.Catalog;
        if (await catalog.WriterFaultAsync(stop).ConfigureAwait(false) is not { } fault)
            return;
        for (var inner = fault; inner is not null; inner = inner.InnerException)
            if (inner is Dbsp.Tables.TableLogFencedException or KafkaLogFencedException)
                return; // fenced: the leadership ends this node
        if (_context.Storage.IsFenced || !_context.IsLeader)
            return;

        _logger.LogError(fault, "Service node {Node}: the catalog's writer has faulted", NodeId);
        _context.Events.Publish("catalog", NodeId, $"writer faulted: {fault.Message}");
        var now = DateTimeOffset.UtcNow;
        if (now - _catalogReopenedAt > TimeSpan.FromMinutes(1))
        {
            try
            {
                await catalog.ReopenAsync(follow: false, stop).ConfigureAwait(false);
                _catalogReopenedAt = now;
                _context.Events.Publish("catalog", NodeId, "reopened from its log");
                _logger.LogWarning("Service node {Node}: the catalog was reopened from its log and is writable again", NodeId);
                return;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogError(ex, "Service node {Node}: the catalog could not be reopened", NodeId);
            }
        }
        await CannotLeadAsync("the catalog cannot be written", stop).ConfigureAwait(false);
    }

    private async Task CannotLeadAsync(string why, CancellationToken stop)
    {
        if (Interlocked.Exchange(ref _cannotLead, 1) != 0)
            return;
        var leadership = _context.Backend.Leadership;
        var peer = await ProbePeerAsync(stop).ConfigureAwait(false);
        if (peer is { Role: NodeRole.Standby, Ready: true })
        {
            _logger.LogError("Service node {Node} cannot lead ({Why}); stepping down to '{Peer}'", NodeId, why, peer.NodeId);
            _context.Events.Publish("node", NodeId, $"cannot lead ({why}); stepping down to '{peer.NodeId}'");
            leadership.StepDownAsync(CancellationToken.None).Forget(); // ends in Resigned: OnFinishedAsync exits with 4
            return;
        }
        _logger.LogCritical("Service node {Node} cannot lead ({Why}) and no ready standby answers; exiting with code 5", NodeId, why);
        _context.Events.Publish("node", NodeId, $"cannot lead ({why}); no ready standby answers");
        CannotLead?.Invoke(why);
        _ended.TrySetResult(5);
        if (_lifetime is not null && _context.Options.ExitOnLeadershipLoss)
        {
            Environment.ExitCode = 5;
            _lifetime.StopApplication();
        }
    }

    /// <summary>
    /// Completes when this node will not lead again in this process, with what its process
    /// would exit with (<see cref="ServiceOptions.ExitOnLeadershipLoss"/>): 3 when another
    /// node replaced it, 4 when it stepped down, 5 when it could not lead. By then its
    /// circuits are abandoned. A host that runs nodes in one process and does not let them
    /// end it (the in-memory backend's two nodes) starts a node again on this, through
    /// <see cref="NodeSupervisor.RunInProcessAsync{THost}"/>; a node that is stopped and never
    /// lost the leadership leaves it incomplete.
    /// </summary>
    public Task<int> Ended => _ended.Task;

    /// <summary>Raised when this node finds it cannot lead and no ready standby answers,
    /// just before it stops the host with exit code 5.</summary>
    public event Action<string>? CannotLead;

    private long _mirrored = -1;

    /// <summary>Writes the catalog to the backend's mirror, if it has one and the catalog
    /// has moved since the last write.</summary>
    private async Task MirrorAsync(CancellationToken cancellationToken)
    {
        var catalog = _context.Catalog;
        if (_context.Backend is not ICatalogMirror mirror || !_context.IsLeader || catalog.Version == _mirrored)
            return;
        var version = catalog.Version;
        await mirror.SaveAsync(catalog.Read().Select(e => new CatalogEntry(e.Key, e.Value)).ToList(), cancellationToken).ConfigureAwait(false);
        _mirrored = version;
    }

    /// <summary>Trims every table's history to its retention floor, the catalog's too.
    /// Runs every <see cref="ServiceOptions.TrimInterval"/> on the leader.</summary>
    public async Task TrimAsync(CancellationToken cancellationToken = default)
    {
        if (!_context.IsLeader)
            return;
        await _context.Tables.TrimAllAsync(cancellationToken).ConfigureAwait(false);

        // The catalog follows the same rule with no readers, and is never trimmed to latest.
        if (_context.Catalog is { Table: { } table, Log: { } log })
        {
            var now = DateTimeOffset.UtcNow;
            _catalogHistory.Enqueue((now, table.LatestVersion));
            while (_catalogHistory.Count > 1 && now - _catalogHistory.ElementAt(1).Time >= _context.Options.FollowerRetention)
                _catalogHistory.Dequeue();
            var oldest = _catalogHistory.Peek();
            if (now - oldest.Time >= _context.Options.FollowerRetention && oldest.Version + 1 > log.OldestRetainedVersion)
                await log.TrimBeforeAsync(oldest.Version + 1, cancellationToken).ConfigureAwait(false);
        }
    }

    private readonly Queue<(DateTimeOffset Time, long Version)> _catalogHistory = new();

    // ── status and health ──────────────────────────────────────────────────────────────

    private StandbyReadiness Readiness()
    {
        var catalog = _context.Catalog;
        var handles = _context.Tables.Handles.ToList();
        var faulted = handles.Count(h => h.FollowFault is not null || h.State is TableState.Faulted or TableState.Failed);
        var opening = handles.Count(h => h.State == TableState.Opening);
        var reasons = new List<string>();

        // First: a node built before the level the logs are written at can read none of
        // what follows, and no wait will change that.
        if (_context.Backend.FormatLevelWritten > _context.Backend.Formats.Reads)
            reasons.Add("format-level");
        if (!catalog.IsOpen)
            reasons.Add("catalog-not-open");
        else if (catalog.Version != _catalogApplied)
            reasons.Add("catalog-behind");
        if (faulted > 0)
            reasons.Add("table-faulted");
        if (opening > 0)
            reasons.Add("table-opening");
        if (_stateLagWindow.Behind)
            reasons.Add("state-behind");
        var pending = Volatile.Read(ref _pluginsPending);
        if (pending > 0)
            reasons.Add("plugin-pending");
        var toBuild = _context.Supervisor.WarmPending();
        if (toBuild > 0)
            reasons.Add("pipeline-building");
        return new StandbyReadiness(
            Math.Max(0, _catalogApplied), catalog.Version, Interlocked.Read(ref _stateLag), opening, faulted, pending, reasons.Count == 0, reasons,
            toBuild);
    }

    public NodeStatus Status
    {
        get
        {
            var leadership = _context.Backend.Leadership.Status;
            var pipelines = _context.Supervisor.Pipelines;
            var standby = leadership.Role == NodeRole.Standby ? Readiness() : null;
            return new NodeStatus(
                NodeId, leadership, _context.Catalog.Version, _context.Tables.Tables.Count, pipelines.Count,
                pipelines.GroupBy(p => p.State).ToDictionary(g => g.Key, g => g.Count()), standby, _context.Backend.Describe(),
                _context.Options.AdvertisedAdminUrl, ComputeHealth(leadership, pipelines, standby), _peer, _lastTakeover);
        }
    }

    public NodeHealth Health => Status.Health;

    // ── the meter's snapshot ───────────────────────────────────────────────────────────

    /// <summary>What the gauges of <see cref="ServiceMeter"/> report for this node; null
    /// until the first refresh.</summary>
    internal volatile GaugeSnapshot? Gauges;

    private volatile TableGauge[] _tableGauges = [];
    private int _gaugeTicks;
    private int _tablesRefreshing;
    private long _kafkaTakeoverRecorded;

    /// <summary>Once a second, after the metrics sample: cheap, and never throws.</summary>
    private void RefreshGauges()
    {
        try
        {
            // Tables ask their logs how far they are behind, so they are refreshed off
            // this thread, every five seconds, one refresh at a time.
            if (_gaugeTicks++ % 5 == 0 && Interlocked.Exchange(ref _tablesRefreshing, 1) == 0)
                Task.Run(RefreshTableGaugesAsync).Forget();
            RefreshNodeGauges();
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Service node {Node}: refreshing the meter's snapshot failed", NodeId);
        }
    }

    internal void RefreshNodeGauges()
    {
        if (Volatile.Read(ref _disposed) != 0)
            return;
        var status = Status;
        var role = status.Leadership.Role;
        bool? standbyReady = role switch
        {
            NodeRole.Standby => status.Standby?.TakeoverWouldBeWarm ?? false,
            NodeRole.Leader when _peerSampled && _context.Backend.Leadership.PeerExpected => _peer is { Role: NodeRole.Standby, Ready: true },
            _ => null,
        };
        Gauges = new GaugeSnapshot(
            NodeId, role, status.Leadership.Term, (int)status.Health.Level, standbyReady,
            role == NodeRole.Standby ? Interlocked.Read(ref _stateLag) : null, status.CatalogVersion,
            status.PipelinesByState, Metrics.Gauges, _tableGauges, ProcessMemory.Read());

        // The leadership's own stages are known only once it calls the takeover done.
        if (status.Leadership.Kafka?.LastTakeover is { } kafka && role == NodeRole.Leader
            && Interlocked.Exchange(ref _kafkaTakeoverRecorded, status.Leadership.Term) != status.Leadership.Term)
        {
            ServiceMeter.RecordTakeover(NodeId, "silence", kafka.Silence);
            ServiceMeter.RecordTakeover(NodeId, "fence", kafka.Fence);
            ServiceMeter.RecordTakeover(NodeId, "storage", kafka.Storage);
            ServiceMeter.RecordTakeover(NodeId, "total", kafka.Total);
        }
    }

    internal async Task RefreshTableGaugesAsync()
    {
        try
        {
            var gauges = new List<TableGauge>();
            foreach (var table in _context.Tables.Tables)
            {
                try
                {
                    var stats = await _context.Tables.StatsAsync(table.Name).ConfigureAwait(false);
                    gauges.Add(new TableGauge(
                        stats.Name, stats.State, stats.Rows, stats.Version, stats.LagVersions, stats.Loader?.Lag,
                        stats.Memory?.RowBytes ?? 0, stats.Memory?.IndexBytes ?? 0, stats.Memory?.HistoryBytes ?? 0));
                }
                catch (Exception ex) when (ex is ServiceException or ObjectDisposedException or InvalidOperationException)
                {
                    // Removed, or reopening: it is in the next refresh or it is gone.
                }
            }
            _tableGauges = [.. gauges];
        }
        finally
        {
            Volatile.Write(ref _tablesRefreshing, 0);
        }
    }

    private NodeHealth ComputeHealth(NodeLeadershipStatus leadership, IReadOnlyList<PipelineStatus> pipelines, StandbyReadiness? standby)
    {
        var reasons = new List<string>();
        var live = _started && Volatile.Read(ref _disposed) == 0;
        switch (leadership.Role)
        {
            case NodeRole.Leader:
            {
                var ready = _context.IsLeader && _context.Catalog.IsOpen;
                var level = ready ? HealthLevel.Healthy : HealthLevel.Unhealthy;
                foreach (var state in new[] { PipelineState.Failed, PipelineState.NeedsReset, PipelineState.Wedged, PipelineState.Blocked })
                    if (pipelines.Any(p => p.State == state))
                        reasons.Add("pipeline-" + state.ToString().ToLowerInvariant());
                var tables = _context.Tables.Tables;
                if (tables.Any(t => t.State == TableState.LoaderFaulted))
                    reasons.Add("loader-faulted");
                if (tables.Any(t => t.State is TableState.Faulted or TableState.Failed))
                    reasons.Add("table-faulted");
                // What a loader or a source reads has not been there for a while: nothing
                // has failed, and a table or a pipeline is empty because of it.
                var waitedSince = DateTimeOffset.UtcNow - _context.Options.MissingInputGrace;
                if (tables.Any(t => t.LoaderWaitingSince is { } since && since <= waitedSince))
                    reasons.Add("loader-waiting");
                if (pipelines.Any(p => p.Sources.Any(s => s.WaitingSince is { } since && since <= waitedSince)))
                    reasons.Add("source-waiting");
                // The absence of a standby is something seen, not something that is not
                // there — but only where a second node was configured at all.
                if (_peerSampled && _context.Backend.Leadership.PeerExpected && _peer is not { Role: NodeRole.Standby, Ready: true })
                    reasons.Add("no-ready-standby");
                if (reasons.Count > 0 && level == HealthLevel.Healthy)
                    level = HealthLevel.Degraded;
                var running = pipelines.Where(p => p.Desired == DesiredState.Running).ToList();
                if (running.Count > 0 && running.All(p => p.State == PipelineState.Failed))
                {
                    level = HealthLevel.Unhealthy;
                    reasons.Add("every-pipeline-failed");
                }
                if (!ready)
                    reasons.Add("not-ready");
                return new NodeHealth(live, ready, level, reasons);
            }
            case NodeRole.Standby:
            {
                var ready = _context.Catalog.IsOpen;
                reasons.AddRange(standby?.NotWarmBecause ?? []);
                var level = !ready || standby is { TablesFaulted: > 0 } ? HealthLevel.Unhealthy
                    : reasons.Count > 0 ? HealthLevel.Degraded
                    : HealthLevel.Healthy;
                return new NodeHealth(live, ready, level, reasons);
            }
            default:
                return new NodeHealth(live, false, HealthLevel.Unhealthy, [leadership.Role.ToString().ToLowerInvariant()]);
        }
    }

    // ── manual role changes ────────────────────────────────────────────────────────────

    /// <summary>On a standby: fences the leader and leads. Refused unless this node's
    /// takeover would be warm; <see cref="NodeActionOptions.Force"/> overrides.</summary>
    public async Task<ActionPlan> TakeOverAsync(NodeActionOptions options, CancellationToken cancellationToken)
    {
        var leadership = _context.Backend.Leadership;
        var status = leadership.Status;
        if (status.Role != NodeRole.Standby)
            throw ServiceException.Conflict($"Node '{NodeId}' is {status.Role}; only a standby can take over.");
        var readiness = Readiness();
        if (!readiness.TakeoverWouldBeWarm && !options.Force)
            throw new ServiceException(
                "not-ready", $"Node '{NodeId}' is not ready to lead; pass force to take over anyway.", 409, readiness.NotWarmBecause);
        // A warm standby has built most pipelines already, on the state it follows: those
        // are started, not built (WarmStandbyPipelines). The plan says which is which.
        var running = _context.Supervisor.Specs.Where(s => s.Desired == DesiredState.Running).Select(s => s.Id).ToHashSet(StringComparer.Ordinal);
        var built = _context.Supervisor.Pipelines.Count(p => running.Contains(p.Id) && p.State == PipelineState.Built);
        var cold = running.Count - built;
        var plan = new ActionPlan("node.takeover", NodeId, options.DryRun,
        [
            status.LeaderNodeId is { } leader
                ? $"node '{leader}' is fenced and stops leading; whatever supervises it starts it again as this node's standby"
                : "there is no leader to fence",
            $"node '{NodeId}' promotes the catalog, {_context.Tables.Tables.Count} table(s) and the state",
            running.Count == 0 ? "no pipeline is to run"
                : built == running.Count ? $"it starts the {built} pipeline(s) it has built while standing by, at their committed step"
                : built == 0 ? $"it builds and starts {cold} pipeline(s) from their committed state"
                : $"it starts the {built} pipeline(s) it has built while standing by, and builds and starts {cold} more from their committed state",
        ]);
        if (options.DryRun)
            return plan;
        _logger.LogInformation("Service node {Node}: takeover requested by {Caller}", NodeId, options.Caller ?? "local");
        await leadership.TakeOverAsync(cancellationToken).ConfigureAwait(false);
        await AuditAsync("node.takeover", options.Caller, plan, cancellationToken).ConfigureAwait(false);
        return plan;
    }

    /// <summary>On the leader: stops the pipelines and resigns, which is final for this
    /// process. Refused unless a ready standby answers; force overrides.</summary>
    public async Task<ActionPlan> StepDownAsync(NodeActionOptions options, CancellationToken cancellationToken)
    {
        var leadership = _context.Backend.Leadership;
        if (leadership.Status.Role != NodeRole.Leader)
            throw ServiceException.Conflict($"Node '{NodeId}' is {leadership.Status.Role}; only the leader can step down.");
        var peer = await leadership.ProbePeerAsync(cancellationToken).ConfigureAwait(false);
        var ready = peer is { Role: NodeRole.Standby, Ready: true };
        if (!ready && !options.Force)
            throw new ServiceException(
                "not-ready",
                peer is null ? "No standby answered; stepping down would stop the service. Pass force to do it anyway."
                    : $"Node '{peer.NodeId}' is {peer.Role} and not ready ({peer.NotReadyReason ?? "no reason given"}); pass force to step down anyway.");
        var plan = new ActionPlan("node.stepdown", NodeId, options.DryRun,
        [
            $"node '{NodeId}' stops its loaders and pipelines (at most {Durations.Text(_context.Options.StepDownTimeout)}), resigns and ends",
            ready ? $"node '{peer!.NodeId}' takes over" : "nothing takes over: the service stops until a node is started",
        ]);
        if (options.DryRun)
            return plan;
        _logger.LogInformation("Service node {Node}: step-down requested by {Caller}", NodeId, options.Caller ?? "local");
        await AuditAsync("node.stepdown", options.Caller, plan, cancellationToken).ConfigureAwait(false);
        await leadership.StepDownAsync(cancellationToken).ConfigureAwait(false);
        return plan;
    }

    private async Task AuditAsync(string action, string? caller, ActionPlan plan, CancellationToken cancellationToken)
    {
        try
        {
            using var write = await _context.Catalog.BeginAsync(cancellationToken).ConfigureAwait(false);
            write.Audit(NodeId, _context.Term, caller, action, NodeId, null, null, plan.Steps);
            await write.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(ex, "Service node {Node}: the audit entry for {Action} could not be written", NodeId, action);
        }
    }

    // ── the end ────────────────────────────────────────────────────────────────────────

    /// <summary>What the process of a node whose leadership has finished exits with:
    /// 4 when it stepped down, 3 when another node replaced it — the two ways a healthy
    /// node ends, which a supervisor answers by starting it again at once — and 5 when
    /// it is fenced by its own doing, a takeover that failed after it had taken the log
    /// (<see cref="NodeLeadershipStatus.TakeoverFailed"/>): it could not lead, and one
    /// that keeps failing so must not be started again at once for ever.</summary>
    internal static int ExitCodeFor(NodeLeadershipStatus status) =>
        status.Role == NodeRole.Resigned ? 4 : status.TakeoverFailed ? 5 : 3;

    /// <summary>Fenced and Resigned are final: abandon every circuit without draining
    /// (their commits can only fail) and, in a host, stop the application so that a
    /// process supervisor restarts it as a standby.</summary>
    private async Task OnFinishedAsync()
    {
        var finished = _context.Backend.Leadership.Status;
        var role = finished.Role;
        _context.Leading = false;
        try
        {
            await _context.Tables.StopLoadersAsync().ConfigureAwait(false);
            await _context.Supervisor.AbandonAllAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Service node {Node}: abandoning its circuits failed", NodeId);
        }

        // Who replaced it, and why, is in the new leader's records in the leader topic;
        // a fence met in a transaction comes before they are read. A moment for them,
        // after the circuits are down and before the process ends.
        if (role == NodeRole.Fenced && !finished.TakeoverFailed)
        {
            var until = Environment.TickCount64 + 2_000;
            while (finished.Previous?.Term != finished.Term && Environment.TickCount64 < until && !_stopping.IsCancellationRequested)
            {
                await Task.Delay(50).ConfigureAwait(false);
                finished = _context.Backend.Leadership.Status;
            }
        }

        // Replaced at an operator's word, or stepped down: the expected ends of a healthy
        // node, said once and not as a warning. A replacement after a silence, or a
        // takeover of its own that failed, is worth one.
        var replacedBy = finished.Previous is { } previous && previous.Term == finished.Term
            ? $" (node '{previous.ReplacedBy}' took over under term {previous.ReplacedByTerm}{(previous.Reason is { } reason ? $", {reason}" : "")})"
            : "";
        var expected = role == NodeRole.Resigned || (!finished.TakeoverFailed && finished.Previous is { Reason: "manual" } asked && asked.Term == finished.Term);
        _logger.Log(expected ? LogLevel.Information : LogLevel.Warning,
            "Service node {Node} is {Role}{Why}; it will not lead again in this process",
            NodeId, role, finished.TakeoverFailed ? " (its takeover failed: it could not lead)" : replacedBy);
        _ended.TrySetResult(ExitCodeFor(finished));
        if (_lifetime is not null && _context.Options.ExitOnLeadershipLoss && Volatile.Read(ref _disposed) == 0 && !_stopping.IsCancellationRequested)
        {
            Environment.ExitCode = ExitCodeFor(finished);
            _lifetime.StopApplication();
        }
    }

    /// <summary>Graceful shutdown. A leader with a ready standby steps down, so the
    /// standby takes over at once; otherwise the pipelines are stopped the same way and
    /// nothing is resigned. The stop budget is 60% of what the host allows.</summary>
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (!_started || _stopping.IsCancellationRequested)
            return;
        _stopping.Cancel();
        _stopBudget = TimeSpan.FromTicks((long)(_context.Options.StopTimeout.Ticks * 0.6));
        try
        {
            await _loop.ConfigureAwait(false);
        }
        catch
        {
        }

        var leadership = _context.Backend.Leadership;
        if (leadership.Status.Role == NodeRole.Leader)
        {
            await Contained("mirroring the catalog", () => MirrorAsync(CancellationToken.None)).ConfigureAwait(false);

            PeerInfo? peer = null;
            try
            {
                peer = await leadership.ProbePeerAsync(cancellationToken).ConfigureAwait(false);
            }
            catch
            {
            }
            if (peer is { Role: NodeRole.Standby, Ready: true })
            {
                await Contained("stepping down", () => leadership.StepDownAsync(CancellationToken.None)).ConfigureAwait(false);
            }
            else
            {
                await _context.Tables.StopLoadersAsync().ConfigureAwait(false);
                await _context.Supervisor.StopAllAsync(_stopBudget.Value).ConfigureAwait(false);
                _context.Leading = false;
            }
        }
        _logger.LogInformation("Service node {Node} stopped", NodeId);
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;
        ServiceMeter.Unregister(this);
        Metrics.Dispose();
        if (_started)
        {
            await StopAsync(CancellationToken.None).ConfigureAwait(false);
            _catalogFollow?.Cancel();

            // A node that was fenced or resigned is abandoning its circuits on another
            // thread: that ends before what they stand on is disposed.
            if (_context.Backend.Leadership.WhenFinished.IsCompleted)
                await _finished.CompletesWithin(_context.Options.StopTimeout + _context.Options.AbandonTimeout).ConfigureAwait(false);
            _context.Supervisor.ReleaseAll();
            await _context.Tables.DisposeAsync().ConfigureAwait(false);
            await _context.Catalog.DisposeAsync().ConfigureAwait(false);
            await _context.Storage.DisposeAsync().ConfigureAwait(false);
        }
        await _context.Backend.DisposeAsync().ConfigureAwait(false);
        _context.Plugins.DeleteOwnCache();
    }
}
