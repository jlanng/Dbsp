using System.Collections.Concurrent;
using System.Reflection;
using System.Text.Json;
using Incremental.Kafka;
using Incremental.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Incremental.Service;

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
    public Func<IServiceProvider, IServiceBackend>? Backend { get; set; }

    /// <summary>The topic prefix of the Kafka backend's own log, when that is the backend.</summary>
    public Func<IServiceProvider, string>? ReservedTopicPrefix { get; set; }
}

/// <summary>What the registry, the plugin catalog and the supervisor share.</summary>
internal sealed class ServiceContext
{
    private readonly ConcurrentDictionary<string, IConnector> _connectors = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, IObjectStore> _objectStores = new(StringComparer.Ordinal);

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

    public void RequireLeader()
    {
        if (IsLeader)
            return;
        var status = Backend.Leadership.Status;
        throw new ServiceException(
            "not-leader",
            $"Node '{NodeId}' is {status.Role}; mutations go to the leader" + (status.LeaderNodeId is { } leader ? $" ('{leader}')." : "."), 409);
    }

    public IReadOnlyList<PipelineSpec> PipelineSpecs() => Supervisor.Specs;

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
        if (!Registrations.ObjectStores.TryGetValue(name, out var factory))
            throw ServiceException.Invalid(
                $"No object store named '{name}' is registered (registered: {string.Join(", ", Registrations.ObjectStores.Keys.Order())}).");
        return _objectStores.GetOrAdd(name, _ => factory(Services));
    }
}

public enum HealthLevel { Healthy, Degraded, Unhealthy }

/// <summary>Computed from the status, never from whether pipelines exist: an empty leader
/// is healthy.</summary>
public sealed record NodeHealth(bool Live, bool Ready, HealthLevel Level, IReadOnlyList<string> Reasons);

/// <summary>How ready a standby is to lead, in terms an operator can act on. On this
/// build a standby holds pipeline state as bytes and builds circuits at takeover.</summary>
/// <remarks><c>PluginsPending</c> counts plugin versions a deployed pipeline names whose files
/// this node has not yet fetched from the blob store.</remarks>
public sealed record StandbyReadiness(
    long CatalogVersionApplied, long CatalogVersionLatest, long StateLagRecords, int TablesBehind, int TablesFaulted,
    int PluginsPending, bool TakeoverWouldBeWarm, IReadOnlyList<string> NotWarmBecause);

/// <summary>Where the service's part of this node's takeover went. The leadership's own
/// record (<see cref="KafkaLeadershipDetail.LastTakeover"/>) has the stages around it:
/// its <c>Prepare</c> is <see cref="Catalog"/> plus <see cref="Tables"/>, and its
/// <c>Start</c> is <see cref="StartLeading"/>.</summary>
/// <remarks><c>Catalog</c>: opening or promoting the catalog and loading stored contracts.
/// <c>Tables</c>: promoting or opening every table, concurrently. <c>Pipelines</c>:
/// finishing pending resets and removals, then building and starting every pipeline from
/// its committed state, in parallel. <c>StartLeading</c>: all of <c>StartLeadingAsync</c> —
/// the leader record, the plugin directory, seeds and the pipelines.</remarks>
public sealed record ServiceTakeoverTimings(
    long Term, DateTimeOffset At, TimeSpan Catalog, TimeSpan Tables, int TableCount, TimeSpan Pipelines, int PipelineCount,
    TimeSpan StartLeading);

/// <summary>A node at a glance. <c>Peer</c> is what the other node last answered (null when
/// none is configured or none answered); <c>LastTakeover</c> is how this node's takeover
/// went (null if it has not led).</summary>
public sealed record NodeStatus(
    string NodeId, NodeLeadershipStatus Leadership, long CatalogVersion, int Tables, int Pipelines,
    IReadOnlyDictionary<PipelineState, int> PipelinesByState, StandbyReadiness? Standby, BackendInfo Backend,
    string? AdvertisedAdminUrl, NodeHealth Health, PeerInfo? Peer = null, ServiceTakeoverTimings? LastTakeover = null);

public sealed record NodeActionOptions(bool Force = false, bool DryRun = false, string? Caller = null);

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
    private int _pluginsPending;
    private volatile PeerInfo? _peer;
    private volatile bool _peerSampled;
    private volatile ServiceTakeoverTimings? _lastTakeover;
    private (TimeSpan Catalog, TimeSpan Tables, int TableCount) _prepared;
    private DateTimeOffset _catalogReopenedAt = DateTimeOffset.MinValue;
    private int _cannotLead;
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
        _logger = loggerFactory.CreateLogger("Incremental.Service.Node");
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
        events.Published += e => Events?.Invoke(e);
    }

    public string NodeId => _context.NodeId;
    public IServiceBackend Backend => _context.Backend;
    public Catalog Catalog => _context.Catalog;
    public ITableRegistry Tables => _context.Tables;
    public IPipelineSupervisor Pipelines => _context.Supervisor;
    public IPluginCatalog Plugins => _context.Plugins;
    public bool IsLeader => _started && _context.IsLeader;

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
        _context.Storage = ChangelogStorage.Follow(backend.State);
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

            await Contained("importing the plugin directory", () => _context.Plugins.ImportPluginDirectoryAsync(cancellationToken)).ConfigureAwait(false);
            foreach (var seed in _context.Registrations.Tables)
            {
                if (catalog.Get("table/" + seed.Name) is null)
                    await Contained($"seeding table '{seed.Name}'", () => _context.Tables.DeclareAsync(seed, cancellationToken, "seed")).ConfigureAwait(false);
            }

            var beforePipelines = clock.Elapsed;
            await _context.Supervisor.StartLeadingAsync(catalog.Read(), cancellationToken).ConfigureAwait(false);
            var pipelines = clock.Elapsed - beforePipelines;
            var pipelineCount = _context.Supervisor.Specs.Count;
            foreach (var seed in _context.Registrations.Pipelines)
            {
                if (catalog.Get("pipeline/" + seed.Id) is null)
                    await Contained($"seeding pipeline '{seed.Id}'",
                        () => _context.Supervisor.DeployAsync(seed, new DeployOptions(Caller: "seed"), cancellationToken)).ConfigureAwait(false);
            }
            _context.Tables.StartLoaders();
            _catalogApplied = catalog.Version;
            _lastTakeover = new ServiceTakeoverTimings(
                term, DateTimeOffset.UtcNow, _prepared.Catalog, _prepared.Tables, _prepared.TableCount, pipelines, pipelineCount, clock.Elapsed);
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

    private async Task Contained(string what, Func<Task> action)
    {
        try
        {
            await action().ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Service node {Node}: {What} failed", NodeId, what);
            _context.Events.Publish("node", NodeId, $"{what} failed: {ex.Message}");
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
        if (status.Role != NodeRole.Standby)
            return new PeerInfo(NodeId, status.Role, status.Term, status.Role == NodeRole.Leader, status.Role == NodeRole.Leader ? null : status.Role.ToString());
        var readiness = Readiness();
        return new PeerInfo(NodeId, status.Role, status.Term, readiness.TakeoverWouldBeWarm, readiness.NotWarmBecause.FirstOrDefault());
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

                                // No plugin code runs on a standby, but its files are fetched
                                // now, so that a takeover loads them from this node's disk.
                                Volatile.Write(ref _pluginsPending, await _context.Plugins.PrefetchAsync(entries, stop).ConfigureAwait(false));
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

                var now = DateTimeOffset.UtcNow;
                if (role == NodeRole.Standby && now - lastLag >= TimeSpan.FromSeconds(1))
                {
                    lastLag = now;
                    _stateLag = (await _context.Backend.Leadership.MeasureStateLagAsync(stop).ConfigureAwait(false))?.Sum() ?? 0;
                }

                if (now - lastWatch >= TimeSpan.FromSeconds(1))
                {
                    lastWatch = now;
                    if (role is NodeRole.Standby or NodeRole.Leader)
                    {
                        _peer = await ProbePeerAsync(stop).ConfigureAwait(false);
                        _peerSampled = true;
                    }
                    if (role == NodeRole.Leader && _context.IsLeader)
                        await WatchCatalogWriterAsync(stop).ConfigureAwait(false);
                }

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
            if (inner is Incremental.Tables.TableLogFencedException or KafkaLogFencedException)
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
        if (_lifetime is not null && _context.Options.ExitOnLeadershipLoss)
        {
            Environment.ExitCode = 5;
            _lifetime.StopApplication();
        }
    }

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
        if (!catalog.IsOpen)
            reasons.Add("catalog-not-open");
        else if (catalog.Version != _catalogApplied)
            reasons.Add("catalog-behind");
        if (faulted > 0)
            reasons.Add("table-faulted");
        if (opening > 0)
            reasons.Add("table-opening");
        if (Interlocked.Read(ref _stateLag) > 0)
            reasons.Add("state-behind");
        var pending = Volatile.Read(ref _pluginsPending);
        if (pending > 0)
            reasons.Add("plugin-pending");
        return new StandbyReadiness(
            Math.Max(0, _catalogApplied), catalog.Version, Interlocked.Read(ref _stateLag), opening, faulted, pending, reasons.Count == 0, reasons);
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
        var plan = new ActionPlan("node.takeover", NodeId, options.DryRun,
        [
            status.LeaderNodeId is { } leader ? $"node '{leader}' is fenced and ends" : "there is no leader to fence",
            $"node '{NodeId}' promotes the catalog, {_context.Tables.Tables.Count} table(s) and the state, then builds and starts " +
            $"{_context.Supervisor.Specs.Count(s => s.Desired == DesiredState.Running)} pipeline(s) from their committed state",
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
            $"node '{NodeId}' stops its loaders and pipelines (at most {_context.Options.StepDownTimeout:g}), resigns and ends",
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

    /// <summary>Fenced and Resigned are final: abandon every circuit without draining
    /// (their commits can only fail) and, in a host, stop the application so that a
    /// process supervisor restarts it as a standby.</summary>
    private async Task OnFinishedAsync()
    {
        var role = _context.Backend.Leadership.Status.Role;
        _context.Leading = false;
        _logger.LogWarning("Service node {Node} is {Role}; it will not lead again in this process", NodeId, role);
        try
        {
            await _context.Tables.StopLoadersAsync().ConfigureAwait(false);
            await _context.Supervisor.AbandonAllAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Service node {Node}: abandoning its circuits failed", NodeId);
        }
        if (_lifetime is not null && _context.Options.ExitOnLeadershipLoss && Volatile.Read(ref _disposed) == 0 && !_stopping.IsCancellationRequested)
        {
            Environment.ExitCode = role == NodeRole.Fenced ? 3 : 4;
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
        if (_started)
        {
            await StopAsync(CancellationToken.None).ConfigureAwait(false);
            _catalogFollow?.Cancel();
            _context.Supervisor.ReleaseAll();
            await _context.Tables.DisposeAsync().ConfigureAwait(false);
            await _context.Catalog.DisposeAsync().ConfigureAwait(false);
            await _context.Storage.DisposeAsync().ConfigureAwait(false);
        }
        await _context.Backend.DisposeAsync().ConfigureAwait(false);
    }
}
