using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using Incremental.Tables;
using Microsoft.Extensions.Logging;

namespace Incremental.Service;

/// <summary>Thrown when a pipeline needs a table that is not open on this node. The
/// pipeline is <c>Blocked</c> on it, not faulted.</summary>
public sealed class TableNotOpenException(string table, string reason)
    : Exception($"Table '{table}' is not open: {reason}")
{
    public string Table { get; } = table;
}

internal interface ITableHandle : IAsyncDisposable
{
    TableSpec Spec { get; }
    ITableDefinition Definition { get; }
    TableState State { get; }
    string? Fault { get; }
    string? LoaderWaitingFor { get; }
    DateTimeOffset? LoaderWaitingSince { get; }
    object? Table { get; }
    bool IsFollowing { get; }
    Exception? FollowFault { get; }
    long LatestVersion { get; }
    Task OpenAsync(bool follow, CancellationToken cancellationToken);
    Task PromoteAsync(CancellationToken cancellationToken);
    Task ReopenAsync(bool follow, CancellationToken cancellationToken);
    void StartLoader();
    Task StopLoaderAsync();
    Task TrimAsync(DateTimeOffset now, IReadOnlyList<(string Pipeline, long Version)> readers, CancellationToken cancellationToken);
    Task<TableStats> StatsAsync(IReadOnlyList<(string Pipeline, long Version)> readers, string writer);
    IReadOnlyList<UnreadableRecord> Unreadable(long afterSequence);
    TableRowPage ReadRows(TableRowQuery query);
    TableSizeEstimate EstimateSize();
}

internal sealed class TableHandle<TKey, TRow>(TableRegistry registry, TableDefinition<TKey, TRow> definition, TableSpec spec) : ITableHandle
    where TKey : notnull
    where TRow : class
{
    private readonly ILogger _logger = registry.Context.LoggerFactory.CreateLogger("Incremental.Service.Tables");
    private readonly TableBuilder<TKey, TRow> _configured = definition.Configured();
    private readonly Ring<UnreadableRecord> _unreadable = new(registry.Context.Options.UnreadableBuffer);
    private readonly Queue<(DateTimeOffset Time, long Version)> _history = new();
    private readonly SemaphoreSlim _gate = new(1, 1);

    private volatile SharedTable<TKey, TRow>? _table;
    private ITableLog<TKey, TRow>? _log;
    private volatile string? _fault;
    private volatile bool _failed;

    private CancellationTokenSource? _loaderStop;
    private Task _loader = Task.CompletedTask;
    private volatile bool _loaderFaulted;
    private TableSourceState _loaderState = TableSourceState.Idle;
    private string? _loaderPosition;
    private long? _loaderLag;
    private string? _loaderFault;
    private int _loaderRestarts;
    private volatile string? _waitingFor;
    private long _waitingSinceTicks;
    private bool _waitingAnnounced;

    private (DateTimeOffset Time, long Version) _rateSample;
    private double _rate;
    private string? _floorHeldBy;

    public TableSpec Spec { get; } = spec;
    public ITableDefinition Definition => definition;
    public object? Table => _table;
    public bool IsFollowing => _table?.IsFollowing ?? false;
    public Exception? FollowFault => _table?.FollowFault;
    public long LatestVersion => _table?.LatestVersion ?? 0;
    public string? Fault => _fault ?? (_loaderFaulted ? _loaderFault : null);

    public TableState State =>
        _failed ? TableState.Failed
        : _fault is not null ? TableState.Faulted
        : _table is null ? TableState.Opening
        : _loaderFaulted ? TableState.LoaderFaulted
        : TableState.Open;

    public async Task OpenAsync(bool follow, CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await OpenCoreAsync(follow, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }
    }

    private async Task OpenCoreAsync(bool follow, CancellationToken cancellationToken)
    {
        try
        {
            var log = await registry.Context.Backend.OpenTableLogAsync<TKey, TRow>(Spec.Name, cancellationToken).ConfigureAwait(false);
            var table = follow
                ? await SharedTable<TKey, TRow>.FollowAsync(Spec.Name, definition.KeyOf, log, _configured.Options, cancellationToken).ConfigureAwait(false)
                : await SharedTable<TKey, TRow>.OpenAsync(Spec.Name, definition.KeyOf, log, _configured.Options, cancellationToken).ConfigureAwait(false);

            // Before any loader runs; on a table with rows each is one scan.
            foreach (var index in _configured.Indexes)
                index.Declare(table);
            _log = log;
            _table = table;
            _fault = null;
            _failed = false;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _fault = $"{ex.GetType().Name}: {ex.Message}";
            _logger.LogError(ex, "Table {Table} could not be opened", Spec.Name);
            throw;
        }
    }

    /// <summary>The table's part of a takeover: a healthy follower is promoted; anything
    /// else is opened from the log as the writer, once.</summary>
    public async Task PromoteAsync(CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_table is { IsFollowing: false })
                return;
            if (_table is { FollowFault: null } follower)
            {
                try
                {
                    await follower.PromoteAsync(cancellationToken).ConfigureAwait(false);
                    return;
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    _logger.LogWarning(ex, "Table {Table} could not be promoted; reopening it from its log", Spec.Name);
                }
            }
            await ReopenCoreAsync(follow: false, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task ReopenAsync(bool follow, CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await ReopenCoreAsync(follow, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }
    }

    private async Task ReopenCoreAsync(bool follow, CancellationToken cancellationToken)
    {
        if (_table is { } old)
        {
            _table = null;
            await old.DisposeAsync().ConfigureAwait(false);
        }
        await OpenCoreAsync(follow, cancellationToken).ConfigureAwait(false);
        registry.OnReopened(Spec.Name);
    }

    // ── the loader ─────────────────────────────────────────────────────────────────────

    public void StartLoader()
    {
        if (Spec.Loader is null || !_loader.IsCompleted || _table is null || _table.IsFollowing)
            return;
        var stop = _loaderStop = new CancellationTokenSource();
        _loader = Task.Run(() => RunLoaderAsync(stop.Token));
    }

    public async Task StopLoaderAsync()
    {
        _loaderStop?.Cancel();
        try
        {
            await _loader.ConfigureAwait(false);
        }
        catch
        {
            // Reported through the table's state.
        }
        _loaderFaulted = false;
        _loaderState = TableSourceState.Idle;
    }

    private async Task RunLoaderAsync(CancellationToken stop)
    {
        var context = registry.Context;
        var policy = context.Options.Restart;
        var faults = new Queue<DateTimeOffset>();
        while (!stop.IsCancellationRequested)
        {
            try
            {
                var table = _table ?? throw new TableNotOpenException(Spec.Name, _fault ?? "it is being reopened");
                var source = registry.CreateSource<TKey, TRow>(Spec.Loader!);
                _loaderState = TableSourceState.Starting;
                await source.RunAsync(new TableSourceContext<TKey, TRow>(this, table, context.Services, _logger), stop).ConfigureAwait(false);
                _loaderFaulted = false;
                _loaderState = TableSourceState.Idle;
                return;
            }
            catch (OperationCanceledException) when (stop.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                if (IsFence(ex) || IsFence(await WriterFaultAsync().ConfigureAwait(false)))
                    return; // The node is ending; nothing here can mend it.

                _waitingFor = null;
                _loaderFaulted = true;
                _loaderState = TableSourceState.Faulted;
                _loaderFault = $"{ex.GetType().Name}: {ex.Message}";
                _loaderRestarts++;
                _logger.LogError(ex, "Loader of table {Table} faulted at {Position}", Spec.Name, _loaderPosition ?? "(no position)");
                context.Events.Publish("table", Spec.Name, $"loader faulted: {_loaderFault}");

                var now = DateTimeOffset.UtcNow;
                faults.Enqueue(now);
                while (faults.Count > 0 && now - faults.Peek() > policy.Within)
                    faults.Dequeue();
                if (faults.Count > policy.MaxRestarts)
                {
                    _failed = true;
                    _fault = $"loader failed {faults.Count} times within {policy.Within} at {_loaderPosition ?? "(no position)"}: {_loaderFault}";
                    _logger.LogError("Table {Table} is Failed: {Fault}", Spec.Name, _fault);
                    context.Events.Publish("table", Spec.Name, $"failed: {_fault}");
                    return;
                }

                try
                {
                    await Task.Delay(policy.BackoffAfter(faults.Count), stop).ConfigureAwait(false);

                    // A faulted writer cannot be mended in place: the table is opened from
                    // its log again, and the pipelines built on the old instance restart.
                    if (await WriterFaultAsync().ConfigureAwait(false) is { } writerFault)
                    {
                        _fault = $"writer faulted: {writerFault.Message}";
                        context.Events.Publish("table", Spec.Name, _fault);
                        await ReopenAsync(follow: false, stop).ConfigureAwait(false);
                    }
                }
                catch (OperationCanceledException) when (stop.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception reopen)
                {
                    _logger.LogError(reopen, "Table {Table} could not be reopened", Spec.Name);
                }
            }
        }
    }

    private static bool IsFence(Exception? ex)
    {
        for (; ex is not null; ex = ex.InnerException)
            if (ex is TableLogFencedException)
                return true;
        return false;
    }

    /// <summary>The writer's fault, if it has one: a commit of nothing asks it.</summary>
    private async Task<Exception?> WriterFaultAsync()
    {
        if (_table is not { IsFollowing: false } table)
            return null;
        try
        {
            await table.Writer.CommitAsync().ConfigureAwait(false);
            return null;
        }
        catch (InvalidOperationException ex) when (ex.Message.Contains("has faulted", StringComparison.Ordinal))
        {
            return ex.InnerException ?? ex;
        }
        catch
        {
            return null; // Owned by a circuit, or mid-commit: not a writer fault.
        }
    }

    public string? LoaderWaitingFor => _waitingFor;

    public DateTimeOffset? LoaderWaitingSince =>
        _waitingFor is not null && Interlocked.Read(ref _waitingSinceTicks) is > 0 and var ticks ? new DateTimeOffset(ticks, TimeSpan.Zero) : null;

    /// <summary>The loader says that what it reads is not there. The first time starts
    /// the clock; when the grace period has passed it is said once, as an event.</summary>
    internal void ReportLoaderWaiting(string waitingFor)
    {
        var now = DateTimeOffset.UtcNow;
        if (_waitingFor is null)
        {
            Interlocked.Exchange(ref _waitingSinceTicks, now.UtcTicks);
            _waitingAnnounced = false;
        }
        _waitingFor = waitingFor;
        _loaderState = TableSourceState.Waiting;
        _loaderFaulted = false;
        if (!_waitingAnnounced && now - new DateTimeOffset(Interlocked.Read(ref _waitingSinceTicks), TimeSpan.Zero) >= registry.Context.Options.MissingInputGrace)
        {
            _waitingAnnounced = true;
            _logger.LogWarning("Loader of table {Table} is still waiting for {What}; the table has {Rows} row(s)", Spec.Name, waitingFor, _table?.Count ?? 0);
            registry.Context.Events.Publish("table", Spec.Name, $"loader waiting for {waitingFor}");
        }
    }

    internal void ReportLoader(TableSourceState state, string? position, long? lag)
    {
        if (_waitingFor is not null)
        {
            _waitingFor = null;
            if (_waitingAnnounced)
                registry.Context.Events.Publish("table", Spec.Name, "loader no longer waiting: what it reads is there");
        }
        _loaderState = state;
        if (position is not null)
            _loaderPosition = position;
        _loaderLag = lag;
        if (state is TableSourceState.Streaming or TableSourceState.Idle)
            _loaderFaulted = false;
    }

    internal void Unreadable(string position, Exception error, ReadOnlySpan<byte> record)
    {
        var text = record.IsEmpty ? null : Encoding.UTF8.GetString(record[..Math.Min(record.Length, 4096)]);
        _loaderPosition = position;
        _unreadable.Add(sequence => new UnreadableRecord(
            sequence, DateTimeOffset.UtcNow, position, error.GetType().Name, error.Message, text));
        if (Spec.OnUnreadable == UnreadablePolicy.Fault)
            throw new TableRecordUnreadableException(Spec.Name, position, error);
    }

    public IReadOnlyList<UnreadableRecord> Unreadable(long afterSequence) => _unreadable.After(afterSequence);

    // ── retention and stats ────────────────────────────────────────────────────────────

    public async Task TrimAsync(DateTimeOffset now, IReadOnlyList<(string Pipeline, long Version)> readers, CancellationToken cancellationToken)
    {
        if (_table is not { IsFollowing: false } table || _log is not { } log)
            return;

        // The newest sample at least FollowerRetention old says where a follower may
        // still be; until one is that old nothing is trimmed on the followers' account.
        var retention = registry.Context.Options.FollowerRetention;
        _history.Enqueue((now, table.LatestVersion));
        while (_history.Count > 1 && now - _history.ElementAt(1).Time >= retention)
            _history.Dequeue();
        var oldest = _history.Peek();
        var followers = now - oldest.Time >= retention ? oldest.Version : Math.Min(oldest.Version, log.OldestRetainedVersion - 1);

        var floor = followers;
        string? heldBy = null;
        foreach (var (pipeline, version) in readers)
        {
            if (version < floor)
            {
                floor = version;
                heldBy = pipeline;
            }
        }
        _floorHeldBy = heldBy;

        // The cap, whatever the floor: a reader further behind than this is cut loose. The
        // in-memory log enforces its own; a Kafka log keeps what it is not told to drop.
        var capped = table.LatestVersion - registry.Context.Backend.RetainedHistoryVersions;
        if (capped > floor)
        {
            floor = capped;
            _floorHeldBy = null;
        }

        // A reader at version v next reads v + 1.
        if (floor + 1 > log.OldestRetainedVersion)
            await log.TrimBeforeAsync(floor + 1, cancellationToken).ConfigureAwait(false);
    }

    public async Task<TableStats> StatsAsync(IReadOnlyList<(string Pipeline, long Version)> readers, string writer)
    {
        var table = _table;
        var log = _log;
        var version = table?.LatestVersion ?? 0;
        var now = DateTimeOffset.UtcNow;
        if (_rateSample.Time == default)
            _rateSample = (now, version);
        else if (now - _rateSample.Time >= TimeSpan.FromSeconds(1))
        {
            _rate = (version - _rateSample.Version) / (now - _rateSample.Time).TotalSeconds;
            _rateSample = (now, version);
        }

        // A follower's lag is asked of the log, and a log whose broker has gone answers
        // after its own patience (the Kafka log's operation timeout, half a minute):
        // far too long for a figure on a page, and for a status that is asked while
        // brokers are down. So the question gets two seconds (LagPatience) — the wait
        // is given up, whether or not the log's call honours the token — and when it
        // had no answer nobody asks again for five (LagAskAgainAfter), for any table:
        // twenty tables do not wait in turn. The lag is then not known, and says so.
        long? lag = null;
        if (table is { IsFollowing: true } && log is not null && Environment.TickCount64 >= Volatile.Read(ref registry.LagUnknownUntil))
        {
            using var patience = new CancellationTokenSource(registry.LagPatience);
            try
            {
                lag = Math.Max(0, await log.LastVersionAsync(patience.Token).AsTask().WaitAsync(patience.Token).ConfigureAwait(false) - version);
            }
            catch
            {
                // Unknown is an honest answer.
                Volatile.Write(ref registry.LagUnknownUntil, Environment.TickCount64 + (long)registry.LagAskAgainAfter.TotalMilliseconds);
            }
        }

        var cap = registry.Context.Backend.RetainedHistoryVersions;
        var readerStats = readers.Select(r =>
        {
            var behind = Math.Max(0, version - r.Version);
            var until = Math.Max(0, cap - behind);
            return new TableReader(r.Pipeline, r.Version, behind, until, _rate > 0 ? TimeSpan.FromSeconds(until / _rate) : null);
        }).ToList();

        LoaderStats? loader = Spec.Loader is { } l
            ? new LoaderStats(l.Kind, _loaderState, _loaderPosition, _loaderLag, _unreadable.Total, _loaderRestarts, _loaderFaulted || _failed ? _loaderFault : null,
                _loaderState == TableSourceState.Waiting ? _waitingFor : null, _loaderState == TableSourceState.Waiting ? LoaderWaitingSince : null)
            : null;

        // A log that keeps its history in this process holds the superseded rows of every
        // version it retains, not only of the table's recent ones; the newest are the
        // same objects, so the larger of the two counts is the one that stands.
        var memory = table?.EstimateMemory();
        if (memory is not null && log is InMemoryTableLog<TKey, TRow> held && held.RetainedChanges > memory.HistoryChanges)
        {
            var changes = held.RetainedChanges;
            memory = memory with
            {
                HistoryChanges = changes,
                HistoryRows = changes,
                HistoryBytes = (long)(changes * (memory.MeanRowBytes + System.Runtime.CompilerServices.Unsafe.SizeOf<TableChange<TKey, TRow>>())),
            };
        }

        return new TableStats(
            Spec.Name, typeof(TKey).Name, typeof(TRow).Name, table?.Epoch ?? "", table?.IsFollowing ?? false,
            State, Fault,
            table?.Count ?? 0, version, log?.OldestRetainedVersion ?? 0, _rate,
            _configured.Indexes.Select(i => i.Name).ToList(), writer,
            loader, lag, readerStats, _floorHeldBy,
            _configured.Indexes.ToDictionary(i => i.Name, i => i.KeyType.Name, StringComparer.Ordinal),
            memory);
    }

    // Rows are contract types, which never unload, so one options instance serves.
    private static readonly JsonSerializerOptions RowJson = TableKeys.Json;

    private const int SizeSample = 256;
    private static readonly TimeSpan SizeEstimateAge = TimeSpan.FromSeconds(30);
    private volatile TableSizeEstimate? _size;

    public TableSizeEstimate EstimateSize()
    {
        var now = DateTimeOffset.UtcNow;
        if (_size is { } recent && now - recent.At < SizeEstimateAge)
            return recent;
        var table = _table ?? throw new ServiceException("not-ready", $"Table '{Spec.Name}' is not open on this node.", 503);
        var log = _log;
        long bytes = 0;
        var sampled = 0;
        long rows;
        using (var snapshot = table.Snapshot())
        {
            rows = table.Count;
            foreach (var (_, row) in snapshot.Rows())
            {
                bytes += JsonSerializer.SerializeToUtf8Bytes(row, RowJson).Length;
                if (++sampled >= SizeSample)
                    break;
            }
        }
        var mean = sampled == 0 ? 0 : (double)bytes / sampled;
        var version = table.LatestVersion;
        var retained = log is null ? 0 : Math.Max(0, version - log.OldestRetainedVersion + 1);
        long? changes = log is InMemoryTableLog<TKey, TRow> memory ? memory.RetainedChanges : null;
        return _size = new TableSizeEstimate(
            sampled, rows, Math.Round(mean, 1), (long)(mean * rows), retained, changes, changes is { } c ? (long)(mean * c) : null, now);
    }

    public TableRowPage ReadRows(TableRowQuery query)
    {
        var table = _table ?? throw new ServiceException("not-ready", $"Table '{Spec.Name}' is not open on this node.", 503);
        using var snapshot = table.Snapshot();
        var count = table.Count;
        var limit = Math.Max(1, query.Limit);
        var rows = new List<TableRowData>(Math.Min(limit, 64));

        static TableRowData Data(TKey key, TRow row) =>
            new(JsonSerializer.SerializeToElement(key, RowJson), JsonSerializer.SerializeToElement(row, RowJson));

        if (query.Index is { Length: > 0 } indexName)
        {
            var index = _configured.Indexes.FirstOrDefault(i => i.Name == indexName)
                ?? throw ServiceException.Invalid(
                    $"Table '{Spec.Name}' has no index named '{indexName}' (declared: {string.Join(", ", _configured.Indexes.Select(i => i.Name))}).");
            if (query.Key is not { Length: > 0 } indexKey)
                throw ServiceException.Invalid("A read by index needs a key.");
            var offset = Math.Max(0, query.Skip);
            var found = index.ReadBy(snapshot, indexKey, offset, limit, out var under);
            foreach (var (key, row) in found)
                rows.Add(Data(key, row));
            var beyond = offset + limit < under;
            return new TableRowPage(snapshot.Version, count, under, false, beyond, rows, beyond ? offset + limit : null);
        }

        if (query.Key is { Length: > 0 } keyText)
        {
            if (snapshot.TryGet(TableKeys.Parse<TKey>(keyText), out var row))
                rows.Add(Data(definition.KeyOf(row), row));
            return new TableRowPage(snapshot.Version, count, 1, false, false, rows);
        }

        // One scan, in the table's own order: stable while the table does not change, and
        // a sample rather than a cursor when it does — the version says which it was.
        // Rows passed over are stepped past, not looked at, and do not count towards the
        // bound: the bound is on the rows serialized to be searched.
        var from = Math.Max(0, query.From);
        var skip = Math.Max(0, query.Skip);
        var contains = string.IsNullOrEmpty(query.Contains) ? null : query.Contains;
        long position = 0, scanned = 0, matched = 0;
        var truncated = false;
        var more = false;
        foreach (var (key, row) in snapshot.Rows())
        {
            if (position < from)
            {
                position++;
                continue;
            }
            if (contains is not null)
            {
                if (scanned >= query.MaxScanned)
                {
                    truncated = true;
                    break;
                }
                var text = JsonSerializer.Serialize(row, RowJson);
                if (!text.Contains(contains, StringComparison.OrdinalIgnoreCase))
                {
                    scanned++;
                    position++;
                    continue;
                }
            }
            if (matched < skip)
            {
                matched++;
                if (contains is not null)
                    scanned++;
                position++;
                continue;
            }
            if (rows.Count == limit)
            {
                more = true; // this row starts the next page
                break;
            }
            rows.Add(Data(key, row));
            scanned++;
            position++;
        }
        return new TableRowPage(snapshot.Version, count, scanned, truncated, more || truncated, rows, more || truncated ? position : null);
    }

    public async ValueTask DisposeAsync()
    {
        await StopLoaderAsync().ConfigureAwait(false);
        if (_table is { } table)
            await table.DisposeAsync().ConfigureAwait(false);
    }
}

/// <summary>Owns the shared tables: opens each from the backend's log as the catalog
/// names it, follows on a standby, promotes at takeover, runs loaders on the leader and
/// trims history to the retention floor.</summary>
internal sealed class TableRegistry(ServiceContext context) : ITableRegistry
{
    private readonly ConcurrentDictionary<string, ITableHandle> _tables = new(StringComparer.Ordinal);
    private readonly SemaphoreSlim _declare = new(1, 1);
    private readonly ILogger _logger = context.LoggerFactory.CreateLogger("Incremental.Service.Tables");

    /// <summary>Until when (<see cref="Environment.TickCount64"/>) no table's statistics
    /// ask a log how far its follower is behind: one question that got no answer in
    /// <see cref="LagPatience"/> stands for them all for <see cref="LagAskAgainAfter"/>
    /// (<c>TableHandle.StatsAsync</c>).</summary>
    internal long LagUnknownUntil;

    /// <summary>How long a table's statistics wait for its log to say where it ends.</summary>
    internal TimeSpan LagPatience { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>How long nobody asks again after a question that had no answer.</summary>
    internal TimeSpan LagAskAgainAfter { get; set; } = TimeSpan.FromSeconds(5);

    public ServiceContext Context => context;

    /// <summary>A table was opened from its log again: whatever was built on the old
    /// instance must be rebuilt.</summary>
    public event Action<string>? Reopened;

    internal void OnReopened(string name)
    {
        context.Events.Publish("table", name, "reopened from its log");
        Reopened?.Invoke(name);
    }

    public IReadOnlyList<TableInfo> Tables =>
        _tables.Values.OrderBy(t => t.Spec.Name, StringComparer.Ordinal)
            .Select(t => new TableInfo(t.Spec.Name, t.Spec.Definition, t.Definition.KeyType.Name, t.Definition.RowType.Name, t.State, t.LoaderWaitingFor, t.LoaderWaitingSince))
            .ToList();

    internal IEnumerable<ITableHandle> Handles => _tables.Values;

    internal ITableHandle? Find(string name) => _tables.GetValueOrDefault(name);

    public SharedTable<TKey, TRow> Get<TKey, TRow>(string name)
        where TKey : notnull
        where TRow : class
    {
        if (!_tables.TryGetValue(name, out var handle))
            throw new TableNotOpenException(name, "no table of that name is declared");
        // A table whose loader or writer has failed still serves its last version; only
        // one with no table object at all — opening, or unopenable — blocks its readers.
        if (handle.Table is null)
            throw new TableNotOpenException(name, handle.Fault ?? "it is still opening");
        return handle.Table as SharedTable<TKey, TRow>
            ?? throw new InvalidOperationException(
                $"Table '{name}' holds {handle.Definition.RowType.AssemblyQualifiedName} by {handle.Definition.KeyType}, not " +
                $"{typeof(TRow).AssemblyQualifiedName} by {typeof(TKey)}. A row type of a shared table must come from a contract " +
                "assembly: the same type name loaded by two plugins is two types.");
    }

    private ITableHandle Require(string name) =>
        _tables.TryGetValue(name, out var handle) ? handle : throw ServiceException.NotFound($"Table '{name}'");

    // ── readers ────────────────────────────────────────────────────────────────────────

    /// <summary>Every pipeline in the catalog that has committed a version of the table,
    /// running or not: a stopped pipeline's checkpoint still needs its history.</summary>
    private List<(string Pipeline, long Version)> Readers(string table, IEnumerable<PipelineSpec> pipelines)
    {
        var readers = new List<(string, long)>();
        foreach (var pipeline in pipelines)
        {
            var committed = context.Storage.Pipeline(pipeline.Id).CommittedOffsets;
            if (committed.TryGetValue(table + "#t", out var read))
                readers.Add((pipeline.Id, read));
            else if (committed.TryGetValue(table + "#w", out var written))
                readers.Add((pipeline.Id, written));
        }
        return readers;
    }

    private string WriterOf(ITableHandle handle, IEnumerable<PipelineSpec> pipelines) =>
        pipelines.FirstOrDefault(p => p.Writes.Contains(handle.Spec.Name)) is { } writer ? $"pipeline:{writer.Id}"
        : handle.Spec.Loader is { } loader ? $"loader:{loader.Kind}"
        : "none";

    public TableStats Stats(string name) => StatsAsync(name).GetAwaiter().GetResult();

    public TableSpec Spec(string name) => Require(name).Spec;

    public TableRowPage ReadRows(string name, TableRowQuery query)
    {
        ArgumentNullException.ThrowIfNull(query);
        return Require(name).ReadRows(query);
    }

    public TableSizeEstimate EstimateSize(string name) => Require(name).EstimateSize();

    public Task<TableStats> StatsAsync(string name)
    {
        var handle = Require(name);
        var pipelines = context.PipelineSpecs();
        return handle.StatsAsync(Readers(name, pipelines), WriterOf(handle, pipelines));
    }

    public IReadOnlyList<UnreadableRecord> Unreadable(string name, long afterSequence = 0) => Require(name).Unreadable(afterSequence);

    internal async Task TrimAllAsync(CancellationToken cancellationToken)
    {
        var pipelines = context.PipelineSpecs();
        var now = DateTimeOffset.UtcNow;
        foreach (var handle in _tables.Values)
        {
            try
            {
                await handle.TrimAsync(now, Readers(handle.Spec.Name, pipelines), cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogWarning(ex, "Trimming table {Table} failed", handle.Spec.Name);
            }
        }
    }

    // ── reconciling with the catalog ───────────────────────────────────────────────────

    private ITableHandle CreateHandle(TableSpec spec)
    {
        var definition = context.Plugins.ResolveTableDefinition(spec);
        return ((ITableDefinitionInternal)definition).CreateHandle(this, spec);
    }

    /// <summary>Opens what the catalog names and this node has not, and drops what the
    /// catalog no longer names. Each table in its own try block: one that fails is
    /// <c>Faulted</c> and the rest carry on.</summary>
    internal async Task ReconcileAsync(IReadOnlyDictionary<string, string> catalog, bool asWriter, CancellationToken cancellationToken)
    {
        var specs = catalog.Where(e => e.Key.StartsWith("table/", StringComparison.Ordinal))
            .Select(e => ServiceJson.Read<TableSpec>(e.Value)).ToDictionary(s => s.Name, StringComparer.Ordinal);

        foreach (var (name, handle) in _tables)
        {
            if (!specs.ContainsKey(name) && _tables.TryRemove(name, out _))
                await handle.DisposeAsync().ConfigureAwait(false);
        }

        var work = new List<Task>();
        foreach (var spec in specs.Values)
        {
            if (_tables.TryGetValue(spec.Name, out var existing))
            {
                if (asWriter && (existing.IsFollowing || existing.Table is null))
                    work.Add(Contained(spec.Name, () => existing.PromoteAsync(cancellationToken)));
                else if (!asWriter && existing.FollowFault is not null)
                    work.Add(Contained(spec.Name, () => existing.ReopenAsync(follow: true, cancellationToken)));
                continue;
            }

            ITableHandle handle;
            try
            {
                handle = CreateHandle(spec);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Table {Table} cannot be created from its definition {Definition}", spec.Name, spec.Definition);
                context.Events.Publish("table", spec.Name, $"cannot be created: {ex.Message}");
                continue;
            }
            _tables[spec.Name] = handle;
            work.Add(Contained(spec.Name, async () =>
            {
                await handle.OpenAsync(follow: !asWriter, cancellationToken).ConfigureAwait(false);
                context.Events.Publish("table", spec.Name, asWriter ? "opened as the writer" : "following");
            }));
        }
        await Task.WhenAll(work).ConfigureAwait(false);
    }

    private async Task Contained(string table, Func<Task> action)
    {
        try
        {
            await action().ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Table {Table} is faulted", table);
            context.Events.Publish("table", table, $"faulted: {ex.Message}");
        }
    }

    internal void StartLoaders()
    {
        foreach (var handle in _tables.Values)
            handle.StartLoader();
    }

    internal Task StopLoadersAsync() => Task.WhenAll(_tables.Values.Select(h => h.StopLoaderAsync()));

    // ── actions ────────────────────────────────────────────────────────────────────────

    /// <summary>Every check a declare makes before it opens anything. Returns the
    /// definition's key and row type names.</summary>
    private (string KeyType, string RowType) CheckDeclare(TableSpec spec)
    {
        context.RequireLeader();
        ServiceNames.Validate(spec.Name, "table name");
        if (spec.Name.StartsWith("svc.", StringComparison.Ordinal))
            throw ServiceException.Invalid("Table names starting 'svc.' are reserved.");
        if (string.IsNullOrWhiteSpace(spec.Definition))
            throw ServiceException.Invalid("A table needs a definition: the full name of a TableDefinition class in a loaded contract.");
        if (_tables.ContainsKey(spec.Name))
            throw ServiceException.Conflict($"Table '{spec.Name}' is already declared.");
        ITableDefinition definition;
        try
        {
            definition = context.Plugins.ResolveTableDefinition(spec);
        }
        catch (InvalidOperationException ex)
        {
            throw ServiceException.Invalid(ex.Message);
        }
        if (spec.Loader is { } loader)
        {
            if (context.PipelineSpecs().FirstOrDefault(p => p.Writes.Contains(spec.Name)) is { } writer)
                throw ServiceException.Conflict($"Table '{spec.Name}' is written by pipeline '{writer.Id}'; it cannot also have a loader.");
            RequireSourceKind(loader);

            // A host's factory serves the tables it serves: ask it now, for this table's
            // types, rather than learn it from a loader that faults at every start.
            if (context.Registrations.TableSources.ContainsKey(loader.Kind))
            {
                try
                {
                    ((ITableDefinitionInternal)definition).CheckSource(this, loader);
                }
                catch (Exception ex) when (ex is not ServiceException and not OperationCanceledException)
                {
                    throw ServiceException.Invalid(
                        $"The {loader.Kind} loader cannot load table '{spec.Name}' ({definition.KeyType.Name} → {definition.RowType.Name}): {ex.Message}");
                }
            }
        }
        return (definition.KeyType.Name, definition.RowType.Name);
    }

    public ActionPlan PlanDeclare(TableSpec spec)
    {
        var (keyType, rowType) = CheckDeclare(spec);
        return new ActionPlan("table.declare", spec.Name, true,
        [
            $"create the log of table '{spec.Name}' ({keyType} → {rowType}) and open it as its writer; if a log of that name exists, its rows come back",
            "record the table in the catalog: a standby opens it and follows",
            spec.Loader is { } loader
                ? $"start its {loader.Kind} loader{(loader.Connection is { Length: > 0 } connection ? $" on connection '{connection}'" : "")}; a record it cannot read {(spec.OnUnreadable == UnreadablePolicy.Skip ? "is counted and skipped" : "faults the loader")}"
                : "no loader: a pipeline (ToTable) or the host writes it",
        ]);
    }

    public async Task<ActionPlan> PlanDeclareAsync(TableSpec spec, CancellationToken cancellationToken)
    {
        var plan = PlanDeclare(spec);
        if (spec.Loader is not { } loader)
            return plan;
        // A look, with a bound: a store or a broker that does not answer is not a reason
        // to refuse the dry run, and says nothing about whether the object is there.
        string? missing = null;
        using var bound = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        bound.CancelAfter(PresenceTimeout);
        try
        {
            if (context.Registrations.TableSources.TryGetValue(loader.Kind, out var factory))
                missing = factory(context.Services) is ITableSourcePresence presence ? await presence.MissingAsync(loader, bound.Token).ConfigureAwait(false) : null;
            else if (loader.Kind == "file")
            {
                var path = loader.Config!.Value.GetProperty("path").GetString()!;
                if (await context.ObjectStore(loader.Connection).StatAsync(path, bound.Token).ConfigureAwait(false) is null)
                    missing = $"object {path} is not in store {loader.Connection}";
            }
            else
            {
                var config = KafkaTableSource<string, string>.Check(context.KafkaConnections, loader);
                if (await context.KafkaConnections.Get(loader.Connection).TopicMissingAsync(config.Topic!, bound.Token).ConfigureAwait(false))
                    missing = $"topic {config.Topic} does not exist on connection {loader.Connection}";
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            missing = null;
        }
        return missing is null
            ? plan
            : plan with { Warnings = [$"{missing}: the table is declared empty and its loader {(loader.Kind == "file" ? "waits until it is there" : "faults and is retried until it is")}. Check the name."] };
    }

    internal static readonly TimeSpan PresenceTimeout = TimeSpan.FromSeconds(5);

    public async Task DeclareAsync(TableSpec spec, CancellationToken cancellationToken, string? caller = null)
    {
        CheckDeclare(spec);

        await _declare.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_tables.ContainsKey(spec.Name))
                throw ServiceException.Conflict($"Table '{spec.Name}' is already declared.");

            // The log is created before the entry is written: a table the catalog names
            // can then always be followed.
            var handle = CreateHandle(spec);
            await handle.OpenAsync(follow: false, cancellationToken).ConfigureAwait(false);
            try
            {
                using var write = await context.Catalog.BeginAsync(cancellationToken).ConfigureAwait(false);
                write.Put("table/" + spec.Name, spec);
                write.Audit(context.NodeId, context.Term, caller, "table.declare", spec.Name, null, null);
                await write.CommitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                await handle.DisposeAsync().ConfigureAwait(false);
                throw;
            }
            _tables[spec.Name] = handle;
            context.Events.Publish("table", spec.Name, "declared");
            handle.StartLoader();
        }
        finally
        {
            _declare.Release();
        }
    }

    public async Task<ActionPlan> RemoveAsync(string name, ActionOptions options, CancellationToken cancellationToken)
    {
        context.RequireLeader();
        var handle = Require(name);
        void RequireNoUsers()
        {
            var users = context.PipelineSpecs().Where(p => p.Reads.Contains(name) || p.Writes.Contains(name)).Select(p => p.Id).Order(StringComparer.Ordinal).ToList();
            if (users.Count > 0)
                throw ServiceException.Conflict($"Table '{name}' is named by pipeline(s) {string.Join(", ", users)}; remove them first.");
        }
        RequireNoUsers();
        var plan = new ActionPlan("table.remove", name, options.DryRun,
        [
            handle.Spec.Loader is null ? "no loader to stop" : $"stop the {handle.Spec.Loader.Kind} loader",
            "close the table on this node; a standby closes it when it reads the catalog",
            "the table's log is kept: declaring the name again brings its rows and epoch back",
        ]);
        if (options.DryRun)
            return plan;

        await _declare.WaitAsync(cancellationToken).ConfigureAwait(false);
        IDisposable? references = null;
        try
        {
            // Checked again under the lock a deploy commits its spec under: from here to
            // the end of the removal no pipeline can come to name the table.
            references = await context.LockReferencesAsync(cancellationToken).ConfigureAwait(false);
            if (!_tables.ContainsKey(name))
                throw ServiceException.NotFound($"Table '{name}'");
            RequireNoUsers();
            using (var write = await context.Catalog.BeginAsync(cancellationToken).ConfigureAwait(false))
            {
                write.Delete("table/" + name);
                write.Audit(context.NodeId, context.Term, options.Caller, "table.remove", name, null, null, plan.Steps);
                await write.CommitAsync(cancellationToken).ConfigureAwait(false);
            }
            if (_tables.TryRemove(name, out var removed))
                await removed.DisposeAsync().ConfigureAwait(false);
            context.Events.Publish("table", name, "removed");
        }
        finally
        {
            references?.Dispose();
            _declare.Release();
        }
        return plan;
    }

    public async Task<ActionPlan> RestartLoaderAsync(string name, ActionOptions options, CancellationToken cancellationToken)
    {
        context.RequireLeader();
        var handle = Require(name);
        if (handle.Spec.Loader is not { } loader)
            throw ServiceException.Conflict($"Table '{name}' has no loader.");
        var plan = new ActionPlan("table.loader.restart", name, options.DryRun,
            [$"stop the {loader.Kind} loader", "start it again: it resumes from the table's committed offsets, or takes its snapshot again"]);
        if (options.DryRun)
            return plan;
        await handle.StopLoaderAsync().ConfigureAwait(false);
        if (handle.State is TableState.Faulted or TableState.Failed)
            await handle.ReopenAsync(follow: false, cancellationToken).ConfigureAwait(false);
        handle.StartLoader();
        return plan;
    }

    public async Task ReopenAsync(string name, CancellationToken cancellationToken)
    {
        var handle = Require(name);
        var lead = context.IsLeader;
        await handle.StopLoaderAsync().ConfigureAwait(false);
        await handle.ReopenAsync(follow: !lead, cancellationToken).ConfigureAwait(false);
        if (lead)
            handle.StartLoader();
    }

    // ── loaders ────────────────────────────────────────────────────────────────────────

    private void RequireSourceKind(TableLoaderSpec loader)
    {
        if (context.Registrations.TableSources.ContainsKey(loader.Kind))
            return;
        switch (loader.Kind)
        {
            case "file":
                context.ObjectStore(loader.Connection);
                if (loader.Config is not { ValueKind: JsonValueKind.Object } file || !file.TryGetProperty("path", out var path)
                    || path.ValueKind != JsonValueKind.String || path.GetString() is not { Length: > 0 })
                    throw ServiceException.Invalid("The file loader needs a config with a \"path\".");
                break;
            case "kafka" or "debezium":
                try
                {
                    KafkaTableSource<string, string>.Check(context.KafkaConnections, loader);
                }
                catch (InvalidOperationException ex)
                {
                    throw ServiceException.Invalid($"The {loader.Kind} loader: {ex.Message}.");
                }
                break;
            default:
                throw ServiceException.Invalid(
                    $"No table source of kind '{loader.Kind}' is registered (built in: file, kafka, debezium; registered: " +
                    $"{string.Join(", ", context.Registrations.TableSources.Keys.Order())}).");
        }
    }

    internal ITableSource<TKey, TRow> CreateSource<TKey, TRow>(TableLoaderSpec loader)
        where TKey : notnull
        where TRow : class
    {
        // A registered kind wins, so a host can replace a built-in loader.
        if (context.Registrations.TableSources.TryGetValue(loader.Kind, out var factory))
            return factory(context.Services).Create<TKey, TRow>(loader);
        return loader.Kind switch
        {
            "file" => new FileTableSource<TKey, TRow>(context.ObjectStore(loader.Connection)),
            "kafka" => new KafkaTableSource<TKey, TRow>(context.KafkaConnections, loader, debezium: false),
            "debezium" => new KafkaTableSource<TKey, TRow>(context.KafkaConnections, loader, debezium: true),
            _ => throw new InvalidOperationException($"No table source of kind '{loader.Kind}' is registered."),
        };
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var handle in _tables.Values)
            await handle.DisposeAsync().ConfigureAwait(false);
        _tables.Clear();
    }
}

/// <summary>
/// The <c>file</c> loader: a snapshot source. It reads one object through a named
/// <see cref="IObjectStore"/> — JSON lines, or one JSON array — makes the table equal to it
/// (<c>ReplaceAllAsync</c>), and does so again whenever the object's identity changes.
/// Config: <c>{ "path": "counterparties.jsonl", "pollInterval": "00:00:05" }</c>.
/// </summary>
internal sealed class FileTableSource<TKey, TRow>(IObjectStore store) : ITableSource<TKey, TRow>
    where TKey : notnull
    where TRow : class
{
    // Rows are contract types, which are never unloaded, so one options instance serves.
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    public async Task RunAsync(TableSourceContext<TKey, TRow> context, CancellationToken cancellationToken)
    {
        var config = context.Config;
        if (config.ValueKind != JsonValueKind.Object || !config.TryGetProperty("path", out var pathProperty) || pathProperty.GetString() is not { Length: > 0 } path)
            throw new InvalidOperationException("The file loader needs a config with a \"path\".");
        var poll = config.TryGetProperty("pollInterval", out var interval) && TimeSpan.TryParse(interval.GetString(), out var parsed)
            ? parsed
            : TimeSpan.FromSeconds(5);

        string? loaded = null;
        while (true)
        {
            var info = await store.StatAsync(path, cancellationToken).ConfigureAwait(false);
            if (info is not null && info.Identity != loaded)
            {
                context.Report(TableSourceState.Snapshotting, path);
                await context.Writer.ReplaceAllAsync(ReadAsync(context, path, cancellationToken), null, cancellationToken).ConfigureAwait(false);
                loaded = info.Identity;
                context.Logger.LogInformation("Table {Table} loaded from {Path} at version {Version}", context.Table.Name, path, context.Table.LatestVersion);
            }
            // Never loaded and nothing there: a file that is yet to be delivered, or a
            // path with a typing mistake in it. Either way the table is as it was, and
            // that is said. An object that goes away after a load leaves its rows — and
            // "after a load" is the table's to say, not this run's: a node that took the
            // table over, or was restarted, starts here with nothing loaded by itself
            // and a table that has every version the load wrote.
            if (info is null && loaded is null && context.Table.LatestVersion == 0)
                context.ReportWaiting($"object {path} in store {context.Loader.Connection}");
            else
                context.Report(TableSourceState.Idle, path);
            await Task.Delay(poll, cancellationToken).ConfigureAwait(false);
        }
    }

    private async IAsyncEnumerable<TRow> ReadAsync(
        TableSourceContext<TKey, TRow> context, string path, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await using var stream = await store.OpenReadAsync(path, cancellationToken).ConfigureAwait(false);
        using var reader = new StreamReader(stream, Encoding.UTF8);

        var first = -1;
        while ((first = reader.Peek()) >= 0 && char.IsWhiteSpace((char)first))
            reader.Read();
        if (first == '[')
        {
            // One array: read as a whole, element by element.
            using var document = JsonDocument.Parse(await reader.ReadToEndAsync(cancellationToken).ConfigureAwait(false));
            var index = 0;
            foreach (var element in document.RootElement.EnumerateArray())
            {
                if (Parse(context, $"{path}[{index++}]", element.GetRawText()) is { } row)
                    yield return row;
            }
            yield break;
        }

        var line = 0;
        while (await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false) is { } text)
        {
            line++;
            if (string.IsNullOrWhiteSpace(text))
                continue;
            if (Parse(context, $"{path}:{line}", text) is { } row)
                yield return row;
        }
    }

    private static TRow? Parse(TableSourceContext<TKey, TRow> context, string position, string json)
    {
        try
        {
            return JsonSerializer.Deserialize<TRow>(json, Json) ?? throw new JsonException("The record is null.");
        }
        catch (Exception ex) when (ex is JsonException or NotSupportedException)
        {
            context.Unreadable(position, ex, Encoding.UTF8.GetBytes(json));
            return null;
        }
    }
}
