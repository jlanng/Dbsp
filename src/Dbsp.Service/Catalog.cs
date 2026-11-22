using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Dbsp.Tables;

namespace Dbsp.Service;

/// <summary>One row of the catalog: a key (<c>table/{name}</c>, <c>plugin/{name}/{version}</c>,
/// <c>pipeline/{id}</c>, <c>node/leader</c>, <c>audit/{seq}</c>) and its value as JSON.</summary>
public sealed record CatalogEntry(string Key, string Value);

/// <summary>An admin mutation as the catalog records it, in the same version as the change.</summary>
public sealed record AuditEntry(
    long Sequence, DateTimeOffset Time, string Node, long Term, string Caller, string Action, string Target,
    long? GenerationBefore, long? GenerationAfter, IReadOnlyList<string> Plan);

/// <summary>What an action will do, or did: the same record is returned by a dry run and by
/// the real action, and is what the audit entry holds.</summary>
public sealed record ActionPlan(string Action, string Target, bool DryRun, IReadOnlyList<string> Steps)
{
    /// <summary>What the action would go ahead with but the caller should look at first:
    /// an input whose file, prefix or topic is not there. Empty when there is nothing.</summary>
    public IReadOnlyList<string> Warnings { get; init; } = [];
}

public sealed record ActionOptions(long? ExpectedGeneration = null, bool DryRun = false, string? Caller = null);

internal static class ServiceJson
{
    /// <summary>For the service's own records (specs, manifests, audit). Never given a
    /// plugin's type: an options instance caches metadata for every type it has seen.</summary>
    public static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        Converters = { new JsonStringEnumConverter() },
    };

    public static string Write<T>(T value) => JsonSerializer.Serialize(value, Options);

    public static T Read<T>(string json) =>
        JsonSerializer.Deserialize<T>(json, Options) ?? throw new InvalidDataException($"A catalog value of type {typeof(T).Name} is null.");
}

/// <summary>
/// The durable record of what is deployed: a shared table, <c>svc.catalog</c>, on the
/// backend's table log, so fencing covers every write and a standby follows it like any
/// other table. Only the leader writes, one action at a time: an action and its audit
/// entry are one version.
/// </summary>
public sealed class Catalog : IAsyncDisposable
{
    public const string TableName = "svc.catalog";

    private readonly IServiceBackend _backend;
    private readonly ServiceOptions _options;
    private readonly SemaphoreSlim _write = new(1, 1);
    private volatile SharedTable<string, CatalogEntry>? _table;
    private ITableLog<string, CatalogEntry>? _log;
    private long _auditSequence = -1;

    internal Catalog(IServiceBackend backend, ServiceOptions options)
    {
        _backend = backend;
        _options = options;
    }

    public bool IsOpen => _table is not null;

    /// <summary>The catalog's version here; 0 until it is open.</summary>
    public long Version => _table?.LatestVersion ?? 0;

    internal SharedTable<string, CatalogEntry>? Table => _table;
    internal ITableLog<string, CatalogEntry>? Log => _log;

    internal async Task OpenAsync(bool follow, CancellationToken cancellationToken)
    {
        var log = await _backend.OpenTableLogAsync<string, CatalogEntry>(TableName, cancellationToken).ConfigureAwait(false);
        var table = follow
            ? await SharedTable<string, CatalogEntry>.FollowAsync(TableName, e => e.Key, log, null, cancellationToken).ConfigureAwait(false)
            : await SharedTable<string, CatalogEntry>.OpenAsync(TableName, e => e.Key, log, null, cancellationToken).ConfigureAwait(false);
        _log = log;
        _table = table;
    }

    /// <summary>Makes a followed catalog the writer; reopens it from the log if its
    /// follower had faulted. Throws when neither works: a node cannot lead without it.</summary>
    internal async Task PromoteAsync(CancellationToken cancellationToken)
    {
        var table = _table ?? throw new InvalidOperationException("The catalog is not open.");
        if (!table.IsFollowing)
            return;
        if (table.FollowFault is null)
        {
            await table.PromoteAsync(cancellationToken).ConfigureAwait(false);
            return;
        }
        await table.DisposeAsync().ConfigureAwait(false);
        _table = null;
        await OpenAsync(follow: false, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Opens the catalog from its log again, in place: the only mend for a writer
    /// that has faulted or a follower that has lost its history. Holds the write lock, so
    /// no action is half-way through the old table.</summary>
    internal async Task ReopenAsync(bool follow, CancellationToken cancellationToken)
    {
        await _write.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_table is { } old)
                await old.DisposeAsync().ConfigureAwait(false);
            _table = null;
            await OpenAsync(follow, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _write.Release();
        }
    }

    /// <summary>The writer's fault, if it has one. A commit of nothing asks it: a faulted
    /// writer refuses every call, and the core exposes its fault no other way.</summary>
    internal async Task<Exception?> WriterFaultAsync(CancellationToken cancellationToken)
    {
        if (_table is not { IsFollowing: false } table || !await _write.WaitAsync(0, cancellationToken).ConfigureAwait(false))
            return null; // not the writer here, or an action holds the lock and will find out itself
        try
        {
            await table.Writer.CommitAsync(cancellationToken).ConfigureAwait(false);
            return null;
        }
        catch (InvalidOperationException ex) when (ex.Message.Contains("has faulted", StringComparison.Ordinal))
        {
            return ex.InnerException ?? ex;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return null;
        }
        finally
        {
            _write.Release();
        }
    }

    /// <summary>Every entry as of the latest version here: one scan of a pinned snapshot.</summary>
    public IReadOnlyDictionary<string, string> Read()
    {
        var entries = new Dictionary<string, string>(StringComparer.Ordinal);
        if (_table is not { } table)
            return entries;
        using var snapshot = table.Snapshot();
        foreach (var (key, entry) in snapshot.Rows())
            entries[key] = entry.Value;
        return entries;
    }

    public string? Get(string key)
    {
        if (_table is not { } table)
            return null;
        using var snapshot = table.Snapshot();
        return snapshot.TryGet(key, out var entry) ? entry.Value : null;
    }

    public T? Get<T>(string key) where T : class => Get(key) is { } json ? ServiceJson.Read<T>(json) : null;

    /// <summary>The newest audit entries after <paramref name="afterSequence"/>, oldest first.</summary>
    public IReadOnlyList<AuditEntry> Audit(long afterSequence = -1, int limit = 200) =>
        Read().Where(e => e.Key.StartsWith("audit/", StringComparison.Ordinal))
            .Select(e => ServiceJson.Read<AuditEntry>(e.Value))
            .Where(a => a.Sequence > afterSequence)
            .OrderBy(a => a.Sequence)
            .TakeLast(limit)
            .ToList();

    /// <summary>Takes the catalog's write lock. Everything put through the returned write
    /// becomes one version at <see cref="CatalogWrite.CommitAsync"/>; disposing without
    /// committing writes nothing.</summary>
    internal async Task<CatalogWrite> BeginAsync(CancellationToken cancellationToken)
    {
        await _write.WaitAsync(cancellationToken).ConfigureAwait(false);

        // Read under the lock: a write that waited behind ReopenAsync must get the table
        // that was opened, not the one that was disposed.
        if (_table is not { } table)
        {
            _write.Release();
            throw new ServiceException("not-ready", "The catalog is not open on this node.", 503);
        }
        if (_auditSequence < 0)
        {
            var last = 0L;
            foreach (var key in Read().Keys)
                if (key.StartsWith("audit/", StringComparison.Ordinal) && long.TryParse(key.AsSpan(6), out var sequence))
                    last = Math.Max(last, sequence);
            _auditSequence = last;
        }
        return new CatalogWrite(this, table);
    }

    public async ValueTask DisposeAsync()
    {
        if (_table is { } table)
            await table.DisposeAsync().ConfigureAwait(false);
    }

    internal sealed class CatalogWrite(Catalog catalog, SharedTable<string, CatalogEntry> table) : IDisposable
    {
        private readonly List<(string Key, string? Value)> _staged = [];
        private bool _released;

        /// <summary>Refuses a value over <see cref="ServiceOptions.MaxCatalogValueBytes"/>
        /// before anything is staged: a row over the log's message limit would fault the
        /// catalog's writer for good.</summary>
        public void Put<T>(string key, T value)
        {
            var json = value as string ?? ServiceJson.Write(value);
            var size = Encoding.UTF8.GetByteCount(json);
            if (size > catalog._options.MaxCatalogValueBytes)
                throw new ServiceException(
                    "too-large", $"The value of '{key}' is {size} bytes; a catalog value may be at most {catalog._options.MaxCatalogValueBytes}.", 413);
            _staged.Add((key, json));
        }

        public void Delete(string key) => _staged.Add((key, null));

        /// <summary>Returns the entry's sequence, which only grows, whoever leads.</summary>
        public long Audit(string node, long term, string? caller, string action, string target,
            long? generationBefore, long? generationAfter, IReadOnlyList<string>? plan = null)
        {
            var sequence = ++catalog._auditSequence;
            Put($"audit/{sequence:D12}", new AuditEntry(
                sequence, DateTimeOffset.UtcNow, node, term, caller ?? "local", action, target,
                generationBefore, generationAfter, plan ?? []));
            if (sequence > catalog._options.AuditEntries)
                Delete($"audit/{sequence - catalog._options.AuditEntries:D12}");
            return sequence;
        }

        /// <summary>
        /// The token is looked at once, before anything reaches the table's writer. A
        /// writer's commit that is cancelled while it waits leaves its rows staged, and
        /// the next commit of anything would then publish an action that was reported as
        /// cancelled; so from the first staged row the write runs to its end.
        /// </summary>
        public async Task<long> CommitAsync(CancellationToken cancellationToken)
        {
            if (_staged.Count == 0)
                return table.LatestVersion;
            cancellationToken.ThrowIfCancellationRequested();
            foreach (var (key, value) in _staged)
            {
                if (value is null)
                    await table.Writer.DeleteAsync(key).ConfigureAwait(false);
                else
                    await table.Writer.UpsertAsync(new CatalogEntry(key, value)).ConfigureAwait(false);
            }
            _staged.Clear();
            return await table.Writer.CommitAsync(CancellationToken.None).ConfigureAwait(false);
        }

        public void Dispose()
        {
            if (_released)
                return;
            _released = true;
            catalog._write.Release();
        }
    }
}
