using System.Text.Json;
using Incremental.Tables;
using Microsoft.Extensions.Logging;

namespace Incremental.Service;

/// <summary>A table definition as the registry holds it, whatever its key and row types.</summary>
public interface ITableDefinition
{
    /// <summary>The default table name, and the default slot name in a pipeline.</summary>
    string Name { get; }
    Type KeyType { get; }
    Type RowType { get; }
}

/// <summary>
/// A shared table's shape, declared once in a contract assembly: its name, how a row gives
/// its key, and its indexes. A <see cref="TableSpec"/> names a definition class and a
/// table name, so one definition can be declared under several names.
/// </summary>
public abstract class TableDefinition<TKey, TRow> : ITableDefinition, ITableDefinitionInternal
    where TKey : notnull
    where TRow : class
{
    public abstract string Name { get; }
    public abstract TKey KeyOf(TRow row);

    /// <summary>Declares indexes and options. Runs once per table opened.</summary>
    protected virtual void Configure(TableBuilder<TKey, TRow> table)
    {
    }

    public Type KeyType => typeof(TKey);
    public Type RowType => typeof(TRow);

    internal TableBuilder<TKey, TRow> Configured()
    {
        var builder = new TableBuilder<TKey, TRow>();
        Configure(builder);
        return builder;
    }

    ITableHandle ITableDefinitionInternal.CreateHandle(TableRegistry registry, TableSpec spec) =>
        new TableHandle<TKey, TRow>(registry, this, spec);

    void ITableDefinitionInternal.CheckSource(TableRegistry registry, TableLoaderSpec loader)
    {
        var source = registry.CreateSource<TKey, TRow>(loader);
        (source as IDisposable)?.Dispose();
    }
}

internal interface ITableDefinitionInternal
{
    ITableHandle CreateHandle(TableRegistry registry, TableSpec spec);

    /// <summary>Asks the loader's factory for a source of this table's types and lets it
    /// go: a factory that does not serve them says so here, before anything is opened.</summary>
    void CheckSource(TableRegistry registry, TableLoaderSpec loader);
}

/// <summary>A secondary index, declared once with its selector, so that no pipeline
/// repeats a selector the core would silently ignore: read it with
/// <c>table.By(InstrumentsTable.ByIsin)</c>.</summary>
public sealed record IndexDefinition<TRow, TIndexKey>(string Name, Func<TRow, TIndexKey?> Selector)
    where TIndexKey : notnull;

public sealed class TableBuilder<TKey, TRow>
    where TKey : notnull
    where TRow : class
{
    internal List<DeclaredIndex> Indexes { get; } = [];

    /// <summary>A declared index: how to add it to a table, and how to read it with a key
    /// that arrives as JSON (the admin API's row lookup).</summary>
    internal sealed record DeclaredIndex(
        string Name, Type KeyType, Action<SharedTable<TKey, TRow>> Declare,
        ReadIndexPage ReadBy);

    /// <summary>A page of the rows under an index key given as text, and how many keys the
    /// index holds under it.</summary>
    internal delegate IReadOnlyList<KeyValuePair<TKey, TRow>> ReadIndexPage(
        TableSnapshot<TKey, TRow> snapshot, string key, int skip, int take, out int total);

    public SharedTableOptions Options { get; set; } = new();

    public TableBuilder<TKey, TRow> Index<TIndexKey>(IndexDefinition<TRow, TIndexKey> index) where TIndexKey : notnull
    {
        ArgumentNullException.ThrowIfNull(index);
        Indexes.Add(new DeclaredIndex(
            index.Name, typeof(TIndexKey), table => table.AddIndex(index.Name, index.Selector),
            (TableSnapshot<TKey, TRow> snapshot, string key, int skip, int take, out int total) =>
                snapshot.RowsBy(index.Name, TableKeys.Parse<TIndexKey>(key), skip, take, out total)));
        return this;
    }
}

public static class TableDefinitionExtensions
{
    /// <summary>The table's rows under a declared index.</summary>
    public static TableView<TIndexKey, TRow> By<TKey, TRow, TIndexKey>(
        this TableRef<TKey, TRow> table, IndexDefinition<TRow, TIndexKey> index)
        where TKey : notnull
        where TRow : class
        where TIndexKey : notnull =>
        table.By(index.Name, index.Selector);
}

internal static class TableKeys
{
    // Keys are contract or primitive types, which never unload, so one options instance serves.
    public static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    /// <summary>A key given as text: JSON (<c>42</c>, <c>"GB00"</c>, <c>{"a":1}</c>), or —
    /// for a key that is a JSON string, such as a string, a Guid or a date — the bare text.</summary>
    public static T Parse<T>(string text) where T : notnull
    {
        try
        {
            if (JsonSerializer.Deserialize<T>(text, Json) is { } parsed)
                return parsed;
        }
        catch (Exception ex) when (ex is JsonException or NotSupportedException or FormatException)
        {
        }
        try
        {
            if (JsonSerializer.Deserialize<T>(JsonSerializer.Serialize(text), Json) is { } quoted)
                return quoted;
        }
        catch (Exception ex) when (ex is JsonException or NotSupportedException or FormatException)
        {
        }
        throw ServiceException.Invalid($"'{(text.Length > 80 ? text[..80] + "…" : text)}' is not a key of type {typeof(T).Name}; give it as JSON.");
    }
}

public enum UnreadablePolicy
{
    /// <summary>The loader faults at the record: a table that silently lacks rows is worse
    /// than one that stops.</summary>
    Fault,

    /// <summary>The record is counted, kept in a ring of the last few, and skipped.</summary>
    Skip,
}

/// <summary>What fills a table: a kind (<c>file</c>, or a table source the host
/// registered), a named connection and the kind's configuration.</summary>
public sealed record TableLoaderSpec(string Kind, string? Connection = null, JsonElement? Config = null);

/// <summary>A declared table, as the catalog holds it under <c>table/{name}</c>.</summary>
public sealed record TableSpec
{
    public required string Name { get; init; }

    /// <summary>The full name of the <see cref="TableDefinition{TKey,TRow}"/> class.</summary>
    public required string Definition { get; init; }

    /// <summary>The simple name of the contract assembly that holds it; any loaded
    /// contract is searched when null.</summary>
    public string? Contract { get; init; }

    /// <summary>Null for a table written by a pipeline (<c>ToTable</c>) or by the host.</summary>
    public TableLoaderSpec? Loader { get; init; }
    public UnreadablePolicy OnUnreadable { get; init; } = UnreadablePolicy.Fault;
}

/// <summary><c>Waiting</c>: what the loader reads is not there yet (see
/// <see cref="LoaderStats.WaitingFor"/>); the table holds what it held.</summary>
public enum TableSourceState { Starting, Snapshotting, Streaming, Idle, Faulted, Waiting }

/// <summary>
/// A custom table loader: the snapshot + delta contract. An adapter with stable positions
/// stamps <c>(partition, offset)</c> on what it writes and resumes from
/// <see cref="SharedTable{TKey,TRow}.CommittedOffsets"/>; one without calls
/// <see cref="TableWriter{TKey,TRow}.ReplaceAllAsync(IAsyncEnumerable{TRow}, IReadOnlyDictionary{int, long}?, CancellationToken)"/>
/// on every start and then applies deltas.
/// </summary>
public interface ITableSource<TKey, TRow>
    where TKey : notnull
    where TRow : class
{
    /// <summary>Runs until cancelled. Throwing is a loader fault: it is restarted under the
    /// restart policy while readers keep the last version.</summary>
    Task RunAsync(TableSourceContext<TKey, TRow> context, CancellationToken cancellationToken);
}

/// <summary>Registered in DI under a kind name (<c>AddTableSource</c>). <c>Create</c>
/// is called when a table's loader starts and also when a declare is checked (the dry run
/// included), where the source is made and dropped unused: it must open nothing, and it
/// throws <see cref="InvalidOperationException"/> for a key or row type, or a config, it
/// does not serve — which refuses the declare.</summary>
public interface ITableSourceFactory
{
    ITableSource<TKey, TRow> Create<TKey, TRow>(TableLoaderSpec loader)
        where TKey : notnull
        where TRow : class;
}

/// <summary>Optional on an <see cref="ITableSourceFactory"/>: says, at a declare's dry
/// run, whether what the loader would read is there. Null when it is, or when the source
/// cannot tell; otherwise what is missing, in words ("view REF.PARTIES does not exist").
/// It must return within a few seconds and open nothing it does not close.</summary>
public interface ITableSourcePresence
{
    ValueTask<string?> MissingAsync(TableLoaderSpec loader, CancellationToken cancellationToken);
}

public sealed class TableSourceContext<TKey, TRow>
    where TKey : notnull
    where TRow : class
{
    private readonly TableHandle<TKey, TRow> _handle;

    internal TableSourceContext(TableHandle<TKey, TRow> handle, SharedTable<TKey, TRow> table, IServiceProvider services, ILogger logger)
    {
        _handle = handle;
        Table = table;
        Services = services;
        Logger = logger;
    }

    /// <summary><see cref="SharedTable{TKey,TRow}.CommittedOffsets"/> is where to resume.</summary>
    public SharedTable<TKey, TRow> Table { get; }
    public TableWriter<TKey, TRow> Writer => Table.Writer;
    public TableLoaderSpec Loader => _handle.Spec.Loader!;
    public JsonElement Config => Loader.Config ?? default;

    /// <summary>The host's services: connections, clients.</summary>
    public IServiceProvider Services { get; }
    public ILogger Logger { get; }

    public void Report(TableSourceState state, string? position = null, long? lag = null) =>
        _handle.ReportLoader(state, position, lag);

    /// <summary>What the source reads is not there: it says what it is waiting for
    /// ("object nope.jsonl in store refdata") and goes on looking. The table shows
    /// <see cref="TableSourceState.Waiting"/> with these words, and when it has lasted
    /// <see cref="ServiceOptions.MissingInputGrace"/> the node says so in an event and in
    /// its health. Any other report ends it.</summary>
    public void ReportWaiting(string waitingFor)
    {
        ArgumentException.ThrowIfNullOrEmpty(waitingFor);
        _handle.ReportLoaderWaiting(waitingFor);
    }

    /// <summary>A record the source could not read. Under <see cref="UnreadablePolicy.Skip"/>
    /// it is counted and kept in the table's ring; under <see cref="UnreadablePolicy.Fault"/>
    /// this throws, which faults the loader at that record.</summary>
    public void Unreadable(string position, Exception error, ReadOnlySpan<byte> record = default) =>
        _handle.Unreadable(position, error, record);
}

/// <summary>Thrown by <see cref="TableSourceContext{TKey,TRow}.Unreadable"/> under
/// <see cref="UnreadablePolicy.Fault"/>.</summary>
public sealed class TableRecordUnreadableException(string table, string position, Exception inner)
    : Exception($"Table '{table}': the record at {position} could not be read: {inner.Message}", inner)
{
    public string Position { get; } = position;
}

public enum TableState { Opening, Open, LoaderFaulted, Faulted, Failed }

/// <summary><c>LoaderWaitingFor</c> and <c>LoaderWaitingSince</c>: set while the table's
/// loader says that what it reads is not there.</summary>
public sealed record TableInfo(
    string Name, string Definition, string KeyType, string RowType, TableState State,
    string? LoaderWaitingFor = null, DateTimeOffset? LoaderWaitingSince = null);

/// <summary><c>WaitingFor</c>, with <c>State</c> <see cref="TableSourceState.Waiting"/>:
/// what the loader is waiting for, in its own words, and since when.</summary>
public sealed record LoaderStats(
    string Kind, TableSourceState State, string? Position, long? Lag, long Unreadable, int Restarts, string? Fault,
    string? WaitingFor = null, DateTimeOffset? WaitingSince = null);

/// <summary>A pipeline that reads or writes a table, by what its checkpoint has committed.</summary>
public sealed record TableReader(
    string Pipeline, long CommittedVersion, long VersionsBehind, long VersionsUntilCutLoose, TimeSpan? TimeUntilCutLoose);

public sealed record UnreadableRecord(
    long Sequence, DateTimeOffset Time, string Position, string ExceptionType, string Message, string? Record);

public sealed record TableStats(
    string Name, string KeyType, string RowType, string Epoch, bool Following,
    TableState State, string? Fault,
    long Rows, long Version, long OldestRetainedVersion, double VersionsPerSecond,
    IReadOnlyList<string> Indexes, string Writer,
    LoaderStats? Loader,
    long? LagVersions,
    IReadOnlyList<TableReader> Readers,
    string? FloorHeldBy,
    IReadOnlyDictionary<string, string>? IndexKeyTypes = null);

/// <summary>
/// An estimate of what a table holds on this node, made from a sample: the first
/// <c>SampledRows</c> rows in the table's own order (hash order, so not sorted by anything
/// meaningful) are written as JSON and their mean length in UTF-8 bytes is
/// <c>MeanRowBytes</c>; <c>RowsBytes</c> is that mean times the row count. It is the size
/// of the data, not of the objects: the process holds each row as a .NET object plus the
/// table's key map and indexes, which is usually a few times more and is not measured.
/// <c>RetainedVersions</c> is how many versions of history the table's log can still
/// replay. <c>HistoryChanges</c> is how many row changes those versions hold and
/// <c>HistoryBytes</c> that count at the same mean — both null when the history is not
/// in this process (a Kafka log keeps it on the brokers).
/// </summary>
public sealed record TableSizeEstimate(
    int SampledRows, long Rows, double MeanRowBytes, long RowsBytes, long RetainedVersions, long? HistoryChanges, long? HistoryBytes,
    DateTimeOffset At);

/// <summary>What to read of a table's rows: one row by <see cref="Key"/>; a page of the
/// rows under one key of <see cref="Index"/> (<see cref="Skip"/> passed over, then up to
/// <see cref="Limit"/>); or, with neither, a page of one scan. A scan passes over the first
/// <see cref="From"/> rows of the table without looking at them, then keeps the rows whose
/// JSON text contains <see cref="Contains"/>, passes over <see cref="Skip"/> of those and
/// returns up to <see cref="Limit"/>. Only a row that is looked at for
/// <see cref="Contains"/> counts towards <see cref="MaxScanned"/>, where the scan stops
/// whatever it has found; the page's <c>Next</c> is the <see cref="From"/> that carries on
/// from there, so every row of a table of any size can be reached.</summary>
public sealed record TableRowQuery(
    int Limit = 50, int Skip = 0, string? Contains = null, string? Key = null, string? Index = null, int MaxScanned = 100_000,
    long From = 0);

/// <summary>One row: its key and the row, both as JSON.</summary>
public sealed record TableRowData(JsonElement Key, JsonElement Row);

/// <summary>Rows read from one pinned snapshot. <c>Version</c> is the snapshot;
/// <c>Scanned</c> how many rows the read looked at; <c>Truncated</c> that the scan stopped
/// at its bound before the end of the table, so "not found" means "not reached";
/// <c>More</c> that another page follows; <c>Next</c> where it starts — a scan's
/// <see cref="TableRowQuery.From"/>, an index read's <see cref="TableRowQuery.Skip"/> —
/// and null when this page reached the end. A scan's <c>Next</c> is a position in the
/// table's own order, which holds for one version: across versions it is a sample, not a
/// cursor.</summary>
public sealed record TableRowPage(
    long Version, long TableRows, long Scanned, bool Truncated, bool More, IReadOnlyList<TableRowData> Rows, long? Next = null);

public interface ITableRegistry
{
    IReadOnlyList<TableInfo> Tables { get; }

    /// <summary>The open table of that name. Throws with both type names when the caller's
    /// types are not the table's — which is what a row type loaded twice looks like.</summary>
    SharedTable<TKey, TRow> Get<TKey, TRow>(string name) where TKey : notnull where TRow : class;
    TableStats Stats(string name);

    /// <summary><see cref="Stats"/> without blocking: on a standby the lag costs one short
    /// read of the table's log.</summary>
    Task<TableStats> StatsAsync(string name);

    /// <summary>The declared table's spec, as the catalog holds it.</summary>
    TableSpec Spec(string name);

    /// <summary>Rows of the table as this node holds it — any node answers, a standby from
    /// its followed copy. Bounded by the query; a row is serialized only if it is returned
    /// or filtered by text.</summary>
    TableRowPage ReadRows(string name, TableRowQuery query);
    IReadOnlyList<UnreadableRecord> Unreadable(string name, long afterSequence = 0);

    /// <summary>An estimate of the bytes the table's rows and retained history hold on
    /// this node, from a sample of at most a few hundred rows; kept for half a minute, so
    /// asking often costs nothing.</summary>
    TableSizeEstimate EstimateSize(string name);

    /// <summary>Leader: opens the table's log as its writer, commits <c>table/{name}</c>
    /// and starts the loader.</summary>
    Task DeclareAsync(TableSpec spec, CancellationToken cancellationToken, string? caller = null);

    /// <summary>Leader: what <see cref="DeclareAsync"/> would do, having made every check
    /// it makes before it opens anything — the name, that it is free, that the definition
    /// is found in a loaded contract, that the loader's kind and connection exist and its
    /// config has what the kind requires.</summary>
    ActionPlan PlanDeclare(TableSpec spec);

    /// <summary><see cref="PlanDeclare"/>, and a look at what the loader would read: when
    /// it is not there, the plan says so in <see cref="ActionPlan.Warnings"/>. The table
    /// can still be declared — it stays empty and its loader waits.</summary>
    Task<ActionPlan> PlanDeclareAsync(TableSpec spec, CancellationToken cancellationToken);

    /// <summary>Leader: refused while a pipeline names the table. Its log is left alone,
    /// so declaring the name again brings the rows back.</summary>
    Task<ActionPlan> RemoveAsync(string name, ActionOptions options, CancellationToken cancellationToken);
    Task<ActionPlan> RestartLoaderAsync(string name, ActionOptions options, CancellationToken cancellationToken);

    /// <summary>Opens a faulted or failed table from its log again and restarts the
    /// pipelines built on the old instance.</summary>
    Task ReopenAsync(string name, CancellationToken cancellationToken);
}
