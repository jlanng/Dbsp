# Design: the enrichment service

> **Status:** proposed design, 2026-11-10, revised once after two reviews. Nothing here is
> built. Build steps S0 to S4 (see *Build order*) record what they change under *As built*.
> Sits on [shared-tables.md](shared-tables.md) and
> [durability-and-standby.md](durability-and-standby.md); it is the "step 7 (hosting)" both
> leave open, and the "pipeline registry" the second one declares out of scope.
> Requirements marked **Settled** were given. Choices marked **Default** were made here and
> are listed again under *Open for James* with the alternative and its cost. What the
> reviews proposed and this design does not take is under *Considered and rejected*.

## Why

The first application is enrichment as a service: pipelines defined at runtime by tenants,
each one circuit, joining streams against a few large reference tables that most pipelines
share. The library has the parts — shared tables, `Join`/`Lookup`/`ToTable`, acknowledged
sinks, the changelog storage, the Kafka logs, leadership, sources — and the failover app
(`tests/Incremental.Kafka.FailoverApp/Program.cs`) shows how to wire them by hand for one
fixed circuit. What is missing is everything that makes the set of tables and pipelines
*change while the process runs* and *survive on another node*:

- a registry that owns the tables, their loaders and their retention;
- a supervisor that deploys, replaces and restarts pipelines one at a time;
- a way to load pipeline code at runtime;
- a durable record of what is deployed, which a standby follows;
- one object that ties leadership to all of it, and an admin surface over it.

**Settled.** Joins are retroactive or on-arrival per join. Pipelines need joins, filters,
computed columns, aggregates and windows, and are code: definition classes in plugin
assemblies. Sources and sinks are Kafka (MSK in production), Debezium CDC, files and
custom adapters written elsewhere. Several processes exist for availability, not scale:
one active, one warm standby, failover in seconds. One circuit per pipeline; tables held
outside circuits. Delivery is at-least-once with keyed upserts. The tenant-facing product
is another repository; this one provides the packages it builds on.

## Shape

```
                       ┌──────────────────────── ServiceNode ────────────────────────┐
 admin API / UI ─────▶ │  Catalog (what is deployed)        Leadership (who leads)   │
 (Incremental.         │     │                                                       │
  Service.Admin)       │     ├─▶ PluginCatalog   contract context + one ALC/version  │
                       │     ├─▶ TableRegistry   SharedTable per table, loaders, trim│
                       │     └─▶ PipelineSupervisor   one circuit per pipeline        │
                       └────────────────────────┬────────────────────────────────────┘
                                                ▼
                                   IServiceBackend (the seam)
                          in-memory: InMemoryTableLog + InMemoryStateChangelog
                          Kafka:     KafkaLog + KafkaLeadership
```

Every node runs the same code and starts as a standby. It follows the catalog, opens what
the catalog names, and — when leadership says so — starts writing. The catalog is the only
input to "what should exist"; the node's role is the only input to "who runs it".

Two rules run through the whole design and are stated once here:

- **Only the catalog and the storage can fail a node.** A table that will not open, a
  plugin that will not load and a pipeline that will not build are failures of that
  table, plugin or pipeline. They are shown, retried and never end a takeover or move
  leadership.
- **Nothing waits without a bound.** Every teardown, step-down and shutdown has a deadline
  after which the thing is abandoned, counted and shown; nothing holds a node-wide lock
  across a drain.

## Packages

| Project | Targets | Depends on | Holds |
|---|---|---|---|
| `src/Incremental.Service` | net8.0; net10.0 | Incremental, Incremental.Hosting, Incremental.Kafka | everything below except HTTP |
| `src/Incremental.Service.Admin` | net8.0; net10.0 | Incremental.Service, `FrameworkReference Microsoft.AspNetCore.App` | JSON API, health endpoints, embedded UI |
| `tests/Incremental.Service.Tests` | net10.0 | both, xunit v3 | Kafka tests skip like `tests/Incremental.Kafka.Tests/KafkaEnv.cs` |

`Incremental.Service` does not use `CircuitHost<TDefinition>`: that type is keyed by a
compile-time class (`src/Incremental.Hosting/CircuitHost.cs`), and a pipeline here is
known only at runtime. It keeps the Hosting reference for the generic-host packages and
follows the same shutdown rule (drain the circuit while consumers still read).

## The backend seam

Everything durable and everything about roles goes through one interface, so the service
is written once and tested without a broker.

```csharp
public interface IServiceBackend : IAsyncDisposable
{
    string NodeId { get; }
    IStateChangelog State { get; }                       // → ChangelogStorage.Follow(State)
    /// On a standby, completes only once some writer has created the table's log.
    Task<ITableLog<TKey, TRow>> OpenTableLogAsync<TKey, TRow>(string name, CancellationToken ct)
        where TKey : notnull where TRow : class;
    IBlobStore Blobs { get; }                            // plugin files, by content hash
    INodeLeadership Leadership { get; }
    BackendInfo Describe();                              // kind, settings, counters — for the UI
}

public interface IBlobStore
{
    ValueTask<bool> ExistsAsync(string sha256, CancellationToken ct);
    Task PutAsync(string sha256, Stream content, CancellationToken ct);       // idempotent
    Task<bool> CopyToAsync(string sha256, Stream destination, CancellationToken ct);
    Task DeleteAsync(string sha256, CancellationToken ct);
}

public interface INodeLeadership
{
    NodeLeadershipStatus Status { get; }
    event Action<NodeLeadershipStatus>? Changed;
    Task WhenFinished { get; }                           // Fenced or Resigned
    long Term { get; }
    void Track(ChangelogStorage storage);
    Task StartAsync(LeadershipCallbacks callbacks, CancellationToken ct);
    Task TakeOverAsync(CancellationToken ct);
    Task StepDownAsync(CancellationToken ct);            // throws unless Role is Leader
    ValueTask<long[]?> MeasureStateLagAsync(CancellationToken ct);   // per state partition; null on a leader
    ValueTask<PeerInfo?> ProbePeerAsync(CancellationToken ct);       // null: no peer configured or no answer
    IUpsertSink<TKey, TRow> Leased<TKey, TRow>(IUpsertSink<TKey, TRow> sink) where TKey : notnull;
    IAppendSink<T> Leased<T>(IAppendSink<T> sink);
}

public sealed record NodeLeadershipStatus(
    string NodeId, NodeRole Role, long Term, string? LeaderNodeId, long LeaderTerm,
    KafkaLeadershipDetail? Kafka);                       // null on the in-memory backend
public sealed record KafkaLeadershipDetail(
    TimeSpan HeartbeatAge, DateTimeOffset? LeaderHeartbeatAt, bool LeaderResigned, bool HoldsLease,
    TakeoverTimings? LastTakeover);
public sealed record PeerInfo(string NodeId, NodeRole Role, long Term, bool Ready, string? NotReadyReason);

public sealed record LeadershipCallbacks(
    Func<CancellationToken, Task> PrepareAsync,          // writer acquired; storage not promoted yet
    Func<long, CancellationToken, Task> StartLeadingAsync, // storage promoted
    Func<CancellationToken, Task> StopLeadingAsync,      // step down: before the resignation
    Func<PeerInfo> DescribeSelf);                        // what this node answers a peer's probe with
```

`NodeRole` is `Incremental.Kafka`'s enum, reused. The status is the service's own record:
the in-memory backend has no heartbeats, lease or state-partition offsets, and making it
invent them would have the UI special-case invented numbers. `Kafka` is null there and the
UI hides that panel.

Both implementations run a takeover in the order `KafkaLeadership` already tests
(`KafkaLeadership.cs`, `PromoteAsync`): acquire the writer → `PrepareAsync` → promote
tracked storages → `StartLeadingAsync(term)`. An exception from any of these ends the
node `Fenced`, with the previous leader already deposed, which is why the service keeps
as much as it can out of them (see *Takeover*).

**The leadership does not track tables.** `KafkaLeadership.Track(table, log)` promotes
tracked tables one after another and any one throwing ends the takeover
(`KafkaLeadership.cs`, the `foreach` over `_tables`). The service promotes tables itself,
inside `PrepareAsync` — the writer is acquired by then, which is all
`SharedTable.PromoteAsync` asks for, and it is still before the storage, which is the
order that matters ("tables before circuits: a circuit's committed table version must
exist"). That lets it promote them concurrently and contain a failure to the one table.
Table lag is measured by the registry as `log.LastVersionAsync() − table.LatestVersion`,
which is all the leadership's own measurement does.

**`InMemoryServiceBackend`.** `InMemoryTableLog` per table and one `InMemoryStateChangelog`,
held in an `InMemoryServiceCluster` that several backends in one process may share
(`cluster.Node("a")`, `cluster.Node("b")`), so the standby path and the takeover order are
testable with no broker. The first node started leads at once. Another leads on
`TakeOverAsync` (which calls `TakeOver()` on every log connection and acquires the
changelog writer, fencing the old node exactly as the core's failover tests do) or when
the leader resigns. There are no heartbeats and no automatic election; `Leased` passes
sinks through. `ProbePeerAsync` asks the other node object directly. Blobs are a
dictionary in the cluster. A lone node is the default single-process mode.

**`KafkaServiceBackend`.** One `KafkaLog`, one `KafkaLeadership` built in `StartAsync`
from the callbacks, a `LeadershipProbe` listener when `ProbeListen` is set. The probe's
line gains a detail token carrying `Ready` and the reason (change K1), and
`ProbePeerAsync` reads it. With no `PeerProbe` configured the peer is unknown, and every
check below that asks "is there a ready standby" answers no.

```csharp
public sealed class KafkaServiceBackendOptions
{
    public required KafkaLogOptions Log { get; init; }
    public TimeSpan TakeoverAfter { get; init; } = TimeSpan.FromSeconds(3);
    public TimeSpan TakeoverCeiling { get; init; } = TimeSpan.FromSeconds(30);
    public TimeSpan? LeaseMargin { get; init; }
    public bool AutomaticTakeover { get; init; } = true;
    public IPEndPoint? ProbeListen { get; init; }
    public (string Host, int Port)? PeerProbe { get; init; }
}
```

`KafkaServiceBackend.Log` is public so a Kafka sink can ask for the transactional
producer (`KafkaSinkOptions.Log`).

**Blobs on Kafka** are a topic of their own, `{p}.svc.blobs`, compacted, one partition,
keyed `{sha256}/{n}` with 256 KB chunks and a last record `{sha256}/end` carrying the
count. They are written by a plain idempotent producer (`acks=all`), not the log's
transactional one, in the service package, using the bootstrap and client settings of
`KafkaLogOptions`. That is safe without fencing because the key is the content's hash: a
deposed leader can only write bytes that are already correct, and a partial upload is a
blob with no `end` record, which `ExistsAsync` reports as absent. A node reads a blob by
scanning the topic for that hash, once, into `CacheDirectory`. Nothing holds blobs on the
heap, puts them in a table version or makes a checkpoint queue behind an upload.

## Catalog

The catalog is the durable record of what is deployed. A standby or a restarted node
reconstructs the service from it and from nothing else.

> **Default (2): the catalog is a shared table.** `SharedTable<string, CatalogEntry>` named
> `svc.catalog`, opened on the backend's table log like any other table. On Kafka that is
> `{p}.table.svc.catalog.rows` (compacted) and `.changes`, written through the log's one
> transactional producer, so fencing covers every catalog write, and a standby follows it
> with `SharedTable.FollowAsync`. It needs no new code in `Incremental.Kafka`. The
> in-memory backend gets the same behaviour, fencing included, from `InMemoryTableLog`.
> Table names starting `svc.` are reserved.

```csharp
public sealed record CatalogEntry(string Key, string Value);     // Value: JSON
```

| Key | Value |
|---|---|
| `table/{name}` | `TableSpec`: definition type, contract, loader kind and config, options |
| `plugin/{name}/{version}` | `PluginManifest`: kind, files with hashes and sizes, definitions found with their settings schema, data-shape fingerprint |
| `pipeline/{id}` | `PipelineSpec` (below), with `generation`, `layout`, `confirmed`, `desired`, and the tables it reads and writes as resolved by the last dry run |
| `node/leader` | node id, term, advertised admin URL, loaded contract versions |
| `audit/{seq:D12}` | one admin mutation (see *Events, audit and logs*); the newest `AuditEntries` (1,000) are kept |

Rules:

- **Only the leader writes, one action at a time.** `TableWriter` has one staging batch
  for all callers and `CommitAsync` takes whatever is staged (`SharedTable.cs`,
  `CommitGatedAsync`), so the catalog has one write lock held from an action's first
  `Upsert` until its `CommitAsync` returns. An action — with its audit entry — is one
  version. A write that fails as fenced ends the node like any other.
- **Intent is durable before anything is done.** The leader commits the catalog change
  before it starts or destroys anything the change describes. Where the act is
  destructive and is a second transaction somewhere else (a state reset, a removal), the
  entry first records the intent, and a leader that finds an unfinished intent finishes
  it (*Reset*, *Remove*). Two things run the other way round and say so where they
  appear: a table's log is created before its entry is written, and a plugin's blobs are
  stored before its manifest.
- **A value is at most `MaxCatalogValueBytes` (256 KB)**, checked before staging. A larger
  spec is refused by the API with 413. The catalog is a table like any other: a row over
  the broker's message limit would fail the append and fault its writer for good.
- **Readers poll.** A node compares `catalog.LatestVersion` with the version it last
  applied every `CatalogPollInterval` (100 ms), takes a `Snapshot()`, scans it, and
  reconciles. The scan needs core change C4. The catalog holds specs, manifests and at
  most 1,000 audit entries, so the scan is small.
- **A manifest is usable when its blobs exist.** A node that reads a `plugin/` entry
  whose blobs it cannot fetch treats the plugin as pending and retries.
- **History.** The catalog's log is trimmed like every other table's (see *Retention*):
  never past where a follower may be.

> **Default (3): plugin binaries live in the backend's blob store**, beside the catalog
> and reached through the same broker. A standby then needs nothing but Kafka to build
> what the leader runs. Each node materialises the files it needs into
> `CacheDirectory/plugins/{name}/{version}/` and checks hashes before loading. The total
> size of stored plugins (the sum over manifests) is capped by `MaxStoredPluginBytes`
> (512 MB); an import past it is refused.

**In-memory backend.** The catalog is process memory. With
`InMemoryBackendOptions.CatalogDirectory` set, the leader writes `catalog.json` and
`blobs/{sha256}` after each commit, with write-then-rename, and loads them on its first
lead. Tables and state are still new at every start, so this restores *what is deployed*,
not its data: pipelines start empty and replay their sources.

**Seeds.** `AddTable` and `AddPipeline` on the DI builder are written to the catalog by
the first leader if their key is absent. After that the catalog wins.

## Connections

Slot and loader configuration is JSON in the catalog, written by tenants through the
product and returned by the admin API. It therefore cannot hold credentials, cannot name
arbitrary hosts or paths, and cannot express MSK's IAM authentication, which is a callback
(`KafkaSourceOptions.ConfigureConsumer`, `KafkaSinkOptions.ConfigureProducer`). Those
belong to the host, registered once under a name:

```csharp
services.AddIncrementalService(…)
    .AddKafkaConnection("msk-main", sp => new KafkaConnection
    {
        BootstrapServers = "…",
        ClientConfig = { ["security.protocol"] = "SASL_SSL", … },
        ConfigureConsumer = b => b.SetOAuthBearerTokenRefreshHandler(…),
        ConfigureProducer = b => b.SetOAuthBearerTokenRefreshHandler(…),
        AllowedTopicPrefixes = { "tenants." },                    // empty: any topic
    })
    .AddObjectStore("refdata", sp => new LocalObjectStore("/var/lib/refdata"));

public interface IObjectStore                           // local files built in; S3 is the host's
{
    ValueTask<ObjectInfo?> StatAsync(string path, CancellationToken ct);
    ValueTask<Stream> OpenReadAsync(string path, CancellationToken ct);
    ValueTask<Stream> OpenWriteAsync(string path, CancellationToken ct);      // write-then-rename
    ValueTask DeleteAsync(string path, CancellationToken ct);
    IAsyncEnumerable<ObjectInfo> ListAsync(string prefix, CancellationToken ct);
}
```

A spec says `{ "connector": "kafka", "connection": "msk-main", "topic": "…" }`. The
built-in connectors and loaders refuse a config that carries bootstrap servers, client
properties or an absolute path; `LocalObjectStore` is rooted and refuses any path that
resolves outside its root. The Kafka backend registers its own log's settings as the
connection `"log"`. A custom connector declares which of its config properties are secret
(`ConnectorDescription.SecretProperties`); the admin API returns those as `"***"`.

## Table registry

```csharp
// In a contract assembly (see Plugins).
public abstract class TableDefinition<TKey, TRow> : ITableDefinition
    where TKey : notnull where TRow : class
{
    public abstract string Name { get; }                 // default table name and default slot
    public abstract TKey KeyOf(TRow row);
    protected virtual void Configure(TableBuilder<TKey, TRow> table) { }
}
public sealed record IndexDefinition<TRow, TIndexKey>(string Name, Func<TRow, TIndexKey?> Selector)
    where TIndexKey : notnull;
public sealed class TableBuilder<TKey, TRow>
{
    public TableBuilder<TKey, TRow> Index<TIndexKey>(IndexDefinition<TRow, TIndexKey> index) where TIndexKey : notnull;
    public SharedTableOptions Options { get; set; }
}

// A contract, as a plugin author sees it:
public sealed class InstrumentsTable : TableDefinition<int, Instrument>
{
    public static readonly IndexDefinition<Instrument, string> ByIsin = new("isin", i => i.Isin);
    public override string Name => "instruments";
    public override int KeyOf(Instrument row) => row.Id;
    protected override void Configure(TableBuilder<int, Instrument> t) => t.Index(ByIsin);
}
public static class Tables { public static readonly InstrumentsTable Instruments = new(); }

public interface ITableRegistry
{
    IReadOnlyList<TableInfo> Tables { get; }
    SharedTable<TKey, TRow> Get<TKey, TRow>(string name) where TKey : notnull where TRow : class;
    TableStats Stats(string name);
    Task DeclareAsync(TableSpec spec, CancellationToken ct);      // leader
    Task<ActionPlan> RemoveAsync(string name, ActionOptions options, CancellationToken ct);       // leader; refused while a pipeline names it
    Task<ActionPlan> RestartLoaderAsync(string name, ActionOptions options, CancellationToken ct); // leader
    Task ReopenAsync(string name, CancellationToken ct);          // a Faulted table: open it from its log again
}
```

The index is declared once, selector included, and a pipeline reads it as
`instruments.By(InstrumentsTable.ByIsin)` (an extension that calls the core's
`By(name, selector)`), so no pipeline repeats a selector the core would silently ignore.
A `TableSpec` names a definition class and a table name; the same definition can be
declared under several names (`instruments-eu`, `instruments-us`).

`TableDefinition`'s own non-virtual members make the generic calls (`OpenAsync`,
`FollowAsync`, `AddIndex`, `PromoteAsync`, row and index reads from JSON keys), so the
registry holds tables as `ITableDefinition`-built handles and uses no reflection beyond
constructing the definition. `Get<TKey, TRow>` throws with both type names when a
caller's types are not the table's — which is what a row type loaded twice looks like.

**Lifecycle of one table.**

| Role | What the registry does |
|---|---|
| Standby, entry appears | `OpenTableLogAsync` (waits until the leader has created the log) → `SharedTable.FollowAsync` → declare indexes |
| Standby, follower faulted | dispose, follow again from the rows topic; the pipelines built on the old instance are rebuilt (see below) |
| Takeover | promoted by the registry inside `PrepareAsync`, all tables concurrently, the catalog first and alone |
| Leader, declared at runtime | open the log *as the writer* (this mints the epoch) → `SharedTable.OpenAsync` → declare indexes → commit `table/{name}` → start the loader |
| Leader, after takeover | start the loader, in `StartLeadingAsync`, after the circuits |
| Removed | stop the loader → dispose the table → delete the entry |

Indexes are declared before any loader runs, as shared-tables.md asks; an index added to
a definition later is built by one scan at the next process start. Removing a table leaves
its topics alone, so declaring the name again brings the old rows and epoch back
(*Open for James*, 8).

**A table's failures stay with the table.** Three things can go wrong with one table
that today end worse than they should:

- *A follower that loses its history.* A follower whose next version has been trimmed
  stops for good: `KafkaTableLog` and `InMemoryTableLog` both throw, `FollowLogAsync`
  files the exception in a private field, and `PromoteAsync` later refuses with "open the
  table from the log again" (`SharedTable.cs`). Core change C6 makes that fault readable
  (`SharedTable.FollowFault`). The registry checks it on each stats sample and before
  every promotion, and reopens: dispose, `FollowAsync` again, which restores from the
  rows topic. A standby's built circuits hold `TableRef`s to the old instance, so the
  supervisor rebuilds the pipelines that name it; they were never started, so that costs
  a build.
- *A table that cannot be opened or promoted at takeover.* `KafkaTableLog.OpenAsync`
  refuses a key type that is not byte-stable and a rows topic whose partition count
  changed; a contract may be missing on this node. In `PrepareAsync` each table is
  opened or promoted in its own try block. One that fails is reopened once as the writer
  (restore from rows); if that fails too it is `Faulted`, the takeover carries on, and
  the pipelines that name it are `Blocked` on it.
- *A writer that faults.* "Any failed append faults the writer for good", and so does an
  index selector that throws (`SharedTable.cs`, the two `Fault(...)` calls in the
  commit). If the cause is a fence the node is ending anyway. Otherwise the table is
  `Faulted`: its loader stops, readers keep the last version, the node keeps leading.
  The registry reopens it under the restart policy — a new `SharedTable` from the log,
  which is the only mend there is — and restarts the pipelines that read or write it,
  since they hold the old instance. A deterministic cause (one row over the broker's
  message limit, a selector that throws on one row) faults the same way at the same
  source position each time, so after `maxRestarts` the table is `Failed` with the
  loader's position and the exception, and stays that way until an operator fixes the
  source and calls `reopen`. Change K2 makes the oversized case name the key.

Only the catalog is different: it is a table whose writer the node cannot do without
(*When it cannot lead*).

**Loaders.** A table has at most one writer: a loader, or one pipeline's `ToTable`, or
nobody (the host writes `registry.Get(...).Writer` itself). `TableSpec.Loader` is a kind,
a connection and a JSON config:

| Kind | What it is |
|---|---|
| `kafka` | `KafkaSource.StartTableLoaderAsync` with `KafkaDeserializers.Keyed` (JSON) over a named Kafka connection |
| `debezium` | the same with `Debezium.Changes<TKey, TRow>()` |
| `file` | a snapshot loader: reads one object through a named `IObjectStore`, `ReplaceAllAsync`, again when the object's identity (length and modified time, or ETag) changes; polls every `pollInterval`. JSON lines or one JSON array |
| *custom* | an `ITableSource` factory the host registered under that name |

```csharp
public interface ITableSource<TKey, TRow> where TKey : notnull where TRow : class
{
    /// Runs until cancelled. Throwing is a loader fault.
    Task RunAsync(TableSourceContext<TKey, TRow> context, CancellationToken ct);
}
public sealed class TableSourceContext<TKey, TRow>
{
    public SharedTable<TKey, TRow> Table { get; }        // .CommittedOffsets is where to resume
    public TableWriter<TKey, TRow> Writer { get; }       // Upsert/Delete/Advance/Commit/ReplaceAllAsync
    public JsonElement Config { get; }
    public IServiceProvider Services { get; }            // the host's: connections, clients
    public ILogger Logger { get; }
    public void Report(TableSourceState state, string? position = null, long? lag = null);
    public void Unreadable(string position, Exception error, ReadOnlySpan<byte> record = default);
}
public enum TableSourceState { Starting, Snapshotting, Streaming, Idle, Faulted }
public interface ITableSourceFactory                    // registered in DI under a kind name
{
    ITableSource<TKey, TRow> Create<TKey, TRow>(JsonElement config) where TKey : notnull where TRow : class;
}
```

This is the snapshot + delta contract of durability-and-standby.md, *Source resumption*,
unchanged: a gRPC adapter with stable sequence numbers stamps `(partition, sequence)` and
resumes from `Table.CommittedOffsets`; one without calls `ReplaceAllAsync` on every start
and then applies deltas.

**Unreadable records.** `TableSpec.onUnreadable` is `fault` (the default: a table that
silently lacks rows is worse than one that stops) or `skip`. With `skip` the registry
passes `KafkaSourceOptions.OnUnreadableTableRecord`, and a custom source calls
`context.Unreadable`; either way the record is counted in `LoaderStats.Unreadable` and its
position, exception type and (when row data may be shown) first 4 KB go into a ring of
the last 64 per table. With `fault` the loader faults at that record, restarts under the
restart policy and ends `Failed` naming the position.

A loader runs on the leader only. When it faults it is restarted with the pipelines'
backoff policy and the table shows `LoaderFaulted` meanwhile; readers keep the last
version.

**Derived tables** are declared like any other, with no loader. A pipeline that calls
`ToTable` is the writer; the core refuses a second writer or a cycle at `Build()`
(`Tables/DerivedTable.cs`), and the registry refuses a loader on a table a deployed
pipeline writes, and a second deployed pipeline writing a table one already writes (the
catalog records each pipeline's resolved tables, so this needs no built circuit).

**Retention.** The leader cannot see where a standby's followers stand (a standby writes
nothing), and a follower that needs a trimmed version is dead until reopened. So the
floor has two parts. Every `TrimInterval` (30 s) the leader records `(now, LatestVersion)`
per table and computes

```
readers  = min over every pipeline in the catalog, in any state, of its committed {table}#t or {table}#w
followers = the table's version as of FollowerRetention (10 min) ago
floor    = min(readers, followers)          — with no reader: followers
```

and calls `log.TrimBeforeAsync(floor)`. The catalog is trimmed by the same rule with no
readers. A standby further behind than `FollowerRetention` loses its follower and the
registry reopens it, which is the right outcome for one that far behind.

The readers' numbers come from each pipeline's `ChangelogStorageProvider.CommittedOffsets`
(core change C2), so a stopped pipeline with no circuit counts. A pipeline that has never
committed holds nothing: it starts at the current version.

`RetainedHistoryVersions` per table caps the history whatever the floor (it is not
`SharedTableOptions.MaxRetainedVersions`, which is pin revocation). The default is
100,000 on Kafka, where history is disk. On the in-memory backend every retained version
is on the heap with its old and new rows (`TableLog.cs`, `History`), so the default there
is 1,024 — at 4,096 rows per `ReplaceAllAsync` version that is about four million row
changes at worst. A pipeline the cap passes is **cut loose**: it refuses its next start
and shows `NeedsReset`, and that moment is logged and raised as an event naming the table
and both versions.

Being cut loose is the delayed cost of stopping a pipeline, so it is announced before it
happens. Each reader in `TableStats` carries `VersionsUntilCutLoose` and an estimate of
the time left at the table's current rate; passing 80% of the cap raises an event and is
a `Degraded` health reason; the plan returned for a `stop` says "this pipeline reads
`instruments`; at the current rate it must restart within about 3 h".

**Stats**, sampled every 5 s and read by the admin API:

```csharp
public sealed record TableStats(
    string Name, string KeyType, string RowType, string Epoch, bool Following,
    TableState State, string? Fault,                             // Open, LoaderFaulted, Faulted, Failed
    long Rows, long Version, long OldestRetainedVersion, double VersionsPerSecond,
    IReadOnlyList<string> Indexes, string Writer,               // "loader:kafka", "pipeline:x", "none"
    LoaderStats? Loader,                                         // state, positions, lag, records read, unreadable, fault
    long? LagVersions,                                           // on a standby: log end − this copy
    IReadOnlyList<TableReader> Readers,                          // pipeline, committed version, versions behind, until cut loose
    string? FloorHeldBy);
```

Bytes are not reported: nothing in the core counts them and asking the broker per table
is not cheap.

## Plugins

**Trust.** A plugin is trusted code. It runs in the service's process with the service's
permissions; an `AssemblyLoadContext` isolates names, not behaviour. A delegate that loops
wedges its circuit until the process restarts (the supervisor bounds what that can block:
*Bounded operations*). Whoever can deploy a plugin can run anything, so every route that
loads code — upload over HTTP and rescanning the plugin directory — sits behind its own
switch and policy (see *Admin*).

**A plugin** is a directory or zip: the main assembly, its `.deps.json`, its private
dependencies. Its **version** is the first 16 hex digits of a SHA-256 over the sorted
`(path, file hash)` list. The same bytes are the same version on every node. Its name
comes from its manifest and must match `[a-z0-9][a-z0-9._-]{0,62}`, like a pipeline id,
because both become directory names.

**Two kinds**, told apart by an assembly attribute:

- A **contract assembly** (`[assembly: IncrementalContract]`) holds `TableDefinition`s and
  the row and key types of shared tables. It is loaded **once per process**, into one
  non-collectible context, and never unloaded.
- A **pipeline assembly** holds pipeline definitions. Each version is loaded into its
  own collectible `AssemblyLoadContext`.

**Type sharing, exactly.** A pipeline context resolves an assembly name in this order:
(1) the host's own — `Incremental*`, `Microsoft.Extensions.*`, `Confluent.Kafka`, the
framework — from the default context; (2) a loaded contract assembly, by simple name, from
the contract context, *even if the plugin ships its own copy*; (3) anything else privately,
through `AssemblyDependencyResolver`. So two plugins that both reference `Acme.Contracts`
get the same `Instrument` type, and it is the type the table was opened with. A row type
of a shared table declared in a pipeline assembly cannot work, and `registry.Get` says so.
A pipeline compiled against a newer contract than the one loaded fails at validation with
the missing member.

**Changing a contract.** A new version under the same assembly name is stored and
reported as *pending*: it takes effect when a process starts. Roll it with the pair:
restart the standby, step the leader down, restart the old leader. Rows are JSON in the
logs, so only additive changes survive this. The import compares the row shapes of every
declared table and, if any changed, is refused with 409 listing the differences (table,
member, was, is) unless the caller passes `acceptSchemaChange=true`. Anything else is a
new table under a new name. During the roll the two nodes run different contract
versions; each node reports the versions it has loaded in `/node` and the leader's are
in `node/leader`, so the plugins page shows "loaded here, pending on the leader". A
pipeline validated on one and not buildable on the other is `Unbuilt` there with that
reason, which `takeoverWouldBeWarm` reports.

**Import.** On the leader, whatever the route:

1. Unpack to a temporary directory. A zip is refused if it has more than `MaxPluginFiles`
   (512) entries, more than `MaxPluginBytes` (128 MB) uncompressed (counted while
   inflating, not from the headers), or an entry whose name is rooted, contains `..` or
   uses characters outside `[A-Za-z0-9._/-]`.
2. Hash; load into a throwaway context; find the definitions and their settings types;
   refuse a plugin with two definitions of one name; compute the **data-shape
   fingerprint** (a hash over the public fields and properties of every non-closure type
   the assembly defines) and each definition's settings schema; unload.
3. Put the files in the blob store (content-addressed, several modest writes, harmless if
   interrupted), then commit the manifest as one catalog version.

`PluginDirectory` is an import source only: at first lead and on `POST /plugins/rescan`
every package in it that the catalog lacks is imported. Nodes load from the catalog and
blob store, never from each other's disks.

**Built-in plugins.** `AddContracts(assembly)` and `AddPlugin(assembly)` register
assemblies compiled into the host. They live in the default context, their version is the
module's MVID, and they are never unloaded. Tests and the simple example use these.

**Unload.** A version is referenced by each pipeline generation built from it, on a leader
or a standby. When the count reaches zero and the supervisor has released the pipeline's
typed stores (core change C1), the context is unloaded.

What stands in the way is known, by reading. The core's per-type statics (`StableHash`,
`ByteStableEquality`, `ShardCodec`) are generic instantiations over the plugin's types and
go with its context. The process-wide `JsonSerializerOptions` instances do not: each
caches metadata for every type it has serialized, and `JsonStateSerializer.DefaultOptions`,
`KafkaDeserializers`/`KafkaSerializers.DefaultJson` and `Debezium.DefaultJson` are statics
in the default context. So nothing of a plugin's may pass through them:

- state: the supervisor gives each generation its own `JsonStateSerializer` over fresh
  options (core change C5: a serializer per provider rather than per storage), dropped
  with the generation;
- connectors: the built-in `kafka` and `file` connectors build one `JsonSerializerOptions`
  per generation and pass it to `KafkaDeserializers.Json(options)` and friends, which
  already take one; settings bind through the same per-generation options;
- tables are not affected: their rows are contract types, which are never unloaded.

Whether that is enough is the first thing built (**S0**, a spike: load a collectible
context, build and dispose a circuit with typed stores through a per-generation
serializer, release, assert the context is collected). The design does not depend on the
answer. Unloading is a request and any static holding a plugin type defeats it, so the
count of live contexts is bounded either way: a node refuses to load a version when
`MaxLoadedPluginVersions` (64) contexts are alive, counting those requested to unload and
not yet collected; the plugins page lists them; the remedy is a restart of that node,
standby first. If S0 fails, contexts are made non-collectible, the unload test is dropped
and the cap is the whole mechanism.

Catalog entries are deleted only by `DELETE /plugins/{name}/{version}`, refused while a
pipeline spec names the version; the delete removes the blobs no other manifest uses.

## Pipelines

### The definition contract

```csharp
public interface IPipelineDefinition                       // no settings
{
    void Build(PipelineBuilder pipeline);
}
public interface IPipelineDefinition<TSettings> where TSettings : class
{
    void Build(PipelineBuilder pipeline, TSettings settings);
}

[AttributeUsage(AttributeTargets.Class)]
public sealed class PipelineAttribute(string name) : Attribute
{
    public string Name { get; } = name;
    public string? Description { get; init; }
}

public sealed class PipelineBuilder
{
    public string PipelineId { get; }
    public CircuitBuilder Circuit { get; }                         // everything the core offers
    public ILogger Logger { get; }
    public IServiceProvider Services { get; }                      // the host's services

    public TableRef<TKey, TRow> Table<TKey, TRow>(TableDefinition<TKey, TRow> table, string? slot = null)
        where TKey : notnull where TRow : class;
    public InputStream<T> Input<T>(string slot) where T : notnull;
    public KeyedInputStream<TKey, TValue> KeyedInput<TKey, TValue>(string slot, Func<TValue, TKey> key)
        where TKey : notnull where TValue : notnull;

    public void UpsertSink<TKey, TRow>(string slot, ZStream<KeyedValue<TKey, TRow>> rows) where TKey : notnull where TRow : notnull;
    public void UpsertSink<T, TKey>(string slot, ZStream<T> rows, Func<T, TKey> key) where T : notnull where TKey : notnull;
    public void AppendSink<T>(string slot, ZStream<T> rows) where T : notnull;
    public void ToTable<TKey, TRow>(TableDefinition<TKey, TRow> table, ZStream<TRow> rows, string? slot = null)
        where TKey : notnull where TRow : class;
    public void ToTable<TKey, TValue, TRow>(TableDefinition<TKey, TRow> table, KeyedZStream<TKey, TValue> rows,
        Func<TKey, TValue, TRow> row, string? slot = null) where TKey : notnull where TRow : class;
}
```

```csharp
[Pipeline("trade-enrichment")]
public sealed class TradeEnrichment : IPipelineDefinition<TradeEnrichment.Options>
{
    public sealed record Options
    {
        [Range(0, 1e12), Description("Trades below this notional are dropped.")]
        public decimal MinNotional { get; init; }
    }

    public void Build(PipelineBuilder p, Options o)
    {
        var instruments = p.Table(Tables.Instruments);
        var enriched = p.Input<Trade>("trades")
            .Where(t => t.Notional >= o.MinNotional)                               // filter
            .GroupBy(t => t.InstrumentId).Named("by-instrument")
            .Join(instruments, (_, t, i) => new EnrichedTrade(t.Id, i.Symbol, t.Notional * i.Multiplier)); // join, computed column
        p.UpsertSink("enriched", enriched, e => e.TradeId);
    }
}
```

`Input` and `Table` return the core's own types, so lookups, aggregates, event time and
windows are the core API with nothing in between. Passing the definition infers key and
row types and gives the default slot name; the spec binds a slot to a declared table, so
one definition can run against `instruments-eu` in one deployment and `instruments-us` in
another, and two deployments of a definition that calls `ToTable` write two tables. Each
sink method calls `ToAcknowledgedOutput()` and remembers a typed closure that will later
start the delivery; each input remembers one that will start the source. No reflection,
and the type parameters never leave the plugin's code.

**Settings.** One settings type per definition, named by the interface, so discovery
learns it without running `Build`. `spec.settings` is bound with `System.Text.Json`:
camelCase names, case-insensitive, string enums, `UnmappedMemberHandling.Disallow` (a
misspelt property is an error, not a silent default). Records and `required`/`init`
members work, since nothing asks for `new()`. After binding, DataAnnotations are
validated (`[Required]`, `[Range]`, …). Both happen in the dry run, so a bad spec is
refused before anything stops. Discovery publishes a schema per definition — property,
JSON type, default, required, `[Description]`, and `[StateNeutral]` (below) — which is
what the UI builds a form from.

**The dry run.** Running `Build` against a `PipelineBuilder` and then
`CircuitBuilder.Describe(options)` (core change C3) instead of `Build()` yields the
pipeline's **shape**: slots with their types and kinds (append or keyed), tables read and
written, the node graph, and the store names the circuit would mint. That is the
validation — every slot bound to a connector that supports its kind, every table declared
with those types, the settings bind and validate — and it has no side effects, because a
table is claimed only at `Circuit.Build()`. The supervisor keeps the last shape of every
pipeline, so the graph exists for one that is stopped, failed or not yet deployed. Store
names that are positional (no `.Named()`) are returned as warnings.

A declarative spec later is one more definition, built in, whose settings are the spec.
Nothing here stands in its way.

### Writing a definition

These are rules for plugin authors; the README of the example repeats them.

- A definition has a public parameterless constructor. `Build` runs many times — at
  import, at every dry run, for every generation, on the standby, on a rollback — and
  must do nothing but describe the circuit: no I/O, no threads, no static writes.
- Delegates are deterministic. A record dropped or mapped one way when inserted must be
  dropped or mapped the same way when retracted (`RecordErrors.cs`).
- **A value captured by a delegate upstream of a stateful operator is part of the
  state.** Changing a filter threshold, a grouping key or a result selector while keeping
  state leaves rows the new logic would not have produced, never retracted. The service
  asks on every such change (*Replace*); the honest answer is usually `reset`. Mark a
  settings property `[StateNeutral]` only if it changes no stored row — a label, a
  batch size.
- Name every stateful operator with `.Named()`. Positional names move when an operator is
  inserted above, and the state then has no home.
- Hold nothing of the plugin's in a static. Statics pin the load context. A service the
  definition needs (a pricing library, a clock) comes from `p.Services`.
- Row and key types of shared tables live in a contract assembly. Everything else is
  private to the plugin.
- State is JSON. A property renamed between versions reads back as its default.

### The spec

```json
{ "id": "trades-eu", "plugin": "acme.pipelines", "version": "9f2c41d07ab35e10", "definition": "trade-enrichment",
  "desired": "Running", "generation": 3, "layout": 3, "confirmed": 3,
  "settings": { "minNotional": 1000 },
  "circuit":  { "workers": 1, "checkpointInterval": 1, "checkpointMaxDelay": "00:00:01", "errorPolicy": "DeadLetter" },
  "tables":   { "instruments": "instruments-eu" },
  "inputs":   { "trades":   { "connector": "kafka", "connection": "msk-main", "topic": "tenants.eu.trades", "startFrom": "earliest" } },
  "sinks":    { "enriched": { "connector": "kafka", "connection": "msk-main", "topic": "tenants.eu.trades.enriched" } },
  "restart":  { "maxRestarts": 5, "within": "00:10:00", "initialBackoff": "00:00:01", "maxBackoff": "00:01:00" } }
```

The id is `[a-z0-9][a-z0-9._-]{0,62}`. It is the `ChangelogStorage.Pipeline(id)` key and
the circuit's name, hence the `circuit` tag on every metric. `circuit` binds the scalar
members of `CircuitOptions`; storage is always the changelog provider. `tables` maps a
slot to a declared table and defaults to the slot's own name.

Three numbers, all assigned by the leader, never by the caller:

- `generation` goes up with every change to the entry. It is the optimistic-concurrency
  token: a mutation carries the generation it was based on and is refused with 412 if the
  entry has moved.
- `layout` is the generation at which the state's meaning last changed: a replace that
  changed plugin version, definition, a settings property not marked `[StateNeutral]`, an
  input or table binding, or the worker count; or a reset. It is written into the
  pipeline's commit records (C1), so the state says which spec made it.
- `confirmed` is the last generation that reached `Running` on a leader. A standby builds
  typed stores only for a confirmed generation.

During a replace the entry also holds `previous` (version, definition, settings,
bindings, layout) and during a reset or removal `pending: "reset" | "remove"`.

**Connectors** are registered in DI under a kind name and given the slot's JSON. A
connector implements the shapes it supports and no others:

```csharp
public interface IConnector { ConnectorDescription Describe(JsonElement config); }   // throws on a bad config; names secret properties
public interface IAppendSourceConnector : IConnector
{
    Task<IRunningSource> StartAsync<T>(SourceContext<T> context, CancellationToken ct) where T : notnull;
}
public interface IKeyedSourceConnector : IConnector
{
    Task<IRunningSource> StartAsync<TKey, TValue>(KeyedSourceContext<TKey, TValue> context, CancellationToken ct)
        where TKey : notnull where TValue : notnull;
}
public interface IRunningSource : IAsyncDisposable
{
    Task Completion { get; }                                  // faults when the source fails
    SourceStats Stats { get; }                                // records read, unreadable, positions, lag, paused since
}
public interface IUpsertSinkConnector : IConnector
{
    IUpsertSink<TKey, TRow> Create<TKey, TRow>(SinkContext context) where TKey : notnull;
}
public interface IAppendSinkConnector : IConnector
{
    IAppendSink<T> Create<T>(SinkContext context);
}
public interface IResettableSinkConnector                     // optional
{
    Task ResetAsync(SinkContext context, CancellationToken ct);   // forget everything this slot wrote
}
```

The dry run refuses a slot bound to a connector that does not implement the slot's kind,
with both named: a Debezium topic bound to a plain `Input` cannot express an update, and
the `kafka` connector's `Describe` refuses `"format": "debezium"` on an append slot.

A context carries the started circuit, the input stream or handle, the config, the
resolved connection, the pipeline id, a per-generation `JsonSerializerOptions`, a logger
and, for sinks, `Func<long> Term` and `long? CommittedStep`. Built in:

- `kafka` — a source for both input kinds with a JSON or Debezium decoder;
  `KafkaUpsertSink`/`KafkaAppendSink`, with `"producer": "log"` selecting the
  transactional producer.
- `file` — over a named object store. As a source, a directory as a log, per
  durability-and-standby.md: partition 0, offset = file ordinal × 2³² + record index,
  where the ordinal is the trailing number in the file name. As an append sink, one
  `{step}.jsonl` per batch by write-then-rename. Under auto-stepping a replay can cut
  different steps than the first run did, so a redelivered step overwriting itself is
  not enough: when a generation starts, the sink deletes every `{step}.jsonl` above
  `CommittedStep` before it accepts a batch, which is the remedy that document names.
- `memory` — in-process handles, for tests, the test harness and the example's generator.

Every sink the supervisor creates is wrapped in `leadership.Leased`.

### The supervisor

```csharp
public interface IPipelineSupervisor
{
    IReadOnlyList<PipelineStatus> Pipelines { get; }
    PipelineStatus Get(string id);
    Task<DeployResult> DeployAsync(PipelineSpec spec, DeployOptions options, CancellationToken ct);  // new, or replace
    Task<ActionPlan> StartAsync(string id, ActionOptions options, CancellationToken ct);     // desired = Running
    Task<ActionPlan> StopAsync(string id, ActionOptions options, CancellationToken ct);      // desired = Stopped
    Task<ActionPlan> RestartAsync(string id, ActionOptions options, CancellationToken ct);   // also clears Failed
    Task<ActionPlan> ResetAsync(string id, ActionOptions options, CancellationToken ct);     // new incarnation, then start
    Task<ActionPlan> RemoveAsync(string id, ActionOptions options, CancellationToken ct);
    IReadOnlyList<DeadLetterRecord> DeadLetters(string id, long afterSequence, int limit);
}
public enum StateChoice { Unspecified, Keep, Reset }
public sealed record DeployOptions(StateChoice State = StateChoice.Unspecified, long? ExpectedGeneration = null, bool DryRun = false);
public sealed record ActionOptions(long? ExpectedGeneration = null, bool DryRun = false, string? Caller = null);
public enum PipelineState { Built, Starting, Running, Stopping, Stopped, Backoff, Blocked, Failed, NeedsReset, Unbuilt, Wedged }
```

`Built` is a standby's state: circuit constructed, not started. `Unbuilt` means this node
holds the pipeline's state as bytes and has no circuit: a plugin or contract is missing,
the definition throws, or a replace is not yet confirmed; it is built at takeover.
`Blocked` is waiting for a table that is not open. `Wedged` is below.

**An `ActionPlan`** is what an action will do, computed from the catalog, the shape and
the stats; with `DryRun` nothing else happens. It lists: state kept or dropped; each
source and where it will resume or restart from; append sinks that will receive history
again; upsert sinks that will keep rows the new run does not produce; derived tables
emptied and the pipelines that read them; table floors that move and by how much;
retention time left for a pipeline being stopped; plugin versions no longer used. The
same record is returned by the real action, so the audit entry holds what was done.

**Bounded operations.** Every operation on one pipeline runs on that pipeline's own
queue, one at a time; pipelines do not wait for each other. `PipelineStatus` shows the
operation at the head of the queue and since when.

- A node-wide gate is taken exclusively by role changes and table declarations, and
  shared by pipeline operations **only around their short sections** — a catalog commit,
  a build, a `Start()` — never across a drain or a backoff wait.
- A teardown has `StopTimeout` (20 s) for the drain. After it every acknowledged output
  is `Abandon()`ed, so the final commit is skipped; the circuit then has `AbandonTimeout`
  (5 s) to finish disposing. A circuit still inside a step after that — a delegate that
  loops, a table commit stalled behind a reader — makes the generation **`Wedged`**: the
  supervisor stops waiting, releases the queue, counts it and shows "wedged: restart
  this node to reclaim". The provider's stores still belong to that circuit, so no new
  generation is built for that id on this node; the id is otherwise operable (remove,
  change desired state) and the next leader runs it. A wedged pipeline is a `Degraded`
  health reason.
- Nothing a wedged circuit does later can hurt: on a node that has resigned or been
  fenced its commits fail, and on a node still leading it is the only generation of that
  pipeline.

**A generation** is one circuit and what hangs off it. The orders are the ones the two
design docs fix:

```
start   tables are open (the registry's job, earlier); a table that is not → Blocked
        check:   provider has never committed, or provider.Layout == spec.layout   — else state-mismatch
        build:   provider.UseSerializer(new per-generation serializer)             (C5)
                 new CircuitBuilder(options with Storage = storage.Pipeline(id)); definition.Build; Circuit.Build()
        circuit.Start()                         — refusal → state-mismatch (core change C7)
        deliveries: handle.DeliverToAsync(leased sink) per sink slot; the dead-letter reader
        sources:    connector.StartAsync per input slot   — after Start(): they read CommittedOffsets
        Running; the leader writes confirmed = generation if it is not already

stop    dispose sources                          — before the circuit
        circuit.DisposeAsync()                   — drains while deliveries still read; final commit waits for acks
          … StopTimeout, then Abandon(), then AbandonTimeout, then Wedged
        await deliveries and the dead-letter reader; dispose sinks
```

The previous generation is always disposed before the next is built: `Build()` claims
derived tables (shared-tables.md, build order 6), and the provider hands the same stores
to the next circuit.

**Faults.** The supervisor watches `circuit.Completion`, every source's `Completion` and
every delivery task. Any of them failing is a pipeline fault: a faulted source is treated
as a faulted circuit, as durability-and-standby.md asks. The generation is torn down in
stop order (nothing is drained; the circuit is already dead). Then:

- if the storage or a table log is fenced, nothing: the node is ending;
- otherwise wait the backoff (doubling from `initialBackoff` to `maxBackoff`) and build a
  new generation on the same provider — `Recover()` undoes what was not committed and the
  sources resume from the committed offsets;
- more than `maxRestarts` faults `within` the window: `Failed`, which is terminal until an
  operator calls `restart`.

`Failed` is this node's knowledge, not the catalog's: a new leader tries again. Each
fault is kept — exception type, message, stack, time, generation — in a list of the last
16 per pipeline, and logged.

**Why a running pipeline is not moving.** `PipelineStatus.BlockedOn` is computed once a
second, with the time it began:

| Value | From |
|---|---|
| `backoff until T` | the supervisor |
| `table:{name}` | the table is `Faulted`, `Failed` or not open |
| `sink-ack:{slot}` | the delivery wrapper has a `WriteAsync` in flight for longer than `StallAfter` (10 s); carries the attempt count and last exception from `SinkDeliveryOptions.OnFailedAttempt` |
| `lease` | a leased sink is waiting for the lease |
| `source-paused:{slot}` | `SourceStats.PausedSince` |
| `no-progress` | an input or exchange queue is not empty and no step has finished for `StallAfter` |

`no-progress` is as far as the core's meters let the service see: a delegate looping
inside a step and a `ToTable` commit waiting for tail room both look like this, and the
status says so. Telling them apart needs a core gauge and is not built.

**Dead letters and unreadable input.** With `errorPolicy: DeadLetter` anywhere in the
circuit the dead-letter output must be read or the circuit stalls. The supervisor always
reads it, into a ring of the last `DeadLetterBuffer` (256) records per pipeline —
operator, weight, time observed, exception type and message, the record as JSON cut at
4 KB — and a counter. Records a source could not decode never reach the circuit: the
built-in sources count them (`SourceStats.Unreadable`, from `KafkaSource.RecordsUnreadable`)
and put them in the same ring as operator `input[{slot}]` with topic, partition and
offset. A `deadLetters` sink slot in the spec also appends all of these to a connector,
best effort: the core does not acknowledge that output. The ring is per node and is lost
on failover; **the `deadLetters` slot is the only durable record**, and the example
configures it.

#### State across a change

A pipeline's state is JSON keyed by store name. Three things are true of it that a
runtime-deployed service has to respect:

1. *The state manifest does not know what the stores mean.* It covers format, workers,
   table epochs and store names, and accepts new stores (`Circuit.cs`,
   `BuildStateManifest`, `ValidateStateManifest`). A different filter threshold, grouping
   key or result selector resumes on the old state with no refusal.
2. *The serializer is lenient.* Unknown members are skipped and missing ones defaulted
   (`Serialization.cs`), so state written by one version's types usually reads into
   another's without error, and writing it back drops what the second did not model.
3. *The provider is typed.* It refuses a store "already created with different key/value
   types" (`ChangelogStorageProvider.cs`, `GetOrCreate`), and a new plugin version always
   has new `Type` objects, even when its source is identical.

So:

- **Keeping state across a change is never implicit.** A deploy whose plugin version,
  definition, non-neutral settings, input or table bindings or worker count differ from
  what is running is refused with 409 `state-choice-required` unless it says `Keep` or
  `Reset`. The response says what differs: the settings that changed, whether the
  data-shape fingerprints differ, the store names added and orphaned. Only a deploy that
  changes none of those (sink bindings, restart policy, `[StateNeutral]` settings,
  desired state) keeps state without being asked. The fingerprint is extra information
  for the person choosing, not permission.
- **The state says which spec made it.** The commit record carries `layout`
  (C1). A generation is started only on state of its own layout, or on none. That is what
  catches a crash in the middle of a replace, and a spec edited behind the service's back.
- **A type change is ordered inside the state log.** When the leader releases a
  pipeline's typed stores for a new layout it writes a *layout marker* — the pipeline's
  commit record again, with the new layout — as its own transaction, before anything of
  the new generation. A follower reading the marker turns that pipeline's typed stores
  back into bytes at that point in partition order, so a record written by layout N+1 is
  never applied to stores of layout N. It then holds bytes until the catalog shows that
  generation `confirmed`.
- **Bytes are never round-tripped through types that have not committed.** When a store
  is created from held bytes, the provider keeps those bytes until the first commit under
  the new layout; releasing before then gives them back untouched. So a `Start()` that
  refuses, or a build that cannot read the state, leaves exactly what the old version
  wrote.

What this does not preserve: members that a version which *did* run and commit does not
model. Version B running over A's state rewrites the entries it touches without them,
and holds the rest without them in memory. A dropped member is gone; `Keep` asserts the
change is compatible and that is the caller's assertion to make.

**Replace** (`DeployAsync` for an id that exists):

1. Refuse a stale `ExpectedGeneration` (412). Decide whether the change touches the
   layout; if it does and no `StateChoice` was given, refuse with
   `state-choice-required`. Nothing touched.
2. Dry-run the new spec. With `Keep`, compare its store names with the running shape's:
   a committed store with no home in the new circuit is `state-incompatible`, refused
   here, with the names. Nothing touched. (With `DryRun` the plan is returned and it
   ends.)
3. Commit the new spec: `generation + 1`, `layout` as decided, `previous` filled in,
   `pending: "reset"` if `Reset`. From here a successor knows everything.
4. Stop the running generation. `provider.ReleaseStores(newLayout)` (C1): typed stores
   become bytes and the layout marker is written.
5. With `Reset`: `provider.StartNewIncarnationAsync()`, call `ResetAsync` on sink
   connectors that have it, then clear `pending`.
6. Build and start. On success write `confirmed` and drop `previous`.
7. If the build cannot read the state or `Start()` refuses, and the choice was `Keep`:
   release (the old bytes come back untouched), commit `previous` as the spec under
   `generation + 2` with its own layout, write that marker, build, start, and report
   `state-incompatible` with the reason. Step 2 makes this rare; it is the backstop for
   what a store-name comparison cannot see (a value that will not deserialize).

A leader that dies anywhere after step 3 leaves an entry its successor can act on:
`pending: "reset"` is finished before the pipeline is built; an unconfirmed generation
is tried, and if its state does not fit, `previous` is restored exactly as in step 7.

Other pipelines run throughout. The old plugin version is unloaded when nothing uses it.

**Reset** is a new incarnation: state gone, input offsets gone. The order is intent
first — commit `pending: "reset"`; stop; `StartNewIncarnationAsync()`; sink resets;
clear `pending` and bump `layout`; start. What it does and does not do, which the plan
spells out per slot:

- sources start again from their `startFrom`;
- append sinks receive the history again (the `file` sink is cleared by its reset);
- **upsert sinks do not converge by themselves.** The delivery keeps no state, so a key
  the old run wrote and the new run never produces — a tightened filter, source records
  aged out of the topic, `startFrom: latest` — stays in the sink. A connector that can
  clear what it wrote implements `IResettableSinkConnector`; the Kafka sink cannot
  enumerate its keys and does not. Otherwise clearing the target is the operator's job
  and the plan says so;
- a derived table loses every row the new circuit does not assert in its first step,
  under the pipelines that read it.

**Remove.** Commit `desired: "Removed"`, `pending: "remove"`; stop;
`StartNewIncarnationAsync()` to tombstone the state (one empty commit record of about 100
bytes stays in the changelog — deleting a pipeline outright is not built in the core);
delete the catalog entry. A leader that finds `pending: "remove"` finishes it and never
starts the pipeline. Tables it read lose a reader, so the floor may rise. A derived
table it wrote keeps its rows and has no writer until another pipeline is deployed to
write it or the table is removed; rows already upserted downstream stay there. The plan
lists both.

**On a standby** the supervisor builds each pipeline whose `desired` is `Running`, whose
generation is `confirmed` and whose provider holds that layout, and does not start it;
the storage follower fills its stores. A layout marker turns the stores back to bytes
(the built circuit is then stale and is disposed); `pending` of either kind disposes it
at once. When `confirmed` catches up the standby builds again. Between the marker and
the confirmation the pipeline is `Unbuilt` there, and a takeover in that window builds
it from bytes — the one case where a takeover pays for decoding state.

> **Default (1): warm circuits on the standby.** It costs the ordering rules above. The
> alternative is under *Open for James*.

### Testing a definition

```csharp
await using var h = await PipelineTestHarness.StartAsync<TradeEnrichment, TradeEnrichment.Options>(
    new() { MinNotional = 1000 },
    tables => tables.Add(Tables.Instruments, instrumentRows));

h.Input<Trade>("trades").Send(new Trade(1, 7, 5000m));
await h.StepAsync();
var rows = h.UpsertSink<long, EnrichedTrade>("enriched").Rows;      // current rows by key
h.Table(Tables.Instruments).Upsert(renamed); await h.CommitTablesAsync();
h.DeadLetters; h.Shape;
```

The harness is in `Incremental.Service`, runs one definition on an in-memory provider
with the `memory` connector on every slot, and is built on the same dry run and
generation code the supervisor uses, so a definition that passes it binds the way it
will when deployed. `RestartAsync()` builds a new generation on the same state, which is
how an author checks that state survives.

## Node

```csharp
public sealed class ServiceNode : IHostedService, IAsyncDisposable
{
    public NodeStatus Status { get; }                        // leadership + catalog version + counts by pipeline state + readiness
    public NodeHealth Health { get; }                        // Healthy | Degraded | Unhealthy, with reasons
    public event Action<ServiceEvent>? Events;               // role, pipeline, table, catalog — feeds SSE
    public IReadOnlyList<ServiceEvent> RecentEvents(long afterSequence);
    public ITableRegistry Tables { get; }
    public IPipelineSupervisor Pipelines { get; }
    public IPluginCatalog Plugins { get; }
    public Task<ActionPlan> TakeOverAsync(NodeActionOptions options, CancellationToken ct);
    public Task<ActionPlan> StepDownAsync(NodeActionOptions options, CancellationToken ct);
}
public sealed record NodeActionOptions(bool Force = false, bool DryRun = false, string? Caller = null);
```

**Start** (every node, whatever it becomes): connect the backend; load built-in
contracts; `ChangelogStorage.Follow(backend.State)` and `Track` it; begin opening the
catalog in the background; `leadership.StartAsync(callbacks)`. Contracts stored in the
catalog are loaded when it is first read, before any table is opened. `StartAsync`
then returns, so the host and the admin API come up while the node is still a standby.
On a deployment's very first start the catalog's log does not exist until some node is
the writer, so that open completes inside the first takeover — the case `PrepareAsync`
exists for.

**As a standby** the node reconciles on each catalog version: follow new tables,
materialise plugins, build confirmed pipelines. `NodeStatus.Standby` says how ready it
is, in terms an operator can act on:

```csharp
public sealed record StandbyReadiness(
    long CatalogVersionApplied, long CatalogVersionLatest,
    TimeSpan LagAge,                         // now − the leader's time on the newest state commit and table version applied
    long StateLagRecords, int TablesBehind, int TablesFaulted,
    int PipelinesBuilt, int PipelinesUnbuilt, IReadOnlyList<string> UnbuiltReasons,
    int PluginsPending,
    bool TakeoverWouldBeWarm, IReadOnlyList<string> NotWarmBecause);
```

`TakeoverWouldBeWarm` is: catalog applied, no follower faulted, every table within
`ReadyLag` of its log's end, state lag under `ReadyLag`, nothing `Unbuilt`. It is what
the node answers a peer's probe with (`PeerInfo.Ready`), what the leader shows as "peer",
and — when no standby answers at all — a `Degraded` reason on the leader, so the absence
of a standby is something seen rather than something not there.

**Takeover.**

| # | Who | What |
|---|---|---|
| 1 | leadership | acquire the writer: fences the old leader, commits the term |
| 2 | `PrepareAsync` | the catalog: await its open, reopen it if its follower faulted, `PromoteAsync()`. Then reconcile and, concurrently and each in its own try block: promote every followed table; open as the writer every table the catalog names that is not open |
| 3 | leadership | promote the storage |
| 4 | `StartLeadingAsync` | write `node/leader`; apply seeds; finish every `pending` reset and removal; start every pipeline whose `desired` is `Running` (`Start()`, deliveries), in parallel; then sources and table loaders, in the background; start the trim loop; import `PluginDirectory` |

That is writer, catalog, tables, storage, circuits, sources. **Only the catalog (step 2's
first sentence) and the storage (step 3) can throw**, and that ends the node `Fenced`.
A table that fails is `Faulted` and its pipelines `Blocked`; a pipeline that fails to
start is `Backoff`, `Failed`, `Unbuilt` or `NeedsReset`; everything in step 4 after
`node/leader` is caught per item. The background work of step 4 is not awaited by the
callback, so the node is `Leader` as soon as the circuits have started.

**Fenced and Resigned are final.** On `leadership.WhenFinished` the node abandons every
circuit without draining (their commits can only fail), sets `Environment.ExitCode` to 3
(fenced) or 4 (resigned) and calls `IHostApplicationLifetime.StopApplication()`. The
process supervisor restarts it and it comes back as a standby.

**When it cannot lead.** Heartbeats come from the log writer, not the circuits, so a
leader that cannot do its job still looks alive to its peer. There is exactly one such
condition the service acts on: the catalog's writer has faulted and the node is not
fenced, so it can record nothing. It reopens the catalog once, in place. If that fails
it steps down when the peer answers as a ready standby, and otherwise exits with code 5
so the process supervisor restarts it. Either way a node that can write the catalog
ends up leading.

Pipelines failing never move leadership. A pipeline is tenant code; one deterministic
bug in the only deployed pipeline would otherwise hand leadership over and exit a
process, and fail the same way on the other node. Every desired-running pipeline being
`Failed` is `Unhealthy` in health, logged and shown; an operator who believes the node
is at fault steps it down by hand.

**Manual step-down and takeover** are checked before they act, and say what they will do.

- `StepDownAsync` on the leader ends in `Resigned`: this process exits with code 4. It is
  refused with 409 unless the peer's probe answers as a standby with `Ready`; the refusal
  carries the peer's answer or "no standby answered". `Force` overrides. On a lone
  in-memory node there is no peer, so it is refused unless forced, and forced it is what
  it says: the service stops.
- `TakeOverAsync` on a standby fences a healthy leader. It is refused with 409 unless
  `TakeoverWouldBeWarm`; the refusal carries the readiness record. `Force` overrides.
- Both return, and with `DryRun` only return, an `ActionPlan`: "node a will resign and
  exit with code 4; node b takes over; pipelines stop for about N s", N being the last
  takeover's measured total when there is one.
- A step-down has `StepDownTimeout` (30 s) for `StopLeadingAsync`. Pipelines not stopped
  by then are abandoned or wedged as above and the resignation is committed regardless:
  the successor's first act fences this node, so nothing it still runs can commit.

**Graceful shutdown.** The generic host stops services in reverse order, so the web
server stops first; the admin package ends every SSE response on
`IHostApplicationLifetime.ApplicationStopping`, or Kestrel would wait on an open event
stream until the timeout (`examples/Incremental.Showcase/Program.cs` documents the
trap). Then, if and only if the node's role is `Leader` (a fenced node's `StopAsync`
also runs, and `StepDownAsync` throws there) and the peer answers as a standby, it steps
down: `StopLeadingAsync` stops loaders and sources and every pipeline in stop order and
in parallel, with a stop budget of 60% of what remains of the host's shutdown time
rather than a fixed figure, and the leadership commits a resignation so the standby
takes over without waiting out `TakeoverAfter`. With no standby answering it stops the
pipelines the same way and does not resign. Then tables, storage, leadership and
backend are disposed. What has not drained is abandoned and replayed by the successor.
The README says to raise `HostOptions.ShutdownTimeout` above its 30 s default when
sinks are slow; `AddIncrementalService` logs a warning at start when it is below
`StopTimeout + AbandonTimeout + 5 s`.

**Health.** `NodeHealth` is computed from the status, never from whether pipelines exist:

| | Leader | Standby |
|---|---|---|
| *live* | the process answers and the backend connection has not failed for `LiveAfter` (30 s) | the same |
| *ready* | leading and the catalog reconciled — true with no pipelines, and with every pipeline stopped on purpose | following the catalog |
| `Degraded` | any pipeline `Failed`, `NeedsReset`, `Wedged` or `Blocked`; a loader faulted or a table `Faulted`; a reader past 80% of its retention; no ready standby | not `TakeoverWouldBeWarm`, with the reasons |
| `Unhealthy` | not ready; or every desired-running pipeline `Failed` | not ready; a follower faulted and not yet reopened |

**Events, audit and logs.** What an operator sees must outlive a page reload, and the
important part of it must outlive a failover.

- *Events.* The node keeps the last `EventBuffer` (500) `ServiceEvent`s with a sequence
  number. A new SSE client is replayed from `Last-Event-ID`; `GET /events/recent` returns
  the ring. This is per node and in memory.
- *Audit.* Every mutation — admin API or `ServiceNode` call — writes `audit/{seq}` in the
  same catalog version as the change it describes: time, node, term, caller
  (`HttpContext.User`'s name, or "local"), action, target, generation before and after,
  options, and the plan. The newest 1,000 are kept. This survives failover and is what
  answers "who reset this".
- *Logs.* Through `ILogger`, with pipeline id, generation, table, term and node as
  structured fields: every role change (Information), pipeline state transition
  (Information), pipeline, loader, table and follower fault (Error, with the exception),
  cut-loose and wedged (Error), every mutation with its caller (Information), and dead
  letters at no more than one line per pipeline per 10 s with a count (Warning).
- *Not kept across failover:* the dead-letter ring, fault lists, `Failed`, and the
  metrics rings. A new leader starts those empty. The durable forms are the `deadLetters`
  slot, the logs and a metrics collector on the `Incremental.Service` meter.

**DI.**

```csharp
builder.Services.AddIncrementalService(o =>
    {
        o.PluginDirectory = "plugins"; o.CacheDirectory = "cache";
        o.AdvertisedAdminUrl = "http://node-a:5180/admin";
    })
    .UseInMemoryBackend()                                   // the default
    // .UseKafkaBackend(k => k.Log = new KafkaLogOptions { … })
    .AddKafkaConnection("msk-main", sp => …)
    .AddObjectStore("refdata", sp => …)
    .AddContracts(typeof(Instrument).Assembly)
    .AddPlugin(typeof(TradeEnrichment).Assembly)
    .AddTable(Tables.Instruments, loader: null)             // seeds
    .AddPipeline(spec)
    .AddTableSource("refdata-grpc", sp => new RefDataSourceFactory(…))
    .AddSourceConnector("my-source", sp => …)
    .AddSinkConnector("my-db", sp => …);
```

`AddIncrementalService` registers `ServiceNode` as a singleton and as a hosted service;
`ITableRegistry`, `IPipelineSupervisor`, `IPluginCatalog` and `ServiceMetrics` resolve
from it. `ServiceOptions` binds from configuration. `AdvertisedAdminUrl` is what
`node/leader` publishes; `Incremental.Service` knows nothing about HTTP, so the host sets
it, and the admin package fills it from `IServerAddressesFeature` when it is unset and
exactly one address is bound.

## Admin API and UI

```csharp
builder.Services.AddIncrementalServiceAdmin(o => { o.ExposeRowData = true; });
app.MapIncrementalServiceAdmin("/admin");                  // UI at /admin, API at /admin/api
app.MapIncrementalServiceHealth("/health");                // /health/live, /health/ready, /health/status
```

**Security posture.**

```csharp
public sealed class ServiceAdminOptions
{
    public string? AuthorizationPolicy { get; set; }           // every endpoint
    public string? PipelinePolicy { get; set; }                // pipeline mutations     ┐
    public string? TablePolicy { get; set; }                   // table mutations        │ each defaults to
    public string? NodePolicy { get; set; }                    // takeover, step-down    │ AuthorizationPolicy
    public string? PluginPolicy { get; set; }                  // upload, rescan, delete ┘
    public bool ReadOnly { get; set; }                         // mutating endpoints answer 403
    public bool ExposeRowData { get; set; }                    // rows, dead-letter records, exception messages
    public bool AllowPluginUpload { get; set; }
    public bool AllowPluginRescan { get; set; }
    public int MaxRows { get; set; } = 200;
    public int MaxScannedRows { get; set; } = 100_000;
    public long MaxUploadBytes { get; set; } = 64 << 20;
    public IList<string> AllowedHosts { get; } = [];
}
```

- **Loopback unless told otherwise.** The package cannot choose what Kestrel binds, so it
  enforces the rule itself: with no `AuthorizationPolicy`, a request gets 403 if its
  remote address is not loopback, if its `Host` is not a loopback name or in
  `AllowedHosts`, or if it carries `Forwarded`, `X-Forwarded-For` or `X-Forwarded-Host` —
  the last because behind a reverse proxy on the same machine every request arrives from
  loopback. Setting a policy lifts these checks and puts `RequireAuthorization` on the
  group. `AllowedHosts` without a policy means **unauthenticated access for whoever can
  reach that name**; the option's documentation says so in those words, and the README's
  two-node section shows a minimal policy, which any cross-host use needs.
- **Mutations** need `Content-Type: application/json` (a zip for an upload) and the header
  `X-Incremental-Admin: 1`. A browser will not send that cross-site without a preflight
  the API never answers, which is what stops a page on the operator's machine from
  posting to loopback.
- **Mutations are conditional and confirmed.** A pipeline mutation carries
  `If-Match: "<generation>"` and gets 412 if the entry has moved. Reset, remove, table
  delete, plugin delete, loader restart, step-down and takeover need `confirm=<the id or
  name>`. Every mutation takes `dryRun=true` and returns the `ActionPlan` without acting;
  the UI's dialog shows that plan and makes the operator type the id.
- **Row data is off by default** (*Open for James*, 4). With it off the row endpoints
  answer 403, and dead letters, unreadable records and faults come back with the
  exception *type* only: `FormatException`, `JsonException` and exceptions thrown by
  plugin code quote the offending value in their messages, so the message is row data.
- **Specs are redacted, not hidden.** Connector properties marked secret are returned as
  `"***"`. With named connections there should be none; this is for custom connectors.
- **Loading code is two switches.** `AllowPluginUpload` for the zip endpoint,
  `AllowPluginRescan` for importing from the plugin directory on request. Both are off
  by default and both use `PluginPolicy`. The import at first lead is the host's own
  configuration and is not affected.
- **Mutations go to the leader.** A standby answers 409 with the leader's node id and
  advertised URL, except `takeover`.

The health endpoints are mapped separately, outside the admin group and its loopback
rule, so a kubelet or load balancer can reach them; they return status and reason codes,
never ids or messages. `/health/live` and `/health/ready` answer 200 or 503;
`/health/status` answers 200 with `Healthy | Degraded | Unhealthy` and reasons. The
package also registers an `IHealthCheck` for hosts that publish health their own way.

**Endpoints** (under `/admin/api`; `M` = mutating, all `M` take `dryRun`):

| | Path | |
|---|---|---|
| GET | `/node` | role, term, leader and its term and URL, Kafka detail (heartbeat age, lease, last takeover's timings) when there is one, standby readiness, peer's last probe, loaded contract versions, backend info, catalog version, health |
| POST M | `/node/takeover?force=`, `/node/stepdown?force=` | on the standby; on the leader |
| GET | `/audit?after=&limit=` | the audit entries, from the catalog; any node |
| GET | `/tables`, `/tables/{name}` | `TableStats`; detail adds the spec and the unreadable ring |
| GET | `/tables/{name}/rows?limit=&skip=&contains=` | up to `MaxRows` rows from one scan of a pinned snapshot that stops after `MaxScannedRows`; `contains` filters on the row's JSON text |
| GET | `/tables/{name}/rows?key=` | one row by primary key (the key as JSON) |
| GET | `/tables/{name}/rows?index=&key=` | the rows under one index key (C4), at most `MaxRows` |
| POST M | `/tables`, `/tables/{name}/loader/restart`, `/tables/{name}/reopen` | declare; restart a loader; reopen a faulted table |
| DELETE M | `/tables/{name}` | |
| GET | `/pipelines`, `/pipelines/{id}` | status with `blockedOn` and the running operation; detail adds spec, shape, incarnation, layout, commit sequence and term, committed input offsets, each table's committed version beside the table's current one, the last 16 faults |
| GET | `/pipelines/{id}/graph` | nodes and edges from the last dry run's shape (C3): there for any pipeline that has ever validated |
| POST | `/pipelines/validate` | dry run of a spec that is not deployed: shape, graph, warnings |
| GET | `/pipelines/{id}/metrics` | 60 one-second and 60 one-minute samples (below) |
| GET | `/pipelines/{id}/dead-letters?after=&limit=` | the ring, newest last, at most `MaxRows` |
| PUT M | `/pipelines/{id}?state=keep\|reset` | deploy or replace; 409 `state-choice-required` / `state-incompatible`, 412, 413 |
| POST M | `/pipelines/{id}/start`, `/stop`, `/restart`, `/reset` | |
| DELETE M | `/pipelines/{id}` | |
| GET | `/plugins`, `/plugins/{name}` | versions, kind, definitions with their settings schema, pipelines using each, load state here and on the leader, live contexts |
| POST M | `/plugins?acceptSchemaChange=`, `/plugins/rescan` | zip upload; import from `PluginDirectory` |
| DELETE M | `/plugins/{name}/{version}` | |
| GET | `/events`, `/events/recent?after=` | server-sent events with ids: `node`, `pipeline`, `table`, `catalog`, `deadletter` on change; `metrics` once a second |

**Row reads are served by any node from its own copy.** A standby follows every table,
so it answers too. Every row response carries `version` (the snapshot read), `tableRows`,
`scanned`, `truncated` and, on a standby, `lagVersions`, so "not found" can be told from
"not reached" and from "not arrived here yet". The pipeline page shows, for each table a
pipeline reads, the version it has committed beside the table's current one: "the row is
right and the pipeline has not seen it" is then visible.

Every list is bounded by `MaxRows` or by what is deployed. Each SSE connection has a
bounded queue of 64 events that drops the oldest: a slow browser loses ticks, not the
server's memory.

**Where the numbers come from.** `ServiceMetrics` (in `Incremental.Service`, so a host
without the admin package has it too) attaches one `MeterListener` to the `Incremental`
meter (`src/Incremental/Diagnostics/CircuitDiagnostics.cs`) and keeps, per `circuit` tag,
two fixed rings: 60 one-second slots and 60 one-minute slots, so the last hour is there
for someone paged late. Anything longer is a collector's job.

| Shown | Source |
|---|---|
| steps, step latency | `incremental.circuit.steps`, `…step_duration` (count, sum, max per slot) |
| records in | `…records`; per source: the connector's `SourceStats` |
| records out | counted per batch in the supervisor's delivery wrapper (upserts, deletes, appends) |
| sink-ack wait | `…sink_ack_wait` |
| checkpoint age | time since the last `…checkpoint_commits` measurement |
| queue depths, watermark lag | the observable gauges, polled once a second |
| dead letters by operator | `…dead_letters` |
| input lag | `KafkaSource.MeasureLagAsync`, sampled every 5 s |
| committed table versions | `ChangelogStorageProvider.CommittedOffsets` (C2) |
| node, standby lag | `NodeLeadershipStatus`, `StandbyReadiness` |
| log writer | `KafkaLog.TransactionsCommitted`, `…Aborted`, `CommitsCompleted`, `RecordsWritten`, `IdleHeartbeats` |

There are no per-operator record counts: the core does not count them, and adding a
counter to every operator's loop is a hot-path cost this design does not take. The graph
is the circuit's static shape with dead-letter counts on the operators that have them.
The service publishes its own meter, `Incremental.Service`, for collectors: pipeline state
and restarts, sink and source records, table rows, version and loader lag, node role,
standby lag, live plugin contexts, and the log writer's counters above.

**UI.** Hand-written `index.html`, `app.css`, `app.js`, embedded as resources and served
by a small endpoint (no file-provider package, no build step), in the manner of
`examples/Incremental.Showcase/wwwroot/index.html`. Hash-routed views: overview (node
banner with health and reasons, counts, event feed), tables (list, detail with stats,
loader, readers with time until cut loose, row sample, key and index lookup), pipelines
(list with sparklines and `blockedOn`; detail with state, actions, the graph as inline
SVG, metrics, faults, dead letters, spec; deploy with a settings form from the schema),
plugins (versions, definitions, contract roll state, upload), node (leadership, readiness,
peer, takeover and step-down), audit. One `EventSource` drives all of it. Every
destructive control opens a dialog that fetches the dry-run plan, shows it, and asks for
the id. On a standby the mutating controls are replaced by a link to the leader.

## Changes this needs elsewhere

Each is the smallest change found; each is to be recorded in the owning design doc when
built. C1 is the large one and is built and tested first.

| | Where | Change | Why |
|---|---|---|---|
| C1 | `ChangelogStorageProvider` | (a) `ReleaseStores(long? layout = null)`: with no circuit running, turn each typed store back into held bytes and forget it; a store that has not committed since it was created from held bytes gives those bytes back untouched (they are kept until then). On a leader with `layout` set, write the commit record again carrying the new layout, as its own transaction. (b) The commit record gains `layout` (absent = 0; a JSON member, so old records read); `Layout` is a public property. (c) A follower reading a commit record whose layout differs from its stores' releases them at that point. (d) A follower that cannot apply a record to a typed store releases that pipeline's stores instead of failing the storage. | A new plugin version has new `Type` objects; state must not cross versions through a lenient serializer; contexts cannot unload while the provider holds their stores; and today "one bad pipeline blocks a takeover" (durability-and-standby.md, *Other departures and limits*). The commit record is a persisted format: adding a member needs that document's note, not a format bump, provided readers ignore unknown members — to be checked in `ReadCommit`. |
| C2 | `ChangelogStorageProvider` | `IReadOnlyDictionary<string, long> CommittedOffsets`, readable in any role | retention floor for pipelines with no circuit; a standby's pipeline page |
| C3 | `CircuitBuilder` | `Describe(CircuitOptions)`: nodes (id, kind, name, inputs, exchange, state id), edges, and the store names `Build()` would mint, without claiming tables or touching storage. `Circuit.Describe()` returns the same. | the graph for a pipeline with no circuit; the pre-stop state check; unnamed-store warnings |
| C4 | `TableSnapshot<TKey, TRow>` | `Rows()`: public form of the internal `RowsAt` scan. `RowsBy<TIndexKey>(string index, TIndexKey key)`: public form of `TableIndex.Read(indexKey, version)` | catalog reconcile, row sampling, lookup by business key |
| C5 | `ChangelogStorageProvider` | `UseSerializer(IStateSerializer)`, allowed while the provider holds no typed store | a serializer per generation, so its metadata cache dies with the plugin version |
| C6 | `SharedTable<TKey, TRow>` | `Exception? FollowFault` (public read of the private field) | a dead follower is otherwise invisible until a takeover fails on it |
| C7 | `Circuit.Start()` | throw `CircuitStateMismatchException : InvalidOperationException` for the refusals whose remedy is a fresh state store | telling a state mismatch from a fault |
| K1 | `LeadershipProbe` | the answer line gains a fourth token supplied by the host; `QueryAsync(host, port)` returns the parsed line. `Tcp(...)` reads only the first token today, so old and new interoperate | the leader learning whether a ready standby exists |
| K2 | `KafkaTableLog.AppendAsync` | before producing anything, refuse a version in which one change or one row record exceeds `MaxRecordBytes`, naming the key and the size | a row over the broker's limit otherwise fails as an anonymous client error; the fault is the same, the message is usable |

`KafkaLeadership` itself is not changed: the service does not track tables with it.

## Reference examples

**(a) `examples/Incremental.TradeEnrichment`, moved onto shared tables.** In process, no
docker, no service packages. Instruments, traders, desks and counterparties become
`SharedTable`s; `trader-desks` is a derived table written by a small circuit (`ToTable`);
the trade circuit uses `Join` for the columns that must be corrected retroactively and
`Lookup` for the ones fixed on arrival, chosen at the prompt as columns are today. The
run shows both: a trader moves desk and historical trades re-attribute through the join;
an instrument is renamed and only trades arriving afterwards carry the new name through
the lookup. `--ticks` and `--blotter` keep working for CI. This is shared-tables.md's
step 7 example. It depends on nothing in the service and is built **first**, with S0: it
is the cheapest check that the core's table seams (C4, index reads) are right.

**(b) `examples/Incremental.EnrichmentService`.** Five projects:

- `Contracts` — a contract assembly: `Instrument`, `Counterparty`, `TraderDesk`, their
  `TableDefinition`s (instruments with an `IndexDefinition` on ISIN), a static `Tables`.
- `Pipelines` — a pipeline assembly built into the host's `plugins/v1/` directory:
  `trade-enrichment` (filter, `Join` to instruments, `Lookup` to counterparties, computed
  notional, upsert sink), `desk-exposure` (windowed sum per desk on event time, upsert
  sink), `trader-desks` (`Changes()` of two tables joined and written with `ToTable`).
- `Pipelines.V2` — the same sources compiled with `DefineConstants=V2` into
  `plugins/v2/`: one change that is state-compatible (a new computed column on the sink
  row) and one that is not (the grouping key of `desk-exposure`), so the replace act
  shows `keep` succeeding and `state-choice-required` then `reset`.
- `Pipelines.Tests` — tests of the definitions with `PipelineTestHarness`.
- the host — `Microsoft.NET.Sdk.Web`, `AddIncrementalService`, the admin UI on
  `http://localhost:5180`, a data generator, and **two adapters written the way the
  owner's will be**: `GeneratedRefDataSource : ITableSource` in two forms — one stamps
  positions and resumes from `Table.CommittedOffsets`, one has no positions and calls
  `ReplaceAllAsync` then applies deltas — and `GeneratedTradeSource`, an
  `IAppendSourceConnector` that resumes from the circuit's committed offsets.

Modes:

- **default**: in-memory backend. Counterparties are loaded by the `file` loader from a
  JSON-lines file the generator rewrites now and then (snapshot reload on change);
  instruments by the custom table source; trades by the custom source connector; sinks
  in `memory` with a `file` dead-letter slot. No docker.
- **`--nodes 2`**: still in memory — two `ServiceNode`s on one `InMemoryServiceCluster`
  in one process, two admin ports — so the standby view, takeover and step-down can be
  seen and smoke-tested without a broker.
- **`--kafka`**: the local connector stack. Instruments live in Postgres and arrive
  through a Debezium connector the host registers with `KafkaConnectClient`
  (`debezium` loader); trades come from a topic the generator produces to; sinks are
  Kafka topics. `--node a|b` and `--peer` run two processes on different ports, each
  with its `AdvertisedAdminUrl`.
- **`--headless`**: deploys the pipelines, runs the generator for a fixed number of
  records, checks the sinks against a model computed from the same generator, replaces
  `trade-enrichment` with v2 keeping state and `desk-exposure` with v2 by reset, checks
  again, and with `--nodes 2` takes over and checks a third time; exits non-zero on any
  mismatch. With `--kafka` it also starts a second node as a child process, kills the
  leader, and checks the survivor finishes.

The README walks through two nodes: start `a` and `b`, open both UIs, kill `a`, watch `b`
go from standby to leader with the takeover's timings, restart `a` as the standby. It
says that the two-node walk across machines needs an authorization policy and shows one.

## Tests

Each stage mutation-checks its own list: break the behaviour, see a test fail, revert.

- **S0.** C1 in the core's own tests: release and rebuild with other types; untouched
  bytes after a refused start; a follower releasing at a layout marker and never
  applying a later layout's record to typed stores; a follower detaching a pipeline it
  cannot apply. C5, C6. The unload spike.
- **S1, in memory.** Registry: declare, follow, promote, loader resume from committed
  offsets, `file` loader re-sync, unreadable `fault` and `skip`; retention floor
  including a stopped pipeline, a derived table's writer and the follower margin (a
  standby paused through several commits still follows); a follower past the margin is
  reopened and its pipelines rebuilt; the cap cutting a pipeline loose, with the warning
  before it; a writer fault leaving the node leading and the table reopened. Supervisor:
  deploy while others run; stop drains and commits; fault → backoff → `Failed`; a faulted
  source faults the pipeline; the dead-letter ring keeps a `DeadLetter` circuit moving;
  a wedged delegate ends `Wedged` within the bound and blocks neither another pipeline
  nor a step-down; stale generation → 412; a settings change → `state-choice-required`;
  `[StateNeutral]` change keeps state unasked; `keep` across a compatible version
  continues from state; orphaned store names refused before anything stops; a start
  refusal rolls back onto byte-identical state; reset and remove resumed by a successor
  from `pending`; the layout check refusing state of another layout. Plugins: two
  pipeline assemblies sharing a contract type join one table; duplicate definition names
  and hostile zips refused; settings binding (unknown member, range, required); the
  context cap. Catalog: concurrent actions are whole versions; an oversized value is
  refused and the writer survives. Node: a second in-memory node follows, reports
  readiness, takes over in the stated order and runs the same pipelines to the same
  sinks; the fenced node's catalog write fails; one table that cannot be promoted and one
  pipeline that cannot build do not block the takeover; step-down and takeover refused
  when the peer is not ready and allowed with force; a replace in flight on the leader
  when the standby takes over; health for an empty leader, a degraded one, a standby.
  The test harness, used by these tests.
- **S2, Kafka.** The S1 node tests against the broker; catalog and blob restore by a cold
  node; and two real processes of a test host (`tests/Incremental.Service.FailoverApp`, in
  the manner of `tests/Incremental.Kafka.FailoverApp`) with a Kafka source, a table
  loader, 20 tables, two pipelines and Kafka sinks: kill -9 the leader, the standby leads
  within a bound, sinks equal the model, counts prove nothing was skipped; a pipeline
  deployed just before the kill exists on the survivor; step down hands over without the
  timeout; the peer probe reports readiness.
- **S3.** Each endpoint against an in-memory node, hosted in process on a loopback port;
  the loopback, host, forwarded-header, admin-header, read-only, row-data, upload and
  rescan switches each refuse what they should; `If-Match`, `confirm` and `dryRun`;
  redaction; bounded responses; an SSE client that stops reading does not grow the
  server, and one that is open does not delay shutdown. Then the node view, lag, peer
  and leader-redirect paths against a Kafka-backed pair (skipped without a broker),
  because the in-memory backend has none of those numbers.
- **S4.** Both examples' headless modes, `--nodes 2` included; the example's own test
  project.

## Build order

0. **S0 — core seams, the spike, example (a).** C1 to C7 with their tests in
   `tests/Incremental.Tests`; the unload spike, whose result fixes whether pipeline
   contexts are collectible; example (a). Hands on: the core API the service is written
   against, and a decision on *Open for James* 1 before the standby half of the
   supervisor exists.
1. **S1 — service core on the in-memory backend.** The backend seam (with the service's
   own status types and `ProbePeerAsync`) and `InMemoryServiceBackend`; catalog with its
   write lock and audit; connections and `LocalObjectStore`; plugin catalog and contexts;
   table registry with the `file` and custom loaders; the `memory` and `file`
   connectors; supervisor; `ServiceNode` with health, readiness, the event ring and
   `AdvertisedAdminUrl`; DI; `ServiceMetrics`; `PipelineTestHarness`. Hands on: the seam,
   and a node that passes the in-memory takeover tests.
2. **S2 — Kafka.** `KafkaServiceBackend` with its blob store, the Kafka connection, the
   `kafka` and `debezium` loaders and connectors, K1 and K2, exit codes, the two-process
   failover test with the table-count measurement. Hands on: measured takeover with
   pipelines built on the standby.
3. **S3 — admin.** The API, the security switches, health endpoints, SSE, the UI; then
   its Kafka-backed pass, which needs S2.
4. **S4 — example (b) and docs.** README, `docs/intro.md`, CLAUDE.md.

S3's in-memory part can start once S1's interfaces exist; S2 and S3 do not touch the
same files.

## Out of scope

- A declarative pipeline spec (the hook: a built-in definition).
- Tenants: authentication, per-tenant authorization, quotas, isolation between plugins.
- More than one standby, or priorities among standbys.
- Clustered circuits: they cannot use shared tables or the changelog provider.
- Sandboxing plugins, or stopping a wedged delegate (it is bounded and shown, not
  stopped).
- An S3 `IObjectStore`, a gRPC adapter: the seams are here, the adapters are the owner's.
- Per-operator record counts and latency; telling a looping delegate from a stalled table
  commit.
- Deleting a removed table's topics, or a removed pipeline's last commit record.
- Changing a shared table's row type in place.
- Clearing a Kafka upsert sink at reset.

## Considered and rejected

Proposals from the two reviews that the design does not take, each with its reason.
Everything else they raised is in the text above.

- *Step down on infrastructure-class pipeline failures across N pipelines.* Classifying
  an exception as infrastructure or tenant code is a guess, and a wrong guess moves
  leadership; pipelines never move it, and an operator can.
- *Check serialized row size in the loader before staging.* It serializes every row
  twice on the load path of large tables; the oversized row is instead a table fault
  that names the key (K2) and leaves the node leading.
- *`LeadFailurePolicy` with `Stay` as the default.* With pipeline failures out of it the
  only case left is a dead catalog writer, where staying is never right; the enum is
  removed rather than given a default.
- *A `node/handover` marker keyed by reason and table.* Nothing steps down for a
  table or a pipeline any more, so there is no ping-pong to guard.
- *Blobs in a compacted topic written "in several modest transactions".* Taken, except
  the transactions: content-addressed data needs no fencing, and keeping it off the
  transactional producer is what stops an upload delaying checkpoints and the lease.
- *Cap in-memory history in row changes as well as versions.* It needs a change to
  `InMemoryTableLog`; a low version cap (1,024) bounds the same thing closely enough for
  the mode that promises least.
- *A Kafka sink that writes tombstones for a supplied key set at reset.* The service has
  no key set to supply — the delivery keeps no state — so the hook is the connector's
  optional `ResetAsync` and the plan says what is left behind.
- *Persist dead-letter totals and last fault in the catalog on a slow cadence.* It makes
  failing pipelines write to the one table the node cannot lose; the `deadLetters` slot,
  the audit entries and the logs are the durable record, and the doc says so.
- *Refuse a deploy that needs a contract version the standby has not loaded.* The leader
  cannot see what a standby has loaded beyond the probe's ready flag; the standby shows
  the pipeline `Unbuilt` with the reason and reports itself not warm.
- *Rate-limit takeover.* It is refused unless the standby is ready, needs a confirm
  parameter, and is audited and logged; a rate limit adds a way to be locked out of the
  one action an incident needs.
- *Put the state manifest computation in a static over node definitions.* Store names
  are minted while operators are constructed (`Circuit.cs`, the build), so the honest
  form is a describing run of that construction (C3), not a second implementation of
  the naming.
- *Derive the admin URL only from `IServerAddressesFeature`.* Behind a proxy or with
  several bindings that is the wrong address; the host's `AdvertisedAdminUrl` comes
  first and the feature is the fallback.
- *Split `ExposeSpecs` into its own switch.* Specs hold no secrets once connections are
  named; redaction of declared secret properties covers custom connectors.

## Open for James

Each has a default this design is built on.

1. **Warm circuits on the standby** (default) — the standby loads plugin code and builds
   every confirmed circuit, as durability-and-standby.md specifies. It is what C1's
   layout marker and follower halves exist for. *Alternative:* the standby holds pipeline
   state as bytes and builds at takeover. No plugin code runs on a standby, C1 shrinks to
   release-with-untouched-bytes, and the marker, `confirmed` and the standby rebuild on a
   reopened table all go; the cost is decoding state during every takeover, roughly a
   microsecond or two per entry per pipeline, in parallel across pipelines but not within
   one — seconds for a pipeline holding millions of entries. S0 measures it; decide
   before S1's supervisor.
2. **The catalog is a shared table** (default). *Alternative:* a dedicated compacted
   `{p}.catalog` topic. One topic instead of two; the cost is a new public
   append-and-follow API on `KafkaLog` and a second follower implementation to test for
   fencing and restore.
3. **Plugin binaries in the backend's blob store** (default; on Kafka a topic).
   *Alternative:* store a location (S3, a shared volume) and a hash. No plugin bytes in
   Kafka at all; the cost is a second storage system whose outage blocks a failover that
   needs a plugin the standby has not cached.
4. **Row data, and exception messages, off in the admin API by default.** *Alternative:*
   on by default, which makes the first run friendlier and makes a host that enables
   remote access expose tenant rows unless someone remembers the switch. The example
   turns it on.
5. **Loading code over the API off by default** (upload and rescan), with the plugin
   directory at first lead as the ordinary route. *Alternative:* on whenever a
   `PluginPolicy` is set.
6. **Pipelines failing never move leadership**; only a dead catalog writer does.
   *Alternative:* step down when every desired-running pipeline has failed, which helps
   when the fault is this node's (a bad disk, a bad network path to a sink) and hurts
   when it is the tenant's code.
7. **`Failed` is not durable**: a new leader retries a pipeline the old one gave up on.
   *Alternative:* record it in the catalog, so a deterministic failure is tried once;
   the cost is a catalog write on the failure path and a pipeline that stays down after
   a failover caused by the very node fault that failed it.
8. **Removing a table keeps its topics**, so the name can be declared again with its
   rows. *Alternative:* delete the topics, which needs topic deletion in `KafkaLog` and
   makes a mistaken removal unrecoverable.
9. **Any change that can touch state must say `keep` or `reset`**, including a settings
   edit. *Alternative:* keep silently when only settings change — one request fewer for
   the commonest action, and silently wrong output whenever the setting feeds a filter
   or a key. `[StateNeutral]` is the escape for settings that truly do not.
10. **An incompatible `keep` rolls back and reports.** *Alternative:* leave the pipeline
    stopped in `NeedsReset` — simpler, and a tenant's mistaken deploy takes their
    pipeline down until someone chooses.
11. **Plugin authors reference `Incremental.Service`** (compile time only).
    *Alternative:* a third package, `Incremental.Service.Abstractions`, holding the
    definition interfaces, `PipelineBuilder`'s surface, `TableDefinition` and the test
    harness's surface, so plugin projects do not see Kafka or hosting types.
12. **Followers are protected by a time margin** (`FollowerRetention`, 10 min of history
    kept whatever the readers have committed). *Alternative:* the standby reports its
    positions to the leader through the probe and the floor uses them exactly; less
    history kept, and a standby that stops reporting has to be timed out anyway.

## Not verified

- That a collectible context unloads once state, connectors and settings use
  per-generation serializer options. S0's spike says; the cap on live contexts holds
  either way.
- That `ReadCommit` ignores a member it does not know, so that adding `layout` to the
  commit record needs no format bump.
- That `Build()`'s construction pass can be run in a describing mode (C3) without
  claiming derived tables or pinning table versions. If it cannot be done cleanly the
  fallback is nodes and edges from the builder and store names from a build against a
  throwaway in-memory provider, with the table claims skipped.
- Takeover time with many tables. Promotion is measured at 10–25 ms for two tables and
  0.25–0.5 s per table when a follower stands short of an aborted tail
  (durability-and-standby.md), which is the kill -9 case; the service promotes
  concurrently, and S2's failover test runs with 20 tables and a target of under 2 s for
  the table stage.
- Takeover time with hundreds of built circuits and their sources; one consumer thread
  per source (durability-and-standby.md, *Limits*) is the likely ceiling.
- State decode time per entry, which prices *Open for James* 1's alternative and the
  replace-in-flight takeover.
- That a follower releasing a large pipeline's stores at a layout marker does not stall
  its state partition for long enough to matter.
- That disposing a built, never-started circuit on a follower is free of side effects in
  every operator. `Circuit.DisposeAsync` runs no step unless started, by reading.
- Scan time of the blob topic near its 512 MB cap.
- The data-shape fingerprint's false positives on real plugin assemblies; it now only
  informs a choice, so a false positive costs a line of text.

## As built

Nothing yet. Each build step records here what it built and where it departs from the
text above.
