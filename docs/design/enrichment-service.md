# Design: the enrichment service

> **Status:** proposed design, 2026-11-10, revised once after two reviews. S1 (the service
> core, in memory, with the part of S0 it needed), S2 (the Kafka backend) and S3 (the admin
> API and UI) and S4 (the two examples and the guide) are built. *As built*, at
> the end, says what exists and where it departs from this text — read it before relying
> on any section. Build steps S0 to S4 (see *Build order*) record what they change there.
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

**Seeds.** `AddTable` and `AddPipeline` on the DI builder are written to the catalog
once per deployment. The catalog records which seeds have been applied (`node/seeds`);
a leader applies the seeds that record does not name, and an entry that is already there
counts as applied. After that the catalog wins: a seeded table or pipeline that is
removed stays removed. (As first built the test was "the key is absent", which a removal
makes true again: see *As built*, S5.)

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
    ValueTask WriteAsync(string path, Func<Stream, CancellationToken, ValueTask> write, CancellationToken ct);
                                                        // whole or not at all: replaces the object when `write` returns (S6)
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
(1) a loaded contract assembly, by simple name, *even if the plugin ships its own copy*;
(2) the host's own — `Incremental*`, `Microsoft.Extensions.*`, `Confluent.Kafka`, the
framework — from the default context, whatever the plugin ships; (3) any other assembly
the host carries, from the default context, if the host's version is at least the one
referenced or the plugin ships none; (4) anything else privately, through
`AssemblyDependencyResolver` and then by file name beside the main assembly. So two plugins that both reference `Acme.Contracts`
get the same `Instrument` type, and it is the type the table was opened with. A row type
of a shared table declared in a pipeline assembly cannot work, and `registry.Get` says so.
A pipeline compiled against a newer contract than the one loaded fails at validation with
the missing member.

**Changing a contract.** A new version under the same assembly name is stored and
reported as *pending*: it takes effect when a process starts — a starting process loads
the version imported last (`PluginManifest.Sequence`), and the older one then reads
*superseded*. Roll it with the pair:
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

1. **Warm circuits on the standby** (default; built in S6 after S1 had taken the
   alternative — see item 30) — the standby loads plugin code and builds
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
    pipeline down until someone chooses. (Built in S6.)
11. **Plugin authors reference `Incremental.Service`** (compile time only).
    *Alternative:* a third package, `Incremental.Service.Abstractions`, holding the
    definition interfaces, `PipelineBuilder`'s surface, `TableDefinition` and the test
    harness's surface, so plugin projects do not see Kafka or hosting types.
12. **Followers are protected by a time margin** (`FollowerRetention`, 10 min of history
    kept whatever the readers have committed). *Alternative:* the standby reports its
    positions to the leader through the probe and the floor uses them exactly; less
    history kept, and a standby that stops reporting has to be timed out anyway.
13. **A replace that was rolled back answers `409 state-incompatible`** (S6), although
    the catalog did change twice (the replace, then the roll-back) and the pipeline is
    running. *Alternative:* `200` with a `rolledBack` member in the body. It reads as
    success to a client that looks only at the status, which is the client that most
    needs to know its change did not take.
14. **A state mismatch with nothing to go back to is `NeedsReset`, at once, with no
    retries** (S6): a spec changed behind the service's back, a table recreated under a
    running deployment. *Alternative:* the restart policy as for any fault, ending in
    `Failed` after five tries. Retrying finds the same state every time and logs a stack
    each time; `Failed` then names no way out, where `NeedsReset` does.
15. **The in-memory backend compacts by default** (S6: `StateCompactAbove` 8,192 records
    per partition, `RetainedHistoryChanges` one million per table). *Alternative:* off by
    default, as the core's `InMemoryStateChangelog` is, and documented as unbounded. The
    cost of on is a pause inside one commit per doubling of a partition (about 0.1 s per
    million live records) and a standby cut loose from a table whose leader wrote more
    than a million row changes while it was not reading.
16. **The meter's instruments carry a `node` tag** (S6). *Alternative:* none, leaving
    the node to the exporter's resource attributes — one tag fewer on every series, and
    two nodes in one process (the in-memory cluster) no longer told apart.
17. **The example references OpenTelemetry** (S6: two packages, for `--otlp`), so that
    the guide's monitoring snippet is compiled code. *Alternative:* the snippet as prose
    in the guide and no dependency; nothing then checks that it still builds.
18. **A dead-letter sink that cannot be written stops the pipeline** (S6): retries, a
    fault, the restart policy, `Failed`. *Alternative:* count the failure, keep the
    records in the ring only, and carry on — the pipeline's results keep flowing and
    the records that were dropped have no durable trace, which is the gap the sink was
    built to close.
19. **`deadLetters` is a member of the spec, not a slot under `sinks`** (S6).
    *Alternative:* a reserved slot name under `sinks`. One list fewer in the spec; a
    definition could then never name a slot `deadLetters`, and every place that walks a
    definition's sinks would have to step over one that is not the definition's.
20. **`PipelineTestHarness` ships inside `Incremental.Service`**, namespace
    `Incremental.Service.Testing` (S6). *Alternative:* a package
    `Incremental.Service.Testing`. It would keep about 350 lines of test-only surface
    out of the service's assembly, at the cost of one more package to version and of
    `InternalsVisibleTo` for it; the harness references no test framework, so the
    service gains no dependency by carrying it.
21. **The files sink's reader rule is "highest term per step"** (S6), which a late
    write by a replaced leader can defeat when the new leader writes nothing for that
    step. *Alternative:* a marker object per term, written when a generation starts and
    naming the step it starts at, so that a reader discards older terms' objects at or
    above it. Exact; one more object per generation start, and every reader — the
    owner's included — has to read the markers first. *S7:* the generation's base step
    is now in every object's name and a reader voids an older term's objects above a
    later term's base, which closes this once the successor has written (item 53).
22. **A files source's position is encoded into the checkpoint's partition → offset
    map** (S6: up to 2 + 2·64 entries per input, name limited to 256 bytes).
    *Alternative:* an opaque position per input in the checkpoint, which is a change to
    the core's checkpoint marker and a format-version bump for every circuit, to serve
    one connector.
23. **The example generates poison by default** (S6: one trade in 200), so that the
    dead-letter page and sink are not empty in the mode a newcomer runs. *Alternative:*
    off unless `--poison N`; the dead-letter page is then empty until asked for.
24. **A reset starts a files dead-letter sink again**: step numbers begin at 1, so the
    old objects are deleted at the first write, while a topic keeps everything.
    *Alternative:* the incarnation in the object name. Dead letters would then survive
    a reset, and a reader would have to know which incarnation is the live one.

25. **A caller nobody authenticated may name itself** (S6: `AcceptOperatorHeader`, on
    by default; the audit records `name (self-declared)`). It makes the default,
    loopback configuration's audit tell operators apart, on their word.
    *Alternative:* off by default, so the audit says `local` until the host
    authenticates or sets `CallerName`. Nothing in the audit is then anyone's word, and
    two colleagues on one loopback surface cannot be told apart.
26. **The CI job for the connector stack starts the repository's compose file**, not
    `services:` containers (S6). Postgres must be started with `-c wal_level=logical`
    and a service container takes no command; and CI then runs the stack a developer
    runs. *Alternative:* service containers with an image that has logical decoding
    configured (`quay.io/debezium/postgres`), which is a second definition of the stack
    to keep in step with the first.
27. **A long circuit is drawn in bands, at a readable size** (S6), rather than scaled
    down until it fits one row. *Alternative:* one row, scaled; the whole shape is
    visible at once and nothing can be read past a dozen operators.
28. **A table's size is an estimate of its data, not of its memory** (S6: the mean JSON
    size of a sample times the row count). *Alternative:* account for bytes as rows are
    written — a counter in `SharedTable`'s write path and a size function per row type,
    which is core work on the hot path of every table writer for a number that the
    working set already bounds from above. *S7:* the alternative was built, sampled
    (item 60); the JSON figure stays beside it as the size of the data.
29. **A file loader without a `path` is refused when the table is declared** (S6); it
    used to be accepted and fault the loader at its first run. *Alternative:* as
    before, which lets a table be declared ahead of knowing its file.
30. **A standby builds every pipeline's circuit, so plugin code runs on it** (S6: the
    definition's `Build` and the operators' constructors; never a step). It is this
    list's first item decided by measurement: without it a takeover decodes every
    pipeline's state, 2.7 s per million orders in the largest join. *Alternative:*
    `WarmStandbyPipelines = false` — no plugin code on a standby, and takeovers that
    grow with the state. A middle way was not built: decoding the followed bytes at
    takeover in parallel within a pipeline, which would cut the constant by the core
    count and keep the growth.
31. **Layouts are a new format byte in the state log** (S6), so a node from before
    that build cannot follow a newer leader and both nodes are replaced together.
    *Alternative:* keep one format and mark a change of layout with a record of its own;
    the cost is that compaction can remove the marker from under a follower that is
    behind, which then applies another version's records to its typed stores — the
    per-record tag exists because that cannot be ruled out.
32. **The takeover ceiling stays at 30 s** (S6; S7 found why it was reached). The loss
    of one broker silences a leader for 8 to 15 s with the client settings of S7 —
    11 to 16 s if the broker stops answering instead of dying, and once 26 s — so the
    ceiling has room; before S7 the client's own delays took a leader past it.
    *Alternative:* a lower ceiling — 20 s would have cleared every run of S7 but that
    one — which shortens the outage when a leader really is alive and stuck and leaves
    less room for a slow cluster; or a lower `broker.session.timeout.ms` on the
    brokers, which is the cluster's to set and is the floor under every figure here.
33. **A refused group lookup is passed over, and no permission on a group is asked
    for** (S6). The broker's authorizer logs each refusal, about one a second per
    consumer. *Alternative:* require `Describe` on the three group names
    (`{prefix}.reader`, `{prefix}.svc.blobs.{node}`, `incremental.source`): a quiet
    log, one more permission to get wrong, and `incremental.source` is not under the
    deployment's prefix.
34. **The broker-stopping tests and the SASL run are not in CI.** They need three
    brokers whose containers the test may kill, take about ten minutes, and have run
    only here. *Alternative:* a nightly job on the `kafka3` and `kafka-sasl` profiles.
    What S7 moved into every run instead: the lost takeover, the stale request that
    caused it, the supervisor contract and the earlier broker fixes are tested on one
    broker or on none.
35. **A takeover that is fenced before the node is the writer leaves it a standby**
    (S7; S6 had it end the node, and one rolling broker restart of 21 left both nodes
    fenced). The reproduction S6 asked for exists (`StaleEpochRequestTests`): the
    fence came from the replaced leader's own client, through a request the broker
    should have refused. The exception to "Fenced is final" is drawn where nothing has
    been promoted and no term committed, so two writers still cannot alternate.
    *Alternative:* keep it final and rely on the supervisor alone, which S7 also built:
    both nodes then restart and one leads from a cold start (S6's *How large* has
    what that costs with the table size) where the standby that stayed leads warm.
36. **Event-time state is checkpointed, and an older checkpoint of an event-time
    pipeline is refused** (S7). The watermark and every deadline are in the checkpoint
    from state-manifest format 7 on. A pipeline that uses `WithEventTime` and whose
    state was committed by an earlier build does not start (`state-incompatible`); it
    needs `state=reset`, which reads its inputs again. A pipeline without event time is
    not touched. *Alternative:* accept the old checkpoint and rebuild the deadlines
    from the keys of its state. The deadlines can be rebuilt; the watermark cannot, so
    the first late record after such a start would still reopen a window — once per
    pipeline, at the upgrade. It would also want an enumeration on every store.
37. **State bytes are kept beside the typed stores until the first commit after a
    start** (S6), so a leader's idle pipeline holds its state twice for as long as it is
    idle. *Alternative:* commit once as soon as a circuit has started on existing state,
    without a step. The copy goes at once; it costs a transaction per start, and it
    makes a keep that started a keep that is confirmed — the roll-back would then cover
    "could not start" and nothing after it, which is all it covers in practice.
    *S7: the alternative is now what happens*, two seconds after a start on restored
    state that has had nothing to read (`Circuit.RestoredCopyGrace`). What it gives up:
    a build that ran idle and never wrote an entry has, after those two seconds, had
    its say — a replace that keeps state then carries over what that build's types
    hold in memory, where before it carried the bytes untouched. The changelog still
    holds every entry as it was last written. *The other way:* leave it as S6 had it
    and accept twice the state on quiet pipelines; the pipeline's page now shows the
    entries held twice either way.
38. **`IObjectStore.WriteAsync(path, write, ct)` replaced `OpenWriteAsync`** (S6, second
    review): the store makes the object from what the callback wrote, and only if it
    returned. A stream that renames when disposed cannot tell success from failure.
    *Alternative:* keep `OpenWriteAsync` and return a stream type with a `CommitAsync`;
    it leaves the old call compiling and every implementer with a stream class to write.
    Nothing has been published, so the break costs the hosts in this repository only.
39. **A files source lists again at most once per `pollInterval` while it has names to
    read** (S6, second review); the rule a producer must keep is "names increase".
    *Alternative:* list before every object, which makes "an object whose name sorts
    after the position is read" exact up to the moment of the listing, at one listing
    per object — on a store that cannot list "after this name", a backlog of n objects
    then costs n² names. *S7:* a source now lists only after its position, a page at a
    time, and holds back behind an object that has not settled (item 52); the rule a
    producer keeps is "within `settle` of the greater names".
40. **Only the `files` sink claims what it writes** (`ConnectorDescription.Owns`; S6,
    second review). Two pipelines may still share a Kafka sink topic or a view, which
    for an append topic is a legitimate fan-in. *Alternative:* the `kafka` connector
    claims the topic of an upsert sink, where two writers do overwrite each other's keys.
41. **A line over `maxLineLength` (1,048,576 characters by default) is a rejected
    record**, a dead letter or a fault by the pipeline's policy, and not an error of the
    source (S6, second review). *Alternative:* fault the pipeline whatever the policy,
    since an object that is not JSON lines is a mistake of deployment and not of data;
    under `DeadLetter` it is now one envelope and the rest of the prefix is read.
42. **A description's note is cut at its first colon when row data may not be shown**
    (S6, second review), by the convention the event feed already has. *Alternative:*
    notes as records with the exception's text in a member of its own; exact, and a
    change to `DefinitionDescription`, `SpecTemplate` and `PluginDiff`.
43. **The example deletes its own files-sink objects**, keeping the newest 200 under
    each prefix (S6, second review), because a directory has no lifecycle rule and the
    example is meant to be left running. *Alternative:* a retention setting on the
    `files` sink itself; then the service deletes a tenant's output on a schedule, which
    a bucket does better.
44. **A changed number of `WithEventTime` sources refuses the kept state** (S7). The
    committed clock is per source in declaration order, so a definition version that
    adds or removes an event-time source (other than its first or its last) cannot
    `keep`. *Alternative:* key each source's entry by the name of its node, which
    survives a reordering when the node is `.Named()` and silently misassigns when it is
    positional and the positions moved; or drop the per-source entries on a mismatch
    and keep only the watermark, which stalls the watermark until every source has
    spoken again but never lets it regress.
45. **`EvictWhen` added to an operator that already has state leaves the keys already
    there without a deadline** (S7): they close only if a record touches them again.
    New stores start empty, as ever, and a deadline store is a store. *Alternative:*
    refuse a `keep` that adds a deadline store beside a non-empty state store of the
    same operator.
46. **A pipeline's state size is readable but not shown** (S7):
    `ChangelogStorageProvider.CountEntries()` walks the state, so it is not in the
    status the UI polls. *Alternative:* keep a running count per store and show it per
    pipeline; it is what would let an operator see a definition whose state grows for
    ever (the second review's `execution-quality`). *Later in S7:* built — the stores
    count their entries, `EstimateSize()` reads the counts, and the pipeline's page, the
    pipelines list and two instruments show them.
47. **Every Kafka client starts from the service's settings, not the client
    library's** (S7, `KafkaClientDefaults`): a reconnect delay of at most 1 s, a socket
    timeout of 6 s, a metadata refresh every 2 s. They are what keeps the loss of a
    broker under the takeover ceiling. *Alternative:* leave the library's defaults and
    document the settings for the host to make; a host that forgets has a leadership
    move at every broker that stops answering. The cost of ours: a request that takes
    longer than six seconds for a good reason is made again (the producers are
    idempotent), and each client asks for metadata every two seconds — a node with
    thirty clients sends fifteen small requests a second.
48. **At the ceiling a standby believes the controller about whether anybody can
    commit** (S7). *Alternative:* prove it by writing — a record of its own to the
    leader topic through a plain producer before it takes over. That also sees a
    cluster whose controllers are gone and whose metadata is therefore stale, which the
    built check does not; it is a second kind of write outside the fenced producer and
    it moves the leader topic's end, which is the sign of life of a leader inside a long
    transaction, so the standby would have to tell its own record from the leader's.
49. **A Kafka sink keeps a producer of its own unless its spec says `"producer":
    "log"`** (unchanged). S7 measured what that costs when a broker stops answering:
    the sink's producer finds the broker out separately, after the writer has. With the
    2 s metadata refresh it usually knows the new leader before it sends.
    *Alternative:* make `log` the default for sinks on the log's own cluster — one
    client fewer to recover, and a sink write that commits with its checkpoint; a sink
    on another cluster cannot use it, and a large step's sink records then ride in the
    writer's transaction.
50. **`NodeSupervisor` is in `Incremental.Service` and the example uses it by default
    on Kafka** (S7). *Alternative:* document the contract only and leave restarting to
    the platform, as before; then the example, the rig and the tests each need
    something that restarts a node, which is how round two came to run without one.
51. **The `kafka3` profile's brokers are also its controllers** (unchanged). It makes
    the loss of the active controller part of every broker test, which a cluster with
    separate controllers (MSK) does not have. *Alternative:* a controller container
    of its own and three plain brokers: the broker-loss figure is then the 9 s case
    only, and the controller failover goes untested.

52. **A files source waits `settle` (5 s) for an object to be left alone before it reads
    it, and reads nothing after one that has not settled** (S7). It closes "an object
    that arrives behind the position" for objects that arrive within `settle` of the
    greater names, at `settle` of latency on every object. *Alternative:* `0` by default
    and the rule "names must increase", as before — no latency, and the hole is open for
    any writer that is not strictly ordered; or a seen-set over a look-back window,
    which reads late objects out of order without holding the others back, and needs a
    position the checkpoint cannot hold today (Open for James 22).
53. **A replaced leader's late sink object is void once its successor writes** (S7: the
    generation's base step is in every object name). *Alternative:* the marker object of
    item 21, written when a generation starts — exact from the start, but it is a write
    outside a step, which the lease rule allows only from the sink's `WriteAsync`, and
    the circuit does not deliver empty steps; so it would need a core change to deliver
    one batch per generation.
54. **Object names changed** (S7): `{step}-{term}-{base}.jsonl`, and snapshots
    `….snapshot.jsonl`. CLAUDE.md's rule was "a new connector name, not an edit"; this
    is an edit because readers and the sink's clean-up still read the old form and a
    deployed sink's next generation writes the new one beside it — nothing loses its
    place. A reader written against the old form does not see the new objects.
    *Alternative:* a connector `files2`.
55. **A missing input is a warning, never a refusal** (S7). A dry run says what is not
    there and the deploy goes ahead; after `MissingInputGrace` (30 s) the node is
    `Degraded`. *Alternative:* refuse the deploy unless `force`; that stops a typing
    mistake at the door, and refuses the ordinary case of a pipeline deployed before its
    first file is delivered.
56. **The morph test runs a real Chrome with `--dump-dom`** (S7) and is skipped where
    there is none. *Alternative:* a DOM shim in Node (tests the shim's idea of a select,
    not a browser's), or Playwright in CI (a package and a browser download per run).
57. **The node asks its own collector for collections** (S7, `MemoryKeeper`): young
    ones only, twenty-five at a time, and only while the collector has committed a
    quarter more than is live and the process is collecting seldom. It is what takes a
    standby that has just restored 2.4 GB of rows and state from 5.2 GB to 3.6. *Alternative:*
    never call `GC.Collect` from a library, and document that a quiet node holds what
    its last burst needed until some twenty collections have passed by themselves —
    hours, on a standby. `ServiceOptions.ReleaseUnusedMemoryEvery = TimeSpan.Zero` is
    that alternative, and turns off the allocator's trim with it.
58. **`System.GC.ConserveMemory` is not set in the node's settings** (S7,
    `Node.Gc.props`). At 5 and 7 it took 0.3 and 0.4 GB off a leader's 3.4 GB at a
    million rows and 0.7 to 1.3 GB off its peak, and nothing off the steady state at
    five million. It works by asking for full blocking collections, and the state fill
    was the slower of its two speeds more often the more was conserved — which the
    order of the runs may explain instead. *Alternative:* set it to 7 for the memory;
    the numbers are in S7.
59. **A Kafka source's fetch queue is 2 MB per partition** (S7), down from 16:
    0.8 GB of client memory at the end of a state fill instead of 2 GB, with no time
    lost in the fill or the steady state on one local broker. *Alternative:* keep the
    larger queue where the brokers are a network away and a refill costs a round trip
    — that was not measured; a connection's `queued.max.messages.kbytes` sets it
    either way. The logs' readers were tried at 2 MB too and left at 8: a restore was
    no faster for it.
60. **What a node says it holds is estimated from samples taken on the write path**
    (S7): each of a table's or a store's first 256 rows or entries and then one in
    512 is sized by reading its fields. *Alternative:* a walk of a sample when asked
    (nothing on the write path, and a figure that follows the rows there now rather
    than the rows as written), which reads a table of any size in hash order — biased
    wherever a row's size goes with its key — or needs a full scan to avoid that.
61. **A bulk load can stop the leader for two seconds** (S7, measured at five million
    rows: a full blocking collection of a heap growing through 7 to 13 GB; the same
    with the settings before this step). `TakeoverAfter` is 3 s. Nothing was done
    about it. *Alternatives:* load a large table before the standby is started;
    raise `TakeoverAfter` for the load; or have the table writer pace a bulk load —
    which is the one that needs building.
62. **Eight heaps, fixed in the node's settings** (S7), four in the example.
    *Alternative:* leave the count to the runtime (a heap per core): on this machine's
    twenty cores that was not run with the rest of the settings, and the adaptive mode
    that chooses the count by itself paused three times as long.

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
- ~~State decode time per entry~~ Measured in S6: 1.3 µs an entry for a join's stores,
  0.4 µs for a total — 2.7 s per million orders in a join pipeline. It is why the
  standby builds circuits.
- That a follower releasing a large pipeline's stores at a change of layout does not
  stall its state partition for long enough to matter. Still not measured: the release
  re-serializes the stores under the provider's lock, which is the decode's cost again,
  once per replace that keeps state.
- That disposing a built, never-started circuit on a follower is free of side effects in
  every operator. `Circuit.DisposeAsync` runs no step unless started, by reading.
- Scan time of the blob topic near its 512 MB cap.
- The data-shape fingerprint's false positives on real plugin assemblies; it now only
  informs a choice, so a false positive costs a line of text.

## As built

Each build step records here what it built and where it departs from the text above.

### S0 — not built as a step; the part S1 needed was built with it

When S1 started, nothing of S0 was in the tree: no core change, no spike, no example. S1
is written against the core, so the seams it could not do without were built first, in
the core, with their tests (`tests/Incremental.Tests/ChangelogReleaseTests.cs`, 5 tests).
The rest of S0 is still to do.

| | Built | As |
|---|---|---|
| C1 | in part | `ChangelogStorageProvider.ReleaseStores()`: typed stores become held bytes again (uncommitted writes are undone first), in either role. **No `layout`**: no parameter, no member in the commit record, no `Layout` property, no marker, no follower halves, no untouched-bytes retention. |
| C2 | yes | `ChangelogStorageProvider.CommittedOffsets` (a copy; any role). |
| C3 | no | `CircuitBuilder.Describe` does not exist. |
| C4 | in part | `TableSnapshot.Rows()`. `RowsBy` is not built. |
| C5 | yes | `ChangelogStorageProvider.UseSerializer`, refused while a typed store exists. |
| C6 | yes | `SharedTable.FollowFault`. |
| C7 | no | A refused `Start()` is still an `InvalidOperationException`. |
| example (a), decode timing | no | |

**What releasing does not make safe.** A store finds an entry by `Equals` and the log
finds its record by the key's bytes, and the provider relies on a key serializing to the
bytes it was read from. That holds across two loads of one assembly (same shape, other
`Type` objects). It does not hold when a type that is part of an entry's *key* gains or
loses a member — the value of a weight store (`Distinct`), the value half of an indexed
weight store (a grouping's records): the new generation writes the entry under new key
bytes and the old record is never overwritten or tombstoned, so a later restore reads
both. A value store's value may change shape freely. `Keep` across such a change is
therefore wrong in a way nothing detects; C1's untouched-bytes half would not catch it
either. The test file shows both cases. Until the provider rewrites or refuses, the
honest rule for authors is in docs/intro.md: `Keep` only while the shapes of types in
store keys are unchanged.

**The unload question is answered: collectible contexts unload.**
`PluginTests.Two_versions_of_one_plugin_run_side_by_side_and_one_unloads_after_its_last_use`
loads two versions, runs circuits with typed stores on both, replaces one pipeline across
versions keeping its state, and sees the first version's context collected. Two things
beyond the design's list had to be true for that:

- **Per-generation `JsonSerializerOptions` are not enough by themselves.**
  System.Text.Json shares one metadata cache between options instances it finds
  equivalent. Two versions' options, built the same way, shared theirs; and the state
  options (`IncludeFields = true`, nothing else) shared the cache of the process-wide
  `JsonStateSerializer.DefaultOptions`, for ever. Each options instance the service builds
  for a plugin version now carries a `DefaultJsonTypeInfoResolver` of its own, which makes
  it equivalent to no other. The mutation that removes this fails the unload test.
- **`System.ComponentModel.DataAnnotations.Validator` keeps a process-wide store of type
  metadata.** Settings are validated attribute by attribute instead
  (`ValidationAttribute.IsValid`), which touches no store.
- **One process-wide cache is left, and it empties only when used** (found in review,
  S5). System.Text.Json compiles the accessors of a type's constructor and members and
  keeps them in a static cache keyed by the type; an entry is dropped about a second
  after its last use, but only when the cache is next used by anyone. The unload test
  passed in the full suite, where other tests use the cache, and failed when run alone:
  on a quiet node an unloaded context stayed alive, and counted against
  `MaxLoadedPluginVersions`, until unrelated JSON work happened. An unload is now
  followed by four uses of that cache over about five seconds, with a type of the
  service's own (`JsonAccessorCache.EvictSoon`). The test passes alone; with the call
  removed it fails alone.

So pipeline contexts are collectible, and `MaxLoadedPluginVersions` is the backstop it was
designed as.

### S1 — the service core on the in-memory backend

`src/Incremental.Service` (net8.0; net10.0; packed in CI), `tests/Incremental.Service.Tests`
(34 tests, in memory) and `tests/Incremental.Service.TestPlugins` (five small projects:
two contract assemblies, two builds of one pipeline plugin under one assembly name, and a
second pipeline plugin; built, never referenced, loaded from their output directories).

| File | Holds |
|---|---|
| `Backend.cs` | `IServiceBackend`, `IBlobStore`, `INodeLeadership`, the status records, `LeadershipCallbacks`, `ICatalogMirror` |
| `InMemoryBackend.cs` | `InMemoryServiceCluster`, `InMemoryServiceBackend`, `InMemoryBackendOptions` (with `CatalogDirectory`) |
| `Catalog.cs` | `Catalog` over `svc.catalog`, the write lock, audit, the value cap; `ActionPlan`, `ActionOptions`, `ServiceException` |
| `ObjectStore.cs` | `IObjectStore`, `LocalObjectStore` |
| `Tables.cs`, `TableRegistry.cs` | `TableDefinition`, `IndexDefinition`, `TableSpec`, the table source contract, the registry, the `file` loader, retention, stats |
| `Plugins.cs` | the contract and plugin load contexts, import and its validation, manifests, settings binding, leases |
| `Pipelines.cs`, `Connectors.cs` | the definition contract, `PipelineBuilder`, `PipelineSpec`, the connector interfaces and contexts, the `memory` connector |
| `PipelineSupervisor.cs` | generations, the per-pipeline queue, deploy/replace/start/stop/restart/reset/remove, faults, dead letters |
| `ServiceNode.cs`, `ServiceOptions.cs`, `ServiceCollectionExtensions.cs` | the node, health, readiness, events, role changes, shutdown; options; DI |

**Departures from the text above.**

1. **A standby builds nothing; it holds pipeline state as bytes** (*Open for James* 1,
   the alternative, taken because C1's follower halves do not exist). The provider already
   keeps a followed pipeline's records as bytes until a circuit creates its stores, so a
   takeover builds each circuit from them and pays the decode. With that go `layout`,
   `confirmed` and `previous` in the spec, the layout marker, `PipelineState.Built` (the
   enum member stays, unused), plugin code on a standby, and "pipelines rebuilt on a
   reopened follower". A standby's pipelines show `Unbuilt`. `StandbyReadiness` is
   catalog applied, no table faulted or still opening, state lag zero.
2. **Replace.** `Keep` is: stop, `ReleaseStores()`, build the new generation on the bytes.
   There is no store-name check before the stop (no C3) and **no rollback**: a `Keep`
   whose new generation cannot read the state faults, backs off and ends `Failed`, with
   the committed state untouched for a redeploy of the old spec or a reset. The spec
   holds `generation` and `pending` only.
3. **The dry run** runs the definition against a `CircuitBuilder` that is never built. Its
   shape is the slots, their kinds and types, and the tables read and written; no node
   graph, no store names, no unnamed-store warnings. A table must be declared and open for
   a spec that names it to validate.
4. **Seam additions.** `IServiceBackend.RetainedHistoryVersions` (the cap is the
   backend's: heap in memory, disk on Kafka; the registry needs it for `VersionsUntilCutLoose`)
   and the optional `ICatalogMirror`, which is how `CatalogDirectory` is implemented
   without the service knowing the in-memory backend.
5. **Locks.** One queue per pipeline, the catalog's write lock, a declare lock in the
   registry and a role gate in the node around `PrepareAsync`/`StartLeadingAsync` and a
   standby's reconcile. There is no node-wide shared/exclusive gate; nothing found needed
   one.
6. **Connectors.** `memory` only (append and keyed inputs, upsert and append sinks,
   resettable). The `file` source and sink connectors, the `deadLetters` sink slot and
   `AddKafkaConnection` are not built. `ITableSourceFactory.Create` takes the
   `TableLoaderSpec`, not only its config, so a factory sees the connection name.
7. **Plugins.** The name is the main assembly's simple name in lower case; the main
   assembly is the one with a `.deps.json` beside it, else the only `.dll`, else the one
   `plugin.json` names. `.pdb` files are not part of a package. A second version of a
   loaded contract is stored and shown `pending`; the row-shape comparison and its 409 are
   not built. The plugin directory is imported before the pipelines start, not after, so
   that a seeded pipeline can name a plugin from it. The settings schema has no defaults.
   The duplicate-definition refusal is written and not tested (it needs a sixth plugin
   project).
8. **Tables.** The 80% retention warning (event and `Degraded` reason) is not built;
   `VersionsUntilCutLoose` and the stop plan's line are. A cut-loose pipeline is recognised
   by the text of the core's refusal ("no longer retained"), there being no C7. A faulted
   writer is recognised by committing nothing and reading the refusal ("has faulted");
   the core should expose the writer's fault. The writer-fault path is tested (an index
   selector that throws on one row: the table reopens under the restart policy, ends
   `Failed` naming the loader's position, the node leads on and the pipelines that read
   it are rebuilt); the follower-fault reopen on a standby is written and **not tested**.
   `registry.Get` refuses only a table with no table object (opening, or unopenable): one
   whose loader or writer has failed still serves its last version, so its readers are
   not `Blocked`. A table object keeps a tail of
   its own, so the history cap bites only when the table is opened from its log again (a
   restart, a takeover) — the cut-loose test restarts the service for that reason.
9. **Node.** Health has no "no ready standby" reason (a lone in-memory node would be
   permanently degraded; S2 decides with a real peer). `BlockedOn` is backoff, table,
   sink-ack and source-paused. Exit code 5 and the catalog-writer watchdog are not built.
   Shutdown's stop budget is 60% of `StopTimeout`, not of the host's remaining time, and
   the `ShutdownTimeout` warning is not logged. `ServiceMetrics` and `PipelineTestHarness`
   are not built.
10. **The in-memory leadership tells a deposed leader directly** that it is fenced, at the
    moment the new one acquires the writer; there is no heartbeat for it to miss.
    `Leased` passes sinks through, so two in-memory nodes give no protection against a
    deposed leader's last sink write landing after its successor's.

**Found while building.**

- *A runtime crash, not understood.* With the four test classes starting a node each at
  the same moment, the test process died with `Internal CLR error (0x80131506)` in about
  one run in five, at about a second in; the crash report shows the crashing thread in
  native runtime code and the other threads in a node's first `PrepareAsync`. It does not
  depend on collectible contexts (it happens with them made non-collectible) or on tiered
  compilation or concurrent GC; disabling W^X made it rarer. Starting one node alone
  before the parallel ones (`TestService.WarmUp`) removed it in 40 runs of 40. A
  production process starts one node, so it is not exposed in the same way; **an example
  that runs two nodes in one process (S4's `--nodes 2`) should start them one after the
  other.** The same error is known on this machine as a rare failure of `dotnet build`.
- *A spec compared by its JSON text.* The first version of the state-choice check
  compared slot configs and settings as raw text, so a spec read back from the catalog
  (compact) and the same spec as a caller formats it differed, and an unchanged redeploy
  after a restart was refused with `state-choice-required`. A mutation run found it (the
  seed check could be removed with no test failing, because the redeploy it allowed was
  being refused for this reason). Specs are now compared in a canonical form: no
  insignificant whitespace, members in name order.
- *`Tables` as the name of a contract's static class* collides with the namespace
  `Incremental.Tables` for code inside an `Incremental.*` namespace (the tests alias it).
  Plugin authors in their own namespaces are not affected.

**Tests** (`dotnet test --project tests/Incremental.Service.Tests`, 34, about 4 s). The
five the step was asked for by name: twelve pipelines sharing a 200,000-row table (results
against a model, the delivered strings reference-equal to the table's rows, and heap
growth under half of one copy of the table); a pipeline deployed, replaced by reset,
changed by a neutral setting and removed while another is fed continuously and ends equal
to its model with no restart; a stopped pipeline holding the retention floor, then
catching up, then releasing it; a poison record driving one pipeline through two backoffs
to `Failed` while its neighbour runs on; and a whole-service restart on the same storage
reproducing five pipelines from the catalog, with sinks equal to a model and each source
having read only what arrived while the service was down. The plugin tests use real
assemblies: two versions side by side, state kept across them, unload seen through a weak
reference, two plugins and a catalog-delivered contract joining one table, hostile zips,
the context cap.

Mutation checks, each a one-line break of the behaviour, run against the suite: 30 killed
— among them trim ignoring readers, no follower margin, never `Failed`, no backoff, the
dead-letter output unread, a plugin given its private copy of a contract, a leaked plugin
lease, the shared JSON cache, no state choice, a neutral setting treated as a change,
a reset that keeps state, a stop that abandons instead of draining, no catalog value cap,
no table fencing at takeover, a source that replays instead of resuming, zip inflation
uncounted, unsafe zip names, blobs not stored, seeds applied every time, a pending reset
not finished by a successor, an unbounded wedge, an unreadable record that never faults,
a second writer of a derived table, a stale generation accepted, an unguarded step-down,
a writer fault unseen, a reopened table not announced to its pipelines. One was
equivalent (dropping the explicit `Unload()`: an unreachable collectible context is
collected anyway — what matters is that nothing reaches it, which the leaked-lease and
shared-cache mutations test). One survives: the deploy path's own `ReleaseStores` on a
layout change is redundant with the check at generation start when the plugin version
changes, and the case where it is not — another definition of the same version, kept —
has no test.

**Not built in S1, in one list** (each is also in the departures above): `layout`,
`confirmed`, `previous` and rollback of a `Keep`; warm circuits on a standby; the graph
and store names of a dry run; `file` source and sink connectors; the `deadLetters` slot;
`AddKafkaConnection`; contract schema-change detection; the 80% retention warning;
`no-progress` and `lease` in `BlockedOn`; the catalog-writer watchdog and exit code 5;
`ServiceMetrics`; `PipelineTestHarness`; the shutdown-timeout warning; C3, C7, `RowsBy`
and the rest of C1; example (a).

**What S2 must know.**

- The seam is `Backend.cs`. A Kafka backend implements `IServiceBackend` and
  `INodeLeadership` and nothing else in the service changes; `ICatalogMirror` is not for
  it. `OpenTableLogAsync` on a standby must complete only once the log exists, and on a
  node that holds the writer must create it — the catalog's first open depends on both.
- The leadership must call `PrepareAsync` after the writer is acquired and before the
  tracked storage is promoted, and `StartLeadingAsync` after. The service sets its own
  "leading" flag at the start of `StartLeadingAsync`, not from `Status.Role`.
- `Leased` is where a real lease goes; the supervisor wraps every sink it creates.
- A standby runs no plugin code. If *Open for James* 1 goes the other way, the supervisor
  needs a build-without-start path and the core needs the rest of C1 first.
- Start two nodes in one process one after the other (see the runtime crash above).

### S2 — the Kafka backend

`KafkaServiceBackend` and everything that makes a pipeline's edges Kafka. Two processes
with the same `KafkaLogOptions` and different node ids are an active and its standby: the
standby follows the catalog, the tables and the state through the broker, fetches plugin
files from it, and leads when the leader is killed, resigns or is taken over.

| File | Holds |
|---|---|
| `KafkaBackend.cs` | `KafkaServiceBackendOptions`, `KafkaServiceBackend`, the leadership over `KafkaLeadership`, the blob topic (`KafkaBlobStore`) |
| `KafkaConnectors.cs` | `KafkaConnection`, `KafkaConnections`, the `kafka` connector, the `kafka` and `debezium` table loaders |
| `tests/Incremental.Service.FailoverApp` | one node as a real process: `AddIncrementalService`, `UseKafkaBackend`, a contract compiled in, a plugin directory; commands on stdin, events on stdout |
| `tests/Incremental.Service.Tests` | `KafkaServiceTests` (8), `KafkaServiceCdcTests` (1), `KafkaServiceProcessTests` (2) — all skipped without a broker — and `LeaderWatchTests` (2, in memory) |

**As designed.** The catalog is `svc.catalog` on the log's transactional producer, so a
deposed leader's catalog write is refused by the broker; tables are `KafkaTableLog`s,
promoted by the registry inside `PrepareAsync`; pipeline state is `KafkaStateChangelog`
followed by `ChangelogStorage.Follow` and promoted by the leadership; every sink is
wrapped in `leadership.Leased`; blobs are `{p}.svc.blobs`, compacted, one partition,
`{sha256}/{n}` in 256 KB records and `{sha256}/end` with the count, written by a plain
idempotent producer. Exit codes are 3 (fenced), 4 (resigned) and 5 (cannot lead, no ready
standby). K1 and K2 are built as specified.

**Departures from the text above.**

1. **The seam grew by two members.** `IServiceBackend.ConnectAsync` — `KafkaLog.ConnectAsync`
   is asynchronous and a backend is made by a synchronous DI factory, so the node connects
   it first in `StartAsync`. `INodeLeadership.PeerExpected` — whether a second node was
   configured at all (Kafka: `PeerProbe` is set; in memory: the cluster has another node).
   `KafkaLeadershipDetail` gained `Fault` (why the node is fenced, as text).
2. **Two more changes to `Incremental.Kafka`, both found by measuring.** They are in the
   log's shared short-read consumer (a table's head, the last term), which a node uses
   once or twice per table, one read at a time:
   - *It no longer waits out its own long poll.* A short read stops at the end of a
     partition, where the client has already sent the broker its next fetch; that fetch
     waits `fetch.wait.max.ms` (half a second) for data that will not come, and the client
     sends a broker one fetch at a time, so the next short read — another table's head —
     started half a second late. A takeover reads one head per table: 21 tables took
     10.5 s in the table stage. The short reader now uses `fetch.wait.max.ms` 5, and they
     take 0.14 s.
   - *It asks for a topic's metadata before it is first assigned to it.* A consumer that
     is already running learns the leader of a topic it has not seen only when the
     client's one-second timer next looks, so every short read of a new topic took a
     second: two per table when a node opens its tables. A standby took 45 s to follow 21
     tables, and a step-down took 1.2 s to hand over (the successor's first read of the
     leader topic). With the metadata fetched first those are 6.7 s and 0.3 s.

   Followers, restores and sources have consumers of their own and were never affected.
   Both are recorded in durability-and-standby.md.
3. **Blobs are read through an index, not by scanning for one hash.** The store reads the
   topic once per process (and what has been added since, before each call) into a map of
   hash → record offsets, then reads a blob's records at their offsets. If a record is
   not where the index says — the blob was written again and the cleaner removed the
   older copy — the index is rebuilt once. A blob is gathered in memory before it is
   written to the caller's stream (a plugin file, a few records), so a half-read file is
   never left behind. `DeleteAsync` writes tombstones with the same plain producer, the
   `end` record first; it is reached only after the catalog entry's deletion has
   committed, which is fenced.
4. **`UseKafkaBackend` registers the log's cluster as the Kafka connection `log`.**
   `"producer": "log"` (the transactional producer) is accepted only on that connection.
   The log's own topics — `{p}.state`, `{p}.leader`, `{p}.table.*`, `{p}.svc.*` — are
   refused as a slot's or a loader's topic on every connection: a spec is tenant-written,
   and a sink bound to the state topic would be a plain producer writing into it.
   `KafkaConnections` is public and resolvable from DI, for a custom connector or table
   source that needs the same cluster settings.
5. **The `kafka` connector's config is a closed record**: `connector`, `connection`,
   `topic`, `startFrom`, `format`, `producer`, `reloadWhenPositionLost`. Anything else —
   bootstrap servers, a client property — is an unknown member and refused at the dry run,
   as is a topic outside the connection's `AllowedTopicPrefixes`. `Describe(config)` does
   not know the slot's kind, so the kind rules (`"format": "debezium"` only on a keyed
   input; `producer` only on a sink; `startFrom` only on a source) are checked by the
   supervisor's dry run asking the built-in connector directly (`KafkaConnector.RequireKind`,
   internal). A custom connector has no such hook: it can refuse only at start. That is a
   gap in `IConnector`, left for whoever needs it first.
6. **Sources.** `SourceContext` and `KeyedSourceContext` gained `Stream` (the input as the
   definition declared it), which `KafkaSource.StartAsync` needs. A record a Kafka source
   cannot decode goes down the core's reject path and so into the pipeline's dead-letter
   ring, rendered as `topic[partition]@offset: value`; `SourceStats` carries records,
   unreadable, positions, lag (one offsets request every 5 s) and `PausedSince`.
7. **Sinks are disposed with their generation.** A sink a connector creates that is
   `IAsyncDisposable` (a Kafka sink owns a producer) is closed after the deliveries end.
   Sink topics are not created by the service.
8. **Loaders.** `kafka` and `debezium` are built in beside `file`; a table source the host
   registers under one of those names replaces the built-in. Rows are read with the
   process-wide JSON defaults (web for `kafka`, `Debezium.DefaultJson` for `debezium`):
   rows are contract types, which never unload. An unreadable record goes to the table's
   ring with its topic, partition and offset under either policy; under `fault` the call
   throws inside the source and the loader stops there.
9. **The retention cap is applied by the registry's trim** (`floor = max(floor, latest −
   RetainedHistoryVersions)`), because a Kafka table log keeps whatever it is not told to
   drop. `KafkaServiceBackendOptions.RetainedHistoryVersions` defaults to 100,000.
10. **A standby fetches plugin files and loads none.** On each catalog version it
    materialises the files of every plugin version a deployed pipeline names into its
    cache directory, hash-checked, so a takeover loads them from local disk.
    `StandbyReadiness.PluginsPending` counts versions not yet fetched and is a not-warm
    reason. The standby still builds no circuits (S1, departure 1): the hand-on "takeover
    with pipelines built on the standby" is **not** delivered; a takeover builds each
    circuit from followed bytes.
11. **The catalog-writer watchdog.** Once a second the leader asks the catalog's writer
    whether it has faulted (a commit of nothing). A fence is left to the leadership.
    Otherwise the catalog is reopened from its log in place, once per minute; a reopen
    that fails, or a second fault within the minute, is "cannot lead": the node steps
    down if the peer answers as a ready standby, and otherwise raises
    `ServiceNode.CannotLead` and stops the host with exit code 5. A standby whose catalog
    follower has faulted follows again from the rows, and a standby's first open of the
    catalog is retried until it succeeds.
12. **Health.** A leader with a peer configured and no ready standby answering is
    `Degraded` with `no-ready-standby`; a lone node is not. `NodeStatus` gained `Peer`
    (the last probe answer, sampled once a second) and `LastTakeover`
    (`ServiceTakeoverTimings`: catalog, tables and their count, pipelines and their count,
    all of `StartLeadingAsync`). The leadership's own `TakeoverTimings` is in
    `Leadership.Kafka.LastTakeover`; its `Tables` is always zero here, because the
    service promotes tables inside `Prepare`.
13. **A takeover waits for a standby that is still opening tables.** The standby's
    reconcile holds the role gate while it opens what the catalog names, and
    `PrepareAsync` takes that gate. A node asked to lead while it is restoring a large
    table finishes the restore first; it needs the rows either way.
14. **Pipelines never move leadership**, as designed — which is not what
    durability-and-standby.md's step-5 note asked of "the host" ("step down or exit when
    its circuits are gone"). The reasoning is in *When it cannot lead*; that document now
    points here.

**Not built in S2:** `LagAge` and a `ReadyLag` threshold in readiness (a standby is warm
when its state lag is zero at the last sample and no table is opening or faulted); the
`file` connectors and the `deadLetters` slot (still as S1); tests of the catalog's
follow-open retry, of a standby reopening a faulted catalog follower, of sink disposal and
of exit code 5 from a real process (the path is tested in memory up to the event). The S1
node tests were not re-run wholesale against the broker; the Kafka tests below were
written instead.

**Measured** (local single broker, Debug build, `TakeoverAfter` 3 s, heartbeat 1 s; two
real processes, 21 tables of 500 rows, two plugin pipelines, the leader killed with
`kill -9` mid-stream; `measurements.log` beside the test assembly):

| | |
|---|---|
| detection (kill → the standby decides) | 3.0 – 3.1 s |
| fence (`InitTransactions`, term) | 145 – 147 ms |
| catalog | 10 – 11 ms |
| tables, 21 concurrently | 134 – 141 ms |
| storage | 9 – 10 ms |
| start (leader record, pipelines built from bytes and started) | 211 – 256 ms, of which the two pipelines 174 – 181 ms |
| takeover total | 515 – 562 ms |
| kill → `Leader` | 3.5 – 3.6 s |
| step-down → the standby is `Leader` | 246 – 362 ms |
| a standby process, from start to warm (catalog, 21 tables, state, plugin files) | 6.7 s |
| a first leader on an empty deployment, from process start (3 s of silence, 20 table seeds, a plugin import) | 7.9 s |

Three runs; the ranges are theirs. The table stage is the number the design asked for
(target under 2 s for 20 tables); it was 10.5 s before departure 2, and the test now
holds it under 2 s. The pipelines here hold little state: the cost of decoding
a large pipeline's state at takeover — the price of *Open for James* 1's alternative — is
still unmeasured.

**Tests.** In process, two nodes on one topic prefix, each with its own `KafkaLog`:
a leader with a Kafka-loaded table and two pipelines on Kafka topics (one sink on the
transactional producer), a standby that sees them, a takeover while the stream and the
table move, both sink topics equal to the model and the sources resumed rather than
replayed; a pipeline deployed, one removed and one replaced by reset immediately before a
takeover that does not wait for the standby to see them; a deposed leader's catalog write
refused by the broker and its leased sink write refused before the sink is touched; a
plugin imported on the leader reaching the standby's cache through the blob topic, the
standby running it after a takeover, and a cold third node with an empty cache rebuilding
everything from the broker after both are gone; step-down refused with no standby,
degraded health meanwhile, then handed over on the probe's "ready" with the takeover
reason `resigned`; the blob store's chunking, idempotence, partial uploads and deletes;
the connector's config rules; the history cap. CDC: Postgres → Debezium → a table declared
with the `debezium` loader → a pipeline's `Lookup`, through snapshot, update, insert and
delete, and on across a takeover where the new leader's loader resumes from the table's
committed offsets. Two processes: the kill -9 run above, ending with the dead node
restarted as the standby; and a fenced process exiting 3, a refused step-down, and a
resigned process exiting 4 with the other leading each time.

Mutation checks, each a one-line break of the behaviour run against the test that should
notice: 23, all killed in the end — the lease made a pass-through, a blob without its
`end` record read as present, a blob written twice, the standby not fetching plugin files,
the catalog watchdog off, the catalog reopened without limit, "cannot lead" never stepping
down, K2 off, K1's token dropped, a probe that never says ready, `no-ready-standby` not
reported, unknown config members allowed, topic prefixes ignored, the service's own topics
bindable, Debezium accepted on an append slot, `"producer": "log"` accepted on any
connection, the history cap off, the `debezium` loader reading plain JSON, the exit code
not set, the standby never syncing pipelines, the short reader assigned without metadata,
the short reader long-polling. One survived its first run: nothing told a sink on the
log's transactional producer from one with its own. The test now counts transaction
markers in the two sink topics, and the mutation is killed.

**What S3 must know.**

- `NodeStatus.Leadership.Kafka` (heartbeat age, lease, the leadership's takeover
  timings, the fence's cause), `NodeStatus.Peer`, `NodeStatus.LastTakeover`,
  `NodeStatus.Standby` and `TableStats.LagVersions` are the node, lag and peer views.
  `TableStats` on a standby costs one short read of the table's head per call; do not
  call it for every table on every request.
- The leader's admin address is `LeaderRecord.AdminUrl` under `node/leader` in the
  catalog, which a standby follows: that is the leader redirect.
- `ServiceNode.TakeOverAsync` and `StepDownAsync` are the guarded commands; after a
  step-down or a fence the process exits (`ExitOnLeadershipLoss`), so the response to a
  step-down request may be the last thing that node's admin API sends.
- `KafkaServiceBackend.Log` and `.KafkaLeadership` are public for what the seam does not
  carry.

**What S4 must know.**

- A two-process run is `UseKafkaBackend(new KafkaServiceBackendOptions { Log = …,
  ProbeListen = …, PeerProbe = … })` with the same log options and different `NodeId`s.
  Start the first node and let it lead before starting the second: two standbys that
  start together both take over, one is fenced on its term commit and exits with 3
  (durability-and-standby.md, *Simultaneous cold start*).
- Give `PluginDirectory` to whichever node may lead first; the other gets plugins from
  the blob topic. Each node needs its own `CacheDirectory`.
- Create sink topics (compacted, for an upsert sink) before deploying; the service does
  not. Spec bindings name `"connection": "log"` or one registered with
  `AddKafkaConnection`.
- A first lead applies table seeds one after another, each creating two topics: twenty
  seeds and a plugin import took about five seconds on a local broker, after the three
  seconds of silence a first node waits before it leads.

### S3 — the admin API and UI

`src/Incremental.Service.Admin` (net8.0; net10.0; `FrameworkReference`
Microsoft.AspNetCore.App and nothing else; packed in CI): the JSON API under
`{prefix}/api`, the health endpoints, and the UI — six hand-written files embedded in the
assembly (`wwwroot/index.html`, `app.css`, `app.js`, `actions.js`, `views.js`, `shell.js`),
no build step, no request to anywhere but the API beside them.

| File | Holds |
|---|---|
| `ServiceAdminOptions.cs` | the switches |
| `AdminExtensions.cs` | `AddIncrementalAdmin`, `MapIncrementalAdmin`, `MapIncrementalHealth`, the health check, the embedded files and their headers |
| `AdminSecurity.cs` | the loopback rule, the mutation rule, the one error shape, the filter every endpoint runs under |
| `AdminApi.cs` | the endpoints and what they return |
| `AdminEvents.cs` | the server-sent event hub |
| `tests/Incremental.Service.AdminHost` | one or two nodes in a process on the in-memory backend with the UI mounted: three tables, eight pipelines (one that fails, one that dead-letters, one stopped, one of sixty operators), a generator. The API's tests run against it; `dotnet run --project tests/Incremental.Service.AdminHost` is how to look at the UI |

**What S3 added outside its package** (each the smallest change found; the first two are
the rest of C3 and C4):

| | Where | Change |
|---|---|---|
| C3, the graph half | `CircuitBuilder` | `Describe()`: `CircuitGraph` — every node (id, kind, state name, inputs, exchange, name, error policy), every output (stream, acknowledged, table) and every loop — read from the builder, before or after `Build()`, claiming nothing. The store names `Build()` would mint, and so the pre-stop state check, are still not built |
| C4 | `TableSnapshot` | `RowsBy<TIndexKey>(index, key)` |
| | `Incremental.Service` | `ServiceMetrics` (`ServiceNode.Metrics`), as designed in *Where the numbers come from* but for the service's own meter; `ITableRegistry.ReadRows`, `StatsAsync`, `Spec`; `TableStats.IndexKeyTypes`; `IPipelineSupervisor.DescribeAsync` (a dry run with its graph), `Graph(id)`, `RedactedSpec(id)`, `DeadLettersBefore`; `PipelineStatus.History` (the last 32 transitions on this node); `IPluginCatalog.RescanAsync` and `Manifest`; `ServiceNode.Options` |

**Departures from the text above.**

1. **Names.** `AddIncrementalAdmin`, `MapIncrementalAdmin`, `MapIncrementalHealth` (the
   step was asked for under these names), not `…ServiceAdmin` / `…ServiceHealth`.
2. **Every list is paged**, not only bounded: `skip` and `limit` on tables, pipelines and
   plugins (`{ total, skip, limit, next, items }`), `before`/`after` and `limit` on the
   sequence-numbered rings (events, audit, dead letters), and `limit` is clamped to
   `MaxRows` whatever is asked. A row read adds `more` beside `truncated`.
3. **The event feed** has, beside the bounded queue (`EventQueueLength`, 64, oldest
   dropped), a cap on streams (`MaxEventClients`, 32; one more is 503) and a write timeout
   (`EventWriteTimeout`, 10 s) after which the connection is aborted — a queue that drops
   bounds the memory, but a client that never reads would otherwise hold a connection for
   ever. A client whose queue dropped is told so (`event: lagged`) before the next frame,
   and the UI reloads its view. The `metrics` tick carries the node's role, health and
   counts beside every pipeline's rates, so the header needs no polling.
4. **With row data off, events and a pipeline's history keep only what stands before the
   first colon** of their message. The service writes a fault's message into the event
   that reports it (`Failed: 4 faults …: FormatException: …`), so the design's rule —
   messages are row data — has to reach there too. Found by the test that greps every
   response for the offending value.
5. **The graph** is drawn from `PipelineGraph` (the core's `CircuitGraph` plus which output
   is which sink slot). Dead letters are counted per node from the ring, an operator's
   name in a dead letter being its node's state name. A node that has never run the
   definition — a standby — returns the declared edges instead (`"source": "declared"`):
   input slots, tables and sinks around one box. There are no dead-letter counts on
   operators from the meter, only from the ring.
6. **`ServiceMetrics` counts a circuit only on the node that runs it.** Two nodes in one
   process hear each other's measurements; the listener asks the supervisor whether this
   node runs, or is starting, that pipeline. Its `Incremental.Service` meter for
   collectors is not built; nor are queue-depth and watermark-lag gauges in the rings, or
   the log writer's counters (`KafkaServiceBackend.Log` has them).
7. **`AdvertisedAdminUrl` is not filled from the server's addresses.** A first leader
   writes `node/leader` inside its `StartAsync`, before the web server has bound. The host
   sets it.
8. **Validate is a mutation** as far as the switches go (`PipelinePolicy`, refused when
   read-only): it runs a definition's `Build`.
9. **`ServiceAdminOptions.UiDirectory`** (not in the design) serves the UI's files from a
   directory instead of the assembly, for working on them without rebuilding.
10. **Not built:** the deploy form generated from a settings schema (the dialog is the
    spec as JSON, with a template per definition and a dry run before the button unlocks);
    a table declaration form (the API has `POST /tables`); `acceptSchemaChange` (there is
    no schema-change detection to accept); the "time until cut loose" warning event; the
    UI in a Kafka-backed browser pass — the API's Kafka-only fields (`leadership.kafka`,
    heartbeat age, lease, the leadership's own takeover timings) are rendered when present
    and were exercised only as far as the in-memory backend produces them, which is not
    at all. That pass is still owed.

**Tests** (`tests/Incremental.Service.Tests`: `AdminApiTests`, 18, and `SeamTests`, 5, in
memory; `tests/Incremental.Tests/DescribeTests`, 3). The API tests run a real Kestrel on a
free loopback port, because the loopback rule reads the connection's address: bounds and
paging of every list and of row reads; the error shape of every kind of refusal; the
loopback rule (forwarded headers, host names, `AllowedHosts`) and health outside it; an
authorization policy for everything and one per area, with a header-driven test scheme;
read-only refusing each of fifteen mutations and changing nothing; row data off (rows 403,
messages, records and stacks withheld, and no response quoting the bad value); mutations
(header, `confirm`, `If-Match` and 412, dry runs that do nothing, `state-choice-required`,
deploy, validate, remove, audit); upload validation (switch, media type, empty, not a zip,
too large, an entry named `../…`, no assembly, not an assembly, then a real plugin stored
and deleted); the feed (ticks, events with ids, replay from `Last-Event-ID` without
duplicates, a socket that never reads costing sixteen frames and then its connection, the
stream cap, the feed ending when the application stops); two nodes (a standby's reads, its
declared graph, `not-leader` with the leader's address, takeover through the API, the old
leader `Fenced` and not ready); and the UI smoke — every file the page names is served
with its type, a content security policy without `unsafe-inline` and a working ETag, no
script or stylesheet names another origin, and every API path the scripts build is a route
the server maps with that method, the read ones among them answering 200.

Mutation checks, each a one-line break run against the test that should notice: 41, of
which 40 killed — among them the forwarded-header and host checks off, `AllowedHosts`
ignored, read-only off, the mutation header and `confirm` not required, `If-Match`
ignored, `limit` not clamped, the row gate, the message gate and the event redaction off,
a client queue a thousand times longer, no write timeout, no cap on streams, no replay, a
replay that duplicates, a feed that outlives the application, upload switched on
regardless, an area policy and the general one not applied, a refusal without the
leader's address, a route renamed under the UI, no content security policy, no ETag, a
second member in the error shape, dead letters not counted on the graph, a standby
counting the leader's circuits, a removed pipeline's rings kept, a ring that forgets, a
scan with no bound, a secret shown, dead letters paged from the wrong end, a sink slot
not named on the graph (each of the three kinds), no history, `RowsBy` reading the latest
version, and `Describe` misreporting an output's kind or a node's error policy. Two
survived their first run and showed thin tests: nothing told apart the three sink-slot
overloads (the test now describes one pipeline of each), and nothing sent an upload with
no declared length. The second still survives and is equivalent: with the handler's own
count removed, the server's request-size limit — which the handler sets to the same
number — refuses the body instead.

**Seen in a browser** (Chromium through Playwright, against the admin host): every view in
light and dark at 1440 px and at 390 px, with no console error, no content-security-policy
violation and no sideways page scroll in any of them; stop, start, restart, reset (the
button stays disabled until the id is typed), replace with the state choice, plugin upload
with a refused file and an accepted one, takeover on a two-node host and the fenced node's
page afterwards; the feed cut by stopping the host (the banner, the dimmed page, recovery
when it returns); read-only and row-data-off hosts (no action anywhere; the notices that
say what is withheld); focus kept in a filter field across live updates; nodes of the
topology reachable and selectable by keyboard. Screenshots are in `docs/images/admin/`.

**Found while building.**

- *Kestrel aborts a connection whose write is cancelled*, which cancels the request, so
  "did the write time out" cannot be read from the request's token afterwards; the write
  timeout has its own clock.
- *A response under a strict style policy cannot carry `style` attributes*, and a DOM
  patcher that copies attributes sets one. The UI's patcher writes `style` through the
  CSSOM instead; the browser pass caught it as two console errors.
- *Keeping state across a change of worker count faults the new generation.* The replace
  dialog offers `keep` for it, the dry run accepts it, and the pipeline then goes to
  `Backoff`: state is sharded by worker count. The design's pre-stop check (C3's store
  names) would refuse it; until then the plan's line "state kept" is not a promise.

**What S4 must know.**

- Mount with `services.AddIncrementalAdmin(o => …)` and `app.MapIncrementalAdmin("/admin")`,
  `app.MapIncrementalHealth("/health")`. The example should set `ExposeRowData`,
  `AllowPluginUpload` and `AdvertisedAdminUrl`, and for `--nodes 2` give each node its own
  port and `ExitOnLeadershipLoss = false` (`tests/Incremental.Service.AdminHost/DemoHost.cs`
  does all of this and is small enough to copy from).
- A `memory` log keeps every record: a generator that runs for hours should upsert over a
  bounded key space into keyed inputs, as the admin host's does.
- The UI shows `Unbuilt` pipelines and declared-edge graphs on a standby; that is the
  alternative of *Open for James* 1 made visible, not a fault.

### S4 — the examples and the guide

Built: `examples/Incremental.TradeEnrichment` on shared tables,
`examples/Incremental.EnrichmentService` (a host, a contract assembly, a plugin in two
versions), [`docs/enrichment-service.md`](../enrichment-service.md), and
`DocSnippetTests`. No change to the core, to `Incremental.Kafka`, to `Incremental.Service`
or to `Incremental.Service.Admin`: the examples found nothing in them that had to move.

**Departures from *Reference examples*.**

- *(a) is a fixed narrative, not the interactive picker.* The design kept the column
  prompt, `--ticks` and `--blotter`. The example now prints seven acts and twelve claims,
  each checked against a model kept beside the circuits, and exits 1 on a wrong one;
  `--trades N` sizes it. The Spectre.Console dependency went with the prompt. Which
  columns are looked up and which joined is fixed in the source: the symbol is a
  `Lookup`, desk (through the derived `trader-desks`) and rating are `Join`s.
- *(b) has four projects, not five.* `Pipelines.Tests` is not built, because
  `PipelineTestHarness` is not. The definitions are tested by the smoke test instead,
  end to end.
- *The host project is in the example's root directory*, with the contract and plugin
  projects beneath it (`DefaultItemExcludes` keeps their files out of it), so that
  `dotnet run --project examples/Incremental.EnrichmentService` works. The plugin is
  copied to `plugins/enrichment.pipelines/` beside the host — one package directory,
  not `plugins/v1/` — and version 2 is a zip in `plugin-packages/`, because a plugin
  directory holding both versions would import both at first lead.
- *The trade record is in the contract assembly.* The host's source connector and the
  plugin have to agree on it; the design put only table rows there.
- *Sinks in memory are a `view` connector written in the host*, not `memory`. A `memory`
  sink is typed by the plugin's row type, which the host cannot name and which differs
  between two versions of a plugin; the `view` sink keeps each key's row as JSON, which
  is also what a Kafka sink topic holds, so one model checks both modes.
- *No `file` dead-letter slot and no `file` sink* (not built in the service).
- *Counterparties are loaded by the `file` loader in both modes*; the generator rewrites
  the file. In the Kafka mode instruments, traders and desks all come from Postgres
  through one Debezium connector, not instruments alone.
- *The two custom table sources load different tables* (instruments with positions,
  traders and desks by snapshot) rather than one source in two forms.
- *`--kafka` takes `--node a|b`*, which fixes the admin and probe ports; there is no
  `--peer`. `--prefix` names the topics and the Postgres tables, and `--clean` removes
  them.
- *The smoke test drives the admin HTTP API*, not the node's objects, in every mode, so
  it also covers the API's refusals (invalid settings 400, `state-choice-required` 409,
  `confirmation-required` 400, a mutation on a standby). With `--kafka` node a is the
  child process and node b is in the test's process: the leader is the one killed.
- *CI does not run the smokes.* The workflow runs no example, and this step was told to
  follow it. The examples are built by the solution build, which is what the guide's
  quoted code relies on.

**The guide's code is the example's.** Each C# block in `docs/enrichment-service.md`
and in the example's README follows a `<!-- snippet: path#name -->` comment and is the
region of that file between `// <snippet:name>` and `// </snippet:name>`.
`DocSnippetTests` compares them and refuses a C# block with no source. The MSK
configuration is such a region too (`MskConfiguration.UseMsk`): compiled, never called.

**Found while building.**

- *A `Lookup` downstream of a retroactive `Join` looks again when the join re-emits.*
  The first draft of `trade-enrichment` joined instruments and then looked the
  counterparty up; a corrected multiplier then refreshed the counterparty too. The
  lookup now comes first. The guide says so.
- *`keep` across versions recomputes nothing*, so a sink holds rows of both versions'
  shapes until something touches them. The model tracks which version last emitted each
  row; the smoke test's claim is that the untouched ones are exactly as version 1 left
  them.
- *A reset does not un-write a Kafka sink.* After `desk-exposure` is reset to group by
  region, its topic still holds every per-desk row. The Kafka smoke asserts exactly
  that union. An operator has to delete the topic or tombstone the keys.
- *A windowed aggregate does not retract a window's row when the window is evicted*: the
  sink keeps closed windows' totals, which is what the model expects.
- *A process started in the background of a non-interactive shell ignores SIGINT*; the
  host stops on SIGTERM as well (the generic host's lifetime), which is what a supervisor
  sends.

**Measured** (local stack, Debug, five tables, four pipelines, `TakeoverAfter` 3 s):
`kill -9` of the leader to the standby answering `/health/ready` as Leader, 3.5 s
(silence 3.0 s, fence 152 ms, tables 32 ms, pipelines 189 ms, start 237 ms; takeover
total 444 ms); in the Kafka smoke, 3.1 s from the kill to ready. A graceful stop of the
leader (SIGTERM) to the other node Leader: 0.9 s. The whole Kafka smoke, including
preparing Postgres, the connector and the topics and removing them: 18 s.

**Tests.** `DocSnippetTests` (five cases) in `tests/Incremental.Service.Tests`, which is
now 75 tests. The smokes are the examples' own: TradeEnrichment checks 12 claims; the
service example 30 on one node, 38 with an in-memory takeover, 37 on Kafka with a kill.
Each was run to exit 0 — the in-memory forms twenty times over without a failure, the
Kafka form three times.

**Mutation checks.** 23: five in TradeEnrichment (a computed column, the derived
table's join, the lookup read as a join, a cancellation not sent, the aggregate's key),
fifteen in the service example (each definition's joins, filter, window and version-2
change; the source connector's resume; the two table sources; the view sink's deletes
and reset), three on the snippet test. 21 were killed at once. Two survived and showed
thin claims: an append source that replayed from offset 0 changed nothing, because the
core drops records at or below an input's committed offset, and a table source that
stamped no positions changed nothing, because its replay collapses into a commit that
changes no row. The takeover act now checks the records read by both pipelines, and the
reference-data act checks the table's committed offset against the feed; both mutants
are killed. The script is in the session's scratch directory, not the repository.

**Run by hand** against the local stack, besides the smokes: the default mode (the UI,
`/views`, a deploy by `curl` as the guide gives it, a clean stop on SIGTERM);
`--nodes 2`; and the two-process walk — `--kafka --node a`, `--kafka --node b`,
`kill -9` of a (b Leader 3.5 s later), a restarted as the standby, SIGTERM to b (a
Leader 0.9 s later), `--kafka --clean`. The UI was looked at in Chromium on both
backends with no console error; that includes the Kafka-only fields (heartbeat age, the
lease, the last takeover's stages), which S3 had not been able to see. The UI's upload
and replace dialogs were not driven again here; the smoke does the same through the API.

**Not done, still open from S0–S3.** The rest of C1 and C3 (warm circuits on a standby,
the pre-stop state check), C7, `PipelineTestHarness`, `file` connectors, the
`deadLetters` slot, contract schema-change detection, the 80% retention warning, a
deploy form generated from the settings schema. Nothing has been run against MSK; the
guide lists what that leaves unverified.

### S5 — the review's findings

A review of S1–S4 confirmed 47 findings. This step fixes them. Each was reproduced with
a failing test where that was feasible, fixed, and the test kept; the UI defects were
reproduced and re-verified in Chromium. What changed in behaviour is below, by area;
the departures from the design above are marked as such.

**The supervisor: one action at a time on one pipeline, for real.**

- *The queue is checked after it is taken.* Every action looked its runtime up, waited
  for the queue and then carried on, whatever had happened meanwhile. A dry run or a
  refused deploy dropped the placeholder of a new id while the real deploy waited on it,
  and the real deploy then committed and started a pipeline no dictionary held; an
  action queued behind a removal wrote the removed pipeline back into the catalog. Now
  a deploy takes the runtime again until the one it holds is the one the supervisor
  holds, the placeholder is dropped under the queue, and every other action checks, once
  it has the queue, that the runtime is still held, still exists and that the node still
  runs pipelines (`EnterExistingAsync`).
- *A reset or a removal is finished, by whoever is there.* Their second halves ran on
  the caller's token with nothing catching a failure, so a client that disconnected or
  a sink store that was briefly unreachable left the pipeline in `Stopping` for good
  with the act pending; `start` and `restart` then ran it around the pending act, and so
  did the backoff retry after a failed finish at takeover — a pipeline pending removal
  came back to life. Now nothing after the intent commit takes the caller's token, and
  every path that can start a generation goes through `ReconcileAsync`, which finishes
  the pending act first. A failure to finish is `Backoff` with the reason
  (`reset not finished: …`), retried with the restart policy, and ends in `Failed` like
  any other; `restart`, or the reset or removal sent again, retries it at once.
  **Departure:** a reset or removal whose finish fails now returns its plan rather than
  throwing — the intent is recorded and the act will be finished — and the pipeline's
  state says what is outstanding.
- *A cancellation is the pipeline's unless it is the takeover's.* An
  `OperationCanceledException` out of a sink connector's `ResetAsync` (an HTTP timeout
  is one) escaped the takeover, fenced the node and did the same to its successor.
  Only a cancellation of the takeover's own token now leaves the takeover.
- *A connector's own calls are bounded.* `IAppendSourceConnector.StartAsync`,
  `IKeyedSourceConnector.StartAsync` and `IResettableSinkConnector.ResetAsync` run off
  the supervisor's thread and get `ServiceOptions.ConnectorTimeout` (30 s, new). Past it
  the generation is torn down and the pipeline faults; a source that starts late is
  disposed. A takeover waits for every pipeline's start, so this bounds it.
- *A start can outlast a takeover.* `AbandonAllAsync` looked at `Current`, which is set
  only when a start has finished, so a generation that was starting when the node was
  fenced was published afterwards and ran on a node that no longer led. The supervisor
  now has a flag for "this node runs pipelines" (set at takeover, cleared by
  `StopAllAsync` and `AbandonAllAsync`); a start asks again before it publishes its
  generation and tears it down instead; abandoning cancels a start in flight and takes
  each pipeline's queue (bounded by `StopTimeout`) before it tears a generation down,
  like every other teardown. `DisposeAsync` waits for that before it disposes what the
  circuits stand on.
- *Naming a thing and removing it share a lock* (`ServiceContext.LockReferencesAsync`).
  A table removal and a plugin delete checked their users and then wrote, with nothing
  between them and a deploy that had already resolved the table or the version. A
  deploy now checks its tables, the one-writer rule and its plugin version again under
  that lock and commits there; a table removal and a plugin delete hold it from their
  check to their end. It is held for a check and a catalog commit, never while a
  pipeline starts or stops.
- *A catalog write looks at its token once*, before the first row reaches the table's
  writer. A commit cancelled while it waited left its rows staged in the writer, and
  the next commit of anything — the leader's once-a-second writer probe — published an
  action that had been reported as cancelled. `BeginAsync` also reads the table after
  it has the write lock, so a write queued behind a reopen gets the reopened table.
- *Seeds are recorded* (`node/seeds`, a `SeedRecord`): **departure from "if their key
  is absent"**, which a removal makes true again. An entry already there counts as
  applied, so a deployment older than the record gains it at its next takeover.
- *A secret sent back as `***` keeps the stored value* for the same slot and connector,
  and is refused where none is stored. The admin UI's *Edit spec…* seeds its editor with
  the redacted spec, and replacing a pipeline from it had stored the mark.
- *Readiness uses three samples.* A standby is `state-behind` only if it was behind at
  each of its last three one-second samples (`StateLagWindow`).
- Durations in reasons and plans are written for a reader (`26 min 31 s`), and hold no
  colon: what follows the first colon of a reason is hidden where row data is, and a
  `TimeSpan` put one inside the service's own text.
- A node that no longer leads logs one warning; a pipeline that ends because of it is
  logged as that, not as a fault with a stack, and its circuit's own errors are written
  at Debug from the moment its outputs are abandoned on purpose (`GenerationLogs`).

**Plugins.**

- `PluginManifest` gains `AssemblyName` and `Sequence` (both optional when read).
  `MainAssembly` is now the main assembly's *file* within the package, as the design
  said, where it had been the assembly's name with `.dll`: a package whose main
  assembly was named or placed otherwise imported and could never load. `Sequence` is
  the audit sequence of the import and orders the versions of one contract: a starting
  process loads the one imported last (the order had been that of the catalog's rows),
  and the other reads `superseded`.
- *Which references go to the host* is now the rule the design gives, with one addition
  (**departure**): an assembly the host merely carries is shared only when the host's
  version satisfies the reference or the plugin ships none; a plugin built against a
  newer one, and shipping it, gets its own copy. Before, anything the host carried or
  had loaded was the host's, whatever its version. Contracts are tried first.
- A plugin context that resolved a name to its own copy before a contract of that name
  arrived is retired when the contract loads: no new use gets it, and it unloads with
  its last. Pipelines already on it keep their copy until restarted, and the log says so.
- Delete takes the import lock and the references lock; it refuses a contract while a
  declared table's definition is in it and in no other stored version; it looks at
  whether the node still leads before each blob it deletes. Importing a stored version
  again puts back any blob the store has lost. **Not closed:** the blob store's writes
  are unfenced, so a leader replaced between that look and the delete can still delete
  a blob its successor has stored again; the repair is the re-import. Fencing blob
  deletes would mean writing them through the log's transactional producer, which the
  blob topic's "plain idempotent producer" rule excludes; it is left as a known limit
  and is in the guide.
- A zip from a stream that cannot seek is spooled to a file in the cache directory and
  refused past `MaxPluginBytes` before `ZipArchive` sees it (it would have buffered the
  whole stream in memory first).
- The cache directory: the default one is the process's and is deleted at dispose; a
  starting node deletes those of processes that no longer run, and import leftovers. A
  configured one is pruned of versions the catalog no longer holds, at takeover and
  whenever a standby applies a catalog version; a delete removes the version's files.
  Deleting a directory is best effort (`IOException`, `UnauthorizedAccessException`),
  which is also what keeps a zip import on Windows from failing after its manifest is
  committed. That last part is by reading: nothing was run on Windows.
- The fingerprint also covers enum members and values and anonymous types' members,
  and the text shown says what it is: `declared data shapes identical; shapes used only
  inside delegates are not compared`.
- A definition's constructor exception is reported as itself, not as
  `TargetInvocationException`.
- A plugin project copies its NuGet dependencies (`CopyLocalLockFileAssemblies=true`)
  and excludes the host's with `ExcludeAssets="runtime;native"`: with `runtime` alone the
  Kafka client's native libraries, about 100 MB, were copied as soon as copy-local was
  on, and with copy-local off — what the guide prescribed — a plugin's own package
  dependencies were not in the package at all. `Acme.Desks` now has one (LZ4) and calls
  into it.

**Admin.**

- Row reads: a scan takes `from` and answers `next`; rows passed over are not counted
  towards `MaxScannedRows` (they had been, so nothing past the bound could be browsed
  and *Next* stayed enabled on empty pages); a search that stops at its bound says where,
  and the UI offers *Keep scanning*. An index read is paged with `skip`, and reads only
  the page.
- The loopback rule also refuses `X-Real-IP`, `Via`, `X-Forwarded-Proto/-Port/-Server`
  and HTTP/1.0, which is what nginx speaks to its upstream unless told otherwise. **It
  remains a default, not authentication:** a proxy that rewrites the host, adds no
  header and speaks HTTP/1.1 cannot be told from a local client, the option's
  documentation and the guide now say so, and mounting the API without a policy logs it.
- A node that no longer leads names the node that does, from its probe (`PeerInfo`
  gains `AdminUrl`; the Kafka probe's detail token carries it as `;admin=…`), since its
  own catalog still names itself. Nodes are probed in every role.
- The event hub no longer measures the state lag: nothing read it.
- UI: the nav shows the total and, beside it, how many need attention; a standby's
  overview and dead-letter pages say that pipelines and dropped records are the
  leader's to report; a refused feed says the node is serving its limit and retries
  every five seconds; the replace dialog refuses another id; an unknown sort key sorts
  by state; the lookup button enables as a key is typed. Screenshots retaken.

**Changes outside the service** (the smallest that would do):

- Core: `TableSnapshot.RowsBy(index, key, skip, take, out total)` — a page of an index
  read. The existing overload materialises every row under the key.
- `Incremental.Kafka`: `KafkaLeadership.StepDownCoreAsync` closes the writer and ends
  `Fenced` when the resignation cannot be committed for a reason other than fencing. It
  had left the role `Leader` with the writer heartbeating for a node whose circuits
  were stopped. **Not tested:** it needs a resignation transaction aborted past the
  retry limit, which nothing here can provoke; the existing step-down tests pass.
- `Incremental.Kafka`: `KafkaConnectClient.ResetOffsetsAsync` (stop the connector,
  delete its offsets). The example's `--clean` calls it: Connect keeps a deleted
  connector's offsets, and the next connector of that name took no snapshot.
- Example: the MSK snippet sets the token refresh on the admin client too; the `View`
  sink's order is by key held, not by insertion ever made.

**Tests.** `LifecycleTests` (new, 19), eleven more in `PluginTests`, more in `SeamTests`
and `AdminApiTests`, one in `DebeziumCdcTests` (a connector removed with its offsets and
created again under its name snapshots again — it times out without the reset), one in
the core's `DescribeTests`. The races are run in rounds (a dry run against a first
deploy 200 times, an action against a removal 60, a table removal against a deploy 150).
`Incremental.Service.Tests` is 104 tests.

**Mutation checks.** 34, all killed: the 33 listed in the scratch scripts (each
re-check under the queue; the retry as a plain start, on a leader and at takeover; a
tenant cancellation escaping the takeover; a start published without asking who leads;
a source start with no bound; the catalog commit cancellable after staging; seeds by
absent key; secrets not restored; the eviction after unload; the references re-checked
under the lock, on both sides; the manifest's file; rows counted against the bound;
index skip; a contract deleted from under its table; the blob repair; the unwrapped
cause; the two cache deletions; the contract order; the host-version rule; the retired
context; enum members in the fingerprint; the core's page), and the connector test
without its reset. A mutation script that restores a file must touch it: the restored
file is older than the binary the mutant produced, and the next build kept the mutant —
which is how a stale core assembly was found during the browser pass.

**Not done.** The step-down change has no test of its own. Blob deletes are still
unfenced (above). Nothing was run on Windows or against MSK. The fingerprint still
cannot see a tuple or a private dependency's type used only inside a delegate.

### S6 — round two

A completeness review of S1–S5 listed eighteen gaps (G1–G18). Round two closes them in
several steps; each records its own part here.

#### Service robustness: G2, G3, G9, G11 and the API half of G7

**A mistaken `keep` no longer takes a pipeline down (G2).** The reviewer's case —
`circuit.workers` 1 → 2 with `state=keep` answered 200, then four failed starts and
`Failed` — is now refused at the dry run and at the call with `409 state-incompatible`
and the core's own finding (`workers changed: the checkpoint was committed with
workers=1, this circuit has workers=2`), before anything stops. Three parts:

- *The core says what a circuit's state is without building it.*
  `CircuitBuilder.DescribeState()` runs every node's operator factory against a recording
  provider and returns each store with its name, kind and key and value types, for every
  worker. Nothing outside the builder is touched: no table is claimed, no storage read,
  and the operators are dropped. `CircuitBuilder.CheckState(provider)` builds the
  manifest from that and compares it with the committed one — the very comparison
  `Circuit.Start` makes (the two now share `StateManifest`), so "refused beforehand" and
  "refused at start" cannot drift apart. A test asserts the described manifest equals the
  committed one for one and three workers.
- *The refusal at start is typed.* `Circuit.Start` throws `CircuitStateMismatchException`
  (an `InvalidOperationException`, so existing catches hold) with `Problems`; this is the
  design's C7. `ChangelogStorageProvider` throws the same when a store's committed bytes
  cannot be read as the types the circuit declares for it — the case no comparison of
  layouts can see. A record this version cannot read at all (an unknown format byte)
  stays an `InvalidDataException`: that is not a mismatch of types.
- *Types that have committed nothing have no say in what the state is.* The design's
  "bytes are never round-tripped through types that have not committed" was not true as
  built: `ReleaseStores` wrote every store back through its current types, so a version
  that was built on the state and refused to start would have dropped the members it did
  not model. A store created from held bytes now keeps those bytes until the provider's
  next commit (or until a record is applied to it, on a follower), and `ReleaseStores`
  gives them back as they were. Cost: the state's bytes are held twice between a build
  and its first commit. On the leader that is after any start on existing state (a
  cold start, a restart, a takeover that built, a keep) and for as long as the
  pipeline's inputs are quiet, since a commit needs a step; nothing bounds that time. A
  follower drops the copy as soon as the leader has checkpointed under the same layout,
  and keeps none when it builds after that. *Open for James* 37.

On these the supervisor does what the design's replace steps 2 and 7 describe, in the
form the as-built standby allows (it holds bytes and builds nothing, so there is no
layout marker and no `confirmed` generation to keep followers in step):

1. A replace that keeps state — chosen, or implied because nothing touching state
   changed — is dry-run with `CheckState` against the pipeline's provider. Problems are
   `state-incompatible`, with `DryRun` too. `state-choice-required` already carries
   `keep is not possible: …` when that is so.
2. The intent commit records `Previous`: the spec being replaced (or *its* `Previous`,
   if it never ran — the state is then still its predecessor's).
3. When the new generation reaches `Running`, `Previous` is cleared
   (`pipeline.replace.confirmed` in the audit; the entry's generation does not move,
   because nothing a caller sent changed).
4. If instead the build or `Circuit.Start` throws `CircuitStateMismatchException`: the
   generation is torn down, the binding is released (the stores go back to their bytes),
   `Previous` is committed as the spec under `generation + 1` with the audit action
   `pipeline.replace.rolled-back` and the problems as its plan, and the pipeline is
   reconciled — started, if the old spec was running. `DeployAsync` then throws
   `state-incompatible` saying the change was rolled back and what state the pipeline is
   in; the event ring gets `rolled back to the spec of generation N: …` (the fact before
   the colon, so it survives `ExposeRowData` off); `PipelineStatus.RolledBack` holds the
   record until the next deploy; `incremental.service.pipeline.rollbacks` counts it.
5. A leader that dies after 2 leaves `Previous` in the catalog. Its successor starts the
   new spec, meets the same mismatch — it is a property of the state, not of the node —
   and rolls back itself. A leader that dies after 4's commit leaves the old spec. So a
   takeover anywhere in a replace ends in one of the two specs, and for a change that
   does not fit, in the old one.
6. A mismatch with no `Previous` is `NeedsReset` at once: one Error line without a
   stack, no retries, and a reason that names what the service can do about it ("deploy
   the spec that wrote the state with state=keep, or reset the pipeline") rather than
   the core's advice about Redis key prefixes.

*Departures.* The roll-back is one commit, not the design's two (`generation + 2` and a
marker): there is no marker to write. A rolled-back replace answers 409 although the
catalog changed (*Open for James* 13). A plugin version that an unconfirmed replace
would go back to cannot be deleted. `DeployResult.Spec` is now the redacted spec — it
returned stored secrets to a caller who had sent `***`.

*Not closed.* Roll-back covers the first start only. A generation that starts, commits
and then faults on what it reads has rewritten the entries it touched, and there is no
going back from that; it is an ordinary fault. Meaning is not checked at all: same
stores, same types, another computation (`desk-exposure` v2) keeps without complaint.

**The in-memory backend is bounded (G3).** The reviewer measured 2.88 M state records
after 73 minutes at 50 trades/s, rising 41,000 a minute, RSS 1.8 GB.

- `InMemoryStateChangelog(partitions, compactAbove)`: a partition that reaches its
  threshold is compacted inside the commit that took it there — last record per key —
  and the threshold moves to twice what survived, never below `compactAbove`. The work is
  amortised over the appends; the log holds at most about twice the live records plus
  `compactAbove` per partition. Tombstones are the delicate part: a reader that had seen
  a record and not yet its tombstone would keep the record for good. Every enumeration of
  `ReadAsync` registers where it stands, and a tombstone is dropped only below the
  lowest of them; with no reader, all go (a later reader starts from 0 and never sees the
  record). This protects a reader for as long as it enumerates, which is how
  `ChangelogStorage` follows; a reader that stopped and resumed from a remembered offset
  would not be protected, and the class says so. Off by default in the core
  (`compactAbove: 0`), on in the service (`InMemoryBackendOptions.StateCompactAbove`,
  8,192).
- `InMemoryTableLog(maxRetainedVersions, maxRetainedChanges)`: history was capped by
  versions only, and one version of a first load can be the whole table. It is now also
  capped by row changes (`RetainedHistoryChanges`, one million), down to the newest
  version, which is always kept so that a follower that is up to date is not cut loose by
  the append it is waiting for.
- The example's `PositionedFeed` was reported as keeping every trade. It does not: a
  record is a function of its position and nothing is buffered. The `View` sink was
  already capped. `MemoryLog<T>` — the memory connector's log, which the admin host's
  generator feeds — did keep everything, by design, so that a test can replay from 0;
  it has `RetainLast`, and the admin host sets 100,000. A source that asks for a record
  already dropped is given the oldest one held, under its true offset.

Measured, example in Release from the built binary, one in-memory node, sampled every
30 s (`stateRecords` is what the log holds, from `GET node`):

| `--rate 5000`, 10 min | 30 s | 90 s | 210 s | 330 s | 451 s | 571 s |
|---|---|---|---|---|---|---|
| RSS, MB | 583 | 560 | 586 | 632 | 653 | 577 |
| state records held | 197,552 | 212,688 | 226,703 | 258,738 | 278,495 | 225,027 |
| appended so far | 1.7 M | 5.5 M | 13.0 M | 20.4 M | 27.9 M | 35.4 M |

| default rate (50/s), 30 min | 2 min | 6 min | 10 min | 14 min | 18 min | 22 min | 26 min | 29.5 min |
|---|---|---|---|---|---|---|---|---|
| RSS, MB | 202 | 292 | 381 | 576 | 667 | 686 | 691 | 684 |
| state records held | 49,804 | 131,167 | 193,479 | 206,350 | 211,722 | 225,171 | 238,646 | 223,180 |
| appended so far | 51,557 | 154,323 | 308,834 | 474,099 | 639,911 | 805,731 | 971,585 | 1,116,620 |

At 5,000/s the 20,000 trade ids are all live within seconds and RSS stays between 537
and 653 MB over the ten minutes. At the default rate the ids take 400 s to fill, the
records held are flat from then (190–260 thousand, the sawtooth of compaction), and RSS
goes on rising to about 690 MB until minute 18 and is 683–696 MB for the last twelve
minutes: the same level the fast run sits at from the start. That late rise with a flat
log is the server GC's heap budget being used, not something being kept — the example
runs server GC with DATAS off (`examples/Directory.Build.props`). Both runs stopped on
SIGTERM with exit code 0.

**The evidence for keep or reset is the state's own shape (G9).** The fingerprint hashed
every public type in the plugin assembly, so the example's own v1 → v2 walkthrough —
which adds a column to a sink row and tells the reader to choose `keep` — showed
`declared data shapes differ`. `StateShape` replaces it as evidence: the stores
`DescribeState` reports for the deployed spec (as last built here, or described on
demand) and for the proposed one, each store's key and value types, and every type those
are made of with its members as the state serializer sees them — public properties and
fields, an enum's members with their numbers, tuples and collections by their arguments.
Table key and row types are in it too. It is text only, so it outlives the load context
it was read from. `StateShapes.Differences` names what differs: a store that is new
(starts empty), gone (keep is refused), or of another type; and per changed type the
members added, dropped or retyped, the stores that hold it, and whether it *identifies*
entries there (a key, or the value of a weight or indexed store: an old entry is not
found again — not safe) or is a *value* (a missing member reads as its default). Equal
shapes read `state shapes: unchanged — the N state store(s) … (types)`. The example's
walkthrough now reads exactly that, the smoke test claims it, and the guide quotes it.
The old caveat — a shape used only inside a delegate is not compared — is gone: whatever
a store holds is a generic argument of the store, tuples and anonymous types included.
`PluginManifest.Fingerprint` is still computed and stored (it is in a persisted record)
and no longer shown.

**A meter for a monitoring system (G11).** `ServiceMeter`, the meter
`Incremental.Service`: 22 instruments — the node's role, term, health, standby readiness
and state lag; the catalog version; takeovers and a histogram of their duration by
stage; pipelines by state, and per pipeline whether it is up, source lag, checkpoint age,
sink-ack wait, dead letters, faults, restarts and roll-backs; tables by state, and per
table rows, version, follower lag and loader lag. One static meter and a registry of
nodes, rather than a meter per node: exporters key a metric by meter and instrument
name, and two meters of one name would have to be merged by them. Tags are `node`,
`pipeline`, `table` and the closed sets `state`, `role`, `stage`. The gauges read a
snapshot: per-pipeline values come out of the one-second sample `ServiceMetrics` already
took (its inputs gained the state and the oldest sink write in flight), node values from
`Status`, table values from `StatsAsync` every five seconds off the sampling thread — a
collection walks arrays and touches nothing. `pipeline.up` is reported by the leader
only: a standby's pipelines are `Unbuilt`, and an alert on `up == 0` must not fire
there. The memory connector's sources now report lag.

The example exports both meters over OTLP with `--otlp` (snippet `monitoring`, quoted in
the guide). Checked end to end against a local HTTP sink: all sixteen `incremental.service.*`
instruments that have a value on a lone in-memory leader arrived at `/v1/metrics`, with
the core's ten.

**Discovery (G7, the API half).** `IServiceDiscovery` on `ServiceNode`, and five GET
routes on the admin API:

- `plugins/{name}/{version}/definitions/{definition}` — settings (now with default,
  enum values, `[Range]` bounds, nullability), input and sink slots with kind, record
  type and key type, and table slots with access, key and row type and definition.
  Slots are found by running `Build` against a `PipelineBuilder` in *detached* mode:
  tables are empty stand-ins made from the `TableDefinition` itself, indexes included,
  so nothing need be declared or bound. Settings are the ones given, else what an empty
  spec binds, else the settings class as its constructor leaves it (with a note).
  Leader only: it runs plugin code.
- `…/template?id=` — a spec with the real slot names. Each slot is bound to a connector
  registered for its kind, the host's own before `memory` and `kafka`, taking the first
  whose `Describe` accepts a config of just its name; a connector that needs more is
  offered with its required properties empty and a note. Table slots are left to bind by
  name where a declared table has the slot's name, and bound to the one declared table of
  the same definition where there is exactly one. The template is then dry-run:
  `valid`, `problems`.
- `connectors` — name, slot kinds, whether it resets, and its config schema if it
  implements `IConfigDescribed` (the built-in `memory` and `kafka` do, and the example's
  two; the `kafka` schema's `connection` lists the registered names).
- `connections` — Kafka connections and object stores, name and kind. The records have
  two members and a test asserts the store's directory is nowhere in the answer.
- `table-sources` — `file`, `kafka`, `debezium` with their config and the kind of
  connection each names, and the host's registered kinds (with a schema if the factory
  implements `IConfigDescribed`).

`SettingInfo` is part of the stored manifest; its five new members are optional, so a
manifest stored before them reads with them absent, and the definition endpoint
describes from the loaded assembly, not from the manifest. The UI half — a deploy form
built on these — is another step's.

**Public API added.**

- Core: `CircuitStateMismatchException`; `CircuitBuilder.DescribeState()`,
  `CheckState(ICheckpointStorageProvider)`; `CircuitStateStore`, `CircuitStateStoreKind`;
  `InMemoryStateChangelog(partitions, compactAbove)`, `.Compactions`;
  `InMemoryTableLog(maxRetainedVersions, maxRetainedChanges)`, `.RetainedChanges`.
- Service: `PipelineSpec.Previous`; `PipelineRollback`; `PipelineStatus.RolledBack`,
  `.ReplaceUnconfirmed`; `InMemoryBackendOptions.StateCompactAbove`,
  `.RetainedHistoryChanges`; `MemoryLog<T>.RetainLast`, `.Retained`,
  `MemoryConnector.RetainLast`; `ServiceMeter` (`Name`); `ServiceNode.Discovery`,
  `IServiceDiscovery`, `ConnectorInfo`, `ConnectionInfo`, `TableSourceInfo`,
  `DefinitionDescription`, `SpecTemplate`, `TableSlotShape`, `IConfigDescribed`,
  `ConfigSchema`, `ConfigProperty`; `SlotShape.KeyType`, `PipelineShape.TableSlots`;
  `SettingInfo.Default`, `.Values`, `.Minimum`, `.Maximum`, `.Nullable`.
- Admin API: the five routes above; the error code `state-incompatible`; `rolledBack`
  and `replaceUnconfirmed` on a pipeline.
- Example: `--otlp`.

**Tests.** Core: `InMemoryLogBoundsTests` (4: 4,000 commits over 300 keys with a
follower reading throughout — over 100,000 records appended, never more than 1,778
held (twice the live entries plus the thresholds), and the follower equal to the writer
at the end; a tombstone kept while a reader
stands before it and dropped once it has gone; no self-compaction without a threshold;
the table log's cap), two in `ChangelogReleaseTests` (bytes given back untouched before
a commit and not after; a store that cannot be read is typed and leaves the state as it
was), three in `StateManifestTests`. Seven existing tests asserted
`InvalidOperationException` exactly and now name the derived type. Service:
`ReplaceTests` (5: the reviewer's repro refused at the dry run and the call with the
pipeline untouched and its sink still matching the model; an unreadable `keep` rolled
back with the sink matching the model across it and the source resuming at its offset;
confirmation; a takeover between the mismatch and the roll-back's commit, ending with
the old spec running on the new leader; `NeedsReset` with no retries), `StateShapeTests`
(3), `MeterTests` (3, with a `MeterListener`: every gauge on a leader and on a standby,
the counters and the takeover histogram, the role moving at a takeover, and every tag
key in the allowed set), `MemoryLogTests` (2), three in `AdminApiTests` and more in its
standby test. `Incremental.Service.Tests` is 120 tests. The example's smoke test has two
more claims (the evidence reads `unchanged`; the template deploys as offered).

**Mutation checks.** 25, all killed (`scratchpad/r2/mutations.py`): the tombstone
horizon ignored; self-compaction never due; the change cap off, and the newest version
unprotected; origin bytes not kept; the decode failure untyped; `CheckState` comparing a
manifest with itself; `DescribeState` naming one worker; the pre-check not refusing;
`Previous` not recorded; the replace never confirmed; the mismatch not recognised; the
roll-back restoring the new spec; identity and value not told apart; the evidence
inverted; a standby reporting `pipeline.up`; restarts not counted; a takeover stage
misnamed; dead letters from the wrong source; the template's memory name; a table slot's
access; a setting's default; a definition described on a standby; the memory log never
trimming; `state-choice-required` not saying keep is closed.

#### Connectors, dead letters, the harness and the example: G4, G5, G6, G15

**The `files` connector (G4).** `FileConnector` (`FileConnectors.cs`) is registered as
the built-in connector `files` and implements all four slot kinds over a named
`IObjectStore`, so whatever store the host registers — a directory, the owner's S3 —
is what it reads and writes. Its config is a closed record (`store`, `prefix`, and for
sources `suffix` and `pollInterval`, for sinks `minInterval`); a prefix that is rooted
or climbs out is refused. `ObjectStores` is the registry of the host's stores, public
and resolvable from DI, as `KafkaConnections` is.

*The source* reads objects in ordinal name order, a record per line, and polls the
prefix for names above the last one read. It keeps one line and at most 1,024 names in
memory. Its position is an object name and a line, and the checkpoint has room for one
non-decreasing number per partition per input, each record stamped with one of them.
`FilePosition` fits the one into the other:

| partition | holds |
|---|---|
| 0 | `ordinal · 2³² + line`, stamped on every record of the object |
| 1 | `ordinal · 2³² + length` of the object's name in UTF-8 bytes |
| `2 + bank · 64 + i` | `ordinal · 2³² + ` bytes `4i…4i+3` of the name; `bank` is the ordinal's parity |

The ordinal counts the objects the source has moved to, so every number only rises,
which is what the input's replay filter requires. Moving to an object sends the name's
parts (`AdvanceAsync`), then partition 1, then the object's records, all through the
input's channel; a checkpoint can fall after any of them. Whatever prefix it holds,
partition 1 names an object whose name is whole — the two banks keep the last name
intact while the next is written — and partition 0 either carries that ordinal and a
line, or a lower one, meaning the object has not been started. A unit test walks every
prefix of the sends for three objects and decodes each. The position does not need the
object it names: a restart with the object deleted carries on with the names above it.
Names are limited to 256 bytes, and the checkpoint carries up to 2 + 2·64 entries for
the input, normally about a dozen. A keyed input takes every line as an upsert; a line
that is not a record goes through `RejectAsync` with a `FileRecord` (object, line,
text), and its message carries `object:line`, because the offset says nothing to a
person. `SourceStats.At` shows the position as `name:line`.

*The sinks* write one object per step with output, `{prefix}{step:D19}-{term:D6}.jsonl`,
through the store's write-then-replace stream. The same step writes the same name; the
first write of a generation deletes the step objects above `CommittedStep` — the core's
rule for an append sink that can drop. An append sink's lines are records; an upsert
sink's are `{"key":…,"row":…}` and `{"key":…,"deleted":true}`. `FileSinkFormat` is the
reader's half (names, `StepObjectsAsync`, `ReadAppendsAsync`, `ReadUpsertsAsync`): the
latest row of a key is the last line that names it in name order, and where a step was
written under two terms the higher term's object is the step. `minInterval` was added
when the example showed one object per step to be several small files a second: the
sink writes the object and then holds its acknowledgement for the interval, the commit
gate waits with it, and steps grow. The wait is after the write and not before it, so
that the write follows the lease check at once.

**The dead-letter sink (G4).** `PipelineSpec.DeadLetters` is a connector config for any
`IAppendSinkConnector`. With it set the supervisor builds the circuit with the core's
new `CircuitOptions.AcknowledgeDeadLetters`: the dead-letter port becomes an acknowledged
output that publishes one `DeadLetterBatch` per step and gates the commit on the
reader's acknowledgement, exactly as `ToAcknowledgedOutput` does. The supervisor
delivers each batch as `DeadLetterEnvelope`s (pipeline, spec generation, step, time,
operator, weight, exception type and message, the record as JSON text, the source
position for a record a source rejected, and every input's last committed position)
through `SinkDelivery.DeliverDeadLettersToAsync`, with the sink wrapped in the
leadership's lease like every sink, and fills the in-memory ring on the way. Without a
sink bound nothing changed: the ring, the count.

*A sink that will not take dead letters.* Decided: the pipeline stops and says why.
The write is retried (`ServiceOptions.SinkDelivery`, new, for every sink); then the
dead letters are abandoned, the circuit faults at its commit, and the fault — from the
circuit or from the delivery, whichever the watcher sees first — reads `The circuit's
dead letters could not be stored (IOException: …)`. The pipeline restarts from its
checkpoint under its restart policy, drops and delivers the records again, and ends
`Failed`. `PipelineStatus.DeadLetterSink` (`deadLetterSink` in the API) carries what
was delivered, the write in flight, the last error and the failed writes, and
`blockedOn` reads `sink-ack:deadLetters` while a write is stalled. The alternatives
were to drop to the ring and carry on (loses the one class of record with no other
trace, which is what the gap was) and to retry for ever (a pipeline that looks healthy
and has stopped).

**The harness (G6).** `Incremental.Service.Testing.PipelineTestHarness`, in the
`Incremental.Service` assembly. It goes through `DefinitionBinding` and
`PipelineBuilder` — the supervisor's own path — with a manual-stepping circuit over an
in-memory checkpoint store, so a step returns only when the sinks (the memory
connector's) and the dead-letter reader have acknowledged. Tables are `SharedTable`s
built from the definitions; a table the definition names and the test does not give is
an empty stand-in. Inputs are the core's handles. It references no test framework.

**The example (G5, G15).** Two definitions more:

- `execution-quality`: two append inputs with event time, joined on (instrument, quote
  interval) with a stream `LeftJoin` under `EvictWhen`, a remembered `Lookup` of the
  instrument (which the second review found keeping an entry per execution for ever; it
  is a `LookupAppendOnly` before the join now), a hopping window, and a linear aggregate
  the plugin defines. In memory its
  quotes are files under `quotes/` read by `files` (the generator writes one per minute
  of event time and deletes those older than thirty); on Kafka a second topic.
- `watchlist-alerts`: an append input, a computed column that throws on a trade with no
  quantity, `LookupAppendOnly`, `AppendSink` to files. Seeded twice — `sanctions-alerts`
  and `credit-alerts` — with the table slot `watchlist` bound to two tables declared
  from one `WatchlistTable`, under `errorPolicy: DeadLetter`, with a dead-letter sink
  (files in memory, a topic on Kafka).

The generator's `--poison N` (200 by default) gives one trade in N no quantity. Trade
prices are now within half a percent of a level per instrument, so that slippage
against a quote is a number worth reading. The smoke test's model covers both
definitions; its acts add the quality windows after every publish, the alert files
(every alert once, read with `FileSinkFormat`), the dead letters (files: once; topic:
at least once), an addition to a watchlist, a replace of `execution-quality` on kept
state with both inputs resuming, and all of it again after the takeover.
`Pipelines.Tests` is an xunit v3 project of eleven tests on the harness, in the
solution and in CI.

**Deviations, and what was found on the way.**

- `deadLetters` is a member of the spec, not an entry under `sinks`. The design's
  sketch had it as a slot; a definition's slot of that name would have collided, so the
  name is reserved (`p.AppendSink("deadLetters", …)` is refused) and a binding under
  `sinks` is refused with a message that says where it goes.
- The files sink writes one object per *step*, not per committed range of steps. A sink
  is told of batches, not of commits; `minInterval` is the knob for object size.
- The term is in the object name. Without it a replaced leader finishing a write late
  would overwrite the new leader's object for the same step.
- `LocalObjectStore.ListAsync` threw when a file was deleted while it listed; it skips
  it now. `FileSinkFormat` readers skip an object listed and then deleted. Both showed
  as a flaky test of the sink's clean-up.
- The first dead-letter delivery wrapped the circuit's own fault as "the dead-letter
  sink could not be written". `DeliverDeadLettersToAsync` now ends quietly when the
  output is completed by a fault and abandons only on a failed write.
- The files sinks first read `CommittedStep` at their first write. By then the new
  generation may have committed steps that gave the sink nothing, and an object an
  earlier generation had left for one of those steps passed for committed. The step is
  now read when the sink is made; a mutation that puts the late read back is killed by
  two tests.
- `ServiceContext.ObjectStore` resolves through `ObjectStores`; one cache, not two.
- `PipelineBuilder` exposes its stand-in tables internally and `InputSlot` carries its
  stream, for the harness; `SlotStart.Leadership` is nullable (no node in a harness).
- The keyed files source has upserts only. A delete line was not designed.
- Existing tests changed: three in `AdminApiTests` list connectors and now include
  `files`.

**Public API added.** Core: `CircuitOptions.AcknowledgeDeadLetters`,
`Circuit.DeadLetterBatches`, `Circuit.AbandonDeadLetters`, `DeadLetterBatch`,
`SinkDelivery.DeliverDeadLettersToAsync`. Service: `FileConnector`, `FileSinkFormat`,
`FileRecord`, `ObjectStores`; `PipelineSpec.DeadLetters` and `.DeadLetterSlot`,
`DeadLetterEnvelope`, `DeadLetterSinkStatus`, `PipelineStatus.DeadLetterSink`;
`SourceStats.At`; `ServiceOptions.SinkDelivery`;
`Incremental.Service.Testing.PipelineTestHarness` (and its `Builder`) and
`HarnessTable<TKey, TRow>`. Admin API: `deadLetterSink` on a pipeline, `at` on a
source, `deadLetters` in a spec. Example: `--poison`; the contract's `Quote`,
`WatchlistEntry`, `WatchlistTable`.

**Tests.** Core: `AcknowledgedDeadLetterTests` (5: the commit waits; a step that dropped
nothing does not; a batch read and not acknowledged before a crash is reported again
from the checkpoint; abandoning faults with the cause; the two readers are exclusive).
Service, 26 more, 146 in all: `FileConnectorTests` (13: the position at every prefix of
its sends; name order, new objects and a late name; a restart with the objects it stood
in deleted, reading exactly the new lines; a restart inside an object; a bad line as a
dead letter with `object:line`, not met again, and a fault under `Fault`; keyed
upserts; the append sink holding each record once across a restart with a stale object
above the checkpoint planted before each start; the upsert sink with deletes and a
reset; `minInterval`; two terms for one step; a takeover with objects landing
throughout; the closed config), `DeadLetterSinkTests` (5: every field of the envelope;
a write held by the sink keeps the checkpoint behind the record and `blockedOn` says
so, and a takeover in that state delivers it from the new leader; a failing sink ends
in `Failed` with the cause, nothing committed past the record, and the records
delivered once mended; the binding's checks and its secret redacted and restored; the
reserved slot name), one in `KafkaServiceTests` (a topic as the dead-letter sink, an
operator's drops and two records the source rejected with `topic[partition]@offset`,
across a takeover), `HarnessTests` (5) and `TwoInputTests` (2: a restart after which
each of two sources has read exactly what was new on its own input; a takeover with
both feeds moving). Example: the smoke test is 47, 59 and 58 claims in its three modes
(from 32, 40 and 39), and `Pipelines.Tests` is 11.

**Mutation checks.** 34, all killed (`scratchpad/r3/mutations.py` and
`mutations2.py`). Core: the commit not waiting for dead letters; abandoning not failing
the gate (this one first hung the test at disposal rather than failing it — the tests
now bound their disposal and refuse a timeout as the expected fault); the delivery
acknowledging before the write; a failed write swallowed and acknowledged. Service: the
supervisor not asking for acknowledged dead letters; the files sink never cleaning,
cleaning everything, and reading the checkpoint late; the source resuming a line early,
ignoring its position, and reading names below it; one bank for the name; the reader
taking the lower term, and ignoring deletes; `minInterval` ignored; a rejected line
sent on; the slot name not reserved; the dead-letter binding unchecked, and its secret
shown; the envelope's generation; the sink's failures not in the status; the stall not
shown; the harness not waiting for sinks, ignoring table bindings, and ignoring the
error policy; the memory source ignoring committed offsets (the two-input tests); a
Kafka rejection without its position. Example, against `Pipelines.Tests`: an unmatched
execution counted as priced; an inner join for the left join; the join on the
instrument alone; a retraction not subtracted; the alerts' minimum ignored; a zero
quantity sized; the enrichment's filter gone; an unknown counterparty dropped.

**Left undone.**

- A replaced leader's late write can leave a files-sink object for a step the new
  leader produces nothing for (*Open for James* 21). Not reproduced; reasoned.
- The files source lists its whole prefix at every poll, and a sink's clean-up lists
  it at every generation start. `IObjectStore.ListAsync` has no "after this name".
- No compression, no Parquet, no time-partitioned paths, no delete line for a keyed
  files input, no compaction of an upsert sink's objects.
- The dead-letter sink's state is in the API and not in the UI; the deploy form has no
  field for `deadLetters`. The UI is another step's.
- A dead letter of an operator carries no source position of its own, only the
  inputs' positions at the last checkpoint: the engine does not track which input
  record a derived row came from.
- `EvictWhen` in `execution-quality` has no test of its own. While a circuit runs, a
  record for a closed key is necessarily older than the watermark and is dropped at the
  input, so eviction cannot be seen from outside; the harness has no view of state
  size. (Not so across a restart: the watermark starts again from nothing and the
  deadlines are not rebuilt. See the second review's findings below, and *Open for
  James* 36.)
- The harness does not restart a circuit from its checkpoint or replace a definition
  on kept state.
- ~~Not soaked~~ Soaked in the second review, below: the alert and dead-letter prefixes
  grew by one object per pipeline every two seconds with nothing to remove them, and
  the example now prunes them.

#### The admin UI and CI: G7 (the UI half), G8, G10, G13, G14, G16, G17

**Deploy and Edit spec (G7, G8).** One dialog, in a seventh UI file (`forms.js`), drawn
from the discovery routes: `plugins` for the plugins, their versions and definitions;
`plugins/{name}/{version}/definitions/{definition}` for the settings and slots;
`…/template` for a starting spec; `connectors` for what can be bound to a slot and each
connector's config fields; `tables` for what a table slot can be bound to. The dialog
edits one spec object. The form writes into it and the *JSON (advanced)* view shows it,
so a member the form does not model — `restart`, an open connector's extra properties —
stays where it is. A version is listed with its import time, who imported it, the
build's informational version and the first eight characters of its hash.

- The id starts empty. `If-Match: "0"` goes with a plain deploy, which the supervisor
  already read as "there must be no such pipeline": a pipeline that appeared between
  the dry run and the deploy is a 412, not a replace.
- An id that is deployed turns the dialog into *Replace pipeline X* — title, a warning
  with the pipeline's state, definition and generation, the button's label — and the
  request carries that generation.
- Any change to the spec clears the last dry run's plan, its error, the keep-or-reset
  question and the answer given to it. The critic's stale panel is gone with that.
- For a replace the dialog calls `pipelines/validate` before the dry run, because the
  dry run of a replace stops at `state-choice-required` before it has looked at the
  bindings: the binding errors now come first.
- Choosing another version of the same definition keeps every setting and binding
  whose name still exists and fills in what is new from the template.
- The *Check* button is not disabled while it is busy. A button that disables itself
  drops keyboard focus out of the dialog, and the next Tab and Enter land somewhere
  else; found by deploying with the keyboard alone.

**Declaring a table (G14).** *Declare* on the tables page: the definition from
`table-definitions` (new: the `TableDefinition` classes of the loaded contracts with
their key and row types and the tables declared from each), a name, a loader kind from
`table-sources` with a connection of the kind it takes and its config fields, and the
unreadable-record policy. The dry run of `POST tables` was a fixed two-line plan that
checked nothing; it is now `ITableRegistry.PlanDeclare`, which makes every check the
declare makes before it opens a log: the name, that it is free, that the definition is
found, the loader's kind, connection and required config. A definition that is not
found used to leave `DeclareAsync` as an `InvalidOperationException` (a 500); it is a
400 now.

**Sizes and memory (G14).** `ITableRegistry.EstimateSize`: the first 256 rows of a
snapshot are written as JSON, and the mean length times the row count is `RowsBytes`;
times the row changes the in-memory log retains it is `HistoryBytes`, which is null on
Kafka, where history is on the brokers. Kept for 30 s per table. The page says how the
number is made and that it is the data's size, not the heap's. `GET node` has `memory`
(working set, GC heap in use and committed, the GC's pause share of the process's life,
full collections), and `PeerInfo.Memory` carries the same for the other node: in memory
from its `DescribeSelf`, on Kafka as a fifth part of the probe line's detail
(`;mem=…`), which an older node ignores and whose absence a newer one reads as "it did
not say".

**Topology (G10).** `CircuitGraphNode.Probes` (core): the table nodes a `Lookup` or
`LookupAppendOnly` reads rows from. They are not among `Inputs`, and must not be — the
lookup probes the table and does not consume its changes — so the edge was simply
missing from `Describe`. The admin API passes it on as `probes`; the UI draws it dashed
and lays the table out beside the lookup. A circuit that does not fit the width is cut
into bands, one below the other, at full size (*Open for James* 27). A selected node
shows its kind with a sentence on what that kind does (join against lookup above all),
its state stores, what it reads, looks up and feeds, and for an input, a sink or a
table the numbers that exist per slot. The card says that per-operator counts and
timings are not collected.

**Plugins (G16).** `PluginManifest` gained `ImportedAt`, `ImportedBy` and
`InformationalVersion` (all optional: a manifest stored before them reads as null).
`SlotShape.Members` spells out the record type of a slot, type by type.
`IServiceDiscovery.DiffAsync` and `GET plugins/{name}/diff` compare two stored versions:
definitions, settings (type, required, default, values, range, state-neutral), and on
the leader the input, sink and table slots with the members of their record types. For
the example's two versions it reports one change: `Size: String` on `EnrichedTrade`,
the row of `trade-enrichment`'s sink. A contract shows the tables declared from its
definitions and cannot be deleted from the page while there are any.

**Small defects (G17).** (1) An event's text over 180 characters is cut with *show
all*. (2) On a fenced or resigned node every pipeline state is drawn as *last known*,
the list and the overview say so, and nothing is counted as needing attention there.
(3) `ServiceTakeoverTimings` gained `LeaderRecord`, `PluginDirectory`, `TableSeeds`,
`PipelineSeeds` and `Finish`, stamped on the clock `StartLeading` already ran; with
`Pipelines` they are all of it. On the example's first lead the 281 ms the critic saw
under "Everything else" is the seed pipelines being deployed (about 200 ms) and the
plugin directory being imported (about 65 ms). (4) `ServiceAdminOptions.CallerName`
and `AcceptOperatorHeader`, `GET whoami`, and a button in the header (*Open for James*
25). (5) `PipelineFault.Generation` was the count of circuits built on the node, under
the same word as the spec's generation; it is the spec's generation now and the count
is `Run`. (6) The theme and the operator's name ride on the links between the two
nodes' pages.

**Dead letters.** The page and the pipeline's tab show the durable sink — connector and
place, records and batches stored, a write in flight, failed writes and the last error
— or say that there is none. The deploy form binds one under *Running it*.

**CI (G13).** `build-and-test` runs the two in-memory smokes and `TradeEnrichment`
after the tests. A new job, `connector-stack`, starts `docker compose --profile
connectors up -d --wait`, builds, runs `Incremental.Kafka.Tests` and
`Incremental.Service.Tests` and fails if either reports a skipped test, runs the Kafka
smoke under a prefix of the run's id, prints the measured failover times and takes the
stack down. **It has not run on GitHub.** What was checked here: the file's structure
by a script (no YAML library or `actionlint` on this machine), `docker compose config`,
and each `run:` line executed by hand against the local stack in Release.

**Deviations.**

- The connector-stack job uses the compose file, not `services:` (*Open for James* 26).
- The UI is seven files, not six; `CLAUDE.md` says so.
- `The_UI_is_served_whole…` used to see only the first `api(…)` call of a statement;
  it now sees each, and requires the discovery routes to be among them.
- G12 (MSK, three brokers, IAM) is not touched: see *Left undone*.

**Public API added.** Core: `CircuitGraphNode.Probes`. Service: `ProcessMemory`,
`PeerInfo.Memory`; `ServiceTakeoverTimings.LeaderRecord`, `.PluginDirectory`,
`.TableSeeds`, `.PipelineSeeds`, `.Finish`; `PipelineFault.Run`;
`PluginManifest.ImportedAt`, `.ImportedBy`, `.InformationalVersion`;
`SlotShape.Members`; `TableSizeEstimate`, `ITableRegistry.EstimateSize` and
`.PlanDeclare`; `TableDefinitionInfo`, `MemberChange`, `DefinitionChange`,
`PluginDiff`, `IServiceDiscovery.TableDefinitions` and `.DiffAsync`. Admin:
`ServiceAdminOptions.CallerName` and `.AcceptOperatorHeader`; routes `whoami`,
`table-definitions`, `plugins/{name}/diff`; `memory` and `processShared` on `node`,
`size` on a table, `probes` on a graph node, `run` on a fault, `sequence`, `importedAt`,
`importedBy`, `informationalVersion` and `tables` on a plugin, `binding` on a
dead-letter sink, `deadLettersBound` on a pipeline. Example: `--ui-dir`.

**Tests.** Core: `DescribeTests` +1 (a remembered lookup, an append-only one and one
through an index each name their table; a join names none). Service: `AdminApiTests`
+3 and additions to three more (memory, the stages adding up, the size estimate, a
contract's tables; table definitions and every refusal of a declare at its dry run; the
caller as hook, header and `local`; probes in the graph; a fault's generation and run;
the UI's routes, each call now seen), `PluginTests` additions (import time, who, build
name, the diff of two versions, a slot's members), `StateShapeTests` +2 (a record type
spelled out; the probe answer's memory, written and read back, and read when absent or
malformed), `DiscoveryDiffTests` 2, a `NodeTests` assertion. One Kafka test
(`Topics_are_created_compacted…`) asked the broker about topics a moment after creating
them and failed twice on a busy broker; it now asks until the broker knows.

**Mutation checks.** 28, all killed after two tests were tightened
(`scratchpad/r4/mutations.py`, `mutations2.py`): a lookup naming no table or the wrong
node; the API dropping probes; a stated name recorded as verified, honoured when
switched off, beating the host's hook, or not cut; the declare's dry run not looking
for the definition, the free name, the loader or the file's path; the size counting
the sample only, or no history; a stage left over; a fault's generation and run; an
import recording nobody; the build name unread; a contract used by no table; a slot
not spelled out; the probe answer without memory or failing on a malformed one; the
form no longer asking for connectors, or calling a route that is not mapped; a changed
setting reported as unchanged.

**In the browser.** Chromium through Playwright against the example (`--nodes 2`, and
two processes on Kafka for the takeover) and against `AdminHost`: every changed view in
light and dark, at 1440 and 390 pixels wide with no horizontal overflow of the page;
a pipeline deployed from the form with the keyboard alone; the two-input pipeline
deployed from the form with `quotes` bound to files, and seen running with both inputs
read; a table declared from the form; a replace to version 2 on kept state through
*Edit spec*; an id typed that exists, and then changed; a takeover, and the fenced
node's pages. The console showed no script error: its entries were the browser's own
lines for refused dry runs and for a node that had been stopped. The content security
policy is unchanged and `The_UI_is_served_whole…` passes.

**Left undone.**

- **G12** (MSK, replication factor 3, broker restarts, IAM) is untouched. Nothing here
  was run against more than one local broker.
- **The connector-stack job has not run on GitHub**, and neither have the three smoke
  steps added to the first job.
- A connector is not told a slot's record type when it is described, so the form
  offers the example's trade feed for a `Quote` input and the dry run passes it; it
  fails when it starts. `IConnector.Describe` would need the slot's types.
- The form describes a definition under its default settings (or, when editing, the
  spec's). A definition whose slots depend on a setting is not described again as the
  setting is edited; switching to the JSON view and back does it.
- The diff compares what is declared. Two versions that declare the same and compute
  differently read as the same, and the page says so.
- The size estimate samples the first 256 rows in the table's own order, not a random
  sample, and says nothing of the process's memory per table.
- The peer's memory on the in-memory backend is this process's, which the page says.
- The topology's bands route an edge that crosses more than one band down the left
  gutter, where several such edges overlap.
- `actionlint` could not be fetched on this machine; the workflow was checked by a
  structural script and by running its commands.

#### Scale and a production-shaped Kafka: G1, G12

**The rig (G1).** `benchmarks/Incremental.Service.Scale` (its README has the options):
two real node processes on the Kafka backend, Release build, a table of wide rows (21
columns, about 520 bytes of JSON, one index) shared by twenty pipelines from a plugin —
two that join keyed orders to the table (two state entries per order), two that keep a
total per party, sixteen that look the party up on arrival and hold nothing — a steady
2,000 orders a second and 500 changed rows a second, and then a cold standby, `kill -9`
of the leader, a takeover from under a healthy leader, and one node started from
nothing. `PipelineStatus.LastStart` (new: plugin, build, start, edges, and whether the
circuit was the standby's) and the takeover's own stage clock are where the breakdown
comes from; the first sink write is measured where a reader of the sink sees it.

The machine: 20 hardware threads, 30 GB of memory of which about 20 GB were free, one
local broker in a container (replication factor 1), NVMe. One run per line unless a
range is given; times move by some tens of milliseconds between runs.

**What the first measurement showed** (one million rows, one million orders of state,
`TakeoverAfter` 3 s; the build before this step):

| kill -9 of the leader | |
|---|---|
| detection | 3.0 s |
| fence | 0.65 s |
| catalog, tables, storage, leader record | 0.06 s |
| pipelines | **5.5 s** |
| kill → first record in the last pipeline's sink | **9.4 s** |
| manual takeover: longest silence in a sink | 6.8 s |

Two things were in the pipelines stage. A takeover built each circuit on the bytes the
standby had followed, and reading them cost 1.3 µs an entry — 2.7 s for a join pipeline
holding a million orders, 0.43 s for a total per party. And although the supervisor
started the pipelines "in parallel", each start ran synchronously up to its first
await, which is after the build: twenty builds ran one after another on the takeover's
thread. The critic's expectation was right, and it would have been 27 s at ten million
orders.

**What was changed.**

1. *A standby builds every pipeline's circuit on the state it follows, and a takeover
   starts it* (`PipelineSupervisor.WarmAsync`, `TakeWarm`;
   `ServiceOptions.WarmStandbyPipelines`, on by default). This is *Open for James* 1's
   default, which S1 had set aside. From the moment a circuit is built, the follower
   applies the leader's commits to the circuit's own typed stores, one record at a
   time, so the decode is paid as the state is written and never again. A takeover's
   pipelines stage is now each circuit's `Start()` (10–20 ms) and its sources and sinks
   (150–200 ms, all pipelines at once).
2. *Layouts* make that safe (durability-and-standby.md, *As built: layouts*). A typed
   store on a follower must never be given a record another version's types wrote —
   during a replace that keeps state the catalog and the state log are two topics, and
   a standby can learn of the new spec before or after the new records arrive. Every
   value a pipeline writes now carries the pipeline's **layout**: twelve hex digits of
   a hash over the plugin, its version, the definition, the settings, the worker count,
   the error policy and the table bindings (`PipelineSupervisor.LayoutOf`). It is a new
   format byte (2) in the state log. A follower holding typed stores turns them back
   into bytes before it applies a value of another layout, and can then build no store
   until it is told the new layout. The leader writes one small commit record under the
   layout before a generation's first start (`CommitLayoutAsync`), so a standby can
   build an idle pipeline too. The standby builds only when the last commit record it
   has applied carries the layout of the spec it would build.
3. *The leader binds by layout too.* A start whose spec has another layout than the
   stores were bound under releases them and binds again, where before only a change
   of plugin version did (a settings change that changed a store's type under the same
   version would have met "already created with different key/value types").
4. *A takeover's starts really are parallel now*: each on a thread of its own. That
   matters for what the standby could not build ahead (below), and for a node started
   from nothing.

**After** (the same run):

| | 1 M rows | 3 M rows |
|---|---|---|
| kill → the standby decides (detection) | 3.0 s | 3.1 s |
| fence | 0.67 s | 0.68 s |
| catalog, tables, storage | 0.04 s | 0.04 s |
| leader record, plugin directory, finish | 0.17 s | 0.04 s |
| pipelines (20 started, all built while standing by) | 0.20 s | 0.21 s |
| takeover, after the decision | **1.1 s** | **0.98 s** |
| kill → first record in the first / median / last pipeline's sink | 4.34 / 4.35 / **4.41 s** | 4.30 / 4.32 / **4.43 s** |
| manual takeover: stages total | 0.91 s | 0.86 s |
| manual takeover: longest silence in any sink | **1.06 s** | **1.05 s** |

And against the size of the state (one million rows; one join pipeline and one total,
so the entries are 3 × the orders):

| orders of state | 250,000 | 1,000,000 | 4,000,000 |
|---|---|---|---|
| state on the leader's heap | 184 MB | 483 MB | 1,307 MB |
| takeover after the decision | 0.93 s | 0.97 s | 0.93 s |
| of which pipelines | 0.16 s | 0.18 s | 0.16 s |
| kill → first record in the last sink | 4.20 s | 4.34 s | 4.24 s |
| a cold standby until it is warm | 10.0 s | 13.4 s | 26.9 s |

A takeover no longer grows with the state or with the table. What is left in it:

- **Detection, 3.0 s**: `TakeoverAfter`. It is configuration; lower it and a slow broker
  is mistaken for a dead leader sooner.
- **Fence, 0.65 s**, of which 0.5 s is one thing: when the dead writer left a
  transaction open (it nearly always has — it heartbeats inside every one), the
  coordinator answers the new writer's first `InitProducerId` with
  `CONCURRENT_TRANSACTIONS` while it aborts that transaction, and the Kafka client
  tries again 500 ms later. The log line the writer now prints shows it: `transactional.id
  initialised in 509 ms, last term read in 118 ms, term committed in 31 ms` (1,011 ms
  when the old leader is alive and commits again in between, as in a manual takeover
  under load). The 500 ms is the client's, not ours; nothing was done about it. The
  118 ms is the end-of-partition wait when the tail of the leader topic is the aborted
  transaction; it could be shaved and was not.
- **Sources and sinks, 0.15–0.2 s**: twenty consumers assigned at their committed
  offsets and twenty producers, in parallel.

**Loading, memory, the standby** (the same runs):

| | 1 M rows | 3 M rows | 5 M rows (one node) |
|---|---|---|---|
| load through the table's writer, versions of 20,000 | 7.2 s (138,000 rows/s) | 20.0 s (150,000 rows/s) | 33.2 s (150,000 rows/s) |
| table topics on the broker (zstd) | 134 MB (141 B/row) | 405 MB (141 B/row) | 675 MB (141 B/row) |
| leader's heap, table loaded | 1,307 MB | 3,934 MB | 6,490 MB |
| added by deploying 20 pipelines over it | 5 MB | 4 MB | 1 MB |
| added by a million orders of state in 4 pipelines | 858 MB | 733 MB | 745 MB |
| state topic on the broker | 142 MB | 144 MB | 144 MB |
| standby's heap, warm | 2,296 MB | 4,809 MB | — |
| working set: leader / standby | 7.8 / 4.0 GB | 12.5 / 8.6 GB | 12.3 GB |
| a cold standby until it is warm | 15.1 s | 25.9 s | — |
| the killed leader back as a standby, until warm | 11.2 s | 23.1 s | — |
| standby's state lag, sampled each second: median / max | 0 / 882 records | 0 / 10,194 records | — |
| standby's table lag: median / max | 0 / 1 versions | 0 / 0 versions | — |
| one node from nothing: process start → leader | 12.4 s | 23.7 s | 33.9 s |
| … → first record in the last sink | 12.5 s | 23.8 s | 34.1 s |

- **One copy of the rows, confirmed**: twenty circuits over a table of one to five
  million rows add 1–5 MB to the heap.
- **A node needs about 1.3 GB of heap per million of these rows** (strings are UTF-16
  in memory: 520 bytes of JSON is about 1.3 KB of objects), the same on a standby.
  The working set is well above the heap: on the leader at the end of the state fill the
  GC had committed 2–5 GB more than was live, and about 2.2 GB was not the GC's at all
  (the Kafka client's buffers for forty-odd clients, the runtime). With
  `DOTNET_GCHeapHardLimit` the GC's share held at the limit (8.0 GB committed for 5.9 GB
  live at four million rows). The GC was not tuned further for footprint, and a cap on
  the gen0 budget changed nothing.
- **Two nodes of five million rows did not fit in this machine**: at three million the
  two working sets were 12.5 and 8.6 GB, at five the leader's alone was 12–14 GB, and
  20–25 GB were free. (Two attempts at four million under a heap limit ended with the
  standby unable to open the table: `OutOfMemoryException`, the table `Faulted`, the
  node up and never warm — which is what a limit set too low looks like.) Five million
  rows were therefore measured on one node, and ten million not at all: by the per-row figures a node would need about
  13 GB of heap for them and some 20 GB of working set. The takeover's stages do not
  involve the rows (a table is promoted with two short reads, 4–14 ms for all of them),
  so nothing suggests the numbers above would move; it was not shown.
- **A cold standby** reads the tables at 210,000–250,000 rows a second, reads the state,
  then builds the circuits on it: 15 s at one million rows and six million entries of
  state, 26 s at three million rows. Until then a takeover is refused unless forced, and a
  forced one waits for the tables (*S2*, departure 13).
- **One node from nothing** is bounded by the same reads and has nobody to wait for:
  about 4.8 s per million rows, plus the state, plus the three seconds of silence it
  must first observe. This is the one path whose length is the data's, and it stays so:
  the rows have to come from somewhere.

**What a standby could not build ahead is built at the takeover**, as before, each in
parallel: a pipeline that writes a table (`ToTable` — its circuit would claim the
table's writer while the table follows), one whose definition throws on the standby
(shown as `Unbuilt` with the reason; the standby stays ready), one deployed in the last
moments before the kill, and all of them when `WarmStandbyPipelines` is off.

**Departures and decisions.**

- **Plugin code now runs on a standby**: the definition's `Build`, and operator
  constructors. Never a step. S2's "a standby fetches plugin files and loads none" and
  S1's departure 1 are reversed; `PipelineState.Built`, which S1 left unused, is a
  standby's built pipeline. *Open for James* 30.
- **A new format byte in the state log.** Nodes from before this build refuse it, so the
  two nodes are replaced together (stop the standby, replace the leader, start the
  standby). State written before it is read as it was. *Open for James* 31.
- **The design's layout marker, `confirmed` and `previous`-on-the-standby were not
  built**; the per-record tag does their work with nothing for the standby to interpret
  but equality. A replace that is rolled back leaves the standby on the bytes it had,
  through the provider's origins (the leader announces the new layout before its circuit
  starts, announces the old one again when it puts the spec back, and the follower's
  stores, which no record was applied to, go back to the bytes they were made of).
- **A standby's readiness has one more reason**, `pipeline-building`, and
  `StandbyReadiness.PipelinesToBuild`: a standby is not warm while a circuit it can
  build is not built. A pipeline it failed to build does not count.
- **`ReleaseStores` on the leader is unchanged**, and with it the limit recorded in S6's
  first part: a replaced generation that has committed has rewritten what it touched.
- **The cold paths decode as before.** A cold standby and a node from nothing read the
  state as bytes and build once; that is 1.3 µs an entry, in parallel across pipelines
  and not within one.

**Three brokers (G12).** `docker compose --profile kafka3` is three brokers in KRaft mode
on ports 9192, 9292 and 9392, replication factor 3, `min.insync.replicas` 2, no volume,
kept out of the default profiles. The test projects take the replication factor from
`INCREMENTAL_KAFKA_REPLICATION` (the two failover apps read it too), and the CDC tests
skip when the broker under test is not the one Kafka Connect writes to. Against it:

| | |
|---|---|
| `tests/Incremental.Kafka.Tests` | 108 passed, 4 skipped (three CDC; one that needs the log cleaner) |
| `tests/Incremental.Service.Tests` | 166 tests with the four broker tests below: in the last full run 164 passed, 1 skipped (CDC) and 1 failed — the rolling-restart test, on a check of its own that named the leader before a takeover had finished. That check and the same one in the outage test were corrected, and the six process tests then passed |
| the scale rig, 200,000 rows, replication 3 | the run passes: takeover after the decision 0.99 s (fence 0.67), first record in the last sink 4.6 s after `kill -9`, a takeover by hand 0.6 s of silence in a sink; the table loaded at 112,000 rows/s (three replicas, acks from two) |

Four new tests in `KafkaServiceProcessTests` stop and start the broker containers
(`INCREMENTAL_KAFKA_BROKERS` names them; skipped without it). Each runs two node
processes, a Kafka-loaded table and a plugin pipeline between two topics, with trades
produced throughout, and ends by comparing the sink topic with the model.

| | what happened |
|---|---|
| a rolling restart: each broker shut down (`docker stop -t 30`), five seconds, started, until every partition has all its replicas in sync | 21 runs (twelve as built, six with `request.timeout.ms` 5 s, three back at the default). In 16 the leader kept leading under term 1, the pipeline did not restart and the sink was complete. In 5 the leader lost the leadership (three of the twelve, two of the six), at the third broker — by then the leader of most partitions and the coordinator of the service's transactions: the leader's open transaction waited 10 to 30 s for the brokers to take its records (the writer logs it: `Kafka transaction has been waiting 30 s for the brokers to take its records`) and the standby saw no heartbeat for `TakeoverCeiling` and began a takeover. In 4 of those it completed and the old leader exited with 3; the test as it then was failed there ("the leader did not stay the leader", or a broken pipe to the node that had exited), **so the sink was not compared in any rolling run in which the leadership moved**. In 1 (`k3-rt5-2`) both nodes ended `Fenced` and exited with 3, and the test timed out with the sink at 7,930 rows of 10,660. The three rolling runs made after the test was corrected had no move |
| each broker killed in turn (`docker kill`), eight seconds, started | nothing lost in 3 runs. In one nobody took over; in two the standby took over at the ceiling after the third broker's kill (the cluster took 43–48 s to be whole again that time, 8–17 s otherwise), and the old leader exited with 3 |
| one broker killed, the leader `kill -9`ed 1.5 s later, the broker started under the new leader | the standby led 3.5–5.3 s after the kill (3 s of that is `TakeoverAfter`); the sink complete; the killed node back as a warm standby. Three runs. |
| two of three brokers killed for 70 s — no controller quorum, nothing writable — then started | the standby had started its takeover at the ceiling and completed it 0.6–3.8 s after the cluster was whole again, at term 2 (four runs); the old leader was fenced and exited with 3; the sink complete, the pipeline not restarted. (Before fix 3 below, that takeover failed and both nodes had exited.) |

**A broker restart can move the leadership.** That is the finding of the two rows
above, and it is not fully explained. While the partitions a broker led and the
transaction coordinator it was are moving, the service's leader can commit nothing; its
heartbeats are records in those commits; and a standby that reads none for
`TakeoverCeiling` takes over whatever the probe says. A takeover that completes is the
designed outcome; that it loses nothing was checked against the sink in the kill runs
(two of three moved) and not in the rolling ones. What is not known is why the wait was
sometimes 30 to 50 s when the cluster was whole again in 12 (and why, in one such
takeover, initialising the transactional id took 14.5 s): a producer
`request.timeout.ms` of 5 s instead of 30 did not prevent it (two moves in six runs),
and the client's own log was not taken at debug level. On one machine with three
brokers it is about one rolling restart in four (5 of 21); what it is on MSK, where a
broker restart is slower and the brokers are not neighbours, is not known. *Open for
James* 32.

**One rolling restart left no node alive** (`k3-rt5-2`, 1 of 21). The standby began its
takeover at the ceiling (leader silent for 30.02 s) and initialised the transactional
id, which fences the leader. Half a second before the leader noticed, the standby's own
first abort was refused — `Failed to end transaction: Local: This instance has been
fenced by a newer instance` — so it took itself for fenced by a third party and exited
with 3 (`Node b could not complete its takeover`); the leader then failed to acquire
its producer id (`There is a newer producer with the same transactionalId`), was
`Fenced`, and exited with 3. The reading that fits the log, unproven: the leader's
client, alive and recovering from the coordinator's move, re-initialised its producer id
at the same moment as the standby's `InitTransactions`, each bumping the epoch under the
other. Nothing committed is lost by it — the test's sink stood at 7,930 of 10,660 rows
because nothing was running — but it is an outage until a supervisor restarts a node,
and that start is a cold one. Not reproduced since, not fixed, and no later change is
known to prevent it. *Open for James* 35.

**What a 60 s+ outage does.** With no quorum the leader can commit nothing and elect
nothing, and its heartbeats stop. The standby cannot tell that from a dead leader, and
at `TakeoverCeiling` (30 s of silence, whatever the probe answers) it starts a takeover —
which waits, because it cannot initialise the transactional id either. When the brokers
return the takeover completes: the standby leads under the next term within four seconds
of the cluster being whole. The old leader is fenced by that and exits with 3; it had
to — its transaction had outlived `transaction.timeout.ms` (60 s) and the coordinator
would have aborted it and bumped its epoch in any case. Nothing is lost: the sources
resume from the last checkpoint and the sink is complete. An outage shorter than the
ceiling ends with the same leader.

**What broke on three brokers, and was fixed.**

1. *A table opened on new topics failed after 30 s, about one time in five.* An admin
   call that needs a partition's leader (`ListOffsets`, `DeleteRecords`) takes it from
   the client's metadata cache. On several brokers a topic created a moment ago can be
   in that cache with no leader, and the call then waits out its request timeout — the
   whole operation timeout — and fails with "No leaders found". The same call again
   finds the same cache. Now `EnsureTopicAsync` asks for the new topic's metadata until
   every partition has a leader (which replaces the cached answer), an admin retry gives
   each attempt half a second, doubling, and asks for the topics' metadata between
   attempts. Twenty of 112 Kafka tests failed before; none after.
2. *The first import of a plugin failed*, and with it every deploy: the blob store asked
   for the end of its topic an instant after creating it, from a broker that did not
   know the topic yet (`Local: Unknown partition`). It asks again now.
3. *A takeover with a broker down read "nothing was ever written" where there was
   plenty.* `ListOffsets` reports a partition it could not ask — its leader is down and
   no other is elected yet — inside its answer, with the offset left unset, and does
   not throw. Read as it stood that was offset −1001, and every caller took it for an
   empty partition. Two things followed, both seen: after the 70 s outage the standby's
   takeover found `Table 'svc.catalog' has no head record`, failed and exited, so that
   both nodes were gone; and with one broker killed just before the leader, the standby
   found no last term in the leader topic and announced **term 1 again** — two leaders'
   records under one term, which the fencing survives and nothing that orders by term
   does. The answer's error is thrown now (`KafkaLog.OffsetOf`) and passes like any
   other missing leader, so the read is made again. Behind it, three guards that do not
   depend on the cause: a writer never announces a term below one this process has
   read; a table log opened on an existing table does not believe a missing head, or
   one older than it has read; and a writer that finds no head does not mint a new
   identity for a table while its partitions hold records that are neither committed
   nor aborted. `OffsetNotAvailable` — the broker's own "my end is not final yet" — is
   retried too.
4. *A duplicate in an append input fails a pipeline for good.* The tests' producer was
   not idempotent, so a retry after a broker died put a trade into the topic twice; the
   join then asserted the same row twice and the upsert sink refused it, at every
   restart, until the pipeline was `Failed`. The producer is idempotent now. The
   behaviour is the product's and is stated in the guide: an append input counts what it
   is given, and a source that can repeat belongs on a keyed input.

One failure was seen once and not explained: on a first lead, the commit that creates a
table's identity was aborted and the abort itself was refused (`Failed to end
transaction: Broker: Producer attempted a transactional operation in an invalid state`),
which ends the writer; the node exited with 3 six seconds after it started. It did not
recur in some forty later starts. The writer's message now also says why the transaction
was being aborted.

**A broker that refuses clients it does not know (G12).** `docker compose --profile
kafka-sasl` is one broker on port 9492 that accepts only SASL/OAUTHBEARER (unsigned
tokens — the broker's development validator) and has an authorizer that denies whatever
no ACL allows. The scale rig takes `--oauth <principal>`: the client configuration then
names the mechanism and nothing else, and the token comes only from the refresh handlers
on `ConfigureProducer`, `ConfigureConsumer` and `ConfigureAdmin`, with a life of twenty
seconds. So a client the service creates without those callbacks cannot authenticate,
and one that does not refresh is cut off inside the run. `grant-acls.sh` gives the
node's principal the permissions and the rig's own clients run as another principal.

The whole run passes: first lead, table load, a `kafka` loader, twenty pipelines
deployed, state, a cold standby, 45 s of steady input, `kill -9`, the standby's
takeover (1.07 s, first record in the last sink 4.28 s after the kill), the killed node
back, a takeover by hand, a cold start from the blob topic. In the minute before the kill the leader's clients were handed 200 tokens and the standby's 81 — every client several times over — and the broker denied the node nothing but the group lookups below. Two things
broke on the way:

1. *A consumer that was asked something before it was ever polled had no token.* A
   consumer's callbacks, the token refresh among them, run only inside `Consume`. The
   blob store's consumer is asked for its topic's end first, and the log's short reader
   for a topic's metadata; both waited sixty seconds and failed with "All broker
   connections are down" — the first lead took 64 s and imported no plugin.
   `AssignedConsumer.ServeCallbacks` is called before such a call now.
2. *A refused group lookup ended a follower, and with it a takeover.* The Kafka client
   requires a `group.id` and looks up the group's coordinator whether or not the group
   is ever joined. Without `Describe` on the group the broker refuses, and the client
   hands that to the application from `Consume` as an error; the state follower treated
   it as a failed read, and the node's first takeover ended fenced.
   `AssignedConsumer.ConsumeAssigned` passes it over, and every consume in the libraries
   goes through it. Nothing the service creates joins a group, so it needs no permission
   on one — the broker's authorizer log showed nothing denied to the node but those
   lookups (about one a second per consumer; `Describe` on the three group names
   quiets it).

The permission set that sufficed, as ACLs and as MSK IAM actions, is in the guide (*On
Amazon MSK*); the IAM column is a translation and has not been run.

**Public API added in this step.**

- Core: `ChangelogStorageProvider.UseLayout`, `Layout`, `CommittedLayout`,
  `CommitLayoutAsync`, `StoreEpoch`; a second record value format (format byte 2).
- Kafka: `AssignedConsumer.ConsumeAssigned` and `ServeCallbacks`.
- Service: `ServiceOptions.WarmStandbyPipelines`; `PipelineStartTimings` and
  `PipelineStatus.LastStart`; `StandbyReadiness.PipelinesToBuild` and the not-warm
  reason `pipeline-building`. `PipelineState.Built` is now used.
- Compose profiles `kafka3` and `kafka-sasl`; `INCREMENTAL_KAFKA_REPLICATION` and
  `INCREMENTAL_KAFKA_BROKERS` for the tests; `benchmarks/Incremental.Service.Scale`.

**Tests.** Core, `ChangelogLayoutTests` (7): the format with and without a layout, read
back as one state; an announcement that moves neither sequence nor offsets and is
written once; a follower with typed stores that follows its layout and takes over on
the same store objects; a record of another layout turning the follower's stores back
into bytes before it is applied, the member the old types do not model intact; an
announcement that is taken back, leaving the bytes the stores were made of; a follower
holding what its leader holds once the leader has checkpointed; a tombstone
the store's types cannot read. Service, `WarmStandbyTests` (9): the takeover ratio
(below); a standby with building switched off; a pipeline that has never checkpointed;
a replace that keeps state across two plugin versions while the standby holds the old
version's stores, then a takeover; a table opened again on the standby; a definition
that throws on the standby; a standby that is not warm while a build is held at a gate;
a pipeline removed on the leader; what a layout covers. Seven existing tests said a standby's pipelines are
`Unbuilt`, have no graph and load no plugin; they now say `Built`.

*The regression test for the requirement* is
`A_takeover_starts_what_the_standby_built_and_does_not_pay_for_the_state_again`: two
pipelines on the in-memory backend, 300 and 90,000 keyed entries. The standby's own
build of each says what reading that state costs (about 5 ms and 200 ms in a Debug
build); after the takeover each pipeline's build and start are compared, and the
assertion is a ratio — 300 times the state must add less than a tenth of what reading
it costs. It takes three seconds. With the standby's circuits not used at a takeover
it fails.

**Mutation checks.** 20 run, 19 killed (`scratchpad/r5/mutations-core.py`,
`mutations-svc.py`; the service ones in a copy of the tree, because the broker tests were
running in this one). Core, 11: a value of another layout applied to a typed store; an
announcement counted as a checkpoint; a commit record of another layout releasing
nothing; the tombstone guard off; values written without the layout; an announcement
advancing the sequence, or written every time; a follower allowed to create stores of
a layout the log has left; the epoch not moving; the tag not skipped on read; a
follower keeping the bytes after its leader has checkpointed (survived its first run —
no test released a follower's stores after a real checkpoint; one does now). Service,
9: a takeover never using what the standby built; the standby building nothing; the
leader not announcing the layout; stores bound under no layout; a reopened table
leaving the built circuit alone; a failed build making the standby unready; the
standby not saying it is still building; the layout ignoring the plugin version
(survived, because the two test plugin versions hold state of the same shape; a unit
test of `LayoutOf` kills it now). One survives: taking the release out of
`SyncFromCatalog` does not fail the removed-pipeline test — the standby's hold on the
plugin version goes by some other path that was not found. The fixes found on the
brokers (`ConsumeAssigned`, `ServeCallbacks`, `OffsetOf`, the term floor, the admin
retries) have no mutation check: each was a failing run on the `kafka-sasl` or `kafka3`
profile before it and a passing one after, and nothing on one plain broker provokes them.

**Open for James** gains 30–34: plugin code on the standby; the new format byte and the
upgrade order it imposes; the takeover ceiling against a broker's failover time; the
group lookups left refused rather than a permission asked for; the broker tests left
out of CI.

**Left undone.**

- **Ten million rows, and two nodes at five million**: the machine could not hold them.
  Five million were measured on one node; the takeover was measured at one and three
  million rows and at a quarter of a million to four million orders of state.
- **The fence's half second** (the client's fixed retry while the broker aborts the
  dead writer's transaction) and the 0.1 s end-of-partition wait behind it.
- **The cold paths are as they were**: a cold standby and a node from nothing read
  everything, and a table restore is one consumer.
- **A pipeline that writes a table is not built ahead** on the standby.
- **Memory was measured, not tuned.** The working set is about twice the live heap on
  the standby and 2.7 to 3.6 times it on the leader (7.8 GB for 2.2 GB live at a million
  rows, 12.5 GB for 4.7 GB at three million), and nothing was done about it; no
  per-table or per-pipeline memory figure exists.
- **Real MSK**: IAM itself, TLS, the IAM action names, hours of token refresh, an
  availability-zone loss, MSK Serverless, MSK Connect. Kafka Connect was not put behind
  SASL.
- **The unexplained aborted first commit** above.
- **The broker tests and the SASL run are not in CI**; neither has run anywhere but
  this machine.
- **The admin UI** shows a standby's `Built` pipelines and the graph, with new
  sentences; no screenshot was retaken and `LastStart` and `PipelinesToBuild` are in
  the API only.

#### The second review's findings

A review of S6 as committed confirmed 29 findings. Each was reproduced where that was
feasible, fixed where it was a defect of the code, and stated where it was a defect of
what the documents claimed. None was wrong on inspection; two were narrower than
reported, and say so below.

**The supervisor.**

- *A second replace that touches no state dropped the spec to go back to.* `Previous`
  was written only when the deploy at hand changed something state depends on, so a
  redeploy of an unconfirmed keep with another sink lost the note, and the mismatch at
  the next start ended in `NeedsReset`. `Previous` is now "the spec that wrote the
  state": kept through every deploy until a generation has run, and dropped only by a
  reset or when the spec deployed is, to the state, the one that wrote it. The plan says
  so (`the state is still generation 1's`) and carries the roll-back line.
- *A standby's build checked one spec and built another.* `Bind` and `Build` read
  `runtime.Spec` again, which the node's loop replaces without the pipeline's queue; a
  replace that landed while the plugin was being fetched gave stores of the old
  version's types under the new spec's layout tag. Both take the spec now.
- *A table reopened while a standby was building on it.* `OnTableReopened` marked only
  circuits that were already kept. It now counts per pipeline, the build records the
  count from before it resolved its tables, and a circuit is good only while the count
  stands — the same test at the takeover.
- *The dead letter's `after` carried a files input's name chunks*, up to 130 entries
  nobody can read. A source connector may word its position (`IPositionDescribing`);
  the envelope has `afterAt`, input → words, and such an input's partitions are left
  out of `after`.

**The `files` connector and the object store.**

- *A checkpoint holding half of a name, then another object read next, mixed the two
  names.* Ordinals were "the header's, plus one", so after a restart the next object
  could be given an ordinal whose first chunks a checkpoint already held for another
  name, and the input dropped the chunks that compared lower. The ordinal is now above
  every ordinal any committed partition carries, and of the other parity than the
  current one so that the bank holding the current name is not written
  (`FilePosition.Next`). The cost is a skipped ordinal in that one case.
- *A line had no bound.* `BoundedLineReader` reads past a line longer than
  `maxLineLength` (new in the slot's config; 1,048,576 characters by default) holding
  4,096 of it, and the line is rejected like any that is not a record.
- *An object arriving among names already listed was skipped.* A listing is now used
  for one `pollInterval` at most. That narrows the hole and does not close it — an
  object that arrives behind the source within one interval is still missed — so the
  guide's rule was rewritten as what a producer must do: names increase. *Open for
  James* 39.
- *A write cut short left a truncated object under the final name.*
  `IObjectStore.OpenWriteAsync` is gone; `WriteAsync(path, write, ct)` makes the object
  only if the callback returned (*Open for James* 38).
- *`LocalObjectStore.ListAsync` walked the whole root.* It walks the directory the
  prefix names. The example's quote source no longer pays for the alert objects.
- *Prefixes were not checked across slots or pipelines.* A sink connector may claim
  what it writes (`ConnectorDescription.Owns`; the files sink claims store and prefix),
  and a second sink slot with the same claim is refused at the dry run and again where
  specs are committed. Not in `Build`: two stored specs in conflict would stop each
  other. Sources are not checked — a source reading what a sink writes is a chain.
- *An appended-to object is read again only after a restart.* Not changed; the guide
  no longer says "is not read again".

**Discovery and the admin API.**

- *The template bound a slot to a connector that cannot carry its type.* A connector
  that carries only some types says so (`ITypedConnector.RequireTypes`), the dry run
  asks it, and the template offers it only for the slots it accepts. Connectors
  without the interface are taken to carry anything, as before.
- *`ConfigProperty.Secret` was read by the form and by nothing else.* A slot's secrets
  are now the union of what `Describe` names and what the schema marks, and the
  schema's hold for a config `Describe` refuses.
- *Notes of a description left with `ExposeRowData` off.* The notes that quote an
  exception do so after their first colon and no note has another; the three routes
  pass them through `Head` (the diff used to blank them whole). *Open for James* 42.
- *A declare's dry run passed for a factory that cannot fill the table.* `CheckDeclare`
  asks a registered factory for a source of the table's types and drops it.
- *A leader-only read on a standby was told "mutations go to the leader".* It is told
  that a definition is described by running its `Build`.
- *"0 state store(s) … hold the same types"* is now "keeps no state stores", with the
  tables' types named as the tables'.

**The UI** (driven in a browser against the admin host and the example; the topology
and deploy-form screenshots were retaken).

- *Text that is not JSON in an array or object setting was dropped without a word.* The
  field keeps the text as a draft, marks itself invalid and says why, and a form with a
  draft on screen is not checked or sent.
- *A dialog outlived the page it was opened on.* It is closed when the route moves to
  another page.
- The deploy form offers, for a slot, the connectors the node's template says can
  carry it (it offered every connector of the slot's kind, the trade feed for quotes
  among them). And the topology's panel called an operator's name its "state stores"
  even for an operator that keeps none; the row reads *Known as* and says what the
  name is for.

**Tests and CI.** `Readers_always_see_exactly_their_pinned_version…` failed about one
run in six in Release, the configuration CI runs: its four readers were pool tasks and
could fail to start before the writer had finished. They have threads of their own and
the writer waits for each to have probed once.

**What the documents claimed.**

- *Broker restarts.* The 21 rolling-restart logs were counted again: 16 with the same
  leader throughout, 5 in which it lost the leadership (not 4), in none of which the
  sink was compared, and one of those with both nodes fenced. The table above and the
  guide say so; the guide no longer tells an operator that a broker restart loses
  nothing either way.
- *Event time across a restart.* The example's comment, the guide and this document
  said a join's state is bounded and a closed window final. Both hold while a circuit
  runs and neither across a restart, because the watermark and the eviction deadlines
  are in memory only. Stated in all three places; not fixed (*Open for James* 36).
- *The roll-back's byte copy.* It is the leader that holds a pipeline's state twice
  from any start until its next commit, for as long as the pipeline is idle; a standby
  only until the leader's first checkpoint under the layout. The finding had it on both
  after every start: `CommitLayoutAsync` writes nothing when the layout already stands,
  so a standby building on a checkpointed layout keeps no copy. Not changed in code:
  the copy is what makes a failed start lose nothing, and a layout does not name the
  build of the contract assemblies — a host restarted with other contract types builds
  under the same layout, and the copy is what lets that be undone (*Open for James* 37).
- *The failover figure* is for a dead process. A leader that is alive and cannot
  commit is taken over at `TakeoverCeiling`; the guide's Failover paragraph, the "No
  leader" alert, the README and `intro.md` say so.
- *"Takeover slow"* watched `start_leading`, which leaves out the wait and the fence;
  it watches `total` and `silence`.
- `intro.md` still said a standby builds nothing; the scale rig's README and this
  document said the working set is twice the heap (it is 2.7 to 3.6 times on the
  leader); the guide's state series did not say it ran one join and one total.

**The example left running.** The guide's "settles at about 690 MB and stays there"
had been measured before the example gained three pipelines and its file outputs.
Measured again (Release binary, one in-memory node, default rate, sampled every 30 s;
the pruning below already in), it did not settle:

| minutes | 10 | 20 | 30 | 40 | 50 | 60 |
|---|---|---|---|---|---|---|
| RSS, MB | 515 | 690 | 734 | 921 | 996 | 1,066 |

and at `--rate 5000` it was at 7.4 GB after twelve minutes with 5.6 million records in
the state log. Before the pruning the files directory also grew by about 90 objects a
minute with nothing to remove them (the reviewer's run: 2,192 objects after 24.5
minutes). Two causes, both in the example:

- *`execution-quality` remembered every execution.* It looked the instrument up after
  its left join with `stream.Lookup`, which keeps what it emitted for each record so
  that a retraction takes exactly that back, and which — being the plain form — has no
  grouping to hang `EvictWhen` on (`shared-tables.md`, *Bounding the memory*). Behind a
  join that retracts, on a stream that never ends, that is an entry per execution for
  ever: 3,000 a minute at the default rate. The lookup is a `LookupAppendOnly` on the
  executions before the join now — they only arrive, so it keeps nothing — and the join
  carries the asset class through. The definition's comment and the guide say why it
  sits there, and the guide's account of `Lookup` now says what it remembers.
- *Nothing deleted the sinks' objects.* The example prunes each files-sink prefix to its
  newest 200 objects every ten quote intervals (*Open for James* 43).

After both (same method):

| minutes | 4 | 8 | 12 | 16 | 20 | 24 | 28 | 32 | 36 | 40 |
|---|---|---|---|---|---|---|---|---|---|---|
| RSS, MB | 337 | 423 | 522 | 609 | 648 | 653 | 655 | 650 | 700 | 683 |
| objects in the files directory | 367 | 731 | 895 | 1,260 | 826 | 1,190 | 1,554 | 1,015 | 1,375 | 835 |

RSS was between 636 and 723 MB from minute 20 to minute 40; the state log held 172 to
314 thousand records from minute 10 on (the sawtooth of four partitions compacting),
the collector's committed memory 490 to 536 MB over the last twenty minutes; the files
directory never held more than 1,691 objects or 9.8 MB. At `--rate 5000` for nine
minutes — as many records as fifteen hours at the default rate, and nine hundred
quote intervals — the state log stayed between 0.99 and 1.25 million records and RSS
between 1.8 and 2.7 GB with no trend (the join holds a minute of event time, which at
that rate is 300,000 executions). Forty minutes is not a day: what these runs show is
that the two things that grew no longer do.

One more defect turned up on the way: a `CacheDirectory` given relative to the working
directory (the soak script's `--data ./dir`) failed every plugin load with "is not an
absolute path". The cache root is made absolute; a test covers it.

**Deviations.**

- `IObjectStore` changed incompatibly (`WriteAsync` for `OpenWriteAsync`).
- `DeadLetterEnvelope` gained `AfterAt`; a files input's entries left `After`.
- The texts of the description notes changed (no colon but before an exception's
  message), and the diff's notes are cut and not blanked with row data off.
- The files source's "sorts after the position" rule is weaker than the guide once
  said, and is documented as it is, not made exact.
- `execution-quality` has other stores than it had (the remembered lookup is gone), so
  a deployed one cannot be replaced by the new plugin version with `keep`: the dry run
  refuses it, and it takes a reset.
- A fix outside the findings: a relative `CacheDirectory`.
- Three findings ended as statements and not as code: eviction across a restart, the
  double-held state, the both-nodes-fenced run.

**Tests added** (each checked by a mutation that it kills, 19 in all): the second
replace keeping `Previous`; the two standby races, through two test hooks in
`ServiceOptions` (`BeforeStandbyBind`, `AfterStandbyBuild`); the half-committed name;
the re-listing; the long line (through the source, and half a billion characters
through the reader); the write that fails part-way; the listing by prefix; the shared
prefix; `afterAt`; the typed connector at the dry run and in the template; the schema's
secret; the factory at the declare; the standby's refusal of a describe; the notes
with row data off; the unchanged line with no stores. The example's smoke gained a
claim: the starting spec for `execution-quality` does not put quotes on the trade feed,
and a spec that does is refused.

**Left undone.**

- The both-fenced takeover is not reproduced and not fixed.
- The watermark and the eviction deadlines are still not checkpointed.
- The leader still holds an idle pipeline's state twice after a start.
- A files source can still miss an object that arrives behind it within one poll
  interval, and still lists its whole prefix at every poll.
- The UI's fixes have no automated test: the UI has none of its own.
- Nothing automated would catch a definition that keeps a store growing for ever, as
  `execution-quality` did: the harness has no view of state size, and the smokes run
  for seconds. The evidence is the two runs above.
- No rolling broker restart was run again: the kafka3 profile was not started in this
  step, so whether a moved leadership in a rolling restart leaves the sink complete is
  still unverified.

### S7 — round three

#### Event-time state survives a restart and a takeover (H2)

The completeness critic's H2: windows and failover did not compose. The watermark and
the eviction deadlines were operator fields, so a pipeline that restarted — a takeover,
a replace that keeps state, a fault and a backoff — admitted late records into windows
that had closed and been delivered, and kept for ever the keys that were open at that
moment and never touched again. It was documented in S6 and in the guide's limits. It
is fixed in the core; `durability-and-standby.md`, *As built: event-time state*, has the
mechanism. For the service it comes to this:

- **Nothing in the service changed to get it.** The clock rides the offsets marker of
  the commit record a standby already follows, and the deadlines are stores of the
  pipeline like any other: followed as bytes, held typed by a warm standby's built
  circuit, handed over by `TakeWarm`, released and re-read by a `keep`. A warm
  takeover rebuilds no queue: the follower put each deadline into the order as it
  applied the record.
- **The one service edit** is in the dead-letter envelope: `after`, the inputs'
  committed positions, no longer lists the circuit's own entries (`$step` was already
  left out; `$watermark` and `$watermark#s{n}` now are too).
- **`GET pipelines/{id}`** shows the clock with the other committed offsets
  (`committedOffsets["$watermark"]`, UTC ticks), on the leader and on a standby.
- **An existing deployment.** A pipeline with event time whose state was committed
  before this build is refused at its start with the reason, and needs `state=reset`
  (Open for James 36). The example's `desk-exposure` and `execution-quality` are such
  pipelines; `--kafka --clean` or a new `--prefix` starts them clean.
- **State shapes.** A deadline store shows in `DescribeState` as kind `Deadline`
  (key type → `long`), so a version that changes a windowed operator's key type is
  caught by `StateShapes` as before, now also for its deadlines.

**Public API added (core).** `IDeadlineStore<TKey>`;
`IStorageProvider.CreateDeadlineStore<TKey>` (a default method: the heap store for a
provider that does not checkpoint, a refusal for one that does and has not implemented
it); `CircuitStateStoreKind.Deadline`; `ChangelogStorageProvider.CountEntries()`;
`RedisStorageProvider` gains a hash and a sorted set per deadline store. State manifest
format 7 with an `eventtime={sources}` line; offsets-marker keys `$watermark` and
`$watermark#s{n}`; `$watermark` is a reserved input name.

**Tests.**

- Core: `EventTimeRestartTests` and `DeadlineStoreTests` (described in
  `durability-and-standby.md`): 76 cases, about 25 s of a core suite that took 10 s, 18 of them on Redis (skipped without it).
- Service, on Kafka:
  `KafkaServiceTests.A_takeover_in_the_middle_of_open_windows_reopens_none_and_still_closes_them_all`.
  Two nodes on the broker; a pipeline of ten-second tumbling totals with a live upsert
  sink and an `OnClose` sink; 280 ticks over 28 s of event time for five traders, so at
  the takeover each trader has two closed windows and one open. The checkpoint's
  `$watermark` is asserted on the leader and on the standby. After the takeover the
  first record the new leader reads is twenty seconds behind the watermark, in a window
  already delivered as final; then one trader's records far ahead. Asserted: both sinks
  equal the model, which has no late record in it; the live topic holds not one more
  write for the closed window than before the takeover; and the pipeline's state
  shrinks to exactly that of a second pipeline, deployed afterwards, that only ever saw
  the last two records (50 entries at the takeover — ten windows of five entries, five of them open and five closed and not yet swept — and 5 afterwards, which is what the second pipeline holds).
- Service, in memory: a dead letter of an event-time pipeline names its input's
  position and none of the clock's keys.
- Mutations: 24, all killed. Twenty in the core — the clock not committed, not
  restored, restored without its per-source maxima or without the watermark; the
  deadlines of the tracker, of the session operator and of the window-close operator
  kept on the heap; rollback, take, set and moved-deadline bookkeeping broken in the
  in-memory and changelog stores; a follower ignoring deadline records or their
  tombstones; stale queue entries taken; the two manifest rules dropped — and three in
  the Redis store (the exact comparison, the lower bound passing a staged removal, a
  commit not lowering it). The Kafka service test is killed by four of the core ones
  (clock not committed, clock not restored, deadlines on the heap, follower ignoring
  deadline records), the dead-letter test by the service edit undone, and the example's
  two-node smoke by the clock not restored: it then prints `desk-exposure`'s first
  window holding the straggler's notional alone where the model has the window's total.
  One mutant survived the first round and got a test: a session key taken back whole
  kept its deadline, which changed no output and no final state.

**The example.** The limit is out of `ExecutionQuality.cs`, the guide and the example's
README. The smoke's takeover act (`--headless --nodes 2` and `--headless --kafka`) has
three more claims: `desk-exposure`'s checkpoint carries the watermark and the standby
has followed it; the first execution the new leader reads is a *straggler* — it carries
the time of the feed's first trade, in windows that closed long before — and
`desk-exposure`'s and `execution-quality`'s sinks equal a model that leaves it out; the
new leader's own checkpoints carry the watermark on from where the old one's stood.
`WorldShape.Stragglers` is the knob, and the model drops a straggler only from the two
event-time pipelines: the others read it like any execution.

**Performance** (the core's hot path changed: every evicting operator's tracker).
The torture rig (`benchmarks/Incremental.Torture`, Release, `--workers 4 --seconds 20
--warmup 8`, a 13th-gen i7-13700H; the showcase graph on the heap provider) was run
before and after, interleaved, four times each:

| | before (dc50885) | after |
|---|---|---|
| records/s | 367.6k, 373.6k, 372.8k, 368.4k (mean 370.6k) | 372.8k, 377.1k, 374.5k, 368.4k (mean 373.2k) |
| allocated per record | 593 B (658 B in the one run a build was running beside) | 566 B |
| worker CPU per record | 7.88–8.01 µs | 7.90–8.02 µs |

A first, not interleaved, set of three each read 376.2k, 371.9k and 383.1k records/s
before and 376.2k, 366.7k and 377.9k after, with 594 B and 565–567 B per record. So:
throughput within the run-to-run spread, allocation down 27 B a record (the list an
eviction used to allocate, and a map entry where there was a set entry and a queue
entry's key). `benchmarks -- --assert`: all 10 contracts hold, before and after. Not
measured: the rig on a checkpointing provider — the changelog record a key now costs
when it opens and the tombstone when it closes — and a takeover against the number of
open keys; that a warm takeover does not depend on it is by construction (the follower
applies each deadline as it reads it) and is not a number.

**Deviations.**

- The task's alternative of rebuilding the deadlines from the keys at start was not
  taken; the deadlines are a store (above, and why).
- Existing event-time checkpoints are refused, not migrated; ones without event time
  are migrated by doing nothing.
- `IStorageProvider` gained a member. It has a default, so an existing provider
  compiles; one that checkpoints is refused at `Build()` of an event-time circuit until
  it implements the member.
- The window-close operator calls `closesAt` a second time, when a key closes: the
  deadline store holds ticks, and the notification carries the `DateTimeOffset` the
  definition gave.
- The in-memory checkpoint provider and the service test's "state returns to its size"
  are asserted by entry count, not content: neither can list itself. The changelog and
  Redis cases compare content.

**Left undone.**

- No state size in the admin API or UI (Open for James 46).
- No test of a clustered (several-process) circuit restoring its clock.
- `closesAt` is not under the dead-letter policy.
- The service test covers a manual takeover in one process. The `kill -9` path is the
  example's `--headless --kafka` smoke, which checks the sinks against the model with a
  straggler behind the kill; it does not measure state size.


#### Broker failures: the stall, the leadership, the supervisor (H1, H4, the Kafka half of H13)

The critic's H1: a broker failure could still take the service down, and nobody knew
why the leader stalled for 30 to 50 s. Both are answered. The causes, the evidence for
each and the mechanisms are in `durability-and-standby.md`, *As built: broker
failures*; this is what it comes to for the service, and the runs.

**What was wrong, in one paragraph.** The stall was three things in a row. The brokers
give a dead broker nine seconds before its partitions move, and that is Kafka's. The
Kafka client then waited out delays of its own — ten seconds before reconnecting,
sixty for a broker that had stopped answering — which was ours to configure and was
not. And the test cluster added more: containers that came back at new addresses and
looked each other up by name left the controllers leaderless for twelve seconds, and
the tests killed the next broker while the last was still recovering, because one
broker's word for "the cluster is whole" was believed. The leadership moved when those
sums passed the thirty-second ceiling. Both nodes ended fenced, once, because a
replaced leader's client asked the transaction coordinator for a new epoch while its
successor's first transaction was open, and the broker (Kafka 3.9) answered that by
fencing the successor; the successor then ended like any fenced node.

**What was built.**

- **`KafkaClientDefaults`** (`src/Incremental.Kafka`): the settings every Kafka client
  of the library and the service starts from — the log's producer, consumers and admin
  client, `KafkaSource`, the Kafka sinks, the blob store — applied before the caller's
  `ClientConfig`. Reconnect delay at most 1 s (was 10), socket timeout 6 s (was 60;
  not for an admin client, whose calls carry their own timeouts), the producer's
  acknowledgement timeout 5 s (was 30), connection set-up 5 s (was 30), a metadata
  refresh every 2 s (was 300). `KafkaLog.EnsureTopicAsync` asks again while the brokers
  cannot answer: a node its supervisor restarts in the middle of a broker fault must
  not end because of it.
- **A takeover that is lost before the node is the writer leaves it a standby.**
  `KafkaLog.AcquireWriterAsync` fenced or cancelled part-way replaces its producer and
  throws `KafkaLogNotAcquiredException`; `KafkaLeadership` goes from `Promoting` back to
  `Standby` (`TakeoversAbandoned`), waits between one and two takeover timeouts, and
  decides again. A takeover is called off this way when the leader's heartbeat is heard
  while the acquisition is still running. In the service nothing had to change: nothing
  is promoted before the acquisition returns, the standby's tables go on following and
  its circuits stay built. A manual takeover that is lost answers 409 `takeover-lost`.
- **At the ceiling a standby asks the brokers first** (`KafkaLog.ProbeWritePathAsync`)
  and does not take over from a leader that answers its probe while a partition of the
  log has no leader or too few replicas in sync (`TakeoversHeld`), nor for two takeover
  timeouts after the brokers last said so, which is the leader's time to be heard.
- **`NodeSupervisor`** (`src/Incremental.Service`): the restart contract a node needs,
  as a call at the top of `Main` — the program runs itself as a child and starts it
  again after every exit but 0 and 2. The example's Kafka mode uses it (off with
  `--no-supervise`), the scale rig's nodes run under it (`node --supervise`), and so do
  the broker tests' (`FailoverApp --supervise 1`). The guide states the contract for a
  platform's supervisor.
- **The three-broker profile** has fixed broker addresses on a network of its own, and
  the brokers name each other by address.
- **The broker tests** (`KafkaServiceProcessTests`, six of them now): supervised nodes;
  a reader of the sink that times its longest silence and the longest a trade took from
  the input's acknowledgement to its row; the cluster asked broker by broker before the
  next fault; a killed broker left dead for fifteen seconds; one test that loses one
  broker (killed, frozen or stopped — `INCREMENTAL_KAFKA_FAULT`) and asserts that the
  leadership did not move; one that keeps a broker away for minutes and kills the
  leader half way. Each run writes a record to `broker-runs.jsonl`. On a cluster
  started a moment ago they first run a throwaway transaction, so that
  `__transaction_state` exists to be waited for (H4).

**Public API added.** `Incremental.Kafka`: `KafkaClientDefaults` (`Settings`,
`Apply`); `KafkaLogNotAcquiredException`; `KafkaLog.WriterResets`;
`KafkaLeadership.TakeoversAbandoned` and `TakeoversHeld`. `Incremental.Service`:
`NodeSupervisor` (`RunAsync`, `IsSupervised`, `EnvironmentVariable`) and
`NodeSupervisorOptions`; backend counters `writerResets`, `takeoversAbandoned`,
`takeoversHeld` in `GET node`; error code `takeover-lost`. Internal seams for tests:
`KafkaLog.WhileAcquiring`, `FailTransaction`, `WritePathOverride`. Behaviour that
changed: `AcquireWriterAsync` fenced before it completes throws
`KafkaLogNotAcquiredException` and leaves the log usable, where it threw
`KafkaLogFencedException` and left it finished; a node may go `Promoting → Standby`.

**The runs.** Three brokers on one machine (a 13th-gen i7-13700H; `kafka3` profile:
each broker also a controller, replication factor 3, `min.insync.replicas` 2, Kafka
3.9.0 with its defaults), two supervised node processes (`TakeoverAfter` 3 s,
`TakeoverCeiling` 30 s, the peer probe on), one plugin pipeline from a two-partition
topic through a Kafka-loaded table to a compacted topic, 200 trades a second for the
whole run. Scripted (`h1/runs.sh` and `h1/series.sh` in the scratchpad directory
beside the gaps file; each run is one execution of the named test on a deployment of
its own, and the records are the `h1/*.jsonl` files there). For every run the test
asserted that the sink equals the model and that exactly one node leads with the
pipeline running while the other stands by, warm; it recorded the sink's longest
silence as a reader of committed records sees it, the longest any trade took from the
input's acknowledgement to its row, who led, and every exit a supervisor saw.

| scenario | runs | sink = model, one leader, one warm standby | leadership moved | node processes ended | sink silent: min / median / max |
|---|---|---|---|---|---|
| one broker killed, 20 s away — it was **not** the active controller | 10 | 10 | 0 | 0 | 7.7 / 9.9 / 10.8 s |
| one broker killed, 20 s away — it **was** the active controller | 20 | 20 | 0 | 0 | 12.6 / 13.6 / 15.1 s, and once 26.0 s |
| the active controller killed, 45 s away, the node's client log at debug | 20 | 20 | 0 | 0 | 12.4 / 13.6 / 15.0 s |
| rolling restart: each of the three brokers shut down, 5 s, started | 30 | 30 | 0 | 0 | 0.6 / 1.1 / 1.8 s |
| one broker frozen (`docker pause`) for 20 s | 6 | 6 | 0 | 0 | 10.6 / 14.7 / 15.7 s |
| one broker frozen for 60 s | 3 | 3 | 0 | 0 | 12.6 / 13.4 / 14.4 s |
| the same, the Kafka client's default settings (two sets of three) | 6 | 6 | **3**, at the ceiling | 3 (exit 3, started again) | 8.3 / 35.3 / 60.6 s |
| every broker killed in turn, 15 s each, the next when every broker calls the cluster whole | 2 | 2 | 0 | 0 | 13.1 and 13.9 s |
| two of three brokers stopped for 40 s and started: no quorum, nothing can commit for 57 to 58 s | 3 | 3 | 0 (the standby held 15 to 39 times at the ceiling) | 0 | 56.8 / 57.9 / 58.2 s |
| two of three stopped for 70 s and started: nothing can commit for 90 to 117 s, past the 60 s transaction timeout | 4 | 4 | 4: the leader's writer ended at 60 s, the standby led when the brokers were back | 4 (exit 3, started again; one of them once more with exit 9) | 89.6 to 117.5 s |
| one broker killed and away for 10 min 10 s, the leader's process killed after 5 min | 1 | 1 | 1, which is the test: the standby led 3.5 s after the kill, on two brokers | 1 (the killed leader; started again, a warm standby on two brokers) | 10.8 s, at the broker's loss |

The exit 9 in the 70-second row was a node started by its supervisor while the
brokers were still down: it could not create its topics within the admin client's
six-second socket timeout and ended; its supervisor started it again. That timeout is
no longer applied to admin clients and topic creation is retried, and the two runs
made after that change have exit 3 alone. Two of the four 70-second runs were made
before the standby was given its pause after the brokers' return and two after; at 70 s
the leader is past its transaction timeout either way. The 40-second runs were all made
after.

Against the targets: no run ended with both nodes fenced, or with any node fenced at
all outside the rows that say so; the leadership moved in none of the thirty rolling
restarts and in none of the thirty kills; the stall is one second for a restart and,
for a kill, what the brokers take to give a broker up — under about ten seconds when
the broker was only a broker, twelve to fifteen when it was also the active
controller (the new controller gives the dead broker nine seconds from its own
election). That second figure is over the ten-second target and is not the service's
to shorten.

**The one run in thirty that took 26 s** (kill 27: broker 3, the active controller,
leader of one state partition and one input partition). The leader's writer waited the
whole twenty seconds the broker was away for a transaction to be taken, logged no
error and no retry, and went on when the broker returned. The node's client log was
not at debug level in the series, so this is read from the brokers' logs: broker 1 was
elected controller three seconds after the kill and took over the dead broker's
partitions at nine more, as in every other run; but its own transaction coordinator
and its alter-partition channel were each waiting out connection set-up timeouts to
the dead broker's address (8 to 12 s apiece — a killed container's address does not
refuse, it does not answer), and the alter-partition channel did not find the new
controller until 20 s after it was elected, at which moment dozens of partitions
changed at once. What the client was waiting for in that time is not established: twenty more
kills of the active controller with the client's debug log on, 45 s away each, did not
show it again (12.4 to 15.0 s every time, nobody took over). If it is what the brokers'
logs suggest — the leader's next transaction could not be registered while the
coordinator still had the last one's markers to write to a partition whose leader had
been the dead broker — then its length is the brokers' own patience with a peer that
does not answer, and in that run it was bounded by the broker's return, not by
anything that timed out first. That is the residue: about one kill of a controller in
forty, 26 s where 14 is usual, the leadership not moved. The one-broker test bounds the
silence at the takeover ceiling for that reason, and asserts as before that the
leadership stays.

**Every kill** (the roles are the broker's at the moment it was killed: the active
controller, the transaction coordinator of the deployment's writer, leader of the
leader topic's partition, of a state partition, of an input partition, of the sink's):

| run | broker killed (what it was) | sink silent, s | slowest trade, s | leads afterwards | leadership moved |
|---|---|---|---|---|---|
| 1 | 1 (coordinator, state, sink) | 9.6 | 9.6 | a, term 1 | no |
| 2 | 2 (controller, coordinator, state, input) | 13.1 | 1.3 | a, term 1 | no |
| 3 | 3 (controller, coordinator, state) | 13.6 | 0.7 | a, term 1 | no |
| 4 | 1 (coordinator, leader-topic, state, input, sink) | 10.0 | 1.7 | a, term 1 | no |
| 5 | 2 (controller, coordinator, state, input) | 13.3 | 13.3 | a, term 1 | no |
| 6 | 3 (controller, coordinator, state, input) | 14.0 | 1.8 | a, term 1 | no |
| 7 | 1 (state) | 10.0 | 1.4 | a, term 1 | no |
| 8 | 2 (controller, state, sink) | 14.5 | 2.2 | a, term 1 | no |
| 9 | 3 (leader-topic, state) | 10.8 | 2.1 | a, term 1 | no |
| 10 | 1 (controller, coordinator, state, input) | 13.8 | 1.3 | a, term 1 | no |
| 11 | 2 (controller, coordinator, leader-topic, state, input) | 13.2 | 1.3 | a, term 1 | no |
| 12 | 3 (coordinator, state) | 8.8 | 0.6 | a, term 1 | no |
| 13 | 1 (controller, coordinator, state, input, sink) | 13.6 | 1.8 | a, term 1 | no |
| 14 | 2 (state, input) | 10.4 | 10.4 | a, term 1 | no |
| 15 | 3 (controller, state, input) | 13.5 | 13.5 | a, term 1 | no |
| 16 | 1 (state, input) | 8.4 | 1.6 | a, term 1 | no |
| 17 | 2 (controller, coordinator, state, sink) | 12.6 | 0.6 | a, term 1 | no |
| 18 | 3 (controller, coordinator, leader-topic, state, input) | 14.1 | 1.4 | a, term 1 | no |
| 19 | 1 (state, input) | 7.7 | 0.6 | a, term 1 | no |
| 20 | 2 (controller, coordinator, leader-topic, state, input) | 13.2 | 1.1 | a, term 1 | no |
| 21 | 3 (controller, coordinator, leader-topic, state, input) | 13.7 | 13.7 | a, term 1 | no |
| 22 | 1 (controller, coordinator, leader-topic, state, input, sink) | 14.0 | 14.0 | a, term 1 | no |
| 23 | 2 (coordinator, state, input) | 10.6 | 1.6 | a, term 1 | no |
| 24 | 3 (controller, state, sink) | 14.1 | 1.9 | a, term 1 | no |
| 25 | 1 (controller, coordinator, state, input) | 12.7 | 0.5 | a, term 1 | no |
| 26 | 2 (state, input) | 9.7 | 1.1 | a, term 1 | no |
| 27 | 3 (controller, state, input) | 26.0 | 14.0 | a, term 1 | no |
| 28 | 1 (controller, coordinator, state, input) | 13.0 | 0.5 | a, term 1 | no |
| 29 | 2 (controller, coordinator, leader-topic, state, input) | 15.1 | 2.4 | a, term 1 | no |
| 30 | 3 (controller, leader-topic, state, input) | 14.2 | 1.7 | a, term 1 | no |

**Every rolling restart** (all thirty: the sink equal to the model, node a leading at
term 1 afterwards, node b standing by, no node process ended):

| run: silent s / slowest trade s | | | | | |
|---|---|---|---|---|---|
| 1: 1.2 / 0.9 | 2: 1.4 / 1.3 | 3: 1.2 / 1.1 | 4: 1.2 / 1.2 | 5: 1.6 / 0.7 | 6: 0.7 / 0.7 |
| 7: 1.4 / 1.1 | 8: 1.3 / 1.3 | 9: 1.0 / 1.0 | 10: 1.1 / 1.5 | 11: 0.6 / 0.6 | 12: 1.0 / 1.5 |
| 13: 1.2 / 1.1 | 14: 0.7 / 0.7 | 15: 1.1 / 1.4 | 16: 1.0 / 1.5 | 17: 0.8 / 0.7 | 18: 1.1 / 1.1 |
| 19: 1.1 / 1.1 | 20: 1.1 / 1.2 | 21: 1.7 / 1.3 | 22: 1.4 / 1.4 | 23: 1.3 / 1.4 | 24: 0.7 / 0.7 |
| 25: 1.8 / 1.8 | 26: 1.1 / 0.7 | 27: 1.3 / 1.3 | 28: 1.2 / 1.1 | 29: 1.2 / 1.2 | 30: 0.9 / 0.9 |

**Before and after.** The same two tests as they stood at the start of this step —
stock client settings, containers at changing addresses, the next broker killed 2 s
after one survivor called the cluster whole — for comparison: three kill-in-turn runs
had 9.0, 35.4 and 22.0 s of silence and the leadership moved in one; three rolling
restarts 6.1, 0.7 and 0.7 s. With fixed addresses and the reconnect delay alone
shortened, four kill-in-turn runs: 10.6, 32.8, 38.2 and 35.0 s, moved in two — which is
what showed that the cadence itself (a second broker killed while the first was still
recovering) was the larger part. Twenty-two more runs of that old cadence with the
client's debug log on, to catch a both-fenced ending: the leadership moved in seven,
silence up to 41 s, and no run ended with both nodes fenced. That ending was reproduced
by sending its cause instead (`StaleEpochRequestTests`). A socket timeout of 12 s,
tried before 6: one broker frozen for 60 s silenced the sink for 26.1, 25.8 and 13.6 s
in three runs — the writer meets the frozen broker twice, as a partition's leader and
as the coordinator.

**The suites on three brokers, from a cluster started seconds before** (H4). The
profile was removed and started, and the commands are CLAUDE.md's, with its three
environment variables:

| command | result |
|---|---|
| `dotnet test --project tests/Incremental.Service.Tests -- --filter-method "*broker*"`, first thing on the new cluster | 7 of 7 passed, none skipped, 7 min 15 s (the six broker tests and one whose name has the word) |
| `dotnet test --project tests/Incremental.Kafka.Tests` | 126 total: 121 passed, 4 skipped (three need Kafka Connect, one needs the log cleaner to have run), **1 failed** — `A_row_too_large_for_one_record…` read a table's head in the instant after its commit returned and found the version before it. That is the flake round two saw once: a transaction's markers trail its commit, by longer on three brokers. The test now waits for the version to be readable. Two more full runs after that: 122 passed and 4 skipped; 123 passed and 3 skipped |
| `dotnet test --project tests/Incremental.Service.Tests` | 194 total: 193 passed, 1 skipped (CDC), 0 failed, 10 min 33 s — the six broker tests among them, and `Dead_letters_go_to_a_topic…` under its new rule |

Before this step the first of these failed after three minutes at its own precondition
(it waited for the replicas of `__transaction_state`, which a new cluster does not have
until something has run a transaction), and the dead-letter test failed with 47 dead
letters for 32 dropped records against a bound of 40.

**The scale rig and the broker that refuses unknown clients** (rerun after the changes;
the critic had not). Both runs were made while a broker-kill loop ran on the three-broker
cluster beside them, so they show that the scenario still passes with supervised nodes
and the new client settings, and are not figures to set against S6's.

| | default broker, 200,000 rows and 200,000 orders of state | `kafka-sasl`: OAUTHBEARER only, deny-by-default ACLs, 20 s tokens, 100,000 rows |
|---|---|---|
| exit code | 0 | 0 |
| `kill -9` of the leader's node → the standby leads | 4.0 s | 4.4 s |
| → the killed node's process started again by its supervisor | 0.6 s after the kill | 0.3 s |
| → that node a warm standby again, from cold | 5.7 s after its start | 6.3 s |
| manual takeover → leader | 1.7 s | 1.9 s |
| → the fenced node (exit 3) a warm standby again | 5.6 s after the request | 4.3 s |
| tokens handed to the leader's and the standby's clients | — | 245 and 88 |

The ACL set is `grant-acls.sh`'s, unchanged: the node was granted nothing new for the
write-path probe (it may describe its own topics already; `__transaction_state` it may
not, and the probe then judges by the leader topic alone). The authorizer's log was not
read for what it refused.

**Tests that run on one broker, by default** (H13).

- `StaleEpochRequestTests`: the request that fenced both nodes, sent by the test over a
  socket (`FindCoordinator` and `InitProducerId` by hand) at the moment a log's first
  transaction is open. The broker answers it "concurrent transactions", the log's
  acquisition is fenced, the log is a reader with a new client, the stale request's
  retry is refused, and the same log acquires at term 1 on its second attempt.
- `NodeSupervisorTests`, four, with no broker: the supervisor over a program that ends
  as a file of exit codes says — started again at once after 3 and 4; after 400, 800,
  1,600 and 3,200 ms for one that keeps failing early; not after 0 or 2; SIGTERM passed
  on and the node's exit code returned.
- `KafkaLeadershipTests`, eight new (the eighth: a leader that commits again once the
  brokers are back keeps leading past the ceiling): a standby fenced in the middle of its takeover
  stands by again and then leads; a log fenced at either point of its acquisition is a
  reader again and can acquire later, with none of its terms in the topic; a takeover
  is given up when the leader is heard, and the leader is never touched afterwards; at
  the ceiling a standby holds while the brokers can commit nothing and takes over when
  they can; the write-path probe against the real broker and Java's hash; a step-down
  whose resignation cannot commit closes the writer and the standby takes over on
  silence (round one's fix, untested until now); a takeover does not announce a term
  below one it has read (the term floor).
- `BrokerFaultSeamTests`, five, without a broker: an offsets answer with no offset is a
  passing failure and never an empty partition (`OffsetOf`); admin attempts are 0.5 s
  doubling with a metadata refresh between them; a refused lookup of the unjoined group
  is passed over and nothing else is (`ConsumeAssigned`); serving a consumer's callbacks
  polls it once (`ServeCallbacks`); the client defaults, with a caller's own winning.
- `KafkaServiceTests.A_standby_fenced_before_its_takeover_commits_is_still_a_warm_standby_and_leads_when_asked_again`:
  the same loss under a whole service node — 409 `takeover-lost`, the pipeline still
  `Built`, a second request leads, the sink equals the model and the input was resumed,
  not read again.
- `KafkaServiceProcessTests.A_supervised_node_that_is_fenced_resigns_or_is_killed_comes_back_as_the_standby_by_itself`:
  two supervised processes through a takeover (exit 3), a step-down (exit 4) and a
  `kill -9`, each time ending as leader and warm standby with nobody restarting anything.
- `WarmStandbyTests.A_pipeline_removed_on_the_leader_is_let_go…` now asserts that the
  plugin version is unloaded on the standby (`LoadState` `stored`). It asserted
  `Users == 0`, which is what the catalog says and was true with the lease still held:
  that is why round two's mutation survived. It is killed now.
- `KafkaServiceTests.Dead_letters_go_to_a_topic…` no longer allows "eight duplicates".
  It asserts the rule: no dropped record is there more than twice, and every one that
  is twice was dropped by one step — the old leader's last, published and not committed
  (H4: on three brokers that step held all fifteen records of the second half).
- `KafkaFailoverProcessTests.A_leader_paused_past_the_timeout…` failed once in this
  work with one `progress` event too many after `SIGCONT`. The signal can land between
  a commit and the line that reports it; the test now asserts that nothing the woken
  process reports was committed past where its successor resumed.

**Mutations: 28, all killed in the end.** Nineteen in the Kafka library (a fenced acquisition final
again; the reset keeping the old client; a fence met while acquiring completing
`WhenFenced`; a heard leader not stopping a takeover; the ceiling ignoring the brokers,
and holding whatever they say; a lost takeover ending the node; a failed resignation
leaving the writer open; an unset offset read as an offset; no term floor; a refused
group lookup ending the reader; callbacks not served; an admin attempt given the whole
timeout; nothing refreshed between attempts; a client default dropped; the coordinator
placed by another hash; no pause for the leader once the brokers are back; that pause
never ending; an admin client given the six-second socket timeout), seven in the
supervisor (a fenced node not restarted; a killed node not restarted; a process that
refused its arguments restarted; no growing delay for a process that keeps failing; a
growing delay for a node that only lost the leadership; a stop request not passed on;
a node that was asked to stop started again), one in the service (a lost manual
takeover not answered as a conflict) and round two's survivor in `SyncFromCatalog`.
One survived its first run: with the pause after the brokers' return made endless the
ceiling test still passed, a minute late, because the stall it uses to silence the
leader gives up after sixty seconds; the test now bounds the wait from above. Not
covered by any test: a takeover whose wait is called off in the instant it completes
(`KafkaLeadership.PromoteAsync` carries on with it; the race could not be provoked).
Script and results: `h1/mut.py` and `h1/mutations.txt` beside the gaps file.

**Deviations.**

- **A node can go `Promoting → Standby`.** CLAUDE.md said that fenced is final and that
  there is no demotion in place; both still hold for a node that has been the writer.
  The new path ends before that.
- **`AcquireWriterAsync` changed what it throws** when it is fenced part-way (above).
- **`ReconnectBackoff`, `SocketTimeout`, `RequestTimeout` and the metadata interval are
  no longer the client's defaults.** A host that set none of them gets ours; one that
  set any keeps its own.
- **The kill-in-turn test leaves a killed broker dead for 15 s and waits for every
  broker's own word before the next kill.** Before, it restarted the broker after 8 s —
  inside the nine the controller gives it — and went on while it was still recovering.
  That cadence is what produced most of round two's "leadership moved" runs; it is a
  two-broker outage, which the 70-second test covers on purpose.
- **The brokers of the `kafka3` profile are still combined broker-controllers.** MSK's
  controllers are separate nodes; losing the broker that is also the active controller
  is the profile's worst case (12 to 15 s) and not MSK's.
- **CI's connector-stack job expects `skipped: 6`** for the service tests, not 4.
- **The scale rig's phase H no longer starts the killed node**; its supervisor does, and
  the rig measures from the supervisor's start line. The rig's two nodes are both
  killed with their supervisors before the cold-start phase. The node that takes over
  by hand in phase I was started by its supervisor with its first arguments, so it has
  the plugin directory, which the node S6 started by hand did not: that takeover's
  `plugin directory` stage is 0.7 s where it was 2 ms, and the takeover 1.7 s where S6
  measured 1.0. It is what a node configured with a plugin directory pays at every
  takeover; it was not looked into.

**Left undone.**

- Nothing ran on MSK, on a cluster with separate controllers, or in CI.
- The write-path probe reads the controller's view. With no controller quorum that
  view does not change, and a standby can still take over at the ceiling into a
  cluster that cannot commit; it then waits inside the acquisition and is called off by
  the leader's first heartbeat or completes (Open for James 48).
- A broker that stops answering is found out by each client separately. A pipeline
  whose sink has a producer of its own waits once more after the writer has recovered
  (the metadata refresh makes that unlikely, not impossible); `"producer": "log"` on
  the sink removes the extra client (Open for James 49).
- `NodeSupervisor` does not tie the child's life to its own: `kill -9` of the
  supervisor leaves the node running with nobody to restart it. On Windows it cannot
  pass a stop request on and kills the child.
- No state-size, memory or latency figure was taken under a broker fault; the reader's
  two timings are what was measured.

#### Files, a missing input, the admin UI, CI and the front door (H3, H5, H8–H12)

**The files connector (H9, and the S3 half of H7's listing).**

- *Paged listing.* `IObjectStore` gains `ListPageAsync(prefix, after, limit)`: the
  objects after a name, in ordinal order, a page at a time. It is a default interface
  member over `ListAsync`, so a store written before it (the owner's S3 store) still
  compiles and is correct, at the cost of a whole-prefix listing per call.
  `LocalObjectStore` implements it (it still walks the prefix's directory, but opens
  only the page's files). The source asks only for names after its position, 1,024 at a
  time; the sink's clean-up pages too. An S3 store implements it as one `ListObjectsV2`
  with `StartAfter` and `MaxKeys`.
- *An object that arrives behind the position.* Closed for objects that arrive within
  `settle` (default 5 s) of the greater names: a listing stops at the first object
  whose modification time is younger than `settle`, and nothing after it is read before
  it. The seen-set alternative was not built: it reads late objects out of order, and
  its window would have to be in the checkpoint, which the partition → offset encoding
  cannot hold (Open for James 22, 52). The cost is `settle` of latency on every object;
  the example's quote files set `"settle": "00:00:00"`, which is right for a writer
  that names in order and writes whole.
- *Deletes from files.* A keyed input with `"format": "changes"` reads
  `{"key":…,"row":…}` and `{"key":…,"deleted":true}` — what the upsert sink writes.
- *Keeping a prefix short.* `retain: N` on an append sink; `compactAfter: N` on an
  upsert sink folds committed objects into a `….snapshot.jsonl` that is written before
  anything it replaces is deleted. The reader's rules (`FileSinkFormat`) start from the
  newest snapshot and list again when an object goes missing under them. The example's
  ad-hoc pruning (`World.TrimSinkObjectsAsync`) is gone: its sinks say `"retain": 200`.
- *A replaced leader's late object.* Reasoned again: the lease check before each write
  leaves the write in flight across a takeover, and the old reader rule (highest term
  per step) cannot void an object for a step the successor gave this sink nothing for.
  Object names now carry the base — the checkpoint step the writing generation started
  from — and a reader voids an older term's objects above any later term's base. It is
  reproduced in `The_late_object_of_a_replaced_leader_is_void_once_its_successor_has_written`
  (objects placed as the two leaders would leave them, then read). The gap that remains:
  until the successor's first object in that prefix, the late object reads as real
  (Open for James 53). Old names are still read (Open for James 54).
- *Settings per kind of slot (H10).* `ConfigProperty.AppliesTo` says which kinds of slot a
  property means something on; the `files` and `kafka` connectors fill it in, and the
  form shows a slot only its own (a property the spec sets anyway is shown, tagged
  "not for a …", so it can be cleared). The connectors refuse another kind's settings
  as before.

**A file, prefix or topic that is not there (H8).** A table's `file` loader that finds
no object, and never loaded one, reports `Waiting` (new `TableSourceState`) with what
it waits for; a `files` source that has found nothing under its prefix says so in
`SourceStats.WaitingFor`. After `ServiceOptions.MissingInputGrace` (30 s) each is one
event and a health reason, `loader-waiting` or `source-waiting`, which makes the node
`Degraded`; both end with an event when the first object is there. Dry runs look first:
`PlanDeclareAsync` (the admin's declare dry run) for a table's object or topic, and a
pipeline deploy dry run for each input through the optional `IPresenceChecked` (the
`files` and `kafka` connectors implement it; a custom table source can implement
`ITableSourcePresence`). What they find is `ActionPlan.Warnings`, shown above the plan in
the UI; the action is never refused for it (Open for James 55). Each look is bounded at
5 s and a store or broker that does not answer gives no warning. The Kafka look is a
metadata request on a consumer built from the connection (its settings and token
handler, `ServeCallbacks` first), which does not create the topic — the test asks twice
and still finds it missing. Kafka sources and loaders on a missing topic still fault at
start and are retried, as before; only the files paths have a waiting state.

**The admin UI (H5, H10, H11).**

- `morph` now sets the properties of form controls from the new tree: a select's
  selection (read from the new options before they are moved into the page), a
  checkbox's or radio's `checked`, and a field's `value` unless it has the focus. The
  deploy form can no longer show one definition and deploy another.
- The morph test (`AdminUiMorphTests`, `tests/Incremental.Service.Tests/Ui/`) runs ten
  cases in a headless Chrome with the UI's own `app.js` and no package: it starts the
  browser with `--dump-dom` and reads the result the page writes. It is skipped where no
  Chrome or Chromium is found; GitHub's Ubuntu runner has one. Against the morph from
  before this change it fails two cases, one of them with H5's exact symptom (the
  definition select reads `desk-exposure` after a notice came and went above it).
- Numeric help reads "At least 1." instead of "From 1 to ∞."
- The takeover dialog and its plan say what a warm standby does: start what it built,
  build the rest. The plan counts each.
- The cluster page's readiness card has a *Pipelines* line (built, still being built,
  to be built at a takeover) and a new card lists each pipeline's last start (warm or
  built at start, and the plugin, build, start, edges and total times).
- A pipeline that has stepped less than once a second and a half over the last half
  minute — a sink's `minInterval`, or an input that is quiet — has its rates over its
  last two intervals and shows *every 2 s* in the list's step column; the rates carry
  `batchedEveryS` and `rateOverS`. The page does not say which of the two causes it is.
- The tables list and the table page read again every three seconds: a loader's first
  load publishes no event, so the page showed the table as declared ("Rows 0") beside a
  row browse of 27.
- `pipeline.replace.confirmed` has a plan line, and the audit shows "2, unchanged"
  instead of "2 → 2".
- The example's second plugin build is version 2.0.0, so the version list tells the
  two apart by more than the import time.
- Waiting tables and inputs show on the overview's attention list, the table page, the
  tables list, the pipeline page and the topology's input panel; dry-run warnings show
  above the plan in every dialog.

**CI (H3).** The workflow runs on pushes to `enrichment-service` and by hand. The first
runs failed in the connector-stack job for two reasons, each fixed at its root: the
summary line carries colour codes on a runner (`\e[m  skipped: 0`), so the anchored grep
never matched; and `LeadershipProbe`'s listener could throw "Not listening" from
`DisposeAsync` when stopped just after an answer — a real race, reproduced by a new test
that stops a listener the moment it has answered, 300 times (it failed 3 runs of 3
before the fix). A third run failed on two things a slow runner shows and this machine
had not: `KafkaSourcePositionTests` asserted the loader's position before its one-second
idle commit, which the runner had already passed (the test now accepts either state,
told apart by the table's version), and the compaction test skipped because the
broker's one cleaner thread, looking every 15 s, had not reached its topic in 90 s
behind the other tests' compacted topics (the compose broker now has two cleaner
threads and a 1 s back-off; that test compacted in 2 s here afterwards).

**The front door (H12).** The example, AdminHost, the scale rig and both failover apps
print their usage for `--help` and exit 2 on an option they do not have
(`tests/Shared/CommandLine.cs`, linked into the four; the example's own table in
`ExampleOptions`).

#### Memory: where a node's working set was, and what it is now (H6)

**The finding.** A node's working set was three to five times what it held. Measured
again on the build this step started from (`ceb4c5e`; the scale rig, a million wide
rows, a million orders of state, twenty pipelines, one local broker):

| | leader | standby |
|---|---|---|
| live on the managed heap, after a full collection | 2,140–2,170 MB | 2,300–2,350 MB |
| working set | 7,700–7,900 MB | 5,000–5,300 MB |
| of it, committed by the collector | 5,400–5,800 MB | 4,600–4,750 MB |
| of it, anonymous memory that is not the collector's | 2,040–2,390 MB | 310–370 MB |

It was read from the processes themselves: `GCMemoryInfo` for the collector's side,
`/proc/<pid>/smaps` and glibc's `mallinfo2` for the rest (a `dotnet-gcdump` of the
leader stops at ten million objects and says nothing at this size). Three things, none
of them the rows:

1. **The Kafka client's fetch queues, kept by the C allocator: 2 GB.** `smaps` shows
   the memory in 64 MB-aligned anonymous mappings — glibc's per-thread arenas — and
   `mallinfo2` says what is in them: 2,135 MB held, **34 MB in use**. `malloc_trim(0)`
   returned 1.7 GB of it in 80 ms. What had filled the arenas was the client's queue of
   fetched records, which is per partition (the documentation says per consumer) and
   was 16 MB for a source and 8 MB for a log reader: twenty pipelines reading a
   backlog of a million orders from a four-partition topic. With every queue at 2 MB
   the same point of the run had 0.8 GB there, and no stage was slower.
2. **The collector kept what the burst had needed: 3.3–3.6 GB on the leader, 2.3 on
   the standby.** Not garbage — after a forced full collection the committed figure on
   the standby did not move — but free regions the collector holds for its next
   allocations. It gives one back only when it has gone unused through some twenty
   collections. The leader under steady load collected 3 times in 30 s on eight
   heaps; the standby, which allocates 16 MB a second once warm, not at all.
3. **The settings.** Eight heaps and nothing else: no fragmentation limit, so the old
   generation was never compacted after the load.

What it was not. The large-object heap holds 460–590 MB at a million rows and all of it
is live: the arrays of the table's map and of the state stores' dictionaries (33 MB
each for a store of a million entries), not what the bootstrap threw away. The
retained history of a table is bounded by `SharedTableOptions.TailChanges` (65,536 row
changes): 1 MB after a load of five million rows, and at most the superseded rows of
65,536 changes — some 85 MB at this width. The state held twice (item 37) is not in
these numbers at all: the rig's stateful pipelines commit continuously and a warm
standby's copies go at the leader's first checkpoint; it is an idle pipeline's
problem, dealt with below. A cap on the young generation's budget
(`GCgen0MaxBudget`) changed nothing, as S6 had found.

**What was tried** (the leader at the end of the steady load; 30 s of it in the first
block, 90 s in the second; every run a fresh prefix; the first block before any fix):

| the nodes' settings | committed by the collector | pauses, share of the steady load | the state fill | |
|---|---|---|---|---|
| eight heaps (round two) | 5,578 MB | 0.4 % | 24.6 s | 3 collections in 30 s |
| four heaps | 3,785 MB | 1.8 % | 29.9 s | |
| four heaps, young-generation budget capped at 64 MB | 3,632 MB | 1.6 % | 29.4 s | no effect |
| four heaps, conserve memory 7 | 2,919 MB | 1.6 % | 29.5 s | |
| … and `MALLOC_ARENA_MAX=2` | 3,280 MB | 1.4 % | **75.1 s** | 1.75 GB still in the arenas |
| workstation collection (and two arenas) | 2,434 MB | **17.5 %** | 99 s | input lag 3,300 records |
| four heaps, conserve 7 — 90 s | 2,621 MB | 2.1 % | 29.7 s | |
| eight heaps, conserve 7 — 90 s | 2,955 MB | **0.8 %** | 25.5 s | |
| adaptive (DATAS) — 90 s | 2,658 MB | 2.3 % | 28.6 s | |
| four heaps, conserve 7, fetch queues 2 MB — 90 s | 2,613 MB | 2.2 % | 29.4 s | not the collector's: 803 MB, from 2,024 |

And with the fixes in, the conserve setting alone (eight heaps, 90–150 s of steady
load, the leader's working set at the end and at its peak, the standby's at the end;
the state fill of every eight-heap run of each setting, those before the fixes among
them):

| | runs | leader | leader's peak | standby | state fill |
|---|---|---|---|---|---|
| nothing conserved | 4 | 3,389–3,464 MB | 6,640–6,820 MB | 3,570–3,680 MB | 24.1–24.7 s in six runs, 28.7 s in one |
| conserve 5 | 5 | 3,045–3,215 MB | 5,650–6,100 MB | 3,450–3,570 MB | 24.1–24.9 s in four runs, 28.7–29.1 s in three |
| conserve 7 | 4 | 2,927–3,042 MB | 4,750–5,510 MB | 3,450–3,640 MB | 24.1 and 25.5 s in two runs, 28.7–29.3 s in seven |

The fill has two speeds, about 24.5 s and about 29 s. It was the slower one more often
the more was conserved — but the slower runs also came after the five-million-row
runs, whose topics the broker was still deleting, and the settings were not run in an
order that would tell the two apart. What happens in a slow fill was not looked into
(the longest stall in one was 0.4 s, so it is not one long collection). The node's
settings were written with 7, then 5, and end with the setting left out: 0.3 GB at a
million rows and nothing at five million does not pay for a setting that asks for
full blocking collections on a leader. The runs under *After* are without it.

The workstation row and the arena row have a second variable in them (the workstation
run also had two arenas); the workstation run was not repeated without it, because 240
young collections in 30 s is the collector's own doing.

**What was built.**

- *A source's fetch queue of 2 MB* (`KafkaSourceOptions.FetchBufferKilobytes`; it was
  16), and both options' documentation corrected to "per partition". The logs' readers
  (`KafkaLogOptions.FetchBufferKilobytes`) were run at 2 MB for most of this step and
  put back to 8: over five runs each a table of a million rows restored in 4.3–6.0 s
  at 2 MB and in 3.9–5.0 s at 8, and a node holds those queues only while it restores.
- *`MemoryKeeper`* (`src/Incremental.Service/MemoryKeeper.cs`), run by every node every
  `ServiceOptions.ReleaseUnusedMemoryEvery` (30 s), one per process. It asks glibc for
  `mallinfo2` and, with 64 MB or more free, calls `malloc_trim(0)`; on another C
  library it finds no such function and does nothing, for good. And it ages the
  collector: while the committed figure is over what was live at the last collection
  by 256 MB and by a quarter, and fewer than twenty-five collections have happened
  since the last look, it asks for twenty-five young-generation collections a quarter
  of a second apart (20–30 ms each at these sizes). If the committed figure has not
  come down by a tenth of the excess at the next look, it rests — a minute, two, four,
  up to ten — and starts afresh when the excess is gone. The decision is a class of
  its own (`CollectorAgeing`) so that it is tested without a heap. It never asks for a
  full collection.
- *The node's collector settings* in one file, `src/Incremental.Service/Node.Gc.props`
  (server, concurrent, not adaptive, eight heaps; conserve-memory deliberately not
  set, item 58), imported by the
  scale rig and by the example (with four heaps; it had one).
- *An idle pipeline's second copy* (item 37): `ICheckpointStorageProvider.HoldsRestoredCopy`
  (a default member, false; `ChangelogStorageProvider` answers from its origins), and
  in `Circuit`'s pump: a circuit that started on such state and has been idle for
  `RestoredCopyGrace` (2 s) commits once without a step — the offsets, the clock and
  the step marker as recovered, under its own manifest. A follower that reads that
  commit drops its own copy, as it does for any checkpoint under its layout.
- *What a node holds, said by the node.*
  - `Incremental.Diagnostics.ObjectSize` (internal): what an object costs on the heap,
    by reading its fields — headers, strings at two bytes a character, arrays, what
    the references lead to, each object once per walk; the empty string and a
    collection's comparer left out as shared, and a walk ended at 4,096 objects so
    that a row which leads to something vast is not sized to the end. `SizeSampler`
    keeps a running mean over the first 256 items offered and one in 512 after, and
    stops sizing for good if sizing ever throws: it runs where a table publishes and a
    store commits, and may cost the estimate but never the write. Both belong to one table or one
    store and hold no static: a store's types are a plugin's.
  - `SharedTable.EstimateMemory()` → `TableMemoryEstimate`: rows (the mean of rows
    sized as they were published or restored, plus the key map: 100 bytes for an
    eight-byte key), indexes (each index counts its groups and its entries as the
    writer changes them), history (the tail counts, per version, the changes that
    replaced or removed a row).
  - `ChangelogStorageProvider.EstimateSize()` → `StateSizeEstimate`, store by store:
    entries (the indexed store now counts its own as they come and go; the others
    are a dictionary's count), the objects of an entry (sampled where the entry is
    written to the log, or applied from it on a follower), the store's own maps from
    their capacity, the mean record size, and the entries that wait as bytes or are
    held twice.
  - Service: `TableStats.Memory` (with the in-memory log's history counted where the
    log is in this process), `PipelineStatus.StateSize`, `ProcessMemory` extended
    (live at the last collection, fragmented, not the collector's, the pause share of
    the last minute, the collector's settings) and carried in the peer probe,
    `ServiceNode.MemoryRelease`.
  - Meter: `incremental.service.table.memory` (tag `part`: rows, indexes, history),
    `pipeline.state_entries`, `pipeline.state_memory`, `process.working_set`,
    `process.gc.live`, `process.gc.committed`, `process.other`,
    `process.gc.pause_ratio`.
  - Admin API: `memory` on a table, `stateSize` on a pipeline and `stateStores` on its
    page (the largest, at most `MaxRows`), `holds` and `memoryRelease` on `node`.
  - Admin UI: a *Memory* column on the tables list and a *State size* column on the
    pipelines list; the table's page has *Memory* where it had *Estimated size* (the
    JSON figure is in its text); the pipeline's page has a *State* card, store by
    store, with a notice while entries are held twice; the cluster page has the
    process's figures for both nodes and a *Memory* card that sets the working set
    against tables, state, the rest of the heap, what the collector holds ready, what
    is not the collector's and the program's own files, as one bar and a table, with
    the largest tables and states under it. Verified in Chromium at 1440 px and
    390 px, light and dark: no console message, no horizontal overflow.
- *The rig*: `--gc` (the nodes' environment: `node`, `round2`, `stock`, or pairs),
  `--client` (a client property for every client), `--memory-only`, `--trim`, `--age`,
  `--hold-seconds`; memory read every second of the steady load from each node,
  without forcing a collection; the pause share of the steady load alone; the longest
  stall (a thread that sleeps 5 ms) and the last full blocking collection; the node's
  own estimates beside the heap's growth; a timeline of every node's working set; the
  nodes' `smaps` kept.

**After** (the same two scenarios; every figure from one run unless a range is given):

| a million rows, a million orders of state | before | after |
|---|---|---|
| leader: working set / live | 7,704 / 2,159 MB | 3,464 / 2,158 MB |
| standby: working set / live | 5,297 / 2,299 MB | 3,579 / 2,354 MB |
| leader's working set at its peak (the end of the state fill) | 7,739–7,923 MB | 6,817 MB |
| collector's pauses, share of the leader's life | 7.3 % | 4.8 % |
| … of the steady load | 0.4 % | 1.9 % (the keeper's collections among them) |
| the longest the leader stood still, steady load / load and fill | not measured | 33 ms / 418 ms |
| `kill -9` → the standby decides | 3,029 ms | 3,001 ms |
| … → leader | 3,916 ms | 3,845 ms |
| … → first record in the first / last sink | 4,094 / 4,118 ms | 3,896 / 4,039 ms |
| takeover by hand: longest silence in a sink | 1,848 ms | 1,689 ms |
| a cold standby until warm | 12.2 s | 12.7 s |
| one node from nothing → first record in the last sink | 12.6 s | 13.2 s |

| five million rows, a million orders of state | before | after |
|---|---|---|
| two nodes on this machine | did not fit (S6) | fit: the whole scenario, exit 0, three times |
| leader: working set / live | 15,132 / 7,221 MB (one node) | 8,156 / 7,211 MB |
| standby: working set / live | — | 9,276 / 7,421 MB |
| leader's working set at its peak | not read | 14,471 MB; 10,846 MB under a 10 GB heap limit |
| collector's pauses, share of the leader's steady load | not read | 1.5 % |
| the longest the leader stood still, steady load / load and fill | not read | 30 ms / 1.7 s |
| `kill -9` → first record in the last sink | — | 4,100 ms |
| takeover by hand: longest silence in a sink | — | 1,812 ms |
| a cold standby until warm | — | 38.9 s |
| one node from nothing → first record in the last sink | 33.9 s | 35.4 s |

The same scenario was run through with the two conserve settings on the way: at a
million rows the leader's working set was 3,215 MB (5) and 2,960 MB (7), the last sink
had its record 4,296 and 4,031 ms after the kill, one node from nothing took 13.4 and
13.8 s; at five million the leader's was 8,127 and 8,115 MB, the standby's 9,265 and
8,648 MB, the peak 14,387 and 12,700 MB, the last sink 4,061 and 3,994 ms, the hand
takeover's silence 2,400 and 1,708 ms. Failover does not move with any of it.

Under `GCHeapHardLimit` of 10 GB (two nodes at five million rows, the shipped
settings): the peak 10,846 MB for 14,471, the table loaded in 34.4 s for 32.7, the
state fill in 32.8 s for 28.8, the collector's share of the leader's life 10.1 % for
7.7 % and of its steady load 2.1 % for 1.5 %, the standby's working set 8,444 MB for
9,276 and warm in 42.8 s for 38.9.

The 1.7 s is a full blocking collection while five million rows are being loaded; it
was 2.0 and 1.9 s with conserve 7, 2.1 s in an earlier run without, and 1.7 and 2.2 s
under the heap limit, so it is the load's and not a setting's (item 61). One node from
nothing led after 12.4 to 13.7 s in five full runs at a million rows, against 12.4 s in
the one run before; the difference is in reading the table back (3.9 to 6.0 s against
4.4 s). In four runs that load, fill and start cold without the failovers in between
it led after 12.0 to 12.4 s. With one run to compare with, whether it is slower at all
is not established; the fetch queue is not the reason (the same spread at 2 MB and at
8).

**The estimates against the heap.** At five million rows the node said its table held
6,506 MB (rows 5,875 MB at 1,132 bytes of objects a row, the index 629 MB, history
1 MB); the heap after the load was 6,490 MB. It said the state of the four stateful
pipelines held 796 MB (six million entries); the heap had grown by 736 MB. And a
million rows or entries at a time, in a process of their own (the harness is
`scratchpad/h6/bytes/bytes.cs`; the figures are in the guide's table):

| | measured | estimated |
|---|---|---|
| a row of 4 columns | 193 B | 189 B |
| … of 9 columns | 401 B | 396 B |
| … of 21 columns | 1,248 B | 1,245 B |
| … the same with one string instance shared by every row | 1,088 B | 1,245 B |
| … with a unique secondary index | 1,381 B | 1,377 B |
| a keyed input's record | 269 B | 268 B |
| a join's side, three records to a key | 359 B | 340 B |
| a decimal total per key | 61 B | 61 B |
| a count per string key | 87 B | 87 B |
| a deadline | 56 B | 55 B |

**Public API added.** Core: `SharedTable<,>.EstimateMemory()`, `TableMemoryEstimate`;
`ChangelogStorageProvider.EstimateSize()`, `.HoldsRestoredCopy`, `StateSizeEstimate`,
`StateStoreSize`; `ICheckpointStorageProvider.HoldsRestoredCopy` (default member).
Kafka: the two defaults. Service: `TableStats.Memory`, `PipelineStatus.StateSize`,
`ProcessMemory`'s new members and `GcSettingsInfo`, `MemoryRelease`,
`ServiceNode.MemoryRelease`, `ServiceOptions.ReleaseUnusedMemoryEvery`; eight
instruments and the `part` tag; `Node.Gc.props`. Admin JSON as above.

**Tests.** Core, `MemoryEstimateTests` (11; a collection of their own, not run beside
other tests): an object's estimate equals the bytes the collector allocates for it —
strings, arrays, records, nested collections, a dictionary — to the byte; a table's
estimate is within a tenth of what the heap holds for 120,000 narrow rows and for
120,000 wide rows with two indexes; history follows replaced and removed rows and
stops at the tail's bound; an index's estimate follows its groups to nothing; a
pipeline's four kinds of store, 100,000 orders, within a quarter of the heap and each
store's maps within bounds of their own; state as bytes, held twice, committed,
released; the indexed store's count against a model through 20,000 random operations.
`RestoredCopyTests` (3): the idle commit happens once, not at once, changes neither
state nor positions, and a follower agrees; a circuit with input commits by itself and
no extra checkpoint is written; a new state commits nothing. Service:
`MemoryFiguresTests` (4), `MemoryKeeperTests` (5: the decision, its rests of 1, 2, 4,
8, 10, 10 minutes, 250 MB of freed blocks returned to the operating system, the
interval, a node running it), the meter's two tests and the admin API's extended.
Thirty-nine mutations, all killed in the end; one survived its first run (a
follower's entries left unsized: the tests asked were the wrong ones, and the right one
did not look at the sample) and got an assertion.
`KafkaLeadershipTests.A_step_down_whose_resignation_cannot_commit…` failed two runs in
five on the unchanged tree — a heartbeat begun between the refused resignation and the
close was counted after the test had read the count — and now waits three beats before
reading it; with the close removed it still fails. And on a runner
`FileConnectorTests.A_line_that_is_not_a_record…` asserted the dead letter the moment
the sink had its last count, before the letter's own consumer had put it in the ring;
it waits for the letter.

**The core's hot paths** (they changed: a count in the indexed store, a count per row
published). `benchmarks -- --assert`: all ten contracts hold. The throughput rig
(Release, `--workers 4 --seconds 20 --warmup 8`, the heap provider), the unchanged tree
and this one in turn, three runs each:

| | before | after |
|---|---|---|
| records a second | 373.6k, 382.2k, 370.2k | 373.6k, 371.0k, 368.4k |
| allocated per record | 565–566 B | 566–567 B |
| worker CPU per record | 7.83–7.98 µs | 7.93–8.03 µs |

The means are 1 % apart and inside each other's spread; allocation did not move. The
rig's pipeline does not use a checkpointing provider, so the sampling at a commit is not
in these figures: it is one sizing per 512 entries written, which the scale rig's state
fill (24.1–24.9 s in six runs of this build, 24.6 s before) does not show.

**Deviations.**

- The service calls `GC.Collect` and `malloc_trim` (item 57). The second is a P/Invoke
  into `libc.so.6`.
- A source's fetch queue default changed (item 59).
- An idle pipeline commits a checkpoint nobody asked for, two seconds after it starts
  on restored state (item 37). Metrics count it as a commit.
- The core's write paths gained counters: one increment per row published to a table
  and one in 512 sized; the indexed store counts entries on every change; a store
  sizes one in 512 of the entries it writes at a commit or applies on a follower.
  Benchmarks and the throughput rig below.
- `ICheckpointStorageProvider` has a new default member; `TableStats`,
  `PipelineStatus` and `ProcessMemory` new optional members. The probe line's `mem=`
  part is longer; a node from before reads none of it and shows "it did not say".
- The example's collector settings changed: four heaps for one.
- The table page's *Estimated size* card is gone; its figure is a sentence of the
  *Memory* card.
- The core suite takes 45 s where it took 31: the memory tests collect in full
  between their readings.

**Left undone.**

- Ten million rows. A soak: nothing here ran for more than four minutes, so the
  keeper's rests, the conserve setting under hours of churn and fragmentation's own
  full collections are reasoned, not watched.
- The bulk load's 2 s pause (item 61).
- The collector's longest pause is not an instrument: the rig measures it, a node does
  not say it.
- At five million rows the node's reading of its own `/proc/self/status` came back
  empty once per run, at the end of the state fill (*not the collector's* shows as
  unknown there), and the rig's timeline, which reads the same file from outside,
  has a gap of a minute at the same place. Not explained.
- The large-object threshold was not looked at for a node (the example has the
  examples' 1 MB; the rig the runtime's 85 KB).
- Nothing on three brokers, behind SASL, on MSK, on Windows or on another C library
  than glibc; there the keeper's allocator half does nothing and `process.other` is
  not reported.
