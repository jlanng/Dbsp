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

### Decide before production

Chosen from the numbered items below by what a wrong default costs once something is deployed:
persisted formats, upgrade rules, availability and data-loss trade-offs, exposure, and
claims only AWS can check. Each was checked against the code in round four (S8); the
numbers are the items' own, from the runs they cite.

1. **Upgrades roll, one node at a time, and a format level once raised is not
   lowered** (31; built in round five, S9). A node reads every persisted format up to
   its build's level and writes the level its deployment is at; the leader raises it
   when its standby says it reads the higher one. Until then the older build can lead
   again; afterwards it cannot. Decide how long a new build is on probation before the
   raise (`ServiceOptions.HoldFormatLevel`; the default is no time at all: the raise
   follows the second node's upgrade by a second), and never run two nodes without
   probe addresses through an upgrade without it. The cost that stays: every format a
   build has shipped needs its reader kept for ever.
2. **The `files` connector's formats are persisted** (22, 54, 63). Object names
   (`{step:D19}-{term:D6}-{base:D19}.jsonl`) and the source's position packed into the
   checkpoint's partition → offset map are what deployed pipelines resume from; changing
   either later means a new connector name and moving every spec to it. A `files` input
   on a `files` sink's prefix is refused, so pipelines are chained through Kafka or a
   table until the sink gets a commit marker.
3. **The writer's transaction timeout is five minutes** (67;
   `KafkaLogOptions.TransactionTimeout`; 60 s until S8). It is how long an outage of the
   brokers a leader rides out; past it the leader's writer usually ends and the standby
   leads, nothing lost (ten of fourteen runs at 60 s, and what decides is established:
   S8). A longer timeout costs a transaction left open for that long only when no node
   of the deployment runs; the brokers cap it (`transaction.max.timeout.ms`, fifteen
   minutes by default, MSK's too). Decide the value, and with it whether a leader that
   takes the log again in place — specified in 67, not built — is wanted at all.
4. **Failover timing: `TakeoverAfter` 3 s, the ceiling 30 s** (32, 61, 75). A bulk
   load is paced since round five (61), so its young collections stop a leader for a
   fifth of a second; a node loaded close to its memory still gets a full blocking
   collection of seconds — measured at 3.1 to 3.8 s for five million rows on four
   cores with a gigabyte spare — which the 3 s does not cover; a
   standby takes more than a processor for about ten seconds per broker lost (75: see
   the item for what the fourth review's step found and did). A broker lost in the same
   moment as the leader is a failover of ten seconds, thirteen to fifteen if it was the
   active controller as well: the brokers' session timeout, which no setting of the
   service shortens (S8). Decide the two timeouts for the production cluster, and
   whether 75 must be fixed first.
5. **Something must restart a node after exit codes 3, 4 and 5** (50). Without it a
   deployment is one node after its first takeover; `NodeSupervisor` does it for a
   process and, since S8, `NodeSupervisor.RunInProcessAsync` for the in-memory backend's
   two nodes. Decide what does it in production (ECS, Kubernetes, systemd) with "start
   again at once after 3 and 4, with a growing delay after 5".
6. **The admin surface's exposure** (4, 5, 25, 30). With no authorization policy the API
   answers loopback only; rows and exception messages are off; upload and rescan are
   off; but a caller nobody authenticated may name itself (`AcceptOperatorHeader` is on),
   and the audit then records `name (self-declared)`. Audit entries written under that
   rule cannot be re-attributed later: decide the authorization policy, and whether the
   header stays on, before the first operator uses it. Plugin code runs on both nodes
   (warm standby builds).
7. **What only AWS can verify** (33, 47, 48, 51, 59; the critic's J5). `UseMsk` is example
   code that nothing calls; the IAM action table is translated from the ACLs tested on
   `kafka-sasl`; the probe's broker question behind ACLs, token refresh over hours, a
   zone loss has not been run; there is no S3 `IObjectStore`. Controllers separate from
   the brokers were run here in S8 (eight runs, one machine), not on MSK. MSK's default
   configuration allows unclean leader election on clusters without tiered storage: the
   log's own topics now refuse it for themselves (78), the cluster's default and the
   sinks' topics are the operator's.
   Decide who runs that check, on what cluster, and what has to pass.
8. **Bounded state per tenant** (69, 70, 72). A keyed input without event time never
   expires a row; a state that only grows is flagged and nothing more; the reference
   definition's `openSeconds` defaults to an hour, about 9 GB of state per node at 1,000
   trades a second. A per-spec limit on state entries is cheap to add on the existing
   counts; decide whether production needs one, and the default window.
9. **Event-time state from before manifest format 7 is refused, not migrated** (36).
   *Moot for a first deployment:* only state committed by a build before format 7
   (round three) is affected, and it needs `state=reset`. What remains is the precedent
   for the next format change: refuse and reset, or migrate.

**Decide before a second tenant:** a failing pipeline never moves the leadership and
`Failed` is not durable (6, 7); plugins compile against `Incremental.Service` rather
than an abstractions package (11), and the harness ships inside it (20); a `kafka` sink
claims no topic, so two pipelines can overwrite each other's keys (40); a per-pipeline
state limit (70); keep-or-reset asked for every change (9).

**Can wait:** 2, 3, 8, 12–19, 23, 24, 26, 27, 29, 35, 38, 41–45, 49, 52, 53, 55–58,
60, 62, 65, 66, 68, 71, 73, 74, 76, 77, 79, 80, 81, 82.

**Made moot by later rounds:** 1 (warm standbys built, S6; 30 is the decision as it now stands), 10 (roll-back
built, S6), 21 (closed by 53 once the successor writes), 28 and 46 (estimates and
counts built, S7), 34 (the broker tests are a CI job started by hand, 74; the SASL run
is still not in CI), 37 (the idle commit is what happens, S7), 39 (superseded by 52).

### The items

1. *Moot (built, S6).* **Warm circuits on the standby** (default; built in S6 after S1 had taken the
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
10. *Moot (built, S6).* **An incompatible `keep` rolls back and reports.** *Alternative:* leave the pipeline
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
21. *Moot once the successor writes (53).* **The files sink's reader rule is "highest term per step"** (S6), which a late
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
28. *Moot (the alternative built, S7).* **A table's size is an estimate of its data, not of its memory** (S6: the mean JSON
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
    *Round five (S9, B6):* this stays true of that one boundary and of no later one.
    From the build before round five on, a change of a persisted format is a format
    level, written only once both nodes read it; nodes are upgraded one at a time.
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
34. *Moot for the broker tests (a CI job started by hand, 74); the SASL run is still not in CI.* **The broker-stopping tests and the SASL run are not in CI.** They need three
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
    *Round five (S9, B6):* rolling upgrades do not change this, and cannot. A node
    keeps a reader for every older format that holds what the newer one needs: format
    7 is read by every build since, format 8 by a node of format level 2, and a
    format-6 checkpoint of a pipeline without event time as before. A format-6
    checkpoint of an event-time pipeline is not a format to read but state that was
    never written — no watermark, no deadlines — so it is refused at every level and
    the pipeline needs `state=reset`. The first format level is therefore the formats
    of the build before round five (manifest 7), not anything older.
37. *Moot (the alternative is what happens, S7).* **State bytes are kept beside the typed stores until the first commit after a
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
39. *Moot (superseded by 52).* **A files source lists again at most once per `pollInterval` while it has names to
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
46. *Moot (built, S7).* **A pipeline's state size is readable but not shown** (S7):
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
48. **At the ceiling a standby asks each broker the metadata names whether it is
    there, and does not take the metadata's word** (S7, as changed by the third
    review: the controller's lists never change once the controller quorum is gone, and
    read "writable" through three of four two-broker outages). The question is a
    `DescribeConfigs` of the broker's own configuration, which only that broker
    answers; a refusal counts as an answer, so it needs no permission.
    *Alternative:* prove the path by writing — a record of the standby's own to the
    leader topic through a plain producer before it takes over. That tests exactly
    what matters, replication included; it is a second kind of write outside the fenced
    producer and it moves the leader topic's end, which is the sign of life of a leader
    inside a long transaction, so the standby would have to tell its own record from
    the leader's.
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
    greater names, at `settle` of latency on every object. "Left alone" is the store's
    modification times against each other, or the reader's own time since it first saw
    the object — never the node's clock against the store's (the third review); what it
    still trusts is that a modification time is when the object appeared, which a file
    moved into place with an old time is not. *Alternative there:* count only what the
    reader has seen for itself — no trust in any stamp, and a backlog read at a page
    (1,024 objects) per `settle`. *Alternative to the default:* `0` by default
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
    with the settings before this step). `TakeoverAfter` is 3 s. *Alternatives:* load
    a large table before the standby is started; raise `TakeoverAfter` for the load;
    or have the table writer pace a bulk load. **Built in round five (B2, S9):** the
    writer paces a large load (`SharedTableOptions.LoadPacing`), which takes the
    long young collections away (three million rows: the longest stop 0.55–0.81 s →
    0.19–0.20 s, the load 5 % longer). It does not take away a full blocking
    collection the collector makes because the process is near its memory threshold,
    which is what five million rows on this round's machine measured (3.1–3.8 s
    either way): that is sizing, and stays a limit. `TakeoverAfter` was not raised.
62. **Eight heaps, fixed in the node's settings** (S7), four in the example.
    *Alternative:* leave the count to the runtime (a heap per core): on this machine's
    twenty cores that was not run with the rest of the settings, and the adaptive mode
    that chooses the count by itself paused three times as long.
63. **A `files` input onto what a `files` sink writes is refused** (S7, the third
    review; it was allowed and documented as "one pipeline feeding another"). A sink's
    objects are rewritten under their names when a step runs again, folded away by
    `compactAfter` with the deleted keys left out, pruned by `retain`; a source that
    follows names loses changes and deletes without a word. *Alternative:* make it
    work — the sink writes a commit marker (or puts the committed step in each name) so
    that a reader reads only steps that will not be run again; a snapshot keeps the
    tombstones of the keys deleted since the one before it; the source keeps "read
    through step N" in its checkpoint, reads a snapshot it is at most one fold behind,
    and faults ("position gone", like a Kafka source) when it is further behind. That
    is a change to the persisted object names and to the source's checkpoint, and a
    reader that must keep within one fold of its writer. Until then: a Kafka topic or a
    table between the pipelines.
64. *Closed (S8, the fourth review).* **A takeover is called off for a heard leader
    only before its first request for the transactional id, and goes through after.**
    As S7 built it the call-off was honoured between one-second slices of
    `InitTransactions`, on the reasoning that a request "may" have left; it had, and it
    fences a leader with a transaction open before the call returns, so every such
    call-off (three of three seen) left a fenced leader and a standby that stood down.
    *Alternative, not taken:* do not call takeovers off at all, as before S7 — the
    difference is now only the instant between a decision and its first request.
65. **A takeover of the node's own that fails after it took the log ends the process
    with exit code 5, not 3** (S7, the third review): 5 already meant "it could not
    lead" and is answered with a growing delay. *Alternative:* a code of its own (6),
    if an operator's alerts should tell "the catalog writer faulted" from "a takeover
    failed"; the events and the log already do.

66. **The records of the broker runs are in the repository**
    (`docs/design/runs/`, S7): the third review found S7's tables citing a scratch
    directory that no longer existed. *Alternative:* keep citing nothing and say so; a
    table nobody can check is then all there is.
67. **`KafkaLogOptions.TransactionTimeout` is five minutes, and a leader whose
    transaction ran into it is not saved** (S7 left it at 60 s without knowing what the
    timeout did; S8 established it and raised it). Past the timeout the transaction
    fails in the client or is aborted by the coordinator, and in ten of fourteen runs
    the writer was finished: the node exited 3, the standby led, nothing was lost, and
    the sink was silent no longer than in the runs where the leader survived — what a
    leader's end costs is the restart and, until the node has restored, a warm standby.
    *Alternatives for the value:* 60 s as before, if a transaction that a dead writer
    left open must not outlast a minute when no node is running; fifteen minutes, the
    brokers' default cap, which rides out anything shorter and stops every node from
    leading on a cluster whose cap is lower. *The alternative to a timeout at all* is a
    leader that takes the log again in place, specified here and not built, because it
    needs "`Fenced` is final" changed:
    - *When.* The writer's client has failed fatally with `INVALID_TXN_STATE` from an
      abort, or with `PRODUCER_FENCED` / `INVALID_PRODUCER_EPOCH`; the leader topic's
      last term, read after the failure, is still this node's; and the peer's probe
      says it is a standby that is not promoting.
    - *How.* A new client initialises the transactional id, the last term is read
      again, and if it is still this node's the writer carries on under the same term:
      nothing was committed in between, so the node's tables, storage and circuits
      are what the log holds.
    - *What cannot be closed.* Checking that nobody else has taken the log and taking
      it are two requests. The one request that does both — `InitProducerId` with the
      producer's own id and epoch, which the client uses inside an abort — is not
      offered to an application, and not after a fatal error. A standby at the ceiling
      takes over in exactly this window: in one traced run its takeover came 60 ms
      before the leader's refused abort. A leader that initialised the id then would
      fence its successor, find a higher term, and have to end after all — two nodes
      restarting where one would have, which is what *How both nodes ended fenced* was.
    - *What it would save.* A restart per outage longer than the timeout: no output,
      by the runs above. With the timeout at five minutes that is outages of more than
      about four and a half.
68. **A table's statistics give the log two seconds to say where it ends, and after
    a question without an answer nobody asks for five** (S7, the third review): a
    standby's lag is then "not known" in its status, the admin's table pages and the
    gauges for as long as the brokers that hold the table's partitions do not answer.
    *Alternative:* the last lag that was measured, with its age; it would be a figure
    on a page that is wrong exactly while it matters.

69. **A keyed input with no event time has no expiry** (S8). Its rows go when the
    source deletes them, and a retroactive join over it keeps every record; the way to
    bound either is event time (`WithEventTime(…, evictWhenFinal: true)`,
    `EvictWhenFinal()`). *Alternatives:* an expiry by processing time — the age of the
    row on the node — which a restart would apply differently from the run it replaces
    (the replayed steps expire other rows), so the sink could differ from an
    uninterrupted run; or a limit on the number of keys, evicting the oldest, which has
    the same problem and an arbitrary order besides. Both are a day's work; neither was
    built, because each gives up the property every other operator has, that a
    restored circuit does what the uninterrupted one would.
70. **A pipeline whose state only grows is flagged, and nothing more** (S8): after
    `ServiceOptions.StateGrowthAlarmAfter` (30 min) with no fall and a rise at twelve of
    fifteen samples of one of its stores it is marked, an event is raised and the gauge
    `pipeline.state_growing` is 1 (store by store, and not for a store that closes on a
    watermark that moves, since the fourth review). It is not a health reason and stops nothing.
    *Alternatives:* make it `Degraded` (a node with one leaky tenant pipeline would
    then always be degraded, and the readiness probe is not the place); a per-spec
    limit on state entries (`circuit.maxStateEntries`) that faults the pipeline when
    passed — a tenant's choice, cheap to add on the same counts, not built for want of
    a number anybody has asked for. **Built in round five (B3, S9):**
    `circuit.maxStateEntries`; the flag stays as it was.
71. **A trade id reused after its trade is final is a new trade** (S8): nothing is
    retracted for the old one, so an upsert sink keyed by the id overwrites the old row,
    or keeps it if the new trade is filtered out. *Alternative:* an id that must stay
    unique could be checked, but only against what the input still holds — the open
    trades — so it would catch nothing the watermark has passed.
    Whether the earlier trade *is* final when the later one arrives depends on how the
    input is cut into steps: a pipeline that reads both in one step — a backlog after a
    restart, a pipeline that had fallen behind — takes them for one trade amended, and
    the first is never delivered. Found in round four's CI work: the smoke test
    published its trades in two halves and waited for `trade-enrichment` between them,
    and on a runner `large-trades` was still on the first half (19 rows short of the
    model). The smoke now waits for both; what a tenant's pipeline delivers for a
    reused id after a replay is the question this item leaves.
    *Round five (S9, K6):* an id that comes back more than the open window later, by
    the two records' own times, is another record however the input is cut into
    steps; within the window it is an amendment, no rule can tell the two apart there,
    and the guide says ids must not come back that soon.
72. **The definition's `openSeconds` defaults to an hour; the example seeds two minutes
    and the smoke test thirty seconds** (S8). An hour is a guess at how late a booking
    correction comes. At 1,000 trades a second it is about 9 GB of state per node for
    `trade-enrichment` alone (the guide's arithmetic, which is a standby's figure; the
    growth flag no longer takes the first hour of it for a leak, and the setting's
    description says what a second of it holds). *Alternative:* no default, so
    that every spec has to say — the dry run would then refuse the template the admin
    offers, which today is valid as it stands.
73. **An in-memory takeover waits up to five seconds for a sink write the replaced
    leader is inside** (S8, *CI*; `InMemoryBackendOptions.DeposedWriteTimeout`). The
    wait is what keeps the old leader's last rows from landing over the new leader's;
    a sink that hangs makes the takeover that much longer, and a write that outlasts
    the wait can still land late. *Alternatives:* no bound (a hung sink then holds an
    in-memory takeover for ever, against "nothing waits without a bound"), or a shorter
    one; the in-memory backend is for tests and for trying things out, where five
    seconds is long enough for any sink that is not broken.
74. **The broker-failure tests run in CI only when asked**
    (`gh workflow run CI --ref <branch> -f broker_tests=true`; S8, *CI*). They take a
    whole runner for a quarter of an hour, and on two shared cores what they assert
    rests on three JVMs and two node processes keeping their heartbeats.
    *Alternatives:* on every push (every push then waits for them, and a slow runner
    fails the build for a reason that is not the push's), or on a schedule (a nightly
    run on the default branch — cheap to add once the branch is merged: `schedule`
    only fires there).
75. *Closed (S8, the fourth review's step).* **A standby took more than one processor
    for as long as a dead broker was still listed as a partition's leader**: about ten
    seconds per broker lost, 1.2 to 1.4 processors here, 27,000 to 30,000 refused
    connections a second. It was the Kafka client's own doing, on the log's admin
    client only: an admin request meant for one broker asks for that broker's
    connection again at every change of any broker's state, asking resets the
    reconnect delay, and a refused connection is a change of state. Admin clients now
    keep a connection to every broker (`enable.sparse.connections=false`, in
    `KafkaClientDefaults` for an `AdminClientConfig`), which asks for nothing: 0.16 to
    0.23 of a processor at most in any second, in four runs. *What was not done:* the
    other clients still connect on demand; an admin client made from a consumer's or a
    producer's handle (a source's, a sink's, the short reader's) shares that client's
    setting, and none of them was seen to spin (two to four such requests in a run
    against 231,000). Keeping every client connected to every broker would close that
    too, at brokers × clients connections per node — which on Amazon MSK with IAM
    counts against a connection rate limit at start-up.
76. **Two in-memory nodes in one process start a node again when it loses the
    leadership** (S8, J7; `NodeSupervisor.RunInProcessAsync`, `ServiceNode.Ended`), in
    the example's `--nodes 2`, its two-node smoke and the AdminHost: the same name, the
    same address, back as the standby. *Alternative:* leave the replaced node `Fenced`
    for good, as before, and say in the UI that the in-memory mode does not restart
    nodes; the leader then shows *no ready standby* until the program is restarted. The
    in-memory cluster takes a node that leaves and one that joins, so nothing was found
    that makes the restart unsound; it was not tried with a sink that hangs across it.
77. **A node that does not lead shows no rates** (S8, J7): its overview tiles, and the
    pipelines list's rate, lag and dead-letter columns, say the figures are on the
    leader instead of showing zeroes. *Alternative:* show the columns with a dash, so
    that the list keeps one shape on both nodes.

78. **Topics the log creates turn unclean leader election off for themselves** (S8,
    J4; `KafkaLog.EnsureTopicAsync` and the blob topic). Kafka's default is off; MSK's
    default configuration turns it on for clusters without tiered storage, and with it
    a partition that has lost its in-sync replicas loses what they had committed.
    Topics that exist are not altered, and a spec's own topic settings still win.
    *Alternative:* leave it to the cluster and say so in the guide — which also spares
    a deployment on a service that refuses the topic setting (none was met; MSK accepts
    topic-level settings, by its documentation and not by a run).
79. **A standby waits two takeover timeouts for a leader after the brokers can commit
    again, and no longer** (S7; seen running out in S8; counted since the fourth
    review from the first answer that is not a hold, and held for brokers that do not
    answer as well as for brokers that say no). In one traced outage past a
    60 s timeout the leader's client was still held up six seconds after the brokers
    were back, and the standby took over; that leader's writer was about to end
    anyway. On the five-minute timeout the leader was heard within 1.1 s in every run
    read. *Alternative:* wait up to the ceiling again once the brokers are back — a
    leader that is alive and truly stuck then holds the deployment for another thirty
    seconds.
80. **A heartbeat that is read a takeover timeout or more after it was written counts
    from when its writer wrote it** (S8, J4; `HeartbeatDating`):
    the last heartbeat of a leader that died with a broker, readable only when the
    brokers give the broker up, no longer calls a takeover off or starts the silence
    again (it cost one takeover timeout, once in thirteen such takeovers). It rests on
    the writer's wall clock moving forward at the pace of the reader's; the two are
    never compared, and a record the dating puts less than a takeover timeout back is
    heard when it is read, so a clock error has to make a record three seconds old to
    matter. *Alternatives:* count from the reading always, as before S8 — nothing to
    assume about clocks, and three seconds more in that one case; count from the
    writing always, as S8 first built it — up to three seconds sooner when a dead
    leader's last record is read late by less than the takeover timeout, and a healthy
    leader whose commits take over half the takeover timeout is taken over from by a
    standby that has no probe to ask (shown by test; S8, *Finished by a second
    session*); or a monotonic
    reading of the writer's clock in the heartbeat record, which a clock that is set
    cannot disturb and a suspended host can.
81. **Nothing guards a pipeline against an event time in the future** (S8, the fourth
    review). The watermark follows the newest event time read, so one record stamped
    far ahead makes every record behind it late until the feed's time catches up; the
    drop is counted and announced (`late_records`), and that is all. *Alternatives:*
    a bound on how far ahead of the node's clock a record's time may be before it is
    dead-lettered — which makes what a pipeline does depend on when it runs: the same
    record replayed a day later passes, so a restored circuit no longer does what the
    uninterrupted one did; or a bound on how far the watermark may advance in one step,
    which delays every honest gap in the feed (an overnight quiet) by the same amount.
    *Round five (S9, K3):* built as a bound from the later of the node's clock and the
    newest event time the input has taken, a day by default, applied where the record
    enters; a record beyond it is a record in error.
82. **Adopting `EvictWhenFinal` or `EvictWhen` on state that exists needs `state=reset`**
    (S8, the fourth review): the checkpoint's rows and facts have no deadlines and the
    start is refused. *Alternative:* give them deadlines at the first start, from the
    rows and facts themselves. It needs the stores to enumerate what they hold, which
    `IValueStore` and `IIndexedWeightStore` do not offer (the changelog and in-memory
    stores could; the Redis provider would scan), a marker in the checkpoint that it
    was done, and one pass over the state by the leader that first runs the new
    generation. A deadline that is a function of the record itself could instead be an
    index the store keeps, with no record of its own in the state topic — which would
    also halve what `EvictWhenFinal` writes there and what a standby holds for it (S8,
    *The early critic's items*, E2: 16 records and 3.9 KB per open trade, against 8 and
    about 2.6 KB).

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
| a rolling restart: each broker shut down (`docker stop -t 30`), five seconds, started, until every partition has all its replicas in sync † | 21 runs (twelve as built, six with `request.timeout.ms` 5 s, three back at the default). In 16 the leader kept leading under term 1, the pipeline did not restart and the sink was complete. In 5 the leader lost the leadership (three of the twelve, two of the six), at the third broker — by then the leader of most partitions and the coordinator of the service's transactions: the leader's open transaction waited 10 to 30 s for the brokers to take its records (the writer logs it: `Kafka transaction has been waiting 30 s for the brokers to take its records`) and the standby saw no heartbeat for `TakeoverCeiling` and began a takeover. In 4 of those it completed and the old leader exited with 3; the test as it then was failed there ("the leader did not stay the leader", or a broken pipe to the node that had exited), **so the sink was not compared in any rolling run in which the leadership moved**. In 1 (`k3-rt5-2`) both nodes ended `Fenced` and exited with 3, and the test timed out with the sink at 7,930 rows of 10,660. The three rolling runs made after the test was corrected had no move |
| each broker killed in turn (`docker kill`), eight seconds, started † | nothing lost in 3 runs. In one nobody took over; in two the standby took over at the ceiling after the third broker's kill (the cluster took 43–48 s to be whole again that time, 8–17 s otherwise), and the old leader exited with 3 |
| one broker shut down (`docker stop`, which is a controlled shutdown and not a kill, as this row first said), the leader `kill -9`ed 1.5 s later, the broker started under the new leader † | the standby led 3.5–5.3 s after the kill (3 s of that is `TakeoverAfter`); the sink complete; the killed node back as a warm standby. Three runs. A broker lost in the same moment as the leader, without a word to the controller, costs more: S7, *The third review's findings*. |
| two of three brokers killed for 70 s — no controller quorum, nothing writable — then started † | the standby had started its takeover at the ceiling and completed it 0.6–3.8 s after the cluster was whole again, at term 2 (four runs); the old leader was fenced and exited with 3; the sink complete, the pipeline not restarted. (Before fix 3 below, that takeover failed and both nodes had exited.) |

† *No record of this run is in the repository (round four's check): the runs were kept in a session's scratch directory, which no longer exists. The row is the account written at the time; `docs/design/runs/` holds every run made since.*

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
open keys. That a warm takeover does not depend on it was claimed here "by
construction (the follower applies each deadline as it reads it)", and was wrong as
built: the follower applied each deadline into a queue that nothing ever took from, so
a standby's queue grew for as long as it stood by and the takeover's first step paid
for it. *The third review's findings*, below, has the measurement and the fix.

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
whole run. Scripted; each run is one execution of the named test on a deployment of
its own. (The scripts and the per-run records of this table were kept in a scratch
directory beside the gaps file — `h1/runs.sh`, `h1/series.sh`, `h1/*.jsonl` — which
no longer exists, so these rows cannot be checked against records any more. The two
two-broker rows were measured again in the third review's step, below, did not
reproduce as stated, and are replaced there; the records of those runs are in the
repository, `docs/design/runs/`.) For every run the test
asserted that the sink equals the model and that exactly one node leads with the
pipeline running while the other stands by, warm; it recorded the sink's longest
silence as a reader of committed records sees it, the longest any trade took from the
input's acknowledgement to its row, who led, and every exit a supervisor saw.

| scenario | runs | sink = model, one leader, one warm standby | leadership moved | node processes ended | sink silent: min / median / max |
|---|---|---|---|---|---|
| one broker killed, 20 s away — it was **not** the active controller † | 10 | 10 | 0 | 0 | 7.7 / 9.9 / 10.8 s |
| one broker killed, 20 s away — it **was** the active controller † | 20 | 20 | 0 | 0 | 12.6 / 13.6 / 15.1 s, and once 26.0 s |
| the active controller killed, 45 s away, the node's client log at debug † | 20 | 20 | 0 | 0 | 12.4 / 13.6 / 15.0 s |
| rolling restart: each of the three brokers shut down, 5 s, started † | 30 | 30 | 0 | 0 | 0.6 / 1.1 / 1.8 s |
| one broker frozen (`docker pause`) for 20 s † | 6 | 6 | 0 | 0 | 10.6 / 14.7 / 15.7 s |
| one broker frozen for 60 s † | 3 | 3 | 0 | 0 | 12.6 / 13.4 / 14.4 s |
| the same, the Kafka client's default settings (two sets of three) † | 6 | 6 | **3**, at the ceiling | 3 (exit 3, started again) | 8.3 / 35.3 / 60.6 s |
| every broker killed in turn, 15 s each, the next when every broker calls the cluster whole † | 2 | 2 | 0 | 0 | 13.1 and 13.9 s |
| two of three brokers away for 40 s — three stopped, three killed (S8, on the five-minute transaction timeout) | 6 | 6 | 0 | 0 | 54.1 / 55.2 / 55.9 s |
| two of three brokers away for 70 s — the same (S8) | 6 | 6 | 0 | 0 | 85.1 / 85.8 / 86.3 s |
| the same with the transaction timeout at 60 s, as it was in S7 (S8) | 6 | 6 | **4**: the leader's writer ended on its timeout, the standby led | 4 (exit 3, started again) | 85.9 / 86.1 / 87.8 s in five, and once 72.3 s and a second silence |
| one broker killed in the same moment as the leader's process (S8: the time is from the kill to the standby leading; these runs do not time the sink) | 21 | 21 | 21, which is the test | 1 each (the killed leader) | to a new leader: 8.7 to 10.6 s (eight runs; 11.9 s once, before a fix) when the broker was not the active controller, 13.2 to 14.5 s (eleven) when it was, and once 34.8 s |
| one broker killed and away for 10 min 10 s, the leader's process killed after 5 min † | 1 | 1 | 1, which is the test: the standby led 3.5 s after the kill, on two brokers | 1 (the killed leader; started again, a warm standby on two brokers) | 10.8 s, at the broker's loss |

† *No record of this row's runs is in the repository: see the paragraph above the table. The two-broker rows were measured again, with records, in* The third review's findings *(series 1 and 2, on a 60 s transaction timeout: 6 of 6 kept the leader at 40 s, 55 to 61 s silent; 3 of 4 lost it at 70 s), and again in S8,* What a quorum outage and a broker's loss cost*, whose runs are the rows above: `docs/design/runs/2026-11-14-j4-*`.*

One thing the 70-second runs of that step showed stands, though their row does not:
a node started by its supervisor while the brokers were still down could not create
its topics within the admin client's six-second socket timeout and ended with exit 9;
its supervisor started it again. That timeout is no longer applied to admin clients
and topic creation is retried.

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

| run † | broker killed (what it was) | sink silent, s | slowest trade, s | leads afterwards | leadership moved |
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

† *These thirty runs' records were in the same scratch directory and are gone; the table is the account written at the time.*

**Every rolling restart** (all thirty: the sink equal to the model, node a leading at
term 1 afterwards, node b standing by, no node process ended):

| run † : silent s / slowest trade s | | | | | |
|---|---|---|---|---|---|
| 1: 1.2 / 0.9 | 2: 1.4 / 1.3 | 3: 1.2 / 1.1 | 4: 1.2 / 1.2 | 5: 1.6 / 0.7 | 6: 0.7 / 0.7 |
| 7: 1.4 / 1.1 | 8: 1.3 / 1.3 | 9: 1.0 / 1.0 | 10: 1.1 / 1.5 | 11: 0.6 / 0.6 | 12: 1.0 / 1.5 |
| 13: 1.2 / 1.1 | 14: 0.7 / 0.7 | 15: 1.1 / 1.4 | 16: 1.0 / 1.5 | 17: 0.8 / 0.7 | 18: 1.1 / 1.1 |
| 19: 1.1 / 1.1 | 20: 1.1 / 1.2 | 21: 1.7 / 1.3 | 22: 1.4 / 1.4 | 23: 1.3 / 1.4 | 24: 0.7 / 0.7 |
| 25: 1.8 / 1.8 | 26: 1.1 / 0.7 | 27: 1.3 / 1.3 | 28: 1.2 / 1.1 | 29: 1.2 / 1.2 | 30: 0.9 / 0.9 |

† *The same: these runs' records are gone with the scratch directory.*

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
(`KafkaLeadership.PromoteAsync` carries on with it; the race could not be provoked —
the third review's step moved that decision into the log and tests it at both points
after the fence). The script and its results were `h1/mut.py` and `h1/mutations.txt`
in the same scratch directory, and are gone with it.

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

That last reading was wrong, and the skip came back: six of the next ten runs of the
job. The third review's step printed the broker's `log-cleaner.log` from the job (it is
a file in the container and was in no output) and the counts in the skip's message, and
both kinds of run show the cleaner at work within a second of every roll. What differed
was the segment being written when the test starts to wait: the cleaner never cleans
it, nothing is written while the test waits, and where it begins depends on the pace
of the run, because a segment also rolls when its first record is older than
`max.compaction.lag.ms`. 47 offsets were left in it on this machine; 186 and 263 on the
runner, which made 893 and 959 records where the test accepts 857 of 3,428 offsets. No
wait would have changed that. The test's state topic now has 4 KB segments in place of
16 KB, so what cannot be cleaned is a quarter of what it was at most. The same job
failed once on `ABatchTheBrokerRefuses_FailsTheWrite…`, sixty seconds without the
acknowledgement of a first record on a topic created a moment before: a sink does not
create its topic and believes a broker that says it is unknown, and a broker answers
from its own copy of the metadata, which follows the controller's. The tests' topic
helpers now wait until the broker knows the topic and its leaders, and the test reports
a failed delivery as that. This one is read from the code and was not reproduced.

After those two changes the job's Kafka step passed in three runs of three (774
records left, 2 s). Two of the three — runs started by hand on the same commit beside
the push's own, which was green — then failed in the service step, each on a test
that had not failed on a runner before, and neither is explained:
`Killing_the_leader_process_mid_stream…` on its wall-clock target (the takeover's
table stage took 2,164 ms for 21 tables against "under 2 s"; 118 to 185 ms in the
five other runs of the job that reached it and 108 to 140 ms on this machine, and
the catalog stage of the same takeover took 122 ms where it takes 9 to 28), and
`A_table_opened_again_while_the_standby_builds_on_it…` (in memory: thirty seconds
without the new leader's sink following the table it writes). Neither came back
here: the second in 25 runs alone on two cores, 6 runs of the in-memory classes on
four, and 30 runs of its class on two cores with three busy loops beside it (one of
which failed another test of the class, `A_spec_replaced_while_the_standby_fetches…`,
once, in 0.3 s); the first not with a table's statistics asked without pause
through four takeovers of twenty tables (table stage 108 to 132 ms, as without). The
service tests start their Kafka classes while some two hundred in-memory tests run
beside them on a runner's four cores; that both failures fell in that stretch is
all that is known.

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

#### The third review's findings

A review of the round-three work confirmed twelve findings. Eleven are fixed in code or
made true in the documents, one is fixed by refusing what could not be made right in
the time (a `files` input on a `files` sink's prefix), and one more defect turned up
while the fixes were being measured and is fixed with them (a takeover decided on a
silence that had ended). The step was made by two sessions: the first was cut off
after the fixes, their tests, the core's mutation runs and the first series of broker
runs; the second read its uncommitted changes and its records, gave the bounded lag
question and the two mutants that had survived their tests, ran the mutations still
outstanding, wrote the second series up from its records and ran the verification.
What each finding was, and what was done:

**Event-time state (the core).**

- **A warm standby's deadline queues grew for as long as it stood by.** The heap
  deadline store drops a stale queue entry when `TakeDue` reaches it, and a follower
  never calls `TakeDue`. The review measured it on a copy of the build: 20,000 steps of
  a windowed pipeline, the leader's stores at 60 deadlines, the standby's queue at
  400,000 entries and 24 MB with the estimate still saying 8,220 bytes, and the new
  leader's first step at 36 ms where the next took 0.05 to 0.08. The store now keeps
  its queue within twice its map and 64 entries at every write that leaves an entry
  stale, whoever writes it, and the estimate counts the queue's array
  (`durability-and-standby.md`, *A follower's queue, which nothing takes from*). The
  sentence in this section that called a warm takeover independent of the number of
  open keys "by construction" is corrected where it stands.
- **A remembered `Lookup` under `EvictWhen` kept a deadline for every version a fact
  ever had**, each a changelog record keyed by the whole fact, until the key closed:
  5,000 deadline entries beside one remembered entry after 5,000 corrections of one
  order (the review's measurement). A fact that is taken back whole now gives its
  deadline back (`EvictionTracker.Forget`); the guide's sentence about what the
  deadlines cost says what this operator's are.
- **An input named `$watermarks` or `$watermark-feed` lost its committed position at
  every restart**: `Start()` skipped every entry of the offsets marker that began with
  `$watermark`, and the service's dead-letter envelope left out every position whose
  key began with `$`. Both now go by the clock's keys exactly.
- **A table restored or followed from Kafka held a second key object per row** for a
  key that is an object (a string): the log decodes the key and the row apart, and the
  estimate counts a key as a reference to "one of the row's own". The table now holds
  a row from a log under the row's own key where the selector gives an equal one, so
  the estimate is right and the strings are not held — about 64 bytes a row for a
  20-character key, by the review's arithmetic; it was not measured at a million rows.

**The leadership.**

- **A takeover that heard the leader after it had fenced it stood down.** The token
  that calls a takeover off was honoured to the end of the acquisition. The review
  read it in a node's log after a two-broker outage: the standby's `InitTransactions`
  completed when the brokers returned, the leader's last heartbeat was read while the
  standby was reading the term, the standby stood down — and the leader, fenced, ended
  at its next transaction. Nobody led for six seconds, on top of the outage. The log
  now looks at that token only until the transactional id is taken
  (`KafkaLog.AcquireWriterUnlessAsync`); after it the takeover goes through. A
  takeover given up before that point no longer waits one to two takeover timeouts
  before it may try again, because of the one case that is left and cannot be closed
  from the client: a request that had left before the client was put away. If it
  lands, the leader ends and the standby takes over on its silence like on any dead
  leader's (Open for James 64).
- **A takeover was decided on a silence that had ended** — found here, not by the
  review. See *The runs* below: with the brokers asked one by one the questions before
  a ceiling takeover take a second or two, and the decision stood on the silence
  measured before them. The silence is measured again when the answers are in.
- **The hold at the ceiling did not engage when two brokers went at once.** The
  write-path probe believed the cluster's metadata, and the metadata is the
  controller's: with the controller quorum gone it never changes. Measured here with a
  probe program against the `kafka3` profile, brokers 1 and 2 killed in one command:
  for the thirty seconds they were away the surviving broker reported `leader 1, isr
  [1,2,3]` whenever it answered, and timed out "waiting for controller" in between;
  a `DescribeConfigs` of each broker's own configuration, which the client sends to
  that broker alone, timed out for brokers 1 and 2 at its 1.5 s and was answered by
  broker 3 in under 10 ms. The probe now reads the partition from any broker's
  metadata and asks each broker named there directly; a leader that does not answer,
  or fewer replicas in sync that answer than the minimum, is "nobody can commit"
  (`durability-and-standby.md`, *The leadership rules*).
- **A takeover that failed for the node's own reasons exited 3**, which a supervisor
  answers at once and for ever. It exits 5 now (`LeadershipStatus.TakeoverFailed`,
  `NodeLeadershipStatus.TakeoverFailed`, `ServiceNode.ExitCodeFor`), which the
  supervisor slows down. A node that another node fenced in the middle of its takeover
  is still 3.

**The runs.** Same machine and profile as the runs above (three brokers, each also a
controller, replication factor 3, `min.insync.replicas` 2; two supervised node
processes, `TakeoverAfter` 3 s, `TakeoverCeiling` 30 s, the peer probe on; 200 trades a
second). Each run is one execution of a test in `KafkaServiceProcessTests` on a
deployment of its own; its record is a line of
a `…-broker-failures-series{n}.jsonl` under `docs/design/runs/` and its summary a line
of the `….measurements.log` beside it, with the commands that made them in the
`README.md` there. In every run the test asserted that the sink equals the model and that one node
leads with the pipeline running while the other stands by, warm.

**Series 1** — made with every change above except the second look at the decision,
which it is what found. Two of three brokers away, asked for 40 s (records:
`docs/design/runs/2026-11-12-broker-failures-series1.jsonl`):

| brokers | away for | the standby's `takeoversHeld` when read | leads afterwards | leadership moved | node exits | sink silent |
|---|---|---|---|---|---|---|
| two stopped (`docker stop -t 1`, one after the other) | 42.7 s | 103 | a, term 1 | no | none | 58.4 s |
| two stopped | 42.6 s | 24 | b, term 2 | **yes**: the standby held through the outage, then took over 20 ms after it had heard the leader | a: 3 | 59.7 s |
| two stopped | 42.4 s | 114 | a, term 1 | no | none | 45.7 s |
| two killed (`docker kill`, one command) | 40.4 s | 6 | a, term 1 | no | none | 55.8 s |
| two killed | 70.4 s (see below) | 3 | b, term 2 | **yes**: the same | a: 3 | 86.3 s |
| two killed | 70.4 s (see below) | 3 | a, term 1 | no | none | 85.7 s |

In all six the standby held at the ceiling, which is what the review's runs lacked in
three of four: its log says why each time — `….leader [0] is led by broker 1, which
does not answer`, or `has 1 replica(s) in sync that answer (1 listed) and needs 2`.
The two that moved did so after the outage. Both logs read `taking over (ceiling;
leader silent for 00:00:00.0200000)`: the brokers had just returned, the standby's
questions took a second or two to be answered, the leader's first heartbeat arrived
meanwhile, and the takeover went ahead on the silence it had measured before asking.
That is the defect listed above, and the fix is in the series that follows.

Two of the killed-broker outages were 70 s and not the 40 asked for: the test read
the standby's counter before it restarted the brokers, and the standby's status took
thirty seconds to answer (below, *Deviations*). The last of them is a leader that
survived 70 s without a quorum, past its 60 s transaction timeout; why that
transaction did not fail is not established.

A takeover with one broker lost (the same file):

| broker 2 | `kill -9` of the leader → the standby leads | of which silence until the decision | taking the log and committing the term |
|---|---|---|---|
| shut down 1.5 s before the kill | 3.9 s | 3.0 s | 0.7 s |
| shut down 1.5 s before | 3.4 s | 3.0 s | 0.2 s |
| shut down 1.5 s before | 3.9 s | 3.0 s | 0.6 s |
| killed in the same moment as the leader | 9.8 s | 8.4 s | 0.7 s |
| killed in the same moment | 9.4 s | 3.0 s | 6.1 s |
| killed in the same moment | 8.8 s | 3.1 s | 5.7 s |

So S6's 3.5 to 5.3 s is the figure for a broker that was shut down, which hands its
partitions over before it goes. A broker that dies with the leader costs the brokers'
own time to give it up on top — the nine seconds of `broker.session.timeout.ms` —
which the standby spends either before it can confirm the silence (it cannot read the
leader topic's end from a dead partition leader) or inside `InitTransactions`. 8.8 to
9.8 s in these three; the review measured 13.4 to 16.3 s in three of its own, which is
what the controller's loss adds when the broker killed was the active controller, and
25.8 s once on a node's first lead with a plugin import pending. Which roles broker 2
had in these three runs was not recorded; the series that follows records them.

**Series 2** — the build with the second look at the decision, the test starting the
brokers again on time, and the standby asked for its status eight seconds before the
end of each outage (records:
`docs/design/runs/2026-11-12-broker-failures-series2.jsonl`). Two of three brokers
away:

| brokers | away for | the standby's `takeoversHeld` | its status, asked during the outage, came in | leads afterwards | leadership moved | node exits | sink silent |
|---|---|---|---|---|---|---|---|
| two stopped | 40.0 s | 24 | 20 ms | a, term 1 | no | none | 57.4 s |
| two stopped | 40.0 s | 20 | 20 ms | a, term 1 | no | none | 55.4 s |
| two stopped (see below) | 40.0 s | 21 | 19 ms | a, term 1 | no | none | 57.2 s |
| two killed | 40.0 s | 6 | 2.0 s | a, term 1 | no | none | 61.5 s |
| two killed | 40.0 s | 4 | 2.0 s | a, term 1 | no | none | 57.7 s |
| two killed | 40.0 s | 3 | 2.0 s | a, term 1 | no | none | 58.9 s |
| two stopped | 70.0 s | 85 | 20 ms | b, term 2 | **yes**, once the leader had ended | a: 3 | 88.4 s |
| two killed | 70.0 s | 25 | 2.0 s | a, term 1 | no | none | 88.9 s |
| two killed | 70.0 s | 22 | 2.0 s | b, term 2 | **yes**, once the leader had ended | a: 3 | 89.1 s |

At 40 s the leadership stayed where it was in six runs of six, stopped or killed, and
no node process ended. The standby held in all nine for as long as the leader answered
its probe, and in none did it take over from a leader it had just heard. The sink was
silent for 55 to 61 s over a 40 s outage: the brokers that had been started again took
fifteen to twenty seconds more to take the leader's transaction. That is close to the
leader's limit, which is the next paragraph's: in the 61.5 s run the transaction ran
into its 60 s timeout, twenty seconds after the brokers were back, was aborted, and
was tried again — that time the abort was accepted.

At 70 s the leadership moved in two runs of three, and in neither by the standby's
choice. The logs of both (kept by `INCREMENTAL_NODE_LOGS`; not in the repository) read
the same way. Sixty seconds into the outage the leader's transaction times out in its
client (`… message(s) timed out … requires epoch bump`). Its abort is then refused —
`INVALID_TXN_STATE` in the stopped run, sixteen seconds after the brokers had been
started again; `This instance has been fenced by a newer instance` in the killed one,
five seconds after the timeout and with the two brokers still away, when no node had
taken the log (the standby's `taking over` is 170 ms later in its log) — the client
calls either fatal, the writer is finished and the node says `Fenced`. A fenced node
no longer answers the probe as the leader, so the standby's next look, 19 ms and
170 ms later, finds a silence past the ceiling and nothing to wait for; it leads 0.8 s
later in the first run and 23 s later in the second, where `InitTransactions` waited
for the brokers. The old leader exits 3 and comes back as the standby. In the third
run the abort was accepted, twenty-eight seconds after the timeout, and the leader
kept its term. What decides between the three — what the coordinator had registered of
the transaction when the brokers went is the likely answer — was not established, and
the "fenced by a newer instance" with nobody to do it is read here as the coordinator
timing the transaction out itself; that was not shown either. It is
`durability-and-standby.md`, *A leader does not outlive its transaction timeout*: an
outage of the quorum longer than `KafkaLogOptions.TransactionTimeout` ends the
leader's term more often than not, the sink is complete either way, and raising the
timeout is the setting for a deployment that must ride out longer ones (Open for
James 67).

The third stopped run's record is whole but was made across a pause: the machine was
suspended for eleven hours after that run's brokers had been started again (its lines
in the measurements log are stamped 07:50 and 19:02). Its figures are the machine's
running time and agree with the two before it; leave it out and the 40 s rows are five
of five.

The standby's status came back in 20 ms with the brokers stopped and in two seconds
with them killed: a killed broker's partitions do not refuse a question, they do not
answer it, and two seconds is the bound a table's statistics now put on asking a log
where it ends (*Deviations*, below — in series 1 the same request took thirty).

A takeover with one broker killed in the same moment as the leader, the broker chosen
and its roles recorded (the same file; `leader-topic`, `state`, `input`: it led that
partition; `coordinator`: the transaction coordinator's partition for the log's id;
`controller`: the active controller):

| broker killed | its roles | `kill -9` of the leader → the standby leads | of which silence until the decision | taking the log and committing the term |
|---|---|---|---|---|
| 1 | leader-topic, state | 10.7 s | 10.2 s | 0.2 s |
| 3 | controller, coordinator, state | 14.5 s | 3.1 s | 11.2 s |
| 1 | coordinator, leader-topic, state, input | 10.3 s | 10.0 s | 0.3 s |
| 3 | coordinator, leader-topic, state, input | 9.0 s | 8.5 s | 0.2 s |

Nine to eleven seconds when the broker killed led a partition the standby reads or
writes, which is the nine seconds of the brokers' session timeout spent before the
standby can confirm the silence (`hears no leader but cannot reach Kafka either; not
taking over`, twice, in the first run's log) and a second of takeover; 14.5 s when it
was the active controller and the coordinator, where the standby decided at three
seconds and `InitTransactions` waited eleven for a new controller and then a new
coordinator. That last is the review's 13.4 to 16.3 s. Four runs; a first lead with a
plugin import pending (the review's 25.8 s) was not run again.

**A check on a cluster started a minute before** (2026-11-14, the tree as committed,
the profile's containers removed and started again; records:
`docs/design/runs/2026-11-14-broker-failures-check.jsonl`). The broker tests of
`KafkaServiceProcessTests`, once each with their defaults, passed in 7 min 34 s. The
70 s outage, brokers stopped: the standby held 90 times and answered its status in
20 ms; the leader's transaction timed out at sixty seconds and its abort was refused
(`INVALID_TXN_STATE`) sixteen seconds after the brokers had been started again; the
standby took over three seconds later — its reason `silence`: it had heard the
leader once more in between, and the leader then stopped answering its probe — and
the old leader came back as the standby (a: 3); sink silent 88.8 s. That is three of
four 70 s outages that moved the leadership, the same way each time. One broker
killed for 20 s, every broker killed in turn and every broker shut down in turn
moved nothing (sink silent 9.9 s, 14.2 s, 0.6 s); broker 3 away for 81 s with the
leader killed half way: the standby led 3.4 s after the kill; a takeover with broker
2 shut down 1.5 s before: 3.9 s.

**Files and tables.**

- **A `files` source on a compacting upsert sink's prefix lost deletes**, read
  snapshots as upserts, and read objects a replaced leader had written late. Looking
  for a fix found a fourth way, which needs no compaction: a step above the sink's
  checkpoint is run again after a restart and written under the same name with a
  different cut, and a source that has read the name never sees the difference. None
  of these can be put right in the source alone — a streaming reader cannot know which
  steps are committed, and cannot delete keys a snapshot no longer mentions. So the
  combination is refused, in either order, at the dry run and under the references
  lock (`ConnectorDescription.Reads` against `Owns`), and the guide says what to use
  instead. That withdraws something the guide had offered ("one pipeline's upsert sink
  can be another's keyed input"); the `changes` format stays, for objects handed on by
  whatever copies them. What it would take to allow it is Open for James 63.
- **A file-loaded table whose object had been removed reported `Waiting`, and made the
  node `Degraded`, after every takeover and restart.** "Never loaded" was a local of
  the loader's run. It is the table's to say now: a table that has a version is not
  waiting.
- **`settle` compared the node's clock with the store's modification time.** It
  compares the store's times with each other, or counts the reader's own time since
  it first saw the object (the guide, and Open for James 52 for what it still trusts).

**The documents.** S6's row for a takeover with one broker down said "killed" for a
controlled shutdown; it says what was done, and the abrupt case is measured above.
S7's tables cited scripts and records in a scratch directory that no longer exists;
they say so, and the two rows the review could not reproduce are replaced by the runs
above. The engine design's account of the 70-second outage is rewritten from them.

**Public API added.** Core: none (`EvictionTracker.Forget` and the store's bound are
internal). `Incremental.Kafka`: `LeadershipStatus.TakeoverFailed`. `Incremental.Service`:
`NodeLeadershipStatus.TakeoverFailed`; `ConnectorDescription.Reads`. Behaviour that
changed: a `files` input on a `files` sink's prefix, and a sink onto a prefix an input
reads, are refused where they were accepted; a node whose own takeover failed exits 5
where it exited 3; `AcquireWriterAsync`'s cancellation is unchanged, and the
leadership's "heard the leader" no longer cancels an acquisition that has taken the
transactional id; the `files` source's `settle` no longer reads the node's clock.
A following table's `LagVersions` is null while its log does not say where it ends
(it was a status that took thirty seconds). Test seams (internal):
`KafkaLog.BrokerAnswersOverride`, `WhyUnwritable`, `BrokerAnswersAsync`, `AnsweredWith`,
`KafkaLeadership.Heard`, `RetryNotBefore`, `ChangelogStorageProvider.QueuedDeadlines`,
`TableRegistry.LagPatience`, `LagAskAgainAfter`.

**Tests.**

- Core (`tests/Incremental.Tests`): `DeadlineStoreTests` — a store that is written and
  never taken from keeps its queue to the size of its map (removals in and out of
  order, keys that only move); a warm standby's queues after 3,000 steps of opening
  and closing windows; the estimate counts the queue it has. `EventTimeRestartTests` —
  a remembered lookup keeps one deadline per fact through 200 corrections, a
  weight-two fact taken back once, a cold copy of the log, and the key's close; an
  input named `$watermarks`, `$watermark-feed` or `$steps` keeps its position across a
  restart; the clock's own keys are refused as names and nothing else is.
  `MemoryEstimateTests` — a table keyed by a string and restored from a log that
  decodes key and row apart is estimated within a tenth of what the heap holds and
  within 2 % of its writer's figure, and holds every row under the row's own key, as a
  follower does; a selector that has changed since the rows were written leaves the
  log's keys.
- Kafka (`tests/Incremental.Kafka.Tests`, one broker): a takeover that hears the
  leader once it has taken the log goes through, at `initialised` and at `term-read`;
  a takeover given up for a heard leader holds nothing back and one lost to a fence
  waits; a leader heard while the standby was asking the brokers is not taken over
  from; a takeover that cannot start says it failed by its own doing and one that
  another node fenced does not; the probe asks each broker and judges what three
  brokers look like with two gone; a broker id nobody has is a second and a half of
  silence; what counts as a broker's answer when the question fails (an error the
  broker sent does, one the client made up and a wait that ran out do not).
- Service (`tests/Incremental.Service.Tests`): an input onto what a `files` sink
  writes is refused and so is a sink onto what an input reads (prefix above, own
  spec, dead-letter slot, suffix); the `changes` format through two pipelines with
  the objects handed on; `settle` with the store's clock six seconds and an hour
  behind the node's and ninety seconds ahead, a backlog, an object that changes while
  it settles; a loaded table whose file was removed is not waiting after a takeover
  or on a node from nothing; an input named as the clock's keys begin in a dead
  letter's positions; exit codes 3, 4 and 5 and the supervisor's delay for each; a
  backlog that is old by the store's own stamps read at once by a reader that has
  only just seen it, with the store's clock an hour off as well; a takeover of the
  node's own that fails on the Kafka backend is told as such and is exit code 5; a
  standby's table statistics over three tables whose logs stop answering come back
  after one wait, not three, with the lag not known, and know it again when the logs
  answer. Kafka, one more: a short read its caller calls off keeps the log's shared
  consumer, which then reads as before, and one that fails gets a new one.

**Mutations.** Core: 13, all killed by `DeadlineStoreTests`, `EventTimeRestartTests`
and `MemoryEstimateTests` — the queue not swept at a removal, never rebuilt, not
compacted when a key moves; the front sweep dropping live entries; the estimate
counting the map for the queue; the lookup never forgetting a retracted fact,
forgetting one still held, `Forget` doing nothing; the clock's keys skipped by prefix
again, `$watermark` allowed as an input name; a restore and a follower holding the
log's key; the row's key held without comparing. Five of them survived their first
run and got a test each (a key that only moves; the estimate with stale entries in
the queue; a weight-two fact taken back once; a selector that changed) or simpler
code (the clock's keys are no longer skipped at all: an input cannot have them). The
second session ran the thirteen again on the committed code: thirteen killed.

Kafka and service: 31 in the first list (17 service, 14 Kafka), run in two halves.
The first session ran 21 of them: 19 killed, and two survived — a backlog that waits
out `settle` object by object (the test's backlog had been seen by the reader for
longer than `settle` already, so the store's stamps were never what read it), and
the Kafka backend dropping `TakeoverFailed` on its way to the node (only the
in-memory backend's was tested). The second session gave each a test (a reader
that starts on a backlog a minute old by the store's stamps; a takeover that fails
on a `KafkaServiceBackend`), which kill them, and ran the other ten: eight killed,
two survived, both in what counts as a broker's answer — a wait that ran out counted
as an answer, and an error the client made up counted as one. The one-broker test
reaches neither branch (an id no broker has comes back as a plain client error). The
judgement is now a function of its own (`KafkaLog.AnsweredWith`) with a test of each
kind of failure, and five mutations of it are killed. Three more for the bounded lag
question, all killed: the wait not given up, a question without an answer not
standing for the other tables, the lag never asked again; and two for the short
reader (a read called off costing the consumer, a failed read keeping it). In all:
13 core and 41 Kafka and service; every one killed at the end, nine of them only
after a test or a change was made for it. The lists and results were in the sessions' scratch
directories (`mutations.txt`, `core.py`, `svc.py` and the second session's
`svc2.py`, `kafka2.py`, `kafka3.py`), not the repository.

**Deviations.**

- **Chaining two pipelines through files is refused, not fixed** (above).
- **A takeover given up between two slices of `InitTransactions` can still have fenced
  the leader.** It is answered — the standby takes over on the silence, with no wait
  of its own first — and not prevented.
- **The 40-second and 70-second rows of S7's table are replaced, not corrected in
  place**, and the older rows of that table stand on runs whose records are gone.
- **A standby's status took thirty seconds with brokers dead, and a table's
  statistics now give up on the question.** In series 1 the no-quorum test read the
  standby's counter before it started the brokers again, the status took thirty
  seconds to come back, and two "40-second" outages were 70: a following table's
  statistics ask the log where it ends, and a Kafka log waits out its operation
  timeout when the partition's broker is dead. The statistics now wait two seconds
  for that answer, whether or not the log's call honours the token, and after a
  question that had none nobody asks again for five seconds — for any table, so
  twenty tables do not wait in turn (`TableHandle.StatsAsync`,
  `TableRegistry.LagPatience`, `LagAskAgainAfter`). The lag is then not known: null
  in the status and the admin API, no value on the `table.lag` gauge. The test asks
  for the status eight seconds before the end of the outage and fails above ten
  seconds; series 2 has what it took (20 ms, and 2.0 s with the brokers killed). The
  log's call is given the same two seconds as its token and stops where it next
  looks at it; a call blocked inside the client ends at the client's timeout with
  nobody waiting for it (Open for James 68). One thing had to follow from that: a
  table's head is read on the log's one shared short-read consumer, which a
  takeover reads every table's head with, and a read that threw — a cancellation
  included — had its consumer thrown away, so that a lag question called off after
  two seconds on a slow machine would have left the next takeover a consumer that
  knows no topic. A read its caller calls off now keeps the consumer
  (`KafkaLog.ShortReadAsync`); a read that failed still does not.
- **`A_takeover_with_one_broker_down…` takes `INCREMENTAL_KAFKA_FAULT` and
  `INCREMENTAL_KAFKA_BROKER`** and records the broker's roles; its default is what it
  did before (broker 2 shut down 1.5 s before the kill).

**Left undone.**

- A `files` sink still has no commit marker a streaming reader could follow (Open for
  James 63).
- The window between two slices of `InitTransactions` (Open for James 64).
- Nothing was run on MSK or on a cluster whose controllers are not its brokers; the
  probe's direct question to a broker was tried on the `kafka3` profile and on one
  broker only, not behind SASL with ACLs (a refusal is counted as an answer, and that
  path was not exercised).
- The key instance a restored table holds was not measured at a million rows.
- A leader is not carried through a quorum outage longer than its transaction
  timeout: two runs of three at 70 s moved the leadership (Open for James 67), and
  what decides whether the timed-out transaction's abort is accepted was not
  established.
- A first lead with a plugin import pending and a broker killed in the same moment
  (the review's 25.8 s) was not run again: in the four killed-broker takeovers of
  series 2 the standby was warm, its plugin loaded.
- The older rows of S7's first broker table (one broker killed, frozen, the rolling
  restarts) stand on runs whose records are gone; only the two two-broker rows and
  the takeover with a broker down were measured again.
- Series 2 has one run made across a suspension of the machine, and its last ten
  runs were made by the script with no session watching; they were written up from
  the records and the node logs afterwards. Nothing in them was rerun.
- The log's call behind a lag question that got no answer is left to end by itself
  (Open for James 68).
- Two service tests failed once each on a CI runner and are not explained (above,
  *CI*): a takeover's table stage over its two-second target, and the warm
  standby's reopened-table test timing out. A third,
  `A_spec_replaced_while_the_standby_fetches…`, failed once here under three busy
  loops on two cores. None was reproduced on demand; the second and third are races
  in a standby's build that a slow machine reaches and this step did not find.
- None of the twelve findings was in the admin UI, so nothing was verified in a
  browser in this step.

### S8 — round four

#### Bounded state for a join over a stream of facts (J1)

The critic's J1: the guide's reference pipeline, `trade-enrichment`, kept every trade.
Its key is the trade id, every trade is a new id, and its retroactive joins are keyed
by instrument and trader, which never close, so `EvictWhen` had nothing to hang on. The
example hid it: the generator's trade ids came round at 20,000, so the state stopped at
four entries per id.

**Every definition, measured.** In memory, one node, 50 trades a second, the seeded
specs, state entries from `GET /admin/api/pipelines` at one, two, five and ten
minutes. *Before* is the tree at a0cbd57 with the new `--trade-ids` option (0: ids
never repeat; 20,000: the old default); *after* is this step's tree. Records:
`docs/design/runs/2026-11-14-j1-state.tsv`.

| pipeline | definition | state keyed by | before, ids never repeat (1 / 2 / 5 / 10 min) | before, ids repeat at 20,000 | after, ids never repeat |
|---|---|---|---|---|---|
| trade-enrichment | trade-enrichment, `minNotional` 0 | input: trade id; lookup: (counterparty, trade); joins: instrument and trader, a trade each | 11,900 / 23,940 / 60,040 / 120,200 — growing | 11,940 / 23,980 / 60,080 / 80,000 — stopped at 4 × 20,000 | 23,882 / 47,980 / 48,059 / 48,061 — level |
| large-trades | trade-enrichment, `minNotional` 20,000,000 | the same; the last join only for the trades that pass | 9,534 / 19,223 / 48,226 / 96,543 — growing | 9,564 / 19,255 / 48,257 / 64,240 | 19,130 / 38,513 / 38,582 / 38,477 — level |
| execution-quality | execution-quality | the join: (instrument, minute), closed at the minute's end (`EvictWhen`); windows: asset class and window | 12,764 / 25,588 / 18,860 / 19,006 | 12,784 / 25,608 / 18,884 / 19,020 | 12,784 / 25,608 / 18,870 / 19,020 |
| desk-exposure | desk-exposure | desk and window, closed at the window's end; the lookup keeps nothing | 16 / 16 / 26 / 28 | 16 / 16 / 28 / 28 | 16 / 16 / 26 / 28 |
| trader-desks | trader-desks | the traders table's rows, under their desk | 24 throughout | 24 | 24 |
| sanctions-alerts, credit-alerts | watchlist-alerts | nothing: an append-only lookup | 0 | 0 | 0 |

Only the two deployments of `trade-enrichment` grew. After the change they stop at the
open window: two minutes of trades (the seeded `openSeconds`, 120), eight entries each —
2 × 4 × 50 × 120 = 48,000 — at about 310 bytes an entry, 14.9 MB. Twice the entries per
trade of before, because every operator keeps a deadline beside each record; level
from the second minute on, where before was 120,200 at ten minutes and gaining 12,000 a
minute. `examples/Incremental.TradeEnrichment` is not a service and runs a fixed
narrative: its state is the live book (7,688 entries for 1,920 live trades at its
end), bounded by the source's deletes — a cancellation, a settlement — which its new
seventh act shows by settling the book and finding the state empty. It keeps its
"every trade they ever did" semantics on purpose and says, at its top, that a feed of
trades that are never deleted needs `EvictWhenFinal`.

**The engine** (`src/Incremental`):

- `GroupedEventTimeStream.EvictWhenFinal()` returns `FinalizingGroupedStream`, a staging
  type that offers only what honours it: the table `Join` and `LeftJoin` (primary key
  and index views) and the remembered `Lookup`. Each record gets a deadline one tick
  after its own event time, in an `EvictionTracker` keyed by (key, record) — the
  existing checkpointed deadline store, `{node}.deadlines.w{n}` — and the joins remove
  final records one entry at a time with a new `IIndexedWeightStore.RemoveEntries`
  (a default member that reads the group; the in-memory, checkpoint and changelog
  stores override it with a point read, the changelog writing a tombstone per entry).
  Sound because the watermark operator upstream drops every record behind the
  watermark, a final record's retraction and its amendments included.
- A table change corrects only open records. The joins apply table changes before
  they drain their deadlines, so in the step that first sees a new watermark a change
  would still reach records that are final but not yet evicted; they are skipped
  (`RecordEviction.IsFinal`), so the outcome does not depend on how the steps fall.
- `KeyedInputStream.WithEventTime(eventTime, lateness, evictWhenFinal)`: the upsert
  operator keeps a deadline per key (`Set` with the row, `Remove` with a delete, so an
  amendment to a later time moves it) and forgets rows the watermark has passed. That
  changes nothing downstream — the retraction such a row would owe is behind the
  watermark and dropped by the declaration — and `Build()` refuses a circuit that also
  reads the input without going through that declaration.
- `ZStream.WithEventTime(eventTime)`, no lateness: re-declares event time after a
  reshape, dropping what is behind the watermark, without registering a clock source.
  After `trade-enrichment`'s filter a second source would have held the watermark at
  the newest trade the filter passed.

**The service:** `ServiceOptions.StateGrowthAlarmAfter` (30 min) and `StateGrowth`, a
ring of sixteen samples per pipeline fed once a second by the sampler with the entry
count it already reads for the gauges. Flagged when, across the period, no sample fell
and at least twelve of fifteen rose; a fall clears it. `PipelineStatus.StateGrowingSince`,
the event (`state growing: … without falling once …`, and `state no longer growing`),
the gauge `incremental.service.pipeline.state_growing`, `stateGrowingSince` in the admin
API, a *growing* badge in the pipelines list, a notice on the pipeline's state card and
an entry on the overview's attention list.

**The harness:** `PipelineTestHarness.StateSize` (`HarnessStateSize`: entries in all and
by store), from the checkpoint store's committed counts
(`InMemoryCheckpointStorageProvider.CommittedEntriesByStore`, new and public).

**The example:** `trade-enrichment` has `openSeconds` (default 3,600; the seeds 120; the
smoke test 30) and the shape the guide tells tenants to copy. The generator's trade ids
no longer repeat unless `--trade-ids N` says so. The smoke test's model now knows a
trade's openness and models the sink as an upsert by id; its reference-data claims say
how many open trades were corrected and how many final ones were not; act 2 publishes
its 2,000 trades in two halves so that no step holds two trades of one id; and act 5
has a new claim that the state levels off — four batches of 125 trades, each reading at
most eight entries per open trade and per trade of the batch (1,976), against the 6,000
that keeping every one of the 1,500 ids would hold. Every definition has a bounded-state
test on `StateSize` in `Pipelines.Tests`, `trader-desks` in a new file.

**Tests and mutations.** New: `EvictWhenFinalTests` (11, core), a `final` scenario in
`EventTimeRestartTests` (the lookup, join and left join under `EvictWhenFinal`, killed
and restored at random steps on all four backings at one and four workers),
`StateGrowthTests` (5, service), six tests in `Pipelines.Tests`. Mutations, all killed
in the end: 16 in the engine (the final-record skip in both joins, eviction a no-op,
the deadline at the event time instead of a tick after, `IsFinal` with `<=`, the upsert's
deadlines never set or never taken on an empty step, the lookup ignoring the policy,
the index join never tracking, the build check, the re-declaration feeding the clock or
not dropping, the changelog's removal writing no tombstone, the checkpoint store's
removal ignoring a staged weight, and two in the definition), seven in the service and
the harness, and one in the smoke test: eviction a minute late — still sound, the sinks
all agreed — failed the levelling claim at 3,834 entries against 1,976. Two engine
mutants and one service mutant survived the first run and were killed by a test added
for them (a re-declared time of its own, a staged weight over a committed one, a quiet
sample after the flag).

**Deviations.**

- The interactive example's trade ids no longer come round at 20,000 by default: the
  bound is now the event-time window, and a feed whose ids repeat was what hid the leak.
- `EvictWhenFinal` is a separate staging type rather than a policy on `GroupedZStream`,
  so that an operator that does not honour it cannot be reached with it.
- A deadline whose record was retracted is not forgotten early by the joins (they do
  not read the weights they write); it falls due at the record's own time. The rule in
  CLAUDE.md about `EvictionTracker.Forget` is for a key's tracked records outliving the
  key; here the deadline is bounded by the same window as the records.
- `trade-enrichment` drops a trade that arrives behind its watermark (a straggler),
  which it used to carry; the smoke test's model and claims say so.
- With `EvictWhenFinal` a pipeline holds twice the entries per open record (the record
  and its deadline). *Corrected by the fourth review's step:* the deadline is kept
  under the record, so it is small only where the record's objects are shared with
  the operator's own store — on the node that read the record from its source, about
  115 bytes — and is the record's size again on a standby and after a restore; and it
  doubles what the state log is given (*The early critic's items*, E2, below).

**Not verified.** Nothing ran longer than ten minutes; the growth flag's thirty minutes
were exercised with a clock the test drives, not waited out. `EvictWhenFinal` under a
changing table across a restart is covered by the directed tests only: the restart
scenario's table does not change mid-run. No Kafka run measured the bound beyond the
smoke test's four batches. The admin UI's new badge, notice and attention entry were
not looked at in a browser in this step.

#### CI that can be trusted (J3)

The critic's J3: six of the last eight runs on the branch had failed in the
`connector-stack` job, and two service tests had failed on a runner for reasons nobody
had found. This step read every failed run on the branch, found the cause of each
failure that was still open, and proved each one — by forcing it, or by measuring the
mechanism with the broker held back — before changing anything. Then it ran one commit
ten times, and the suite twenty times here on two cores, and did the same with what
those found. Four were defects of the product: three are fixed, two of them in the
path a takeover takes, and the fourth was found in the step's last hour and is an open
item. The rest were tests that looked a moment too early.

**Every failure on the branch.** Twelve runs failed between the first push to the
branch's workflow (d459e8c, 11 November) and this step, all in `connector-stack`; seven
more failed among the 59 runs this step made, and four of the suite's 59 runs here. By
what failed:

| what failed | runs | cause | state |
|---|---|---|---|
| the job's own check that nothing was skipped | 43079381475 | the summary's colour codes hid `skipped: 0` from the `grep` | fixed in fdd5d9b (round three) |
| `KafkaLeadershipTests.A_probe_query_returns_the_whole_answer…` | 43128441300 | the test's listener was stopped while it accepted | fixed in 96055e9 (round three) |
| `KafkaSourcePositionTests.ALoader_RecordsThePosition…` | 43135482953 | the test asserted a position the loader had not recorded yet | fixed in ceb4c5e (round three) |
| `KafkaEndToEndTests.Cold_start_from_a_compacted_state_topic…` skipped, which fails the step | 43135482953, 43176457481, 43598420364, 43599691061, 43600575363, 43600582106 | the test waited for the cleaner to compact the segment being written, which it never touches | fixed in f52a991 (round three); not skipped in any run since |
| `FileConnectorTests.A_line_that_is_not_a_record_is_a_dead_letter…` | 43162497577 | the dead letter reaches the node's ring through a reader of its own, a moment after the sink has the last count; the test asserted it at once | fixed in 0ca9037 (round three); read again here, and the counter it asserts next is raised before the record is rejected, so nothing else in it races |
| `KafkaSinkTests.ABatchTheBrokerRefuses…`, one minute without an acknowledgement | 43599691061 | a topic created a moment before was unknown to the broker the sink asked, so the sink's delivery had failed and the test waited for an acknowledgement that could not come | round three's reading, proved here (below); its fix stands |
| `KafkaServiceProcessTests.Killing_the_leader_process_mid_stream…`, table stage 2,164 ms against 2 s | 43602050386 | **product:** every topic new to the log's short reader cost 100 ms | found and fixed here |
| `WarmStandbyTests.A_table_opened_again_while_the_standby_builds_on_it…`, 30 s | 43602055418 | **product:** a deposed in-memory leader's rows landing over its successor's | found and fixed here |
| `KafkaLogTests.A_short_read_its_caller_calls_off…`, "the writer's term is in the leader topic" | 43612148168 | the test read the end of the leader topic right after the commit; the marker is written after the commit returns | test fixed here |
| `FileConnectorTests.An_upsert_sink_told_to_compact_folds…`, two snapshots where one was expected | none on a runner; run 20 of this step's first loop of twenty here | the test listed the sink's directory while the sink was inside the write that folds | test fixed here |
| the Kafka smoke test, "large-trades … delivered its 1,095": 1,076 rows | 43625822629 (one of ten runs of db23011) | the smoke published its trades in two halves and waited between them for one of the two pipelines that key them by id | smoke fixed here |
| `SupervisorTests.Many_pipelines_share_one_large_table…`, "the table is only 10,229,144 bytes" | 43625839461 (another of the ten; the only failure ever in `build-and-test`) | the test reads the whole process's heap, in a class that ran beside the others | test moved to run alone, here |
| the Kafka smoke test, "trade-enrichment's state levels off": 1,602, 1,738, 1,954, 1,952 | 43627490271 (one of ten runs of 6f15177); 43628127621 (one of ten of 78af0b5, the in-memory smoke: 1,070, 1,462, 1,954, 1,952) | the claim compared its four readings with each other; every one was inside its bound | smoke fixed here |
| `WarmStandbyTests.A_spec_replaced_while_the_standby_fetches…`, a layout expected and null read | none on a runner; once here in the suite's fourth loop, and once in round three under load, unexplained then | the test's hook counted the build and then noted the layout; the test waits on the count | test fixed here |
| `ReplaceTests.Keep_that_the_checkpoint_cannot_follow…`, "No exception was thrown" | 43629484693 (one of ten runs of f2e5ceb); twice in seven runs of the suite's fifth loop here | the test asked for a replace that keeps state before the first checkpoint was committed | fixed here, with eight tests of the same kind |
| `KafkaServiceTests.A_standby_on_kafka_sees_what_the_leader_deploys…`, state lag 322 where 0 was expected | 43629500952 (another of the ten) | a figure sampled once a second, read once, while the leader's last commit was on its way | test fixed here |
| `KafkaServiceProcessTests.Losing_one_broker_of_three…`, "the loss of one broker of three moved the leadership", and the test after it | 43629495626, in the `broker-failures` job only | the standby's process took most of a two-core runner for as long as the dead broker was still listed as a leader | **product**, found and not fixed (Open for James 75) |

**A deposed in-memory leader delivered over its successor.** The in-memory backend's
`Leased(sink)` returned the sink as it was ("there is no heartbeat to miss"). A leader
that is replaced is told at once — its role is `Fenced` before the takeover prepares
anything — but what tears its circuits down is its own node, on a thread-pool thread,
a moment later. In that moment a circuit of the old leader's is whole: it reads the
input both nodes share, joins against its own copy of a table, which no longer follows
anything, and delivers. Its commit is then refused, but the rows are in the sink — and
when they land after the new leader's rows for the same keys, they stay. The test that
timed out on the runner appends twenty trades and changes an instrument straight after
a takeover; in the failed runs the new leader had delivered its twenty-eight rows in
one batch, and four of them had then been overwritten by the old leader's, joined to
the instrument as it had been (`SYM4` where the model has `AGAIN`); the old leader's
circuit had run three steps and committed two. Reproduced on two cores with three
busy loops beside it: 6 of 34 runs of `WarmStandbyTests` failed, every one this test
with these four rows.

Fixed in the backend (`InMemoryBackend.cs`): a leased sink begins a write only if its
node is the one the cluster calls leader, decided under the cluster's gate where a
takeover names the new one; and the takeover, having named itself, waits for the
deposed leader's writes in flight before `PrepareAsync`, for at most
`InMemoryBackendOptions.DeposedWriteTimeout` (5 s, new). So a write either began
before the takeover, and is in the sink before anything of the new leader's, or does
not reach the sink. A refused write is a `ChangelogFencedException`, which the
supervisor already reports as "this node no longer leads". `InMemoryFencingTests`
forces both halves with a sink that holds a write inside itself: the takeover does
not prepare while the write is held, the fenced node's next write is refused, a bound
that is reached lets the takeover go on; and through two service nodes, where the
leader is held in the sink with a row, the instrument is renamed, the standby takes
over, and the sink must end with the new leader's row. After the fix: 0 of 34 runs of
`WarmStandbyTests` under the same load.

On Kafka the same gap is narrower and was already measured: the service's Kafka sinks
write through the log's transactional producer, which the takeover fences; a sink
outside Kafka has the lease, which a leader replaced by hand holds until it reads the
new term (`KafkaFailoverProcessTests`: "0 sink record(s) from the deposed leader after
its successor's first").

**Every topic new to the short reader cost a tenth of a second.** The log has one
consumer for short reads — a table's head, the last term. Before it is assigned to a
topic it has not met, the topic's metadata is asked for on its handle (round two: a
running consumer otherwise waits a second). That question went through an admin client
made for it and disposed at once, and disposing an admin client waits for its poll
loop, which looks up every 100 ms: build 0.2 ms, the request 0.3 ms, the disposal
99.9 ms (traced on 90 first reads). Reads take turns on that consumer, so it added up:

- 100 ms in the fence stage of every takeover, because the read of the last term is the
  reader's first sight of the leader topic;
- 200 ms per table when a node opens its tables (the rows topic, then the changes
  topic), which is most of what a standby's start took;
- 100 ms per table in a takeover whose reader had not met the tables' topics — a reader
  replaced after a read that failed, or, before a0cbd57, after a statistics question
  that was called off at its two seconds. That is the 2,164 ms: the same run's catalog
  stage was 122 ms where it is otherwise 9 to 36, and its Kafka tests' two-table
  takeovers took 8, 110 and 215 ms by turns — none, one or two tables at 100 ms each.
  The clusters are in every `measurements.log` kept here: 7–30, 106–129 and 205–249 ms.

The admin client is now made once for the consumer and disposed with it
(`KafkaLog.AssignForShortRead`); a sink without the log's producer puts its own away
off the path of its first batch. `KafkaLogTests.A_short_reader_that_meets_many_topics…`
holds it by count, not by the clock: twelve tables opened, the reader replaced, twelve
heads read, and two admin clients made in all — one for each consumer. Measured in that
test, one local broker, Debug:

| twelve tables | before | after |
|---|---|---|
| opened by a reader that has met none of them | 2,691 ms | 217 ms |
| their last versions, read on a reader just replaced | 1,299 ms | 84 ms |

On the runners, the service's process tests (21 tables, two processes) before and
after, from the runs' own `measurements.log` (kill to leader: 3,469 to 5,587 ms before,
3,319 to 3,983 since):

| | eleven runs before | the 59 runs since |
|---|---|---|
| kill -9 failover, fence stage | 129–162 ms (638 once) | 26–120 ms in 52 runs, 535–550 ms in 7 |
| kill -9 failover, table stage, 21 tables | 109–185 ms (2,164 once) | 86–253 ms, median 121 |
| a standby warm, from process start | 7.1–8.4 s | 2.2–5.7 s, median 3.6 |
| step-down: resignation to the standby leading | 236–475 ms | 110–480 ms, median 199 |
| the Kafka tests' takeover, two tables | 8–13, 110 or 213–217 ms | 5–61 ms in 351 of 354; three above 100 (126, 220, 843) |

**After a leader killed inside a transaction the storage stage waited for an empty
fetch.** Found on the way, not by a failed run. A scratch test stopped a leader inside a
transaction of 2,048 state records, six times, and between transactions, six times:

| the leader stopped | fence | storage | fence and storage |
|---|---|---|---|
| between transactions | 22–54 ms | 1–3 ms | 24–56 ms |
| inside a transaction that had written state | 383–474 ms | 33–126 ms | 507–510 ms, every time |
| the same, with the reader's change below | 380–491 ms | 1–13 ms | 387–493 ms |

Two waits ended together, half a second after the takeover began. The fence waits
while the broker aborts the transaction; the guide's account of it — the client asks
again after a pause of its own — was not traced again here (in the service's runs it
was a fence of 638 to 692 ms against 130 to 220 before this step's first fix, in 2 of
14 takeovers in a loop here and 1 of 11 on the runners; 535 to 550 ms against 26 to
120 in 7 of 59 since). The storage stage waited for
the standby's follower to reach the end of the state topic: the follower has read every
committed record, and that the aborted ones behind them are the end the client tells
an application with an end-of-partition event — after a fetch that came back empty,
which the broker holds for the consumer's `fetch.wait.max.ms`, 500 ms. So when the
fence was quick the storage stage was slow by what was left of the half second: 326 ms
beside a fence of 166 in a scale-rig run, 418 ms beside 95 once on a runner in the
Kafka tests' takeover between two processes (3 or 4 ms otherwise).

The client itself passes the aborted records and their marker as soon as it has read
them, and its position says so. The reader loop (`KafkaLog.ReadLoop`, which every
follower and restore runs) now asks the consumer for its position whenever a poll
returns nothing and reports it if it is past what it last reported — an empty batch
with a position, which the loop's callers already took from the end-of-partition
event. `KafkaLogTests.A_follower_reaches_the_end_behind_an_aborted_transaction…` holds
the empty fetch for four seconds and gives the follower two; it also holds the position
to the end exactly, and never falling. What is left after such a leader is the fence's
own wait, which is the half second the guide names.

**The scale rig**, 500,000 rows, 500,000 orders of state, twenty pipelines, one local
broker, beside the critic's run of the same on a0cbd57 (3,938 ms from the kill to the
leader, 4,123 to the last sink, a cold standby warm in 7.1 s, a takeover by hand
1,635 ms, one node from nothing leading after 7.8 s):

| | with the short reader's fix | with the reader's position too |
|---|---|---|
| kill to leader, to the last sink | 3,685, 3,769 ms | 3,806, 3,892 ms |
| the takeover's stages after the kill | 680 ms: fence 166, storage 326, pipelines 125 | 802 ms: fence 544, storage 7, pipelines 130 |
| a cold standby warm, with the state | 6.1 s | 6.3 s |
| a takeover by hand, to leader | 1,609 ms | 1,590 ms |
| one node from nothing, to leader | 7.6 s | 8.1 s |

One run each. The stages are the two cases of the table above — the client's pause in
the second run, the follower's wait in the first — and the rig has not yet shown a run
with neither. The Kafka smoke test: 3.1, 3.1, 2.8 and 3.1 s from the kill to the new
leader in four runs here (`TakeoverAfter` is 3 s of it), 3.1 to 3.3 before.

**A commit returns before its marker is written.** `KafkaStateChangelog.EndOffsetAsync`
says so, and round three's short-read test asked for the end of the leader topic
straight after the writer's first commit. With nothing else running here the end was
behind a commit that had returned in 33 of 400 tries, and never 0 after an acquisition
(0 of 40); with the broker held to 0.3 of a processor, 98 of 400 and 5 of 40. The test
now waits for the end to pass the term. The other reads of an end in
`tests/Incremental.Kafka.Tests` were read through: each is behind a new writer's
`InitTransactions`, which waits for the markers, or already waits.

**A topic made a moment ago can be unknown.** Sixty topics created and asked about at
once by another client: known every time on an idle broker (0 of 60, twice), and
unknown 2, 19 and 1 times of 60 with the broker held to 0.08 of a processor. That is
what round three read from the code for `ABatchTheBrokerRefuses…`; its helper now waits
until the topic is known, and the test reports a failed delivery as that. In
production a sink whose topic was created that instant fails its first delivery and
the pipeline is started again, which is the rule for any sink failure.

**The smoke test waited for one pipeline of two.** Its second act publishes 2,000
trades over 1,500 ids in two halves, "the second once the first is through: never two
trades of one id in a step" — and waited for `trade-enrichment` alone. `large-trades` is
the same definition with its own state and its own steps; on a runner it was still on
the first half when the second arrived, took a reused id's two trades for one trade
amended, and never delivered the first: 1,076 rows of the model's 1,095. Forced here by
stopping `large-trades` until both halves were published: 980 of 1,095, the same claim
failing the same way. The smoke now waits for both between the halves. What it says
about the product is under Open for James 71: whether a reused id's earlier trade is
final when the later one arrives depends on how the input is cut into steps.

**The smoke test compared four readings that are not comparable.** J1's claim that
`trade-enrichment`'s state levels off holds each of four readings, a batch of 125
trades apart, to a bound computed from the trades still open (1,976 entries) — and
asked besides that the last two be within a tenth of the first two. A reading is taken
when the sink has the batch, which is before the step that delivered it is committed
and counted, so it lies anywhere between the state without the batch and with it:
984 to 1,954 in runs here, in no order. On a runner they came as 1,602, 1,738, 1,954,
1,952. The bound is what the claim is: a state that kept the feed would be a thousand
entries past it after one batch, and J1's mutant that evicted a minute late read
3,834. The second clause is gone.

**Tests went on from an output to something that reads the checkpoint.** A step's
output is in the sink before the step is committed — commit after delivery — so "the
sink has it" says nothing yet about the checkpoint. `Keep_that_the_checkpoint_cannot_follow…`
waited for the first output and at once asked for a replace that keeps state with two
workers, which is refused by reading the checkpoint; asked before there was one, keep
is open, and the test failed with "No exception was thrown". (Nothing is wrong in the
service there: a replace that turns out not to fit is found when its generation
starts and rolled back, which another test holds.) That is one test; the rule it
broke is broken wherever a test does the same, and waiting for each to fail in one
run of a hundred is no way to find them. So every sink's acknowledgement was held back
half a second after its rows were visible, and the broker-free classes run: eight
more tests failed — a takeover and a restart that count what the new leader read
against a checkpoint not yet written (`NodeTests`, twice; `TwoInputTests`;
`WarmStandbyTests`), a state mismatch that needs a checkpoint to mismatch
(`ReplaceTests`), figures that exist once a commit has happened (`MeterTests`,
`MemoryFiguresTests`), and one that the hold itself breaks (a shutdown that drains
5,000 records through a sink now half a second slow per batch, past its budget).
`TestService.Settled()` waits until every running pipeline has committed what its
sources have handed it and no sink is inside a write; the seven that were real wait
on it, as does the first. With the hold still in place the broker-free classes then
passed but for the shutdown test, three runs of three, and the Kafka-backed in-process
classes passed (14 of 14). On Kafka the same rule gave the other failure of that
round: a standby's state lag is sampled once a second, and the test read it once.

**A hook counted before it noted.** `A_spec_replaced_while_the_standby_fetches…` is
the test round three saw fail once under three busy loops and could not explain. Its
hook after the standby's build incremented a counter and, on the next line, stored the
layout the stores were bound under; the test waits for the counter and then reads the
layout. With the hook's thread taken off the processor between the two lines the test
read null — once in about 130 runs of the test here on two cores. Forced with a delay
between the lines: the same message. The hook now stores the layout first. Nothing in
the product: the standby's build was right each time.

**A test that reads the process's heap ran beside others.**
`Many_pipelines_share_one_large_table_with_one_copy_of_its_rows` takes the collector's
total before it loads 200,000 rows, after, and again with twelve circuits on the table.
On a runner the table measured 10.2 MB; run alone it measures 89.6 MB, three times to
within a kilobyte. Another class had let go of the difference between two readings. It
is now a class of its own (`SharedTableMemoryTests`) in a collection that runs with
nothing beside it, as the Kafka-backed classes do.

**A files sink tidies up inside its write.** The first twenty runs of the service
tests on two cores (below) ended with a failure in the twentieth:
`An_upsert_sink_told_to_compact_folds…` found two snapshot objects. A fold writes its
snapshot whole and then deletes what it stands for, the snapshot before it included,
inside the write of the step that set it off; a reader of the sink takes the newer of
two and is right at every moment (`A_reader_that_listed_before_a_fold…` holds that).
The test waited for the last round's rows, which are readable as soon as that round's
step object is written, and listed the directory at once — which, when the last round
is one that folds, is the moment between the snapshot and the deletes. The listing it
failed on is exactly that state: the old snapshot, the three objects it was folded
with, the new snapshot, and the step that set the fold off. Alone the test did not
fail in 40 runs on two busy cores; with the fold's deletes held back 300 ms and the
test ended on a round in which a fold was in flight it failed with the same message,
and with the listing taken once the sink's write has returned
(`SinkSlotStatus.WriteInFlightSince` is null) it passed on that round and nine
others. `An_append_sink_told_to_retain…` counted the objects of a sink that deletes the
one too many in the same way; it has not been seen to fail and waits the same way now.
Nothing was changed in the sinks.

**Clocks in assertions.** The table stage of the kill -9 failover was held to two
seconds in all. It is now held to 100 ms a table (2.1 s for the 21): the unit in which
every wait that has crept into that stage has come — a second, half a second, a tenth
— and the measured stage is 86 to 253 ms for all of them on the runners since. The
other wall-clock bounds
in the two test projects (a step-down's hand-over under half its timeout, a leader
stopped inside a transaction replaced within three timeouts, a deploy and a takeover
under ten seconds) were left: none has failed in any run but one with the broker
starved (below), and each is several times what it measures.

**What was added to see by.** Debug lines, none on a hot path: an admin call that is
made again (attempt, how long it was given, the error); a short read that waited more
than a quarter of a second for its turn; a table's last version, or its promotion,
that took as long. None of them existed when the 2,164 ms happened, which is why it
took a trace to find.

**The broker tests in CI.** `gh workflow run CI --ref <branch> -f broker_tests=true`
adds a job, `broker-failures`, that starts the `kafka3` profile and runs the tests
whose names contain "broker" (the six that stop, kill and freeze brokers, and one that
needs none stopped), failing on a skip and keeping the node logs on a failure. It does
not run on a push. Ten runs of it so far, on six commits: nine green,
the job taking 11 min 45 s to 17 min 21 s of a runner (the seven tests 9 to 12½
minutes of that; the rest is the build and the brokers' start), and one red, for the
reason in the next paragraph.

**A standby takes more than a core while a dead broker is still listed as a leader.**
What the one red broker run had: the standby's log, 192 MB of it, with 522,625
refused connections from the log's admin client in three broker losses — up to 27,000
a second. A standby asks where the state topic ends once a second (its lag), and
where the leader topic ends while the leader is quiet; the questions go to the
partition's leader, which for the nine seconds or so before the controller drops a
dead broker is the dead one, and while a request waits for a broker that refuses
connections the Kafka client connects again without a pause. Here, one broker of
three killed for 20 s, each node's processor time read every second: the standby
between 1.35 and 1.42 processors for eleven seconds and under 0.1 after, the leader
never above 0.6. (The log does not show it here: one address for `localhost`, and
the client folds identical errors; on the runner there are two.) On a runner's two
processors, with three brokers beside it, that is most of the machine, and once the
leader's heartbeat was late enough for the standby to take over — which is what the
test is there to catch, and in a deployment the two nodes do not share processors.
Not fixed: Open for James 75.

**With the broker starved.** The whole of `tests/Incremental.Kafka.Tests` with the
broker held to 0.3 of a processor, far below a runner: 131 of 134 passed in 6 min
40 s, and the three that failed each read a clock — a leader stopped inside a
transaction replaced in 2,408 ms against a bound of three takeover timeouts
(2,400 ms; 1.3 s on a runner), a leader that commits again once the brokers are back
replaced all the same within twice a two-second ceiling, and the source-resumption
test's own check that most of its crashes landed after a checkpoint. None has failed
on a runner in any run. Left as they are, and named here so that the next one to fail
is recognised.

**Ten runs of one commit.** Ten runs of 53d8c83, which was the last commit that had
changed code or tests when they were made: the push's own and nine started by hand
straight after it, the last with the broker tests. All ten green. (It is not the last
any more. Nine later commits of this round changed code or tests — 6f39d94, d77f78b,
69fa8a5, 22fcb1a, ee68f86, ac42c06, 809bbbc, 92a2b4f and 8a7b453: the admin UI, the
in-process supervisor, the transaction timeout and the leadership's dating of
heartbeats — and so did the fourth review's fixes after them. None of those was run
ten times: each has its own push's run, and what the last of them has is in *The
fourth review's findings*, under CI.)

| run | started by | build-and-test | connector-stack | perf-contracts | broker-failures | result |
|---|---|---|---|---|---|---|
| 43632984819 | the push | 4 min 37 s | 10 min 50 s | 1 min 02 s | not asked for | success |
| 43632999415 | by hand | 4 min 50 s | 11 min 26 s | 0 min 39 s | not asked for | success |
| 43633005112 | by hand | 6 min 00 s | 11 min 34 s | 0 min 38 s | not asked for | success |
| 43633010192 | by hand | 5 min 41 s | 11 min 37 s | 0 min 52 s | not asked for | success |
| 43633015440 | by hand | 4 min 47 s | 11 min 46 s | 0 min 58 s | not asked for | success |
| 43633020393 | by hand | 4 min 27 s | 10 min 42 s | 0 min 55 s | not asked for | success |
| 43633025557 | by hand | 4 min 38 s | 10 min 41 s | 0 min 47 s | not asked for | success |
| 43633030602 | by hand | 4 min 35 s | 10 min 06 s | 0 min 45 s | not asked for | success |
| 43633035565 | by hand | 4 min 54 s | 10 min 35 s | 0 min 52 s | not asked for | success |
| 43633040370 | by hand | 5 min 53 s | 9 min 54 s | 0 min 55 s | 11 min 45 s | success |

It took seven commits to get there, and most sets of runs before it found something:

| commit | runs | green | what the red ones were |
|---|---|---|---|
| 71521ba, 23fcecb (the first two fixes; the broker job) | 3 | 3 | |
| db23011 | 10 | 8 | the smoke's two halves; the heap test beside others |
| 6f15177 (the reader's position) | 10 | 9 | the smoke's four readings |
| 78af0b5 | 10 | 9 | the smoke's four readings again, in memory this time |
| fa307a8 | 6, and 4 called off unstarted | 6 | |
| f2e5ceb | 10 | 7 | a replace before the first checkpoint; a sampled state lag read once; the broker job |
| 53d8c83 | 10 | 10 | |

Every job of every run, with its duration: `docs/design/runs/2026-11-14-j3-ci-runs.tsv`.
`connector-stack` took 9½ to 12 minutes where it passed (55 runs), `build-and-test`
4 to 6.

**Twenty runs here on two cores.** The service test project — 251 tests, of which
the six that need three brokers skip — pinned to two processors with `taskset`, and
with it the node processes it starts; the one local broker was not limited. Twenty runs of
53d8c83, the last build at the time (not of the commits after it, above): twenty green,
149 to 156 s each.

It took six goes at twenty. The first, on the build with the first two fixes, passed
nineteen and failed the twentieth on the fold. The next three were stopped when a run
here or on a runner had shown the next thing: 5 of 5, 2 of 2, and 4 of 5 (the hook).
The fifth was 5 of 7 (the checkpoint, twice) when it was stopped. 59 runs of the
suite in all, four of them red, each in the table above. Besides those: the
broker-free classes on two other processors with three busy loops beside them, 15 of
15 on f2e5ceb before that loop was stopped; `WarmStandbyTests` under the same load,
28 of 34 before its fix and 34 of 34 after; the two-node in-memory smoke under it, 25
of 25. On the final tree, once each: `tests/Incremental.Tests` 709 of 710 (one opt-in
skip), `tests/Incremental.Kafka.Tests` 134 of 134, the example's `Pipelines.Tests` 17
of 17, the four smokes (55, 69 and 69 claims, and the narrative's 13). Records:
`docs/design/runs/2026-11-14-j3-local.txt`.

**Deviations.**

- **The in-memory backend's takeover can now wait**: up to `DeposedWriteTimeout` (5 s)
  for a write the old leader is inside. It waited for nothing before, and a sink that
  hangs makes an in-memory takeover five seconds longer (Open for James 73).
- **A fenced in-memory leader's sink write throws**, where it used to go through. Its
  pipeline ends as "this node no longer leads", as on Kafka.
- **The broker tests are a job started by hand**, not part of every run (Open for
  James 74), and not stable on a two-core runner until Open for James 75 is.
- **Every follower's reader asks the client for its position ten times a second while
  nothing arrives**, and reports it when it has moved. A local call; not measured as a
  cost.
- **The table-stage assertion still reads a clock.** A count holds the defect that was
  found (`ShortReadAdminsCreated`); the stage's bound stays as the net for the next
  wait of that kind, with the reason for its size beside it.

**Left undone.**

- `KafkaConnection.TopicMissingAsync` (a dry run's question whether a topic exists)
  still makes and disposes an admin client: a tenth of a second per Kafka slot at a dry
  run, on no path that is timed.
- The fence's wait after a leader killed inside a transaction (380 to 690 ms where it
  is otherwise 20 to 220) was not traced further than the guide's account of it and
  was not worked around.
- The core's Redis tests skip when Redis does not answer their first connection in
  time, and two did in one run of `tests/Incremental.Tests` pinned to sixteen cores
  beside other work here (3 skipped instead of 1). The `build-and-test` job does not
  count skips; it was not looked into.
- **Not explained.** Three of the Kafka tests' 354 two-table takeovers on the runners
  since the fix took over 100 ms (126, 220, 843); nothing asserts on them and they
  were not looked into. Why `Keep_that_the_checkpoint_cannot_follow…` raced its
  checkpoint in no run of some forty and then in two of seven once the heap test had
  been moved out of the classes that run side by side is put down to what then ran
  beside it, and was not shown. Every failure of a test in this step's runs, here and
  on the runners, has its cause in the table above.

#### The admin UI read as an operator reads it, and the decision list (J2, J7, J8)

**The shifted columns (J2).** The overview's Tables card had a Memory header and no
Memory cell, so a table's version showed under Memory and its readers under Version;
the tables list had the cell and not the header, so every column after Rows was under
its neighbour's name. Both came from building the header row and the cells separately.
Every table with a header row in the UI's scripts is now drawn by one helper, `grid`
(`wwwroot/app.js`), from a list of columns given once: a column's label is the header's
text and every cell's `data-label`, which the phone layout prints beside the value. A
column left out (a standby's, or a button nobody may press) is left out of both.
Sixteen of the seventeen tables with a header row were moved to it; the plugin diff (grouped rows with
a cell spanning four columns) is the one still built by hand. Going through them found
no third mismatch in count; it found labels that differed from their header only in
the pipelines list's spark column (header "Records in, last minute", cell "Last
minute"; now "In, last minute" for both).

The check is `AdminUiTablesTests`: a two-node AdminHost with the generator running, the
UI's own files plus `Ui/tables-check.js`, and headless Chrome with `--dump-dom`, as the
morph test. The script visits every page that lists something — overview, pipelines,
one pipeline's overview, topology and faults, tables, one table, plugins, dead letters,
cluster, audit — on the leader and then on the standby, and for every table with a
header row checks each body row: cells (a `colspan` counted for what it spans) against
headers, and each `data-label` against its header's text. A page that should list
something and shows no row fails, as does a view that failed to draw or anything on the
console. The live feed is an `EventSource` that never ends, which stops a headless
browser's virtual clock for good; `Ui/tables-stub.js`, loaded first, replaces it, and
the UI then loads each page by itself. About 9 s for both nodes. CI's runners have
Chrome (the morph test is not among the skips of `build-and-test`), so it runs on every
push. Mutations: a `grid` that drops the Memory cell (the J2 shape) and one that labels
Rows "Row count": both killed, on the overview, the tables list and the state table.

**What read wrong (J7).** Used with the example's two in-memory nodes and its generator
for about fifteen minutes, on the leader and the standby, at 1440 and 390 px, light and
dark, before and after a takeover:

- *A standby's builds shown as starts.* "Last start of each pipeline" listed a
  standby's warm builds as starts, "built at start", with Start and Edges at 0 µs. A
  standby build is the record with run 0: it is now "standby build", Start and Edges
  are a dash ("not started"), and on a node whose every row is one the card is called
  *Standby build of each pipeline*. A takeover's starts read "warm start" or "built at
  start". The first version of the dash was one DOM node placed in every row, and a node
  can be in one place: it showed in the last row only. Seen in the browser, fixed, and
  written into CLAUDE.md's UI rules.
- *Two notations for one position.* A source read `0:10,209 / trades#p0:10,209`. It is
  now one line per partition, `p0 10,209 / 10,209` (or `10,209 / 10,209` for a source
  with one), read / committed, under the header *Position: read / committed* with a
  tooltip. A connector that keeps more in the checkpoint than the partitions it reads by
  — the `files` source's name banks — had them printed as offsets in the hundreds of
  millions; they are counted ("and 13 more checkpoint entries") instead.
- *A fenced in-memory node never came back* — see below.
- *A standby's zeroes.* The standby's overview showed Records in 0/s, Records out 0/s
  and Step latency "–, no steps in the last second", and its pipelines list In /s 0,
  Out /s 0, a flat spark and Dead letters 0 for every pipeline — figures it does not
  measure. The tiles now say the rates are on the leader; the list leaves out the rate,
  lag and dead-letter columns on a node that does not lead and says so in its lede
  (Open for James 77).
- *The peer standby's term* read 0 for a standby that has never led; it says so.
- *The growth flag* (J1's): the overview's attention entry read "grown without a pause
  since 32 min ago"; it reads "for 32 min". The list's badge had an ISO timestamp as its
  tooltip; a local time now. The flag itself was not seen raised in the browser: no
  example pipeline grows any more, and the thirty-minute period was not waited out.
- *State entries* ("One entry") read "0 B" for a store whose key and value have no
  objects of their own (a deadline store keyed by a long); "none", with a tooltip saying
  the Memory column is the map.
- *The phone layout.* At 390 px a store's name in the state table broke a letter per
  line. Sources, sinks, state stores, a pipeline's tables, definitions and the cluster's
  table lag now use the card layout that the lists already had. No page scrolls
  sideways at 375 px of content.
- *The pipeline tile's text* "upserts, deletes and appe…" was cut at 1440 px; it is
  "upserts, deletes, appends".

Console: no message on the pages visited after the fixes, in light and dark, at 1440 and
390 px; the
content security policy is the one served, unchanged (no inline style or script was
added: the meter widths go through the CSSOM as before). Retaken: `docs/images/admin/`
overview, pipeline and cluster-standby (AdminHost), `docs/images/enrichment-service/`
overview and pipeline (the example). The guide's *takeover* screenshot is of the Kafka
mode and still shows the old "warm" label; it was not retaken.

**A fenced in-memory node comes back (J7).** In the example's and the AdminHost's
`--nodes 2` the node a takeover replaced stayed `Fenced`, and the new leader showed
*Degraded: no ready standby* until the program was restarted. The in-memory cluster
takes a node that leaves (`DisposeAsync` removes it) and one that joins (it is the
standby if a leader exists), so the same supervisor contract as on Kafka was built in
the process instead of saying the mode cannot do it:

- `ServiceNode.Ended` (`Task<int>`) completes with what the process would exit with — 3
  replaced, 4 resigned, 5 could not lead — once the node's circuits are abandoned.
- `NodeSupervisor.RunInProcessAsync<THost>(start, node, started, options, stop)` waits
  for it, disposes the host, and starts it again under the same name and address: at
  once after 3 and 4, with the doubling delay of `RunAsync` otherwise, and a start that
  throws is retried the same way. The host it holds when `stop` is cancelled is the
  caller's to dispose, in the order its nodes need.
- The example's `--nodes 2`, its two-node smoke (a seventieth claim: a is back, warm,
  behind b) and the AdminHost (`DemoOptions.SuperviseNodes`, on in its program and off
  in the tests that look at a fenced node) use it. Both programs now take SIGTERM as the
  stop request: with nodes that come back, a node's host stopping no longer ends the
  program, and the AdminHost, which waited for Ctrl+C alone, ignored a SIGTERM.
- `InProcessSupervisorTests`: a takeover (a ends with 3 and is the warm standby behind b
  under the same name and URL, and b's health loses *no-ready-standby*), then a step-down
  (b ends with 4, a leads, b comes back). Mutations: `Ended` not completed, the
  supervisor not starting again, the old host not disposed (its port still taken): all
  three killed. Measured once in the example: the replaced node was a warm standby again
  within the six seconds before the first look.

**The decision list (J8).** *Open for James* now begins with *Decide before production*:
nine items (lockstep upgrades; the `files` formats; the transaction timeout; failover
timing with 61, 64 and 75; the restart contract; the admin surface's exposure; what
needs AWS; bounded state per tenant; and event-time state before format 7, which is moot
for a first deployment), a line for *before a second tenant*, one for *can wait*, and
the items later rounds made moot, which are also marked in place. Each was checked
against the code as it was then: the 60 s `TransactionTimeout` (five minutes since the
step after this one, which rewrote that item), `TakeoverAfter` 3 s and
`TakeoverCeiling` 30 s, `FormatTagged = 2`, manifest format 7 with the 6 → 7 refusal
for event-time circuits, `AcceptOperatorHeader` on and `ExposeRowData`,
`AllowPluginUpload` and `AllowPluginRescan` off, `UseMsk` called by nothing, no S3
store, `OpenSeconds` 3,600. Nothing was renumbered or deleted. The rows of S6's and
S7's broker tables whose run records were in a scratch directory that is gone carry a
dagger and a footnote: S6's four three-broker rows, S7's summary rows other than the
two-broker ones, its thirty kills and its thirty rolling restarts.

**Public API added.** `ServiceNode.Ended`; `NodeSupervisor.RunInProcessAsync<THost>`.
Test host: `DemoOptions.SuperviseNodes`, `DemoNode : IAsyncDisposable`. UI: `grid` on
`window.__admin`.

**Deviations.**

- The pipelines list has fewer columns on a node that does not lead (Open for James 77).
- The plugin diff table is not drawn by `grid`; the check covers it (its rows span).
- `--dump-dom` with a stubbed `EventSource` checks the pages as loaded by polling, not as
  updated by the live feed; a view that redraws differently on a feed tick is not
  covered.
- A graceful stop of the two-node in-memory program still steps the leader down to the
  standby on the way out (each node's host hears the signal); with the supervisor
  watching, the resigned node could be started again in the moment before the program
  ends. It is disposed with the others; nothing was seen go wrong in the runs made.

**Numbers.** On the final tree: build 0 warnings; `tests/Incremental.Tests` 710, 709
passed, 1 skipped; `tests/Incremental.Kafka.Tests` 134/134; `tests/Incremental.Service.Tests`
253, 247 passed, 6 skipped (the kafka3 broker tests); `Pipelines.Tests` 17/17. Smokes:
`--headless` 55 claims, `--headless --nodes 2` 70, `--headless --kafka` 69 (2.8 s from
the kill to b leading), `TradeEnrichment` 13; all exit 0. `benchmarks -- --assert` was
not run: `src/Incremental` was not touched.

**Left undone.**

- The growth flag and its notice were read in the code and not seen raised in a browser.
- The guide's takeover screenshot (Kafka) was not retaken.
- The in-process restart was not tried with a sink that hangs across it, nor with
  `DeposedWriteTimeout` running out.

#### What a quorum outage and a broker's loss cost, and whose time it is (J4)

Round three measured three things and explained none of them: two of three brokers
away for 40 s silenced the sink for 55 to 61 s; away for 70 s, the leader ended in three
runs of four and "what decides … was not established"; a broker killed in the same
moment as the leader took 9 to 15 s to fail over where a process alone takes 3 to 4.
This step read all three from the node's Kafka client log (`debug=eos,protocol`), the
brokers' logs and their controllers' (`controller.log`, and
`kafka.coordinator.transaction` at `DEBUG` for the traced runs), on the `kafka3`
profile and, for the part that turned out to be the profile's, on three brokers with
three controllers of their own. Records: `docs/design/runs/2026-11-14-j4-*`.

**What decides a leader's fate past its transaction timeout** is in the engine design
(`durability-and-standby.md`, *A leader does not outlive its transaction timeout*),
with the table of the eight traced outages. In short: `transaction.timeout.ms` is two
clocks. The client fails a transaction whose records it could not deliver for that
long; the coordinator aborts one it has registered that is that old, and raises the
producer's epoch as it does. What follows depends on where the writer's commit cycle
stood when the brokers went and on whether the coordinator was on a broker that
stayed:

- the transaction had been begun and never registered (the coordinator refused
  `AddPartitionsToTxn` throughout): the client's abort of it is answered
  `INVALID_TXN_STATE`, which librdkafka 2.16 treats as fatal — four of eight;
- its commit was already with a coordinator that stayed: the coordinator gives up
  writing it after the timeout, its sweep aborts it and raises the epoch, and the
  writer is `fenced by a newer instance` that does not exist — one of eight, with the
  brokers still away;
- its records were on their way, or nothing of it had reached a coordinator: the abort
  or the new epoch is accepted when the brokers return, and the leader goes on — two
  of eight, which is round three's "one run in four survived";
- and once the standby took over at the ceiling in earnest, 60 ms before the
  coordinator would have refused the leader's abort: its wait for the leader had ended
  six seconds after the brokers could commit, with the leader's client still held up.

So the leader's end at 70 s was never a fence by the other node (but for that last
run), and never a loss. All fourteen outages of 70 s made on a 60 s timeout in this
step: the leader ended in ten (seven of seven with the brokers stopped, three of seven
with them killed), the node exited 3 and came back as the standby each time, and the
sink was complete.

**What is done: the timeout is five minutes** (`KafkaLogOptions.TransactionTimeout`;
it was the clients' default of one). The timeout is the length of outage a leader
rides out, less the brokers' own time to commit again, and at one minute a 40-second
outage was already at the edge of it. What a longer one costs here is small, because
every way a node comes to lead begins by initialising the transactional id, which
aborts whatever a dead writer left open: only with no node of the deployment running
does a transaction stay open for the timeout, and what it then hides from
`read_committed` readers is what lies past its first record in the log's own topics
(nothing but its own records) and in the topics of sinks with `"producer": "log"`
(another producer's records in the same partitions). The brokers refuse a timeout above
`transaction.max.timeout.ms` — fifteen minutes by Kafka's default, which Amazon MSK
keeps and a custom configuration may change — so five minutes leaves a cluster room to
have a lower cap than the default; the choice is Open for James 67. With it:

| two of three brokers away (`kafka3`: every broker a controller) | transaction timeout | runs | leadership moved | node processes ended | sink silent: min / median / max |
|---|---|---|---|---|---|
| 40 s — three stopped (`docker stop -t 1`, one after the other), three killed in one command | 5 min | 6 | 0 | 0 | 54.1 / 55.2 / 55.9 s |
| 70 s — the same | 5 min | 6 | 0 | 0 | 85.1 / 85.8 / 86.3 s |
| 70 s, the node's client log at debug — two and two | 5 min | 4 | 0 | 0 | 85.0 / 86.0 / 86.5 s |
| 70 s, past the timeout — three stopped, three killed | 60 s | 6 | 4 (stopped 3 of 3, killed 1 of 3) | 4 (exit 3, started again) | 85.9 / 86.1 / 87.8 s in five, and once 72.3 s followed by a second silence |
| 70 s, past the timeout, the client log at debug — the eight traced runs | 60 s | 8 | 6 (stopped 4 of 4, killed 2 of 4) | 6 | 85.2 / 86.3 / 89.0 s |

In every run the test asserted that the sink equals the model and that one node leads
with the pipeline running while the other stands by, warm; under the timeout it now
also asserts that the leadership did not move and that no process ended
(`Seventy_seconds_without_a_quorum…`). The standby held at the ceiling throughout in
every one. The run with 72.3 s is one in which a record became readable two seconds
after the brokers were started and the next fourteen seconds later; the test records
the longest silence only.

**Where the fifteen to twenty seconds after the brokers' return go.** None of it is the
service's to give back. From the brokers' and their controllers' logs, the same in
every run, counted from `docker start`:

| after the two brokers are started | time | whose |
|---|---|---|
| the JVMs are up and the three controllers have elected a leader | 2.0 to 3.1 s | Kafka's (`controller.quorum.election.timeout.ms`, one second, and the start of a JVM) |
| the new controller will not take the returning brokers' registration: to it they are still registered, by processes that are gone (`DUPLICATE_BROKER_REGISTRATION`, more than a dozen times in the run that was read), until it has fenced those for missing their heartbeats — `Fencing broker N because its session has timed out`, 9.0 s after its election to the millisecond | 9.0 s | Kafka's: `broker.session.timeout.ms`, counted afresh by a controller that has just been elected |
| the brokers' next attempt to register is accepted (their retries had backed off), they catch up and are unfenced | 3.9 to 5.0 s | Kafka's |
| a second replica is back in sync, the coordinator can write, and the leader's open transaction commits | 0.3 to 1.1 s after the first broker is unfenced (four runs read from the client log) | the brokers', and the round trips of the leader's client |
| the sink's reader has the first record | 0.05 s after that commit | the service's |

The middle rows are what losing the controller quorum costs, and the `kafka3` profile
loses it with its brokers because every broker there is a controller. On a cluster
whose controllers are not its brokers — which is how Amazon MSK's documentation
describes its KRaft clusters: controller nodes that MSK runs, apart from the brokers —
two brokers' sessions run out while they are away, under a controller that never changed,
and the returning brokers register at once. Measured on such a cluster here (three
brokers, three controllers, the same image; its compose file is with the records):

| two of three brokers away (three controllers of their own) | runs | leadership moved | sink silent |
|---|---|---|---|
| 40 s — two killed, one stopped | 3 | 0 | 42.2, 43.2 and 43.8 s |
| 70 s — one killed, one stopped | 2 | 0 | 71.9 and 73.5 s |

Two to four seconds after the brokers are started instead of fourteen to sixteen, and
the standby's status came back in 20 ms with the brokers killed as with them stopped
(the controllers had taken the dead brokers off the lists, so nothing was asked of a
broker that was not there). The settings that govern the rest on a real cluster are
the brokers' own: `broker.session.timeout.ms` (nine seconds; not among the properties
an MSK configuration may set) for a KRaft cluster, `zookeeper.session.timeout.ms`
(eighteen seconds by MSK's default, six to eighteen allowed) for a ZooKeeper one.

**A broker killed in the same moment as the leader.** The six to eleven seconds over a
process kill are the same session timeout, met from the other side: the standby can do
nothing useful until the controller has given the dead broker up and its partitions
have new leaders. Against the controllers' log, nine takeovers on `kafka3` (broker and
leader killed in one moment; the seconds are from the kill):

| the broker killed was | the controller gives it up at | the standby decides at | it leads at | of which the takeover's own work |
|---|---|---|---|---|
| the leader topic's partition leader, not the controller (4 runs) | 7.6 to 8.8 s (three runs read) | 8.4 to 10.2 s — it cannot confirm the silence from a partition without a leader | 9.7 to 10.5 s | 0.3 to 1.7 s |
| the active controller (4 runs) | 11.8 to 12.3 s (three runs read): a new controller after 2.8 to 3.3 s, then its nine seconds | 3.0 s | 13.2 to 14.5 s | 9.5 to 11.1 s inside `InitTransactions`, which has the dead leader's open transaction to abort in partitions that have no leader yet, then 0.4 to 0.7 s |
| the leader of state and input partitions only (1 run) | not read; the brokers released the leader topic at 8.9 s | 3.0 s, **given up at 8.9 s, taken again at 11.7 s** | 11.9 s | 0.3 s |

Whatever the dead broker was, nothing completes before the brokers have given it up,
because it was a replica of every partition (replication factor three): the leader
topic's records are not readable past it, the dead leader's transaction cannot be
aborted past it, and a partition it led has no leader. After that the standby leads in
0.9 to 2.5 s (six runs read), of which 0.3 to 0.7 s is the service's own stages once
it has the log — 1.1 s in the run of the next paragraph but one. With controllers of
their own there is no second kind: three takeovers there, the broker killed with the
leader, took 9.8, 10.2 and 10.5 s. On a real cluster the nine seconds are
`broker.session.timeout.ms` again, and twelve to fifteen where the zone that took the
leader and a broker also took the active controller.

The third row was the service's, and is fixed. The standby had decided at three
seconds and was inside `InitTransactions`, waiting on the brokers like everything else.
When they gave the broker up, the first thing they released was the dead leader's last
heartbeat — written 8.9 s before, and not readable until then because its partition's
high watermark was waiting for the replica that had died. The standby's watch read it,
took it for the leader, called the takeover off ("the leader was heard before this node
had taken the log") and began to count the silence again: a takeover timeout lost, 11.9
s where 9.2 were to be had. Once in the thirteen such takeovers of rounds three and
four. A heartbeat is evidence that the leader was alive when it wrote it, and the
leadership now counts from then: each record is dated on the standby's own clock from
the writer's clock in it, by the smallest difference between the two that the
standby has seen (`HeartbeatDating`; the two clocks are never compared, so their offset
does not matter, and the estimate is never earlier than the writing). A record written
within the silence a takeover was decided on calls it off, as before; an older one does
not, does not start the silence again, and does not end the wait for a leader that the
brokers had held up. A record younger than the takeover timeout is heard when it is
read, as before (*Finished by a second session*, below, has why that had to be put
back).

Nine more takeovers were made on that build, and three on the final one (below, *A
check on a cluster just started*): none was called off. That is not the fix shown at
work — the case comes up once in a dozen runs and cannot be made to — and the build of
the nine did not yet say when it read a record late; the leadership's tests make the
case by hand, and a node now logs it (`read a record that node a wrote … before`; in
the check's other tests it logged a live leader's records shown 7.7 to 13.8 s late
by a broker's loss, and 85.5 s late after the 70-second outage, with the leadership
where it was). With them the step has twenty-one such takeovers of record:

| the broker killed with the leader was | runs | the standby led after |
|---|---|---|
| not the active controller | 9 | 8.7 to 10.6 s in eight; 11.9 s in the one that was called off, before the fix |
| the active controller | 12 | 13.2 to 14.5 s in eleven; **34.8 s** once |

**The 34.8 seconds** are the brokers' as well, and the same thing as S7's one kill in
forty that took 26 s. The broker killed was the controller, the transaction coordinator
and the leader topic's leader at once. The standby decided at 12.6 s, when the new
controller had given the broker up, and its `InitTransactions` was then answered
`CONCURRENT_TRANSACTIONS` forty-seven times in 22.0 s: the new coordinator had the dead
leader's transaction to abort, and its own client was waiting to connect to the dead
broker — `[TransactionCoordinator id=1] Disconnecting from node 3 due to socket
connection setup timeout`, twice, the second time 0.36 s before it answered. A broker
whose address no longer answers at all (a container that is gone here, a host that is
gone elsewhere) does not refuse a connection; another broker that has a request queued
for it waits `socket.connection.setup.timeout.ms`, ten seconds and then twenty
(`socket.connection.setup.timeout.max.ms` is thirty). The service's clients are on the
other side of the port mapping and are refused at once, which is why nothing of the
service's waits that way here; on a real network they would meet the same silence, and
`KafkaClientDefaults` already holds them to five seconds for it.

One more second was the service's in one takeover of nine: the catalog's head took
0.9 s to read where it takes 0.01, because the log's short-read consumer — assigned to
nothing between reads — still believed in the dead broker as that partition's leader,
fetched from it, and waited out the client's half-second pause after a fetch error.
A short read now asks for a topic's leaders again when its answer is more than a second
old (`KafkaLog.AssignForShortRead`, `ShortReadLeadersFor`), without waiting long for
the answer.

**A check on a cluster just started** (2026-11-15, the final build, the profile's
containers removed and started again 45 s before; records
`docs/design/runs/2026-11-14-j4-check.jsonl`). The broker tests of
`KafkaServiceProcessTests`, once each with their defaults: 7 of 7 passed in 7 min 26 s.
Seventy seconds without a quorum, the two brokers stopped: the leader kept its term,
no process ended, the sink was silent for 85.6 s — the assertion this step added. One
broker (state) killed for 20 s, every broker killed in turn and every broker shut down
in turn moved nothing (sink silent 8.8 s, 14.8 s, 1.8 s). Broker 3 (controller,
coordinator, state, input) away for 79 s with the leader killed half way: the standby
led 3.4 s after the kill. A takeover with broker 2 shut down 1.5 s before: 3.2 s. Then
three takeovers with a broker killed beside the leader: 13.3 and 14.4 s where it was
the active controller, 10.6 s where it led the leader topic.

**What else the reading turned up.**

- **A transaction timeout the brokers refuse was read as a fence.** `InitProducerId`
  answers `INVALID_TRANSACTION_TIMEOUT` when the timeout is above the brokers' cap, the
  client calls it fatal, and the log said "another process has taken the log over" —
  for good, since a client that has failed fatally answers every call with its first
  error. It is a `KafkaLogException` that names `transaction.max.timeout.ms`, and the
  log reads on with a new client.
- **A replaced client could mark the log fenced.** The client's fatal error reaches
  the log as the failed call's exception and as its error callback, on two threads,
  and the callback's check that its client is still the log's was made outside the
  lock that replaces the client. Found by the test of the item above, which failed two
  runs in two until the check moved under the lock (`MarkFenced`, `ofGeneration`): a
  standby whose lost acquisition had put it back to reading could have ended `Fenced`.
- **Unclean leader election.** Amazon MSK's default configuration sets
  `unclean.leader.election.enable` to `true` for clusters without tiered storage
  (its documentation, read 2026-11-14). With it, a partition that has lost every
  replica in sync goes on from one that is behind, and what was committed past that
  replica is gone — a quorum outage would then cost data, where every run here cost
  time only (Kafka's own default, and this profile's, is `false`). The topics the log
  creates now carry the setting themselves (`KafkaLog.EnsureTopicAsync`, the service's
  blob topic); topics that exist are not altered (Open for James 78).
- **The standby's wait after the brokers return can run out on a leader that is about
  to end anyway** (the fourth bullet at the top): six seconds from the moment the
  brokers can commit. On the five-minute timeout the leader was heard 0.3 to 1.1 s
  after the brokers were back in every run read, so the wait is left as it is (Open
  for James 79).
- **`debug=broker` on a standby with brokers killed** writes every refused connection
  to the node's log: 4.3 GB in the first minute of the first traced run. CLAUDE.md
  says so now; it is Open for James 75 seen from another side.

**Public API added.** `KafkaLog.UncleanElection` (the setting's name, a constant).
Changed: `KafkaLogOptions.TransactionTimeout` defaults to five minutes;
`AcquireWriterAsync` fails with a plain `KafkaLogException` for a timeout the brokers
refuse; topics the log creates carry `unclean.leader.election.enable=false`; a
standby's `LeadershipStatus.HeartbeatAge` counts from when the leader wrote its last
record if that record was a takeover timeout old or more when it was read. The backend's settings (`BackendInfo`, the admin's cluster page) show
`transactionTimeout`. Test seams (internal): `HeartbeatDating`,
`KafkaLeadership.Heard(record, now)`, `KafkaLog.ShortReadLeadersFor`,
`ShortReadLeadersAsked`. The failover app takes
`INCREMENTAL_KAFKA_TRANSACTION_TIMEOUT_SECONDS`.

**Tests.** Kafka (`tests/Incremental.Kafka.Tests`, one broker, or none for the first):
`HeartbeatDatingTests` — a record that waited to be readable is dated when it was
written; a date is never after the reading nor before the writing (2,000 records,
prompt and late); what a starting watch reads is dated now; a new term and a clock
set back start again; what is remembered is two stretches of the writer's time; a
late record that begins a stretch is dated by the one before.
`KafkaLeadershipTests` — a dead leader's last heartbeat read late does not call the
takeover off and the silence goes on; a heartbeat written within the ceiling calls a
ceiling takeover off; an old heartbeat shown as the brokers return does not end the
wait for the leader; the silence a standby reports counts from the writing.
`KafkaLogTests` — a transaction timeout the brokers refuse is said by name, twice, is
not a fence, and another log acquires as term one; a short read asks for a topic's
leaders again once its answer is a second old and not before; the topics are created
with unclean election off as their own setting. Service
(`KafkaServiceProcessTests`, the `kafka3` profile): the no-quorum test asserts that
nothing moved under the timeout.

**Mutations.** Nineteen, each run against the tests named for it; seventeen killed,
one answered by simpler code, one equivalent. The log (8, all killed by
`KafkaLogTests`): the default timeout back at 60 s; a refused timeout not recognised;
the refused timeout keeping the failed client; the client's generation checked outside
the lock (the test fails as it did before the fix); a met topic never asked about
again; every short read asking; the time of the answer not kept; topics created
without the unclean-election setting. The dating and the leadership (11): every
record dated when it is read; any record calling a takeover off; the takeover timeout
deciding at the ceiling; a new term keeping the old clock; a clock set back not
noticed; stretches never ending; a new stretch forgetting the one before; an old
record ending the wait after the brokers' return; the silence counted from the
reading — nine killed, by `HeartbeatDatingTests` and the four leadership tests. "A
new writer keeps the old clock" survived, because a term is one writer's and the
writer's name said nothing more: the name is no longer part of what the dating keeps.
"A takeover after a resignation is called off only by a fresh record" survived and is
equivalent: what follows a resignation is another writer's first record, which is
dated when it is read. The default timeout's own check is the broker test, which
cannot run in CI: its assertion failed in none of the ten runs at five minutes and
would have in ten of the fourteen at 60 s. Four of them (the three of the short
reader's question and "the silence counted from the reading") were run again after
their two tests had been made to tolerate a slow machine: killed. The lists and
results were in the session's scratch directory (`mut.py`, `mut2.py`), not the
repository.

**Numbers.** On the final tree: `dotnet build --no-incremental` 0 warnings;
`tests/Incremental.Tests` 710, 709 passed, 1 skipped; `tests/Incremental.Kafka.Tests`
146/146 (134 before this step); `tests/Incremental.Service.Tests` 253, 247 passed, 6
skipped — the broker tests, run on `kafka3` as above; `Pipelines.Tests` 17/17. Smokes:
`--headless` 55 claims, `--headless --nodes 2` 70, `--headless --kafka` 69 (2.8 s from
the kill to b leading), `TradeEnrichment` 13; all exit 0. `benchmarks -- --assert` was
not run: `src/Incremental` was not touched. Two of the new tests were then made to
tolerate a machine too slow for their clocks, and the Kafka project ran again: 146/146.

**Deviations.**

- **The default timeout changed**; a deployment that relied on a dead writer's
  transaction being gone after a minute with no node running now waits five.
- **Heartbeats are dated by their writer's wall clock**, and the date is acted on for
  a record it puts a takeover timeout or more before its reading. A leader whose clock
  is set back by less than the gap between two heartbeats, or slowed by its time
  daemon, has its records dated early by up to that much — under a second — for at
  most ten seconds of its heartbeats, which matters only to a record that was already
  most of a takeover timeout late; one whose commit
  takes longer than the takeover timeout after its last heartbeat, and that is heard
  only then by a standby already taking over, is no longer spared. Both need a standby
  that is taking over — the leader's probe unanswered or not configured, or the
  ceiling — and neither was seen.
- **Short reads cost a metadata request** when the last answer for the topic is over a
  second old: one for each table in a takeover, and one a second per table for a
  standby's statistics.
- **The cluster with controllers of its own is not a profile of the repository**: its
  compose file is kept with the run records, and CI does not use it.
- **The first commit of this step says "ten runs of sixteen"** for the outages past a
  60 s timeout; the count is ten of fourteen, as above.

**Left undone.**

- **A leader that takes the log again in place** after its transaction timed out
  (Open for James 67): specified, not built.
- Nothing was run on MSK. The figures for a cluster with its own controllers are from
  eight runs on one machine; a ZooKeeper cluster was not run at all.
- The one 40 s row of round three that ran into the timeout (61.5 s) was not seen
  again: on the five-minute timeout it cannot happen, and at 60 s the 40 s outage was
  not rerun.
- Open for James 75 (a standby's processor time while a dead broker is still listed)
  is as it was.
- The 0.9 to 1.8 s between the brokers giving a broker up and the standby leading was
  not taken apart further: a new partition leader does not give its end until a
  follower has caught up with it, the coordinator loads its partition, and the dead
  leader's transaction is aborted; which of them is the longest was not measured.

**Finished by a second session** (2026-11-15). The session that did the work above was
cut off before it could report. A second one read its three commits against the
leadership's rules, its tables against the records (they agree), and ran the checks
again. It changed one thing.

*A heartbeat younger than the takeover timeout is heard when it is read.* As built
above, the silence counted from every record's writing. A heartbeat is written when its
transaction begins and is readable when the transaction commits, so on brokers where a
commit takes *c* each record is *c* old when it is read and the next is read *c* later:
counted from the writing, a leader that commits all along is silent for 2*c*; counted
from the reading, as before S8, for *c*. The dating absorbs a slowdown only once its
two five-second stretches have forgotten faster deliveries, so for up to ten seconds
after brokers turn slow the tolerance was half of what it had been. A test written to
find out — a leader whose every commit takes 1.2 s, a takeover timeout of 2 s, a
standby with no probe — had the standby fence the leader in three runs of three on
that build, and in none of three with the leadership as it was before S8; the same
again, three and three, with the clocks the test now has (1.8 s against 3 s, wider for
a slow machine). With a probe that answers the standby would have
waited for the ceiling, so the service as the guide configures it was not exposed
unless the probe failed in the same seconds; a deployment without `PeerProbe`, and any
user of `KafkaLeadership` without one, was. The second cost was quieter: the delivery
lease is argued from "the standby's silence starts no earlier than its reading of the
heartbeat", and a silence counted from an estimate of the writing puts the writer's
wall clock into that argument — an estimate up to a second early (above) against a
lease margin of one second.

Now `KafkaLeadership.Heard` uses the date only for a record it puts a takeover timeout
or more before its reading: that record does not start the silence again, and the
rules for calling a takeover off and for ending the wait after an outage are as above.
A younger record counts from its reading, exactly as before S8. The case S8 fixed is
untouched — the dead leader's last heartbeat was 8.9 s old — and what is given back is
the case nobody measured: a dead leader's last record read late by less than the
takeover timeout starts the silence again, as it always did (Open for James 80).
`KafkaLeadershipTests.A_leader_whose_commits_are_slow_and_inside_the_takeover_timeout_is_left_alone`
is the test (it says so and checks nothing on a machine where a commit took four
fifths of the timeout), and `The_silence_is_counted…` has a third record, half a takeover timeout
old. Four mutations, all killed: every record counted from its writing (S8's first
build: both tests fail), every record heard now (before S8: the dead leader's
heartbeat calls the takeover off again), "younger" meaning within the ceiling,
nothing younger.

*What else the reading looked for and did not find.* A node whose transaction timeout
the brokers refuse is not `Fenced` and fences nobody: the brokers refuse before they
touch the id (the refused-timeout test now also asks with a writer in place, which
commits again afterwards), and the node stands by and says the limit's name at every
attempt, a takeover timeout or two apart — it never leads, which is the
configuration's doing, and nothing but its log says so. The five-minute timeout changes nothing in how soon
a dead leader is found: that is the heartbeat's silence, and a takeover's
`InitTransactions` aborts whatever was open. The producer's socket timeout stays at
`KafkaClientDefaults`' six seconds (the client would otherwise take it from the
transaction timeout). The short reader's second question waits at most half a second
and its failure is not the read's, so a broker that pauses adds at most that to a read
that would have waited on the same broker.

*The check* (records `docs/design/runs/2026-11-15-j4-check.*`; the profile's containers
removed and started nine seconds before): the broker tests once each, 7 of 7 in
7 min 27 s. Seventy seconds without a quorum: the leader kept term 1, no process
ended, the sink was silent 84.8 s, the standby held 377 times and answered its status
in 20 ms. One broker killed for 20 s — this time the active controller, the
coordinator and the leader topic's leader — moved nothing (14.2 s); every broker
killed in turn 14.3 s, shut down in turn 1.3 s. Broker 3 away 79 s with the leader
killed half way: the standby led 3.4 s after the kill. A takeover with broker 2 shut
down 1.5 s before: 4.4 s, of which 1.0 s was `InitTransactions` (one whole one-second
slice; 13 to 670 ms in the five such runs of record before it — one run, not looked
into). Then three takeovers with a broker killed beside the leader, each time the
active controller: 13.5, 13.3 and 13.9 s, none called off. The standby's log has the
late records: a live leader's shown 7.1 to 14.5 s late by a broker's loss and 83.4 s
late after the outage, with the leadership where it was.

*Numbers on the final tree.* `dotnet build --no-incremental` 0 warnings;
`tests/Incremental.Tests` 710, 709 passed, 1 skipped; `tests/Incremental.Kafka.Tests` 147/147 (146 before:
the new test); `tests/Incremental.Service.Tests` 253, 247 passed, 6 skipped — the six skipped are the
broker tests, run as above; `Pipelines.Tests` 17/17. Smokes: `--headless` 55 claims,
`--headless --nodes 2` 70, `--headless --kafka` 69 (3.1 s from the kill to b leading),
`TradeEnrichment` 13; all exit 0. `benchmarks -- --assert` was not run: `src/Incremental`
is untouched.

#### The fourth review's findings

A review of round four found nine defects, confirmed each, and an early critic pass
added four more items (E1, E2, E5, E7). They are taken in the review's order; each has
a test that failed before its fix unless it says otherwise.

**1. A record whose event time cannot be read faulted a keyed input that forgets final
rows, under the dead-letter policy** (`UpsertOperator`). The input's own state runs the
event-time selector — a row's deadline is its event time — upstream of the declaration
whose guarded loop dead-letters such a record. Under `DeadLetter` the deadlines are now
read by a loop of their own (`DeadlinesGuarded`), chosen once per step: a row whose time
cannot be read is not held, whatever its key held before goes (its retraction was
emitted), and nothing is reported there — the declaration downstream runs the same
selector on the row and reports it once. A later upsert of the key is a new row, as it
is for a key forgotten as final. Under `Fault` it faults, as before. Test:
`EvictWhenFinalTests.A_row_whose_event_time_cannot_be_read_is_dead_lettered_and_not_held…`.

**2. Adopting the eviction on state that was written without it left that state for
good.** A deadline is set when a row or a fact is written, so a checkpoint from the
unbounded definition has none, and nothing of it would ever fall due under the new
one. `StateManifest.Problems` now reports it: a `{operator}.deadlines.w{n}` store that
is new while the checkpoint holds any store of the same operator. So
`CircuitBuilder.CheckState` says it beforehand, `Circuit.Start` refuses with
`CircuitStateMismatchException`, and the service refuses the replace with
`state-incompatible` before anything stops; the answer is `state=reset`, which the
guide now says (*Adopting it on a pipeline that is running*). It applies to `EvictWhen`
added to a grouping that has state as well: the keys never touched again had the same
fate. *Not built:* giving the old state deadlines at the first start — the stores have
no enumeration in their interface (*Open for James* 82). Test:
`EvictWhenFinalTests.Adopting_the_eviction_on_a_checkpoint_that_holds_state_without_deadlines_is_refused`.

**3 and 5 (and E1). The growth flag** was one ring of samples of a pipeline's total. It
flagged a bounded pipeline whose open window was longer than the alarm period — the
reference definition at its default `openSeconds` among them, seen by the early critic
at 31 minutes — and it missed a leak beside a store that churns, because any fall of
the total cleared it. `StateGrowth` now keeps a ring per store and judges each by
itself. A store that closes on the watermark (every store of an operator that has a
`.deadlines` store, by name) is not held to the rule while the committed watermark
moves — it is filling what its definition keeps open — and is flagged only if the
watermark has not moved in the whole period while it grew, with a text of its own. The
watermark is read from the last commit's offsets (`$watermark`), once per sample, so a
standby judges as the leader does. A flag now also clears when its store has not risen
for a whole period (a count per reference key stops growing and never falls), and for a
stalled store when the watermark moves. `PipelineStatus.StateGrowth`
(`PipelineStateGrowth`: store, since, whether the watermark stood still), `stateGrowth`
in the admin API, and the store's name in the event, the list's badge, the attention
entry and the state card's notice. Tests: `StateGrowthTests` (12; a definition of the
reference shape through the harness, open for an hour and fed for 32 sampler minutes;
and on a node, a pipeline that fills, one that leaks beside churn — its total fell at
five readings of sixteen — and the two there were). *What it still does not see* is in
the guide: a leak inside a store that also evicts, and growth slower than a sample
interval that pauses.

**4. A trade that first arrives behind the watermark is dropped, and nothing said so.**
That is what event time with a lateness means, and round four's `trade-enrichment` took
it on without saying it of first-time trades or of what moves the watermark. Now the
drop is counted: `WatermarkOperator` counts the insertions it drops (per worker, summed
by `WatermarkClock.LateRecords`; a retraction behind the watermark is not another late
record), `Circuit.LateRecords` and the counter `incremental.circuit.late_records` carry
it, and the service adds it up per pipeline: `PipelineStatus.LateRecords` and
`Watermark`, the counter `incremental.service.pipeline.late_records`, an event when it
starts and at most every ten minutes while it goes on, a notice on the pipeline's page,
`PipelineTestHarness.LateRecords`. The definition's comment, its `openSeconds`
description and the guide say that a trade is late by its time alone, that the
watermark follows the newest execution time read from any partition, and what a trade
stamped ahead does. Nothing guards against a time in the future (*Open for James* 81).
Tests: `EvictWhenFinalTests.A_record_first_seen_behind_the_watermark_is_dropped_and_counted_as_late`,
`StateGrowthTests.A_pipeline_counts_the_records_it_drops_as_late_and_says_so_once`,
`TradeEnrichmentTests.A_trade_that_first_arrives_behind_the_watermark_is_dropped_and_counted`.

*Mutations of 1 to 5:* 18, all killed in the end; two survived the first run (the
supervisor judging the total; the supervisor not reading the watermark) and were killed
by the node-level test's two new pipelines.

**6. Brokers that go silent without refusing a connection did not hold a standby at
the ceiling.** The fourth review froze two of three brokers (`docker pause`: the
connections stay open and nothing answers) for 45 and 70 s; in five runs of eight the
standby took over within half a second of the brokers' return from a leader that had
ridden the outage out. Two causes, both fixed (`KafkaLeadership.HeldByBrokersAsync`,
`KafkaLog.ProbeWritePathAsync`):

- *The question was not answered, and that held nothing.* The write-path question
  begins with a metadata request to any broker; with two of three frozen it sits on a
  frozen one until its two seconds are up (the admin client has no socket timeout to
  move it to the third), which was `WritePath.Unknown` — "cannot be told", no hold. It
  is now `WritePath.Unreachable`, an answer of its own, and it holds a standby whose
  leader answers its probe exactly as "nothing can be committed" does. An error a
  broker *sends* is still `Unknown` (`KafkaLog.Unasked`).
- *The leader's time after the outage was counted from the wrong end.* A leader is
  given two takeover timeouts to be heard once the brokers answer again. They counted
  from the last hold; but the question itself waits on the brokers — nine seconds were
  seen, answered in the instant they thawed — and by then the six seconds had gone.
  This was found by the first three runs made with the first fix alone: all three
  still moved the leadership, with two holds each and a takeover 0.6 s after the thaw.
  The time now counts from the first answer that is not a hold.

On the three-broker profile after both (the no-quorum test with
`INCREMENTAL_KAFKA_FAULT=pause`, which the test now takes): two of three frozen for
45 s, five runs, and for 70 s, three runs — the leader kept term 1 in all eight, no
process ended, the sink was silent 45.8 to 46.7 s and 71.1 to 72.0 s (the outage and
about a second: frozen brokers have nothing to re-register), the standby held 2 to 9
times and answered its status in 20 ms to 2.0 s. Tests:
`KafkaLeadershipTests.At_the_ceiling_a_standby_does_not_take_over_while_the_brokers_do_not_answer_and_waits_for_the_leader_when_they_do`
(the answer arrives after a wait longer than the leader's grace), and the mapping in
`What_the_brokers_say_about_writing…`. A debug line at each question
(`INCREMENTAL_LOG_LEVEL=Debug`) says what was answered and how long ago the standby
was last held, which is how the second cause was read.

**7. A takeover called off for a heard leader had already fenced that leader**
(`KafkaLog.AcquireCoreAsync`; *Open for James* 64, which is closed by this). The
acquisition looked at its call-off token between one-second slices of
`InitTransactions`. But the first slice sends `InitProducerId`, and when the leader
has a transaction open the coordinator's answer is to raise the epoch and abort that
transaction at once, and to tell the standby to ask again until the abort is done: the
leader is fenced while `InitTransactions` has not returned. A heartbeat heard in that
second called the takeover off — three times of three in the review's runs — and left
a fenced leader, a standby that had stood down, and nobody leading for a takeover
timeout. The token is now looked at only before the first call; a takeover that has
asked for the log goes through. What that gives up is the call-off itself, which is
left only the instant between the decision and the first request; a takeover that
begins during an outage of the coordinator alone is now carried through when it
returns, where it might have been called off. With finding 6 fixed a ceiling takeover
no longer begins while the brokers cannot be asked. Test:
`KafkaLeadershipTests.A_takeover_that_hears_the_leader_after_it_has_asked_for_the_log_goes_through`
(a leader stopped inside a transaction; the standby's calls for the id given 20 ms, so
that the first runs out while the coordinator aborts; the leader heard there).

**8. A takeover's table read that began as a partition lost its leader waited out a
doubled attempt** (`KafkaLog.RetryTransientAsync`). Admin calls are short attempts with
a metadata refresh between them, and the attempts doubled without a stop: 0.5, 1, 2,
4, 8 s. An attempt that begins with no leader hears nothing until its time is up, so
the 4 s and 8 s attempts ran to their ends although the partition had its leader
again after about nine seconds: 15.6 s for the table stage in the review's run.
Attempts are now 0.5, 1, 2, 2, 2, 2 s and only then 4 and 8
(`KafkaLog.AttemptTimeout`), so a call loses at most two seconds once the leader is
back while a broker's loss is being settled. Tests:
`BrokerFaultSeamTests.An_admin_call_loses_at_most_two_seconds_once_the_partition_has_its_leader_back`
(a call that hangs for its whole time while leaderless, 4.5 s: it returned 3.5 s after
the leader was back with the old schedule, under 2.5 s now) and the schedule itself.

**9. S8 said its ten green runs were of "the last commit that changed code or
tests".** True when written and not at the end of the round: nine later commits
changed code or tests. The sentence and the one about the twenty local runs now say
which commit they were of and that the later ones were not run ten times.

*Mutations of 6 to 8:* 8, all killed in the end; one survived the first run (the
probe reporting silence as "not known", which nothing exercised at the call site) and
was killed by a seam for the question itself failing (`BeforeWritePathQuestion`).

**What the frozen-broker and late-kill runs gave** (records
`docs/design/runs/2026-11-15-r4-review.*`; the three-broker profile, started for the
step). Seventeen takeovers with a broker killed 3.05 to 3.24 s after the leader — the
review's way of landing the broker's death in the standby's takeover: the standby led
11.4 to 16.8 s after the kill in sixteen and 3.2 s in one (the broker died after the
takeover was over). The wait was in whichever stage first needed a commit — the fence
(9.0 to 12.9 s), the storage's promotion (9.7, 10.1 s) or the start (8.2 to 13.0 s) —
which is the brokers' session timeout and nobody's to shorten here. The table stage
was 5 to 14 ms in sixteen and 612 ms in one: none of the seventeen landed in a table
read for more than one attempt, so the 15.6 s of finding 8 was not seen again, before
or after its fix; what holds the fix is the test.

#### The early critic's items (E1, E2, E5, E7)

**E1 — the growth flag against a pipeline's own open window** is findings 3 and 5
above. What the critic saw (the reference definition at its default hour, flagged at
31 minutes) is `StateGrowthTests.A_definition_that_keeps_trades_open_for_an_hour_is_not_flagged_while_it_fills`
and the node-level test's `fills` pipeline. The default `openSeconds` stays an hour
(*Open for James* 72); its description in the deploy form now says what a second of
it costs (eight entries per trade per second of the feed).

**E2 — what an `EvictWhenFinal` deadline costs.** S8 said "about a sixth of a join's
record" and the guide's table 56 bytes; the node's own state card showed deadline
stores as large as the facts beside them. Both were right somewhere. A record's
deadline is kept under the record (`EvictionTracker<KeyedValue<TKey,TFact>>`), so it
costs its map and queue where the record's objects are the ones the operator's own
store holds, and the record again where they are not. Measured (a scratch harness on
the core API: a keyed input, a remembered lookup and two retroactive joins over a
nine-column trade, `ChangelogStorage` on a `KafkaLog`, one local broker; the heap after
full collections; records `docs/design/runs/2026-11-15-r4-deadline-cost.txt`):

| per open trade | the node that read it from its source | a follower with typed stores | `EstimateSize` |
|---|---|---|---|
| no eviction (4 entries) | 706 B | 2,243 B | 2,044 / 2,084 B |
| `EvictWhenFinal` (8 entries) | 1,167 B | 3,891 B | 3,482 / 3,602 B |

- *On the node that read the trades* every store holds the same trade — the input's
  row, the lookup's remembered record, each join's fact, each deadline — and the four
  deadlines add 461 bytes, 115 each. The node's estimate, which sizes each store's
  entries by themselves, reads three times the heap there, with or without eviction.
  That was a known limit of the estimate ("an object two stores share is counted in
  both"); its size on a real shape was not known, and it is not small.
- *On a follower* each store's entries are read from the log as objects of their own,
  and the estimate is right to within 7 % (it is 7 % under). The deadlines add
  1,648 bytes to an open trade: 65 for the keyed input's, 384 to 598 for each record's.
  The same holds on any node for state it restored.
- So the estimate is what a standby holds, which is what both nodes must be sized for
  (the leader holds as much after a takeover until its restored state has been
  evicted), and it is not what a leader's heap shows. The guide's sizing section, the
  state card's help and `EstimateSize`'s documentation say so with these figures; the
  guide's table has the two kinds of deadline apart; S8's sentence above is corrected.
  *Not done:* making the estimate see sharing (it would have to follow one record
  through every store, or walk them), and making a follower share — a deadline that
  is a function of its record could be an index the fact store keeps, with no entry
  and no log record of its own (*Open for James* 82).

*In the state topic* an open trade is one record per entry when it arrives and one
tombstone per entry when it is final: 16.00 records per trade through its life,
counted (`KafkaLog.RecordsWritten`), against 4 without eviction. A deadline's record
and its tombstone carry the trade in their key (227 to 336 bytes of record for the
three, 27 for the keyed input's). On the broker's disk, zstd: 88 bytes per trade
without eviction, 155 per open trade with it, 258 for a trade's whole life. Flat out
with a thousand trades to a step that circuit ran 14,850 trades a second — 238,000
state records a second — committing a step in 67 ms on average and 132 ms at most; a
follower with typed stores had the leader's entry count at eleven of eighteen
readings and was one or two steps' entries behind at the others (8,000, and once
16,400), all but one of them while the state was still filling.

*On the service* (the example on the connector stack, `--kafka`, two processes,
`--rate 1000`, three minutes; records
`docs/design/runs/2026-11-15-r4-service-1000-per-second.jsonl`): `trade-enrichment`
level at 960,400 to 961,800 entries on both nodes (2 × 4 × 1,000 × 120), 287 MB by the
leader's estimate and 296 MB by the standby's; the state topic took 32,000 records
and 0.69 MB a second for the two trade pipelines once both were full (that is 59 GB a
day written and tombstoned again, which the log cleaner has to follow — not watched
for longer than these minutes); the standby's state lag read 0 at sixteen of nineteen
readings and 658 to 1,678 records at three; the node's steps averaged 15 to 21 ms in
the seconds that had none of the alert pipelines' two-second file steps in them; once
the state was full the leader's live heap was 250 to 360 MB and the standby's 600 to
870 MB for the same state, the sharing again. A `kill -9` of the leader then: the standby led
after 3 s of silence and 867 ms of takeover, 284 ms of it the service's (six
pipelines started as built, 960,000 and 770,000 entries of state among them) — the
guide's takeover screenshot, retaken from this run.

**E5 — the broker-failure residue.** *Open for James* 75 is found and fixed: see the
item. The reconnect delay of `KafkaClientDefaults` was being reset, not ignored. The
log's admin client asks where a partition ends of the partition's leader; while that
leader is a broker that has died and is still listed, the request waits for the
broker, and the client's way of waiting is to ask for the broker's connection again
at every change of any broker's state — which sets the delay before the next
connection to nothing. A refused connection is a change of state. In a traced run
(`debug=broker`, one broker of three killed for 20 s) the standby's admin client
logged 231,436 such requests and as many refused connections in eight seconds, about
30,000 a second, at 1.2 to 1.4 processors; every other client of the node had two to
four. With `enable.sparse.connections=false` on admin clients nothing asks: the same
run had 148 refused connections from all clients together, and the standby used at
most 0.16 to 0.23 of a processor in any second the broker was away, in four runs
(three kills, one freeze). The one-broker test now reads the standby's processor time
each second and fails above 0.9 of a processor in more than two of them; with the
setting turned back on by the test's own client setting it failed (five seconds, as
it should). The CI job is under *Numbers* below.

**E7 — seven small things in the admin UI,** each looked at in Chromium afterwards
(the example, two nodes in memory, 1440 and 390 px; no console message; no page wider
than its window):

1. *Dead letters flooded the overview's recent events.* A pipeline that keeps
   dropping records now says so as an event at once and then once a minute
   (`PipelineSupervisor.DeadLetterEventEvery`; the log line stays every ten seconds),
   and the card has a switch per kind of event, so a kind can be left out; it shows
   the newest forty of the two hundred it reads.
2. *A standby's rate tiles cut their text.* They say "shown on the leader".
3. *The state timeline named the wrong generation for `Stopping`* after a replace:
   the spec is already the new one when the old generation stops. It names the
   generation that runs (`SupervisorTests`).
4. *The takeover plan said the old leader "is fenced and ends".* It says the node
   stops leading and is started again as the standby by whatever supervises it.
5. *In memory the memory card named a Kafka client.* The texts depend on the backend.
6. *The guide's takeover screenshot* (Kafka) is retaken, from the run above.
7. *Pipelines were listed by their plugin's hash alone.* The list and the pipeline's
   page show what the plugin's build calls itself beside it (`build` in the admin API,
   from the package's manifest; `AdminApiTests`).

#### The fourth review's step: what it added, changed and left

**Public API added.** Core: `Circuit.LateRecords` and the counter
`incremental.circuit.late_records`. Service: `PipelineStatus.StateGrowth`
(`PipelineStateGrowth`), `PipelineStatus.LateRecords` and `Watermark`,
`PipelineTestHarness.LateRecords`, the counter
`incremental.service.pipeline.late_records`. Admin JSON, on a pipeline: `stateGrowth`,
`lateRecords`, `watermark`, `build`. Kafka: `KafkaClientDefaults.Settings` has
`enable.sparse.connections` (applied to admin clients only). Test seams, internal:
`KafkaLog.InitTransactionsSlice`, `BeforeWritePathQuestion`, `Unasked`,
`AttemptTimeout`, `WritePath.Unreachable`; the `slice` point of `WhileAcquiring`.

**Behaviour that changed.**

- `Circuit.Start` and `CircuitBuilder.CheckState` refuse a checkpoint on which an
  operator that holds state gains deadlines; the service answers `state-incompatible`.
  Before, such a start went through and left the old state for good.
- The growth flag is per store and waits on the watermark for stores that close on it;
  its event names the store; a flag clears after a period without a rise as well as on
  a fall.
- A dead-letter event is `N record(s) dropped, the last at {operator}`, at most one a
  minute per pipeline (it was `… dropped at {operator}` every ten seconds).
- A takeover is not called off once it has asked for the transactional id. A standby
  at the ceiling waits for brokers that do not answer, and the leader's time after an
  outage counts from the first answer that is not a hold.
- An admin call's attempts are 0.5, 1, 2, 2, 2, 2, 4, 8 s (they were 0.5 doubling to
  32).
- Admin clients keep a connection to every broker.

**Deviations and choices.**

- Finding 2 is a refusal, not a repair: the old state is not given deadlines (82).
- Finding 4 is a count and a notice, not a guard: a record stamped ahead still moves
  the watermark (81).
- Finding 7 gives up the call-off rather than trying to tell whether the first request
  had left; S7's line was drawn where the client returns, and the coordinator acts
  before that.
- The node's state estimate is left as it is — a standby's figure — and said to be
  one, rather than made to see what stores share (E2).
- Sparse connections are turned off for admin clients only, not for every client
  (75).
- The frozen-quorum fix was made in two goes: the first three runs after the first
  half still moved the leadership, and those records are kept.

**Numbers on the final tree.** `dotnet build --no-incremental`: 0 warnings.
`tests/Incremental.Tests` 717, 716 passed, 1 skipped (710 before the step: seven new);
`tests/Incremental.Kafka.Tests` 150 of 150 (147: three new); `tests/Incremental.Service.Tests`
260, 254 passed, 6 skipped — the broker tests, run below (253: seven new);
`Pipelines.Tests` 18 of 18 (17). Smokes: `--headless` 55 claims, `--headless --nodes 2`
70, `--headless --kafka` 69 (2.8 s from the kill to b leading), `TradeEnrichment` 13;
all exit 0. `benchmarks -- --assert`: all 10 contracts hold. Mutations: 32 in
all, every one killed in the end; three survived a first run and got a test (two in the
supervisor's use of the growth rule, one at the probe's call site). Not mutated: the
dead-letter event's one-a-minute rule, which no test times. The admin UI's
changes were looked at in Chromium on the example, in memory (two nodes) and on Kafka
(the takeover).

**The broker tests on the final build** (the three-broker profile; records
`docs/design/runs/2026-11-15-r4-final-brokers.*`): 7 of 7 in 7 min 14 s. Seventy
seconds without a quorum (stopped): the leader kept term 1, the standby held 23 times,
the sink was silent 84.3 s. One broker killed for 20 s — the active controller, the
coordinator and the leader topic's leader: nothing moved, and the standby used at most
0.17 of a processor in any second. Every broker killed in turn: 14.5 s of silence;
shut down in turn: 0.7 s. A broker away 64 s with the leader killed half way: the
standby led 3.4 s after the kill. A takeover with a broker shut down 1.5 s before:
3.3 s.

**CI.** The step's first four pushes each had a run and all four were green
(43701879223, 43707559251, 43707944137, 43708659102: build-and-test, connector-stack,
perf-contracts). The fifth (43710439541) was red in build-and-test, on a test this
step had not touched: `FileConnectorTests.The_append_sink_holds_each_record_once_across_a_restart…`
read the pipeline's committed step as soon as the sink's objects were there, and a
step's objects are there before the step is committed (commit after delivery) — on
that runner the first commit had not happened and there was no step to read. It is
the race S8's CI section fixed in eight other tests (`TestService.Settled`), in one it
missed; the test waits for the checkpoint now. A run of the same commit started by
hand a few seconds later (43710447803) passed build-and-test, connector-stack and
perf-contracts. The run of the push that carries this paragraph, and the
`broker-failures` job started by hand on it, finish after it is committed: they are
in the step's report.

**Left undone.**

- The 15.6 s table stage of finding 8 was not reproduced, before or after the fix:
  seventeen attempts did not land a broker's death inside a table read for more than
  one attempt.
- Nothing ran for longer than three minutes; the log cleaner on a state topic that
  turns over at 32,000 records a second was not watched, nor three brokers under that
  load.
- A leak inside a store that also evicts, and a stalled watermark on a pipeline whose
  stores do not grow at twelve samples of fifteen, are not flagged.
- An admin client made from a consumer's or a producer's handle still connects on
  demand (75).
- MSK, IAM, S3 and the owner's adapters (the critic's E3), memory headroom over hours
  (E4), a reused trade id (E6) and the size of this document (E8) were not this
  step's.


### S9 — round five

The last round: [enrichment-service-next-steps.md](enrichment-service-next-steps.md) is
its plan. Each step adds its part here.

#### Event time where records enter (K1, K3, K6)

Three things the last critic left, and one place that answers all three: the input's
port. A record's partition is known only where it enters the circuit; a record refused
there never enters; and the port is the one place that sees an input's records in the
order they were sent, before a step makes a set of them.

**The binding.** `WithEventTime(selector, lateness)` called on an input itself — an
`InputStream<T>` or a `KeyedInputStream<TKey, TValue>`, not a stream derived from one —
gives the input's registration an `InputEventTime<T>`: the clock, the source's number,
the selector, the circuit's options. The port calls it for every record it drains
(`InputPort.DrainInto`, `KeyedInputPort.DrainInto`), on the coordinator, between steps;
the declaration's `WatermarkOperator` is then built with no source of its own and only
filters. A declaration anywhere else is what it was. Clustered circuits are not bound:
their processes share one time per source.

It follows that a definition must declare event time on the input to get any of this.
The example's `execution-quality` and `desk-exposure` declared it after a
`LookupAppendOnly`; they now declare on the input and re-declare without a lateness on
the new shape. That is a rule a tenant has to know, and the guide says it first.

**K1: a time per partition.**

- *The source's part.* `InputHandle.ExpectPartitionAsync(p)` — the source reads this
  partition and its records may be on their way — and `IdlePartitionAsync(p)` — the
  source stands at its end. They travel in the input's channel as entries with no
  offset, so they take effect in order with the records around them, and a run that
  is fed the same entries in the same steps has the same clock (which is what
  `EventTimeRestartTests` needs of anything that moves the watermark; an idle timeout
  in the clock would not have it). They are sent only to an input that keeps time:
  elsewhere a note would cost a step and a commit for nothing.
- *The clock's part* (`WatermarkClock`). A source's time is the least over its named
  partitions that are not idle. A partition named and not yet heard from holds every
  claim back: the watermark stays where it is. An idle one stops counting until its
  next record, which counts it again without anything being said. With every
  partition idle the time is the newest seen anywhere. Records of a partition nobody
  named, and unstamped records, go to the source's one slot as before, and that slot
  holds nothing back once partitions are named — which is what lets a format-7
  checkpoint, whose one time is the newest read from any partition, resume.
  `Circuit.WatermarkHeldBy` says which source, input and partition the last freeze
  found lowest; the service puts it in a pipeline's status and on its page.
- *The checkpoint.* Two marker entries per named partition, `$watermark#s{n}#m{p}`
  (its max, `long.MinValue` before its first record) and `$watermark#s{n}#i{p}`
  (1 or 0), restored at `Start()`. State manifest format 8. A format-7 checkpoint is
  resumed as it is and rewritten as 8 at the next commit; a build that writes 7
  refuses an 8, as it must — it would restore the newest of the partitions' times as
  the source's. For a pair of nodes that means the standby is upgraded first, as for
  any format change; what rolling upgrades do about it is B6's.
- *The Kafka source* (`KafkaSource.PartitionEnds`). It names every partition at every
  start, before the first record. It calls a partition idle when an end-of-partition
  answer from the broker finds that it has stood at the end for `IdlePartitionAfter`
  (5 s). It asks again every `IdlePartitionConfirmEvery` (1 s) by seeking to where it
  stands, which makes the client fetch afresh and report the end again — measured,
  the answer comes about a second after the seek, two of the client's fetch waits.
  A partition that has not answered for three intervals and two seconds (5 s) is no
  longer idle: it holds the watermark until it is read to its end and has stood there
  for `IdlePartitionAfter` again.

Three things the first builds got wrong, each found by a test:

1. *The idle flag in the key's name* (`…#a3` or `…#i3`). Redis keeps the marker with
   `HSET` and removes nothing, so after a partition's first change both names were
   there. `EventTimeRestartTests`' new partitioned cases failed on Redis only (three
   of three). Two keys with fixed names now.
2. *Idle by the passing of time.* The source called a partition idle when it had
   stood at the end for five seconds with evidence no older than five seconds — which
   a partition whose broker had died five seconds ago satisfies exactly. First run of
   the broker test, broker killed: 130 of 10,002 records late. A partition is called
   idle only on an answer now, never on a timer.
3. *Asking again before the answer.* With a confirmation interval shorter than the
   answer's latency every seek cancelled the fetch before it; the end was never
   confirmed and an empty partition held the watermark for good (the empty-partition
   test, 60 s). One question at a time.

*Measured* (records: `docs/design/runs/2026-11-15-k1-partitions.measurements.log`).

| | one time for the input (declared after a `Select`) | a time per partition |
|---|---|---|
| A backlog of 3 × 600 records, ten minutes of event time on each partition, lateness 5 s, one local broker; four runs | 1,155, 1,188, 951, 1,188 of 1,800 dropped as late | 0, 0, 0, 0 |
| The same backlog on three brokers, where the client reads the partitions' leaders side by side; three runs | 685, 821, 0 | 0, 0, 0 |
| A restart onto 3 × 300 records behind partitions the checkpoint called idle; seven runs | not run | 0 in each |
| Two empty partitions of three: until they stop holding the watermark (`IdlePartitionAfter` 2 s in the test) | — | 2.0, 2.0, 2.1 s; 2.5 s on three brokers |
| Three brokers, six partitions, 300 records a second stamped as produced, lateness 5 s, the source's defaults; one broker **killed** for 20 s; three runs of the build as committed | 913, 803, 765 of about 10,100 | 0, 0, 0 |
| …one broker **frozen** (`docker pause`) for 20 s; three runs | 747, 1,223, 822 of about 9,900 | 0, 0, 0 |

The two columns of a broker run are one run: two circuits read the same topic side by
side. Four earlier runs of each fault are in the record too — two before the baseline
circuit was added (0 late), two with it (2,074 and 955 killed, 2,174 and 1,092 frozen,
against 0) — and so is the first run of all, with the source that called a partition
idle on a timer: 130 late with a broker killed. The service's own six broker tests
(`KafkaServiceProcessTests`, the `kafka3` profile) ran once on this build: all passed.

The example's Kafka topics for trades and quotes have three partitions now, and the
`--headless --kafka` smoke publishes its takeover's 500 trades while nobody leads: the
new leader finds a backlog on three partitions and is held to one late record per
pipeline, the straggler the act plants (a new claim). The smoke's state bound allows
twenty-five trades for the watermark standing at the slowest partition's newest trade
(1,988 entries against 1,976 in the first three-partition run).

**K3: a bound on a time in the future.** `CircuitOptions.MaxEventTimeAhead`;
`ServiceOptions.MaxEventTimeAhead`, a day by default; `circuit.maxEventTimeAhead` in a
spec. A record whose event time is further ahead than that of the later of the node's
clock and the newest event time its input has taken is refused at the port as a record
its source could not read is: its offset consumed, a dead letter
(`EventTimeAheadException`) or a fault, as the error policy says.

- *Why the node's clock, which item 81 argued against.* The other bound — against the
  clock's own state only — is deterministic and wedges a pipeline for good after any
  quiet spell longer than the bound: every record that follows is "too far ahead" of
  the last. Against the later of the two a quiet feed is taken up again, and a feed
  whose time runs ahead of the node's (a replay with shifted times, a test) goes on as
  long as it moves in steps shorter than the bound. What it costs is 81's objection:
  a record refused and not yet committed may pass when read again later. What a
  committed step did stays done, and the verdict only ever moves from refused to
  taken.
- *Why at the port.* Inside the circuit an insertion and its later retraction could be
  judged differently (the reference moves towards the record), leaving a retraction
  with nothing to retract; a stateless operator cannot remember what it refused, and
  a stateful one would need an exchange. A record refused at the port never enters,
  and a keyed input's refused upsert never becomes the key's row, so nothing can come
  after it that takes it back. The one case left is a producer that sends its own
  retraction (weight −1) of a record that was refused, later, when it would pass;
  the built-in sources send none.
- *The default is on, and that changes what a deployed pipeline does* with a record
  more than a day ahead: a dead letter or a fault where it was a stall. It is what K3
  asks for; `StateGrowthTests`, which stamps its trades in 2100 on purpose, turns it
  off, and a pipeline whose event time is rightly in the future sets its own bound.
- A record less than the bound ahead is still taken and still holds everything behind
  it late for up to the bound: the harm is bounded, not gone.

**K6: an id that comes back.** A sound rule exists for a key that comes back *after*
its record's open window, and none within it.

- *The rule* (`RecordEviction.Supersedes`). On a keyed input that forgets its final
  rows, an upsert whose event time is later than the held row's by more than the
  lateness begins a new record: the held row is final by the newer one's own time —
  one step later the watermark will have passed it — so `UpsertOperator` emits no
  retraction for it. And `KeyedInputPort.DrainInto` looks at each upsert before it
  takes it and ends the drain before one that supersedes an upsert of the same key
  already drained, so the two are never in one step. Both halves are needed. A
  step's commands for a key collapse to the last, so without the cut the earlier
  record is never delivered (the smoke's finding, item 71); and a delta is a set, so
  the two rows in one step would reach an upsert sink keyed by the id in no order.
  Steps are the only order a sink sees.
- *What it makes independent of the steps.* For a key whose next record is more than
  the lateness later in event time: the earlier record is delivered, not retracted,
  and delivered before the later one — read a record a step, as one backlog, or
  anything between, and whether or not the watermark had passed the earlier one when
  the later arrived. `PartitionWatermarkTests.An_id_that_comes_back_after_its_window…`
  (400 trades over 40 ids, amendments among them, five layouts, one and four
  workers: the same net rows and the same last row per id) and
  `TradeTimeTests` on the example's definition through the harness (the sink's rows
  equal one by one and as a backlog). The smoke's act 2 publishes its 2,000 trades at
  once now, where it waited between two halves; with the rule taken out it fails as
  item 71 says (1,421 rows of 1,446).
- *Why there is no rule within the window.* An amendment and a reused id are the same
  thing to the input: an upsert of a key that has a row. Their times cannot tell
  them apart either — an amendment may move a record's event time. And what an
  upsert near the edge of the window does downstream depends on whether the
  retraction of the row it replaces is still ahead of the watermark, which is frozen
  between steps: it depends on what was read before, and in which step. That is how
  lateness works in this engine for every record (a record a step behind the one
  that moves the watermark is late, the same record in the same step is not), not a
  property of keys, and making the watermark move within a step would give up what
  the freeze is for — every worker seeing one watermark for a whole step. So within
  the window the guide says: an id must not be used for another record within the
  open window, and why.
- *A late version of a record is not applied.* Found while reading the upsert for
  the rule above, and built with it: an upsert whose own event time is behind the
  watermark was applied like any other — the row the key held was taken back, and if
  that row was still open its retraction passed the declaration while the late
  version's insertion was dropped there. One late version of a trade deleted the trade
  from everything downstream (probed before the fix: trade 7 at 200 s, the watermark
  at 180 s, a version of it stamped 105 s; afterwards only trade 8 stood). The input
  now leaves the key's row alone and hands the late version on to be dropped and
  counted (`A_late_version_of_a_record_does_not_take_back_the_open_row_its_key_holds`).
  Whether a record is late still depends on the step it is read in, as it does for
  every record.
- *What the rule costs.* An amendment that moves a record's event time forward by
  more than the lateness is now taken for another record, always. Before, it was
  taken for one unless the watermark had already passed the old row — which in a
  busy feed it usually had, so the rule makes the common outcome the only one. The
  guide says it. On the port, a keyed input with `evictWhenFinal` reads the event
  time of each upsert and keeps the keys of one drain in a dictionary: one selector
  call and one hash insert per upsert, not measured by itself.

**Tests.** `PartitionWatermarkTests` (17: the clock per partition, idle, restore,
format 7, the keyed input, the bound under both policies and its two clocks, the
returning key); three scenarios of `EventTimeRestartTests` fed by partition with idle
and re-named partitions on every backing (12 cases: killed and restored at random
steps, outputs, state and the marker equal the uninterrupted run's);
`KafkaPartitionWatermarkTests` (three on the connector stack, two on three brokers,
skipped elsewhere); `TradeTimeTests` (3) on the example's definition; one node-level
test of the bound in `StateGrowthTests`. Mutations, each run against the new tests:
the operator never beginning a new record, the port never cutting, the source's time
the newest instead of the least — killed by 3, 3 and 6 tests; the rule removed whole
— killed by the in-memory smoke.

**Left.**

- A declaration that is not on the input has one time and no bound, and nothing warns
  the author: the dry run does not say "this input's event time is declared downstream
  of it". The guide does.
- A lateness under five seconds is not protected from a broker's loss: the other
  partitions move the watermark for up to that long before the source knows a
  partition has stopped answering.
- A quiet partition delays closes by up to `IdlePartitionAfter`, and a restart or a
  takeover by at least that: every partition holds the watermark until it has been
  read to its end.
- The `files` connector and a tenant's own connectors name no partitions. A connector
  that reads a partitioned source can (`ExpectPartitionAsync`, `IdlePartitionAsync`
  are public); the guide's connector section does not show it.
- The two admin UI tests that need a browser (`AdminUiMorphTests`,
  `AdminUiTablesTests`) were skipped on the machine this step ran on — it has no
  Chrome — after a change to `views.js` (one helper and two sentences). CI's
  `build-and-test` ran them green on the commit that made the change and the two after
  (43768221117, 43770594077, 43771626891); `views.js` has not changed since.
- *CI.* Of the step's first three pushes, `build-and-test` and `perf-contracts` were
  green and `connector-stack` red at its Kafka step each time: on the first for two
  source tests that counted the channel's places (the notes about partitions took
  two; they are no longer sent to an input without event time), then because the job
  demands that no Kafka test is skipped and the two broker tests skip on one broker.
  The workflow now expects exactly those two there and runs them in
  `broker-failures`. That change has not run: the runs of the last four pushes were
  not started ("recent account payments have failed or your spending limit needs to
  be increased"), which is the account's, not the branch's. Everything those jobs
  run was run here on the last commit's build, the broker tests included.

#### Rolling upgrades across a change of a persisted format (B6)

Until this step a build that changed a persisted format needed both nodes replaced
together — and the step before this one had just made such a change (state manifest
format 8, which the build before it refuses). What was built is one mechanism for
every such change, with that one as its first case.

**The level.** `Storage/PersistedFormat.cs`. A *format level* is a number that names
everything a node writes that another node must read: the state log's values and commit
records, the state manifest and the keys of the checkpoint's marker, the table logs'
records (the catalog is one). Level 1 (`FormatLevel.Base`) is what the build before
round five wrote; level 2 (`PartitionTimes`) is manifest format 8 and the partitions'
event times in the marker. A node has one `PersistedFormat`, shared by its log, its
storage and its circuits: `Reads`, fixed — the build's level, or a lower one the node
is pinned to, in which case it refuses what a build of that level would refuse — and
`Writes`, the deployment's, which starts at 1, is never lowered, and is read once per
transaction. A change of a persisted format is from now on a new constant there, the
new format written only where `Writes` has reached it, and the reader of everything
below it kept.

**Who decides.** On Kafka the writer announces the level it writes in every record of
the leader topic (`LeaderRecord.Format`, a member a build from before levels ignores
and never writes; absent reads as 1). A new writer goes on at the level it reads there
with the term (`AcquireCoreAsync`), and at no less than the highest level it has itself
met in a log (`PersistedFormat.Seen`: a manifest of format 8, the proving byte) — which
is what keeps the level when a writer from before levels led in between and announced
none. `KafkaLog.RaiseFormatAsync` raises it: a heartbeat that announces the level is
committed first, and only then is anything written under it, so no reader meets a
record of a level it was not told of. In the service the leader raises
(`ServiceNode.RaiseFormatLevelAsync`, once a second, with the probe it already makes):
when its peer has just answered as a standby and said what it reads (`;fmt=N` in the
probe's detail, which a reader skips if it does not know it; none reads as 1), to the
lower of the two nodes' levels. No answer decides nothing. A node told of no peer
raises to its own level at once; `ServiceOptions.HoldFormatLevel` stops every raise.

**Who is refused, and when.** A node that does not read the level the logs are at:
does not take over by itself (`WaitToTakeOverAsync`); is refused by
`AcquireCoreAsync` before `InitTransactions`, so it fences nobody — from what it heard
of the leader topic, or from a read of the topic's tail when it heard nothing; stops
following at the first commit record or value of the level (`FormatLevelException`
from `ChangelogStorageProvider.ApplyReplicated` / `ReadCommit`, and from a table log's
`Readable`); and says `format-level` as the first reason it is not ready. `GET node`
has `formats: { reads, written, held }`.

**The first real case: manifest 7 → 8.** `StateManifest.Problems` takes the node's
level: a format-8 checkpoint is refused by a node that reads level 1 (as the old build
refuses it) and formats 6 (without event time) and 7 are read by every node; what the
circuit *writes* is no longer the build's constant but `Writes` at each commit
(`Circuit.CommitCheckpoint` restages the manifest when the level has moved, and
`WatermarkClock.AppendCheckpoint` writes the partitions' keys or not by the same
level). At level 1 the marker has one time per source — the newest across its
partitions, which is what the older build would have written there and what it takes
it for. A level-2 node resuming such a checkpoint holds the watermark where it was
committed until its source has named its partitions again and each has brought a
record or is idle: it never regresses and makes nothing late that was not.

**The proving level.** A real change of a *format byte* — of the state log's values or
the table logs' records — has not happened since layouts, and the mechanism would be
unproven for exactly the case the plan names. `FormatLevel.Proving` is a level no
build writes or asks for (internal; a test's `PersistedFormat.ForTesting`): under it
every state-log value and every table-log record is the level below's with byte `0x7E`
in front, which a node below the level refuses as a build refuses a byte it does not
know. It costs one branch per value read and per transaction written. It is how the
whole path — hold back, hear, raise, refuse — runs on every build against formats that
really are unreadable below the level.

**What the state manifest's history means now (Open for James 36).** Said plainly:
nothing changed for format 6. A reader is kept for every format that *holds* what a
newer one needs; a format-6 checkpoint of an event-time pipeline holds neither the
watermark nor the deadlines, so there is nothing a reader could read, and it is
refused at every level as before (`state=reset`). Formats 5 and older are refused as
before. Rolling upgrades begin at the build before round five; the boundaries before
it (layouts, event-time state) stay lockstep or reset, and no deployment is on those
builds.

**Measured and tested.**

- *Two real builds.* `KafkaServiceProcessTests.The_build_from_before_format_levels…`:
  the build of commit `9de8da9` and this one as processes on one deployment (a table
  loaded from Kafka, two pipelines from a plugin, 3,200 trades, a reference row
  changed at every stage). Older leader and older standby; standby replaced by this
  build; takeover; the older build back as standby, warm — it reads the catalog, the
  table and the state this build writes — and the level still 1 after three more
  probes; takeover *back* to the older build, which leads on state this build wrote;
  takeover again; the second node replaced; the level raised to 2 within the leader's
  next probe; a takeover at level 2. Sinks equal to the model at each of the eight
  stages, four takeovers. Then the hazard, on the build that has no levels: the older
  build started after the raise calls itself warm, takes over, and leads with both
  pipelines `NeedsReset` (*format=8 … this circuit has format=7*); this build takes
  over again, goes on at level 2 although the older leader announced none, and the
  sinks equal the model. Four whole runs, all passed
  (`docs/design/runs/2026-11-16-b6-rolling-upgrade.measurements.log`). The test skips
  without `INCREMENTAL_OLDER_BUILD`; CI's `connector-stack` builds the older node from
  history and sets it.
- *Two nodes of this build at different levels*, in one process on Kafka
  (`RollingUpgradeTests`, for 1 → 2 and for 2 → proving): the same upgrade with a
  table, a join and a windowed pipeline whose event time is declared on a
  three-partition input; windows close while it goes on; at every stage the three
  sinks equal the model, at the end every window's final is there once, the last
  leader counted no late record, and the logs hold the higher level only after the raise (manifest and marker
  keys; the proving byte in the state topic and the table's rows topic). An older
  node started at the end says `format-level` and its forced takeover is refused with
  the leader undisturbed. A second test: a lone leader writes its own level; a leader
  told to hold does not raise for a standby that reads more.
- *The logs alone* (`KafkaFormatLevelTests`, 3): a writer goes on at the announced
  level; a raise outlives its writer; a node below the level is refused before the
  fence while the writer it would have replaced still commits; a change of the format
  byte is read by a node of the level and by none below, on the state log and a table
  log; a standby below the level stands by through five takeover timeouts without
  trying.
- *The engine* (`FormatLevelTests`, 8): a pair of storages through the same upgrade
  on the in-memory changelog — including a takeover back to the lower level before
  the raise — with a partitioned event-time circuit, five seeded workloads over the
  pairs 1 → 2, 2 → proving and 1 → proving, outputs integrated as a transactional
  sink would hold them and equal to an uninterrupted run's; what level 1 writes for a
  partitioned source; the manifest's readers by level.
- Mutations, each against the engine tests: the marker written at the build's level
  whatever the deployment's (3 fail), a new writer starting at the base level instead
  of the level it has read (5), a follower not looking at a commit record's manifest
  (4); and the standby's guard removed against the Kafka test (1).

**Left.**

- *The build before this one cannot say what it reads.* After the raise it must not be
  started: it calls itself a warm standby and, leading, stops every pipeline as
  `NeedsReset`. Shown above to lose nothing, but nothing runs until the newer build
  leads again, and a `reset` there would discard state. Every later build refuses by
  itself. The raise follows the second node's upgrade by about a second unless
  `HoldFormatLevel` is set, so the window in which the upgrade can be taken back is
  only as long as the operator makes it.
- *No peer, no question.* A leader with no probe address raises at once. Two nodes
  without probes and with different builds are therefore protected only by
  `HoldFormatLevel`; the guide says so. A raise by an operator's command instead of a
  setting was not built.
- *One refusal can still come after the fence.* A node that read the leader topic's
  tail, found a level it reads, and is then overtaken by a raise before its
  `InitTransactions` lands learns of the level only with the term — after it has
  fenced the leader. It then fails its takeover and exits 5; a node of the level takes
  over. It needs a raise and a takeover by a lower node within the same moment, and
  the leader raises only for a standby that reads the level, so the lower node would
  have to be a third one. Not built against, not tested.
- *The leader asks one standby.* The service has two nodes. A third process that reads
  the logs — nothing in the service is one — would not be asked.
- *What a level does not name.* Behaviour that differs between builds with no format
  changing: for the length of an upgrade whichever build leads decides. The
  event-time step is the example — a returning key, a late version, the bound on a
  time in the future are judged by the leader's build — and the two-build test above
  uses pipelines without event time, so it does not exercise it. The leader topic's
  records and the probe's line are outside the levels too: they are how the level
  travels, and may only gain members a reader can skip. The `files` connector's
  formats and the blob topic stay as CLAUDE.md has them.
- *A level-1 checkpoint has no partition times*, so a node that leads at level 1 pays
  K1's restart cost in full at each takeover (every partition holds the watermark
  until it has brought a record or is idle), for as long as an upgrade is held.
- *Found on the way, not B6's:* a burst that arrives after an input's partitions have
  all gone idle is read a partition at a time, and the first partition read moves the
  watermark before the others are heard — the idle ones hold nothing back. With 5 s of
  event time produced at once over three partitions and a lateness of 2 s, 19 of 50
  records were late (the first build of `RollingUpgradeTests`' feed, twice in two
  runs; the feed now stays inside the lateness). It is K1's idle rule working as
  written, and the same as before K1 — but K1's "a backlog is not late" does not cover
  a backlog that begins after five quiet seconds.
- *Not run:* the scale rig and the torture rig; the three-broker profile; the two
  admin UI tests (no Chrome on this machine; `app.js` gained one line of text, the
  reason `format-level`). CI has not run since the account's billing stopped it, so
  the workflow's new step (build the older node from history) has never run.

#### Paced bulk loads and a limit on a pipeline's state (B2, B3)

**B2, what a large load stops.** The scale rig gained `--load-only` (one node, the
table loaded through its writer, nothing after) and `--no-pacing`, and its nodes log
the collector's own account: each gen2 with the runtime's reason and kind, each pause
over 200 ms with its generation and the memory load. On this round's machine (four
cores, its memory moving between 15.6 and 11.6 GB while the runs were made, about
3 GB of it the connector stack) that account said two things about the stops
(`runs/2026-11-16-b2-exploration.txt`):

- *Young collections that copy gigabytes.* Under eight server heaps the young
  generations fill with rows that survive, and one collection copies them into the
  old generation: a gen1 holding 3.4 GB took 2.4 s at five million rows, one took
  1.4 s at four. That is the load's to fix. Waiting between commits does nothing (the
  collector runs on bytes allocated, not on time: a load at half speed stopped as
  long); asking for a young collection every step of allocation bounds each copy.
- *Full blocking collections under memory pressure.* The multi-second stops at five
  million were compacting gen2s the collector chose because the memory load stood at
  its high-load threshold (reason AllocSmall, blocking, at 11,321 of 11,321 MB). The
  table is 6.5 GB live; on a machine of 12 GB with 3 GB taken there is no pace at
  which it fits beneath the threshold, and a collection asked for there is the
  collector's cue for the full one (an induced gen1 became a 3.5 s gen2).

**Built.** `SharedTableOptions.LoadPacing` (`Tables/TableLoadPacing.cs`), on by
default, null for off. Once a run of commits — commits with no gap of more than
`QuietGap` (2 s) between them — has written `LargeAfterChanges` (100,000) changes,
the writer calls `GC.Collect(1, Forced, blocking, not compacting)` after a commit
whenever the process has allocated `StepBytes` (128 MB) since the last; above
`MemoryLoadCeiling` (85 %) of the collector's high-load threshold it asks for nothing
and counts the step as skipped. It runs inside the commit, after the version is
published, under the commit gate, so it covers `CommitAsync`, `ReplaceAllAsync` and a
derived table's writes alike, and never delays what readers see. A follower (a
standby applying the log) is not paced: its stops move no leadership. What it did is
`TableWriter.Pacing` (steps, skipped, time in them), which the rig reports.
`TableLoadPacingTests` (5) hold when it acts and when it does not.

**Measured** (`runs/2026-11-16-b2-paced-load.tsv`, its collector log beside it):
before and after alternated, `--load-only`, the leader's longest stall during the load.

| rows | before (no pacing) | after | load before → after |
|---|---|---|---|
| 3,000,000 (three runs each) | 554 / 742 / 810 ms | 190 / 192 / 200 ms | 36.5–36.6 s → 38.2–39.1 s |
| 4,000,000 (two each) | 1,533 / 1,891 ms | 308 / 775 ms | 47.9–49.8 s → 51.5–51.6 s |
| 5,000,000 (two each) | 3,061 / 3,502 ms | 3,299 / 3,759 ms | 63.8–64.6 s → 66.1–66.7 s |

Three million is the largest this machine held without the collector's high-load
mode: there the stops are young collections, and pacing takes the longest from about
0.7 s to 0.2 s for 5 % on the load's time (73 steps, about 100 ms each). At four
million the machine had shrunk to 12.3–13.5 GB: one run reached the ceiling for 18
of its steps and kept a 0.78 s stop, the other 0.31 s. At five million every run,
with or without pacing, had a full blocking collection of 3.1 to 3.8 s when the
memory load reached the threshold (45 steps skipped in each paced run) — past
`TakeoverAfter`. So the plan's "done when" — five million rows with the leader's
longest stop well under the timeout — is **not shown**: this machine cannot hold five
million rows without memory pressure, and under memory pressure pacing changes
nothing. `TakeoverAfter` was not raised (decision 11's spirit; the plan said so).

**Left (B2).**

- *Five million rows on a machine that holds them.* The S7 machine (20 cores, enough
  memory for two such nodes) stopped for 1.7 to 2.2 s in a full blocking collection
  that was not memory pressure; what reason the collector gave there was not logged,
  and whether pacing takes it away is not known. The run to make: `--load-only --rows
  5000000` with and without `--no-pacing` where 9 GB are free for the node, and the
  `gc` lines of its stderr for the reason of any blocking gen2.
- *Near the threshold a node stops for seconds whatever the pace.* A container sized
  at 1.35 times what the node holds (the guide's arithmetic) has its threshold at
  90 % of the limit; a load that takes it there gets a full blocking collection,
  measured here at 3.1–3.8 s for 6.5 GB on four cores — 0.1 to 0.15 s a gigabyte on
  the S7 machine. The guide says so under *The collector's settings*: it is sizing.
- *Large-object garbage from the table log.* Each version's change records are
  copied into arrays of up to `MaxRecordBytes` (~900 KB) and the chunk buffer grows
  afresh per version: about 84 MB/s of large-object garbage during a load, collected
  only by gen2s, and part of what brings the memory load to the threshold. Changing
  the chunk size changes no format (the reader takes any number of chunks) but was
  not done or measured.
- *The steps are a collector's knob in a library.* They are the only collections the
  engine itself asks for; `MemoryKeeper` is the service's. A host that wants none sets
  `LoadPacing = null` on its tables' options.

**B3, a limit on a pipeline's state.** `PipelineCircuitOptions.MaxStateEntries`
(`circuit.maxStateEntries`, null for none, below 1 refused at the dry run). The
supervisor's sampler already reads every pipeline's `EstimateSize()` once a second for
the status and the growth flag; on the leader, a running generation whose reading is
above its spec's limit is faulted there (`Generation.Faulted` with
`StateLimitExceededException`), its watcher tears it down, and `HandleFault` makes the
pipeline `Failed` with the exception's text as the reason (*holds N state entries,
more than its limit of M (circuit.maxStateEntries) …*) — not `Backoff`, because a
restart resumes the same state and would pass at once. A replace with a higher limit
and `state=keep` runs it on its state. Nothing else is touched: the growth flag is
unchanged and still flags; health, tables and other pipelines are not involved. A
standby judges nothing. The deploy form's *Running it* has the field; the harness has
`.MaxStateEntries(n)`, judged on `StateSize` after each step, throwing the same
exception from that step and every later one. `StateLimitTests` (4): three pipelines
of one definition on one node, one with a limit it passes — it is `Failed` with the
reason and one fault, not retried two seconds on — and its two neighbours (no limit,
a limit far off) run on, unrestarted, without a fault, delivering every trade sent
after; a raised limit with `state=keep` runs it again; a limit of 0 refused at the
dry run; the harness.

**Left (B3).** The limit is on entries, not bytes, and judged once a second, so a
pipeline can pass it by a second's growth before it is stopped. The admin UI tests
were not run (no Chrome here); `forms.js` gained one field.
