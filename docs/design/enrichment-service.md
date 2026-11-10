# Design: the enrichment service

> **Status:** proposed design, 2026-11-10. Nothing here is built. Build steps S1 to S4 (see
> *Build order*) record what they change under *As built*.
> Sits on [shared-tables.md](shared-tables.md) and
> [durability-and-standby.md](durability-and-standby.md); it is the "step 7 (hosting)" both
> leave open, and the "pipeline registry" the second one declares out of scope.
> Requirements marked **Settled** were given. Choices marked **Default** were made here and
> are listed again under *Open for James* with the alternative and its cost.

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
assemblies. Sources and sinks are Kafka, Debezium CDC, files and custom adapters written
elsewhere. Several processes exist for availability, not scale: one active, one warm
standby, failover in seconds. One circuit per pipeline; tables held outside circuits.
Delivery is at-least-once with keyed upserts. The tenant-facing product is another
repository; this one provides the packages it builds on.

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

## Packages

| Project | Targets | Depends on | Holds |
|---|---|---|---|
| `src/Incremental.Service` | net8.0; net10.0 | Incremental, Incremental.Hosting, Incremental.Kafka | everything below except HTTP |
| `src/Incremental.Service.Admin` | net8.0; net10.0 | Incremental.Service, `FrameworkReference Microsoft.AspNetCore.App` | JSON API, embedded UI |
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
    INodeLeadership Leadership { get; }
    BackendInfo Describe();                              // kind, settings, counters — for the UI
}

public interface INodeLeadership
{
    LeadershipStatus Status { get; }                     // the Incremental.Kafka record, reused
    event Action<LeadershipStatus>? Changed;
    Task WhenFinished { get; }                           // Fenced or Resigned
    long Term { get; }
    void Track<TKey, TRow>(SharedTable<TKey, TRow> table, ITableLog<TKey, TRow> log)
        where TKey : notnull where TRow : class;         // promoted at takeover, in track order
    void Untrack(string tableName);
    void Track(ChangelogStorage storage);
    Task StartAsync(LeadershipCallbacks callbacks, CancellationToken ct);
    Task TakeOverAsync(CancellationToken ct);
    Task StepDownAsync(CancellationToken ct);
    ValueTask<FollowerLag?> MeasureLagAsync(CancellationToken ct);
    IUpsertSink<TKey, TRow> Leased<TKey, TRow>(IUpsertSink<TKey, TRow> sink) where TKey : notnull;
    IAppendSink<T> Leased<T>(IAppendSink<T> sink);
}

public sealed record LeadershipCallbacks(
    Func<CancellationToken, Task> PrepareAsync,          // writer acquired; nothing promoted yet
    Func<long, CancellationToken, Task> StartLeadingAsync, // tables and storage promoted
    Func<CancellationToken, Task> StopLeadingAsync);     // step down: before the resignation
```

Both implementations run a takeover in the order `KafkaLeadership` already tests
(`KafkaLeadership.cs`, the takeover; durability-and-standby.md, *As built (step 5)*):
acquire the writer → `PrepareAsync` → promote tracked tables in track order → promote
tracked storages → `StartLeadingAsync(term)`. A failure anywhere ends the node `Fenced`.

**`InMemoryServiceBackend`.** `InMemoryTableLog` per table and one `InMemoryStateChangelog`,
held in an `InMemoryServiceCluster` that several backends in one process may share
(`cluster.Node("a")`, `cluster.Node("b")`), so the standby path and the takeover order are
testable with no broker. The first node started leads at once. Another leads on
`TakeOverAsync` (which calls `TakeOver()` on every log connection and acquires the
changelog writer, fencing the old node exactly as the core's failover tests do) or when
the leader resigns. There are no heartbeats and no automatic election; `Leased` passes
sinks through. A lone node is the default single-process mode.

**`KafkaServiceBackend`.** One `KafkaLog`, one `KafkaLeadership` built in `StartAsync`
from the callbacks, a `LeadershipProbe` listener when `ProbeListen` is set.

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

## Catalog

The catalog is the durable record of what is deployed. A standby or a restarted node
reconstructs the service from it and from nothing else.

> **Default (1): the catalog is a shared table.** `SharedTable<string, CatalogEntry>` named
> `svc.catalog`, opened on the backend's table log like any other table. On Kafka that is
> `{p}.table.svc.catalog.rows` (compacted) and `.changes`, written through the log's one
> transactional producer, so fencing covers every catalog write; a standby follows it with
> `SharedTable.FollowAsync`; a takeover promotes it first; its lag is in
> `MeasureLagAsync` with the others. It needs no new code in `Incremental.Kafka`. The
> in-memory backend gets the same behaviour, fencing included, from `InMemoryTableLog`.
> Table names starting `svc.` are reserved.

```csharp
public sealed record CatalogEntry(string Key, string Value);     // Value: JSON, or base64 for a blob
```

| Key | Value |
|---|---|
| `table/{name}` | `TableSpec`: definition type, contract, loader kind and config, options |
| `plugin/{name}/{version}` | `PluginManifest`: kind, files with hashes, definitions found, data-shape fingerprint |
| `blob/{sha256}/{n}` | one 64 KB chunk of a plugin file, base64 |
| `pipeline/{id}` | `PipelineSpec` (below), with `generation` and `desired` |
| `node/leader` | node id, term, admin URL — so a standby's UI can link to the leader |
| `node/handover` | the last "cannot lead" step-down: term, node, reason |

Rules:

- **Only the leader writes**, one admin action per `CommitAsync`, so an action is one
  atomic version. A write that fails as fenced ends the node like any other.
- **Durable before it acts.** The leader commits the catalog change before it starts
  anything the change describes; durability-and-standby.md requires a definition to be
  durable before the pipeline's first commit. Two exceptions run the other way round and
  say so where they appear: a table's log is created before its entry is written, and a
  plugin is validated before it is stored.
- **Readers poll.** A node compares `catalog.LatestVersion` with the version it last
  applied every `CatalogPollInterval` (100 ms), takes a `Snapshot()`, scans it, and
  reconciles. The scan needs core change C4.
- **Chunk size.** `KafkaTableLog` cuts a version into records at `MaxRecordBytes` (900,000)
  *after* the change that crosses it (`KafkaTableLog.cs`, the chunk loop), and the broker's
  default limit is 1,048,588. A 64 KB chunk is 87 KB of base64, which keeps a record under
  the limit. Blobs are keyed by content hash, so they are written once and never replaced.
- **History.** No circuit reads the catalog, so the leader trims its log to the latest
  version after each commit.

> **Default (2): plugin binaries live in the catalog.** A standby then needs nothing but
> Kafka to build what the leader runs. Each node materialises the files it needs into
> `CacheDirectory/plugins/{name}/{version}/` and checks hashes before loading.

**In-memory backend.** The catalog is process memory. With
`InMemoryBackendOptions.CatalogDirectory` set, the leader writes `catalog.json` (every
entry but blobs) and `blobs/{sha256}` after each commit, with write-then-rename, and loads
them on its first lead. Tables and state are still new at every start, so this restores
*what is deployed*, not its data: pipelines start empty and replay their sources.

**Seeds.** `AddTable` and `AddPipeline` on the DI builder are written to the catalog by
the first leader if their key is absent. After that the catalog wins.

## Table registry

```csharp
// In a contract assembly (see Plugins).
public abstract class TableDefinition<TKey, TRow> : ITableDefinition
    where TKey : notnull where TRow : class
{
    public abstract string Name { get; }                 // default table name
    public abstract TKey KeyOf(TRow row);
    protected virtual void Configure(TableBuilder<TKey, TRow> table) { }
}
public sealed class TableBuilder<TKey, TRow>
{
    public TableBuilder<TKey, TRow> Index<TIndexKey>(string name, Func<TRow, TIndexKey?> selector) where TIndexKey : notnull;
    public SharedTableOptions Options { get; set; }
}

public interface ITableRegistry
{
    IReadOnlyList<TableInfo> Tables { get; }
    SharedTable<TKey, TRow> Get<TKey, TRow>(string name) where TKey : notnull where TRow : class;
    TableStats Stats(string name);
    Task DeclareAsync(TableSpec spec, CancellationToken ct);      // leader
    Task RemoveAsync(string name, CancellationToken ct);          // leader; refused while a pipeline names it
    Task RestartLoaderAsync(string name, CancellationToken ct);   // leader
}
```

`TableDefinition`'s own non-virtual members make the generic calls (`OpenAsync`,
`FollowAsync`, `AddIndex`, `Track`, `CommittedVersion`), so the registry holds tables as
`ITableDefinition`-built handles and uses no reflection beyond constructing the
definition. `Get<TKey, TRow>` throws with both type names when a caller's types are not the
table's — which is what a row type loaded twice looks like.

**Lifecycle of one table.**

| Role | What the registry does |
|---|---|
| Standby, entry appears | `OpenTableLogAsync` (waits until the leader has created the log) → `SharedTable.FollowAsync` → declare indexes → `leadership.Track` |
| Takeover | the leadership promotes it (tables before storage, catalog first) |
| Leader, declared at runtime | open the log *as the writer* (this mints the epoch) → `SharedTable.OpenAsync` → declare indexes → commit `table/{name}` → start the loader |
| Leader, after takeover | start the loader, in `StartLeadingAsync`, after the circuits |
| Removed | stop the loader → dispose the table → `Untrack` → delete the entry |

Indexes are declared before any loader runs, as shared-tables.md asks; an index added to
a definition later is built by one scan at the next process start. Removing a table leaves
its topics alone, so declaring the name again brings the old rows and epoch back
(*Open for James*, 9).

**Loaders.** A table has at most one writer: a loader, or one pipeline's `ToTable`, or
nobody (the host writes `registry.Get(...).Writer` itself). `TableSpec.Loader` is a kind
and a JSON config:

| Kind | What it is |
|---|---|
| `kafka` | `KafkaSource.StartTableLoaderAsync` with `KafkaDeserializers.Keyed` (JSON) |
| `debezium` | the same with `Debezium.Changes<TKey, TRow>()` |
| `file` | a snapshot loader: reads one object through `IObjectStore`, `ReplaceAllAsync`, again when the object's identity (length and modified time, or ETag) changes; polls every `pollInterval` |
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
    public ILogger Logger { get; }
    public void Report(TableSourceState state, string? position = null, long? lag = null);
}
public enum TableSourceState { Starting, Snapshotting, Streaming, Idle, Faulted }
public interface ITableSourceFactory                    // registered in DI under a kind name
{
    ITableSource<TKey, TRow> Create<TKey, TRow>(JsonElement config) where TKey : notnull where TRow : class;
}
public interface IObjectStore                           // local files built in; S3 is the host's
{
    ValueTask<ObjectInfo?> StatAsync(string path, CancellationToken ct);
    ValueTask<Stream> OpenReadAsync(string path, CancellationToken ct);
    IAsyncEnumerable<ObjectInfo> ListAsync(string prefix, CancellationToken ct);
}
```

This is the snapshot + delta contract of durability-and-standby.md, *Source resumption*,
unchanged: a gRPC adapter with stable sequence numbers stamps `(partition, sequence)` and
resumes from `Table.CommittedOffsets`; one without calls `ReplaceAllAsync` on every start
and then applies deltas. The `file` loader reads JSON lines or one JSON array.

A loader runs on the leader only. When it faults it is restarted with the pipelines'
backoff policy and the table shows `Faulted` meanwhile; readers keep the last version. A
table *writer* that faults is different — "any failed append faults the writer for good"
(shared-tables.md) — and cannot be mended in place, so if the node is not fenced that is a
"cannot lead" condition (see *Node*).

**Derived tables** are declared like any other, with no loader. A pipeline that calls
`ToTable` is the writer; the core refuses a second writer or a cycle at `Build()`
(`Tables/DerivedTable.cs`), and the registry refuses a loader on a table a deployed
pipeline writes.

**Retention.** Every `TrimInterval` (30 s) the leader computes, per table,

```
floor = min over every pipeline in the catalog, in any state, of its committed {table}#t or {table}#w
```

and calls `log.TrimBeforeAsync(floor)`. The numbers come from each pipeline's
`ChangelogStorageProvider.CommittedOffsets` (core change C2), so a stopped pipeline with
no circuit counts. A pipeline that has never committed holds nothing: it starts at the
current version. `MaxRetainedVersions` per table (default 100,000) trims regardless; the
pipeline cut loose refuses its next start and shows `NeedsReset`. The table's stats name
the pipeline holding the floor.

**Stats**, sampled every 5 s and read by the admin API:

```csharp
public sealed record TableStats(
    string Name, string KeyType, string RowType, string Epoch, bool Following,
    long Rows, long Version, long OldestRetainedVersion, double VersionsPerSecond,
    IReadOnlyList<string> Indexes, string Writer,               // "loader:kafka", "pipeline:x", "none"
    LoaderStats? Loader,                                         // state, positions, lag, records read, fault
    long? StandbyLagVersions,                                    // from FollowerLag.TableVersions
    IReadOnlyList<TableReader> Readers,                          // pipeline, committed version, versions behind
    string? FloorHeldBy);
```

Bytes are not reported: nothing in the core counts them and asking the broker per table
is not cheap.

## Plugins

**Trust.** A plugin is trusted code. It runs in the service's process with the service's
permissions; an `AssemblyLoadContext` isolates names, not behaviour. A delegate that loops
wedges its circuit until the process restarts. Whoever can deploy a plugin can run
anything, so uploading one over HTTP is off unless enabled (see *Admin*).

**A plugin** is a directory or zip: the main assembly, its `.deps.json`, its private
dependencies. Its **version** is the first 16 hex digits of a SHA-256 over the sorted
`(path, file hash)` list. The same bytes are the same version on every node.

**Two kinds**, told apart by an assembly attribute:

- A **contract assembly** (`[assembly: IncrementalContract]`) holds `TableDefinition`s and
  the row and key types of shared tables. It is loaded **once per process**, into one
  non-collectible context, and never unloaded.
- A **pipeline assembly** holds `IPipelineDefinition`s. Each version is loaded into its
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
logs, so only additive changes survive this; the upload is refused if a declared table's
row shape changes unless the caller passes `acceptSchemaChange=true`. Anything else is a
new table under a new name.

**Discovery and import.** On the leader: unpack to a temporary directory, hash, load into
a throwaway context, find the definitions and their settings types, compute the
**data-shape fingerprint** (a hash over the public fields and properties of every
non-closure type the assembly defines), unload, then commit the manifest and blobs in one
catalog version. `PluginDirectory` is an import source only: at first lead and on
`POST /plugins/rescan` every package in it that the catalog lacks is imported. Nodes load
from the catalog, never from each other's disks.

**Built-in plugins.** `AddContracts(assembly)` and `AddPlugin(assembly)` register
assemblies compiled into the host. They live in the default context, their version is the
module's MVID, and they are never unloaded. Tests and the simple example use these.

**Unload.** A version is referenced by each pipeline generation built from it, on a leader
or a standby. When the count reaches zero the supervisor has already released the
pipeline's typed stores (core change C1) and the context is unloaded. Unloading is a
request: any static that holds one of the plugin's types keeps the context alive. The
plugin page lists contexts still alive after unload; a leaked one costs memory, not
correctness. Catalog entries are deleted only by `DELETE /plugins/{name}/{version}`,
refused while a pipeline spec names the version.

**State when a plugin's types change.** A pipeline's state is JSON keyed by store name,
and the state manifest covers store names, not types (`Circuit.cs`,
`BuildStateManifest`). JSON is forgiving: a renamed property reads back as a default, and
then retractions stop matching. So the service does not rely on the manifest alone. A
replace compares the old and new versions' data-shape fingerprints; if they differ it is
refused unless the caller chooses `keepState` (they assert the change is compatible) or
`reset`. See *Replace*.

## Pipelines

### The definition contract

```csharp
public interface IPipelineDefinition
{
    void Build(PipelineBuilder pipeline);
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
    public TSettings Settings<TSettings>() where TSettings : class, new();   // bound from spec.settings

    public TableRef<TKey, TRow> Table<TKey, TRow>(string name) where TKey : notnull where TRow : class;
    public InputStream<T> Input<T>(string slot) where T : notnull;
    public KeyedInputStream<TKey, TValue> KeyedInput<TKey, TValue>(string slot, Func<TValue, TKey> key)
        where TKey : notnull where TValue : notnull;

    public void UpsertSink<TKey, TRow>(string slot, ZStream<KeyedValue<TKey, TRow>> rows) where TKey : notnull where TRow : notnull;
    public void UpsertSink<T, TKey>(string slot, ZStream<T> rows, Func<T, TKey> key) where T : notnull where TKey : notnull;
    public void AppendSink<T>(string slot, ZStream<T> rows) where T : notnull;
    public void ToTable<TKey, TRow>(string table, ZStream<TRow> rows) where TKey : notnull where TRow : class;
}
```

```csharp
[Pipeline("trade-enrichment")]
public sealed class TradeEnrichment : IPipelineDefinition
{
    public sealed class Options { public decimal MinNotional { get; set; } }

    public void Build(PipelineBuilder p)
    {
        var o = p.Settings<Options>();
        var instruments = p.Table<int, Instrument>("instruments");
        var enriched = p.Input<Trade>("trades")
            .Where(t => t.Notional >= o.MinNotional)                               // filter
            .GroupBy(t => t.InstrumentId)
            .Join(instruments, (_, t, i) => new EnrichedTrade(t.Id, i.Symbol, t.Notional * i.Multiplier)); // join, computed column
        p.UpsertSink("enriched", enriched, e => e.TradeId);
    }
}
```

`Input` and `Table` return the core's own types, so lookups, aggregates, event time and
windows are the core API with nothing in between. Each sink method calls
`ToAcknowledgedOutput()` and remembers a typed closure that will later start the delivery;
each input remembers one that will start the source. No reflection, and the type
parameters never leave the plugin's code.

Running `Build` against a `PipelineBuilder` and *not* calling `Circuit.Build()` yields the
pipeline's **shape** — slots with their types, tables read and written, the settings
type. That dry run is the validation: every slot bound, every table declared with those
types, the settings JSON binds. It has no side effects, because a table is claimed only
at `Circuit.Build()`.

A declarative spec later is one more `IPipelineDefinition`, built in, whose settings are
the spec. Nothing here stands in its way.

### The spec

```json
{ "id": "trades-eu", "plugin": "acme.pipelines", "version": "9f2c41d07ab35e10", "definition": "trade-enrichment",
  "desired": "Running", "generation": 3,
  "settings": { "minNotional": 1000 },
  "circuit":  { "workers": 1, "checkpointInterval": 1, "checkpointMaxDelay": "00:00:01", "errorPolicy": "DeadLetter" },
  "inputs":   { "trades":   { "connector": "kafka", "topic": "trades", "startFrom": "earliest" } },
  "sinks":    { "enriched": { "connector": "kafka", "topic": "trades.enriched" } },
  "restart":  { "maxRestarts": 5, "within": "00:10:00", "initialBackoff": "00:00:01", "maxBackoff": "00:01:00" } }
```

The id is `[a-z0-9][a-z0-9._-]{0,62}`. It is the `ChangelogStorage.Pipeline(id)` key and
the circuit's name, hence the `circuit` tag on every metric. `circuit` binds the scalar
members of `CircuitOptions`; storage is always the changelog provider.

**Connectors** are registered in DI under a kind name and given the slot's JSON:

```csharp
public interface ISourceConnector
{
    Task<IRunningSource> StartAsync<T>(SourceContext<T> context, CancellationToken ct) where T : notnull;
    Task<IRunningSource> StartAsync<TKey, TValue>(KeyedSourceContext<TKey, TValue> context, CancellationToken ct)
        where TKey : notnull where TValue : notnull;
}
public interface IRunningSource : IAsyncDisposable
{
    Task Completion { get; }                                  // faults when the source fails
    SourceStats Stats { get; }                                // records read, positions, lag, paused
}
public interface ISinkConnector
{
    IUpsertSink<TKey, TRow> CreateUpsert<TKey, TRow>(SinkContext context) where TKey : notnull;
    IAppendSink<T> CreateAppend<T>(SinkContext context);
}
```

A context carries the started circuit, the input stream or handle, the config, the
pipeline id, a logger and (for sinks) `Func<long> Term`. Built in: `kafka` (source for
both input kinds with a JSON or Debezium decoder; `KafkaUpsertSink`/`KafkaAppendSink`,
with `"producer": "log"` selecting the transactional producer), `file` (a directory as a
log for inputs, per durability-and-standby.md: partition 0, offset = file ordinal × 2³² +
record index, where the ordinal is the trailing number in the file name; an append sink
writing one `{step}.jsonl` per batch by write-then-rename, so a redelivered step
overwrites itself), and `memory` (in-process handles, for tests and the generator). Every
sink the supervisor creates is wrapped in `leadership.Leased`.

### The supervisor

```csharp
public interface IPipelineSupervisor
{
    IReadOnlyList<PipelineStatus> Pipelines { get; }
    PipelineStatus Get(string id);
    Task<DeployResult> DeployAsync(PipelineSpec spec, DeployOptions options, CancellationToken ct);  // new, or replace
    Task StartAsync(string id, CancellationToken ct);       // desired = Running
    Task StopAsync(string id, CancellationToken ct);        // desired = Stopped
    Task RestartAsync(string id, CancellationToken ct);     // also clears Failed
    Task ResetAsync(string id, CancellationToken ct);       // new incarnation, then start
    Task RemoveAsync(string id, CancellationToken ct);
    IReadOnlyList<DeadLetterRecord> DeadLetters(string id, long afterSequence, int limit);
}
public sealed record DeployOptions(bool KeepState = false, bool Reset = false);
public enum PipelineState { Built, Starting, Running, Stopping, Stopped, Backoff, Failed, NeedsReset, Unbuilt }
```

`Built` is a standby's state: circuit constructed, not started. `Unbuilt` means the node
could not build it (plugin missing, definition throws); on a standby its state is held as
bytes and it is built at takeover.

Every operation on one pipeline runs on that pipeline's own queue, one at a time.
Pipelines do not wait for each other. Role changes and table declarations take a
node-wide gate exclusively; pipeline operations take it shared.

**A generation** is one circuit and what hangs off it. The orders are the ones the two
design docs fix:

```
start   tables are open and promoted (the registry's job, earlier)
        build:   new CircuitBuilder(options with Storage = storage.Pipeline(id)); definition.Build; Circuit.Build()
        circuit.Start()                         — refusal → NeedsReset (core change C5)
        deliveries: handle.DeliverToAsync(leased sink) per sink slot; the dead-letter reader
        sources:    connector.StartAsync per input slot   — after Start(): they read CommittedOffsets

stop    dispose sources                          — before the circuit
        circuit.DisposeAsync()                   — drains while deliveries still read; final commit waits for acks
          … up to StopTimeout (30 s); then Abandon() every acknowledged output: the commit is skipped
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

`Failed` is this node's knowledge, not the catalog's: a new leader tries again.

**Dead letters.** With `errorPolicy: DeadLetter` anywhere in the circuit the dead-letter
output must be read or the circuit stalls. The supervisor always reads it, into a ring of
the last `DeadLetterBuffer` (256) records per pipeline — operator, weight, step, exception
type and message, the record as JSON cut at 4 KB — and a counter. A `deadLetters` sink slot
in the spec also appends them to a connector, best effort: the core does not acknowledge
that output. The ring is per node and is lost on failover.

**Replace** (`DeployAsync` for an id that exists, with another version, definition or
settings):

1. Compare the data-shape fingerprints of the two plugin versions. If they differ and
   neither `KeepState` nor `Reset` is set: refuse, `shape-changed`, nothing touched.
2. Validate the new spec by dry run. Failure: refuse, nothing touched.
3. Stop the running generation. `provider.ReleaseStores()` (C1): the stores go back to
   bytes, because the new version's types are different `Type` objects even when their
   source is identical, and the provider refuses a store "already created with different
   key/value types" (`ChangelogStorageProvider.cs`, `GetOrCreate`).
4. With `Reset`: `provider.StartNewIncarnationAsync()`.
5. Commit the spec with `generation + 1`.
6. Build and start. If the build cannot read the state, or `Start()` refuses, and `Reset`
   was not set: release, rebuild the *previous* version (the bytes are untouched), commit
   the previous spec under `generation + 2`, and report `state-incompatible`. The caller
   decides and asks again with `Reset`.

Other pipelines run throughout. The old plugin version is unloaded when nothing uses it.

**Reset** is a new incarnation: state gone, input offsets gone. Sources start again from
their `startFrom`; upsert sinks converge, append sinks receive the history again; a
derived table loses every row the new circuit does not assert in its first step.

**Remove.** Stop; `StartNewIncarnationAsync()` to tombstone the state (one empty commit
record of about 100 bytes stays in the changelog — deleting a pipeline outright is not
built in the core); delete the catalog entry. Tables it read lose a reader, so the floor
may rise.

**On a standby** the supervisor builds each pipeline whose `desired` is `Running` and
does not start it; the storage follower fills its stores. When the catalog shows a new
generation it disposes the built circuit, releases the stores and builds again. The
leader's new records can reach the standby's state follower before its catalog poll sees
the new generation; C1's second half is what makes that harmless.

## Node

```csharp
public sealed class ServiceNode : IHostedService, IAsyncDisposable
{
    public NodeStatus Status { get; }                        // LeadershipStatus + catalog version + counts by pipeline state
    public event Action<ServiceEvent>? Events;               // role, pipeline, table, catalog — feeds SSE
    public ITableRegistry Tables { get; }
    public IPipelineSupervisor Pipelines { get; }
    public IPluginCatalog Plugins { get; }
    public Task TakeOverAsync(CancellationToken ct);
    public Task StepDownAsync(CancellationToken ct);
}
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
materialise plugins, build pipelines. It holds what the takeover needs: every table at
the log's head, every pipeline's state, every circuit built.

**Takeover.**

| # | Who | What |
|---|---|---|
| 1 | leadership | acquire the writer: fences the old leader, commits the term |
| 2 | `PrepareAsync` | await the catalog open; `catalog.PromoteAsync()` (reads to the log's end); reconcile — tables the catalog names that are not open yet are opened as the writer, and tracked |
| 3 | leadership | promote tracked tables (the catalog is first and already done), then the storage |
| 4 | `StartLeadingAsync` | write `node/leader`; apply seeds; start every pipeline whose `desired` is `Running` (`Start()`, deliveries), in parallel; then sources and table loaders, in the background; start the trim loop; import `PluginDirectory` |

That is writer, catalog, tables, storage, circuits, sources. A pipeline that fails to
start becomes `Backoff`, `Failed` or `NeedsReset`; only a failure of the node itself
(catalog, table promotion, storage promotion) throws, and that ends the node `Fenced`.

**Fenced and Resigned are final.** On `leadership.WhenFinished` the node abandons every
circuit without draining (their commits can only fail), sets `Environment.ExitCode` to 3
(fenced) or 4 (resigned) and calls `IHostApplicationLifetime.StopApplication()`. The
process supervisor restarts it and it comes back as a standby.

**When it cannot lead.** Heartbeats come from the log writer, not the circuits, so a
leader with nothing running still looks healthy to its peer. The node steps down when a
table writer has faulted, or when every pipeline whose `desired` is `Running` is `Failed`
(`LeadFailurePolicy.StepDown`, the default; `Stay` and `Exit` are the others). Before it
does it writes `node/handover`. A leader that takes over and finds that entry from the
previous term does not step down for the same reason during its own term, so one incident
moves leadership once, not back and forth. Any pipeline reaching `Running` clears the
entry.

**Graceful shutdown.** The web server stops first (the generic host stops services in
reverse order, and the server is registered last). Then, on a leader with a backend that
has standbys, `StepDownAsync`: `StopLeadingAsync` stops loaders and sources, stops every
pipeline in stop order and in parallel, and the leadership commits a resignation, so the
standby takes over without waiting out `TakeoverAfter`. Then tables, storage, leadership
and backend are disposed. The host's shutdown timeout bounds all of it; what has not
drained is abandoned and replayed by the successor. A table commit stalled on a lagging
reader is not interruptible (shared-tables.md); the process exits anyway. A standby just
disposes its followers.

**DI.**

```csharp
builder.Services.AddIncrementalService(o =>
    {
        o.PluginDirectory = "plugins"; o.CacheDirectory = "cache";
        o.LeadFailurePolicy = LeadFailurePolicy.StepDown;
    })
    .UseInMemoryBackend()                                   // the default
    // .UseKafkaBackend(k => k.Log = new KafkaLogOptions { … })
    .AddContracts(typeof(Instrument).Assembly)
    .AddPlugin(typeof(TradeEnrichment).Assembly)
    .AddTable<InstrumentsTable>(loader: null)               // seeds
    .AddPipeline(spec)
    .AddTableSource("refdata-grpc", sp => new RefDataSourceFactory(…))
    .AddSourceConnector("my-source", sp => …)
    .AddSinkConnector("my-db", sp => …)
    .AddHealthCheck();
```

`AddIncrementalService` registers `ServiceNode` as a singleton and as a hosted service;
`ITableRegistry`, `IPipelineSupervisor`, `IPluginCatalog` and `ServiceMetrics` resolve
from it. `ServiceOptions` binds from configuration.

## Admin API and UI

```csharp
builder.Services.AddIncrementalServiceAdmin(o => { o.ExposeRowData = true; });
app.MapIncrementalServiceAdmin("/admin");                  // UI at /admin, API at /admin/api
```

**Security posture.**

```csharp
public sealed class ServiceAdminOptions
{
    public string? AuthorizationPolicy { get; set; }           // applied to every endpoint
    public string? MutationAuthorizationPolicy { get; set; }   // mutating endpoints; defaults to the above
    public bool ReadOnly { get; set; }                         // mutating endpoints answer 403
    public bool ExposeRowData { get; set; }                    // table rows, dead-letter records
    public bool AllowPluginUpload { get; set; }
    public int MaxRows { get; set; } = 200;
    public int MaxScannedRows { get; set; } = 100_000;
    public long MaxUploadBytes { get; set; } = 64 << 20;
    public IList<string> AllowedHosts { get; } = [];
}
```

- **Loopback unless told otherwise.** The package cannot choose what Kestrel binds, so it
  enforces the rule itself: with no `AuthorizationPolicy`, a request whose remote address
  is not loopback, or whose `Host` is not a loopback name or in `AllowedHosts`, gets 403.
  Setting a policy lifts both checks and puts `RequireAuthorization(policy)` on the group.
- **Mutations** need `Content-Type: application/json` (a zip for an upload) and the header
  `X-Incremental-Admin: 1`. A browser will not send that cross-site without a preflight
  the API never answers, which is what stops a page on the operator's machine from
  posting to loopback.
- **Row data is off by default** (*Open for James*, 5). Reference rows and dead letters are
  tenant data; everything an operator needs to run the service — states, lags, counts,
  graphs, exception messages — is not. With it off the row endpoints answer 403 and dead
  letters come back without the record.
- **Plugin upload is off by default.** It is remote code execution by design.
- **Mutations go to the leader.** A standby answers 409 with the leader's node id and
  admin URL, except `takeover`.

**Endpoints** (under `/admin/api`; `M` = mutating):

| | Path | |
|---|---|---|
| GET | `/node` | role, term, leader and its term, heartbeat age, lease, lag, last takeover's timings, backend info, catalog version |
| POST M | `/node/takeover`, `/node/stepdown` | on the standby; on the leader |
| GET | `/health` | 200 for a leader with something running or a standby that is following; body says which |
| GET | `/tables`, `/tables/{name}` | `TableStats`; detail adds the spec |
| GET | `/tables/{name}/rows?limit=&skip=&contains=` | up to `MaxRows` rows from one scan of a pinned snapshot that stops after `MaxScannedRows`; `contains` filters on the row's JSON text. Order is the map's: stable only while the table does not change |
| GET | `/tables/{name}/row?key=` | one row by primary key (the key as JSON) |
| POST M | `/tables`, `/tables/{name}/loader/restart` | declare; restart a loader |
| DELETE M | `/tables/{name}` | |
| GET | `/pipelines`, `/pipelines/{id}` | status; detail adds spec, shape, incarnation, commit sequence and term, committed input offsets and table versions, fault, restart history |
| GET | `/pipelines/{id}/graph` | nodes and edges from `Circuit.Describe()` (C3) |
| GET | `/pipelines/{id}/metrics` | the last 60 one-second samples (below) |
| GET | `/pipelines/{id}/dead-letters?after=&limit=` | the ring, newest last, at most `MaxRows` |
| PUT M | `/pipelines/{id}?keepState=&reset=` | deploy or replace; 409 `shape-changed` / `state-incompatible` |
| POST M | `/pipelines/{id}/start`, `/stop`, `/restart`, `/reset` | |
| DELETE M | `/pipelines/{id}` | |
| GET | `/plugins`, `/plugins/{name}` | versions, kind, definitions with settings properties, pipelines using each, load state |
| POST M | `/plugins?acceptSchemaChange=`, `/plugins/rescan` | zip upload (needs `AllowPluginUpload`); import from `PluginDirectory` |
| DELETE M | `/plugins/{name}/{version}` | |
| GET | `/events` | server-sent events: `node`, `pipeline`, `table`, `catalog`, `deadletter` on change; `metrics` once a second |

Every list is bounded by `MaxRows` or by what is deployed. Each SSE connection has a
bounded queue of 64 events that drops the oldest: a slow browser loses ticks, not the
server's memory.

**Where the numbers come from.** `ServiceMetrics` (in `Incremental.Service`, so a host
without the admin package has it too) attaches one `MeterListener` to the `Incremental`
meter (`src/Incremental/Diagnostics/CircuitDiagnostics.cs`) and keeps, per `circuit` tag,
a fixed ring of 60 one-second slots:

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
| node, standby lag | `LeadershipStatus`, `FollowerLag` |
| log writer | `KafkaLog.TransactionsCommitted`, `…Aborted`, `CommitsCompleted`, `RecordsWritten`, `IdleHeartbeats` |

There are no per-operator record counts: the core does not count them, and adding a
counter to every operator's loop is a hot-path cost this design does not take. The graph
is the circuit's static shape with dead-letter counts on the operators that have them.
The service publishes its own meter, `Incremental.Service`, for collectors: pipeline state
and restarts, sink and source records, table rows, version and loader lag, node role,
standby lag, and the log writer's counters above.

**UI.** Hand-written `index.html`, `app.css`, `app.js`, embedded as resources and served
by a small endpoint (no file-provider package, no build step), in the manner of
`examples/Incremental.Showcase/wwwroot/index.html`. Hash-routed views: overview (node
banner, counts, event feed), tables (list, detail with stats, loader, readers, row sample
and key lookup), pipelines (list with sparklines; detail with state, actions, the graph as
inline SVG, metrics, dead letters, spec), plugins (versions, definitions, upload), node
(leadership, lag per partition and table, takeover and step-down). One `EventSource`
drives all of it. On a standby the mutating controls are replaced by a link to the leader.

## Changes this needs elsewhere

Each is the smallest change found; each is to be recorded in the owning design doc when
built.

| | Where | Change | Why |
|---|---|---|---|
| C1 | `ChangelogStorageProvider` | `ReleaseStores()`: with no circuit running, serialize each typed store back to held bytes and forget the store. And: a follower that cannot apply a record to a typed store does the same for that pipeline instead of failing the storage. | A new plugin version has new `Type` objects; contexts cannot unload while the provider holds stores of their types; and today "one bad pipeline blocks a takeover" (durability-and-standby.md, *Other departures and limits*). |
| C2 | `ChangelogStorageProvider` | `IReadOnlyDictionary<string, long> CommittedOffsets`, readable in any role | retention floor for pipelines with no circuit; a standby's pipeline page |
| C3 | `Circuit` | `Describe()`: nodes (id, kind, name, inputs, exchange, state id) from the definitions it already holds | the graph |
| C4 | `TableSnapshot<TKey, TRow>` | `Rows()`: public form of the internal `RowsAt` scan | catalog reconcile, row sampling |
| C5 | `Circuit.Start()` | throw `CircuitStateMismatchException : InvalidOperationException` for the refusals whose remedy is a fresh state store | telling `NeedsReset` from a fault |
| K1 | `KafkaLeadership` | `Untrack(string tableName)` | removing a table without a restart |

## Reference examples

**(a) `examples/Incremental.TradeEnrichment`, moved onto shared tables.** In process, no
docker, no service packages. Instruments, traders, desks and counterparties become
`SharedTable`s; `trader-desks` is a derived table written by a small circuit (`ToTable`);
the trade circuit uses `Join` for the columns that must be corrected retroactively and
`Lookup` for the ones fixed on arrival, chosen at the prompt as columns are today. The
run shows both: a trader moves desk and historical trades re-attribute through the join;
an instrument is renamed and only trades arriving afterwards carry the new name through
the lookup. `--ticks` and `--blotter` keep working for CI. This is shared-tables.md's
step 7 example.

**(b) `examples/Incremental.EnrichmentService`.** Three projects:

- `Contracts` — a contract assembly: `Instrument`, `Counterparty`, `TraderDesk`, their
  `TableDefinition`s (one with an index on ISIN).
- `Pipelines` — a pipeline assembly built into the host's `plugins/` directory:
  `trade-enrichment` (filter, `Join` to instruments, `Lookup` to counterparties, computed
  notional, upsert sink), `desk-exposure` (windowed sum per desk on event time, upsert
  sink), `trader-desks` (`Changes()` of two tables joined and written with `ToTable`).
- the host — `Microsoft.NET.Sdk.Web`, `AddIncrementalService`, the admin UI on
  `http://localhost:5180`, a data generator.

Modes:

- **default**: in-memory backend, tables loaded by the generator through the `memory`
  connector, sinks in memory. No docker.
- **`--kafka`**: the local connector stack. Instruments live in Postgres and arrive
  through a Debezium connector the host registers with `KafkaConnectClient`
  (`debezium` loader); trades come from a topic the generator produces to; sinks are
  Kafka topics. `--node a|b` and `--peer` run two nodes on different ports.
- **`--headless`**: deploys the pipelines, runs the generator for a fixed number of
  records, checks the sinks against a model computed from the same generator, replaces
  one pipeline with a second plugin version, checks again, exits non-zero on any
  mismatch. With `--kafka` it also starts a second node as a child process, kills the
  leader, and checks the survivor finishes.

The README walks through two nodes: start `a` and `b`, open both UIs, kill `a`, watch `b`
go from standby to leader with the takeover's timings, restart `a` as the standby.

## Tests

Each stage mutation-checks its own list: break the behaviour, see a test fail, revert.

- **S1, in memory.** Registry: declare, follow, promote, loader resume from committed
  offsets, `file` loader re-sync, retention floor including a stopped pipeline and a
  derived table's writer, the cap cutting a pipeline loose. Supervisor: deploy while
  others run; stop drains and commits; fault → backoff → `Failed`; a faulted source
  faults the pipeline; the dead-letter ring keeps a `DeadLetter` circuit moving; replace
  with identical shape continues from state; `shape-changed`; `state-incompatible` rolls
  back; reset. Plugins: two pipeline assemblies sharing a contract type join one table; a
  replaced version unloads (weak reference after GC); a contract loaded twice is refused.
  Catalog: a second in-memory node follows, takes over in the stated order and runs the
  same pipelines to the same sinks; the fenced node's catalog write fails; a pipeline
  that cannot build on the standby does not block the takeover.
- **S2, Kafka.** The S1 node tests against the broker; catalog restore by a cold node;
  and two real processes of a test host (`tests/Incremental.Service.FailoverApp`, in the
  manner of `tests/Incremental.Kafka.FailoverApp`) with a Kafka source, a table loader,
  two pipelines and Kafka sinks: kill -9 the leader, the standby leads within a bound,
  sinks equal the model, counts prove nothing was skipped; a pipeline deployed just
  before the kill exists on the survivor; step down hands over without the timeout.
- **S3.** Each endpoint against an in-memory node, hosted in process on a loopback port;
  the loopback, host, header, read-only, row-data and upload
  switches each refuse what they should; bounded responses; an SSE client that stops
  reading does not grow the server.
- **S4.** Both examples' headless modes.

## Build order

1. **S1 — service core on the in-memory backend.** The backend seam and
   `InMemoryServiceBackend`; catalog; plugin catalog and contexts; table registry with
   the `file` and custom loaders; supervisor; `ServiceNode`; DI; `ServiceMetrics`; core
   changes C1 to C5. Hands on: the seam, and a node that passes the in-memory takeover
   tests.
2. **S2 — Kafka.** `KafkaServiceBackend`, the `kafka` and `debezium` loaders and
   connectors, K1, exit codes, the two-process failover test. Hands on: measured takeover
   with pipelines built on the standby.
3. **S3 — admin.** The API, the security switches, SSE, the UI.
4. **S4 — examples and docs.** Both examples; README, `docs/intro.md`, CLAUDE.md.

S3 can start once S1's interfaces exist; S2 and S3 do not touch the same files.

## Out of scope

- A declarative pipeline spec (the hook: a built-in `IPipelineDefinition`).
- Tenants: authentication, per-tenant authorization, quotas, isolation between plugins.
- More than one standby, or priorities among standbys.
- Clustered circuits: they cannot use shared tables or the changelog provider.
- Sandboxing plugins, or stopping a wedged delegate.
- An S3 `IObjectStore`, a gRPC adapter: the seams are here, the adapters are the owner's.
- Per-operator record counts and latency.
- Reading a table through a secondary index from the admin API (the core exposes index
  reads only inside a circuit).
- Deleting a removed table's topics, or a removed pipeline's last commit record.
- Changing a shared table's row type in place.

## Open for James

Each has a default this design is built on.

1. **Warm circuits on the standby** (default) — the standby loads plugin code and builds
   every circuit, as durability-and-standby.md specifies, and needs C1's follower half.
   *Alternative:* the standby holds pipeline state as bytes and builds at takeover. No
   plugin code runs on a standby and C1 shrinks to `ReleaseStores()`; the cost is decoding
   state during the takeover, roughly a microsecond or two per entry per pipeline, in
   parallel across pipelines — seconds for a pipeline holding millions of entries.
2. **The catalog is a shared table** (default). *Alternative:* a dedicated compacted
   `{p}.catalog` topic. One topic instead of two and no version history nobody reads; the
   cost is a new public append-and-follow API on `KafkaLog` and a second follower
   implementation to test for fencing and restore.
3. **Plugin binaries in the catalog** (default). *Alternative:* store a location (S3,
   a shared volume) and a hash. The catalog stays small; the cost is a second storage
   system whose outage blocks a failover that needs a plugin the standby has not cached.
4. **Contract changes take effect at process restart, additive only** (default).
   *Alternative:* none in place — a table cannot change its row type while circuits hold
   it. The other route is a versioned table name per incompatible change, which is what
   the default already asks for.
5. **Row data off in the admin API by default.** *Alternative:* on by default, which
   makes the first run friendlier and makes a host that enables remote access expose
   tenant rows unless someone remembers the switch. The example turns it on.
6. **Plugin upload over HTTP off by default**, with the plugin directory as the ordinary
   route. *Alternative:* on whenever an authorization policy is set.
7. **A leader whose pipelines have all failed steps down, once per incident.**
   *Alternatives:* stay (visible in health, no failover for a deterministic fault that
   would fail on the other node too) or exit.
8. **`Failed` is not durable**: a new leader retries a pipeline the old one gave up on.
   *Alternative:* record it in the catalog, so a deterministic failure is tried once;
   the cost is a catalog write on the failure path and a pipeline that stays down after
   a failover caused by the very node fault that failed it.
9. **Removing a table keeps its topics**, so the name can be declared again with its
   rows. *Alternative:* delete the topics, which needs topic deletion in `KafkaLog` and
   makes a mistaken removal unrecoverable.
10. **An incompatible replace rolls back and reports**; reset is always a second,
    explicit request. *Alternative:* leave the pipeline stopped in `NeedsReset` — simpler,
    and a tenant's mistaken deploy takes their pipeline down until someone chooses.
11. **Plugin authors reference `Incremental.Service`** (compile time only).
    *Alternative:* a third package, `Incremental.Service.Abstractions`, holding
    `IPipelineDefinition`, `PipelineBuilder`'s surface and `TableDefinition`, so plugin
    projects do not see Kafka or hosting types.

## Not verified

- That a collectible context actually unloads after `ReleaseStores()`: `System.Text.Json`
  metadata caches and the core's per-type caches (`StableHash`, `ByteStableEquality`,
  `ShardCodec`) may hold plugin types. S1's unload test will say.
- That disposing a built, never-started circuit on a follower is free of side effects in
  every operator. `Circuit.DisposeAsync` runs no step unless started, by reading.
- Takeover time with hundreds of built circuits and their sources; one consumer thread
  per source (durability-and-standby.md, *Limits*) is the likely ceiling.
- The data-shape fingerprint's false positives on real plugin assemblies.
- Catalog size and restore time with many plugin versions kept.

## As built

Nothing yet. Each build step records here what it built and where it departs from the
text above.
