# Incremental

DBSP-style incremental stream processing library for .NET (libraries multi-target
net8.0/net10.0; tests/examples net10.0; SDK pinned via mise.toml).

## Commands
- Build: `dotnet build`
- Test: `dotnet test --project tests/Incremental.Tests` (xunit v3 on Microsoft.Testing.Platform,
  opted in by `global.json` — hence `--project`; Redis tests skip unless `docker compose up -d` has run)
- Kafka tests: `dotnet test --project tests/Incremental.Kafka.Tests` — integration tests of
  `Incremental.Kafka` against the connector stack (start it first, below): the logs,
  leadership, sources, sinks, and CDC from Postgres through Debezium. Tests skip, not fail,
  when what they need is unreachable — the broker for most, Postgres and Kafka Connect
  too for `DebeziumCdcTests`; a handful need nothing and always run. Each test uses its
  own topic prefix (and its own Postgres tables, slot and connector). About a minute and a half.
  `INCREMENTAL_KAFKA_BOOTSTRAP` points them at another broker. `KafkaFailoverProcessTests`
  launch `tests/Incremental.Kafka.FailoverApp` as real processes and kill, stop and
  command them; measured numbers go to `measurements.log` beside the test assembly.
- After a mutation script has restored a source file, touch it (or the next build keeps
  the mutated binary: the restored file is older than the output it produced).
- Service tests: `dotnet test --project tests/Incremental.Service.Tests` — about a minute
  and a half with the connector stack up, a few seconds without it: the Kafka-backed classes
  (`KafkaService*Tests`) skip when the broker is unreachable, the CDC one also without
  Postgres and Connect. They load real plugin assemblies from
  `tests/Incremental.Service.TestPlugins/*/bin` and launch
  `tests/Incremental.Service.FailoverApp` as real processes, both of which a solution build
  produces (the test project references them build-order only). The Kafka classes share
  one non-parallel collection; measured failover times go to `measurements.log` beside
  the test assembly.
- Admin UI: `dotnet run --project tests/Incremental.Service.AdminHost` serves a node with
  tables, pipelines and a generator at http://127.0.0.1:5180/admin/ (`-- --nodes 2` adds a
  standby on 5181; `--read-only`, `--no-rows`, `--port`;
  `--ui-dir src/Incremental.Service.Admin/wwwroot` serves the UI's files from the checkout,
  so an edit is a browser reload away). The admin API's tests (`AdminApiTests`) run that
  host on a free port.
- Enrichment service example: `dotnet run --project examples/Incremental.EnrichmentService`
  serves one in-memory node with the admin UI at http://127.0.0.1:5180/admin/ and its sinks
  at `/views` (`-- --nodes 2`: a standby on 5181). `-- --kafka --node a` and
  `-- --kafka --node b` are two processes on the connector stack (reference data in
  Postgres through Debezium, Kafka sinks; everything named after `--prefix`, default
  `enrich`; `-- --kafka --clean` removes it). `-- --headless [--nodes 2 | --kafka]` is the
  smoke test: it drives the admin API, checks every sink against `Host/Model.cs`, and
  exits 1 on a claim that does not hold — run it after changing the service. CI runs the two
  in-memory ones (job `build-and-test`) and the Kafka one (job `connector-stack`, which starts
  the compose stack and fails if a Kafka or service test was skipped). `-- --ui-dir
  src/Incremental.Service.Admin/wwwroot` serves the admin UI from the checkout.
  `-- --otlp` exports both meters over OTLP (`OTEL_EXPORTER_OTLP_ENDPOINT`); the example is
  the only project that references OpenTelemetry. To leave it running in the background,
  run the built binary and stop it with SIGTERM (a backgrounded `dotnet run` ignores SIGINT).
  Its build copies the plugin (`Pipelines/`) to `plugins/` beside the host and zips
  version 2 (`Pipelines.V2/`, the same sources with `V2` defined) to `plugin-packages/`.
  `--poison N` sets how many generated trades have no quantity (the dead letters; 200).
  The definitions' own tests are `dotnet test --project
  examples/Incremental.EnrichmentService/Pipelines.Tests` (xunit v3 on
  `PipelineTestHarness`; under a second, and CI runs them).
- Trade enrichment on shared tables: `dotnet run --project examples/Incremental.TradeEnrichment`
  prints a narrative of twelve checked claims and exits 1 if one is wrong (`-- --trades N`).
- Service guide: `docs/enrichment-service.md`. Its C# blocks are regions of the example's
  sources (`// <snippet:name>` … `// </snippet:name>`, quoted under
  `<!-- snippet: path#name -->`); `DocSnippetTests` in `tests/Incremental.Service.Tests`
  fails when a block and its region differ or a C# block has no source. Edit the source,
  then copy the region into the document.
- Connector stack: `docker compose --profile connectors up -d` adds Kafka (`localhost:9092`), Postgres with
  logical decoding (`localhost:5432`, user/password/db `incremental`) and Debezium's Kafka
  Connect (`localhost:8083`). Plain `docker compose up -d` stays Redis-only.
- Three brokers: `docker compose --profile kafka3 up -d` (replication factor 3,
  `min.insync.replicas` 2, on `localhost:9192,localhost:9292,localhost:9392`; no volume).
  Run the Kafka and service tests against it with `INCREMENTAL_KAFKA_BOOTSTRAP` set to
  those three and `INCREMENTAL_KAFKA_REPLICATION=3`; the CDC tests skip there (Kafka
  Connect writes to the single broker). With
  `INCREMENTAL_KAFKA_BROKERS=incremental-kafka3-1-1,incremental-kafka3-2-1,incremental-kafka3-3-1`
  as well, four tests in `KafkaServiceProcessTests` stop and start those containers
  (every broker shut down in turn, and killed in turn; a takeover with one down; 70 s
  without a quorum): `-- --filter-method "*broker*"`, about eight minutes; skipped
  everywhere else. They assert that nothing is lost and one node leads, and record in
  `measurements.log` whether the leadership moved — a broker restart can move it.
  `docker compose rm -sf kafka3-1 kafka3-2 kafka3-3` when done — not `down`, which
  takes Redis and the rest of the stack with it.
- A broker that refuses unknown clients: `docker compose --profile kafka-sasl up -d`
  (`localhost:9492`, SASL/OAUTHBEARER with unsigned tokens, deny-by-default ACLs). Only
  the scale rig talks to it (`--oauth`); its README has the commands. Remove it with
  `docker compose rm -sf kafka-sasl`.
- Examples: `dotnet run --project examples/Incremental.Example`,
  `dotnet run --project examples/Incremental.LogAnalytics [-- --redis]`
- Showcase (the all-features demo): `dotnet run --project examples/Incremental.Showcase`
  serves a web app on http://localhost:5170. Flags: `--port`, `--no-open`, `--redis`,
  `--workers N`, `--nodes N` (`--cluster` is an alias for `--nodes 2`).
  `-- --headless` runs every tab unattended and exits non-zero if a verified act fails —
  that is the smoke test. `Microsoft.NET.Sdk.Web` plus the library projects; no NuGet packages of its own. The live
  circuit runs under `Incremental.Hosting` — one generic host per generation, because the
  crash act kills it; the scaling and cluster acts build theirs by hand (throwaway / not hostable). Tab 5
  measures worker-count scaling and can relaunch the process into another topology; run it
  `-c Release` if the throughput column is the point (Debug flattens it).
- Benchmarks: `dotnet run -c Release --project benchmarks/Incremental.Benchmarks -- --filter '*'`
  (BenchmarkDotNet); `-- --assert` runs the CI performance contracts — scaling-ratio
  assertions (delta cost vs state size), which is what `.github/workflows/ci.yml` gates on;
  `-- --log-sizes` prints bytes per row of the Kafka logs' record format (JSON vs the dense
  codec, raw and compressed)
- Service scale rig: build `benchmarks/Incremental.Service.Scale` in Release and run the
  built binary — `dotnet Incremental.Service.Scale.dll run --rows 1000000 --state-keys 1000000
  --broker-container incremental-kafka-1` from its output directory. Two node processes
  on Kafka, a wide table shared by twenty pipelines, large state, then a cold standby,
  `kill -9`, a manual takeover and a node from nothing, each timed by stage; about three
  minutes at a million rows, and it needs the memory (its README says how much). Run it
  after changing anything a takeover or a standby does; the numbers of record are in
  docs/design/enrichment-service.md (S6, *Scale*).
- Throughput rig: `dotnet run -c Release --project benchmarks/Incremental.Torture -- --workers 4`
  runs the showcase pipeline flat out over a sliding order population and reports records/s,
  the process CPU split (workers vs GC), and allocation per record (`--alloc-types` for a
  histogram by type; `--processes N` for the cluster wire path). Its README carries the
  current numbers and the GC settings that matter

## Architecture invariants
- Streams carry `ZSet<T>` **deltas** (value → signed weight); operators must be fully
  incremental and honor negative weights (retractions) everywhere.
- Execution is step-based. All workers execute all nodes in the same topological order each
  step; exchange nodes rely on this for barrier alignment (`Circuit.RunWorkerStep`).
- Operator state is sharded per worker and never shared across threads. Any operator whose
  state is keyed must sit downstream of an exchange on that key: `GroupBy` inserts one,
  `Distinct` shuffles by value, keyed inputs place commands by key at drain (plus an exchange
  across processes), windowed groupings route by the user key, and
  window-collapsing regroups (windowed `TopK`, window-sticky `GroupBy`) re-exchange by the
  window. Adding a stateful operator without this breaks multi-worker correctness silently.
- The one exception to "never shared across threads" is `Tables/SharedTable`: reference rows
  held once per process and read by many circuits (`docs/design/shared-tables.md`). Its
  rules: one writer; readers never write and never lock per read — they pin a version and
  read as of it; rows are immutable; only the writer trims, and never past the oldest pin.
  Anything else that wants cross-thread state should go through a table, not copy the trick.
- Table operators follow the same exchange rule. A table `Join` keeps its facts per key, so
  the table's changes go through an exchange of their own, routed by the very hash the
  facts' grouping used (`TableView.RouteLikeFacts`; an index join re-keys the changes by the
  index first) — otherwise a change lands on a worker that does not hold the facts it
  corrects. A remembered `Lookup` keeps what it emitted per fact, so it sits behind the
  grouping's exchange on the lookup key (`stream.Lookup(table, key, …)` is
  `GroupBy(key).Lookup(…)`). Only `LookupAppendOnly` has no state and no exchange; probes
  themselves need no partitioning.
- Exchange routing must be process-stable: route by `StableHash` (or a user-supplied
  stable hash), never `GetHashCode` — .NET randomizes string hashes per process, which
  silently breaks cluster routing and multi-worker checkpoint restore.
- Per-step allocation is the dominant cost at throughput: the GC's share of process CPU
  scales with it while a circuit's live set stays large and mostly static. A new operator
  that allocates per delta row — or worse, per touched key — is a regression even when its
  asymptotics are right. The rules:
  - Draw the output Z-set from the per-operator `OutputBuffer` (double-buffered, cleared per
    execution, sized from the delta it derives from) instead of allocating one. The contract
    that makes this safe (`OutputBuffer.cs`): nothing an operator returns from `Step` is
    written after return, nothing retains a delta past one more execution of its producer,
    and output ports copy — never adopt — worker slots.
    The one exception is `TableRowsOperator`'s bootstrap: the first step of a new circuit
    over `table.Changes()` emits every row of the table, once, in a Z-set allocated for that
    step — so the operator's reused buffer is not left the size of the table. Do not copy it
    for anything that happens more than once per circuit.
  - Keep flatten/probe/put scratch as cleared-per-step fields rather than rebuilding it.
  - Use the stores' `ApplyAll` / `GetGroupsInto` (batched update with no result array;
    batched group read into caller-owned buffers) rather than the allocating variants
    wherever the results are not themselves needed. `AggregateOperator` keeps `GetGroups`
    because the group is handed to a user aggregate, and `AntiJoinOperator` keeps `UpdateAll`
    because it reads the updated counts; both say so in a comment. An operator that needs the
    results should do the same rather than be "fixed".
  - The matching startup configuration (server + concurrent GC, DATAS off, GC heap count
    sized to `CircuitOptions.Workers`) is applied to every example by
    `examples/Directory.Build.props` plus a `<CircuitWorkers>` property per example csproj —
    keep that property in step with the worker count the example's `CircuitOptions` actually
    uses. The torture rig deliberately opts out so its published numbers stay stock-GC.
- The cross-process exchange wire format is the dense codec in `Cluster/ShardCodec.cs`
  (compiled per type from the same structural rules as `StableHash`), with a JSON fallback for
  types it cannot round-trip and a format byte on every frame. Redis state serialization is
  still JSON and must stay so — it is a persisted format.
- Durable logs on Kafka (`src/Incremental.Kafka`, docs/design/durability-and-standby.md).
  Everything durable goes through the one transactional producer a `KafkaLog` owns —
  never produce outside it, or fencing no longer covers the write. A commit fails only as
  fenced (`KafkaLogFencedException`: the writer is finished) or as aborted
  (`KafkaLogException`: nothing readable, writer usable); an unknown outcome is resolved
  inside the writer loop, never reported. Readers are `read_committed`; "the end" is the
  last stable offset, and positions must pass transaction markers. Record values start
  with a format byte; keys must be byte-stable because compaction compares bytes.
  Assign consumers at numeric offsets, never `Offset.Beginning`: a logical offset on a new
  consumer costs 500 ms. A read on the log's shared short-read consumer assigns through
  `KafkaLog.AssignForShortRead`, never `consumer.Assign`: a running consumer pointed at a
  topic it has not seen waits a second for the client to look the leader up, and that
  consumer keeps `fetch.wait.max.ms` at 5 so one read does not wait out the last one's
  long poll. Both cost a takeover or a start-up a second or half a second per table.
- Kafka clients, wherever one is made (found on three brokers and on a broker with ACLs;
  docs/design/durability-and-standby.md, last section). Consume through
  `AssignedConsumer.ConsumeAssigned`, never `consumer.Consume`: the client reports a
  refused lookup of its never-joined group as a consume error. Call `ServeCallbacks`
  before asking a consumer that is not being polled for metadata or watermarks: its
  token refresh runs only inside `Consume`. Take a `ListOffsets` answer through
  `KafkaLog.OffsetOf`: a partition with no leader comes back as an unset offset, not an
  exception, and −1001 read as "empty" has cost a repeated term and a lost table head.
  Admin calls go through `RetryAsync` with the attempt's timeout. Build every client
  with the connection's `Configure…` callback, or it cannot authenticate where tokens
  come from a handler.
- Kafka sources and sinks (`KafkaSource`, `KafkaSinks.cs`). A source's position lives in
  the checkpoint, never in a consumer group: records are handed on stamped with
  (partition, offset), and a source assigns each partition at the committed offset plus
  one (`circuit.CommittedOffsets`, or `table.CommittedOffsets` for a table loader) — no
  group is joined, nothing is committed to Kafka, and a position that is gone is an error.
  A record the source cannot deserialize goes through `InputHandle.RejectAsync`, so its
  offset is consumed and the circuit's error policy decides. A source's memory is the
  input's bounded channel plus the client's fetch buffer; never read ahead into a
  collection. A table loader is the table's writer: leader only. Sinks get an idempotent
  producer of their own unless given the log's (`KafkaSinkOptions.Log`); nothing else may
  create a producer — except the service's blob store, whose records are content-addressed
  (see `Incremental.Service` below).
- Leadership (`KafkaLeadership`) lives in Kafka only. The log writer heartbeats to
  `{p}.leader` in every transaction and when idle; a standby takes over on silence. Safety
  is fencing, never the election or the probe. Every node starts as a standby; `Fenced`
  and `Resigned` are final (the process exits) — no demotion in place. Takeover order:
  writer, tables, storage, circuits. A node that cannot lead must stop heartbeating.
- `CircuitBuilder.Describe` reports a `Lookup`'s table as `Probes`, not among `Inputs`:
  the lookup probes the table and does not consume its changes, so the edge belongs to
  the picture and must never become an execution input.
- Checkpointed state identity is covered by the state manifest (committed with every
  checkpoint, validated at `Start()`); store names are positional unless pinned with
  `.Named()` — changing store naming or the manifest format needs a format-version bump.
- Backpressure comes only from bounded channels (inputs, outputs). Never buffer unboundedly.
  This includes outputs: stream-shaped outputs never maintain integrated (snapshot) state —
  only table-shaped (keyed/windowed) outputs may, and theirs is bounded by eviction. The
  dead-letter output and sink delivery (`Io/Sinks.cs`) obey it too: one bounded channel, one
  batch in flight.
- Commit ordering. A circuit with no acknowledged output commits its checkpoint, then
  publishes (at-most-once). With one (`ToAcknowledgedOutput`, which sinks are delivered
  from) the order turns round: publish, wait for every acknowledged output's
  acknowledgement, then commit — and no step runs while that gate waits (`Circuit.GatesCommits`;
  docs/design/durability-and-standby.md, *Commit after delivery*). A new output kind that
  must be durable before the checkpoint implements `IAcknowledgedOutputPort`; nothing may
  acknowledge a batch before its sink has it.
- Derived tables (`ToTable`, `Tables/DerivedTable.cs`). The circuit is the table's single
  writer: `table.Writer` refuses while it is claimed. A step's table version commits
  before the checkpoint that covers it (it is an acknowledged output), and the checkpoint
  records that version under `{table}#w`; a restart undoes the versions written past it.
  A circuit never reads a table it writes, directly or through other derived tables —
  `Build()` refuses.
- `ChangelogStorage` (`Storage/ChangelogStorage*.cs`). Its record keys
  (`pipeline NUL incarnation NUL store NUL entry`) and the format byte in front of every
  value are a persisted format: changing either needs a new format byte, not an edit
  (1: the serializer's output; 2: a layout tag in front of it). A
  follower applies the log to the stores until `PromoteAsync`, and `Recover()` and commits
  throw before it. Takeover order: promote tables, then the storage, then `Start()` each
  circuit.
- Layouts (`ChangelogStorageProvider.UseLayout`; docs/design/durability-and-standby.md,
  *As built: layouts*). A follower may hold typed stores only under a layout: every value
  its types' leader writes carries the layout, and the follower turns its typed stores
  back into bytes before it applies a value of another one. Whatever decides a store's
  types must be in the layout; a circuit built on a provider is started only if
  `StoreEpoch` has not moved since it was built. Do not apply a record to a typed store
  on a follower by any path that skips the tag check.
- Per-record errors (`RecordErrors.cs`). Under `RecordErrorPolicy.Fault` (the default) an
  operator's record loop must contain no exception handling: the dead-letter variant is a
  separate loop or method chosen once per step on `OperatorContext.Errors` being non-null.
  Dropping a record is only correct if its retraction is dropped too — stateless operators
  get that from deterministic delegates, stateful ones must record the drop in their state.
  Null checks on generic values go through `RecordErrors.IsNull` (`is null` boxes).
- `Incremental.Hosting` (generic host + DI) is a separate package so the core keeps its one
  dependency. A hosted circuit is keyed by its definition class (`CircuitHost<TDefinition>`);
  the host supervises on `Circuit.Completion`. Shutdown order is the invariant: drain the
  circuit first, with consumers and views still reading, and stop those after — an unread
  output blocks the drain. Clustered circuits are not hostable yet.
- `Incremental.Service` (docs/design/enrichment-service.md, *As built*) is one circuit per
  pipeline over shared tables held by a registry, with the catalog — itself a shared table —
  as the only record of what exists. Its rules:
  - Everything durable or about roles goes through `IServiceBackend`; outside
    `InMemoryBackend.cs`, `KafkaBackend.cs` and `KafkaConnectors.cs` nothing in the service
    names Kafka or the in-memory logs. Only the leader writes the catalog,
    one action per version, and commits intent before it acts. Once an intent is committed
    the action takes no caller's token and is carried through; a catalog write looks at
    its token once, before the first row reaches the table's writer.
  - A pipeline's runtime is acted on only under its queue, and everything is checked
    again once the queue is held (`EnterExistingAsync`): the runtime is still the one the
    supervisor holds, it exists, and the node still runs pipelines. A runtime leaves the
    dictionary only under its own queue.
  - Every path that can start a generation goes through `ReconcileAsync`, which finishes
    a pending reset or removal first and never starts a pipeline around one; a failure to
    finish is `Backoff` and the act is retried. `StartCoreAsync` asks who leads again
    before it publishes the generation, because a start can outlast a takeover.
  - Naming a thing and removing it share one lock (`ServiceContext.LockReferencesAsync`):
    a deploy checks its tables and plugin version and commits under it; a table removal
    and a plugin delete hold it from their check of users to the end. It is taken after a
    pipeline's queue, never before, and (for a plugin delete) after the import lock.
  - The service promotes its tables itself, inside the leadership's `PrepareAsync`,
    concurrently and each contained; it never calls `KafkaLeadership.Track(table, log)`,
    which would let one table end a takeover. Only the storage is tracked.
  - Pipelines failing never move leadership. The node gives it up by itself only when
    the catalog's writer has faulted and one reopen did not mend it: step down to a ready
    standby, else exit 5. Fenced exits 3, resigned 4; both are final for the process.
  - A spec names a connection (`AddKafkaConnection`, `AddObjectStore`) and never carries
    bootstrap servers, client properties or absolute paths: specs are tenant-written JSON
    that the admin API returns. A built-in connector's config is a closed record with
    unknown members refused.
  - Plugin blobs are written by a plain idempotent producer, unfenced, which is safe only
    because the key is the content's hash and a blob without its `end` record reads as
    absent. Do not put anything not content-addressed on that topic.
  - Every sink a connector creates is wrapped in `leadership.Leased` by the supervisor;
    a connector must not write from anywhere but the sink's `WriteAsync`.
  - The `files` connector's formats are persisted: the sink's object names
    (`{prefix}{step:D19}-{term:D6}.jsonl`) and line shapes (`FileSinkFormat`), and the
    source's position in the checkpoint's partition → offset map (`FilePosition`:
    partition 0 the line, 1 the name's length, 2… the name in two banks, each offset
    prefixed by the object's ordinal so that it only rises). Change either and deployed
    pipelines lose their place: a new connector name, not an edit.
  - Dead letters with a sink bound (`PipelineSpec.DeadLetters`) are an acknowledged
    output: nothing acknowledges a batch before the sink has it, and a sink that fails
    faults the pipeline — never drop to the ring and carry on.
  - `PipelineTestHarness` builds a definition through the same `DefinitionBinding` and
    `PipelineBuilder` as the supervisor; keep it so, or its tests stop meaning what a
    deploy would do.
  - A table, plugin or pipeline that fails is that thing's failure: shown, retried, never
    thrown out of a takeover callback. Only the catalog and the storage may fail a node.
  - Nothing waits without a bound, and nothing holds another pipeline's queue: teardown is
    `StopTimeout`, then abandon, then `Wedged`; a connector's `StartAsync` and `ResetAsync`
    are tenant code and get `ConnectorTimeout`.
  - Seeds are applied once per deployment and recorded in `node/seeds`; never test "the
    key is absent" to decide whether to seed — a removal makes that true again.
  - Nothing of a plugin's may be reachable from a static or a process-wide cache, or its
    load context never unloads. That includes `JsonSerializerOptions`: a pipeline's types
    go only through the per-version options on its `PluginLease`, and those are built with
    a resolver of their own, because System.Text.Json shares one metadata cache between
    options instances it considers equivalent. `Validator` (DataAnnotations) is avoided
    for the same reason. One cache cannot be avoided — System.Text.Json's accessor cache,
    which drops an entry only when it is next used — so an unload is followed by a few
    uses of it (`JsonAccessorCache.EvictSoon`). The unload test in `PluginTests` is the
    check, and it must pass when run alone.
  - A manifest's `MainAssembly` is a file within the package and `AssemblyName` the
    assembly's simple name; they differ, so never derive one from the other. Which
    reference goes to the host is `HostAssemblies.Provides`: contracts first, then the
    assemblies whose types cross the boundary, then by version.
  - A replace that keeps state across a change records the spec it replaced
    (`PipelineSpec.Previous`) until its first generation runs. A generation that cannot
    start on the state (`CircuitStateMismatchException`, from the store that could not be
    read or from `Circuit.Start`) is rolled back to it in one catalog commit — by this
    leader or by its successor — and never retried; with no previous spec it is
    `NeedsReset`. What `CircuitBuilder.CheckState` can decide is refused before anything
    stops. Do not add a path that starts a kept-state replace without `Previous`, or one
    that clears it before the generation is `Running`.
  - The evidence for keep-or-reset is `StateShape`: text only, never a `Type`, so it
    outlives the plugin version it was read from. It compares what state stores hold
    (`CircuitBuilder.DescribeState`), not what the assembly declares.
  - `ServiceMeter` (the meter `Incremental.Service`): tags are `node`, `pipeline`, `table`
    and the closed sets `state`, `role`, `stage` — nothing unbounded, nothing from a
    record. Gauges read the snapshot a node refreshes once a second; a callback must not
    touch a circuit, a table or the backend.
  - Discovery (`IServiceDiscovery`, the admin's `connectors`, `connections`,
    `table-sources`, definition and template endpoints) returns names, kinds and config
    schemas, never a connection's settings. Describing a definition runs its `Build`
    against stand-in tables (`PipelineBuilder` detached) and is leader-only.
  - The in-memory backend must stay bounded: the state log compacts itself
    (`StateCompactAbove`), table logs cap history by row changes, and a tombstone is
    dropped only once every enumerating reader has passed it.
  - The state binding: a pipeline's typed stores belong to one plugin version. Changing
    the version, or anything else that changes what the state means, goes through
    `ChangelogStorageProvider.ReleaseStores()` first. The binding has a name,
    `PipelineSupervisor.LayoutOf(spec)` (plugin version, definition, settings, workers,
    error policy, table bindings): `Bind` gives it to the provider, and a start whose
    spec has another layout rebinds. Add to `LayoutOf` anything new that can change a
    store's key or value type.
  - A standby builds each pipeline's circuit on the state it follows
    (`PipelineSupervisor.WarmAsync`, `ServiceOptions.WarmStandbyPipelines`) and a
    takeover starts it (`TakeWarm`): a takeover must not do work that grows with a
    pipeline's state. `WarmStandbyTests` holds the ratio; the scale rig
    (`benchmarks/Incremental.Service.Scale`) measures it. A standby's build is contained
    like everything else of a pipeline's: it never makes the node unready or fails it.
- `Incremental.Service.Admin` (the JSON API and the embedded UI). Its rules:
  - Nothing is returned unbounded: every list goes through `Page`/`Newest` and every
    `limit` is clamped to `MaxRows`. The event feed is one bounded drop-oldest queue per
    client, a cap on clients and a write timeout; a frame is serialized once for all.
  - A new endpoint is mapped through `Get(…)` or `Mutation(…, areaPolicy)` so that it runs
    under `AdminEndpointFilter` (loopback rule, read-only, the mutation header, the error
    shape). A handler throws `ServiceException` or `AdminException`; it never writes an
    error body of its own.
  - Exception messages, stacks, rows and records pass through `AdminState.Message`,
    `Fault`, `Head` or `Event` on their way out: with `ExposeRowData` off none of them may
    leave, and a service event's text after its first colon counts as a message.
  - The UI is seven files in `wwwroot`, no build step, no dependency, no request to another
    origin, and a content security policy without `unsafe-inline`: no inline script, no
    `style` attribute (set styles through the CSSOM), no `innerHTML` with data. Every
    render goes through `morph`, and handlers are `on:` properties found by the delegated
    listener — `addEventListener` on a rendered node is lost at the next patch. A path the
    scripts call must be a route the API maps: `The_UI_is_served_whole…` checks it.
  - The deploy and declare forms (`forms.js`) are drawn from the discovery routes
    (`plugins`, a definition's description and template, `connectors`, `connections`,
    `table-definitions`, `table-sources`) and edit one spec object, which the JSON view
    shows: a member the form does not model stays in the spec untouched. Nothing is sent
    for real until a dry run of exactly that spec has passed; a plain deploy carries
    `If-Match: "0"` and a replace the generation it replaces.
  - Who made a change comes from `AdminSecurity.Caller` and nowhere else: the host's
    `CallerName` hook, then the authenticated identity, then the name a caller states in
    `X-Incremental-Operator` (recorded as `name (self-declared)`), then `local`.
- Public API is fluent delegates with LINQ-dialect names (`Select`/`Where`/`GroupBy`),
  intentionally not LINQ/expression trees. The 0.2 surface is specced in `docs/intro.md`:
  windows exist only on groupings of an `EventTimeStream`; grouping types are staging types
  (no output/projection); outputs are unnamed definitions redeemed via `circuit.Handle`;
  table-shaped outputs bootstrap consumers automatically (batch zero = current state).
