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
- Connector stack: `docker compose --profile connectors up -d` adds Kafka (`localhost:9092`), Postgres with
  logical decoding (`localhost:5432`, user/password/db `incremental`) and Debezium's Kafka
  Connect (`localhost:8083`). Plain `docker compose up -d` stays Redis-only.
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
  consumer costs 500 ms.
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
  create a producer.
- Leadership (`KafkaLeadership`) lives in Kafka only. The log writer heartbeats to
  `{p}.leader` in every transaction and when idle; a standby takes over on silence. Safety
  is fencing, never the election or the probe. Every node starts as a standby; `Fenced`
  and `Resigned` are final (the process exits) — no demotion in place. Takeover order:
  writer, tables, storage, circuits. A node that cannot lead must stop heartbeating.
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
  value are a persisted format: changing either needs a new format byte, not an edit. A
  follower applies the log to the stores until `PromoteAsync`, and `Recover()` and commits
  throw before it. Takeover order: promote tables, then the storage, then `Start()` each
  circuit.
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
- Public API is fluent delegates with LINQ-dialect names (`Select`/`Where`/`GroupBy`),
  intentionally not LINQ/expression trees. The 0.2 surface is specced in `docs/intro.md`:
  windows exist only on groupings of an `EventTimeStream`; grouping types are staging types
  (no output/projection); outputs are unnamed definitions redeemed via `circuit.Handle`;
  table-shaped outputs bootstrap consumers automatically (batch zero = current state).
