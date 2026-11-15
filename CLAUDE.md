# Incremental

DBSP-style incremental stream processing library for .NET (libraries multi-target
net8.0/net10.0; tests/examples net10.0; SDK pinned via mise.toml).

## Commands
- Build: `dotnet build`
- Test: `dotnet test --project tests/Incremental.Tests` (xunit v3 on Microsoft.Testing.Platform,
  opted in by `global.json` — hence `--project`; Redis tests skip unless `docker compose up -d` has run)
- CI (`.github/workflows/ci.yml`): three jobs on every push; a fourth, `broker-failures`
  (the six broker tests on the `kafka3` profile), only by hand:
  `gh workflow run CI --ref <branch> -f broker_tests=true`.
  A test that reads "the end" of a Kafka partition right after its own commit must wait
  for it (`KafkaEnv.EventuallyAsync`): the marker is written after the commit returns.
  A service test that has seen an output and goes on to something that reads the
  checkpoint (a takeover or restart counted as resumed, a replace that keeps state, a
  figure taken at a commit) waits for `TestService.Settled()` in between: the output is
  in the sink before its step is committed. One that counts a files sink's objects
  waits until the sink's write has returned (`SinkAtRestAsync`). One that reads the
  whole process's heap goes in the `measured alone` collection. A hook sets what the
  test reads before the count the test waits on.
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
  host on a free port. With two nodes a node that loses the leadership is started again
  in the process (`DemoOptions.SuperviseNodes`), as in the example's `--nodes 2`.
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
  `--poison N` sets how many generated trades have no quantity (the dead letters; 200);
  `--trade-ids N` makes trade ids come round after N (default 0: never — the pipelines'
  state is bounded by event time, not by the generator). On Kafka the trades and quotes
  topics have three partitions (`KafkaStack.FeedPartitions`).
  The definitions' own tests are `dotnet test --project
  examples/Incremental.EnrichmentService/Pipelines.Tests` (xunit v3 on
  `PipelineTestHarness`; under a second, and CI runs them).
- Trade enrichment on shared tables: `dotnet run --project examples/Incremental.TradeEnrichment`
  prints a narrative of thirteen checked claims and exits 1 if one is wrong (`-- --trades N`).
- Service guide: `docs/enrichment-service.md`. Its C# blocks are regions of the example's
  sources (`// <snippet:name>` … `// </snippet:name>`, quoted under
  `<!-- snippet: path#name -->`); `DocSnippetTests` in `tests/Incremental.Service.Tests`
  fails when a block and its region differ or a C# block has no source. Edit the source,
  then copy the region into the document.
- Connector stack: `docker compose --profile connectors up -d` adds Kafka (`localhost:9092`), Postgres with
  logical decoding (`localhost:5432`, user/password/db `incremental`) and Debezium's Kafka
  Connect (`localhost:8083`). Plain `docker compose up -d` stays Redis-only.
- Three brokers: `docker compose --profile kafka3 up -d` (replication factor 3,
  `min.insync.replicas` 2, on `localhost:9192,localhost:9292,localhost:9392`; no volume;
  fixed addresses on a network of their own, 10.89.93.0/24 — a container cluster whose
  brokers change address or look each other up by name stalls for reasons that are not
  Kafka's, see durability-and-standby.md, *As built: broker failures*).
  Run the Kafka and service tests against it with `INCREMENTAL_KAFKA_BOOTSTRAP` set to
  those three and `INCREMENTAL_KAFKA_REPLICATION=3`; the CDC tests skip there (Kafka
  Connect writes to the single broker). With
  `INCREMENTAL_KAFKA_BROKERS=incremental-kafka3-1-1,incremental-kafka3-2-1,incremental-kafka3-3-1`
  as well, six tests in `KafkaServiceProcessTests` stop, kill and freeze those containers
  under a running pipeline (every broker shut down in turn, and killed in turn; one
  broker lost; a takeover with one down; 70 s without a quorum; one broker gone for
  minutes with the leader killed half way): `-- --filter-method "*broker*"`, about seven
  minutes, on a cluster started a moment ago or an hour ago; skipped everywhere else.
  They run their nodes under `NodeSupervisor`, assert that the sink equals the model,
  that one node leads and the other stands by, and — for one broker lost, and for the
  70 s without a quorum, which is under the transaction timeout — that the
  leadership did not move; each run's line goes to `measurements.log` and its record to
  `broker-runs.jsonl` beside the test assembly (the sink's longest silence, the longest a
  trade took, who led). `INCREMENTAL_KAFKA_BROKER` (0–2), `INCREMENTAL_KAFKA_FAULT`
  (`kill`, `pause`, `stop`), `INCREMENTAL_KAFKA_AWAY_SECONDS` and
  `INCREMENTAL_KAFKA_ZONE_MINUTES` choose what the one-broker and the minutes-long tests
  do; `INCREMENTAL_KAFKA_OUTAGE_SECONDS` and
  `INCREMENTAL_KAFKA_TRANSACTION_TIMEOUT_SECONDS` set the no-quorum test's outage and
  the nodes' transaction timeout (60 with the default outage is an outage past the
  timeout, where the leader usually ends and only "nothing lost" is required;
  `INCREMENTAL_KAFKA_FAULT=pause` freezes its two brokers instead of stopping them);
  `INCREMENTAL_KAFKA_BROKER_AFTER_MS` kills the takeover test's broker that long after
  the leader instead of before it;
  `INCREMENTAL_NODE_LOGS=<dir>`, `INCREMENTAL_LOG_LEVEL=Debug` and
  `INCREMENTAL_KAFKA_CLIENT="debug=eos,protocol"` keep each node's Kafka client
  log in a file, which is how a stall is read (add `broker` to the debug list only
  with brokers that are stopped, not killed: a standby logs every refused connection,
  gigabytes a minute).
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
  docs/design/enrichment-service.md (S6, *Scale*; S7, *Memory*). `--memory-only` stops
  after the steady load (memory figures, no failover); `--gc` overrides the nodes'
  collector settings (`node`, the default, is `src/Incremental.Service/Node.Gc.props`;
  `round2`, `stock`, or `name=value` pairs); the working set over time is in
  `<out>.timeline.tsv`. Run it after changing what a node holds or how it gives memory
  back.
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
    A service node is another kind of process (many one-worker circuits over gigabytes
    that stay): its settings are `src/Incremental.Service/Node.Gc.props`, which the
    enrichment example and the scale rig import — eight heaps (four in the example),
    and deliberately no `ConserveMemory` (measured: 0.3 GB at a million rows, nothing
    at five, and it asks for full blocking collections). Change them only with the
    scale rig's numbers beside the change.
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
  The metadata question `AssignForShortRead` asks goes through one admin client kept
  for the consumer's life: disposing an admin client (a `DependentAdminClientBuilder`
  one too) waits out its poll loop's 100 ms, so one made and disposed per question is
  a tenth of a second per topic — it was, in every takeover's fence stage and once per
  table when the reader was new. Never dispose an admin client on a path that is timed.
  A reader (`KafkaLog.ReadLoop`) reports the client's own position when nothing arrives:
  the records of an aborted transaction are passed over inside the client, and the
  end-of-partition event that would say so comes one empty fetch later
  (`fetch.wait.max.ms`, half a second) — which a takeover's storage stage waited for
  after a leader killed inside a transaction. A position reported is a promise about
  every offset below it and never falls.
  A short read its caller calls off (a status that gives a table's lag two seconds)
  keeps that consumer and what it knows of the topics; only a read that failed
  replaces it (`KafkaLogTests`, the short reader's test).
  `AssignForShortRead` asks for a topic's leaders again when its answer is over a
  second old (`ShortReadLeadersFor`): the reader is assigned to nothing between reads,
  what it knows goes stale, and a takeover after a broker's loss otherwise fetches from
  the dead broker first and waits out the client's half-second pause. Not on every
  read, and never waited on for long.
  The writer's transaction timeout (`KafkaLogOptions.TransactionTimeout`, 5 min) is how
  long a leader rides out brokers that cannot commit: past it the transaction fails in
  the client or is timed out by the coordinator, and either usually ends the writer
  (durability-and-standby.md, *A leader does not outlive its transaction timeout*).
  Do not shorten it to hurry a dead writer's transaction away — a takeover and a
  restart both abort it at once — and keep it under the brokers'
  `transaction.max.timeout.ms` (15 min by default); a value above that is refused at
  acquisition by name, not as a fence.
  A client's fatal error reaches the log as an exception and as a callback, on two
  threads: whether the callback's client is still the log's is decided under the lock
  that replaces it (`MarkFenced(…, ofGeneration)`), or a replaced client marks a log
  fenced that had gone back to reading.
  Topics the log creates say `unclean.leader.election.enable=false` themselves
  (`EnsureTopicAsync`; the service's blob topic too): a cluster's default may allow it
  (MSK's does without tiered storage), and everything in these topics is committed state.
  Every Kafka client starts from `KafkaClientDefaults` (reconnect delay at most 1 s,
  socket timeout 6 s, a metadata refresh every 2 s), applied before the caller's own
  settings: librdkafka's defaults leave a healthy leader silent for 10 to 60 s after a
  broker goes, which is a takeover. A new client that skips them brings that back.
  An admin client also gets `enable.sparse.connections=false` from them: one that
  connects on demand asks for a dead broker's connection at every change of broker
  state while a request waits for that broker, which resets the reconnect delay —
  30,000 refused connections a second and more than a processor on a standby. An admin
  client made from another client's handle shares that client's setting: do not give
  it requests meant for one broker (`ListOffsets`, a broker's `DescribeConfigs`) on a
  path that runs while a broker may be down.
- Memory a node holds and does not use (docs/enrichment-service.md, *Sizing a node*;
  docs/design/enrichment-service.md, S7, *Memory*). Three things made a node's working
  set three to five times its live heap, and each has one answer that must stay:
  the Kafka client's fetch queues are per partition, so a source's
  `FetchBufferKilobytes` is 2 MB, not the 16 it was (the logs' readers keep 8: a
  node holds theirs only while it restores, and 2 was no faster);
  glibc keeps what
  the client frees, so `MemoryKeeper` calls `malloc_trim` every
  `ServiceOptions.ReleaseUnusedMemoryEvery`; and the collector holds a burst's regions
  until some twenty collections have passed, so the keeper asks a quiet node for
  young-generation collections. The keeper never asks for a full blocking collection
  (seconds on a leader with a large heap), and `MALLOC_ARENA_MAX` is not the fix (it
  more than doubled a state fill).
- What a node says it holds is estimated from samples taken as rows and entries are
  written or read, never by walking a table or a store: `SharedTable.EstimateMemory`,
  `ChangelogStorageProvider.EstimateSize` (`Diagnostics/ObjectSize.cs`). A sampler and
  the type layouts it has learnt belong to one table or one store and to nothing
  static — a store's types are a plugin's. `MemoryEstimateTests` holds each estimate to
  what the collector reports; keep it so when a store's or the table's structure changes.
- A provider that keeps a second copy of restored state until its next commit says so
  (`ICheckpointStorageProvider.HoldsRestoredCopy`), and an auto-stepping circuit that
  starts on such state and stays idle commits once, without a step, two seconds after
  its start (`Circuit.RestoredCopyGrace`): the checkpoint as recovered, under the new
  manifest. Without it an idle pipeline holds its state twice for as long as it is idle.
- Kafka clients, wherever one is made (found on three brokers and on a broker with ACLs;
  docs/design/durability-and-standby.md, last section). Consume through
  `AssignedConsumer.ConsumeAssigned`, never `consumer.Consume`: the client reports a
  refused lookup of its never-joined group as a consume error. Call `ServeCallbacks`
  before asking a consumer that is not being polled for metadata or watermarks: its
  token refresh runs only inside `Consume`. Take a `ListOffsets` answer through
  `KafkaLog.OffsetOf`: a partition with no leader comes back as an unset offset, not an
  exception, and −1001 read as "empty" has cost a repeated term and a lost table head.
  Admin calls go through `RetryAsync` with the attempt's timeout, and an attempt is no
  longer than two seconds while a broker's loss is being settled
  (`KafkaLog.AttemptTimeout`): one that begins without a partition leader runs its
  whole time, so its length is what a takeover loses once the leader is back. Build every client
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
  The one way back to `Standby` is a takeover that ends before the node is the log's
  writer (the leader was heard before this node had taken the transactional id; the
  acquisition was fenced, which a replaced leader's stale request to the coordinator
  can do — `StaleEpochRequestTests`): nothing is promoted before `AcquireWriterAsync`
  returns, and that must stay so. Such an acquisition replaces the producer
  (`KafkaLogNotAcquiredException`); never keep a client whose `InitTransactions` was
  cancelled — it finishes by itself and fences the leader. A takeover is called off
  for a heard leader only before the first call of `InitTransactions`
  (`AcquireWriterUnlessAsync`): that call's first request makes the coordinator fence a
  leader whose transaction is open, long before the call returns, and the takeover
  must go through — do not look at the abandon token between its slices or past them,
  or a fenced leader and a standby that stood down are what is left. A takeover given
  up for a heard leader holds the next attempt back by nothing; one lost to a fence
  waits.
  The process must run under something that starts it again after exit codes
  3, 4 and 5 (`NodeSupervisor`, or the platform's restart policy): without it a
  deployment is one node after its first takeover. 3 and 4 are how a healthy node ends
  (replaced, resigned) and are answered at once; 5 is a node that could not lead —
  including a takeover of its own that failed after it had taken the log
  (`LeadershipStatus.TakeoverFailed`) — and is answered with a growing delay. Do not
  map a failure of the node's own to 3. Nodes that share a process (the in-memory
  backend's two) cannot exit for it: their `ExitOnLeadershipLoss` is off and
  `NodeSupervisor.RunInProcessAsync` starts one again on `ServiceNode.Ended`.
  At the ceiling a standby is held by brokers that say nothing can be committed and by
  brokers that do not answer the question at all (`WritePath.Unreachable`: frozen
  brokers keep their connections and time the question out) — not by an error a broker
  sends. The leader's two takeover timeouts afterwards count from the first answer that
  is not a hold, never from the last hold: the question itself can wait on the brokers
  for longer than that.
  The ceiling's question to the brokers (`KafkaLog.ProbeWritePathAsync`) does not
  believe the metadata's lists of leaders and replicas in sync by themselves: with no
  controller quorum they never change. It asks any broker for the metadata and each
  listed broker directly; keep it so.
  A heartbeat is a sign of life at the moment it was written, not the moment it is
  read: the last one of a leader that died with a broker is readable only when the
  brokers give the broker up, nine seconds on. `KafkaLeadership.Heard` dates each
  record through `HeartbeatDating` (the writer's `At` plus the smallest difference seen
  between this node's clock and it — the clocks are never compared), counts the
  silence from that date when the date is a takeover timeout or more before the
  reading, calls a takeover off only for a record written within the
  silence it was decided on, and ends the wait after a broker outage only for one
  written within the takeover timeout. A record younger than the takeover timeout is
  heard when it is read, and that must stay too: a leader whose commits take over half
  the takeover timeout writes each heartbeat one commit before it is readable, and
  counted from the writing it is silent for two commits and taken over from while it
  commits (`A_leader_whose_commits_are_slow…`, which the first build of the dating
  failed three runs in three). Do not go back to "heard is now" for a late record; do
  not make the dating remember more than its two five-second stretches of the writer's
  time: an error of the writer's clock lasts as long as what is remembered.
- `CircuitBuilder.Describe` reports a `Lookup`'s table as `Probes`, not among `Inputs`:
  the lookup probes the table and does not consume its changes, so the edge belongs to
  the picture and must never become an execution input.
- Event-time state is checkpointed state (docs/design/durability-and-standby.md, *Event-time
  state*). The clock commits in the offsets marker (`$watermark`, `$watermark#s{n}`, and
  per named partition `$watermark#s{n}#m{p}` and `#i{p}`) and is
  restored at `Start()`; those keys exactly — never tell the clock's entries from an
  input's by how a key starts (`$watermark-feed` is an input). Every deadline an operator waits on lives in an `IDeadlineStore`
  (`{node}.deadlines.w{n}`), never in a field: an operator that evicts or announces on the
  watermark goes through `EvictionTracker` (or sets deadlines in a store itself, as the
  session operator does), calls `DrainClosed()` once per step on every way out, and keeps
  no queue of its own. An operator whose tracked keys come and go under a grouping's key
  (the remembered `Lookup` tracks facts) calls `EvictionTracker.Forget` for one it holds
  nothing for any more, or every key ever tracked keeps a deadline record until the
  group closes. A deadline is set when its key or record is written, so state written
  before an operator had deadlines never falls due: `StateManifest.Problems` refuses a
  `{operator}.deadlines.w{n}` store that is new beside committed state of that operator —
  do not turn that into "new stores are fine". An event-time selector run outside a
  `WatermarkOperator` (the keyed input's `evictWhenFinal` deadlines) has its own guarded
  loop under `DeadLetter` and reports nothing: the declaration downstream reports the
  record once. A checkpointing `IStorageProvider` must implement
  `CreateDeadlineStore`; a changelog follower that holds the store typed keeps its order
  as it applies records, so a takeover rebuilds nothing — and a follower only ever
  writes the store, so a deadline store must bound its own order at `Set` and `Remove`,
  not only when `TakeDue` drains it (`DeadlineStoreTests`). `EventTimeRestartTests` is the
  check: killed and restored at any step, a circuit's outputs and final state equal the
  uninterrupted run's.
- Event time where records enter (`Runtime/InputEventTime.cs`, `WatermarkClock`;
  docs/design/enrichment-service.md, S9). `WithEventTime(sel, lateness)` declared on an
  input itself binds the input's port to the clock: the port feeds the tracker as it
  drains — on the coordinator, between steps — and the declaration's operator only
  filters (it is built with tracker −1). Two things live there and nowhere else:
  - *A time per partition.* A source names its partitions (`ExpectPartitionAsync`) and
    says when it stands at the end of one (`IdlePartitionAsync`); these travel in the
    input's channel, in order with the records, and are read before `Admit` (they have
    no offset). The tracker's time is the least over the named partitions that are not
    idle; a named partition with no record yet holds every claim back; with none
    counting it is the newest seen anywhere. Records of partitions nobody named go to
    the tracker's own slot, as all records did. The partitions' times and idle flags
    commit under `$watermark#s{n}#m{p}` and `$watermark#s{n}#i{p}` — two keys whose
    names never change, because a provider may keep the marker by setting keys and
    removing none (Redis does): do not put a flag in a key's name. State manifest
    format 8; a format-7 checkpoint resumes as it is. Never make a partition idle on
    the passing of time: a source says so only on evidence that it is at the end
    (`KafkaSource.PartitionEnds`: on an end-of-partition answer, after
    `IdlePartitionAfter`), takes it back when the evidence stops coming, and names
    every partition again at every start — what a checkpoint says of a partition's end
    was true when it was written. Clustered circuits are not bound (one time per
    source, shared between processes).
  - *The bound on a time in the future* (`CircuitOptions.MaxEventTimeAhead`,
    `ServiceOptions.MaxEventTimeAhead`, `circuit.maxEventTimeAhead`). A record beyond it
    is refused at the port, like a record its source could not read: offset consumed,
    dead letter or fault. It must stay at the port. The verdict depends on the node's
    clock, so judged inside the circuit an insertion and its later retraction could be
    judged differently; a record refused at the port never enters, and a keyed input's
    refused upsert is never the key's row. Do not move it into `WatermarkOperator`.
  A definition that declares event time after a `Select` or a lookup gets neither: the
  example's definitions declare on the input and re-declare (`WithEventTime(sel)`)
  after reshaping. `PartitionWatermarkTests`, `KafkaPartitionWatermarkTests` (two of
  its tests need the `kafka3` profile, as the service's broker tests do) and the
  `+partitions` cases of `EventTimeRestartTests` are the checks.
- A keyed input that forgets its final rows judges a returning key by the records' own
  times (`RecordEviction.Supersedes`): an upsert later than the held row by more than
  the lateness is a new record — `UpsertOperator` emits no retraction for the held row —
  and `KeyedInputPort.DrainInto` peeks before it reads and ends the drain before such
  an upsert of a key it has already drained, so the two are never in one step (a delta
  has no order; steps are the only order a sink sees). Both halves are needed: without
  the cut a step's commands collapse to the last per key and the earlier record is
  never delivered. Whoever steps a circuit by hand steps until `HasPendingInput` is
  false (`PipelineTestHarness.StepAsync` does). Within the lateness an upsert is an
  amendment, and no rule tells a reused key from one there: the guide says ids must
  not come back within the open window. An upsert that is itself behind the watermark
  is not applied (`UpsertOperator.IsLate`): it is emitted alone, for the declaration to
  drop and count, and the key's row stays — applied, it would retract a row that may
  still be open and insert one that is dropped.
- Per-record eviction (`EvictWhenFinal()` → `FinalizingGroupedStream`; docs/design/shared-tables.md,
  *Per fact*). A record's deadline is one tick after its own event time (the watermark
  keeps ticks ≥ it, `TakeDue` takes ≤ it), tracked by (key, record) in an
  `EvictionTracker`, removed with `IIndexedWeightStore.RemoveEntries`. It is offered only
  on an event-time grouping and only by operators that honour it — the table `Join`,
  `LeftJoin` (both views) and `Lookup`; do not hang it on an aggregate or a stream join.
  An operator that applies table changes before it drains must skip final records
  (`RecordEviction.IsFinal`), or a change corrects them in the step that first sees the
  watermark. A keyed input that forgets final rows (`WithEventTime(…, evictWhenFinal:
  true)`) owes no retraction for them only because its declaration drops it: `Build()`
  refuses any other reader of the input; keep it so. `WithEventTime(sel)` without a
  lateness re-declares time and registers no clock source.
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
    (`{prefix}{step:D19}-{term:D6}-{base:D19}.jsonl`, snapshots `….snapshot.jsonl`;
    the older `{step}-{term}.jsonl` is still read), the reader's rules and line shapes
    (`FileSinkFormat`), and the
    source's position in the checkpoint's partition → offset map (`FilePosition`:
    partition 0 the line, 1 the name's length, 2… the name in two banks, each offset
    prefixed by the object's ordinal so that it only rises). Change either and deployed
    pipelines lose their place: a new connector name, not an edit.
  - A `files` input never reads what a `files` sink writes: a sink rewrites a step
    under its name, folds and prunes its objects, and a source follows names. It is
    refused in either order through `ConnectorDescription.Reads` against `Owns`, at the
    dry run and under the references lock. Do not relax it without giving the sink a
    commit marker a streaming reader can follow. The source's `settle` never compares
    the node's clock with the store's: the store's stamps against each other, or the
    reader's own time since it first saw the object.
  - A table loader says `Waiting` only for a table that has never had a version: what
    a loader run has loaded itself is not evidence, since a takeover and a restart begin
    a run with nothing.
  - Dead letters with a sink bound (`PipelineSpec.DeadLetters`) are an acknowledged
    output: nothing acknowledges a batch before the sink has it, and a sink that fails
    faults the pipeline — never drop to the ring and carry on.
  - `PipelineTestHarness` builds a definition through the same `DefinitionBinding` and
    `PipelineBuilder` as the supervisor; keep it so, or its tests stop meaning what a
    deploy would do. Its `StateSize` is the checkpoint store's committed count, by store;
    every example definition has a test that its state returns to a bound under a long feed.
  - A pipeline whose state only grows is flagged (`StateGrowth`,
    `ServiceOptions.StateGrowthAlarmAfter`): from the entry count the sampler already
    reads, a fixed ring per pipeline — never by walking a store. It flags; it does not
    change health or stop anything (Open for James 70).
  - A table, plugin or pipeline that fails is that thing's failure: shown, retried, never
    thrown out of a takeover callback. Only the catalog and the storage may fail a node.
  - Nothing waits without a bound, and nothing holds another pipeline's queue: teardown is
    `StopTimeout`, then abandon, then `Wedged`; a connector's `StartAsync` and `ResetAsync`
    are tenant code and get `ConnectorTimeout`.
  - What a status, the admin API or a gauge reads must not wait on a broker. A following
    table's lag is the one figure that asks the log (`TableHandle.StatsAsync`): the
    question gets `TableRegistry.LagPatience` (2 s) whether or not the log's call
    honours the token, and after one without an answer nobody asks for
    `LagAskAgainAfter` (5 s) — for any table. The lag is then null ("not known"),
    never a wait: with two of three brokers dead a standby's status once took thirty
    seconds. A new figure that has to ask the backend gets a bound the same way.
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
    and the closed sets `state`, `role`, `stage`, `part` — nothing unbounded, nothing from a
    record. Gauges read the snapshot a node refreshes once a second; a callback must not
    touch a circuit, a table or the backend.
  - Discovery (`IServiceDiscovery`, the admin's `connectors`, `connections`,
    `table-sources`, definition and template endpoints) returns names, kinds and config
    schemas, never a connection's settings. Describing a definition runs its `Build`
    against stand-in tables (`PipelineBuilder` detached) and is leader-only.
  - The in-memory backend's fencing reaches the sinks: a leased sink begins no write
    unless its node is the cluster's leader, decided under the cluster's gate, and a
    takeover waits for the deposed leader's write in flight
    (`InMemoryBackendOptions.DeposedWriteTimeout`) before it prepares anything. A
    replaced leader is told at once but tears its circuits down a moment later; without
    the lease its last rows land over the new leader's (`InMemoryFencingTests`).
  - The in-memory backend must stay bounded: the state log compacts itself
    (`StateCompactAbove`), table logs cap history by row changes, and a tombstone is
    dropped only once every enumerating reader has passed it.
  - The state binding: a pipeline's typed stores belong to one plugin version. Changing
    the version, or anything else that changes what the state means, goes through
    `ChangelogStorageProvider.ReleaseStores()` first.
  - A standby's build (`WarmAsync`) is made from the one spec it read at its start:
    `Bind` and `Build` take the spec and never read `runtime.Spec` again, because the
    node's loop replaces that without the pipeline's queue. What it built is good only
    while the spec generation, the layout, the store epoch and `TableReopens` (counted
    from before the build) are what they were.
  - `Previous` on a stored spec is the spec that wrote the state. It stays through
    every deploy until a generation has run, whatever that deploy changes.
  - A slot's secrets are what `Describe` names and what the connector's `ConfigSchema`
    marks; `Secrets(config)` is the one place that says which. A sink's claim
    (`ConnectorDescription.Owns`) is checked at the dry run and again under the
    references lock — never in `Build`, where two stored specs would stop each other.
  - Objects are written through `IObjectStore.WriteAsync`: whole when the callback
    returns, nothing when it throws. A source or a sink's clean-up lists with
    `ListPageAsync` (after a name, a page at a time), never the whole prefix per poll; a
    source holds back behind an object younger than `settle`. A missing input is a
    warning and a `Waiting` state, never a refusal or a fault of the files paths. The `files` source holds one line of at most
    `maxLineLength` characters; its ordinals only rise, past anything a checkpoint may
    hold of a half-sent name (`FilePosition.Next`).
  - `ITableSourceFactory.Create` and `ITypedConnector.RequireTypes` run at dry runs:
    they open nothing. The binding has a name,
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
    leave, and a service event's text after its first colon counts as a message. The
    notes of a definition's description, template and diff follow the same convention:
    a note that quotes an exception does so after its first colon and has no other.
  - The UI is seven files in `wwwroot`, no build step, no dependency, no request to another
    origin, and a content security policy without `unsafe-inline`: no inline script, no
    `style` attribute (set styles through the CSSOM), no `innerHTML` with data. Every
    render goes through `morph`, and handlers are `on:` properties found by the delegated
    listener — `addEventListener` on a rendered node is lost at the next patch. A path the
    scripts call must be a route the API maps: `The_UI_is_served_whole…` checks it.
    `morph` sets form controls' properties (selection, `checked`, an unfocused
    field's `value`) from the new tree: `AdminUiMorphTests` runs `Ui/morph-tests.js`
    in a headless Chrome and is the check.
    Every table with a header row is drawn by `grid` (`app.js`), which takes each column
    once: its label is the header's text and every cell's `data-label` (the phone
    layout's label), so the two cannot disagree. Do not build a `thead` by hand.
    `AdminUiTablesTests` runs `Ui/tables-check.js` in headless Chrome against a two-node
    AdminHost with data and fails on a row whose cells do not match its headers in
    number or name, on the leader and on the standby. A DOM node made once and placed
    in many cells lands in the last one only: make it per cell.
    A field whose text may not be a value yet (JSON for an array or object setting)
    keeps the text as a draft, and a form with a draft on screen is not sent; a dialog
    is closed when the route moves to another page.
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
