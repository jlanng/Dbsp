# Incremental

DBSP-style incremental stream processing library for .NET (libraries multi-target
net8.0/net10.0; tests/examples net10.0; SDK pinned via mise.toml).

## Commands
- Build: `dotnet build`
- Test: `dotnet test --project tests/Incremental.Tests` (xunit v3 on Microsoft.Testing.Platform,
  opted in by `global.json` — hence `--project`; Redis tests skip unless `docker compose up -d` has run)
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
  assertions (delta cost vs state size), which is what `.github/workflows/ci.yml` gates on
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
- Checkpointed state identity is covered by the state manifest (committed with every
  checkpoint, validated at `Start()`); store names are positional unless pinned with
  `.Named()` — changing store naming or the manifest format needs a format-version bump.
- Backpressure comes only from bounded channels (inputs, outputs). Never buffer unboundedly.
  This includes outputs: stream-shaped outputs never maintain integrated (snapshot) state —
  only table-shaped (keyed/windowed) outputs may, and theirs is bounded by eviction.
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
