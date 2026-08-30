# Incremental

DBSP-style incremental stream processing library for .NET (libraries multi-target
net8.0/net10.0; tests/examples net10.0; SDK pinned via mise.toml).

## Commands
- Build: `dotnet build`
- Test: `dotnet test tests/Incremental.Tests` (Redis tests skip unless `docker compose up -d` has run)
- Examples: `dotnet run --project examples/Incremental.Example`,
  `dotnet run --project examples/Incremental.LogAnalytics [-- --redis]`
- Showcase (the all-features demo): `dotnet run --project examples/Incremental.Showcase`
  serves a web app on http://localhost:5170 (`--port`, `--no-open`, `--redis`, `--cluster`,
  `--workers N`); `-- --headless` runs every tab unattended and exits non-zero if a verified
  act fails — that is the smoke test. Web-only: `Microsoft.NET.Sdk.Web` costs no NuGet
  packages. Tab 5 measures worker-count scaling and can relaunch the process into another
  topology; run it `-c Release` if the throughput column is the point (Debug flattens it).
- Benchmarks: `dotnet run -c Release --project benchmarks/Incremental.Benchmarks -- --filter '*'`
  (BenchmarkDotNet); `-- --assert` runs the CI performance contracts — scaling-ratio
  assertions (delta cost vs state size), which is what `.github/workflows/ci.yml` gates on

## Architecture invariants
- Streams carry `ZSet<T>` **deltas** (value → signed weight); operators must be fully
  incremental and honor negative weights (retractions) everywhere.
- Execution is step-based. All workers execute all nodes in the same topological order each
  step; exchange nodes rely on this for barrier alignment (`Circuit.RunWorkerStep`).
- Operator state is sharded per worker and never shared across threads. Any operator whose
  state is keyed must sit downstream of an exchange on that key (`GroupBy` inserts it;
  `Distinct` shuffles by value; windowed groupings route by the user key, and
  window-collapsing regroups — windowed `TopK`, window-sticky `GroupBy` — re-exchange by
  the window) — adding a stateful operator without this breaks multi-worker correctness
  silently.
- Exchange routing must be process-stable: route by `StableHash` (or a user-supplied
  stable hash), never `GetHashCode` — .NET randomizes string hashes per process, which
  silently breaks cluster routing and multi-worker checkpoint restore.
- Checkpointed state identity is covered by the state manifest (committed with every
  checkpoint, validated at `Start()`); store names are positional unless pinned with
  `.Named()` — changing store naming or the manifest format needs a format-version bump.
- Backpressure comes only from bounded channels (inputs, outputs). Never buffer unboundedly.
  This includes outputs: stream-shaped outputs never maintain integrated (snapshot) state —
  only table-shaped (keyed/windowed) outputs may, and theirs is bounded by eviction.
- Public API is fluent delegates with LINQ-dialect names (`Select`/`Where`/`GroupBy`),
  intentionally not LINQ/expression trees. The 0.2 surface is specced in `docs/intro.md`:
  windows exist only on groupings of an `EventTimeStream`; grouping types are staging types
  (no output/projection); outputs are unnamed definitions redeemed via `circuit.Handle`;
  table-shaped outputs bootstrap consumers automatically (batch zero = current state).
