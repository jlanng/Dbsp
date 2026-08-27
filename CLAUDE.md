# Incremental

DBSP-style incremental stream processing library for .NET (net10.0, SDK pinned via mise.toml).

## Commands
- Build: `dotnet build`
- Test: `dotnet test tests/Incremental.Tests` (Redis tests skip unless `docker compose up -d` has run)
- Examples: `dotnet run --project examples/Incremental.Example`,
  `dotnet run --project examples/Incremental.LogAnalytics [-- --redis]`
- Benchmarks: `dotnet run -c Release --project benchmarks/Incremental.Benchmarks -- --filter '*'`
  (BenchmarkDotNet); `-- --assert` runs the CI performance contracts — scaling-ratio
  assertions (delta cost vs state size), which is what `.github/workflows/ci.yml` gates on

## Architecture invariants
- Streams carry `ZSet<T>` **deltas** (value → signed weight); operators must be fully
  incremental and honor negative weights (retractions) everywhere.
- Execution is step-based. All workers execute all nodes in the same topological order each
  step; exchange nodes rely on this for barrier alignment (`Circuit.RunWorkerStep`).
- Operator state is sharded per worker and never shared across threads. Any operator whose
  state is keyed must sit downstream of an exchange on that key (`IndexBy` inserts it;
  `Distinct` shuffles by value) — adding a stateful operator without this breaks
  multi-worker correctness silently.
- Backpressure comes only from bounded channels (inputs, outputs). Never buffer unboundedly.
- Public API is fluent delegates, intentionally not LINQ/expression trees.
