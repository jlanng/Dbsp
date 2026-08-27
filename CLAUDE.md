# StreamProcessor

DBSP-style incremental stream processing library for .NET (net10.0, SDK pinned via mise.toml).

## Commands
- Build: `dotnet build`
- Test: `dotnet test tests/StreamProcessor.Tests`
- Example: `dotnet run --project examples/StreamProcessor.Example`

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
