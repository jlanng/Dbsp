# Incremental

An incremental stream processing library for .NET, built on the ideas of
[DBSP](https://arxiv.org/abs/2203.16684) (the theory behind Feldera): every collection is a
**Z-set** (values with signed weights), every stream carries **deltas**, and every operator
— including joins and aggregations — processes only the changes, never the full state.

```csharp
var builder = new CircuitBuilder(new CircuitOptions { Workers = 4 });

var orders    = builder.Input<Order>("orders");
var customers = builder.Input<Customer>("customers");

orders.Filter(o => o.Amount > 0)
      .IndexBy(o => o.CustomerId)
      .Join(customers.IndexBy(c => c.Id),
            (_, order, customer) => new RegionAmount(customer.Region, order.Amount))
      .IndexBy(x => x.Region)
      .Sum(x => x.Amount)
      .Output("revenue");

await using var circuit = builder.Build();
circuit.Start();

var ordersIn = circuit.Input<Order>("orders");
var revenue  = circuit.Output<KeyedValue<string, decimal>>("revenue");

await ordersIn.InsertAsync(new Order(101, custId: 1, amount: 60m));
await ordersIn.RemoveAsync(new Order(101, custId: 1, amount: 60m)); // cancelled — retracts downstream

await foreach (var delta in revenue.ReadAllAsync())
    Console.WriteLine(delta); // e.g. {(emea, 40) => +1, (emea, 100) => -1}
```

The API is a plain fluent builder over delegates — deliberately **not** LINQ: no expression
trees, no `IEnumerable`/`IQueryable` contract, nothing that constrains what operators can
exist or how they execute.

## Concepts

**Z-sets and weights.** A `ZSet<T>` maps values to signed 64-bit weights. Weight +1 is an
insertion, -1 a deletion; weights add, and zero-weight entries vanish. Because deletion is
just data, retraction flows through the entire pipeline for free: cancel an order and the
join output, the group's `SUM`, and the group's `MAX` all correct themselves incrementally.

**Circuits and steps.** `CircuitBuilder` declares a dataflow DAG; `Build()` freezes it into
a `Circuit`. Execution is step-based: each step drains a batch from the inputs, pushes the
resulting deltas through every operator in topological order, and publishes per-output
deltas. Integrating an output's deltas reproduces the current state of that view.

**Incremental operators.**

| Operator | Incrementalization |
| --- | --- |
| `Map` / `Filter` / `FlatMap` / `Plus` / `Minus` / `Negate` | Linear — applied to the delta directly, stateless |
| `Join` | Bilinear: Δ(L ⋈ R) = ΔL ⋈ R + (L + ΔL) ⋈ ΔR, probing integrated per-side state |
| `Count` / `Sum` (`AggregateLinear`) | Linear aggregates: folded straight from the delta, state is one (row count, accumulator) pair per key — no group contents stored, update cost independent of group size |
| `Min` / `Max` | Tournament tree per key: values are hash-partitioned into leaf buckets, each level above holds its children's extrema, the root's extremum is the result. Updates — deleting the current minimum included — touch a fixed number of ≤16-entry buckets and propagate up only while a bucket's extremum changes, so cost is independent of group size |
| `Aggregate` (`Fold`, custom) | Integrated group state per key; recomputes touched groups, retracts the stale result (-1) and asserts the new one (+1). Correct for any non-invertible aggregate under deletion |
| `Distinct` | Integrated weight per value; emits only presence transitions |
| `TopK` | Integrated group per key; emits only membership changes (displaced rows retracted). A threshold short-circuit skips the re-rank whenever a key's delta rows all fall strictly below the current K-th entry, so steady-state cost is O(delta) even for huge groups. Multiplicities count toward K |
| `AntiJoin` | Integrated left side plus per-key right-side row counts; a key whose match-presence flips retracts or re-asserts its left rows |
| `LeftJoin` / `FullJoin` | Pure composition: inner join ⊕ mapped anti-join(s) — the Z-set algebra makes the union incremental for free |
| `TumblingWindow` / `HoppingWindow` | Pure data transformation (flat-map): records join every window covering their event time; keying by `(window, …)` turns any aggregate into a windowed one. Late and deleted events correct their windows via ordinary deltas |
| `SessionWindow` | Stateful, unlike the fixed windows: integrated events per session key, re-sessionized per touched key (never a full rescan). Rows whose session bounds change are retracted and re-asserted, so a bridging or late event merges sessions and a deletion shrinks or splits one — downstream aggregates correct themselves via the ordinary deltas |
| `Recurse` | Fixed-point subcircuit (DBSP recursion), iterated to convergence within each step; the loop's `Join`/`Distinct` state is profiled by inner iteration (incremental in both time dimensions), so the emitted delta is exactly the change between successive fixed points — retractions included, cyclic derivations unwound in well-founded order |

**Watermarks and eviction.** Correctness never depends on watermarks here — late events
just produce correcting deltas. Watermarks serve one purpose: bounding state.
`WithWatermark(v => v.At, allowedLateness)` declares an event-time source; the circuit's
watermark advances to (max event time seen − lateness), min across sources, frozen between
steps so all workers agree, and events below it are dropped. Downstream,
`.WithEviction(key => closesAt)` on a keyed stream lets aggregations and TopK delete a
key's state once the watermark passes its deadline — already-emitted results stay final,
and a windowed pipeline runs forever in constant memory:

```csharp
views.WithWatermark(v => v.At, allowedLateness: TimeSpan.FromSeconds(2))
     .TumblingWindow(windowSize, v => v.At)
     .IndexBy(w => (Window: w.WindowStart, Page: w.Value.Page))
     .WithEviction(key => key.Window + windowSize)
     .Count();
```

**Session windows.** `.SessionWindow(gap, v => v.At, v => v.UserId)` groups each key's
events into sessions separated by at least `gap` of event-time inactivity; a session spans
[first event, last event + gap), and an event exactly at a session's end starts a new one.
Session assignment can't be a flat-map — which events share a session depends on which other
events exist — so the operator sits downstream of an exchange on the session key (inserted
automatically, like `IndexBy`) and re-sessionizes a key's integrated events whenever a step
touches it, retracting the rows whose bounds changed. Keying the result by
`(key, start, end)` turns any aggregate into a per-session one, and with a watermark a key's
event state is dropped once the watermark passes its last event + gap:

```csharp
clicks.WithWatermark(c => c.At, lateness)
      .SessionWindow(TimeSpan.FromMinutes(30), c => c.At, c => c.UserId)
      .IndexBy(s => (User: s.Key, Start: s.Value.WindowStart, End: s.Value.WindowEnd))
      .WithEviction(s => s.End)
      .Count(); // clicks per session, corrected as sessions extend, merge, or split
```

**Window-close notifications.** `.WindowCloses()` on a keyed stream is a first-class stream
that announces each key exactly once — with weight +1, never retracted — when the watermark
passes its deadline (the attached eviction policy's, or an explicit
`.WindowCloses(key => closesAt)`). Closed means what it means for eviction: no further
events for the key can be admitted, so its results are final. The notifications stay
partitioned by key (the worker owning a key announces it), so joining them with the
window's aggregate publishes finalized results in the same step the state is evicted:

```csharp
var windowed = views.WithWatermark(v => v.At, lateness)
                    .TumblingWindow(windowSize, v => v.At)
                    .IndexBy(w => w.WindowStart)
                    .WithEviction(window => window + windowSize);
var counts = windowed.Count();
windowed.WindowCloses()
        .Join(counts, (window, closedAt, count) => (window, count))
        .Output("finalCounts"); // one (window, count) row per window, once it is final
```

**Recursion (fixed points).** `.Recurse(body)` closes a stream over itself and computes the
least fixed point `X = Distinct(seed ∪ body(X))` — the DBSP recursive circuit, with the
stream it is called on as the seed and `Distinct` as the built-in stabilizer. Transitive
closure is the classic shape:

```csharp
var closure = edges.Recurse(paths =>
    paths.IndexBy(p => p.To)
         .Join(edges.IndexBy(e => e.From), (_, p, e) => (p.From, e.To)));
```

Within every outer step the subcircuit iterates to convergence — a feedback edge (z⁻¹ over
iterations) circulates the loop delta, outer streams referenced by the body enter at
iteration 0 only (δ0), and the loop stops when no delta circulates on any worker — then
emits the net change of the new fixed point versus the previous step's, so downstream sees
ordinary incremental deltas. The stateful operators inside a loop (`Join`, `Distinct`) are
incremental in *both* time dimensions: their state remembers at which iteration each row's
weight arrived, which is what makes deletions unwind derivations in well-founded order — 
deleting an edge retracts exactly the paths with no remaining acyclic derivation, even when
paths "support themselves" around a cycle. Workers iterate in lockstep (the convergence
decision is a global all-workers reduction on the exchange barrier), so the body's joins
shuffle correctly mid-loop, and a `maxIterations` guard (default 10 000) faults the circuit
if a non-monotone body never converges. Bodies may use the linear operators plus
`IndexBy`/`Join`/`Distinct`; aggregations and the like are rejected at definition time.

**Exchange (multi-threading).** `CircuitOptions.Workers = N` runs N worker threads, each
owning its own instance of every operator and its own shard of all operator state.
`IndexBy` inserts an *exchange* operator — the DBSP/timely-dataflow shuffle — which
hash-partitions each delta by key and redistributes it across workers mid-step (barrier
synchronized), so joins and grouped aggregations are correct and scale with the worker
count. `Distinct` shuffles by value automatically. There is no locking on operator state:
one shard, one thread. The engine adapts to the worker count: with `Workers = 1` steps
run inline on the coordinator (no worker thread, no handoff — hundreds-of-µs step
latency for small workloads), while multi-worker exchanges synchronize on a spinning
barrier rather than kernel block/wake, since mid-step the workers are already hot.

**Cross-process exchange (clustering).** The same shuffle spans machines, behind the same
operator API: the circuit definition is built unchanged on every process — user code
(`IndexBy`/`Join`/aggregations) never learns the cluster exists — and only
`CircuitOptions.Cluster` declares the process's place in a static topology. Each of the N
processes hosts a contiguous slice of the global worker set (process p owns global workers
[p·Workers, (p+1)·Workers)), every exchange hash-partitions by key across all Workers × N
of them, and shards bound for a remote worker are serialized (the storage serializer's
JSON — a delta batch is just (value, weight) pairs, retractions included) and shipped as
length-prefixed frames over a transport: `TcpExchangeTransport` (plain sockets, no
dependencies; each process gets the shared endpoint list and its own index, connections are
retried until the mesh is up) or `InMemoryExchangeTransport` for tests.

```csharp
// process 0 of 2 — process 1 runs the identical code with processIndex: 1
var options = new CircuitOptions
{
    Workers  = 4,                     // local workers; the cluster has global workers 0..7
    AutoStep = false,                 // clustered circuits step explicitly, in lockstep
    Cluster  = new ClusterOptions
    {
        Transport = new TcpExchangeTransport(
            processIndex: 0,
            endpoints: ["hostA:9000", "hostB:9000"]),
    },
};
```

Steps are lockstep: every process calls `StepAsync` for every step (hence
`AutoStep = false`), each draining its own inputs — an input is declared everywhere and may
be fed on any process; feed each source where its producer lives. A step aligns at two
points. Mid-step, a worker cannot proceed past an exchange until every peer's shards for
that (step, exchange) have arrived; empty shards are still sent, so the per-peer frame
sequence is dense and alignment never depends on data being present — frames arrive in send
order, which is the same deterministic node order on every process, and a mismatch faults
loudly as a desync instead of merging the wrong step's data. At the end of the step the
processes exchange step-end frames carrying their watermark observations, so the frozen
watermark is the same global min-reduction everywhere and eviction/window-close decisions
stay consistent cluster-wide. Output handles carry the deltas the local workers produced;
consume every process's handle (or key the final operator so the rows you care about land
on one process) for the full stream. Backpressure crosses the wire too: send queues are
bounded, so a slow peer — or its slow output consumer — stalls the cluster's step instead
of buffering unboundedly.

A peer disconnecting or faulting poisons the local circuit through the ordinary fault path
(a descriptive exception on the step, the handles, and `Circuit.Fault`) — never a hang.
Checkpointing stays per process: each commits its own shards' state and its own inputs'
offsets, under keys carrying the process index plus a step marker, so processes sharing one
physical store never clobber each other; lockstep steps and equal `CheckpointInterval`s
mean every process commits the same step boundaries, and after a crash all processes must
resume from the same committed step — compare `Circuit.CommittedStep` across processes
before feeding data (there is deliberately no distributed commit protocol). `Recurse` is
not supported in clustered circuits — the per-iteration convergence decision would need a
cross-process round trip every loop iteration — and is rejected at `Build()`.

**Backpressure.** Everything between producer and consumer is a bounded
`System.Threading.Channels` channel:

```
producer --await InsertAsync--> [bounded input channel] --drain--> step --publish--> [bounded output channel] --> consumer
```

A slow consumer fills its output channel, which stalls step publishing, which stops input
draining, which fills the input channels, which suspends producers inside `InsertAsync`.
No configuration, no dropped data.

**Faults.** When any worker throws mid-step (a user delegate, a storage error), the fault
propagates cooperatively instead of poisoning the circuit: the faulting worker poisons the
exchange barrier, so peers blocked mid-shuffle — recursion-loop iterations included — bail
out of the step rather than deadlock, and the circuit moves to a terminal faulted state
within that step. The faulting `StepAsync` throws the original exception (an
`AggregateException` when several workers fault in the same step), consumers' `ReadAllAsync`
observes it after the pre-fault deltas, producers blocked in `InsertAsync` are released with
a `CircuitFaultedException` (original fault as its inner) instead of hanging, and
`Circuit.Fault` exposes it; `DisposeAsync` of a faulted circuit completes quietly. A faulted
circuit's in-memory state is unusable — the faulting step is half-applied — but that step is
never committed, so with a checkpointing storage provider the last committed checkpoint is
intact: rebuild the circuit against the same provider to recover exactly-once (below).

**Keyed inputs.** `builder.KeyedInput<K,V>(name, keySelector)` declares an input with
upsert/delete-by-key semantics: the handle tracks the current row per key, so
`UpsertAsync(row)` emits the retraction of the previous version automatically and
`DeleteAsync(key)` retracts the current one. This is the natural way to feed reference /
master data (and any stream whose rows are amended in place) — one upsert to a reference
row re-attributes everything joined against it.

**Storage.** Stateful operators keep their state behind `IStorageProvider`
(`IWeightStore`, `IIndexedWeightStore`, `IValueStore`), created per operator *per worker*
so implementations never see cross-thread access to one store.

- `InMemoryStorageProvider` (default) — heap dictionaries.
- `RedisStorageProvider` (`Incremental.Storage.Redis`) — state in Redis hashes; survives
  restarts, can exceed the heap, and checkpoints (below):

  ```csharp
  var redis = await ConnectionMultiplexer.ConnectAsync("localhost");
  var options = new CircuitOptions { Storage = new RedisStorageProvider(redis) };
  ```

- Candidates that make sense next: RocksDB (large state, embedded, fast scans), SQLite.

**Checkpointing and exactly-once restart.** A provider implementing
`ICheckpointStorageProvider` (Redis does; `InMemoryCheckpointStorageProvider` is the
in-process stand-in for testing) makes checkpoints of the circuit: operators stage their
writes in per-store overlays, and every `CheckpointInterval` steps (default: every step) the
circuit commits {all staged state changes + the input offsets those steps consumed}
atomically — for Redis, one MULTI/EXEC transaction covering every worker's shard plus a
`{prefix}:checkpoint` offsets marker, issued only after all workers have finished their
steps. Either the whole checkpoint lands or none of it does, so durable state and offsets
always agree, even across a crash mid-commit. Producers can stamp records with source
offsets — `SendAsync(value, weight, offset)`, e.g. a Kafka partition offset; unstamped
records are numbered as they are consumed. After a crash, rebuild the circuit against the
same provider: the state is already there, `circuit.CommittedOffset("orders")` says where
each input got to, and replayed records stamped at or below the checkpoint are dropped on
arrival — resuming a log-style source is exactly-once from the state's point of view.
(Output deltas are transient and not checkpointed: consumers re-derive views by
integrating, they don't replay.)

**Stepping modes.** By default a pump task steps the circuit whenever input is available.
With `AutoStep = false` the circuit advances only on explicit `StepAsync()` calls — exact,
deterministic step boundaries for tests and batch jobs.

## Project layout

```
src/Incremental                    core: Z-sets, builder API, operators, windows, runtime, in-memory
                                       storage, cluster transports (TCP + in-memory)
src/Incremental.Storage.Redis      Redis-backed IStorageProvider
tests/Incremental.Tests            xunit: Z-set algebra, per-step delta semantics, multi-worker runs,
                                       Redis integration (auto-skipped when Redis is down)
examples/Incremental.Example       joined + aggregated order analytics with live retraction
examples/Incremental.LogAnalytics  simulated clickstream → 5s tumbling windows → counts → live top-3
                                       per window, rendered as an in-place Spectre.Console dashboard
                                       (bar gauges per window, watermark in the header);
                                       `--redis` keeps operator state in Redis
examples/Incremental.TradeEnrichment trades ⋈ reference data, composed interactively: pick which
                                       enrichment columns to join (grouped by table: instrument;
                                       trader → desk, a two-hop join; counterparty) and which
                                       dimensions to group by — no dimensions gives a top-trades
                                       blotter instead. Every input is a keyed upsert input:
                                       amendments, cancellations, and master-data changes (desk
                                       moves, re-ratings) are single UpsertAsync/DeleteAsync calls
                                       that re-attribute historical trades in microseconds
benchmarks/Incremental.Benchmarks  BenchmarkDotNet suite (Z-set primitives, stateless pipeline,
                                       join / sum / min / top-k delta cost vs state size), plus
                                       `-- --assert`: CI performance contracts that check scaling
                                       ratios (delta cost must not grow with state size) instead of
                                       wall-clock numbers, so they hold on noisy shared runners
examples/Incremental.TaxiAnalytics genuine big data: ~13M real NYC yellow-taxi trips (parquet, read
                                       via Parquet.Net, downloaded and cached on first run, ~50MB/month;
                                       `--months N`, `--synthetic`) through join + counts + revenue +
                                       top-5 zones per borough; narrated phases, then continuous
                                       single-row churn with a live view until you press a key —
                                       ~1M rows/s bulk load; single-row updates in ~250 µs,
                                       thousands of × cheaper than a full recompute
```

## Benchmarks

```sh
dotnet run -c Release --project benchmarks/Incremental.Benchmarks -- --filter '*'   # full BenchmarkDotNet suite
dotnet run -c Release --project benchmarks/Incremental.Benchmarks -- --assert      # CI performance contracts
```

The `--assert` mode is what CI gates on. Shared runners are too noisy for absolute timing
assertions, so each contract runs an identical fixed-size delta against a small and a 64×
larger preloaded state (interleaved trials, medians) and fails if the large/small ratio
exceeds a generous bound (1.5× allocations, 5× time). A regression from O(delta) to O(state)
shows up as a ~64× ratio, so the check is high-signal without being flaky. The full suite
runs on demand via the *Benchmarks* GitHub workflow (`workflow_dispatch`), which uploads
the BenchmarkDotNet artifacts.

## Running with Redis

```sh
docker compose up -d        # starts Redis on localhost:6379
dotnet test                 # Redis integration tests now run instead of skipping
dotnet run --project examples/Incremental.LogAnalytics -- --redis
```

## Status / roadmap

The semantics above are implemented and tested, and the roadmap below is fully ticked —
what remains is headroom (more storage backends, more operators, cluster tooling), not
missing semantics:

- [x] Linear-aggregate fast path: `COUNT`/`SUM` maintained from the delta alone with O(keys)
      state (`ILinearAggregate`); custom linear aggregates via `AggregateLinear`
- [x] Tournament trees for `MIN`/`MAX` over large groups (`Min`/`Max` maintain a
      hash-partitioned reduction tree in ordinary indexed stores, so it works over any
      storage provider; deleting the extremum no longer rescans the group)
- [x] Batched/pipelined Redis access (operators drive batched store operations, so a step
      costs a handful of pipelined round trips instead of one per state touch)
- [x] Checkpointing input offsets with state for exactly-once restart (staged writes
      committed atomically with consumed offsets — Redis: one MULTI/EXEC per checkpoint;
      producers stamp records with source offsets, replays below the checkpoint are
      dropped; `InMemoryCheckpointStorageProvider` for testing the semantics)
- [x] `TopK` (per-key ranking with retraction of displaced rows)
- [x] Tumbling and hopping event-time windows (assignment; late data handled by deltas)
- [x] Watermarks: late-event filtering and eviction of closed-key state (aggregate/TopK)
- [x] Anti-join, left and full outer joins
- [x] Window-close notifications (`WindowCloses`: a keyed stream announcing each window
      final, exactly once, when the watermark passes its deadline; joins with the window's
      aggregate to publish finalized results)
- [x] Watermark-driven eviction for join/anti-join/distinct state (keyed `Distinct`
      keeps its state grouped by key so the policy applies)
- [x] Session windows (per-key inactivity gap; a bridging or late event merges sessions and
      a retraction splits them, all via ordinary deltas; key state evicted once the
      watermark passes last event + gap)
- [x] Recursive/iterative circuits (DBSP fixed points) for transitive closure-style queries
      (`Recurse`: per-step fixed-point iteration with a hard-wired `Distinct` stabilizer;
      loop `Join`/`Distinct` state is iteration-profiled so retractions unwind cyclic
      derivations correctly; lockstep multi-worker iteration with a global convergence
      barrier; `maxIterations` guard)
- [x] Cooperative fault propagation across workers (a fault poisons the exchange barrier so
      peers bail out of the step instead of deadlocking; the circuit becomes terminally
      faulted within the step — original exception on the step, the output channels, and
      `Circuit.Fault`; producers released with `CircuitFaultedException`; the committed
      checkpoint untouched, so rebuild-from-checkpoint is the recovery story)
- [x] Cross-process exchange (workers on different machines) behind the same operator API
      (`CircuitOptions.Cluster`: N processes × M workers as contiguous slices of one global
      worker set; exchanges hash-partition across all of them and ship remote shards as
      length-prefixed frames over a transport — TCP over plain sockets, in-memory for
      tests; explicit lockstep steps with dense per-exchange frame alignment, globally
      min-reduced watermarks, cross-process fault propagation, per-process checkpoints
      keyed by process index; `Recurse` + cluster is rejected at build time)
- [ ] Build all singing, all dancing demo showing all features
- [ ] Remove redis dependency from core assembly.
- [ ] Investigate off-heap in-processs storage to reduce GC
- [ ] Investigate higher order APIs with more user friendly or domain specific applications
