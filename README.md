# Incremental

An incremental stream processing library for .NET, built on the ideas of
[DBSP](https://arxiv.org/abs/2203.16684) (the theory behind Feldera): every collection is a
**Z-set** (values with signed weights), every stream carries **deltas**, and every operator
— including joins and aggregations — processes only the changes, never the full state.

```csharp
var builder = new CircuitBuilder(new CircuitOptions { Workers = 4 });

var orders    = builder.Input<Order>("orders");
var customers = builder.Input<Customer>("customers");

var revenueDef = orders
    .Where(o => o.Amount > 0)
    .GroupBy(o => o.CustomerId)
    .Join(customers.GroupBy(c => c.Id),
          (_, order, customer) => new RegionAmount(customer.Region, order.Amount))
    .GroupBy(x => x.Region)
    .Sum(x => x.Amount)
    .ToOutput();

await using var circuit = builder.Build();
circuit.Start();

var ordersIn = circuit.Handle(orders);      // definitions redeem for typed handles —
var revenue  = circuit.Handle(revenueDef);  // no string lookups, no restating types

await ordersIn.InsertAsync(new Order(101, custId: 1, amount: 60m));
await ordersIn.RemoveAsync(new Order(101, custId: 1, amount: 60m)); // cancelled — retracts downstream

await foreach (var batch in revenue.Changes())
    Console.WriteLine(batch); // e.g. {(emea, 40) => +1, (emea, 100) => -1}
```

The API is a plain fluent builder over delegates. It borrows LINQ's names where the
semantics genuinely match (`Select`, `Where`, `GroupBy`, `Distinct`) but is deliberately
**not** LINQ: no expression trees, no `IEnumerable`/`IQueryable` contract, nothing that
constrains what operators can exist or how they execute. See `docs/intro.md` for the full
introduction.

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
| `Select` / `Where` / `SelectMany` / `Concat` / `Minus` / `Negate` | Linear — applied to the delta directly, stateless |
| `Join` | Bilinear: Δ(L ⋈ R) = ΔL ⋈ R + (L + ΔL) ⋈ ΔR, probing integrated per-side state |
| `Count` / `Sum` (`AggregateLinear`) | Linear aggregates: folded straight from the delta, state is one (row count, accumulator) pair per key — no group contents stored, update cost independent of group size |
| `Min` / `Max` | Tournament tree per key: values are hash-partitioned into leaf buckets, each level above holds its children's extrema, the root's extremum is the result. Updates — deleting the current minimum included — touch a fixed number of ≤16-entry buckets and propagate up only while a bucket's extremum changes, so cost is independent of group size |
| `Aggregate` (`Fold`, custom) | Integrated group state per key; recomputes touched groups, retracts the stale result (-1) and asserts the new one (+1). Correct for any non-invertible aggregate under deletion |
| `Distinct` | Integrated weight per value; emits only presence transitions |
| `TopK` | Integrated group per key; emits only membership changes (displaced rows retracted). A threshold short-circuit skips the re-rank whenever a key's delta rows all fall strictly below the current K-th entry, so steady-state cost is O(delta) even for huge groups. Multiplicities count toward K |
| `AntiJoin` | Integrated left side plus per-key right-side row counts; a key whose match-presence flips retracts or re-asserts its left rows |
| `LeftJoin` / `FullJoin` | Pure composition: inner join ⊕ mapped anti-join(s) — the Z-set algebra makes the union incremental for free |
| `TumblingWindow` / `HoppingWindow` | Pure data transformation (flat-map) on a grouping: records join every window covering their event time, and the window composes into the group's identity automatically — aggregates over the windowed grouping are windowed aggregates, with eviction derived from the window. Late and deleted events correct their windows via ordinary deltas |
| `SessionWindow` | Stateful, unlike the fixed windows: integrated events per session key, re-sessionized per touched key (never a full rescan). Rows whose session bounds change are retracted and re-asserted, so a bridging or late event merges sessions and a deletion shrinks or splits one — downstream aggregates correct themselves via the ordinary deltas |
| `Recurse` | Fixed-point subcircuit (DBSP recursion), iterated to convergence within each step; the loop's `Join`/`Distinct` state is profiled by inner iteration (incremental in both time dimensions), so the emitted delta is exactly the change between successive fixed points — retractions included, cyclic derivations unwound in well-founded order |

**Watermarks and eviction.** Correctness never depends on watermarks here — late events
just produce correcting deltas. Watermarks serve one purpose: bounding state.
`WithEventTime(v => v.At, lateness)` declares an event-time source (and carries the
timestamp in the type: windows only exist on groupings of an event-timed stream, enforced
at compile time); the circuit's watermark advances to (max event time seen − lateness),
min across sources, frozen between steps so all workers agree, and events below it are
dropped. Windowed groupings derive eviction from the window automatically — state for a
window is deleted once the watermark closes it, already-emitted results stay final, and a
windowed pipeline runs forever in constant memory:

```csharp
views.WithEventTime(v => v.At, lateness: TimeSpan.FromSeconds(2))
     .GroupBy(v => v.Page)
     .TumblingWindow(windowSize)
     .Count()                 // rows (WindowStart, WindowEnd, Page, Count); eviction automatic
     .TopK(3, r => r.Value);  // top pages within each window
```

For keyed state whose lifecycle isn't window-derived, the escape hatch is
`.EvictWhen(key => closesAt)` on any grouping.

**Session windows.** `.GroupBy(c => c.UserId).SessionWindow(gap)` groups each key's
events into sessions separated by at least `gap` of event-time inactivity; a session spans
[first event, last event + gap), and an event exactly at a session's end starts a new one.
Session assignment can't be a flat-map — which events share a session depends on which
other events exist — so the operator runs on the worker owning each key and re-sessionizes
a key's integrated events whenever a step touches it, retracting the rows whose bounds
changed. The session bounds compose into the group's identity like any window, so
aggregates are per-session automatically and a key's event state is dropped once the
watermark passes its last event + gap:

```csharp
clicks.WithEventTime(c => c.At, lateness)
      .GroupBy(c => c.UserId)
      .SessionWindow(TimeSpan.FromMinutes(30))
      .Count(); // clicks per session, corrected as sessions extend, merge, or split
```

**Emit-on-close.** Live windowed results flap by design — each update retracts the stale
row and asserts the new one. `.OnClose()` on a windowed result emits each row exactly once,
final, when the watermark closes its window, in the same step the window's state is
evicted:

```csharp
views.WithEventTime(v => v.At, lateness)
     .GroupBy(v => v.Page)
     .TumblingWindow(windowSize)
     .Count()
     .OnClose()   // one final (window, page, count) row per group, no flapping
     .ToOutput();
```

Underneath sits `.WindowCloses()` on a grouping — a first-class stream announcing each key
exactly once (weight +1, never retracted) when the watermark passes its deadline — which
remains available for close-driven pipelines the paved road doesn't cover.

**Recursion (fixed points).** `.Recurse(body)` closes a stream over itself and computes the
least fixed point `X = Distinct(seed ∪ body(X))` — the DBSP recursive circuit, with the
stream it is called on as the seed and `Distinct` as the built-in stabilizer. Transitive
closure is the classic shape:

```csharp
var closure = edges.Recurse(paths =>
    paths.GroupBy(p => p.To)
         .Join(edges.GroupBy(e => e.From), (_, p, e) => (p.From, e.To)));
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
`GroupBy`/`Join`/`Distinct`; aggregations and the like are rejected at definition time.

**Exchange (multi-threading).** `CircuitOptions.Workers = N` runs N worker threads, each
owning its own instance of every operator and its own shard of all operator state.
`GroupBy` inserts an *exchange* operator — the DBSP/timely-dataflow shuffle — which
hash-partitions each delta by key and redistributes it across workers mid-step (barrier
synchronized), so joins and grouped aggregations are correct and scale with the worker
count. `Distinct` shuffles by value automatically. There is no locking on operator state:
one shard, one thread. The engine adapts to the worker count: with `Workers = 1` steps
run inline on the coordinator (no worker thread, no handoff — hundreds-of-µs step
latency for small workloads), while multi-worker exchanges synchronize on a barrier that
spins briefly — mid-step the workers are already hot — and parks on a kernel wait after a
spin budget, so a long wait (a network round trip, a slow peer) stops burning cores.

**Routing hashes.** Exchanges route by `StableHash`, a process- and machine-stable
structural hash (FNV-1a over string contents, raw bit patterns for numbers, a compiled
per-type fold over public members for records/tuples/collections) — never by
`GetHashCode`, whose string hashing is randomized per process. A randomized route would
send the same key to different workers on different processes, silently splitting a
join's state across a cluster or orphaning it across a checkpoint restart. A key type
`StableHash` cannot derive a hash for (interfaces, delegates, unordered collections,
types with no public readable members) is rejected at definition time; a type whose
equality diverges from its public structure (say, a case-insensitive key) takes the
`GroupBy(keySelector, stableHash)` / `Distinct(stableHash)` overloads — the supplied
`Func<TKey, uint>` must return the same value for equal keys in every process, on every
machine.

**Cross-process exchange (clustering).** The same shuffle spans machines, behind the same
operator API: the circuit definition is built unchanged on every process — user code
(`GroupBy`/`Join`/aggregations) never learns the cluster exists — and only
`CircuitOptions.Cluster` declares the process's place in a static topology. Each of the N
processes hosts a contiguous slice of the global worker set (process p owns global workers
[p·Workers, (p+1)·Workers)), every exchange hash-partitions by key across all Workers × N
of them, and shards bound for a remote worker — a delta batch is just (value, weight)
pairs, retractions included — are encoded by a dense per-type codec (compiled from the same
structural rules as `StableHash`, with a JSON fallback for types it cannot round-trip and a
format byte on every frame) and shipped as length-prefixed frames over a transport:
`TcpExchangeTransport` (plain sockets, no dependencies; each process gets the shared
endpoint list and its own index, connections are retried until the mesh is up) or
`InMemoryExchangeTransport` for tests.

> **Security**: the TCP transport is unauthenticated cleartext — the handshake is a bare
> process index, and anyone who can reach the port can join the mesh and inject state.
> Run it only on a trusted, isolated network segment (or inside a mesh/tunnel the
> deployment provides); TLS/authentication is deliberately deferred. Frame payloads are
> capped (`maxFrameBytes`, default 64 MB, same value everywhere) so a corrupt or hostile
> length prefix is rejected rather than allocated.

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
(a descriptive exception on the step, the handles, and `Circuit.Fault`) — never a hang. A
peer that is *stuck* rather than dead — a wedged delegate, a half-dead host holding its
connections — is covered by `CircuitOptions.ExchangeTimeout` (null by default: wait
forever): when set, any single cross-process wait — an exchange's shards, the step-end
alignment, space in a peer's bounded send queue — that outlasts it faults the circuit with
a descriptive `TimeoutException` instead of stalling every process. Size it well above the
slowest healthy step; a fired timeout is terminal like any fault — rebuild from the last
committed checkpoint once the cluster is healthy. Checkpointing stays per process: each
commits its own shards' state and its own inputs' offsets, under keys carrying the process
index plus a step marker, so processes sharing one physical store never clobber each other;
lockstep steps and equal `CheckpointInterval`s mean every process commits the same step
boundaries, and after a crash all processes must resume from the same committed step —
compare `Circuit.CommittedStep` across processes before feeding data (there is deliberately
no distributed commit protocol). `Recurse` is not supported in clustered circuits — the
per-iteration convergence decision would need a cross-process round trip every loop
iteration — and is rejected at `Build()`.

**Backpressure.** Everything between producer and consumer is a bounded
`System.Threading.Channels` channel:

```
producer --await InsertAsync--> [bounded input channel] --drain--> step --publish--> [bounded output channel] --> consumer
```

A slow consumer fills its output channel, which stalls step publishing, which stops input
draining, which fills the input channels, which suspends producers inside `InsertAsync`.
No configuration, no dropped data.

**Faults.** When any worker throws mid-step (a user delegate, a storage error), the fault
propagates cooperatively instead of deadlocking the circuit: the faulting worker poisons the
exchange barrier, so peers blocked mid-shuffle — recursion-loop iterations included — bail
out of the step rather than deadlock, and the circuit moves to a terminal faulted state
within that step. The faulting `StepAsync` throws the original exception (an
`AggregateException` when several workers fault in the same step), consumers’ `Changes()`
observes it after the pre-fault deltas, producers blocked in `InsertAsync` are released with
a `CircuitFaultedException` (original fault as its inner) instead of hanging, and
`Circuit.Fault` exposes it; `DisposeAsync` of a faulted circuit completes quietly. A failed
or cancelled output publish faults the circuit too — by then state has already advanced, so
dropping the step's merged deltas would silently desynchronize every consumer's integrated
view. A faulted circuit's in-memory state is unusable — the faulting step is half-applied —
but that step is never committed, so with a checkpointing storage provider the last
committed checkpoint is intact: rebuild the circuit against the same provider to recover
exactly-once (below).

**Observability.** `CircuitOptions.Name` names the circuit in its logs and as the
`circuit` tag on every metric, so circuits sharing a process stay distinguishable;
`CircuitOptions.LoggerFactory` receives the logs (categories `Incremental.Circuit` and
`Incremental.Cluster`: lifecycle at Information, checkpoint commits at Debug, faults and
cluster timeouts at Error — record contents are never logged):

```csharp
var options = new CircuitOptions { Name = "orders", LoggerFactory = loggerFactory };
```

Metrics need no wiring: the `Incremental` `Meter` is always on and costs nothing
unlistened.

| Instrument | Meaning |
| --- | --- |
| `incremental.circuit.steps`, `.step_duration` | steps completed; wall-clock per step, drain through publish |
| `incremental.circuit.records` | input records drained into steps (after replay dedup) |
| `incremental.circuit.faults` | transitions to the terminal faulted state |
| `incremental.circuit.checkpoint_commits`, `.checkpoint_commit_duration` | atomic state+offsets commits |
| `incremental.circuit.input_queue_depth`, `.output_queue_depth` | bounded-channel depths, per port |
| `incremental.circuit.watermark_lag` | now minus the frozen watermark, seconds |
| `incremental.circuit.barrier_wait` | cumulative seconds each worker has spent waiting on exchanges (barrier or point-to-point) and loop barriers |
| `incremental.cluster.frames_sent/received`, `.bytes_sent/received`, `.send_queue_depth` | exchange traffic, per peer |

**Keyed inputs.** `builder.KeyedInput<K,V>(name, keySelector)` declares an input with
upsert/delete-by-key semantics: the handle tracks the current row per key, so
`UpsertAsync(row)` emits the retraction of the previous version automatically and
`DeleteAsync(key)` retracts the current one. This is the natural way to feed reference /
master data (and any stream whose rows are amended in place) — one upsert to a reference
row re-attributes everything joined against it. Caveat (design in progress): the upsert
map lives on the producer side and is not checkpointed, and keyed inputs carry no source
offsets — treat a keyed input as a full-replay source after a restart.

**What your types must satisfy.** Values are Z-set members and dictionary keys, so every
type flowing through a stream needs value equality — records are the natural fit. A
`class` without `Equals`/`GetHashCode` overrides breaks silently: a retraction never finds
the insertion it should cancel, and the two coexist at weights +1/−1. With Redis-backed
state or a cluster, values must additionally round-trip losslessly through the storage
serializer's JSON (public properties/fields; no polymorphism, no reference cycles, no
private-only state — a type serializing as `{}` collapses every instance into one); the
cluster's dense codec follows the same structural rules. Types whose equality diverges
from their public structure need the custom stable-hash overloads (above). Delegates
handed to operators — selectors, predicates, aggregates, eviction deadlines — must be pure.
`Sum` over integer accumulators is checked (overflow faults the circuit instead of
wrapping); floating-point sums drift permanently under retraction (adding x then −x does
not restore the accumulator), so prefer `decimal` or `long`.

**Storage.** Stateful operators keep their state behind `IStorageProvider`
(`IWeightStore`, `IIndexedWeightStore`, `IValueStore`), created per operator *per worker*
so implementations never see cross-thread access to one store.

- `InMemoryStorageProvider` (default) — heap dictionaries.
- `RedisStorageProvider` (`Incremental.Storage.Redis`) — state in Redis hashes; survives
  restarts, can exceed the heap, and checkpoints (below). `KeyPrefix` is required and must
  be unique per circuit sharing the Redis instance — two circuits under one prefix would
  silently interleave state:

  ```csharp
  var redis = await ConnectionMultiplexer.ConnectAsync("localhost");
  var options = new CircuitOptions
  {
      Storage = new RedisStorageProvider(redis, new RedisStorageOptions { KeyPrefix = "orders-prod" }),
  };
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
always agree, even across a crash mid-commit. The commit verifies every command in the
transaction landed — a checkpoint is never reported committed with writes missing.

Producers stamp records with source offsets — `SendAsync(value, weight, partition, offset)`
(and matching `InsertAsync`/`RemoveAsync` overloads), e.g. a Kafka (partition, offset);
dedup is per partition, so one input fronts a whole topic. The single-offset overloads map
to partition 0, and unstamped records are numbered as they are consumed — but an input is
one or the other: mixing stamped and unstamped records throws (immediately at the handle,
and as a circuit fault for a producer that switches kinds across a restart) instead of
silently corrupting the replay position. After a crash, rebuild the circuit against the
same provider: the state is already there, `circuit.CommittedOffsets("orders")` says where
each partition got to (`CommittedOffset` still serves the unstamped and single-partition
cases), and replayed records stamped at or below their partition's checkpoint are dropped
on arrival — resuming a log-style source is exactly-once from the state's point of view.

Restarts are guarded by a **state manifest**: every checkpoint commits a description of
the state it belongs to — each store's identity, the worker and process counts, a format
version — and a rebuilt circuit refuses to start against a checkpoint whose manifest does
not match, listing the mismatches, instead of silently reading another operator's state.
By default a store's identity is its position in the circuit, so inserting or reordering
operators orphans downstream state (detected, loudly); `.Named("revenue")` on the stream a
stateful operator returns pins its identity by name instead, so the circuit can evolve
around it and a rebuild still finds its state. Pure additions — new operators, whose state
starts empty — are always allowed. A name pins identity, not meaning: state is resumed
as-is, so rename the operator when its computation changes.

Output batches are transient and not checkpointed — consumers re-derive views by
integrating. Batches published between the last commit and a crash are never replayed, so
**table-shaped outputs resync automatically**: a keyed or windowed result's `ToOutput()`
maintains the view's integrated state behind the storage provider (checkpointed and
recovered with everything else), and the handle's `Changes()` begins with one batch
asserting the current integrated state — treat it as a reset. That makes consumers correct
across restarts and mid-run subscription with no opt-in to remember;
`ToOutput(bootstrap: false)` opts a stateless consumer out of the state cost.
`MaterializeInMemory()` builds on the same resync: a live in-memory view with consistent
`Current()` snapshots on demand — its cost is in its name (O(live rows) heap). Outputs are
single-consumer (a second `Changes()` throws rather than silently splitting the feed), and
plain stream outputs are pure changefeeds: at-most-once across a crash, exactly-once being
the downstream consumer's offset/idempotency concern. (In a cluster an output covers the
local process's workers, like the delta stream itself.)

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
examples/Incremental.Showcase      the all-features demo: a marketplace payments desk, served as a
                                       local web app (`dotnet run`, then http://localhost:5170) in
                                       five tabs — enrichment joins (inner/left/full/anti) and
                                       aggregates with one upsert re-attributing history, event-time
                                       windows (tumbling/hopping/session, live vs OnClose, late
                                       events), recursive fraud-ring closure with live edge
                                       deletion, a mid-stream crash recovered exactly-once from
                                       checkpoints (in-memory by default, `--redis` for real), and
                                       scaling out: the same declaration measured at 1/2/4 worker
                                       threads (sharding costs latency and buys throughput — the tab
                                       measures both rather than asserting either) and, with
                                       `--nodes 2` (alias `--cluster`), re-spawned as a second OS
                                       process exchanging shards over TCP. Picking a topology
                                       relaunches the showcase into it and the page reconnects
                                       itself. Each tab shows the fluent declaration that runs it
                                       and buttons that fire its scripted moments; a sidebar
                                       streams the raw enriched changefeed with z-set weights, so
                                       retract-then-assert is visible rather than described; the
                                       footer reads the `Incremental` meter. `--workers N` sets the thread count
                                       (default 2, so the other tabs really exercise the exchange
                                       path); `--exchange barrier|p2p|streaming` picks how the live
                                       pipeline's exchanges synchronize. No NuGet dependency beyond
                                       the library: `Microsoft.NET.Sdk.Web` resolves ASP.NET Core
                                       from the shared framework, the page is one dependency-free
                                       HTML file, and state reaches it over server-sent events.
                                       `--headless` runs every tab and trigger unattended, prints
                                       the panels as text, and exits non-zero if a verified act
                                       (crash recovery, cluster shard merge) fails to verify — that
                                       is the CI smoke test. Deliberately not shown: the escape
                                       hatches (Minus/Negate, EvictWhen, WindowCloses, Rows,
                                       weighted SendAsync) — reference material (docs/intro.md),
                                       not headline features
examples/Incremental.TaxiAnalytics genuine big data: ~13M real NYC yellow-taxi trips (parquet, read
                                       via Parquet.Net, downloaded and cached on first run, ~50MB/month;
                                       `--months N`, `--synthetic`) through join + counts + revenue +
                                       top-5 zones per borough; narrated phases, then continuous
                                       single-row churn with a live view until you press a key —
                                       ~1M rows/s bulk load; single-row updates in ~250 µs,
                                       thousands of × cheaper than a full recompute
benchmarks/Incremental.Benchmarks  BenchmarkDotNet suite (Z-set primitives, stateless pipeline,
                                       join / sum / min / top-k delta cost vs state size), plus
                                       `-- --assert`: CI performance contracts that check scaling
                                       ratios (delta cost must not grow with state size) instead of
                                       wall-clock numbers, so they hold on noisy shared runners
benchmarks/Incremental.Torture     throughput rig: the showcase pipeline flat out over a sliding
                                       order population — records/s, process CPU split (workers vs
                                       GC), allocation per record; see Benchmarks below
```

## Benchmarks

```sh
dotnet run -c Release --project benchmarks/Incremental.Benchmarks -- --filter '*'   # full BenchmarkDotNet suite
dotnet run -c Release --project benchmarks/Incremental.Benchmarks -- --assert      # CI performance contracts
dotnet run -c Release --project benchmarks/Incremental.Torture -- --workers 4      # sustained throughput
```

The `--assert` mode is what CI gates on. Shared runners are too noisy for absolute timing
assertions, so each contract runs an identical fixed-size delta against a small and a 64×
larger preloaded state (interleaved trials, medians) and fails if the large/small ratio
exceeds a generous bound (1.5× allocations, 5× time). A regression from O(delta) to O(state)
shows up as a ~64× ratio, so the check is high-signal without being flaky. The full suite
runs on demand via the *Benchmarks* GitHub workflow (`workflow_dispatch`), which uploads
the BenchmarkDotNet artifacts.

`Incremental.Torture` answers the other question — what a whole graph costs per record when
it is running flat out, rather than what one operator costs per delta row. It runs the
showcase pipeline over a sliding population of orders (inserting order *N* retracts order
*N−live*, so state settles at a fixed size and retractions are on every step), reports the
process CPU split between workers and the GC, and has an in-process allocation profiler by
type (`--alloc-types`). `--processes N` runs the cluster graph across co-hosted processes,
which is what puts the exchange wire format on the measured path. See its README for current
numbers and for the runtime GC settings that matter more than any single code change:
**size the server GC's heap count to `CircuitOptions.Workers` rather than to the core count,
turn DATAS off** — a circuit's live set is large and mostly static, which is the shape
DATAS handles worst — **and raise the LOH threshold to 1 MB** so medium arrays stop
triggering gen2 collections.

The operators themselves allocate almost nothing per step — outputs, input batches and the
exchange wire path all run on reused buffers — so at throughput the remaining allocation is
mostly the rows *your* lambdas return. The one consumer-side lever worth knowing: declare
small row and accumulator types as `readonly record struct` (a linear aggregate constructs
a fresh accumulator every fold; as a struct it never touches the heap). `docs/intro.md` has
the details under *Running it fast*, and the showcase's `Records.cs` / `Aggregates.cs` show
the struct-vs-record split.

Every example ships with that configuration applied. `examples/Directory.Build.props` turns
on server and concurrent GC and turns DATAS off for all of them, and each example declares
the worker count it builds its circuit with as `<CircuitWorkers>`, which becomes the
runtimeconfig heap count. So `dotnet run` on any example measures the library rather than
the default GC's idea of what to do with a 20-thread machine. It is startup configuration —
a library cannot set it for you — so an application gets it by copying those few lines into
its own project; `docs/intro.md` has them under *Running it fast*.

## Packaging

MIT-licensed (see `LICENSE`). The two shipped libraries (`Incremental`,
`Incremental.Storage.Redis`) multi-target **net8.0 and net10.0** and carry full NuGet
metadata — `dotnet pack` produces consumable packages with XML docs, SourceLink, and
symbol packages (not yet published to nuget.org; consume by project reference or a local
feed). The only runtime dependency of the core library is
`Microsoft.Extensions.Logging.Abstractions`.

## Running with Redis

```sh
docker compose up -d        # starts Redis on localhost:6379
dotnet test                 # Redis integration tests now run instead of skipping
dotnet run --project examples/Incremental.LogAnalytics -- --redis
```

## Status / roadmap

The semantics above are implemented and tested. The known limitations are deliberate,
disclosed trade-offs (below), not missing semantics:

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
- [x] Production hardening pass: process-stable exchange routing (`StableHash` — the
      randomized string hash silently broke cluster routing and multi-worker restore),
      state manifests + `.Named()` checkpoint identities, per-partition input offsets,
      snapshot outputs for consumer bootstrap, output-publish failures fault the circuit,
      `ILogger` + `Meter` observability, cluster exchange timeouts + frame caps + parking
      barrier, verified Redis commits + required key prefix, checked integer sums, MIT
      license, net8.0/net10.0 NuGet packaging
- [x] 0.2 consumer API (`docs/intro.md`): LINQ-dialect names, group-then-window typed
      ladder (windows only on event-timed groupings, eviction derived, per-window `TopK`,
      `OnClose`), definitions redeemed for typed handles (`circuit.Handle(def)` — outputs
      unnamed), changefeed-first outputs with automatic table-shaped resync and
      `MaterializeInMemory` views

### Known limitations (deliberate, disclosed)

- The TCP cluster transport is **unauthenticated cleartext** — trusted, isolated networks
  only; TLS/authentication is deferred.
- No peer reconnection: a disconnect or fired `ExchangeTimeout` is terminal — rebuild
  every process from the last committed checkpoint.
- Cluster checkpoints commit per process with no distributed commit protocol: a crash can
  leave processes one checkpoint apart — compare `Circuit.CommittedStep` across processes
  before feeding data.
- Exactly-once ends at the output edge: batches published after the last commit are not
  replayed after a crash; table-shaped outputs resync consumers automatically (batch zero
  of the changefeed), stream-shaped outputs are at-most-once across a crash by design.
- Floating-point sums drift under retraction — prefer `decimal`/`long` accumulators.
- Changing a persisted value type's shape (renaming/retyping members) invalidates its
  serialized state; there is no schema migration — change the key prefix and rebuild from
  source, or keep state types stable.

### Headroom

- [x] All-singing, all-dancing demo showing every feature together
      (`examples/Incremental.Showcase` — see Project layout)
- [ ] Keyed-input restart story (checkpoint the upsert map, or derive retractions from
      operator state; the Showcase's Act 4 demonstrates the current behavior honestly —
      after a restore it does *not* re-feed keyed inputs, because replaying upserts on top
      of checkpointed state double-counts)
- [ ] Investigate off-heap in-process storage to reduce GC (RocksDB, SQLite candidates)
- [ ] Cluster transport TLS/authentication
