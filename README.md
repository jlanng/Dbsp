# StreamProcessor

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
| `Aggregate` (`Min`, `Max`, `Fold`, custom) | Integrated group state per key; recomputes touched groups, retracts the stale result (-1) and asserts the new one (+1). Correct for non-invertible aggregates (`MIN` under deletion) |
| `Distinct` | Integrated weight per value; emits only presence transitions |
| `TopK` | Integrated group per key; re-ranks touched groups and emits only membership changes (displaced rows retracted). Multiplicities count toward K |
| `AntiJoin` | Integrated left side plus per-key right-side row counts; a key whose match-presence flips retracts or re-asserts its left rows |
| `LeftJoin` / `FullJoin` | Pure composition: inner join ⊕ mapped anti-join(s) — the Z-set algebra makes the union incremental for free |
| `TumblingWindow` / `HoppingWindow` | Pure data transformation (flat-map): records join every window covering their event time; keying by `(window, …)` turns any aggregate into a windowed one. Late and deleted events correct their windows via ordinary deltas |

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

**Exchange (multi-threading).** `CircuitOptions.Workers = N` runs N worker threads, each
owning its own instance of every operator and its own shard of all operator state.
`IndexBy` inserts an *exchange* operator — the DBSP/timely-dataflow shuffle — which
hash-partitions each delta by key and redistributes it across workers mid-step (barrier
synchronized), so joins and grouped aggregations are correct and scale with the worker
count. `Distinct` shuffles by value automatically. There is no locking on operator state:
one shard, one thread.

**Backpressure.** Everything between producer and consumer is a bounded
`System.Threading.Channels` channel:

```
producer --await InsertAsync--> [bounded input channel] --drain--> step --publish--> [bounded output channel] --> consumer
```

A slow consumer fills its output channel, which stalls step publishing, which stops input
draining, which fills the input channels, which suspends producers inside `InsertAsync`.
No configuration, no dropped data.

**Storage.** Stateful operators keep their state behind `IStorageProvider`
(`IWeightStore`, `IIndexedWeightStore`, `IValueStore`), created per operator *per worker*
so implementations never see cross-thread access to one store.

- `InMemoryStorageProvider` (default) — heap dictionaries.
- `RedisStorageProvider` (`StreamProcessor.Storage.Redis`) — state in Redis hashes via
  `HINCRBY`; survives restarts and can exceed the heap:

  ```csharp
  var redis = await ConnectionMultiplexer.ConnectAsync("localhost");
  var options = new CircuitOptions { Storage = new RedisStorageProvider(redis) };
  ```

- Candidates that make sense next: RocksDB (large state, embedded, fast scans), SQLite.

**Stepping modes.** By default a pump task steps the circuit whenever input is available.
With `AutoStep = false` the circuit advances only on explicit `StepAsync()` calls — exact,
deterministic step boundaries for tests and batch jobs.

## Project layout

```
src/StreamProcessor                    core: Z-sets, builder API, operators, windows, runtime, in-memory storage
src/StreamProcessor.Storage.Redis      Redis-backed IStorageProvider
tests/StreamProcessor.Tests            xunit: Z-set algebra, per-step delta semantics, multi-worker runs,
                                       Redis integration (auto-skipped when Redis is down)
examples/StreamProcessor.Example       joined + aggregated order analytics with live retraction
examples/StreamProcessor.LogAnalytics  simulated clickstream → 5s tumbling windows → counts → live top-3
                                       per window, rendered as an in-place Spectre.Console dashboard
                                       (bar gauges per window, watermark in the header);
                                       `--redis` keeps operator state in Redis
examples/StreamProcessor.TaxiAnalytics genuine big data: ~3M real NYC yellow-taxi trips (downloaded and
                                       cached on first run, ~50MB/month; `--months N`, `--synthetic`)
                                       through join + counts + revenue + top-5 zones per borough, then
                                       single-row updates timed against a naive full recompute —
                                       ~1M rows/s bulk load, per-update speedups of hundreds of ×
```

## Running with Redis

```sh
docker compose up -d        # starts Redis on localhost:6379
dotnet test                 # Redis integration tests now run instead of skipping
dotnet run --project examples/StreamProcessor.LogAnalytics -- --redis
```

## Status / roadmap

Scaffold — the semantics above are implemented and tested, with plenty of headroom:

- [x] Linear-aggregate fast path: `COUNT`/`SUM` maintained from the delta alone with O(keys)
      state (`ILinearAggregate`); custom linear aggregates via `AggregateLinear`
- [ ] Tournament trees for `MIN`/`MAX` over large groups
- [ ] Batched/pipelined Redis access (currently one round trip per state touch)
- [ ] Checkpointing input offsets with state for exactly-once restart (Redis provider)
- [x] `TopK` (per-key ranking with retraction of displaced rows)
- [x] Tumbling and hopping event-time windows (assignment; late data handled by deltas)
- [x] Watermarks: late-event filtering and eviction of closed-key state (aggregate/TopK)
- [x] Anti-join, left and full outer joins
- [ ] Window-close notifications (emit "window X is final" for downstream consumers)
- [ ] Watermark-driven eviction for join/anti-join/distinct state; session windows
- [ ] Recursive/iterative circuits (DBSP fixed points) for transitive closure-style queries
- [ ] Cooperative fault propagation across workers (a faulted worker currently poisons the
      circuit; disposal is required)
- [ ] Cross-process exchange (workers on different machines) behind the same operator
