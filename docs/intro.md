# An introduction to Incremental

> **Status:** this document describes the 0.2 API surface, which is being built. It doubles as
> the design spec for that surface; sections marked **TODO** are intentionally thin for now.
> The appendix mapping 0.1 → 0.2 will be deleted once the migration lands.

Incremental is a stream processing library for .NET in the style of
[DBSP](https://arxiv.org/abs/2203.16684): you declare a dataflow over your data once, feed it
a stream of changes, and every derived result — counts, joins, windowed leaderboards — is
kept up to date by processing **only the changes**, never by recomputing. An update that
touches one row costs work proportional to that row, not to the size of your data.

Two ideas carry the whole library:

- **Everything is a change.** Streams carry *deltas*: a record plus a signed weight
  (+1 inserted, −1 retracted). An update is a retraction of the old value and an insertion of
  the new one. Because retractions flow through every operator, downstream results correct
  themselves — a trade amendment re-attributes totals, a reference-data change re-enriches
  historical joins.
- **Everything is declared up front.** You describe the dataflow with a `CircuitBuilder`,
  `Build()` compiles it into a `Circuit` — a fixed operator graph executed step-by-step by
  one or more workers — and then you push changes in and read changes out.

## Your first circuit

Live page-view analytics: count views per page in 5-second windows, keep the top 3 pages per
window, tolerate events arriving up to 2 seconds late.

```csharp
using Incremental;

var builder = new CircuitBuilder(new CircuitOptions { Workers = 2 });

var views = builder.Input<PageView>("views");

var topPages = views
    .WithEventTime(v => v.At, lateness: TimeSpan.FromSeconds(2))
    .GroupBy(v => v.Page)
    .TumblingWindow(TimeSpan.FromSeconds(5))
    .Count()                    // live count per (window, page); closed windows evicted
    .TopK(3, r => r.Value)      // top 3 pages within each window
    .ToOutput();

await using var circuit = builder.Build();
circuit.Start();

var input  = circuit.Handle(views);      // InputHandle<PageView>
var output = circuit.Handle(topPages);   // OutputHandle<…>, types inferred

var consumer = Task.Run(async () =>
{
    await foreach (var batch in output.Changes())
        Render(batch);                   // each batch: assertions and retractions
});

await input.InsertAsync(new PageView("/pricing", DateTimeOffset.UtcNow));
// … keep feeding events; the leaderboard maintains itself.

input.Complete();
await consumer;    // always await your consumer: it observes the final batches — and any fault

record PageView(string Page, DateTimeOffset At);
```

Things worth noticing:

- No windows-in-keys bookkeeping and no state-cleanup code. `TumblingWindow` after `GroupBy`
  makes the window part of each group's identity automatically, and state for a window is
  evicted once the watermark passes its end plus the allowed lateness. This pipeline runs
  forever in constant memory.
- `views` and `topPages` are *definitions*. After `Build()` you redeem them with
  `circuit.Handle(...)` for live, fully-typed handles — no string lookups, no restating
  types.
- The output is a **changefeed**, not snapshots. When a count changes, you receive a
  retraction of the old leaderboard row and an assertion of the new one. Integrating changes
  into a view is your choice (see [Consuming outputs](#consuming-outputs)).
- Nothing downstream of the inputs is named. Definitions are references; the circuit's
  structure never leaks into strings.
- The consumer task is awaited, not fire-and-forgotten. Output channels are bounded, so an
  abandoned consumer eventually stalls the circuit and blocks disposal — and its exceptions
  vanish. `Changes()` completes when the circuit disposes and throws `CircuitFaultedException`
  if the circuit faults, so awaiting the consumer surfaces both.

## The type ladder

The API steers you by what each type offers. There is one order of operations, and it is the
one that compiles.

```
ZStream<T>                                   deltas of T
  │  Select · Where · SelectMany · Distinct · Concat
  │
  ├─ GroupBy(keySelector) ──▶ GroupedZStream<TKey, T>          a staging type
  │     │  aggregates: Count · Sum · Min · Max · TopK · Aggregate(…)
  │     │  joins:      Join · LeftJoin · FullJoin · AntiJoin   (on the shared key)
  │     ▼
  │  aggregate result: KeyedZStream<TKey, TValue>              rows (Key, Value)
  │     │  MapValues · Values · Grouped() — continue on the same key, no reshuffle
  │
  └─ WithEventTime(sel, lateness) ──▶ EventTimeStream<T>
        │  Where and Distinct keep event time; Select returns a plain ZStream
        │  (reshaping loses the timestamp — re-declare event time afterwards)
        │
        └─ GroupBy(keySelector) ──▶ event-timed grouping: everything above, plus
              │  TumblingWindow · HoppingWindow · SessionWindow
              ▼
           WindowedGroupedZStream<TKey, T>                     aggregates only
              ▼
           aggregate result: WindowedZStream<TKey, TValue>     rows (WindowStart, WindowEnd, Key, Value)
              │  Select over values · Where · GroupBy (window-sticky)
              │  TopK (ranks within each window) · OnClose()
              │  Rows()  ← explicit exit: flatten to a plain ZStream, dropping window semantics
              ▼
           ToOutput()
```

Deliberate narrowness, and why:

- **Windows require event time, and the types enforce it.** `TumblingWindow` and friends only
  exist on groupings of an `EventTimeStream` — windowing without a declared event time is a
  compile error, not a runtime surprise. `WithEventTime` carries the timestamp selector to
  the window site, which is why reshaping the record (`Select`) drops back to a plain
  `ZStream`: the selector no longer applies, so you re-declare event time on the new shape.
- **`ZStream<T>` has no window methods**, so "window-then-group" — where you'd have to
  compose `(window, key)` keys and wire eviction by hand — is not a path you can stumble
  into.
- **`GroupedZStream` is a staging type**, not a stream: you can aggregate it, window it (if
  event-timed), or join it, but not `Select` or output it. Grouping only means something as a
  prelude to stateful work. (If you know Kafka Streams: this is `KGroupedStream`.) Aggregate
  results come back as real keyed streams; `Grouped()` on one continues stateful work on the
  same key without a reshuffle — a distinct count is `.Distinct().Grouped().Count()`.
- **On windowed results, everything is per-window by construction.** `GroupBy` is
  window-sticky: regrouping windowed rows composes the new key *within* the window and
  re-derives eviction — you cannot accidentally aggregate across windows. `TopK` ranks
  entries within each window (ranking within a `(window, key)` group would be one row per
  group — degenerate — so per-window is the only sensible reading, and needs no extra call).
  Be aware that per-window ranking concentrates each window's rows on a single worker — the
  price of a global ranking. Crossing windows on purpose is `Rows()`: a named, grep-able exit
  after which you own key composition and state lifecycle yourself.
- **Session windows are uniform.** Because windows attach to groupings,
  `GroupBy(k).SessionWindow(gap)` has the same shape as tumbling and hopping, even though
  session boundaries are inherently per-key.

Method naming follows LINQ where the semantics genuinely match (`Select`, `Where`,
`SelectMany`, `GroupBy`, `Distinct`) — this is the dialect .NET developers already speak, and
since `ZStream` is not an `IEnumerable`, none of LINQ's own operators apply to it: the
familiar names bind unambiguously to Incremental's operators. Operations with no LINQ
analogue keep their own names (`WithEventTime`, `TumblingWindow`, `TopK`, `AntiJoin`); where
LINQ semantics would mislead, the LINQ name is deliberately avoided (z-set subtraction is
`Minus`, an advanced operator — not `Except`, whose LINQ meaning is distinct set-difference).
This is **not** a LINQ provider: there are no expression trees and no query translation —
your lambdas run as-is.

## Your records are the contract

Streams and state are z-sets: dictionaries from your record to a signed weight. Three
consequences for the types you flow through a circuit:

- **Value equality is required.** A retraction only cancels an assertion if the two are
  *equal*. Use `record` / `record struct` types (or types with correct `Equals`/
  `GetHashCode`). A mutable class with reference equality silently breaks everything: weights
  never cancel, state grows without bound, and "corrections" become duplications.
- **Weight zero means gone.** A fully-retracted row vanishes from results; it does not appear
  with a zero. Correspondingly, `Count()` never emits 0 for an emptied group — the row for
  that group is retracted instead.
- **Durable state round-trips through serialization.** With a persistent storage provider
  (Redis today), every stateful operator's records are serialized (`System.Text.Json` by
  default): no cycles, serializable members, and a shape you keep stable across deployments —
  checkpoints outlive your binaries.

## Event time, watermarks, and lateness

`WithEventTime(selector, lateness)` declares which field carries event time and how much
disorder to tolerate. The **watermark** — the maximum event time seen, minus the allowed
lateness, taken as the *minimum across all event-timed inputs* — drives everything
time-based:

- a window closes when the watermark passes its end; its state is then evicted;
- events arriving later than the allowed lateness are dropped (**TODO:** late-event
  side-channel — design pending);
- `circuit.CurrentWatermark` exposes the current value for monitoring.

Because the watermark is the minimum across sources, an **idle source stalls it** — windows
stop closing and state stops evicting until that source advances. (**TODO:** idleness
handling — timeout/mark-idle design pending.)

**When do windowed results emit?** Live, every step: as events arrive you see the current
count rise via retract-and-assert corrections. If you only want each window's *final* result,
`OnClose()` on a windowed result emits each row exactly once, when the watermark closes its
window — the incremental analogue of emit-on-close/suppression:

```csharp
var finals = views
    .WithEventTime(v => v.At, lateness: TimeSpan.FromSeconds(2))
    .GroupBy(v => v.Page)
    .TumblingWindow(TimeSpan.FromSeconds(5))
    .Count()
    .OnClose()      // one final row per (window, page), no flapping
    .ToOutput();
```

## Grouping, aggregation, and correctness under change

`GroupBy` does two jobs: it defines aggregation/join keys, and it **shards state** — records
are routed to workers by a stable hash of the key, so each worker owns its slice of state.
Two consequences:

- Aggregates are **incremental over retractions**. `Sum` decreases when a record is
  retracted; `Count` goes down; `TopK` re-ranks; joins un-match. You never see stale results
  because "everything is a change" applies inside operators, too.
- Keys must hash **identically in every process** (checkpoint restore and clustering depend
  on it). The built-in stable hash covers common key types (primitives, strings,
  `DateTimeOffset`, tuples and records of them). For anything else, supply one:
  `GroupBy(selector, stableHash: k => …)`. `object.GetHashCode()` is never used — .NET
  randomizes it per process.

Custom aggregates plug in via `Aggregate(IAggregateFunction<…>)`, or
`AggregateLinear(ILinearAggregate<…>)` when the aggregate is a homomorphism over weights
(cheaper: no per-key group state).

> **TODO:** worked custom-aggregate example (e.g. HDR percentile sketch).

## Joins

Joins live on groupings that share a key:

```csharp
var trades      = builder.KeyedInput<long, Trade>("trades", t => t.Id);
var instruments = builder.KeyedInput<int, Instrument>("instruments", i => i.Id);

var enriched = trades.Values()
    .GroupBy(t => t.InstrumentId)
    .Join(instruments.Values().GroupBy(i => i.Id),
          (trade, instrument) => new EnrichedTrade(trade, instrument.Symbol, instrument.Currency));
```

`Join` is an incremental equi-join: when either side changes, only affected pairs are
re-emitted. Reference data is not a special case — it is just another stream, and a
`LeftJoin`/`FullJoin`/`AntiJoin` cover the outer and negative cases. When an instrument row
is *upserted*, the old enriched rows are retracted and new ones asserted automatically —
historical results re-attribute themselves.

> **TODO:** windowed / interval joins (stream-stream within a time bound) — not yet designed;
> today this is manual via the escape hatches.

## Inputs and outputs

**Inputs.** `builder.Input<T>(name)` gives an insert/retract stream. Awaiting `InsertAsync`
means "accepted into the input buffer" (it applies backpressure), not "processed". Names are
required on inputs: they anchor durable identity for checkpointed source offsets
(`InsertAsync(value, offset)` for exactly-once resumption from replayable sources).

`builder.KeyedInput<TKey, TValue>(name, keySelector)` gives upsert semantics — `UpsertAsync`
replaces the current row for a key (emitting the retraction of the old row itself) and
`DeleteAsync` removes it.

> **Keyed-input caveat (design in progress):** the upsert map that generates retractions is
> producer-side and not yet checkpointed, and keyed inputs don't carry source offsets. For
> now, treat keyed inputs as full-replay sources: after a restart, replay the reference data
> from scratch rather than resuming mid-stream. Fixing this properly (checkpointing the
> upsert map, or deriving retractions from operator state) is an open engine decision — see
> the appendix.

**Outputs.** `.ToOutput()` marks any stream (or table-shaped result) as an output. Outputs
are not named: they carry no durable state, so the definition reference is their identity.
(`.Named("…")` remains available on any stage purely for logs and diagnostics.)

Both return definitions that `circuit.Handle(definition)` redeems for live handles after
`Build()`. The asymmetry is deliberate: an input's name identifies an *external* thing — the
feed whose checkpointed offsets must survive restarts and topology edits — while everything
internal is identified by reference.

### Consuming outputs

The first-class output API is the changefeed:

```csharp
await foreach (ZSet<Row> batch in output.Changes())
    ApplyToMyStore(batch);   // each batch: rows with +/− weights
```

Each iteration yields one batch of changes — every row the last step touched, weighted +/−.
This is deliberate. A changefeed is bounded work per update regardless of result size, and it
composes with wherever the data is going next (a database, a cache, a UI, another system).

The contract:

- **One consumer per output.** An output is a single bounded channel; a second `Changes()`
  subscription throws rather than silently splitting the feed between competitors. Need the
  same data twice? Declare two outputs on the same stream.
- **Table-shaped outputs resync automatically.** Batches emitted between the last checkpoint
  and a crash are never replayed, and a consumer that subscribes mid-run has missed
  everything earlier — so on an output of a keyed or windowed result, `Changes()` always
  begins with one batch asserting the current integrated state (empty on a fresh start),
  then follows with live batches. Treat that first batch as a reset. Correctness across
  restarts is the default, not an option you have to know to ask for. The output maintains
  checkpoint-backed snapshot state to serve this — O(live rows), bounded because the result
  is a table; `ToOutput(bootstrap: false)` opts out when the consumer is stateless and the
  state cost isn't wanted.
- **Stream-shaped outputs are pure changefeeds.** An output of a plain `ZStream` (append-heavy
  events headed to a broker, say) has no meaningful "current state" to resync — maintaining
  one would be unbounded durable state. Across a crash these are at-most-once; exactly-once
  delivery is the downstream consumer's offset/idempotency concern.

When you want a live table in memory, opt in — the name says what you are buying:

```csharp
using var view = output.MaterializeInMemory();   // holds the integrated result in memory
foreach (var entry in view.Current())            // consistent snapshot, on demand
    Console.WriteLine($"{entry.Row} ×{entry.Weight}");
```

`MaterializeInMemory` is that output's single consumer, and it needs a table-shaped output —
the automatic resync is what makes it correct across restarts and mid-run attachment. It keeps the fully integrated result set on the heap for the
lifetime of the view — O(live rows) memory: fine for windowed/evicted results and small
dimensions, a deliberate decision for anything with an unbounded keyspace. `Current()` is
taken on demand — nothing re-emits the full result per update — and yields each row once with
its `Weight` rather than duplicating multi-weight rows.

> **TODO:** point-lookup views (query a key's current value without materializing the whole
> result) — design pending.

## Running a circuit

```csharp
var builder = new CircuitBuilder(new CircuitOptions
{
    Workers = 4,                                  // parallel workers, state sharded by GroupBy keys
    Storage = new RedisStorageProvider(redis, …), // default: in-memory
    CheckpointInterval = 1,                       // steps between checkpoints
});
// … declare the dataflow …
await using var circuit = builder.Build();        // compiles + validates the graph
circuit.Start();                                  // restores from the latest checkpoint if present
```

Execution is step-based: workers ingest bounded batches from inputs, run them through the
graph in lockstep, and emit output batches. Backpressure is bounded channels end-to-end — and
because steps are lockstep, **one slow output consumer stalls the whole circuit**: all
inputs, all other outputs, and watermark progress wait for it. That is the deliberate price
of bounded memory; there is no unbounded buffering to hide behind. It is also why you always
await your consumers (see the first example): an abandoned consumer eventually fills its
channel and blocks disposal.

State lives in a pluggable storage provider (in-memory or Redis today). Checkpoints commit
operator state and input offsets atomically, so a restarted circuit resumes where it left
off; table-shaped output consumers re-sync automatically (their first batch is the current
state). If you rename or restructure a
circuit, pin operator identity with `.Named("…")` on the stages that carry state you want to
survive the change.

### Running it fast

A circuit holds a large, mostly-static live set and allocates a little on every step, which
is the workload shape the .NET GC defaults handle worst: the garbage collector's share of
process CPU can exceed the workers'. A few settings in your project file are worth more than
any code change, and none of them is something the library can set for you — they are read
before your `Main` runs:

```xml
<PropertyGroup>
  <ServerGarbageCollection>true</ServerGarbageCollection>
  <ConcurrentGarbageCollection>true</ConcurrentGarbageCollection>
  <GarbageCollectionAdaptationMode>0</GarbageCollectionAdaptationMode>   <!-- DATAS off -->
</PropertyGroup>
<ItemGroup>
  <!-- Heap count: match CircuitOptions.Workers, not the core count. -->
  <RuntimeHostConfigurationOption Include="System.GC.HeapCount" Value="4" />
  <!-- Belt-and-braces: keep occasional medium arrays (input batches, store growth)
       off the LOH, where their churn would trigger gen2 collections. -->
  <RuntimeHostConfigurationOption Include="System.GC.LOHThreshold" Value="1048576" />
</ItemGroup>
```

Server GC otherwise gives one heap per logical core, so a 4-worker circuit on a 20-thread
machine collects 20 heaps with 20 threads, most of them for memory no worker ever touches.
DATAS (on by default for server GC since .NET 9) then keeps trimming the heap, which against
a large static live set is close to continuous gen2 work. On the throughput rig the pair is
worth 16.6 µs → 11.6 µs of CPU per record, paid for in working set (2.2 GB → 2.6 GB).

The other lever is yours, not the runtime's: **declare small row and accumulator types as
`readonly record struct`**. The library's own machinery reuses its buffers, so at
throughput most remaining allocation is the records *your* lambdas return — and a linear
aggregate constructs a fresh accumulator on every fold, because records are immutable. A
small struct (`Spend(long Orders, decimal Amount)`) lives inline in operator state and
Z-set entries and never touches the heap, while keeping the value equality Z-sets need and
the JSON shape Redis state needs. Keep wide rows (many fields, several strings) as
reference records: there, one reference is what every probe and entry copy moves. The
showcase's `Records.cs` and `Aggregates.cs` show the split.

If the worker count is only known at launch — a command-line flag, a container knob — set
`DOTNET_GCHeapCount` in the environment instead of the runtimeconfig option. **It is read as
hexadecimal**, so twelve heaps is `DOTNET_GCHeapCount=c`.

Every example in this repo ships with this applied; see `examples/Directory.Build.props` and
the `<CircuitWorkers>` property in each example's `.csproj`.

> **TODO:** hosting & DI integration (`services.AddIncremental(…)`, `IHostedService`) — planned,
> not yet designed.
> **TODO:** cluster deployment (multi-process exchange over a transport), checkpoint
> operations guide, observability/metrics reference. The engine supports all three today;
> consumer-facing docs pending.

## Escape hatches

The declarative surface compiles onto a small set of primitives, all of which remain public
for the cases the paved road doesn't cover:

- **Window-as-data**: windows are just records — `SelectMany` an event into
  `Windowed<T>` values and `GroupBy` a composite key manually when you need non-standard
  window shapes. You then own eviction:
- **`EvictWhen(key => closesAt)`** on any grouping: declare when a key's state can be
  dropped, for keyed state with a lifecycle that isn't window-derived.
- **`GroupBy(selector, stableHash: …)`**: custom routing hash for exotic key types.
- **`Minus` / `Negate`**: pointwise weighted subtraction and negation of streams
  (deliberately not called `Except` — results can carry negative weights, which is exactly
  the point).
- **`WindowCloses()`** on a grouping: the raw close-notification stream `OnClose()` is
  built from — each key announced exactly once (weight +1, never retracted) when the
  watermark passes its deadline — for close-driven pipelines the paved road doesn't cover.
- **`OutputHandle.Reader`**: the output's raw channel (one batch per completed step, no
  bootstrap, no consumer claim) for step-level consumption — tests and instrumented
  consumers.
- **Recursion** (`Recurse`): fixed-point iteration for transitive-closure-style computations.
- **Weighted sends**: `InputHandle.SendAsync(value, weight)` for pre-weighted deltas from
  systems that already speak changefeeds (e.g. CDC).

These are documented in the API reference rather than here; if you find yourself reaching for
them routinely, that's design feedback we want to hear.

---

## Appendix: mapping from the 0.1 surface *(delete after migration)*

`CircuitBuilder`/`Circuit`/`CircuitOptions` keep their names (a `Dataflow` rename was
considered and rejected: TPL Dataflow owns that word in .NET).

| 0.1 | 0.2 |
| --- | --- |
| `Map` / `Filter` / `FlatMap` | `Select` / `Where` / `SelectMany` |
| `Plus` | `Concat` (duplicate-preserving, matching LINQ `Concat`) |
| `Minus` / `Negate` | kept, advanced (**not** renamed `Except` — LINQ semantics differ) |
| `WithWatermark(sel, allowedLateness)` | `WithEventTime(sel, lateness)` → `EventTimeStream<T>` |
| `IndexBy(key)` → `KeyedZStream` | `GroupBy(key)` → `GroupedZStream` (staging; exchange insertion unchanged) |
| `TumblingWindow`/`HoppingWindow` on raw streams | removed; windows live on event-timed groupings only |
| `SessionWindow(sel, key, gap)` | `GroupBy(key).SessionWindow(gap)` |
| manual `IndexBy(w => (w.WindowStart, key))` + `WithEviction` | implicit in `GroupBy(…).XWindow(…)` |
| `WithEviction(closesAt)` | `EvictWhen(closesAt)` (escape hatch) |
| `WindowCloses()` | `OnClose()` on windowed results (emit-on-close, no flapping) |
| `Output(name)` + `circuit.Output<T>(name)` | `ToOutput()` + `circuit.Handle(def)` (outputs unnamed) |
| `Output(name, snapshot: true)` | automatic on table-shaped outputs (`ToOutput(bootstrap: false)` to opt out) |
| `circuit.Input<T>(name)` string lookup | `circuit.Handle(def)` |
| `OutputHandle.ReadAllAsync()` | `OutputHandle.Changes()` (single consumer, enforced) |
| hand-rolled integration loops | `MaterializeInMemory()` (opt-in, cost in the name) |
| `MapValues` / `Values` | kept on keyed/windowed results (`Select` overloads where unambiguous) |

Implementation notes (not consumer-facing):

- The execution model is untouched: same operators, same exchange/barrier discipline, same
  storage and checkpoint machinery. This is a surface rewrite that compiles to the same
  graph. `EventTimeStream` needs no engine change: it carries the timestamp selector at
  build time and applies it at the window site (which is why `Select` drops it — the
  selector no longer typechecks against the reshaped record).
- `GroupBy` must carry the exchange + stable-hash discipline of `IndexBy` unchanged.
- Windowed **aggregation** routes by the user key only (all windows of a key on one worker;
  eviction worker-local). Window-collapsing regroupings — `TopK`, window-sticky `GroupBy` —
  re-exchange keyed by the window, so `DateTimeOffset` is part of the stable-hash contract,
  and each window's ranking work lands on a single worker (as in 0.1's LogAnalytics example).
- Table-shaped outputs' automatic resync builds on 0.1's `SnapshotOperator` (checkpointed
  view state). The opt-out lives at `ToOutput`, not at `Changes()`: snapshot state must
  exist *before* a crash to serve the resync after it, so it cannot be a subscribe-time
  choice. Stream-shaped outputs never maintain it — the integrated result of an append-only
  stream is unbounded.
- Restructuring changes positional store names → bump the state-manifest format version
  (no deployed checkpoints exist; the bump is hygiene).
- Decisions settled during implementation: windowed rows are the flat
  `WindowedValue<TKey,TValue>` record and unwindowed keyed rows stay
  `KeyedValue<TKey,TValue>` (their equality and serialized shape are public contract, see
  "Your records are the contract"); the composite state key is `WindowKey<TKey>` and
  window-collapsing ops route by `WindowBounds` (both public — they name checkpointed
  state); the event-timed types are `EventTimeStream<T>` / `GroupedEventTimeStream<TKey,T>`;
  `TopK` output shapes are unchanged; `GroupedZStream.Distinct()` returns keyed rows
  (continue with `.Grouped()` for a distinct count).
- Still open: the spelling for collapsing a windowed result to the window alone
  (aggregate-of-aggregates — window-sticky `GroupBy` to a unit key works but is ugly;
  `TopK` doesn't need it, it ranks per window natively); the keyed-input restart mechanism
  (checkpoint the upsert map vs. derive retractions from operator state); watermark
  idleness handling; whether required input names can go too — today they are the durable
  key for checkpointed source offsets (a reference dies with the process, positional
  identity would re-bind offsets across topology edits), but a derived-with-optional-pin
  scheme like store naming may be possible.
