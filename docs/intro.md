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
  │  Lookup(table, key, …) · LookupAppendOnly(table, key, …)   enrich from a shared table
  │  ToTable(table)                                            write a shared table (rows are records)
  │
  ├─ GroupBy(keySelector) ──▶ GroupedZStream<TKey, T>          a staging type
  │     │  aggregates: Count · Sum · Min · Max · TopK · Aggregate(…)
  │     │  joins:      Join · LeftJoin · FullJoin · AntiJoin   (on the shared key)
  │     │  tables:     Join · LeftJoin · Lookup                (against builder.Table(…))
  │     ▼
  │  aggregate result: KeyedZStream<TKey, TValue>              rows (Key, Value)
  │     │  MapValues · Values · Grouped() — continue on the same key, no reshuffle
  │     │  ToTable(table, (key, value) => row) — write a shared table
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
              │  MapValues · Where · GroupBy (window-sticky)
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
`SelectMany`, `GroupBy`, `Distinct`): it is the dialect .NET developers already speak, and
because `ZStream` is not an `IEnumerable`, none of LINQ's own operators apply to it — the
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
  with a zero. So `Count()` never emits 0 for an emptied group — the row for that group is
  retracted instead.
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

var enriched = trades
    .GroupBy(t => t.InstrumentId)
    .Join(instruments.GroupBy(i => i.Id),
          (_, trade, instrument) => new EnrichedTrade(trade, instrument.Symbol, instrument.Currency));
```

`Join` is an incremental equi-join: when either side changes, only affected pairs are
re-emitted. The result selector receives the shared key first. Reference data is not a
special case — it is just another stream — and `LeftJoin`/`FullJoin`/`AntiJoin` cover the
outer and negative cases. When an instrument row is *upserted*, the old enriched rows are
retracted and new ones asserted automatically — historical results re-attribute themselves.

> **TODO:** windowed / interval joins (stream-stream within a time bound) — not yet designed;
> today this is manual via the escape hatches.

### Joining shared tables

A reference stream joined this way is stored by every join that reads it, in every circuit.
When the same reference data serves many circuits — or is simply large — hold it once per
process in a `SharedTable` instead and attach it wherever it is needed:

```csharp
// Once per process. One writer; a commit publishes everything staged as one version.
var instruments = new SharedTable<int, Instrument>("instruments", i => i.Id);
await instruments.Writer.UpsertAsync(new Instrument(7, "ACME", "USD"));
await instruments.Writer.CommitAsync();

// In each circuit.
var inst = builder.Table(instruments);                 // TableRef<int, Instrument>

var enriched = trades
    .GroupBy(t => t.InstrumentId)
    .Join(inst, (_, trade, instrument) => new EnrichedTrade(trade, instrument.Symbol, instrument.Currency));
```

A table is a staging type like a grouping: you join against it, and it is not itself a
stream. The join above behaves exactly like the stream join — change the instrument and
every trade joined to it is retracted and re-asserted — but the circuit stores only the
trades. `LeftJoin(inst, matched, unmatched)` is the outer form. Each step reads one
*version* of the table, the latest when the step began, so a step never sees half a commit;
that version is checkpointed with the input offsets, and a restarted circuit picks up the
changes it missed. A circuit built after the table was loaded needs nothing special: its
first trades find the rows already there.

**Enrich once instead.** Retroactive correction has a price — when a hot row changes, every
trade ever joined to it is re-emitted. `Lookup` enriches each record as it arrives and
leaves it alone afterwards:

```csharp
var enriched = trades.Lookup(
    inst,
    t => t.InstrumentId,
    (trade, instrument) => new EnrichedTrade(trade, instrument.Symbol, instrument.Currency),
    missing: trade => new EnrichedTrade(trade, "?", "?"));   // omit to drop unmatched records
```

"As it arrives" means as of the step that processed it; it is not event time. Retractions
still work, because the lookup remembers what it emitted for each record: amend or cancel a
trade and exactly the row it produced is taken back, whatever the table says by then, and
the amended trade is enriched afresh. That memory is one entry per live record. Bound it
the usual way — `trades.GroupBy(t => t.InstrumentId).EvictWhen(…).Lookup(inst, …)` is the
same operator on a grouping you control — or, for a stream that only ever inserts (ticks,
events, log lines), use `LookupAppendOnly`, which keeps nothing and faults the circuit if a
retraction ever reaches it.

**Other columns.** To join on something that is not the table's key, ask the table for an
index. It is maintained with the rows, shared by every circuit that asks for the same name,
and may hold several rows per value — a join then yields one result per row, and a left
join's unmatched row appears only while there are none:

```csharp
var byIsin = inst.By("isin", i => i.Isin);            // null from the selector = not indexed
quotes.GroupBy(q => q.Isin).Join(byIsin, (isin, quote, instrument) => …);
quotes.Lookup(byIsin, q => q.Isin, (quote, instrument) => …);
```

**The table as a stream.** `inst.Changes()` is the table's own rows as a `ZStream` — the
old row retracted and the new one asserted on every change — for aggregating or re-keying
reference data. In a new circuit it begins with every row the table already holds; a
circuit restored from a checkpoint resumes where it left off.

A table never completes. An auto-stepping circuit ends, as always, once its inputs have
completed and been drained — the tables it reads are not waited for — and a circuit whose
only inputs are tables runs until it is disposed.

**Derived tables.** A circuit can write a table as well as read one: `ToTable` makes a
table-shaped result the contents of a shared table, for other circuits to join. A chain of
dependent joins then costs each pipeline one hop — `traders ⋈ desks` is computed once,
and every pipeline joins trades to the result:

```csharp
var traderDesks = new SharedTable<int, TraderDesk>("trader-desks", td => td.TraderId);

// A small circuit of its own, not part of any pipeline.
derive.Table(traders).Changes()
      .GroupBy(t => t.DeskId)
      .Join(derive.Table(desks), (deskId, trader, desk) => new TraderDesk(trader.Id, trader.Name, desk.Region))
      .ToTable(traderDesks);                               // keyed by the table's own key

// An aggregate's result, one row per key.
orders.GroupBy(o => o.CustomerId).Count()
      .ToTable(orderCounts, (customerId, count) => new OrderCount(customerId, count));

// Any pipeline, as with any table.
trades.GroupBy(t => t.TraderId).Join(pipeline.Table(traderDesks), (id, trade, td) => …);
```

- **One row per key.** Each step's changes are collapsed to one upsert or delete per key
  and committed as one table version; a step that changes nothing publishes nothing. The
  stream must be table-shaped — a key's old row retracted in the step that asserts its new
  one. A second row for a key, in the same step or a later one, or a retraction of a row
  the table does not hold, faults the circuit.
- **The circuit is the table's only writer** from `Build()` until it is disposed or faults;
  `table.Writer` throws meanwhile. The table holds what the circuit computes and nothing
  else: the first step of a circuit that starts with no checkpoint removes any row it does
  not produce itself. Because `Changes()` begins with a table's current rows, a derived
  table can be added while its sources are already loaded.
- **Crash consistency.** The table version is committed before the circuit's checkpoint
  (commit after delivery, below), and the checkpoint records which version it covers. A
  restarted circuit first takes back what it wrote after that version, then writes the
  replayed steps — in one version, so a replay that repeats the lost step changes nothing.
  The table always equals what the circuit's state describes. Readers may see a row step
  back and forward again: a `Join` against the table corrects itself, a `Lookup` does not.
  `circuit.CommittedVersion(table)` is the version the checkpoint covers; a table's log
  must keep its history back to there.
- **Restarts need the table and the circuit to agree.** A table opened over a log
  (`SharedTable.OpenAsync`) and a checkpointed circuit resume together. A table made with
  `new SharedTable(…)` is new every process start, so its circuit must start from empty
  state too and recompute it; a circuit resuming a checkpoint against a recreated table
  refuses to start, as does one given `ToTable` after its first checkpoint.
- **Backpressure.** A table with no log makes its writer wait for a reader that has fallen
  a full tail behind. For a derived table the writer is a circuit, so that circuit stops,
  and its inputs fill, until the reader steps.
- **Not allowed** (`Build()` throws): reading a table the circuit writes, directly or
  through other derived tables; a second circuit writing the same table; a clustered
  circuit. Nor inside `Recurse`.

**Feeding a table.** `UpsertAsync`/`DeleteAsync` stage, `CommitAsync` publishes; a commit
that changes nothing publishes nothing. The offset-stamped overloads and
`table.CommittedOffsets` give a replayable source the same exactly-once resume as a circuit
input. `ReplaceAllAsync(rows)` is snapshot sync: it upserts the snapshot in chunks and then
deletes every key the snapshot did not mention. Rows must be immutable (records with
init-only members): every circuit in the process is handed the same objects.

Limits today: a table lives in one process, so a clustered circuit cannot attach one;
table joins and lookups are not available inside `Recurse`; and a table built with
`new SharedTable(…)` starts empty with every process — `SharedTable.OpenAsync(…, log)`
restores from a change log, of which only the in-memory one exists so far.

## Inputs and outputs

**Inputs.** `builder.Input<T>(name)` gives an insert/retract stream. Awaiting `InsertAsync`
means "accepted into the input buffer" (it applies backpressure), not "processed". Names are
required on inputs: they anchor durable identity for checkpointed source offsets
(`InsertAsync(value, offset)` for exactly-once resumption from replayable sources).

`builder.KeyedInput<TKey, TValue>(name, keySelector)` gives upsert semantics — `UpsertAsync`
replaces the current row for a key (the circuit emits the retraction of the old row itself)
and `DeleteAsync` removes it. The current row per key is circuit state, checkpointed under
the input's name, so a keyed input restarts cleanly: re-upserting rows the recovered state
already holds changes nothing (re-feed the whole table), and the offset-stamped overloads
replay exactly-once from `CommittedOffsets`. Several commands for one key within a step
resolve last-write-wins; a key needs one logical producer. A key deleted at the source while
the circuit was down is only retracted if the re-feed sends its delete — keyed inputs have
no snapshot-sync `ReplaceAllAsync`; shared tables (under *Joins*) do.

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

### Acknowledged outputs: at-least-once to a sink

A changefeed that loses the batches between a checkpoint and a crash is the wrong tool for
writing to a sink that must not miss anything. For that, declare the output acknowledged:

```csharp
var enriched = trades.Join(…).ToAcknowledgedOutput();          // AcknowledgedOutput<Row>
// …
await circuit.Handle(enriched).ConsumeAsync(async (batch, ct) =>
{
    await sink.UpsertAsync(batch.Sequence, batch.Changes, ct);  // return = delivered
});
```

Each `OutputBatch<T>` carries the step's `Changes` and its `Sequence`. `ConsumeAsync`
acknowledges a batch when your delegate returns; if you would rather drive it yourself, read
`Batches()` and call `batch.Acknowledge()` (or `handle.Acknowledge(sequence)` — an
acknowledgement covers every earlier batch too, so a sink that confirms in groups
acknowledges the last one).

What changes in the circuit is the order of two things. Normally a step commits its
checkpoint and then publishes. With an acknowledged output — and a checkpointing storage
provider — it publishes first and **commits only once every batch published so far has been
acknowledged**. The checkpoint therefore never runs ahead of what the sink holds, and a
crash loses nothing: the restarted circuit re-runs the steps since its last checkpoint and
delivers their batches again. The contract:

- **At-least-once.** After a crash a sink sees some batches twice. Apply them idempotently
  (upserts by key do), or use the sequence: sequences are step numbers, committed with every
  checkpoint and continued after a restart, so a batch whose `Sequence` is not above the
  highest one already applied is a redelivery. `Circuit.CommittedStep` is the sequence the
  last checkpoint reflects; everything above it is what a restart delivers again.
- **Redelivery repeats sequences, not necessarily contents.** An auto-stepping circuit may
  cut the replayed input into different steps, so a redelivered batch can differ from the
  one that first carried its sequence. The replayed batches still add up to the same
  change, which is why keyed upserts converge. With manual stepping and the same step
  boundaries, the batches are identical.
- **The circuit waits for the sink at each commit point** — every `CheckpointInterval`
  steps — and runs no step while it waits; inputs back up behind it, bounded as ever. At
  `CheckpointInterval = 1` that is one sink round trip per step. To let steps flow between
  commits, raise the interval and set `CheckpointMaxDelay` (below): the replay window is
  then bounded in time. `incremental.circuit.sink_ack_wait` measures what the waiting
  costs. With manual stepping, a `StepAsync` that commits returns only after the
  acknowledgement, so the consumer must be running concurrently.
- **A consumer that goes away faults the circuit.** Ending `Batches()` early — cancelling
  it, breaking out, throwing from the loop or from the `ConsumeAsync` delegate — means the
  remaining batches can never be confirmed, so the circuit faults instead of waiting for
  ever, without committing past the unconfirmed batch. A sink that is merely slow or down
  and retrying stalls its circuit and faults nothing.
- **No state, no bootstrap, one consumer.** An acknowledged output keeps no snapshot and
  has no "batch zero"; redelivery is what makes it correct across restarts. It works on any
  stream, keyed and windowed results included.
- **Every output of that circuit is published before the commit.** A plain `ToOutput()`
  beside an acknowledged one holds no commit up, but after a crash its consumer can also
  see the replayed steps' batches again (as it already can with `CheckpointInterval > 1`).
  Table-shaped outputs resync on their bootstrap batch regardless.
- Not available on clustered circuits, which must commit on the same steps on every process.

### Sinks: keyed upserts and appends

Most sinks want one of two shapes, and both are built on the acknowledged output, so you
write the sink and not the loop around it:

```csharp
var totals = orders.GroupBy(o => o.Region).Sum(o => o.Amount).ToAcknowledgedOutput();
// …
await circuit.Handle(totals).DeliverToAsync(new RegionTotalsSink(db));

sealed class RegionTotalsSink(Db db) : IUpsertSink<string, long>
{
    public async ValueTask WriteAsync(UpsertBatch<string, long> batch, CancellationToken ct)
    {
        // batch.Upserts: key → row as of this step.  batch.Deletes: keys with no row now.
        await db.ApplyAsync(batch.Upserts, batch.Deletes, ct);   // return = durable
    }
}
```

- **`IUpsertSink<TKey, TRow>`** is for table-shaped results. Each step's delta is collapsed
  to at most one upsert or delete per key — a row that changed arrives as one upsert of the
  new row, not a delete and an insert — and the step is acknowledged when `WriteAsync`
  returns. A keyed result (`KeyedValue<TKey, TRow>`) is keyed by its key; for any other
  stream pass a key selector: `DeliverToAsync(sink, row => row.Id)`. The stream must hold
  one row per key at a time: a step that leaves a key with two live rows faults the circuit
  rather than let the sink keep one of them at random.
- **`IAppendSink<T>`** is for insert-only streams (events, enriched facts). `AppendBatch<T>`
  carries the step's records and its `Sequence`; a retraction on the stream faults the
  circuit, because an append sink cannot take a record back.
- **Failures.** A write that throws is retried with a doubling backoff
  (`SinkDeliveryOptions`: `MaxRetries` 5, `InitialBackoff` 100 ms, `MaxBackoff` 5 s,
  `OnFailedAttempt` to log). When the retries run out the circuit faults without committing
  past the batch, and a restart delivers it again. `MaxRetries = null` retries for ever: the
  pipeline stalls behind a sink that is down and resumes when it is back.
- **Backpressure.** One batch is with the sink at a time and nothing is buffered beyond the
  output's bounded channel, so a slow sink slows the circuit and, through it, the producers.
- **`InMemoryUpsertSink<TKey, TRow>` and `InMemoryAppendSink<T>`** are the reference
  implementations, for tests.

What at-least-once means for each, precisely:

- *Upserts.* After a crash the steps since the checkpoint are delivered again. With the same
  step boundaries (manual stepping) the batches repeat exactly and applying them twice
  changes nothing. An auto-stepping circuit may cut the replay into different steps; a key
  can then step back to an older row and forward again, which converges. One case does not:
  a key whose row was delivered before the crash and is gone again by the end of a merged
  replay step nets out to no change, so nothing tells the sink to delete it, and the row
  stays until that key next changes. Where a transient key left behind matters, step
  manually, or reconcile the sink against a table-shaped `ToOutput()` snapshot.
- *Appends.* A redelivered batch repeats its sequence, so with the same step boundaries the
  sink skips (or replaces) a sequence it already has. Under auto-stepping a replayed record
  can come back under a different sequence. The rule that is always exact: on restart,
  before delivery resumes, drop what the sink holds above `circuit.CommittedStep`
  (`InMemoryAppendSink.TruncateAfter`). A sink that cannot drop — a topic — should carry
  the sequence on its records and leave deduplication to its readers.

When you want a live table in memory, opt in — the name says what you are buying:

```csharp
using var view = output.MaterializeInMemory();   // holds the integrated result in memory
foreach (var entry in view.Current())            // consistent snapshot, on demand
    Console.WriteLine($"{entry.Value} ×{entry.Weight}");
```

`MaterializeInMemory` is that output's single consumer, and it needs a table-shaped output —
the automatic resync is what makes it correct across restarts and mid-run attachment. It
keeps the fully integrated result set on the heap for the lifetime of the view — O(live
rows) memory: fine for windowed/evicted results and small dimensions, a deliberate decision
for anything with an unbounded keyspace. `Current()` is taken on demand — nothing re-emits
the full result per update — and yields each row once with its `Weight` rather than
duplicating multi-weight rows.

> **TODO:** point-lookup views (query a key's current value without materializing the whole
> result) — design pending.

## Records that cannot be processed

By default a delegate that throws — a `Select`, a `Where`, a key selector, an aggregate —
faults the circuit, and so does a null key. That is the safe default: nothing is dropped
silently. It also means one malformed record stops the pipeline. The alternative is to set
the record aside and carry on:

```csharp
var builder = new CircuitBuilder(new CircuitOptions { ErrorPolicy = RecordErrorPolicy.DeadLetter });
// …
await foreach (var letter in circuit.DeadLetters.ReadAllAsync())
    log.LogWarning(letter.Exception, "{Operator} dropped {Record} ({Weight})",
        letter.Operator, letter.Record, letter.Weight);
```

A `DeadLetter` carries the record, its signed weight, the operator that dropped it (its
state name: `n3.select`, or `named.enrich.join` if you named it), the exception, and the
step. `incremental.circuit.dead_letters` counts them, tagged by operator. Override the
policy for one operator with `.OnError(…)` on the stream it returns:
`orders.Select(Parse).OnError(RecordErrorPolicy.DeadLetter)` in a circuit that otherwise
faults, or `.OnError(RecordErrorPolicy.Fault)` on the one operator that must never drop.

The rules:

- **A dropped record's retraction must be dropped too**, or it would take away something
  that was never added. For stateless operators that follows from the delegate being
  deterministic: what threw for the insertion throws for the retraction, and both are
  reported, each with its own weight — so an operator's dead letters are themselves a
  stream of changes, and integrating them gives the bad records present right now. A
  delegate that throws for a record at one time and not at another breaks retraction just
  as a non-deterministic `Select` already does, and a stateless operator cannot detect it.
  Operators with state do not rely on it: an aggregate whose function throws leaves the key
  with no result (retracting the one it had) until a later change mends the group, and a
  remembered `Lookup` remembers that it dropped a fact, so the retraction takes nothing away.
- **What can dead-letter:** `Select`, `Where`, `SelectMany`, `MapValues`, `GroupBy`'s key
  selector (throwing or returning null), `WithEventTime`'s selector, `Aggregate`, `Sum`,
  `Min`, `Max`, the result selectors of `Join`, of the stream `LeftJoin`/`FullJoin` and of the
  table `Join`/`LeftJoin`, `Lookup` and `LookupAppendOnly` (which also reports a retraction
  instead of faulting), and a keyed input's key selector. `OnError` on a stream
  `LeftJoin`/`FullJoin` covers all of that join's selectors. A delegate returning null where a row is required counts as
  throwing. **What still faults:** `TopK`'s ordering, `EvictWhen`/`WindowCloses` deadlines,
  session windows, custom routing hashes, a linear aggregate's `Combine`, anything inside
  `Recurse` that is not one of the operators above, and every failure that is not about one
  record (storage, a sink, the engine).
- **Keyed inputs.** Under `DeadLetter` an `UpsertAsync` whose key selector throws or returns
  null does not throw to the producer: the record travels to the circuit, is reported as a
  dead letter from `input[name]`, and its offset is consumed like any other. This follows
  the circuit's policy only. `DeleteAsync(null)` is still an argument error.
- **Records a source could not read.** A source adapter that cannot turn a record into a
  value — it does not deserialize — calls `handle.RejectAsync(record, exception, partition,
  offset)` instead of `InsertAsync`. The rejection takes the record's place in the feed: its
  offset is consumed and checkpointed, so a restart does not meet it again. Under
  `DeadLetter` it is reported from `input[name]` with whatever the source passed as the
  record (a `KafkaSource` passes a `KafkaRecord`: topic, partition, offset, key and value
  bytes); under `Fault` it faults the circuit.
- **Positions with no record.** `handle.AdvanceAsync(partition, offset)` — on an input, a
  keyed input, and as `table.Writer.AdvanceAsync(partition, offset)` on a shared table —
  says the source has read that far with nothing to hand on: an offset that holds no
  record (a Kafka transaction's marker), a record that means nothing here. The position is
  consumed and checkpointed like a record's, so a restart does not go back to offsets the
  source may no longer hold. For a table the next `CommitAsync` records it as a version
  with no changes.
- **The dead-letter output is bounded and must be read.** It is one channel of
  `DeadLetterCapacity` (1024) entries, published after each step like any output. Nobody
  reading it stalls the circuit once it is full — backpressure, not unbounded memory.
  `circuit.DeadLetters` throws if nothing in the circuit has the `DeadLetter` policy.
- **Delivery.** Dead letters are published like a plain output: after the checkpoint
  commits, so a crash can lose the ones from the last step — unless the circuit has an
  acknowledged output, in which case everything is published before the commit and a
  restart reports them again, under the same `Step`. Published is not stored, though:
  the channel can hold them when the process dies.
- **Dead letters that must not be lost.** `CircuitOptions.AcknowledgeDeadLetters` makes
  the dead-letter output an acknowledged one. Dropped records then arrive a step at a
  time on `circuit.DeadLetterBatches`, and the checkpoint waits until the reader has
  acknowledged each batch (`batch.Acknowledge()`), exactly as it waits for a sink — so a
  record is in the reader's store before the checkpoint that passes it, and is dropped
  and reported again after a crash before that. `circuit.DeliverDeadLettersToAsync(sink,
  letter => …)` does the reading for any `IAppendSink<T>`, with the sink retries; when
  they run out the circuit faults with the sink's failure in its message rather than
  commit past the batch.
- **Cost.** Under `Fault` the operators run the loops they always ran; there is no
  exception handling on that path. Under `DeadLetter` a record that does not throw costs a
  `try` region and allocates nothing.

## Running a circuit

```csharp
var builder = new CircuitBuilder(new CircuitOptions
{
    Workers = 4,                                  // parallel workers, state sharded by GroupBy keys
    Storage = new RedisStorageProvider(redis, …), // default: in-memory
    CheckpointInterval = 1,                       // steps between checkpoints
    CheckpointMaxDelay = null,                    // …or at most this long uncommitted (TimeSpan)
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

State lives in a pluggable storage provider (in-memory, Redis, or in memory with a
changelog). Checkpoints commit
operator state and input offsets atomically, so a restarted circuit resumes where it left
off; table-shaped output consumers resync automatically (their first batch is the current
state). If you rename or restructure a circuit, pin operator identity with `.Named("…")` on
the stages that carry state you want to survive the change. A circuit whose `Start()`
throws — the checkpoint does not match it, its storage is a standby, a table it writes
cannot be resumed — is finished: it cannot be stepped or started again, and disposing it
processes and commits nothing. Deal with the cause and build a new one.

`ChangelogStorage` is the provider for a service that needs a warm standby. State stays on
the heap, and each checkpoint appends what changed to a shared log:

```csharp
// The active process. One storage per process; one provider per circuit, by a stable id.
await using var storage = await ChangelogStorage.OpenAsync(changelog);   // restores, takes the writer
var builder = new CircuitBuilder(new CircuitOptions { Storage = storage.Pipeline("orders-by-region") });
```

`changelog` is an `IStateChangelog` — in production a compacted Kafka topic (`log.State`
of a `KafkaLog`, below), in tests `InMemoryStateChangelog`. A commit writes the entries the steps touched, not
the state, plus one commit record with the offsets. The standby runs the same code with two
differences: it follows instead of opening, and it does not start its circuits.

```csharp
// The standby: same tables, same circuits, nothing started.
await using var storage = ChangelogStorage.Follow(changelog);            // keeps applying commits
var instruments = await SharedTable<int, Instrument>.FollowAsync("instruments", i => i.Id, tableLog);
var circuit = Build(storage.Pipeline("orders-by-region"), instruments);  // Build(), not Start()

// Takeover — tables before circuits.
await instruments.PromoteAsync();     // reads the table log to its end, becomes its writer
await storage.PromoteAsync();         // fences the old active, reads the changelog to its end
circuit.Start();                      // finds the last checkpoint already in memory
```

After `Start()` the circuit is exactly where the old active's last checkpoint left it:
`CommittedOffsets`, `CommittedStep` and `CommittedVersion` say where the sources resume, and
acknowledged outputs deliver again whatever was not yet checkpointed. Starting a circuit on
a storage that has not been promoted throws. The old active, if it is still alive, cannot
commit: its next checkpoint throws `ChangelogFencedException`, its circuits fault, and
`storage.WhenFenced` completes, which is the signal to exit the process.
`provider.StartNewIncarnationAsync()` discards a pipeline's state durably, for the cases
where a circuit is told to "start against an empty state store". Clustered circuits cannot
use this provider.

**On Kafka.** The `Incremental.Kafka` package supplies both logs. One `KafkaLog` per
process owns the one transactional producer everything durable is written through:

```csharp
await using var log = await KafkaLog.ConnectAsync(new KafkaLogOptions
{
    BootstrapServers = "b-1.example:9098,b-2.example:9098",
    TopicPrefix = "enrichment",                    // {prefix}.state, {prefix}.leader, {prefix}.table.{name}.*
    ClientConfig = new Dictionary<string, string>  // any librdkafka setting, for every client
    {
        ["security.protocol"] = "SASL_SSL",
        ["sasl.mechanism"] = "OAUTHBEARER",
    },
});

// The active.
await log.AcquireWriterAsync();                    // fences whoever wrote before
var instruments = await SharedTable<int, Instrument>.OpenAsync(
    "instruments", i => i.Id, await log.OpenTableLogAsync<int, Instrument>("instruments"));
await using var storage = await ChangelogStorage.OpenAsync(log.State);

// The standby: no AcquireWriterAsync, FollowAsync and Follow instead of the two opens.
// At takeover it is the first call:
await log.AcquireWriterAsync();
await instruments.PromoteAsync();
await storage.PromoteAsync();
circuit.Start();
```

- `ConnectAsync` creates the topics it needs and checks the ones that exist. The state
  topic has a fixed partition count (`StatePartitions`, 12 by default) that can never
  change; a topic with another count is refused. Set `ReplicationFactor` to 1 for a single
  local broker.
- Checkpoints and table versions from every circuit and table in the process queue for one
  writer thread, which commits whatever is waiting as one Kafka transaction. A commit
  completes when its transaction has. It fails in two ways only: `KafkaLogException` — the
  transaction was aborted, nothing of it is readable, the writer carries on — or fenced.
- Fencing is Kafka's: `AcquireWriterAsync` initialises the shared `transactional.id`
  (`{prefix}.writer` unless set), after which the previous holder cannot commit. Its next
  checkpoint throws `ChangelogFencedException`, its next table version
  `TableLogFencedException`, and `log.WhenFenced` completes. A broker that times a
  transaction out (`TransactionTimeout`, 60 s) looks the same to the client, so an outage
  longer than that also ends the active.
- A table's key type must serialize equal values to the same text — integers, strings,
  `Guid`s, enums, and tuples or records of them. `decimal`, `double` and `DateTimeOffset`
  keys are refused when the log is opened: Kafka compacts by key bytes.
- A standby can open a table's log only after the active has opened it once; until then
  `OpenTableLogAsync` waits.
- `table.Writer` over a Kafka log publishes a version once its transaction has committed,
  so a commit costs a round trip. Batch what arrived meanwhile into the next commit.
- `tableLog.TrimBeforeAsync(version)` deletes change history below a version. Nothing calls
  it for you yet; the floor is the lowest `CommittedVersion(table)` of any pipeline that
  must still be able to restart.

**Who leads.** `KafkaLeadership` decides which of two nodes is the active, using Kafka and
nothing else. The log's writer leaves a heartbeat in `{prefix}.leader` with every
transaction, and commits one by itself each `HeartbeatInterval` (1 s) when idle. Every
node starts as a standby and builds everything without starting it; the leadership takes
over when it should:

```csharp
await using var log = await KafkaLog.ConnectAsync(new KafkaLogOptions { /* … */ NodeId = "node-a" });

var leadership = new KafkaLeadership(log, new KafkaLeadershipOptions
{
    TakeoverAfter = TimeSpan.FromSeconds(3),                        // the default
    ProbeLeader = LeadershipProbe.Tcp("node-b", 7100),              // optional: "is the leader still there?"
    StartLeadingAsync = (term, ct) => { circuit.Start(); return Task.CompletedTask; },
    StopLeadingAsync = async ct => await circuit.DisposeAsync(),    // used by StepDownAsync
});
await using var probe = LeadershipProbe.Listen(leadership, new IPEndPoint(IPAddress.Any, 7100));

var tableLog = await log.OpenTableLogAsync<int, Instrument>("instruments");
var instruments = await SharedTable<int, Instrument>.FollowAsync("instruments", i => i.Id, tableLog);
var storage = ChangelogStorage.Follow(log.State);
leadership.Track(instruments, tableLog);     // promoted first at takeover, in this order
leadership.Track(storage);                   // then the storage; then StartLeadingAsync
var circuit = Build(storage.Pipeline("enrichment"), instruments);

leadership.Changed += status =>
{
    if (status.Role is NodeRole.Fenced or NodeRole.Resigned)
        Environment.Exit(3);                 // the supervisor restarts it as a standby
};
await leadership.StartAsync();
```

- **Roles.** `Standby → Promoting → Leader`, ending in `Fenced` (replaced, or the takeover
  failed) or `Resigned` (stepped down). Both ends are final for the process.
- **When a standby takes over.** The leader topic has been silent for `TakeoverAfter` and
  the probe, if there is one, does not answer as a leader; or silent for `TakeoverCeiling`
  (30 s) whatever the probe says; or the leader resigned; or `TakeOverAsync()` was called.
  The probe only prevents needless failovers when Kafka itself stalls. It never decides
  who may write: that is the `transactional.id`. Two standbys that decide together both
  try; one is fenced. A leader in the middle of one long commit is not silent: it keeps
  writing heartbeats into the open transaction, and a standby that sees the leader
  topic's end move waits, as for a probe that answers — so one commit may take up to
  `TakeoverCeiling`, not `TakeoverAfter`.
- **A first start is a takeover.** Nobody heartbeats, so the node leads after
  `TakeoverAfter`, or at once on `TakeOverAsync()`. A new table's log can only be opened
  by the writer, so on that first start `OpenTableLogAsync` completes during the
  takeover: start the build before `StartAsync` and await it in
  `KafkaLeadershipOptions.PrepareAsync`.
- **Commands.** `TakeOverAsync()` on a standby fences a live leader and replays what it
  had not committed. `StepDownAsync()` on the leader stops its circuits, commits a
  resignation and lets the standby take over at once with nothing to replay; calls made
  together share one step-down. Disposing the leadership in the middle of a takeover
  closes the log's writer and ends the node `Fenced`.
- **Being replaced.** A leader reads the new term in the leader topic within milliseconds;
  otherwise its next commit or heartbeat fails. Either way every later commit throws, so
  its circuits fault, and the role becomes `Fenced`.
- **Sinks.** Fencing covers what is written through the log, not an external sink.
  `leadership.Leased(sink)` makes every write wait for the lease: the leader's last
  committed transaction must have begun less than `TakeoverAfter` minus `LeaseMargin` ago,
  which is before any standby can have decided to replace it. The lease is timed on a
  clock that keeps counting through a host suspend (`CLOCK_BOOTTIME` on Linux); a paused
  virtual machine whose clocks stop with it is beyond any local clock. A sink that can
  compare terms can also store `leadership.Term` with each row and refuse an older one.
- **Status.** `leadership.Status` gives role, term, the leader's node id and term, the
  heartbeat's age, whether the lease is held, and a standby's lag — offsets behind per
  state partition, versions behind per table (`MeasureLagAsync()` measures it now).

**Reading from Kafka.** `KafkaSource` reads one topic into a circuit input, a keyed input
or a shared table. Start it after `circuit.Start()`, on the leader only (in
`StartLeadingAsync`), and dispose it before the circuit:

```csharp
var orders = new KafkaSourceOptions { BootstrapServers = "…", Topic = "orders" };

// A stream input: each record with a value is one insertion (JSON unless told otherwise).
await using var facts = await KafkaSource.StartAsync(circuit, ordersInput, orders);

// A keyed input: a value is an upsert, a record with no value (a tombstone) deletes its key.
await using var accounts = await KafkaSource.StartAsync(circuit, accountsInput, accountsTopic);

// A shared table: the source is the table's writer.
await using var loader = await KafkaSource.StartTableLoaderAsync(instruments, instrumentsTopic);
```

- **The position is the checkpoint.** Every record is handed on with its partition and
  offset. The source assigns every partition itself, one record after
  `circuit.CommittedOffsets(input)` — for a table, after `table.CommittedOffsets` — so a
  restart or a takeover re-reads exactly what the checkpoint does not cover. It joins no
  consumer group and commits no offsets. A topic never read before starts at `StartFrom`:
  `Earliest` (the default), or `Latest` — the end as it was at the first start, which is
  recorded, so later starts resume from it and never take "the end" again. A committed
  position that is no longer in the topic is `KafkaSourcePositionLostException`, never a
  silent jump: for a fact topic that is lost data. A table loader can instead re-read a
  compacted topic from its start and replace the table's rows
  (`ReloadTableWhenPositionLost`). Offsets that yield nothing — a transaction's marker, a
  tombstone on a stream input, a record the decoder passes over, a table change that
  changes nothing — move the position too: at once where there is a record, and for the
  rest once the source has stood at the end for `CommitIdlePositionAfter` (30 s), so that
  retention cannot overtake the position of an idle topic.
- **Backpressure.** Records go into the input's bounded channel one at a time. When the
  circuit is slow the write waits, the source stops polling, and the client stops fetching
  once it holds `FetchBufferKilobytes` (16 MB). A wait longer than `PauseAfter` (1 s)
  also pauses the partitions. Memory is the channel plus that buffer, whatever the size
  of the topic.
- **Formats.** A stream input takes a `KafkaDeserializer<T>`; a keyed input or a table
  takes a `KafkaChangeDecoder<TKey, TRow>`, which reads a record as an upsert, a delete
  or nothing. `KafkaDeserializers` has JSON (the default), UTF-8 text, raw bytes, the
  plain keyed decoder, and `From(IDeserializer<T>)` for any Confluent deserializer — the
  schema-registry ones included (`AsSyncOverAsync()` first); this package does not
  reference the registry client.
- **Unreadable records.** For a circuit input they follow the circuit's `ErrorPolicy`
  (above): dead-lettered as a `KafkaRecord` with its offset checkpointed, or a fault. A
  table has no dead-letter output, so its loader stops on one unless
  `OnUnreadableTableRecord` is set, which is then called and the record passed over.
- **The table loader** stages each change with its partition and offset — the table drops
  anything it is handed twice — and commits a version whenever it has read all that has
  been fetched, or `MaxTableBatch` (4096) records. `MeasureLagAsync()` and `Positions`
  say how far behind it is; `WhenCaughtUpAsync()` completes when the initial load is in.
  It needs the table's writer, so it runs on the leader: a standby's table follows its
  log, and `StartTableLoaderAsync` refuses it until the table is promoted.
- **Limits.** One topic per source, one consumer and one thread per source. Partitions
  added to a topic are seen at the next start.

**Change data capture.** `Debezium.Changes<TKey, TRow>()` is a decoder for the topics
Debezium writes with the JSON converter, with or without embedded schemas:

```csharp
var customers = new SharedTable<int, Customer>("customers", c => c.Id);
await using var loader = await KafkaSource.StartTableLoaderAsync(
    customers,
    new KafkaSourceOptions { BootstrapServers = "…", Topic = DebeziumPostgres.Topic("shop", "public", "customers") },
    Debezium.Changes<int, Customer>());

var enriched = orders.GroupBy(o => o.CustomerId)
    .Join(builder.Table(customers), (_, o, c) => new Enriched(o.Id, c.Name, c.Region));
```

- Snapshot rows (`op` `r`), creates and updates upsert `after`. A delete removes the key
  in the record's key, and the tombstone Debezium writes after it removes it again, which
  changes nothing. The key is the table's primary key: one column read as a scalar
  (`int`, `string`, `Guid`, …) or several read as a record with a property per column.
- Column names match properties in snake_case, ignoring case. `numeric` needs the
  connector's `decimal.handling.mode=string`; `timestamptz` arrives as ISO-8601 text;
  `timestamp` and `date` arrive as numbers. Pass your own `JsonSerializerOptions` for
  anything else.
- Debezium delivers at least once. After a connector restart, or a second snapshot, the
  same rows arrive again at new offsets; upserts absorb them, and an upsert that changes
  nothing mints no table version.
- **Replica identity.** The default identity is enough for a table with a primary key,
  because deletes are keyed by the record key. `before` is then null on an update and
  holds only the key (other columns at their defaults) on a delete — use
  `REPLICA IDENTITY FULL` if you read `before` through `Debezium.Events<TRow>()`. A table
  with no primary key has no record key: it needs `REPLICA IDENTITY FULL` and
  `Debezium.Changes<TKey, TRow>(keyOf: row => …)` for its deletes to be applied. A
  truncate is refused (the connector does not emit them unless asked).
- `KafkaConnectClient` wraps the Connect REST calls needed to register a connector
  (validate, create or update, status, restart, delete), and
  `DebeziumPostgres.ConnectorConfig(…)` builds a configuration the decoder can read.

**Writing to Kafka.** Two sinks for `DeliverToAsync`:

```csharp
await using var totals = new KafkaUpsertSink<string, RegionTotal>(
    new KafkaSinkOptions { Topic = "region-totals", BootstrapServers = "…", Term = () => leadership.Term });
_ = circuit.Handle(totalsOutput).DeliverToAsync(leadership.Leased(totals), row => row.Region);

await using var audit = new KafkaAppendSink<Enriched>(
    new KafkaSinkOptions { Topic = "enriched-orders", Log = log });       // through the log's producer
_ = circuit.Handle(enrichedOutput).DeliverToAsync(audit);
```

- `KafkaUpsertSink` writes `key → row` and a tombstone for a delete: the shape of a
  compacted topic. A keyed `KafkaSource` in another circuit reads it back as the same
  rows. `KafkaAppendSink` writes one record per appended record.
- Every record carries the header `incremental.step` (the step's sequence, decimal text)
  and, with `Term` set, `incremental.term`. Keys are placed as the Java client places
  them (murmur2), so a key stays in one partition whichever client wrote it. The sink
  does not create its topic.
- **Delivery is at-least-once**, as for any sink: the batch is acknowledged when every
  record is durable in the topic, the checkpoint commits after that, and a crash in
  between delivers the batch again. Upserts make that harmless. For appends, a reader
  that sees the step fall back along a partition knows a restart happened; with manual
  stepping (the replay cuts the same steps) it can skip the steps it has seen, and
  otherwise it must tolerate duplicates.
- **Which producer.** With `BootstrapServers` the sink has an idempotent producer of its
  own (`acks=all`): any cluster, readers need no isolation level, but a deposed leader
  can still write — wrap the sink in `leadership.Leased(…)` and stamp the term. With
  `Log` the sink writes through the log's transactional producer: a deposed leader
  cannot write the topic at all, at the price of the topic being on the log's cluster
  and readers using `read_committed`.

A commit is due at the end of the step that reaches `CheckpointInterval`, or — with
`CheckpointMaxDelay` set — of the first step that ends after the oldest uncommitted step has
waited that long; an auto-stepping circuit that goes idle with steps uncommitted commits
when the delay runs out. The circuit awaits the provider's `CommitAsync` and runs no step
until it completes, so a provider whose commit is a network round trip holds no thread for
it. A custom provider only has to implement `Commit`; `CommitAsync` defaults to calling it.

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

### Hosting it

Under the generic host — a worker service, an ASP.NET Core app — the `Incremental.Hosting`
package owns the lifecycle. Put the dataflow in a **definition class**: its constructor takes
the `CircuitBuilder` (any further parameters come from the container), and the inputs and
outputs you want reachable are its members.

```csharp
public sealed class PageAnalytics
{
    public InputStream<PageView> Views { get; }
    public Output<WindowedValue<string, long>> TopPages { get; }

    public PageAnalytics(CircuitBuilder builder)
    {
        Views = builder.Input<PageView>("views");
        TopPages = Views
            .WithEventTime(v => v.At, lateness: TimeSpan.FromSeconds(2))
            .GroupBy(v => v.Page)
            .TumblingWindow(TimeSpan.FromSeconds(5))
            .Count()
            .TopK(3, r => r.Value)
            .ToOutput();
    }
}

builder.Services
    .AddIncremental<PageAnalytics>()
    .BindConfiguration("Incremental:PageAnalytics")          // Workers, capacities, CheckpointInterval, …
    .Configure((o, sp) => o.Storage = new RedisStorageProvider(
        sp.GetRequiredService<IConnectionMultiplexer>(), "page-analytics"))
    .AddOutputConsumer<LeaderboardPublisher, WindowedValue<string, long>>(d => d.TopPages)
    .AddHealthCheck();
```

The class is the circuit's key in the container — still no strings, and as many circuits as
you have definition classes. Anything that feeds or reads the circuit injects
`CircuitHost<PageAnalytics>` and redeems members for handles, exactly as `circuit.Handle`
does:

```csharp
app.MapPost("/views", (PageView view, CircuitHost<PageAnalytics> analytics) =>
    analytics.Handle(d => d.Views).InsertAsync(view));

app.MapGet("/top", (CircuitHost<PageAnalytics> analytics) =>
    analytics.View(d => d.TopPages).Current());      // a shared MaterializeInMemory view
```

What the host does for you:

- **Start.** The circuit is built when `CircuitHost<T>` is first resolved (so handles are
  valid in any constructor; early records wait in the input channel, and an output
  subscribed early waits for the start) and started — checkpoint
  recovery included — before the host reports started, so nothing serves traffic against a
  circuit that has not resumed.
- **Shutdown, in the right order.** The host completes the inputs and drains the circuit
  *while registered consumers and views keep reading*, then stops them: `AddOutputConsumer`
  and `View` see every batch, including what the drain produces. A consumer you write by
  hand must follow the same rule — read the output to completion rather than cancelling on
  the stopping token — or the drain blocks on the unread output until the host's shutdown
  timeout abandons it (with a warning; state up to the last checkpoint is safe).
- **Sinks.** `AddAcknowledgedOutputConsumer(d => d.Enriched, async (batch, ct) => …)` (or the
  `IAcknowledgedOutputConsumer<T>` class form) delivers an acknowledged output: returning
  acknowledges the batch. At shutdown the drain ends with the final checkpoint waiting for
  the last acknowledgements, inside the shutdown timeout; if the timeout wins, that commit
  is skipped and those steps are delivered again after the restart. A sink that throws
  stops the circuit without committing past the failed batch — nothing is read and
  discarded on its behalf — and the fault policy below applies.
  `AddUpsertSink(d => d.Totals, sp => new RegionTotalsSink(…))` and
  `AddAppendSink(d => d.Events, sp => …)` register a sink directly: collapse, retries and
  acknowledgement as in *Sinks* above, with the sink created once, when the circuit starts.
- **Dead letters.** `ErrorPolicy` and `DeadLetterCapacity` are on `HostedCircuitOptions`.
  With `DeadLetter`, register `AddDeadLetterConsumer((letter, ct) => …)`: the host keeps the
  dead-letter output read for the life of the circuit and through its drain.
- **Faults.** A fault is terminal and recovery is a rebuild from the checkpoint, which under
  a host means a restart: by default the host logs at Critical, sets a non-zero exit code and
  stops the application (`CircuitFaultBehavior.StopHost`). `Ignore` leaves the host running;
  pair it with the health check. A registered consumer that throws is treated the same way,
  and the rest of its output is discarded so shutdown can still drain.
- **Wiring.** The container's `ILoggerFactory` is used unless you set one; the circuit's
  name defaults to the definition class's name, so logs and the `circuit` metric tag read as
  the thing you wrote. Options are read once, at build — configuration reloads are not
  observed, because a circuit's shape cannot change under it.

`examples/Incremental.LogAnalytics` is this section as a runnable worker service: a producer
`BackgroundService`, a dashboard reading a `View`, and registration order doing the shutdown
ordering. The definition class is plain: `new PageAnalytics(builder)` works just as well in a test or
a console app with no container in sight. Clustered circuits are not hostable yet — they
step in explicit lockstep across processes, which needs a host-side stepping protocol.

### Running many pipelines

`Incremental.Hosting` runs one circuit whose definition is a class you compiled in.
`Incremental.Service` is for the other shape: many circuits, deployed and changed while
the process runs, joining a few large tables they all share. Each pipeline is one circuit;
the tables live outside them, once per process.

```csharp
builder.Services.AddIncrementalService(o => o.PluginDirectory = "plugins")
    .UseInMemoryBackend()                                   // the default; or UseKafkaBackend, below
    .AddObjectStore("refdata", _ => new LocalObjectStore("/var/lib/refdata"))
    .AddContracts(typeof(Instrument).Assembly)              // row types and table definitions
    .AddPlugin(typeof(TradeEnrichment).Assembly)            // definitions compiled into the host
    .AddTable(Tables.Instruments, new TableLoaderSpec("file", "refdata", config));
```

A **table** is declared once, in a *contract assembly* (`[assembly: IncrementalContract]`),
as a `TableDefinition<TKey, TRow>`: its name, its key, its indexes. A **pipeline** is a
definition class; `Build` describes one circuit against named slots:

```csharp
[Pipeline("trade-enrichment")]
public sealed class TradeEnrichment : IPipelineDefinition<TradeEnrichment.Options>
{
    public sealed record Options
    {
        [Range(0, 1e12)] public decimal MinNotional { get; init; }
        [StateNeutral] public string? Label { get; init; }     // changes no stored row
    }

    public void Build(PipelineBuilder p, Options o)
    {
        var instruments = p.Table(Tables.Instruments);          // a TableRef: Join, Lookup, By(index)
        var enriched = p.Input<Trade>("trades")                 // the core's InputStream<Trade>
            .Where(t => t.Notional >= o.MinNotional)
            .GroupBy(t => t.InstrumentId)
            .Join(instruments, (_, t, i) => new EnrichedTrade(t.Id, i.Symbol, t.Notional * i.Multiplier))
            .Named("joined");
        p.UpsertSink("enriched", enriched, e => e.TradeId);
    }
}
```

`p.Circuit` is the core's `CircuitBuilder`, so everything in this document is available
inside a definition. What the service adds is at the edges: `Input`/`KeyedInput` and
`UpsertSink`/`AppendSink` name *slots*, and `Table`/`ToTable` name declared tables. A
**spec** binds them — which plugin version and definition, the settings, and a connector
config per slot:

```csharp
await node.Pipelines.DeployAsync(new PipelineSpec
{
    Id = "trades-eu", Plugin = "acme.pipelines", Definition = "trade-enrichment",
    Settings = Json("""{ "minNotional": 1000 }"""),
    Tables = new Dictionary<string, string> { ["instruments"] = "instruments-eu" },   // slot → declared table
    Inputs = new Dictionary<string, JsonElement> { ["trades"] = Json("""{ "connector": "memory", "name": "trades-eu" }""") },
    Sinks = new Dictionary<string, JsonElement> { ["enriched"] = Json("""{ "connector": "memory", "name": "enriched-eu" }""") },
}, new DeployOptions(), ct);
```

What to know before relying on it:

- **A deploy is checked before anything is touched.** The settings are bound (camelCase,
  unknown members refused, string enums, then DataAnnotations), the definition is run
  against a builder that is never built, and every slot must be bound to a registered
  connector that supports its kind. `DeployOptions.DryRun` returns the plan and stops.
- **Keeping state across a change is never implicit.** Replacing a pipeline with another
  plugin version, definition, worker count, input or table binding, or a settings value
  not marked `[StateNeutral]`, is refused with `state-choice-required` until the caller
  says `StateChoice.Keep` or `StateChoice.Reset`. A value captured by a delegate upstream
  of a stateful operator is part of the state: after a filter threshold changes, `Keep`
  leaves rows the new filter would not have produced. `Keep` across plugin versions reads
  the old version's state as the new version's types, through JSON; it is safe while the
  types that appear in a store's *key* (a grouping's records, a `Distinct`'s values) keep
  their shape, and not otherwise. The refusal says which it is: it compares the state
  stores of the two specs and the types they hold, and names any store, type and member
  that differs — or says `state shapes: unchanged`.
- **A `Keep` that cannot work does not take the pipeline down.** A change the committed
  checkpoint cannot follow — another worker count, a stateful operator removed or
  renamed, a table recreated — is refused with `state-incompatible` at the dry run and at
  the call, before anything stops (`CircuitBuilder.CheckState`). What only shows when
  the state is read is found when the new generation is built
  (`CircuitStateMismatchException`): the previous spec is put back and started, the
  outcome is in the audit, the event feed and the pipeline's status (`RolledBack`), and
  the caller gets `state-incompatible`. A successor that takes over in the middle does
  the same.
- **Pipelines do not disturb each other.** Each has its own queue of operations. One that
  faults is torn down and restarted with a doubling backoff on its committed state; more
  than `maxRestarts` faults within the window and it is `Failed` until an operator
  restarts it. A stop has `StopTimeout` to drain; after that its outputs are abandoned,
  and a circuit that still will not stop is `Wedged` — shown, counted, and not rebuilt on
  that node.
- **The dead-letter output is always read**, into a ring of the newest records per
  pipeline, so a circuit under `RecordErrorPolicy.DeadLetter` cannot stall on it.
- **Plugins are trusted code** loaded into a collectible `AssemblyLoadContext` per
  version. A plugin gets the host's copy of `Incremental*` and of any contract assembly,
  even if it ships its own, which is what lets two plugins join one table. Hold nothing
  of a plugin's in a static, and never serialize a plugin's type with a process-wide
  `JsonSerializerOptions` — use the one the slot context gives you — or its version can
  never unload. A version unloads when no pipeline uses it.
- **Table history is kept for whoever may need it**: every pipeline in the catalog that
  has committed a version of the table, running or stopped, and — because the leader
  cannot see a standby's position — everything newer than `FollowerRetention`. The
  backend caps it (`RetainedHistoryVersions`); a stopped pipeline the cap passes needs a
  reset, and the plan a `stop` returns says how many versions it has.
- **The catalog is the only input to what exists.** It is a shared table (`svc.catalog`)
  on the backend's log. A node that starts on storage that already holds one rebuilds
  the same tables and pipelines and resumes each from its checkpoint; a standby follows
  it, follows the tables, holds each pipeline's state as bytes, and builds the circuits
  when it takes over.

`InMemoryServiceCluster` gives several nodes in one process — `cluster.Node("a")`,
`cluster.Node("b")` — with real fencing, which is how the takeover order is tested without
a broker.

**On Kafka, for availability.** The load fits one node; the second process exists so that
losing the first costs seconds. Both are configured alike except for their names:

```csharp
builder.Services.AddIncrementalService(o => { o.PluginDirectory = "plugins"; o.CacheDirectory = "/var/cache/enrichment"; })
    .UseKafkaBackend(new KafkaServiceBackendOptions
    {
        Log = new KafkaLogOptions { BootstrapServers = "…", TopicPrefix = "enrichment", NodeId = "node-a" },
        ProbeListen = new IPEndPoint(IPAddress.Any, 7400),     // where this node answers its peer
        PeerProbe = ("node-b", 7400),                           // where the other node listens
    })
    .AddKafkaConnection("msk-main", _ => new KafkaConnection
    {
        BootstrapServers = "…",
        ClientConfig = { ["security.protocol"] = "SASL_SSL", ["sasl.mechanism"] = "OAUTHBEARER" },
        ConfigureConsumer = b => b.SetOAuthBearerTokenRefreshHandler(Refresh),   // MSK IAM
        ConfigureProducer = b => b.SetOAuthBearerTokenRefreshHandler(Refresh),
        AllowedTopicPrefixes = { "tenants." },
    })
    .AddContracts(typeof(Instrument).Assembly);
```

Every node starts as a standby. The one that finds nobody leading takes over after
`TakeoverAfter` (3 s) of silence; the other follows the catalog, the tables and every
pipeline's state through the broker, fetches plugin files from it, and builds nothing.
When the leader is killed the standby fences it, promotes the catalog, the tables and the
state, builds the circuits from their committed state and starts the sources at their
committed offsets. Locally, with 21 tables and two pipelines, that is about 3 s to notice
and under a second to take over.

A table loaded from a topic, and a pipeline bound to topics, name a connection and a
topic and nothing of the cluster:

```csharp
await node.Tables.DeclareAsync(new TableSpec
{
    Name = "instruments", Definition = typeof(InstrumentsTable).FullName!,
    Loader = new TableLoaderSpec("debezium", "msk-main", Json("""{ "topic": "tenants.cdc.public.instruments" }""")),
}, ct);                                                        // or "kafka": a value upserts, a tombstone deletes

Inputs = { ["trades"]   = Json("""{ "connector": "kafka", "connection": "msk-main", "topic": "tenants.eu.trades" }""") },
Sinks  = { ["enriched"] = Json("""{ "connector": "kafka", "connection": "msk-main", "topic": "tenants.eu.trades.enriched" }""") },
```

What to know:

- **The connection is the host's; the spec is the tenant's.** A `kafka` config accepts
  `connection`, `topic`, `startFrom` (`earliest`/`latest`), `format` (`json`, or
  `debezium` on a keyed input), and on a sink `producer` (`own`, or `log` for the
  backend's transactional producer, on the connection `log` only). Bootstrap servers or
  client properties in a spec are refused, as is a topic outside the connection's
  `AllowedTopicPrefixes`. The backend's own cluster is registered as the connection `log`.
- **Delivery is at least once.** A new leader resumes each source from the offsets in the
  pipeline's last checkpoint and delivers again whatever the old one had not committed;
  an upsert sink converges, an append sink sees duplicates. Create sink topics yourself —
  compacted, for an upsert sink.
- **Fenced and resigned are final.** A node that has been taken over, or has stepped
  down, stops its host with exit code 3 or 4 and comes back, when its supervisor restarts
  it, as the standby. `node.StepDownAsync` is refused unless the peer's probe answers as a
  ready standby; `node.TakeOverAsync` unless this node is warm; both take `Force`.
- **Failing pipelines do not move leadership.** Only a node that can no longer write its
  catalog gives leadership up by itself: it steps down to a ready standby, or exits with 5.
- **Start the first node before the second.** Two nodes that start together on an empty
  deployment both take over; one is fenced at once and exits.
- **Give each node its own `CacheDirectory`**, and the plugin directory to whichever may
  lead first. Plugins reach the other through a compacted topic, `{prefix}.svc.blobs`.
  A configured directory is kept between runs and pruned of versions the catalog no
  longer holds; the default is a temporary directory that goes with the process.
- **A reset or a removal is carried through once it is recorded.** The intent is
  committed first; after that a caller that goes away cancels nothing, a failure leaves
  the pipeline in `Backoff` with the act still pending and tried again, and no path —
  start, restart, a backoff retry, a takeover — starts the pipeline around it.
- **A connector's own calls are bounded** by `ServiceOptions.ConnectorTimeout` (30 s): a
  source that does not start, or a sink that does not finish its reset, is that
  pipeline's fault and holds up neither its queue nor a takeover.
- **Seeds are applied once.** `AddTable`/`AddPipeline` are written to the catalog by the
  first leader that finds them not yet applied, and the catalog records it: a seeded
  pipeline or table that is removed stays removed.
- `node.Status` has what a status page needs: role and terms, heartbeat age, whether the
  delivery lease is held, how the last takeover's time was spent, the peer's answer, and
  on a standby how far its catalog, tables and state are behind.
- **For a monitoring system there is a meter**, `Incremental.Service`
  (`ServiceMeter.Name`), beside the core's `Incremental`: pipelines by state, per
  pipeline whether it is up, its source lag, checkpoint age, sink-ack wait, dead letters,
  faults, restarts and roll-backs; tables by state with rows, version, follower lag and
  loader lag; the node's role, term, health, standby readiness and state lag; takeovers
  and their duration by stage. Tags are the node, a pipeline id or a table name, and
  closed sets. Add both meters to whatever exporter the host uses;
  [enrichment-service.md](enrichment-service.md#operating-it-monitoring) lists the
  instruments and what to alert on.
- **The in-memory backend is bounded.** Its state log compacts itself
  (`InMemoryBackendOptions.StateCompactAbove`) and its table logs cap history by row
  changes as well as by versions, so a node left running holds what is live. It is still
  one process's memory: Kafka is the backend to run on.

#### Looking at it: the admin API and UI

`Incremental.Service.Admin` mounts a JSON API and a web UI over the node:

```csharp
builder.Services.AddIncrementalAdmin(o =>
{
    o.ExposeRowData = true;                 // rows, dead-letter records, exception messages
    o.AllowPluginUpload = true;             // code over the API: off unless you say so
    // o.AuthorizationPolicy = "admins";    // without one, loopback requests only
});
app.MapIncrementalAdmin("/admin");          // UI at /admin/, API under /admin/api
app.MapIncrementalHealth("/health");        // /live, /ready, /status: for a load balancer
```

- **It is loopback-only until a policy is set.** With no `AuthorizationPolicy` a request
  is refused unless it comes from a loopback address, names a loopback host (or one in
  `AllowedHosts`), carries no forwarding header and is not HTTP/1.0 (which is what nginx
  speaks to its upstream by default). That is a default for a developer's machine, not
  authentication: a proxy on the same machine that adds no header and speaks HTTP/1.1
  cannot be told from a local client. Behind a proxy, or across machines, set a policy; `PipelinePolicy`, `TablePolicy`, `NodePolicy` and `PluginPolicy` then say
  who may change what. `ReadOnly` refuses every mutation, and the UI shows no actions.
- **Row data is a switch.** Off, the row endpoints answer 403, and faults, dead letters,
  unreadable records and events come back with the exception's type and not its message:
  a `FormatException` quotes the value it could not read.
- **Mutations** carry the header `X-Incremental-Admin: 1`, take `dryRun=true` (the answer
  is the plan and nothing happens), and — reset, remove, table and plugin deletion,
  takeover, step-down — `confirm=<the id>`. A pipeline mutation may carry
  `If-Match: "<generation>"` and gets 412 if the pipeline has moved. On a standby they are
  refused with the leader's id and address, except takeover.
- **Everything is bounded.** Lists are paged (`skip`, `limit`, at most `MaxRows`). A page
  of a table's rows answers `next`, which is the `from` of the page after it, so every
  row of a table of any size can be reached; a search (`contains`) looks at no more than
  `MaxScannedRows` rows per request and says when it stopped short (`truncated`, with a
  `next` to carry on from) — "not found" and "not reached" are different answers, and on
  a standby `lagVersions` adds "not arrived here yet". The event feed (`GET api/events`, server-sent events with
  ids, replayed from `Last-Event-ID`) queues 64 frames per client and drops the oldest.
- **Durations are milliseconds; a spec comes back in the form it is sent in**, with any
  property its connector declares secret replaced by `***`. Sent back so, `***` keeps the
  stored value for that slot and connector, and is refused where none is stored.

| | Path | |
|---|---|---|
| GET | `info`, `node`, `node/metrics` | what the API allows; role, terms, health, readiness, peer, last takeover; the node's rates |
| POST | `node/takeover`, `node/stepdown` | `force=`, `confirm=<node id>` |
| GET | `tables`, `tables/{name}` | statistics, loader, readers and who holds the floor, unreadable records |
| GET | `tables/{name}/rows` | `limit`, `from`, `contains` — or `key`, or `index`, `key` and `skip` |
| POST | `tables`, `tables/{name}/loader/restart`, `tables/{name}/reopen`; DELETE `tables/{name}` | |
| GET | `pipelines`, `pipelines/{id}` | `q=`, `state=`; detail adds the spec, sources, sinks, committed table versions, faults, history |
| GET | `pipelines/{id}/graph`, `/metrics`, `/dead-letters` | the circuit; 60 one-second and 60 one-minute samples; the ring, newest first by `before=` |
| PUT | `pipelines/{id}?state=keep\|reset` | deploy or replace |
| POST | `pipelines/validate`, `pipelines/{id}/start`, `/stop`, `/restart`, `/reset`; DELETE `pipelines/{id}` | |
| GET | `plugins`, `plugins/{name}` | versions, definitions and their settings, which pipelines use which |
| GET | `plugins/{name}/{version}/definitions/{definition}` | leader: its settings with defaults, ranges and enum values; its input and sink slots with kinds and types; the tables it reads and writes. `settings=` describes it under those |
| GET | `plugins/{name}/{version}/definitions/{definition}/template` | leader: a starting spec with the real slot names bound to this host's connectors, and whether the dry run accepts it. `id=` |
| GET | `connectors`, `connections`, `table-sources`, `table-definitions` | what a spec may name: connectors with the slot kinds they support and their config; Kafka connections and object stores by name and kind, nothing else; loader kinds with their config; the table definitions in the loaded contracts with their key and row types and the tables declared from each |
| GET | `plugins/{name}/diff?from=&to=` | what two stored versions declare differently: definitions, settings and — on the leader — slots, tables and the members of the slots' record types |
| GET | `whoami` | the caller as the audit would record it, whether the host verified it, and the header a caller may state a name in |
| POST | `plugins` (a zip), `plugins/rescan`; DELETE `plugins/{name}/{version}` | |
| GET | `audit`, `events/recent`, `events` | who changed what; this node's event ring; the live feed |

The UI is the same API in a browser — overview, pipelines (metrics, topology, config,
faults, dead letters), tables (loader, readers, rows), plugins, cluster, audit — and is
embedded in the package: nothing to build, nothing fetched from elsewhere.
`dotnet run --project tests/Incremental.Service.AdminHost` runs it with data.

**Files, dead letters, and a test harness.** Besides `kafka` and `memory` a slot can be
bound to the built-in `files` connector: JSON-lines objects under a prefix of an
`IObjectStore` the host registered (a local directory is built in; S3 is five methods to
write). As a source it reads objects in name order with the object and line in the
checkpoint; as a sink it writes one object per step, named by the step so that a step
delivered again overwrites itself. A spec's `deadLetters` binds any append sink
connector as the pipeline's dead-letter sink: dropped records are delivered there, at
least once, before the checkpoint that passes them, and a sink that will not take them
stops the pipeline and says why. And `Incremental.Service.Testing.PipelineTestHarness`
runs one definition in a test — settings bound as the service binds them, tables the
test fills, inputs it feeds, steps it takes, sinks and dead letters it reads — with no
node and no HTTP.

**A guide and a complete host.** [enrichment-service.md](enrichment-service.md) walks
through writing a contract, a definition, a plugin project and its tests, the host and
its adapters (table sources with and without positions, a source connector, a sink
connector), the UI, and operating notes. It is written around `examples/Incremental.EnrichmentService`,
which runs in memory (`dotnet run --project examples/Incremental.EnrichmentService`), as
two processes on Kafka with reference data arriving from Postgres through Debezium
(`-- --kafka --node a`, `-- --kafka --node b`), and as a smoke test that checks every sink
against a model (`-- --headless`). `examples/Incremental.TradeEnrichment` shows the table
operators on their own, without the service.

What is built and what is not is in
[design/enrichment-service.md](design/enrichment-service.md), *As built*.

> **TODO:** cluster deployment (multi-process exchange over a transport), checkpoint
> operations guide, observability/metrics reference. The engine supports all three today;
> consumer-facing docs pending.

## Escape hatches

The declarative surface compiles onto a small set of primitives, all of which remain public
for the cases the paved road doesn't cover:

- **Window-as-data**: windows are just records — `SelectMany` an event into
  `Windowed<T>` values and `GroupBy` a composite key manually when you need non-standard
  window shapes. You then own eviction, via the next item.
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
| `MapValues` / `Values` | kept on keyed/windowed results |

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
  `TopK` doesn't need it, it ranks per window natively); keyed-input snapshot sync (retract
  keys a full-table re-feed omits); watermark
  idleness handling; whether required input names can go too — today they are the durable
  key for checkpointed source offsets (a reference dies with the process, positional
  identity would re-bind offsets across topology edits), but a derived-with-optional-pin
  scheme like store naming may be possible.
