# Design: shared tables

> **Status:** agreed design, 2026-11-09; build steps 1–6 done, hosting (7) to come (see *Build order*).
> Sections marked **Decided** record a choice James made between stated alternatives.

## Why

The first application is an enrichment service: many user-defined pipelines, each its own
circuit, joining streams against a few large reference datasets that most pipelines share.
Today that shape costs too much and cannot grow while running:

- A reference table fed through `KeyedInput` is held once in the input's upsert state and
  once more in every join that reads it — per circuit. Ten pipelines joining instruments
  hold twenty copies.
- A pipeline added later sees nothing of a table that is already loaded: the rows are in
  state, but only changes flow, and re-upserting an unchanged row is a no-op.
- `LeftJoin` keeps a second copy of the fact side in its anti-join.
- Snapshot sync (`ReplaceAllAsync`) has nowhere to live, because value stores cannot be
  scanned.

A **shared table** is a reference dataset held **once per process**, outside any circuit,
that any number of circuits join against. It is the one place in the library where state
is deliberately shared across threads, so the design below is mostly about making that
safe and cheap.

## What it is

A shared table is two things kept in step:

- a **change log**: an ordered sequence of *versions* 1, 2, 3, …, each one batch of row
  changes applied atomically;
- a **multi-version view**: the current rows by key, readable *as of* any version that a
  running step has pinned.

```
 source adapter ──▶ TableWriter ──▶ ┌──────────── SharedTable ────────────┐
 (Kafka, gRPC snapshot+delta,       │ change log:  v1  v2  v3 … vL        │
  file, CDC)                        │ view:        key ─▶ row @ version   │
                                    └───────┬──────────────┬──────────────┘
                              changes (p, t] │              │ probe(key, t)
                                             ▼              ▼
                                   circuit A step     circuit B step
                                   pins version t     pins version t′
```

One writer applies changes. Circuits never write. Each circuit step pins one version of
each table it reads and sees exactly that version for the whole step, on every worker.

## How a circuit sees a table

To a circuit, a table is an **offset-stamped input whose current rows live outside the
circuit**. That one idea reuses most of the existing machinery:

- **Step start (drain).** For each attached table the coordinator reads the table's latest
  version `t`, pins it, and fetches the net changes since the circuit's previous version
  `p` — one `TableChange(Key, Old?, New?)` per key that differs between `p` and `t`.
- **During the step.** Operators probe the table at `t`. A version the writer publishes
  mid-step is invisible to this step.
- **Step end.** The pin is released once the workers finish — before output publishing, so
  a slow consumer never holds a version open.
- **Checkpoint.** `t` is committed with the circuit's other input offsets. After a restart
  the circuit resumes from its committed version and reads the changes it missed from the
  log, exactly as a Kafka input resumes from a committed offset.

A new circuit starts at the table's current version with no previous one. It reads no
changes and needs no backfill: its fact state is empty, so there is nothing for old table
rows to join against, and the first facts to arrive probe the table directly. That is the
whole answer to "add a pipeline to a running service".

## API

```csharp
// Process level — typically registered with the host.
var instruments = new SharedTable<int, Instrument>("instruments", i => i.Id);
// …or over a change log, restoring whatever the log holds:
var instruments = await SharedTable<int, Instrument>.OpenAsync("instruments", i => i.Id, log);

// One logical producer: the source adapter.
var writer = instruments.Writer;
await writer.UpsertAsync(row, partition, offset);
await writer.DeleteAsync(key, partition, offset);
await writer.CommitAsync();                          // everything staged, as one version
await writer.ReplaceAllAsync(snapshotRows, offsets); // snapshot sync

// In any circuit, in any number of circuits.
var inst = builder.Table(instruments);               // TableRef<int, Instrument>

// Retroactive: a later change to an instrument re-enriches the trades it matches.
trades.GroupBy(t => t.InstrumentId)
      .Join(inst, (id, trade, instrument) => …);
trades.GroupBy(t => t.InstrumentId)
      .LeftJoin(inst, (id, trade, instrument) => …, (id, trade) => …);

// Enrich on arrival: each trade is enriched once and never corrected.
trades.Lookup(inst, t => t.InstrumentId, (trade, instrument) => …, missing: trade => …);
trades.LookupAppendOnly(inst, t => t.InstrumentId, (trade, instrument) => …);   // no state

// Joining on a column that is not the table's key.
var byIsin = inst.By("isin", i => i.Isin);           // TableView<string, Instrument>
quotes.GroupBy(q => q.Isin).Join(byIsin, (isin, quote, instrument) => …);

// The table's own changes as a stream, for aggregating reference data itself.
inst.Changes();                                      // ZStream<Instrument>
```

`TableRef` is a staging type like `GroupedZStream`: it can be joined, looked up, indexed
or turned into a stream, and nothing else. What the joins and lookups actually take is its
base type, `TableView<TKey, TRow>` — "a table's rows, found by `TKey`" — which is also what
`By` returns, with the index key in place of the table's. So there is one `Join`, one
`LeftJoin` and one `Lookup`, and each works on the table's key or on an index.

**The writer stages, then commits.** `UpsertAsync` and `DeleteAsync` stage a change and
complete at once; `CommitAsync` publishes everything staged as one version, and is where
the writer waits (for the log, or for a lagging circuit). Batch boundaries are the
adapter's to choose because a version is atomic: a source transaction, a poll's worth of
records, a snapshot chunk. A commit that changes nothing mints no version. Each of the two
calls also has an overload without partition and offset, for sources that cannot be
replayed.

## The two joins

### Retroactive join

State: the fact side only, indexed by join key, sharded by the usual `GroupBy` exchange.
The table changes travel through the same exchange (as `TableChange` records routed by
key), so each worker receives the changes for the keys it owns.

Per step, with `F` the fact state before this step, `ΔF` the fact delta, `ΔT` the table
changes from `p` to `t`, and `T_t` the table at the pinned version:

```
Δ(F ⋈ T)  =  F ⋈ ΔT   +   ΔF ⋈ T_t
```

in that order: table changes are applied to the facts already held (retract the row built
from `Old`, assert the row built from `New`), then the new facts probe the table at `t`,
then `ΔF` is integrated. This is the existing bilinear rule with the right-hand state
replaced by a versioned probe.

`LeftJoin` falls out of the same operator. A key whose table row appears or disappears is
a `TableChange` with a null on one side, so the operator retracts the unmatched rows and
asserts the matched ones (or the reverse) from the single fact store. No anti-join, no
second copy of the facts.

Eviction is unchanged: `EvictWhen` on the grouping drops closed keys' fact state. Only
facts are tracked for it; a table change creates no state, so a change to a key with no
facts here (most of them, in most pipelines) costs one empty group read and nothing else.

As built (`Operators/TableJoinOperator.cs`), three things the sketch above leaves open:

- **"The same exchange" is a second exchange with the same hash.** The fact side's
  exchange already exists by the time `Join` is called, so the join adds one of its own
  over the table's changes. It must route exactly as the grouping did, including a
  caller-supplied `GroupBy(key, stableHash)`, so a grouping now carries its routing hash,
  through `EvictWhen`, the aggregates and `Grouped()`. Each table join pays one exchange
  barrier per step for this, whether or not the table changed.
- **Not inside `Recurse`.** A table's delta and its pinned version are per outer step;
  neither means anything per iteration. Both joins reject it at definition time.
- **One row per key is what makes the left join one operator.** The stream `LeftJoin`
  needs an anti-join because its right side is a multiset; a table key is matched or it
  is not, and the null side of a `TableChange` says which.

### Lookup (enrich on arrival)

Lookup ignores `ΔT` entirely. Each arriving fact probes `T_t` once and is emitted. It takes
a key selector directly rather than a grouping, because nothing about the table needs
co-partitioning — though the remembered form's own state does need an exchange, below.

"On arrival" means *as of the step that processed the record*. It is not event time, and
after a crash a replayed fact may be enriched with a newer row than the first time. With
at-least-once keyed upsert outputs that converges on the newer enrichment.

The difficulty is retraction. A fact that is later amended or cancelled must retract
exactly the row that was emitted for it, and the table may have moved on since. Two modes:

- **Remembered (default).** The operator keeps the enriched row it emitted per fact and
  retracts that. State is one copy of the enriched facts; outputs never churn when
  reference data changes; a hot reference key causes no fan-out.
- **Append-only.** No state at all. The stream is declared insert-only, and a
  negative-weight record is an error (routed by the per-record error policy once that
  exists; a fault until then).

> **Decided.** *Remembered* is the default and append-only is an explicit opt-in per
> lookup. Upserts are the common shape and a non-cancelling retraction silently corrupts
> everything downstream. The price: lookup saves fan-out and churn rather than memory
> unless the pipeline opts into append-only.

As built (`Operators/TableLookupOperator.cs`):

- **Append-only is its own method**, `LookupAppendOnly`, not a flag: the two differ in
  state, in partitioning and in what input they accept, and a flag would hide all three.
  It is one stateless operator on whichever worker the record was staged on. A
  negative-weight record throws, which faults the circuit, with a message that names the
  table and points at `Lookup`. Under the per-record error policy
  (`RecordErrorPolicy.DeadLetter`, durability-and-standby.md, *As built (sinks and error
  policy)*) the retraction is reported as a dead letter instead and the step continues, as
  the text above anticipated. The remembered form under that policy records a fact whose
  delegates threw as held with no rows, so its retraction takes nothing away.
- **What "per fact" means.** The remembered state is keyed by the fact itself and holds the
  rows it was emitted as. The invariant is that the output integrates to the sum, over
  held facts, of the fact's weight times its remembered rows. A step's net change `d` to
  a fact keeps it one of two ways: a retraction (`d < 0`) of a held fact scales what is
  remembered and reads nothing from the table; an assertion (`d > 0`), or any change to a
  fact that is not held, probes the table and moves the fact's *whole* weight onto the
  rows found now. So a fact asserted a second time while still held is re-enriched as of
  the later assertion, both copies — there is one remembered enrichment per fact, not one
  per copy. With upserts (the old fact retracted, a different new fact asserted) the
  question never arises. A fact that found no row and has no `missing` selector is still
  remembered, as "emitted nothing", so that its weight is known when it is asserted again.
- **The exchange is on the lookup key.** State keyed by fact has to sit downstream of an
  exchange that sends equal facts to the same worker. The lookup key does that — equal
  facts have equal keys — and unlike the fact itself it is certain to have a stable hash,
  being a table or index key. `stream.Lookup(table, key, …)` is therefore exactly
  `stream.GroupBy(key).Lookup(table, …)`, with a `stableHash` overload like `GroupBy`'s.
  The cost is skew: a hot reference key puts all its facts on one worker. Routing by a
  hash of the whole fact would spread them, at the price of hashing every fact and of
  rejecting fact types `StableHash` cannot derive; not done.
- **Bounding the memory.** The grouping form honours `EvictWhen`: once a key closes,
  everything remembered for its facts is dropped and what was emitted stays final. Each
  remembered fact is tracked for this individually, since the state is per fact, so
  eviction costs a deadline-queue entry per live fact where a join's costs one per key.
  As with every evicting operator, the queue is rebuilt from arriving records and not
  checkpointed, so facts remembered before a restart are only evicted if they are touched
  again. There is no bound other than the watermark: no count or age limit, and nothing
  on the plain `stream.Lookup` form, which has no grouping to hang a policy on.
- **Per-fact overhead.** The state is an indexed weight store keyed by fact (for its
  batched, allocation-free reads and writes), in which every fact is a group of its own,
  nearly always of one entry. The in-memory provider therefore holds a group of one value
  inline in its map and gives it a dictionary only when a second value arrives. It used
  to allocate a dictionary per group, which on an upserted stream — the old fact out, a
  new one in — was about 200 bytes allocated and discarded per record; the `--assert`
  suite now puts a ceiling on the lookup cycle's allocation, not only on how it scales.
- **Indexes.** A lookup through a non-unique index emits one row per match, `missing` only
  when there are none, and remembers them all; matches that enrich to equal rows are
  remembered as one row with a count.
- **Not inside `Recurse`**, like the joins.

## The multi-version view

Requirements: one writer; many reader threads at different pinned versions; a probe that
is O(1), lock-free and allocation-free; small overhead when rows rarely change.

**Structure.** A concurrent map from key to the head of a short version chain:

```
key ─▶ Slot { Row | tombstone, Version, Older ─▶ Slot … }
```

- **Read at `t`:** follow the chain from the head to the first slot with `Version ≤ t`.
  In the steady state the chain has one slot.
- **Write:** the writer builds a version's slots, links each in front of its key's chain,
  then publishes the new latest version with a single volatile write. A reader that
  pinned the previous version skips the new slots by their version number, so a
  half-linked batch is never observable.
- **Trim:** let `m` be the oldest version any in-flight step has pinned (or the latest
  version when none has). In each chain the newest slot with `Version ≤ m` is the last one
  any reader can need, so the writer cuts everything behind it, and removes a key outright
  when that slot is a tombstone. The writer keeps a queue of (version, key) for keys it
  changed and trims as `m` advances, so trimming costs O(changes), never a table scan.

Taking a pin races with trimming: a coordinator could read the latest version just as the
writer, seeing no pins, trims behind a newer one. One small lock closes it: a pin reads the
latest version and registers itself under the lock, and the writer computes the trim floor
under the same lock. Either the writer sees the pin, or the pin sees the newer version.
The lock is taken once per step per table and once per version written — never per probe —
and the stress test targets this protocol directly.

Pins are held only while a step's workers run, so the retained window is normally a
handful of versions. A step that never finishes (a wedged delegate) would hold it open
forever; `MaxRetainedVersions` (default 4096) bounds that — a pin more than that many
versions behind the latest is revoked, and that circuit's next probe faults it. Revoking
is one volatile write of the cutoff version, made before the writer trims; every probe
compares its version to the cutoff *after* reading its chain, so a read that trimming
could have disturbed always sees the revocation. A scan of the rows (the bootstrap of
`Changes()`, below) is a read like any other and long enough to be overtaken, so it makes
the same comparison after every row and after the last: a key trimmed past the pin reads
as absent, and a scan that ended short without saying so would be a table with rows
missing. The same applies to a `Snapshot()` held by application code. While a table has no log this cap rarely decides anything for a
circuit: the writer stops for a circuit that is `TailVersions` behind (below) long before
its pin is that old.

**Probing API**, shaped like the stores' batched reads so operators keep caller-owned
buffers: `TryGet(key, version, out row)` and `GetManyInto(keys, version, rows, found)`.

**Alternative considered: persistent (structure-sharing) map.** Each version is an
immutable root, a pin is a reference, and the runtime's GC does the trimming. Simpler to
get right, but slower to probe and heavier to write. Measured by
`benchmarks/Incremental.Benchmarks/TableBenchmarks.cs` (short job, one machine; a plain
`Dictionary` is what probing a join's private copy costs today):

| 1,000,000 rows | plain `Dictionary` | version chains | persistent map |
| --- | --- | --- | --- |
| probe, random key | 20 ns | 34 ns | 53 ns |
| write, per changed row | — | 389 ns, 421 B | 350 ns, 1,410 B |
| allocated per probe | 0 | 0 | 0 |

At 100,000 rows the probes are 7, 14 and 20 ns. Version chains are the structure: about
1.7× a private dictionary to probe, and a third of the persistent map's allocation per
change — allocation that would all be long-lived garbage. The cost is memory per key:
92 bytes of structure against a dictionary's 39 (concurrent-map node plus one chain
entry), paid once per process rather than once per join. A single-writer open-addressed
map could reclaim most of that later without changing anything above it.

One thing the measurement turned up: the writer's batch scratch keeps the capacity of the
largest batch it has seen (60 bytes per key after a single million-row commit). Bulk
loads therefore commit in chunks, which `ReplaceAllAsync` does by design.

**Secondary indexes.** `By(name, selector)` declares an index, identified by name because
delegates cannot be compared. It is a second versioned map from index key to the primary
keys under it, maintained by the writer in the same version as the rows. An index
requested after the table is loaded is built by the writer between versions (one scan) and
is available from the next version. Index groups are copy-on-write per change, which is
fine for unique and small groups and is the known limit for very large ones.

As built (`Tables/TableIndex.cs`, `Tables/VersionChains.cs`,
`Operators/TableIndexJoinOperator.cs`):

- **One structure for rows and indexes.** The version chains are a class of their own,
  `VersionChains<TKey, TValue>`; the rows are one instance and each index another, whose
  values are immutable arrays of primary keys. Pins, trimming and revocation therefore
  cover indexes with no rules of their own, and an emptied group is a deletion that
  trimming removes like a deleted row.
- **Declaration.** `table.AddIndex(name, selector)` declares one directly; `By` declares
  and returns the view. The first selector under a name stands, and the same name with a
  different key type is an error. A selector may return null: that row is under no key.
  An index is never dropped, and costs the writer one selector call or two per changed
  row for as long as the table lives.
- **A late index is usable at once, not "from the next version".** Declaring one on a
  loaded table takes the writer's commit gate, scans the rows at the latest version and
  stamps the groups with that version. No reader holds the index before the call returns,
  so nothing older can ask for it. The call blocks while a commit is in flight —
  including a commit waiting on a lagging circuit — so declare indexes before loading
  where possible.
- **Selectors run before the append.** The writer works out each index's changes while it
  builds the version, ahead of the log. A selector that throws therefore fails the commit
  with nothing published or appended; the batch is already consumed by then, so the writer
  faults for good, as it does for a failed append.
- **Group writes within one version** replace the group's unpublished entry in place
  rather than chaining one per row, but each still copies the group: loading *n* rows
  into one group in one version is O(n²) copying. That is the "known limit".
- **Probing** reads the group at the pinned version, then each row at the same version:
  two chain reads per row instead of one, no allocation.
- **Joining through an index** is a separate operator from the primary-key join, because
  a key has a group of rows rather than one or none. The table's changes are re-keyed by
  the index (old row out of its group, new row into its own, both emitted even when the
  key is unchanged) and exchanged by the facts' hash; rows in that delta are compared by
  identity, not value. For the left join the operator needs to know whether a group was
  empty before the step: it reads the group's size at the pinned version and subtracts
  the step's net change, which it sees whole because the changes are routed like the
  facts.

**Rows must be immutable reference types.** A row object is shared by every circuit in
the process. This is the existing "delegates must be pure" rule with a much larger blast
radius; the doc for `SharedTable` says so first. Rows are constrained to `class` (records):
reference rows are what wide reference data should be anyway, and a null row is then the
tombstone, in the view and in `TableChange` alike.

## The change log

`ITableLog` is the seam between this design and the durability design:

- `Append(version record)` — the changes of one version plus the source offsets the writer
  consumed to produce it, atomically;
- `Read(fromExclusive, toInclusive)` — version records in order;
- an identity (**epoch**) minted when the log is created.

The durability design ([durability-and-standby.md](durability-and-standby.md)) fills this
seam with Kafka and sharpens its shape in six ways, all adopted here:

- **Visible means durable.** `Append` is awaitable, and the writer publishes a version to
  the view only after it completes. Otherwise a circuit could pin and checkpoint a version
  that a crash then erases. Table updates therefore lag by one log commit, and the writer
  batches whatever arrived meanwhile into the next version.
- **The writer assigns version numbers** and passes them to `Append`. They are positions in
  the log logically, but not a physical offset: Kafka offsets are not dense under
  transactions.
- **Restore** is its own entry point — the current rows plus the head (epoch, last version,
  source offsets) — so restoring is bounded by table size, not by history.
- **Retention** is explicit: trim before a version, and report the oldest retained one.
  The floor is the lowest version committed by any pipeline that still exists, running or
  not, so the host must own that list.
- **Follow mode**: a read that waits for new versions, which is how a standby keeps up.
- **`Append` can fail as fenced** (another process has taken over). That faults the table
  writer and demotes the process.

As built, the interface is: `Epoch`, `AppendAsync(version)`, `ReadAsync(from, to)`,
`FollowAsync(from)`, `RestoreAsync(row => …)` returning the head, `TrimBeforeAsync(version)`
and `OldestRetainedVersion`; a fenced append throws `TableLogFencedException`. Any failed
append faults the writer for good, fenced or not, because whether the version reached the
log is then unknown.

**The standby's table** (built with durability step 2). The seam gained
`LastVersionAsync()`: the last durable version, which is how far a follower must have read
before it may write. `SharedTable.FollowAsync(name, key, log)` opens a table that restores
from the log and then applies every version `ITableLog.FollowAsync` yields to its own
view — rows, secondary indexes, count, source offsets and the tail, as the writer would,
without appending anything. Circuits can be built against it; its `Writer` refuses every
call, and a `ToTable` circuit cannot write it. `table.PromoteAsync()` reads to
`LastVersionAsync()`, stops following and opens the writer, which carries on at the next
version with the replay positions the last version recorded. The caller must have fenced
the previous writer first; the table does not. A follower that cannot continue (the log
no longer holds the version it needs next) cannot be promoted and must be reopened.
`SharedTable` is `IAsyncDisposable` for one reason: to stop a follower that is dropped
without being promoted.

**The Kafka log** (built with durability steps 3 and 4) is `KafkaTableLog` in
`Incremental.Kafka`: `await kafkaLog.OpenTableLogAsync<TKey, TRow>(name)`. It is the
durable log this section keeps referring to; what it adds and where it departs from the
durability design is recorded there (*As built: `Incremental.Kafka`*). Two things a table
owner must know: the key type must serialize equal values alike (it is refused
otherwise), and nothing trims the log unless the host calls `TrimBeforeAsync`.

**The Kafka adapter** (built with durability step 6) is `KafkaSource.StartTableLoaderAsync`
in `Incremental.Kafka`: it reads a topic as the table's writer, stages each change with
its partition and offset, commits a version whenever it has read all that was fetched,
and resumes from `table.CommittedOffsets`. With `Debezium.Changes<TKey, TRow>()` as its
decoder the topic is a database table's change stream. Recorded in
durability-and-standby.md, *As built (step 6)*.

`InMemoryTableLog` now models fencing. An instance is one connection to the log;
`Connect()` returns another to the same log (the standby's), `TakeOver()` makes a
connection the writer, and an append through any other connection throws
`TableLogFencedException`.

A table need not have a log at all. `new SharedTable(...)` has none: it mints its own
epoch, so it is a new table every time the process starts, and "durable" means nothing
more than "published". `SharedTable.OpenAsync(..., log)` restores from the log and appends
to it. `InMemoryTableLog` is the heap-backed log — rows, head, and a capped number of
versions of history — which stands in for the Kafka one in tests and lets a table outlive
the object reading it within one process.

The table keeps a bounded in-memory tail of recent versions, which serves every circuit
that is keeping up. It is bounded twice: in versions (`TailVersions`, default 1024) and
in row changes across them (`TailChanges`, default 65,536). The second is the one that
bounds memory, because each change in the tail holds the row it replaced: bounded in
versions alone, the tail kept every superseded row of a resync alive — a second copy of
the table — with no reader to want them. The newest version always stays, whatever its
size, so one commit larger than the budget is held until the next; bulk loads commit in
chunks for this reason too. A circuit further behind reads from the log itself. A running circuit is *attached* to each table it reads, from `Start()`
until it is disposed or faults, and the tail gives up a version only when every attached
circuit has consumed it or the log still holds it.

**Coalescing.** A circuit moving from `p` to `t` gets one change per key: the `Old` of the
key's first change after `p` and the `New` of its last change up to `t`. That needs only
the log, never the view as of `p`, so the view retains nothing for circuits that are
between steps or stopped.

**Catching up.** A circuit that is behind always moves to the latest version in one step.
It cannot stop part-way, because probing needs the view at the version it stops at and
old versions are not retained. The catch-up delta is bounded by the number of distinct
keys changed, at worst the table size.

**Falling off the in-memory tail.** With a durable log (the Kafka implementation in the
durability design) the circuit reads the older records from it. With the in-memory log
used in tests and single-process runs there is nothing older to read:

> **Decided.** The writer waits — bounded backpressure, consistent with the rest of the
> library, at the price that one stalled pipeline delays reference updates for the others
> sharing that table. With the durable log this never happens: the lagging circuit reads
> the older records from it.

The wait is in `CommitAsync`, before the batch is taken, so cancelling it loses nothing.
That is as far as cancellation goes: once the batch has been handed to the log the commit
runs to its end, and the caller's token is not passed to `Append`. An append abandoned
half-way is a version whose durability nobody knows, which is the terminal fault above;
a timeout on one commit must not be able to cause it. The wait is counted in row changes
as well as versions (`TailChanges`), so a circuit that is not stepping stops the writer
after a few large versions rather than after a thousand of any size.
It is why an auto-stepping circuit steps when a table publishes a version even if no
input record arrives: otherwise an idle pipeline would stall the writer, and a retroactive
join would sit on a change until the next fact. A manually stepped circuit that is never
stepped does stall it; that is the stated price.

A log that holds history only up to a cap (the in-memory one, or a durable one with a
retention cap) lets the tail move on, and a circuit that then falls behind the cap as
well finds its range truncated and faults. That is the cap doing its job — it exists so
that one dead pipeline cannot pin history forever — and such a circuit needs a fresh
state store, as under *Retention*.

A circuit's position moves when a step drains, not when it commits. A step that faults
has therefore consumed changes that were never checkpointed; the rebuilt circuit reads
them again from its committed version, like any replayed input.

**Retention.** A circuit can only resume if the log still holds its committed version.
The log must therefore retain at least as far back as the oldest attached circuit's
checkpoint; a circuit whose version has been truncated refuses to start, like a manifest
mismatch, and needs a fresh state store.

**Writer exactly-once.** Each version records the source offsets it consumed, so after a
restart the adapter resumes from `table.CommittedOffsets` — the same contract as circuit
inputs, including the other half of it: a record stamped at or below its partition's
position is dropped, so an adapter that resumes a little early cannot step a row back to
an older value (which a lookup would never correct). Offsets of records that changed
nothing are not worth a version of their own; they ride with the next version that changes
something, so `CommittedOffsets` can trail the last record accepted, and replaying those
records after a crash changes nothing again.

That holds only while the source still has those records. An adapter that has gone idle
past the committed position — on records that changed nothing, or on offsets that hold no
record, such as a Kafka transaction's marker — calls `Writer.AdvanceAsync(partition,
offset)`: the next `CommitAsync` then records the position even with nothing to publish,
as a version with no changes. It is the one case in which such a version is minted, and
the caller chooses when it is worth one (the Kafka loader waits until its topic has been
quiet for `CommitIdlePositionAfter`). Added for durability-and-standby.md, *Offsets that
yield nothing are recorded too*.

## Snapshot sync

`ReplaceAllAsync(rows)` is the "snapshot" half of a snapshot + delta source. The writer
upserts the rows in chunks (each chunk a version), remembers the keys it saw, and finishes
with a version that deletes every key the snapshot omitted — a scan the table can do
because it owns its rows. Readers may observe the intermediate versions; each is a valid
mix of old and new rows, and the final one equals the snapshot. An adapter then continues
with ordinary `UpsertAsync`/`DeleteAsync` stamped with the source's sequence numbers.

As built: it takes an `IEnumerable` or an `IAsyncEnumerable`, and chunks by
`SharedTableOptions.ReplaceChunkRows` (default 4096). The last version carries the final
partial chunk together with the deletions, so a snapshot of *n* rows is at most
⌈n / chunk⌉ versions, and fewer when chunks change nothing — re-syncing an unchanged
snapshot mints none. The keys seen are held in a set for the duration, so a sync costs
memory proportional to the snapshot's key count. Three things to know:

- **Offsets.** The optional `offsets` argument is the source position the snapshot is as
  of. It is staged with the last version, so it is recorded only if the snapshot
  completes, and from then on delta records at or below it are dropped. It is recorded
  even when the sync's last version changed nothing — as a version with no changes, like
  `AdvanceAsync` — because a snapshot is taken when the old position is no good, and an
  unrecorded one would be taken again at every start. (As first built it rode with the
  next version that changed something.) Without `offsets`, re-syncing an unchanged
  snapshot still mints nothing.
- **Stopping early.** If the sequence throws or the call is cancelled, the chunks already
  committed stay, nothing is deleted, and rows staged but not committed stay staged for
  the next commit. The table is a valid mix; the next sync finishes the job.
- **Not concurrent with other writes.** Anything staged before the call is committed with
  its first chunk. A row upserted by another caller while a sync runs is not in the
  snapshot and would be deleted by the last version; the writer does not police this.

## The table as a stream

`TableRef.Changes()` is the table's change delta as a `ZStream<TRow>`: each `TableChange`
becomes its old row with weight −1 and its new row with +1.

- **Bootstrap.** A circuit with no committed position in the table (a new circuit, or one
  that never reached a checkpoint) starts the stream with every row at its first step's
  pinned version, as insertions. The table's port scans the rows at that version — safe
  beside the writer because the version is pinned — and stages them straight into the
  `Changes()` operator, round-robin across workers. The delta is the size of the table,
  in one step.
- **The rows are pending work.** A circuit that has not bootstrapped yet has something
  to do whether or not the table ever changes again, so its port reports work pending
  until that first step has run: the pump takes the step unprompted, and disposing a
  circuit that never stepped drains it like anything else buffered. Without this a
  pipeline over reference data that is loaded and quiet would stay empty.
- **A scan the writer overtakes faults the step.** The scan's pin can be revoked like any
  other (`MaxRetainedVersions`), and a table too large to scan inside that window under
  its write rate cannot be bootstrapped; the step fails rather than deliver the rows it
  happened to reach. Without a log the writer waits for the circuit long before that
  (`TailVersions`); with one, or with a small cap, raise the cap.
- **One table-sized delta, and what it leaves behind.** The rows go out in a Z-set of
  their own, not through the operator's reused output buffer. Every operator downstream
  still sizes its own buffers from that delta, and emptying a buffer costs time in
  proportion to its capacity — so reused Z-sets now give capacity back once it has gone
  unused for 64 executions (`ZSet.Clear`), and a `--assert` contract holds the cost of a
  small change after a million-row bootstrap to that of one after a thousand. Two things
  this does not reach: an operator that never runs again (a table that never changes)
  keeps the buffer it has, and operators' scratch dictionaries keyed by the delta's
  distinct keys keep their capacity as they always have after any large delta.
- **The first step's ordinary changes are dropped.** The versions published between
  `Start()` and that first step are already in the rows just scanned, so they are not
  staged as changes as well. That holds for the whole circuit, not only for `Changes()`:
  a join in a circuit with no checkpoint has no facts yet for those changes to correct.
- **Restart.** A circuit with a committed version resumes from it and receives only the
  changes since; its state already has the rows.
- **The limitation.** Bootstrap is decided by the committed position, not by whether
  `Changes()` was there before. Adding `Changes()` to a circuit that already has a
  checkpoint with this table attached gives a stream that starts at the committed
  version, with no rows before it. Such a pipeline needs a fresh state store.
- Like everything else on a table, not inside `Recurse` — though the stream it returns
  can be imported into one.

## Checkpoints and identity

- The pinned version commits in the existing offsets marker under `{table}#t`, alongside
  input offsets. `#` is already reserved in input names; table names get the same check.
  The table's port is an input port under that name, which is all it takes for the version
  to be committed, seeded on `Start()` and counted as pending work by the pump. It differs
  from an input in one way: it never completes. The pump therefore ends when every *other*
  input has completed and been drained — it completes the table ports itself and detaches
  them, so a finished circuit that nobody has disposed yet holds no history back — and a
  circuit whose only inputs are tables runs until it is disposed.
  `circuit.CommittedVersion(table)` reads it back — the number a retention floor is built
  from.
- The state manifest gains one line per attached table, `table={name}@{epoch}`. A table
  that was recreated has a new epoch, the committed line has no match, and the circuit
  refuses to start instead of applying a version number to the wrong log. This is a new
  line kind, so the manifest format version is bumped (to 5). Only a changed epoch under
  the same name is a mismatch: a table attached since the checkpoint starts at its
  current version, and one no longer attached is ignored, as a removed input is.
- A table the circuit *writes* (`ToTable`) has the same manifest line, and commits the
  version its checkpoint covers under `{table}#w`; see *Derived tables*.
- A circuit also refuses to start when the table is *behind* its committed version (same
  epoch, but restored from somewhere that never reached it), or when the changes after
  that version are retained nowhere.
- Join state is ordinary operator state: one indexed store for the retroactive join
  (`facts`), one for the remembered lookup (`remembered`). `.Named()` works as elsewhere.
  Indexes are not persisted or logged: they are rebuilt from the rows when declared.

## Workers and clusters

- **Workers > 1.** Any worker may probe the table; reads need no partitioning. Table
  changes are staged round-robin like plain input records and reach the right worker
  through the join's exchange.
- **Clustered circuits.** A table lives in one process. A circuit with both
  `CircuitOptions.Cluster` and a table is rejected at `Build()`. High availability is one
  active process plus a standby, which does not use the exchange cluster.
- **Standby.** A standby follows the same log and so reaches the same versions, which is
  what makes a pipeline's committed version meaningful after failover. The details belong
  to the durability design; this design only promises that versions are positions in the
  log, not counters local to a process.

## Derived tables

> **Decided.** Derived tables are in the first version; the star join is not.

A derived table is a shared table whose writer is a circuit instead of a source adapter.
It turns a dependent chain into one hop: `traders ⋈ desks` is computed once, published as
`trader-desks`, and every pipeline joins trades to it directly — one fact copy instead of
one per hop, and the two-hop work done once for the whole process.

```csharp
// A small circuit owned by the service, not by any tenant pipeline.
var traders = builder.Table(tradersTable);
var desks   = builder.Table(desksTable);

traders.Changes()
       .GroupBy(t => t.DeskId)
       .Join(desks, (deskId, trader, desk) => new TraderDesk(trader.Id, trader.Name, desk.Name, desk.Region))
       .ToTable(traderDesksTable);          // keyed by the table's own key selector
```

- **Bootstrap.** `Changes()` on a new circuit begins with the pinned version's rows as
  insertions (see *The table as a stream*), so a derived table can be added while its
  sources are already loaded.
- **Writing.** `ToTable` collapses each step's retractions and assertions to one upsert or
  delete per key and commits them as one table version per step. More than one live row
  for a key is an error: a table has one row per key.
- **Crash consistency.** The table version for a step is written before the circuit
  commits that step's checkpoint — the same commit-after-delivery rule as external sinks.
  After a crash the circuit recomputes the steps since its checkpoint and writes them
  again; rows that are already current are no-ops and create no version, so the table
  converges. A reader may briefly see a row step back to an older value and forward
  again, which a retroactive join corrects and a lookup does not — the usual
  at-least-once caveat.
- **Recovery and standby.** A derived table has its own log and is restored or followed
  like any other table. The only ordering rule is the general one: tables before circuits.
- **Cycles.** A circuit may not write a table it reads, directly or through other derived
  tables; registration rejects it.

As built (`Tables/DerivedTable.cs`: `ToTable`, `DerivedTablePort`, `TableLineage`;
`DerivedTableTests.cs`). The bullets above stand except where this says otherwise.

- **API.** `stream.ToTable(table)` on a `ZStream<TRow>`, and
  `keyed.ToTable(table, (key, value) => row)` on an aggregate's result. There is no key
  selector argument, unlike the sketch: the table already has one, and a second could only
  disagree with it. `ToTable` returns nothing — there is no handle to redeem.
- **The circuit is the table's writer.** `Build()` claims the table; the claim ends when
  the circuit is disposed, faults, finishes (an auto-stepping circuit whose inputs have
  all completed) or refuses to start. While it is held, every call on `table.Writer`
  throws. A circuit built and then dropped without being disposed keeps the claim.
- **The table is the circuit's output and nothing else.** The first step of a circuit with
  no checkpoint deletes every row it does not itself assert, in the same version as its
  first rows — so a table that already matches loses nothing, and rows an adapter or an
  earlier circuit left behind do not survive. Changes staged on the writer and never
  committed are dropped.
- **The port.** An acknowledged output (`IAcknowledgedOutputPort`), so commit after
  delivery applies with no change to the gate: delivery and acknowledgement are one event,
  the table version committing, and the circuit's flush is that commit. The stream is not
  partitioned by the table's key, so the port merges the workers' deltas on the
  coordinator before it collapses them; the merged delta and the collapse scratch are
  reused across steps. A step that changes nothing mints no version.
- **Table-shaped is checked against the table's own rows**, not only within the step as
  an external sink must: a retraction has to match the row the table holds for the key,
  and an assertion with no retraction needs the key to be absent. So a second row for a
  key arriving in a *later* step faults the circuit too, where an upsert sink would
  overwrite the first and later delete the key under the second. These are faults under
  either error policy. A step rejected this way — or whose commit fails or is cancelled —
  leaves nothing staged on the writer: what it had staged before the bad key is discarded
  with it. (As first built it stayed, and the next direct `CommitAsync` after the circuit
  gave the table up published half of the rejected step.)
- **Crash consistency is by position, not by replay.** The bullet above says rows that are
  already current are no-ops, "so the table converges". That holds only when the replay
  cuts the same steps (the transient-key gap recorded in durability-and-standby.md). What
  is built instead: every checkpoint records the table version it covers, under
  `{table}#w` in the offsets marker. A circuit resuming from a checkpoint reads the
  versions written after that one — from the tail, or from the log when the table was
  reopened — and restores each changed key's old row, in the same version as its first
  step's changes. The table therefore equals what the circuit's state describes after
  every step, however the replay is cut and even if the source cannot replay at all. A
  replay that repeats the crashed step exactly nets out to no version; otherwise readers
  see the rows step back and forward, as the bullet says. This is the "equivalent
  step-sequence stamp": the writer's offset stamps are not used, because their dedup would
  drop a replayed step whose contents differ from the first run's.
- **The first step is owed.** A circuit that writes a table takes one step as soon as it
  starts, whether or not it has input, because the reconciliation above is that step's
  work (like a pending `Changes()` bootstrap). With manual stepping it is the first
  `StepAsync`.
- **What a restart needs retained.** The versions between the circuit's checkpoint and the
  table's latest. The table's tail keeps them (`_writerHold`): it does not evict past the
  writing circuit's last checkpoint, and — unlike a reader's position — this never makes
  the writer wait, since it would be waiting for its own checkpoint. So with
  `CheckpointInterval` K the tail can hold K steps' versions beyond its bounds. The hold
  outlives the circuit, for the one that replaces it. It ends when a circuit starts on the
  table without resuming, or when the table is written directly through `table.Writer`
  with no circuit holding it: nothing advances a dead circuit's hold, so every direct
  version after it would otherwise stay in the tail for good (as first built it did). A
  circuit that later resumes from the old checkpoint finds the versions it must undo
  still in the tail, or is refused at `Start()`. With a
  log, retention must reach back to `circuit.CommittedVersion(derivedTable)`, which now
  answers for a table the circuit writes as well as for one it reads: the host's
  retention floor includes it.
- **Refusals at `Start()`**, each needing a fresh state store for the writing circuit: the
  table's epoch differs from the checkpoint's (the manifest's `table=` line, shared with
  tables that are read); the table is behind the checkpoint's version; the versions since
  it are retained nowhere; and `ToTable` was added to a circuit that already had a
  checkpoint (its state holds rows the table was never given).
- **No log.** A table made with `new SharedTable(…)` is a new table every process start.
  A circuit with a durable checkpoint is refused against it, as above — nothing can
  rebuild the unchanged rows from changes alone. The consistent pairings are a logged
  table with a checkpointed circuit, or an unlogged table with a circuit that also starts
  empty and recomputes everything (from `Changes()` bootstraps or replayed inputs).
  Within one process an unlogged table and a checkpointed circuit do work: the table
  object outlives the circuit, and the tail holds what a restart undoes.
- **Backpressure.** The writer-waits rule reaches the circuit unchanged: the table commit
  waits for a reader a full tail behind, the flush does not land, the next step waits for
  the flush, and the inputs fill. Disposing a circuit stalled this way waits too.
- **Cycles and second writers are rejected at `Build()`**, not at a registration step:
  `TableLineage` records, per table, the writing circuit and the tables it reads, and
  the build that would close a loop — or claim a claimed table — throws. Two `ToTable`
  onto one table from one circuit is refused where it is declared. Clustered circuits
  are refused at `Build()`; inside `Recurse` at the call.
- **Checkpoint format.** No manifest bump. The derived table's line reuses the `table=`
  kind (a circuit cannot both read and write a table, so names do not collide), and
  `{table}#w` is one more entry in the offsets marker, which older code ignores. A
  checkpoint from before the circuit wrote the table is the refusal above, not a silent
  misread.
- **One table-sized step.** The bootstrap of a large derived table is one version, by the
  rule above, where a source adapter would chunk. The port and the writer give their
  scratch back after a step of more than 65,536 keys; a `--assert` contract holds a small
  change after a million-row bootstrap to the cost of one after a thousand.
- **Not done.** Hosting knows nothing of this yet (step 7): in particular the host's
  shutdown timeout cannot abandon a table commit that is waiting on a lagging reader.
  `OnError(DeadLetter)` does not apply to a row `ToTable` rejects. Nothing stamps a
  version with the step that wrote it.

## What it costs and what it bounds

- **Memory.** One row object, one slot and one map entry per key, per process — however
  many pipelines join the table — plus the tail: up to `TailChanges` recent changes, each
  holding the row it replaced until it leaves the tail (and one version beyond that
  budget, if a single commit exceeds it). Each retroactive join adds its fact-side state; each
  remembered lookup adds its enriched facts; an append-only lookup adds nothing.
- **Probe.** One concurrent-map read plus one chain step, no allocation.
- **Not bounded by this design.** A retroactive join still emits one retraction and one
  assertion per matching fact when a hot reference row changes; that is what lookup is
  for. A chain of joins against *independent* tables still stores the widened fact once
  per hop; dependent chains (trades → trader → desk) collapse to one hop through a
  derived table, and the star join below is the follow-up for the rest.

## Invariants this changes

`CLAUDE.md` says operator state is sharded per worker and never shared across threads.
That stays true of operator state. Shared tables become the single stated exception:
single writer, readers lock-free at a pinned version, rows immutable. Nothing a circuit
does may write to a table — except the one circuit that is a derived table's writer, which
writes it from its coordinator between steps, through the same writer and under the same
rules as an adapter.

## Out of scope, with the hook each needs

- **Star join.** One operator joining a fact against several tables, storing the fact once
  with an index per foreign key. Needs the view as of the *previous* version during the
  step, which means pinning two versions instead of one.
- **As-of event-time joins.** Would keep versions keyed by event time rather than log
  position. A different retention rule, the same view structure.
- **Wait-for-reference in lookup.** Parking facts whose reference row has not arrived yet.
  Today that is what the retroactive join does.
- **Processing-time expiry of fact state.** Still watermark-only via `EvictWhen`.

## Tests

- **Differential property test.** Random interleavings of fact deltas and table commands,
  with random version boundaries and worker counts: the table join's integrated output
  equals the existing `Join` over a `KeyedInput` fed the same commands. The same for
  `LeftJoin`.
- **Concurrency stress.** One writer churning while several circuits step: every probe
  inside a step returns the row as of the pinned version; chains are trimmed back to one
  slot once the circuits are idle.
- **Restart.** Checkpoint, more table versions, rebuild: the circuit resumes from its
  committed version and its output matches an uninterrupted run. Epoch mismatch refuses
  to start.
- **Lag.** A circuit stopped for longer than the in-memory tail catches up in one step
  with the coalesced delta.
- **Performance contracts** (the `--assert` suite): probe time and allocation independent
  of table size and of the number of versions written; zero bytes allocated per probe;
  per-step cost of an attached but unchanged table is constant. As built: the suite has a
  delta-cost contract each for the table join, the index join and the remembered lookup
  (1,000 → 64,000 rows and facts), one for a small change to a table read through
  `Changes()` after its bootstrap (1,000 → 1,000,000 rows), an absolute ceiling on what
  the lookup cycle allocates, and zero allocation per probe — by key and through an
  index — is asserted exactly by a unit test (`TableIndexTests`) rather than as a ratio.
- **Lookup.** Differential against a direct model of the invariant (each held fact
  enriched as of its last assertion), for the key, the key with `missing`, and a
  non-unique index, at one and four workers; retractions sent to a different worker from
  their assertions; append-only faulting on a retraction.
- **Indexes.** The index join and left join against the stream join on a grouping of the
  same column; readers checking exact group membership at their pinned version under a
  writer that rewrites every row every version.

- **Derived tables.** A two-circuit pipeline (`A --ToTable--> T --Join--> B`) against the
  single circuit doing both joins on streams, with A, B and the source table's commits
  interleaved at random, at one and four workers. A crash property: stamped commands, a
  crash at a random step at each of three points (before the table version, after it and
  before the checkpoint, after the checkpoint), restart from the log or in process, replay
  with a different cut — the table must equal the model after *every* step, including the
  restart's first. Directed: the transient key, the guard rails, each refusal, the tail
  hold, a lagging reader stalling the writer and its producers.

## Build order

1. **Done.** `SharedTable` core: view, writer, pins, trimming
   (`src/Incremental/Tables/SharedTable.cs`), benchmarked against the persistent-map
   alternative.
2. **Done.** `ITableLog` and `InMemoryTableLog` (`Tables/TableLog.cs`); the staged,
   offset-stamped writer with append-before-publish; the tail, attached readers and the
   waiting writer; `MaxRetainedVersions` revocation; `builder.Table()`, `TableRef`, the
   table port and the coalescer (`Tables/TableRef.cs`); the committed version and the
   manifest line. What step 3 builds on, both internal to `TableRef`: `Probe`
   (`TryGet`/`GetManyInto` at the step's pinned version, from any worker) and
   `ChangeStream` (the step's `TableChange` delta, weight +1 each, to be routed through
   the join's `GroupBy` exchange). The write numbers in the table above predate the tail:
   each changed row now also costs its entry (key and two references) in the version
   record the tail holds, and they have not been re-measured.
3. **Done.** Retroactive `Join` and `LeftJoin` of a grouping against a `TableRef`
   (`Operators/TableJoinOperator.cs`, `Grouping.cs`), with the differential properties and
   directed tests (`TableJoinTests.cs`) and a `--assert` contract: table-join delta cost
   against table size and fact-state size.
4. **Done.** `Lookup` (remembered, on a stream or a grouping) and `LookupAppendOnly`
   (`Operators/TableLookupOperator.cs`, `ZStream.cs`, `Grouping.cs`; `TableLookupTests.cs`).
5. **Done.** `ReplaceAllAsync` (`Tables/SharedTable.cs`); secondary indexes — `AddIndex`,
   `By`, `TableView`, the index join (`Tables/TableIndex.cs`, `Tables/VersionChains.cs`,
   `Operators/TableIndexJoinOperator.cs`; `TableIndexTests.cs`); `Changes()` with
   bootstrap (`Operators/TableRowsOperator.cs`, the table port; `TableChangesTests.cs`).
   Step 6 can build `ToTable` on `Changes()` as it stands; the hosting step should
   declare indexes at registration, before the adapters load.
   **Review fixes, done.** A pending bootstrap counts as pending work; a bootstrap scan
   checks for revocation; the pump no longer waits for tables to complete; the append is
   not cancellable; the tail is bounded in row changes (`TailChanges`); one-value groups
   are inline in the in-memory indexed store; reused Z-sets give back unused capacity.
   Each is recorded in the section it belongs to above.
6. **Done.** Derived tables: `ToTable` on a stream and on a keyed result
   (`Tables/DerivedTable.cs`; the writer's owned path and the tail's writer hold in
   `Tables/SharedTable.cs`; the write position in the checkpoint, `Circuit.cs`;
   `DerivedTableTests.cs`). Built as a port of its own — an acknowledged output whose
   acknowledgement is the table version committing — with its own collapse rather than
   `UpsertDelivery`'s, because it checks each key against the table's row. The
   transient-key gap is closed by undoing, on restart, what was written past the
   checkpoint. See *As built* under *Derived tables*. What step 7 must know: `Build()`
   claims the table, so build a derived table's circuit once per generation and dispose
   the old one first (a faulted circuit has already let go); order tables before
   circuits, and a derived table's writing circuit before or after its readers as
   convenient — readers attach at whatever version the table is at; include
   `CommittedVersion(derivedTable)` of the writing circuit in the log's retention floor;
   and a stalled table commit is not ended by the host's shutdown timeout.
7. Registration in `Incremental.Hosting`; the TradeEnrichment example moved onto tables as
   the worked example.
