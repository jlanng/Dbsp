# Design: durability and warm standby

> **Status:** agreed design, 2026-11-09. Build-order steps 1 to 4 are built: commit after
> delivery (with the sink side of keyed upsert delivery and the per-record error policy; see
> the two *As built* sections under *Commit after delivery*); the state changelog seam,
> the changelog provider, the in-memory changelog and follower mode for stores and tables
> (see *As built (step 2)* under *Warm standby*); and, in `Incremental.Kafka`, the log
> writer, the state changelog and the table log on Kafka (see *As built:
> `Incremental.Kafka`*). The leader protocol, the source adapter and the hosting roles are
> not.
> Sibling of [shared-tables.md](shared-tables.md); it fills the `ITableLog` seam that
> design leaves open. Blocks marked **Decided** record a choice James made.
> Kafka and MSK facts were checked against the sources in *References*; anything not
> confirmed is listed under *Not verified*.

## Why

The enrichment service runs many pipelines, each its own circuit (usually `Workers = 1`),
in one process, joining streams against shared tables. Load fits one node, so the goal is
availability, not scale: one **active** process and one **warm standby** that takes over in
seconds. The requirements are fixed: Kafka (probably MSK) holds the replicated logs; sinks
get at-least-once delivery of keyed upserts; the exchange cluster is not involved; the Redis
provider and its JSON format are untouched.

What exists today falls short in three ways. Checkpoints commit *before* outputs are
published (`RunStepGatedAsync`), so a crash loses the batches in between. The only durable
provider is Redis, and restoring from it is a cold read. Nothing stops two processes from
both believing they are active.

## Shape

```
                 ┌───────────────────────── Kafka ─────────────────────────┐
 sources ──▶     │ {p}.leader                    1 partition  (heartbeats) │
 ┌────────┐      │ {p}.table.{name}.changes      1 partition  (versions)   │      ┌─────────┐
 │ ACTIVE │─txn─▶│ {p}.table.{name}.rows         R partitions (compacted)  │─read─▶ STANDBY │
 │circuits│      │ {p}.state                     P partitions (compacted)  │      │ no steps│
 └───┬────┘      └─────────────────────────────────────────────────────────┘      └─────────┘
     ▼ sinks (keyed upserts, acknowledged)
```

Everything the active must not lose goes through **one transactional producer**. The
standby reads the same topics with `isolation.level=read_committed` and applies them to
in-memory state of the same shape as the active's. It runs no steps and writes nothing.

Three packages:

- **core `Incremental`** gains the engine change (commit after delivery), an
  `IStateChangelog` seam beside `ITableLog`, and a `ChangelogStorageProvider` written
  against it, with an in-memory changelog so every rule below is testable without Kafka.
- **`Incremental.Kafka`** (new, depends on `Confluent.Kafka`) implements both seams, the
  leader protocol and the Kafka source adapter.
- **`Incremental.Hosting`** gets the active/standby roles last.

## The log writer

One object per process owns the transactional producer. Table writers and checkpoint
commits hand it work and wait:

```
loop:  take everything queued (wait up to a few ms if nothing is)
       BeginTransaction
       produce every queued append + one heartbeat record to {p}.leader
       CommitTransaction          -- abortable error: abort, retry the same batch
       complete the waiters       -- fatal or fenced: fail them all, demote the process
```

This is group commit. A Kafka producer runs one transaction at a time, so a producer per
pipeline would mean a `transactional.id` per pipeline and one fencing call each at
takeover; a single producer means one. The price is that every commit waits for the
transaction it rides in. If one producer's rate becomes the limit, a fixed pool of N
producers (ids `…-0` to `…-N-1`, all initialised at takeover) is the growth path.

A transaction gives two things. **Atomicity** across records and topics: a
`read_committed` reader sees all of it or none. **Fencing**: see *Leadership*. Because
every write is in a transaction and the standby only acts on its state after reading to
the end of committed data, readers never need to detect transaction boundaries.

## Record format

Every record value starts with a **format byte**, like the cluster's frames. Keys are
UTF-8 `segment NUL segment …` with the user key serialized last.

> **Decided (1): serialization.** JSON through the existing `IStateSerializer`, behind the
> format byte, with producer compression (`zstd` or `lz4`) — one persisted format and one
> set of type rules across Redis and Kafka, readable with stock tooling. Before
> `Incremental.Kafka` is declared stable, benchmark log bytes and encode cost on real row
> shapes and decide then whether the dense codec (`ShardCodec`) is worth making a
> persisted format; the format byte lets it be introduced per topic without rewriting
> history. Either way record *keys* must be byte-stable for a given value, because
> compaction compares bytes. The Redis provider already depends on that property.

The benchmark has been run; the numbers, and what they suggest, are under *As built:
`Incremental.Kafka`*. The decision itself is still open.

## Kafka-backed table log

Each shared table gets two topics, both written in the same transaction.

**`{p}.table.{name}.changes`** has one partition, `cleanup.policy=delete`,
`retention.ms=-1`. It is the version history. One version is one or more records:

```
key:    version (8 bytes, big-endian)
value:  format | epoch | version | chunk i of n | changes[(key, old?, new?)]
        the last chunk also carries the source offsets consumed up to this version
```

A version normally fits one record. A large one (a `ReplaceAllAsync` chunk, the final
snapshot delete, a derived-table step) is split into chunks under `max.message.bytes`;
the transaction makes the chunks atomic. `old` is recorded because coalescing needs it.

**`{p}.table.{name}.rows`** has `R` partitions (default 4, by `StableHash` of the key) and
`cleanup.policy=compact`. It is the snapshot: one record per changed key,
`key → (row, version)`, null for a delete. Each transaction also rewrites one `$head`
record per partition: `(epoch, version, source offsets)`.

**Version numbers** are assigned by the writer, 1, 2, 3 and so on, and travel in the
record. They are not Kafka offsets: transaction markers occupy offsets, so offsets are
not dense, and the rows topic needs the number inside the same transaction, before any
offset is known. Fencing guarantees a single writer, and a new writer continues from the
`$head` it restored. The active and the standby keep a sparse `version → offset` index
for the changes topic, built while reading, so a range read can seek.

**Epoch** is a GUID minted when the topics are created, repeated in `$head` and in every
change record. Dropping and recreating a table produces a new epoch, which is what the
circuit's `table={name}@{epoch}` manifest line catches.

**Visible means durable.** The writer publishes a version to the in-memory view only
after its transaction commits. Otherwise a circuit could pin and checkpoint a version
that a crash then erases. The cost is that table updates lag by one commit, and the
writer should batch whatever arrived during the previous commit into the next version.

**Restore.** Read the rows topic from the start to the end of committed data, applying
records to the view; `$head` gives the version and the source offsets. Then read the
changes topic from its first retained record to fill the in-memory tail and the offset
index. Restore time is bounded by table size, not by history: compaction keeps the rows
topic near the live size (about twice with default cleaner settings, plus the active
segment, which is never cleaned). Secondary indexes are rebuilt from rows, not logged.
A restore must finish within `delete.retention.ms` (24 h default) or it can miss a
tombstone.

> **Adopted (2): the table snapshot is a compacted topic.** No new dependency, atomic with
> the change log, and the standby follows it for free. MSK tiered storage cannot hold it,
> so the full table sits on broker disks. The alternative — periodic snapshot files in S3
> plus replay from the snapshot's version — adds a second storage system and is worth
> revisiting only if a very large table's cold restore proves too slow. (Put to James as
> the stated default rather than as a separate question.)

**Retention.** The floor is the lowest `{table}#t` committed by any pipeline that still
exists, running or not. The active computes it from the commit records it holds and calls
`DeleteRecords` on the changes topic up to the offset of that version. A cap
(`MaxRetainedVersions`, or bytes) stops one dead pipeline from pinning history forever:
past it the log is truncated anyway and that pipeline refuses to start, exactly as
shared-tables.md describes. A deposed active's truncation is harmless because floors only
rise and its view of them is stale, hence lower.

**A lagging circuit** asks for `(p, t]`. Versions still in the in-memory tail come from
there. Older ones are read from the changes topic by seeking through the offset index, and
coalesced as they stream: per key, the `old` of the first change and the `new` of the
last. The view is never consulted, so nothing extra is retained.

## Changelog-backed checkpoint provider

`ChangelogStorageProvider` implements `ICheckpointStorageProvider`. It is
`InMemoryCheckpointStorageProvider` with a log underneath: reads and staged writes stay in
heap dictionaries, and `Commit` appends the staged layer before promoting it.

One compacted topic, **`{p}.state`**, with `P` partitions (default 12), holds every
pipeline. All of a pipeline's records go to one partition, chosen by `StableHash` of the
pipeline id, so they stay ordered. `P` can never change.

```
key:    pipeline NUL incarnation NUL store NUL entry
value:  weight store          entry = value          → weight
        indexed weight store  entry = key NUL value  → weight
        value store           entry = key            → value
        null = removed (RemoveGroup writes one tombstone per member)

key:    pipeline NUL incarnation NUL $commit         (written last)
value:  commit sequence, leader term, input offsets (including {table}#t), manifests
```

Values are absolute, as the staged overlays already are, so replay is idempotent and
compaction is safe. Store names already carry the worker id, so per-worker state needs no
further mapping. The `$commit` record carries exactly what `Recover()` and
`CommittedManifest()` return.

**Commit** serializes the staged layer, hands the records to the log writer, waits, then
promotes. A failure throws, which faults the circuit, as a failed Redis commit does today.
`Commit` is synchronous, and with hundreds of circuits blocking thread-pool threads on a
transaction, starvation is a real risk. The engine change below adds
`ValueTask CommitAsync(...)` to the interface, defaulting to `Commit`, and the step loop
awaits it.

**Restore** reads the pipeline's partition to the end of committed data and applies each
record to the named store. It is bounded by live state, for the same reason as the rows
topic.

**Incarnations.** Starting a pipeline "against a fresh state store" (manifest mismatch,
truncated table version) bumps its incarnation. Records of older incarnations are ignored
on read and tombstoned by a background sweep. Deleting a pipeline is the same sweep.

> **Decided (3): one shared compacted topic with a fixed partition count.** Topic and
> partition counts never grow with the number of pipelines: with ten tables the whole
> design is about 63 leader partitions. The price is that removing a pipeline means
> tombstoning its keys, one partition's restore reads its neighbours' records too, and
> `P` can never change. A topic per pipeline was rejected: at replication factor 3 about
> a thousand pipelines reaches MSK's recommended 1,000 partitions per broker, and MSK
> Serverless allows only 120 compacted partitions per cluster.

## As built: `Incremental.Kafka` (steps 3 and 4)

Code: `src/Incremental.Kafka` — `KafkaLog.cs` (the connection, the log writer, topic
set-up, the readers), `KafkaLogOptions.cs`, `KafkaStateChangelog.cs`, `KafkaTableLog.cs`.
Tests: `tests/Incremental.Kafka.Tests` (`KafkaLogTests.cs`, `KafkaTableLogTests.cs`,
`KafkaEndToEndTests.cs`), all against the local broker and skipped when `localhost:9092`
is unreachable. Benchmark: `benchmarks/Incremental.Benchmarks/LogSerializationBenchmarks.cs`.
Built against Confluent.Kafka 2.16.0 and tested against Apache Kafka 3.9.0 (KRaft, one
broker).

```csharp
await using var log = await KafkaLog.ConnectAsync(new KafkaLogOptions
{
    BootstrapServers = "…", TopicPrefix = "enrichment",       // transactional.id defaults to "{prefix}.writer"
    ClientConfig = { /* MSK: security.protocol, sasl.mechanism, … */ },
});

// The active.
await log.AcquireWriterAsync();                                // InitTransactions: fences; commits the next term
var instruments = await SharedTable<int, Instrument>.OpenAsync(
    "instruments", i => i.Id, await log.OpenTableLogAsync<int, Instrument>("instruments"));
await using var storage = await ChangelogStorage.OpenAsync(log.State);

// The standby: the same, with FollowAsync / Follow and no AcquireWriterAsync. Takeover:
await log.AcquireWriterAsync();
await instruments.PromoteAsync();
await storage.PromoteAsync();
circuit.Start();
```

**One object per process.** `KafkaLog` owns the one transactional producer, the admin
client and the topic names. `log.State` is the `IStateChangelog`; `log.OpenTableLogAsync`
returns an `ITableLog`. A log is a reader until `AcquireWriterAsync`, which
`ChangelogStorage.OpenAsync` and `PromoteAsync` also reach through
`IStateChangelog.AcquireWriterAsync`; it runs once, and later calls return the first
result. The `IStateChangelogWriter` handed to the storage does not own the producer —
disposing it does nothing — because tables write through the same one. `KafkaLog.DisposeAsync`
closes everything.

**The log writer.** A dedicated thread, because every librdkafka transaction call blocks.
A commit (a checkpoint, or a table version) is a list of records and a completion. The
thread takes everything queued, runs one Kafka transaction over it, and completes the
commits. `GroupCommitLinger` defaults to zero: commits that arrive while a transaction is
in flight ride in the next one, which is where the batching comes from. In the test, 300
concurrent committers doing three rounds each — 900 commits — took 6 Kafka transactions
(it asserts at most a quarter as many transactions as commits). `MaxTransactionRecords` (default 100,000) bounds how much
one transaction carries; a single larger commit is never split. Outcomes:

| What the client says | What the writer does |
|---|---|
| `CommitTransaction` succeeds | completes every commit in the batch |
| `KafkaRetriableException` from commit or abort | calls again; the call resumes, the outcome is still open |
| `KafkaTxnRequiresAbortException`, or any other non-fatal error from commit | aborts, then runs the same batch again, up to `MaxAbortRetries` (5) with a short backoff; then gives each commit a transaction of its own; a commit that still aborts fails with `KafkaLogException` |
| `Produce` refuses one record (too large, say) | aborts; that commit alone fails with `KafkaLogException`; the rest of the batch runs again |
| fatal, `Local_Fenced`, or an abort that fails | the writer is finished: every waiting and later commit fails with `KafkaLogFencedException`, `WhenFenced` completes |

That is the seam's two failure meanings. `KafkaStateChangelog` rethrows the second as
`ChangelogFencedException` and `KafkaTableLog` as `TableLogFencedException`, each with the
`KafkaLogFencedException` inside. A commit is not cancellable once queued. A transaction
the broker times out (`transaction.timeout.ms`, default 60 s) comes back as a fence, so a
Kafka outage longer than that ends the active even with no standby taking over; that is a
departure from *Failure modes* ("Both recover when Kafka does") and is kept, because the
client cannot tell the two apart.

**Fencing, verified** (resolves the item under *Not verified*). With client 2.16.0
against Kafka 3.9.0, a producer whose `transactional.id` has been initialised elsewhere
gets `ErrorCode.Local_Fenced` ("This instance has been fenced by a newer instance"):

- from `CommitTransaction`, `AbortTransaction` and `BeginTransaction` as a `KafkaException`
  with `Error.IsFatal == true`, and on the error handler, also fatal;
- from a later `Produce` as a `ProduceException` with the same code but `IsFatal == false`.
  The writer therefore checks the code as well as the flag;
- a producer fenced while idle does not find out until it writes again. Its next
  `BeginTransaction` and `Produce` succeed locally; the fatal error reaches the error
  handler when that transaction first talks to the coordinator, and the commit then
  fails. So `WhenFenced` completes on a zombie's next write, not before. Telling an idle
  zombie it has been deposed is the heartbeat topic's job (step 5).

`InitTransactions` took about 0.5 s every time against the local broker, with or without
an open transaction to abort. That is the floor of a takeover here; its cause was not
investigated.

**Terms.** `AcquireWriterAsync` reads `{p}.leader` (one partition, compacted) for the last
term and commits the next under the key `term`, before it returns. Reading after
`InitTransactions` makes the last term final; committing through the transactional
producer makes the new one unique. This is the "write the new term" of takeover step 4,
brought forward because `IStateChangelogWriter.Term` needs it. Heartbeats, the election
and the delivery lease are still step 5.

**Reading to the end.** Two facts about Kafka shaped the readers:

- *A commit is not readable the moment it returns.* The broker writes a transaction's
  markers after it answers the commit, so the last stable offset trails by a few
  milliseconds. `EndOffsetAsync` asked right after one's own commit can therefore be the
  offset before it — the in-memory changelog never does this. Asked after
  `AcquireWriterAsync` it is exact, because `InitTransactions` waits for the previous
  writer's markers, and that is the case a takeover depends on. Nothing in the core relies
  on the other case; tests that read back their own writes either take over first or poll.
- *The client reports the end of a partition only on an empty fetch,* which costs
  `fetch.wait.max.ms` (0.5 s), and never while a busy writer keeps fetches full. So a
  reader does not wait for it. The offset just below a last stable offset is always a
  marker (every write is transactional and one producer's transactions do not overlap), so
  having read the record two below the end is having read everything. A follower reports
  `NextOffset` at the end as soon as the fetch that brought the last record says the end
  is one offset further; the end-of-partition event still covers the case where the tail
  is an aborted transaction. This took a caught-up standby's takeover from 2.0 s to
  0.55 s, and a follower now stands at the end 1 to 3 ms after the commit returns.

Each `ReadAsync` and each table `FollowAsync` runs its own consumer on its own thread, with
a bounded channel of two batches to the caller. A storage therefore holds `P` consumers
while it follows. One consumer for all partitions is the obvious saving and is not built.
Consumers use `auto.offset.reset=error`, belong to no group and commit no offsets.

**Topics.** `ConnectAsync` creates `{p}.state` (`StatePartitions`, default 12,
`cleanup.policy=compact`, `delete.retention.ms` from `DeleteRetention`, default 24 h) and
`{p}.leader`; `OpenTableLogAsync` creates `{p}.table.{name}.changes` (one partition,
`cleanup.policy=delete`, `retention.ms=-1`, `retention.bytes=-1`) and
`{p}.table.{name}.rows` (`TableRowPartitions`, default 4, compacted). An existing topic is
checked: a state topic with another partition count, or any topic with the wrong
`cleanup.policy`, is refused with `KafkaLogException`. A rows topic keeps the partition
count it was created with, which the head record also carries; if the two ever differ the
table is refused. `ReplicationFactor` defaults to 3. `StateTopicConfig`,
`TableRowsTopicConfig` and `TableChangesTopicConfig` add or override topic settings at
creation (`min.insync.replicas`; small segments in the compaction tests). Topic and table
names are limited to letters, digits, `.`, `_` and `-`.

**Record format.** Every value starts with format byte `1`: JSON, UTF-8. Compression is the
producer's (`Compression`, default zstd), not the record's. State records are passed
through as the changelog provider built them. Table records:

```
changes  key: version, 8 bytes big-endian
         value: 1 {"epoch":"…","version":7,"chunk":0,"changes":[{"k":K,"o":OLD,"n":NEW},…],"last":true,"offsets":{"0":41}}
rows     key: K's serialized bytes          value: 1 {"v":7,"r":ROW}      (null value: deleted)
         key: NUL "$head"                   value: 1 {"epoch":"…","version":7,"partitions":4,"offsets":{"0":41}}
```

`o` and `n` are omitted when null. A row is serialized once and written into both records.
The serializer seam is `KafkaLogOptions.Serializer`, an `IStateSerializer`: the default
JSON serializer's output is written and read directly as UTF-8; any other goes through its
strings, embedded as JSON strings. Departures from *Kafka-backed table log*:

- **Chunks say "last", not "i of n".** A version is cut into another record whenever it
  reaches `MaxRecordBytes` (default 900,000), so the count is not known when the first
  chunk is written. Offsets ride on the last chunk as specified.
- **Table keys must serialize equal values alike.** The rows topic finds a key by its
  bytes. A key type for which that can fail (`decimal`, `double`, `DateTimeOffset`, a
  hand-written `Equals`; the changelog provider's `ByteStableEquality` test) is refused at
  `OpenTableLogAsync` rather than handled by remembering instances as the provider does.
- **The epoch is written by the writer, the first time it opens the table,** as a version-0
  head in every rows partition. A reader that opens a table nobody has created waits until
  the active has. So the active must open a new table before the standby can.
- **The head is found by reading the tail of rows partition 0,** not by reading the
  partition: every version ends with its head, so one short read gives the epoch and the
  last version. `LastVersionAsync` is that read (on a writer that has appended or looked
  since it took over, it is the number in memory).
- **Restore is consistent across partitions.** Reading each rows partition "to its end"
  could mix versions while the writer is busy. `RestoreAsync` first reads every partition
  to the end it had when the restore began, then pauses a partition whenever it stands
  right after a head at the highest version seen anywhere and resumes it if another
  partition shows a higher one; it is done when all stand after the head of the same
  version. A test restores twelve times under a writer that rewrites every key each
  version and checks each snapshot is one version. Rows are collected in a dictionary
  first, because `RestoreAsync` can only hand rows over, not take one back.
- **The offset index is filled as records pass, not at restore.** The design has restore
  read the changes topic to fill the in-memory tail and the index. Neither happens:
  `SharedTable.OpenAsync` starts with an empty tail (a lagging circuit reads the log), and
  the index gains an entry when the writer appends a version (from the delivery report) or
  a reader passes one. A range read seeks to the nearest entry at or below the version it
  wants, or to the start of the topic when there is none, and skips forward. The index is
  bounded (8,192 entries, thinned by half when full).
- **Retention is `TrimBeforeAsync` and nothing else.** It finds the first offset of the
  version and calls `DeleteRecords`. Computing the floor from the pipelines' commit
  records, and the cap, are the host's and are not built.
  `OldestRetainedVersion` is what this process last saw: at open, after its own trim, on a
  reader's `LastVersionAsync`, and when a read runs into missing history.
- **No `Old` in memory.** A followed version's `Old` and `New` rows are fresh instances
  deserialized from the record, so rows must have value equality (records do), as
  retraction already requires.

**Core changes.** One: `InternalsVisibleTo` for `Incremental.Kafka` and
`Incremental.Benchmarks` in `Incremental.csproj`, for `JsonStateSerializer.Options`,
`ByteStableEquality` and (the benchmark) the dense codec. No seam changed.

**Measured** (one machine, one local broker, Debug build unless said; indicative only):

- Takeover by a caught-up standby, in process: about 0.55 s — 0.52 s to fence and commit
  the term, 20 ms to promote two tables, 1 to 2 ms to promote the storage, under 1 ms to
  start the circuit.
- A 200,000-row table of wide reference rows (19 fields, about 410 bytes of JSON), four
  rows partitions: loaded in 2.0 s as 40 versions of 5,000 rows; restored by a new process
  in 2.5 s (80,000 rows/s).
- Compaction: a rows topic of 3,072 records over 50 keys came down to 53, and a state
  partition at end offset 3,428 to 775 records, within 15 s of small segments rolling;
  restores before and after agree, and a circuit cold-started from the compacted state
  topic carries on to the same sinks.

**The serialization benchmark Decided (1) asked for.** `LogSerializationBenchmarks`
(`dotnet run -c Release --project benchmarks/Incremental.Benchmarks -- --filter
'*LogSerialization*'` for cost, `-- --log-sizes` for bytes), on two shapes: a trade (12
fields: ids, a quantity, a `decimal` price, an instant, a few short strings) and a wide
reference row (an instrument, 19 fields, mostly short strings). JSON is the format as
built; dense is the cluster's `ShardCodec` holding the same row in the same record.
One run, i7-13700H, .NET 10, Release; BenchmarkDotNet short job for the times.

| Shape | Encoding | Raw B/row | zstd B/row | lz4 B/row | Encode ns/row | Decode ns/row | Decode alloc B/row |
|---|---|---:|---:|---:|---:|---:|---:|
| trade | JSON | 279 | 19 | 59 | 313 | 840 | 552 |
| trade | dense | 99 | 26 | 41 | 105 | 160 | 312 |
| instrument | JSON | 414 | 29 | 72 | 446 | 1,220 | 946 |
| instrument | dense | 208 | 31 | 51 | 195 | 282 | 671 |

Bytes are a row record's key and value; compressed figures are 1,000 records compressed
as one batch (zstd level 3, lz4 fast — the client's defaults), without Kafka's per-record
framing. Encoding allocates nothing in either format. Compressing cost 1.1 to 1.5 µs a
row for JSON under zstd and 0.3 to 0.6 µs under lz4, with managed ports of the
compressors, so those are a guide only. The rows are synthetic and regular — sequential
ids, a handful of repeated strings — which flatters every compressed number; real rows
will compress less well in both formats.

What the numbers say:

- **Bytes.** Uncompressed, JSON is two to three times the dense size. Under zstd the
  difference disappears (JSON is slightly *smaller*: field names are the most
  compressible thing in the batch). Under lz4 JSON is about 40% larger. With zstd, the
  dense codec buys nothing on disk or on the wire.
- **CPU.** JSON costs about 0.2 to 0.25 µs more per row to encode and 0.7 to 0.9 µs more
  to decode. Encoding is on the commit path, where a Kafka round trip of milliseconds
  dwarfs it. Decoding is the restore path: 200,000 wide rows are about 0.25 s of JSON
  decoding against 0.06 s dense, inside a restore that took 2.5 s.

On these numbers the dense codec is not worth making a persisted format: it would save a
fraction of a second per few hundred thousand rows restored and no storage, at the price
of a second format whose stability becomes a compatibility promise. **Kept open for
James**, as Decided (1) reserves the decision; the default stays JSON with zstd, and the
format byte leaves the door open per topic. The case that would change the answer is a
table of tens of millions of rows whose cold restore must be fast.

**Not built, or left open.**

- Heartbeats, election, the leader term on deliveries, the delivery lease (step 5). The
  writer commits nothing when idle.
- Metrics: none yet (transactions, commits per transaction, commit latency, follower lag).
  `KafkaLog` exposes counters (`TransactionsCommitted`, `TransactionsAborted`,
  `CommitsCompleted`, `RecordsWritten`) but publishes nothing.
- The producer pool and the single multiplexed consumer.
- MSK: `ClientConfig` passes any librdkafka setting through and `ConfigureProducer` /
  `ConfigureConsumer` / `ConfigureAdmin` expose the builders for an OAUTHBEARER refresh
  handler, but nothing has been run against MSK, IAM or TLS.
- A commit larger than the client's queue (`queue.buffering.max.messages`, 100,000 by
  default) is handled by waiting for the queue to drain; one larger than the broker's
  transaction can sensibly hold has no guard beyond `transaction.timeout.ms`.

## Warm standby

The standby builds the same objects as the active and feeds them from Kafka.

- **Tables.** It restores each table as above, then tails the changes topic and applies
  each version to its own `SharedTable` view, acting as that view's single writer. No
  steps run, so nothing is pinned and chains trim immediately.
- **Pipelines.** It builds every circuit (operators construct and create their stores
  through the provider) but does not call `Start()`. A follower thread applies `{p}.state`
  records to those stores. No worker thread exists yet, so each store still has a single
  owner; `Start()` is the handover. Records for a store that does not exist yet are held
  as bytes until the pipeline is built.
- **Leader topic.** It tails heartbeats.

To take over in seconds the standby must hold: every table view at the last committed
version, with the tail and offset index; every pipeline's stores at its last commit, and
that commit record; every circuit built; the source adapters constructed but not
connected; a transactional producer created but not initialised. It uses as much memory
as the active.

**Pipeline definitions are the service's problem.** The standby can only build circuits
it knows about, so the service must replicate its pipeline registry, and a new pipeline's
definition must be durable there before its first commit. This design requires that and
does not provide it.

### As built (step 2)

Code: `Storage/StateChangelog.cs` (the seam), `Storage/InMemoryStateChangelog.cs`,
`Storage/ChangelogStorage.cs` (one per process: roles, the followers, promotion),
`Storage/ChangelogStorageProvider.cs` (one per pipeline: the stores and the record
encoding), `Tables/SharedTable.cs` (`FollowAsync`, `PromoteAsync`), `Tables/TableLog.cs`
(`LastVersionAsync`; `InMemoryTableLog.Connect` and `TakeOver`). Tests:
`ChangelogStorageTests.cs`, `ChangelogFailoverTests.cs`, `TableFollowerTests.cs`. Perf
contract: "changelog provider: commit cost vs state size".

**The seam.** `IStateChangelog` is the compacted topic and its one transactional producer,
with nothing of Kafka in it:

```csharp
public interface IStateChangelog
{
    int PartitionCount { get; }
    ValueTask<IStateChangelogWriter> AcquireWriterAsync(CancellationToken ct = default); // fences; new term
    ValueTask<long> EndOffsetAsync(int partition, CancellationToken ct = default);
    IAsyncEnumerable<ChangelogBatch> ReadAsync(int partition, long fromOffset, CancellationToken ct = default);
}
public interface IStateChangelogWriter : IAsyncDisposable
{
    long Term { get; }
    IStateChangelogTransaction BeginTransaction();
}
public interface IStateChangelogTransaction : IDisposable   // dispose uncommitted = abort
{
    void Append(int partition, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value);
    void AppendTombstone(int partition, ReadOnlySpan<byte> key);
    ValueTask CommitAsync(CancellationToken ct = default);
}
```

- `AcquireWriterAsync` is `InitTransactions`: when it returns, every earlier writer's
  transactions are decided and none can commit again. The writer it returns carries the
  leader term.
- A transaction is a unit of atomicity, not necessarily one Kafka transaction. Many
  circuits commit through one writer at once, each with its own transaction object; the
  Kafka log writer of step 3 is free to carry several in one physical transaction (the
  group commit of *The log writer*), as long as each lands whole or not at all.
- `CommitAsync` fails in exactly two ways, and the implementation must make sure it is one
  of them. `ChangelogFencedException`: the writer is finished (fenced, or failed fatally);
  the process is no longer the active. Anything else: the transaction was aborted, nothing
  of it is readable, and the writer carries on. An implementation that cannot tell whether
  a commit landed has to resolve that itself before reporting. This is the "abortable:
  abort and retry; fatal or fenced: fail and demote" split of *The log writer*, stated as a
  contract.
- `ReadAsync` returns committed records in batches and does not end at the end of the log;
  that is follow mode. Each batch says where the reader now stands (`NextOffset`), and
  `EndOffsetAsync` says where committed data ends. "Read to the end" is "read until
  `NextOffset` reaches the end offset". Offsets are not assumed dense: the in-memory
  changelog leaves an offset with no record behind every transaction, as a commit marker
  does. **For step 3:** the end offset must be the end of *committed* data (the last stable
  offset), and the reader must report a position past trailing markers, or a read to the
  end never finishes.

**Two objects, not one.** The text describes `ChangelogStorageProvider` as the whole thing.
As built it is two. `ChangelogStorage` is per process and per changelog: it holds the
writer, runs one reader per partition, and knows whether the process leads or follows.
`storage.Pipeline(id)` returns the `ChangelogStorageProvider` for one pipeline, which is
what goes in `CircuitOptions.Storage`. The split is forced by the shared topic: a
partition holds many pipelines' records, so one reader per partition must serve them all,
and fencing is a fact about the process, not about a pipeline. A pipeline's partition is
`StableHash(pipeline id) mod P`.

**State is one live layer plus a record of what was touched** — not the staged overlay the
text describes ("reads and staged writes stay in heap dictionaries, and `Commit` appends
the staged layer before promoting it"). The stores are the default in-memory stores, and
writes go straight into them. Beside each, a map remembers, for every entry touched since
the last commit, the value it had at that commit. A commit serializes the touched entries
(their current values; an entry back at its committed value is skipped) and clears the
map. `Recover()` puts every touched entry back. The map keeps its capacity from one commit
to the next, and gives back what one large commit left only after 64 commits in a row at
under a sixteenth of it — `ZSet.Clear`'s rule. (As first built it was replaced after any
commit of more than 1024 entries, so a circuit whose ordinary commit was larger regrew
every store's map each commit; a test now holds the allocation per entry of a 4096-entry
commit to that of a 1000-entry one.) The reasons: reads take the same path as
the default provider, with no second dictionary and no merged group built per read, which
the staged provider allocates per touched key; and the commit is proportional to the
delta by construction. The cost is one extra dictionary probe per write. Against the
default provider, on the contract's cycle (256-row deltas through a join, a count and a
distinct, committing every step, log discarded), the provider added about 20% time and
about 19 bytes allocated per record written (one run, one machine).

**Records.** As the layout above, with these particulars:

- Value = format byte `1` + the serializer's output. A weight is its decimal text. Removal
  is a tombstone; `RemoveGroup` writes one per member.
- The commit record's value is JSON: `{"sequence":…,"term":…,"offsets":{…},"manifests":{…}}`.
  `offsets` is the circuit's offsets map as given, so it carries `$step`, `{table}#t` and
  `{table}#w` with the input offsets. `sequence` counts the incarnation's commits and
  continues across restarts and failovers. The record repeats every manifest each time,
  because compaction keeps only the newest.
- The commit record is the last record of its transaction.
- Serialization goes through `IStateSerializer` as decided. For the default JSON
  serializer the provider writes the same JSON straight to UTF-8 rather than through a
  string; a test compares the bytes of the two paths. Any other serializer goes through
  its strings. A serializer's output must never contain a NUL (JSON's cannot), and
  deserializing a key and serializing it again must give the same bytes.
- **One record per entry, whichever equal instance touches it.** This replaces the
  requirement first written here, that a serializer give "the same bytes for equal
  values": the default JSON serializer does not meet it. `10.0m` equals `10.00m`, one
  instant under two offsets is one `DateTimeOffset`, `0.0` equals `-0.0`, and a type may
  define any equality it likes — and a store finds an entry by `Equals` while the log,
  its compaction and a restore find a record by its bytes. As first built a retraction
  wrote its tombstone under the retracting instance's bytes, the original record stayed,
  and the row came back after a restart or a promotion. Now a store remembers, for each
  live entry, the instance its record was written under (`HeldInstances`), and writes
  the entry's record and tombstone under that instance only; a restore or a follower
  holds the instance it read. An entry removed and added again within one commit keeps
  its record's key. Nothing is held for a type whose equal values always serialize
  alike (`ByteStableEquality`): integers, `bool`, `char`, `string`, `Guid`, enums,
  `TimeSpan`, `DateOnly`, `TimeOnly`, and tuples, records and plain structs built only
  from those with the compiler's or the runtime's equality. The analysis errs towards
  holding. For the other types the cost is one more copy of each live key (for an
  indexed store, of each key/value pair) and one more probe per entry per commit. The
  engine's window types count as serializing alike, on the grounds that the engine
  builds every window bound at offset zero; a user who builds a `WindowKey` by hand at
  another offset and uses it as state is outside that. The Redis provider has the same
  mismatch in a different form (the serialized text is the hash field) and is unchanged.

**Restore and follow are one mechanism.** A storage always starts by reading every
partition from offset 0. Records for a store that no circuit has created yet are held as
bytes, one entry per key, and applied when the store is created — which is how a restore
can run before any circuit is built, and how the standby tolerates building its circuits
late. `ChangelogStorage.Follow(log)` is that and nothing more: the standby.
`ChangelogStorage.OpenAsync(log)` is `Follow` then `PromoteAsync`: a cold start of the
active. Consequence: a storage reads the whole changelog, so it holds the records of
every pipeline in it, including ones this process never builds (as bytes, until swept).

**Promotion** (`storage.PromoteAsync()`), in order: acquire the writer, which fences the
old active; ask every partition for its end offset; wait until the followers have reached
them; stop the followers. From then on the stores belong to the circuits. `Recover()` and
`CommitAsync` throw on a follower, so `Start()` on a standby's circuit fails until the
storage is promoted; that is the guard on "`Start()` is the handover". After promotion
`Start()` is the ordinary start: `Recover()` returns the followed commit record's offsets,
the manifest is validated, inputs are seeded, tables attach at their committed versions.
Nothing is replayed that was committed.

**Fencing.** A commit that throws `ChangelogFencedException` faults its circuit, as any
failed commit does. It also marks the storage (`IsFenced`, `WhenFenced`): every later
commit or start on it fails without going to the log. `WhenFenced` is the hook for "a
fenced active exits"; nothing here exits the process. Every commit record carries the
term it was written under.

**Incarnations.** `provider.StartNewIncarnationAsync()` is the "fresh state store". It
differs from the text in three ways. The sweep is not a background job: one transaction
tombstones every record of the current incarnation (the provider knows them all — they
are its state) and writes an empty commit record under the next. There is no separate
incarnation marker: a reader that meets a higher incarnation discards what it holds for
the pipeline and moves to it, and ignores records of lower ones. And nothing calls it
automatically: a circuit that refuses to start says so, and the service decides. Deleting
a pipeline (the same sweep with no successor) is not built.

**Tables on the standby.** `SharedTable.FollowAsync(name, key, log)` restores the table
and then applies every version from `ITableLog.FollowAsync` to its own view, as that
view's writer; `table.Writer` refuses calls meanwhile, and a `ToTable` circuit cannot
write a table that is following. `table.PromoteAsync()` reads to the log's end
(`ITableLog.LastVersionAsync`, added to the seam for this), stops following and opens the
writer, which continues from the last version and the source offsets recorded with it.
The table does not fence anyone: that is the log's business, done before the table is
promoted. `InMemoryTableLog` models it with connections — `log.Connect()` is the standby's
handle, `TakeOver()` makes a handle the writer, and an append through any other throws
`TableLogFencedException`.

**The takeover as tested**, mapping to the table in *Takeover sequence*: fence the table
log (`TakeOver`); `table.PromoteAsync()` for each table; `storage.PromoteAsync()`;
`circuit.Start()` for each circuit; the sources re-send from the committed offsets. Tables
before circuits. A circuit that writes a table (`ToTable`) fails over the same way: its
table follows, is promoted, and the circuit's first step undoes the versions the dead
leader wrote past its last checkpoint.

**What the failover test proves, and under what conditions.** A leader runs a random
workload with a standby attached — a stream join, a table join, a remembered lookup, a
sum, a max, a distinct, a keyed input, four keyed upsert sinks — and is killed at random
points in one of four ways: between steps; with a batch delivered and not acknowledged;
with everything acknowledged and the commit failed; and with its commit in flight when
the standby takes over (the commit must fail as fenced and the old circuit must fault).
The standby is promoted, the workload continues, and at the end the sinks hold exactly
what an uninterrupted run produces, and a reader of the log alone holds exactly the last
leader's state, compacted or not. "Exactly" depends on two things the test harness does,
both consequences of gaps already recorded above, not new ones:

- it re-sends the uncommitted input cut into the steps it was first run as (*A gap in
  "keyed upserts converge"*);
- it commits table changes only when the circuit has nothing uncommitted, so a replayed
  step reads the table version it read the first time. Otherwise a `Lookup` enriches a
  replayed fact from a newer row, as *After a crash* says, and a table join can lose a
  transient key the same way a merged step can.

A service that does neither gets at-least-once with those gaps.

**Other departures and limits.**

- The existing checkpoint and restart suites were not parameterised over providers
  (*Tests* asks that they pass "unchanged"). Instead a property test drives the changelog
  provider and `InMemoryCheckpointStorageProvider` through the same random store
  operations, commits, recoveries, compactions and incarnation changes and compares every
  read, with one follower that has its stores from the start and one that has none; and
  the circuit-level restart and failover tests run on the changelog provider.
- A clustered circuit refuses the provider at `Build()` (*Out of scope*).
- A follower that fails — a record it cannot parse, a record that does not deserialize
  into the store the circuit declared — fails the whole storage: `PromoteAsync` throws and
  the storage has to be reopened. One bad pipeline therefore blocks a takeover.
- There is no lag metric yet. `WhenCaughtUpAsync()` waits for the follower to reach the
  log's current end.
- A commit failure that is not a fence leaves the provider usable: the circuit faults, and
  a rebuilt circuit's `Recover()` undoes the uncommitted writes in memory. Nothing retries
  a commit; retrying abortable errors belongs inside the log writer.
- `Commit` (synchronous) blocks on `CommitAsync`. The circuit never calls it.
- **A circuit refused at `Start()` is finished.** It runs no step, from `StepAsync` (which
  throws) or from the drain in `DisposeAsync`, and so commits nothing and writes no state.
  As first built the refusal left the circuit marked started: disposing it with records in
  its input ran them through operators that had never been matched to the checkpoint and
  committed their offsets (one worker), or waited for worker threads that were never
  created (several). On a follower the same stray step wrote into the followed stores, and
  the rollback at the next `Recover()` then put those entries back to stale values under
  the promoted leader. The stores themselves still accept writes in any role: nothing but
  the circuit's own state machine stops a write to a follower's store.
- `InMemoryStateChangelog.Compact(dropTombstones: true)` drops tombstones at once. A
  follower part-way through a partition must not straddle that, which is the
  `delete.retention.ms` rule under *Restore*; the tests keep tombstones while a follower
  may be behind.

## Leadership and fencing

**What Kafka fences.** `InitTransactions` on a `transactional.id` bumps the producer
epoch. If the previous holder has a transaction open it is aborted; if that transaction
had begun committing, the call waits for it to finish. Afterwards any transactional
request from the old epoch fails (how the .NET client reports that is under *Not
verified*). So once the new
active's `InitTransactions` returns, the outcome of the old active's last transaction is
decided and it can never commit another.

**What Kafka does not fence.** Deliveries to sinks. Reads from sources. Admin calls
(`DeleteRecords`, topic changes). Anything produced outside the transactional producer,
which is why nothing may be. And it does not choose a leader: it only makes the last
caller of `InitTransactions` the sole writer.

**Election.** The active's every transaction includes a heartbeat in `{p}.leader`, and it
commits an otherwise empty one each second when idle. A visible heartbeat therefore means
"the active can still commit". The standby takes over when either:

- no heartbeat for `TakeoverAfter` (default 3 s) **and** a direct health probe of the
  active fails; or
- no heartbeat for `TakeoverCeiling` (default 30 s), whatever the probe says.

The probe is a plain TCP or HTTP check between the two nodes. It is a hint: it stops a
broker leader election or an MSK rolling patch, which stall the active's commits for a
few seconds, from causing a needless failover. Safety never depends on it. All timing is
elapsed local time; no clocks are compared across machines.

Each takeover increments a **leader term**, written in the new active's first
transaction and stamped on every commit record and sink delivery.

**The sink gap.** A deposed active that was paused (GC, a frozen VM) can wake and
deliver an old upsert after the new active delivered a newer one, leaving a stale row
until that key next changes. Two mitigations, neither complete:

- The active stops delivering to sinks when its last successful transaction is older
  than `TakeoverAfter` minus a margin. This is a lease held in Kafka; it fails only if
  the process pauses between the check and the write.
- Deliveries carry `(term, pipeline, commit sequence)`, so a sink that can do a
  conditional upsert can reject an older term. A Kafka sink written through the
  transactional producer is fenced outright.

For a sink that can do neither, the residual risk is stated, not solved.

> **Decided (4): leadership lives in Kafka only.** Heartbeat topic, peer probe as a hint,
> `transactional.id` fencing for safety; the thing that elects is the thing that fences.
> The standby also gets a manual "take over now" command for planned switchovers. An
> external lease (DynamoDB, Kubernetes `Lease`) was rejected as a second system whose
> outage blocks failover while adding no safety the fencing does not already give.

**A fenced active exits.** It faults every circuit and stops the process; the supervisor
restarts it as the standby. Demoting in place would save a cold restore but needs every
staged layer and view unwound correctly, and is not worth it in the first version.

## Takeover sequence

| # | Step | Waits for |
|---|------|-----------|
| 1 | Decide the active is gone | heartbeat silence plus failed probe |
| 2 | `InitTransactions` | old transaction aborted or completed by the coordinator |
| 3 | Read every followed partition to the end of committed data | the standby's lag at that moment |
| 4 | Stop the followers; write the new term and first heartbeat | one transaction |
| 5 | Attach table writers: version and source offsets from `$head` | nothing |
| 6 | `Start()` every circuit: `Recover()` returns the followed `$commit`, manifest is validated, inputs are seeded | milliseconds per circuit |
| 7 | Connect sources at their committed positions; start sinks | source reconnects |
| 8 | Steps resume; outputs since each pipeline's last commit are redelivered | — |

Tables before circuits, as shared-tables.md requires. Step 3 cannot observe a pipeline
whose committed `{table}#t` exceeds the table's last version, because a version is
committed before any circuit can pin it.

**What dominates.** Detection: about 3 s when the process dies and the probe fails fast,
up to probe timeout plus `TakeoverAfter` when the host vanishes. Steps 2 to 6 should total
well under two seconds with a caught-up standby, but that is an estimate; nothing has been
measured against MSK. Step 7 is under a second or two for Kafka sources. The exception is
a source that must re-snapshot (below): pipelines run immediately against the last
committed table version, and the table catches up when the snapshot lands, which can take
minutes.

## Commit after delivery

An output declared as an acknowledged sink (the keyed upsert output, and `ToTable`)
reports the highest step whose batch its consumer has confirmed. Plain `ToOutput`
changefeeds are unchanged: they stay at-most-once with bootstrap resync, and no commit
waits for them.

The change to `RunStepGatedAsync`, for a circuit with at least one acknowledged output:

```
drain → workers → (cluster step end) → freeze watermark        unchanged
await previous flush; stage this step's outputs; start flush   moved before the commit
if ++_stepsSinceCommit >= CheckpointInterval:
    await this step's flush                                    batches are in the channels
    await every acknowledged output's ack through _stepNumber  sinks have confirmed
    await CommitCheckpointAsync()                              state + offsets + table versions
```

A circuit with no acknowledged output keeps today's order exactly, commit before publish.
A step that staged nothing for an output counts as acknowledged. For a derived table the
acknowledgement is the table version's transaction committing.

**Why wait at the commit point.** The staged overlay has no step granularity: a commit
persists everything staged so far. So the checkpoint at step N can be taken only when N
itself is delivered, and the circuit cannot run step N+1 while it waits.

**`CheckpointInterval`.** At 1, every step waits for a sink round trip and a transaction,
and the overlap of flush and next step is lost. At K, steps overlap their flushes as
today and only every Kth waits. The crash replay window grows to K steps. A time bound
(`CheckpointMaxDelay`) would suit this better than a step count and should be added with
the change.

**Backpressure.** A slow sink delays the acknowledgement, the coordinator waits, nothing
drains, input channels fill and producers suspend. Bounded throughout. A sink that is
down stalls its pipeline without faulting it; a sink consumer that throws faults it, as
now. A stalled pipeline holds no table pin, but its committed version stops advancing, so
it holds the table's retention floor until the cap cuts it loose.

**After a crash.** Input since the committed offsets is replayed. Step boundaries may
differ, so batches differ, but they integrate to the same result and keyed upserts
converge. A sink may see a key step back to an older row and forward again, and a delete
may be delivered twice. A `Lookup` may enrich a replayed fact with a newer table row.

**Shutdown.** The final commit in `DisposeAsync` waits for the last acknowledgements,
inside the host's shutdown timeout. If the timeout wins, the commit is skipped and the
steps are redelivered on restart.

> **Decided (5): gate now, seal if measured.** The gate at the commit point (above) is
> built first, with a metric for time spent waiting on sink acknowledgements. The sealed
> overlay — seal the staged layer at step N, carry on into a fresh one, commit the sealed
> layer when N is acknowledged — makes throughput independent of sink latency but adds a
> third layer to every store's read path; it is built only if real sinks show the stall.

### As built (step 1)

Code: `Circuit.cs` (`GatesCommits`, `AwaitAcknowledgementsAsync`, `CommitCheckpointAsync`),
`Runtime/Ports.cs` (`AcknowledgedOutputPort`), `Io/Handles.cs`
(`AcknowledgedOutputHandle`, `OutputBatch`), `Storage/IStorageProvider.cs` (`CommitAsync`),
`Incremental.Hosting` (`AddAcknowledgedOutputConsumer`). Tests:
`CommitAfterDeliveryTests.cs`, `HostingAcknowledgedOutputTests.cs`.

The gate is as specified above. What was decided while building it, and where it departs
from the text:

- **The acknowledged output is its own declaration, not yet the keyed upsert output.**
  `stream.ToAcknowledgedOutput()` works on any stream and delivers `OutputBatch<T>`: the
  step's Z-set delta plus the step's sequence. It does not collapse a step to one upsert or
  delete per key. That shape, and `ToTable`'s acknowledgement by table-version commit, are
  shared-tables step 6 and should sit on `AcknowledgedOutputPort` / `IAcknowledgedOutputPort`
  (the only thing the gate asks of a port is `WaitAcknowledgedAsync`).
- **Acknowledgement.** Cumulative, by sequence: `batch.Acknowledge()` or
  `handle.Acknowledge(sequence)` confirms that batch and every earlier one.
  `handle.ConsumeAsync(sink)` acknowledges when the sink delegate returns. A port counts a
  batch as published when it is staged, so the gate waits for it whether or not its flush
  has landed.
- **Stable sequences.** The sequence is the circuit's step number. It is committed with
  every checkpoint under the offsets-marker key `$step` and resumed on `Start()`, so the
  steps re-run after a crash carry the sequences they were first delivered under; a sink
  recognises a redelivery as a sequence it has already applied. `Circuit.CommittedStep` now
  reports it on a single process too. As the text says, an auto-stepping circuit may cut
  the replay into different steps, so a repeated sequence does not promise repeated
  contents; with manual stepping and the same boundaries the batches are identical (the
  crash tests assert that). **Manifest format 5 → 6**: a checkpoint without `$step` would
  restart the sequence below what sinks have seen, so it is refused. `$step` is reserved as
  an input name.
- **Plain outputs in a gated circuit.** The text says plain `ToOutput` changefeeds are
  unchanged. No commit waits for them, and their API is untouched, but in a circuit that
  has an acknowledged output *every* output is published before the commit (the flush is
  one task), so after a crash a plain stream output's consumer can see the replayed steps'
  batches again. That is already true of any circuit with `CheckpointInterval > 1`. A
  circuit with no acknowledged output keeps commit-before-publish exactly.
- **A consumer that goes away.** "A sink that is down stalls its pipeline without faulting
  it" holds for a consumer that is still there. One that ends its enumeration before the
  output completes (cancels, breaks out, throws) can never acknowledge, so the port fails
  both the publish in flight and the gate, and the circuit faults without committing — the
  alternative was a circuit stalled for good with nothing to release it. `handle.Abandon()`
  says the same thing explicitly.
- **Shutdown.** As specified: the final commit in `DisposeAsync` waits for the last
  acknowledgements. `DisposeAsync` takes no timeout of its own; when the host's shutdown
  timeout fires, `CircuitHost` abandons the acknowledged outputs, which ends the wait, and
  the commit is skipped. Under the host, a sink that throws is not followed by a
  read-and-discard of the rest of its output (that would let the checkpoint pass undelivered
  batches); the circuit faults and the usual fault policy applies.
- **Manual stepping.** A `StepAsync` that reaches a commit point returns only after the
  acknowledgement, so the consumer has to run concurrently with the stepping.
- **`CheckpointMaxDelay`** is added (`CircuitOptions`, `HostedCircuitOptions`): a commit is
  also due at the end of the first step that finishes after the oldest uncommitted step has
  waited that long, and an idle auto-stepping circuit commits when the delay runs out (the
  pump wakes for it and runs the same gate). With manual stepping it is checked only as
  steps end. Default null.
- **`CommitAsync`** is `ValueTask CommitAsync(IReadOnlyDictionary<string, long> inputOffsets,
  CancellationToken cancellationToken = default)`, a default interface method that calls
  `Commit`. The circuit always commits through it and passes no token: a commit lands or
  faults. Redis implements it with `ExecuteAsync`; its monitor is held only while the
  transaction is built and while the overlays are discarded, which relies on the circuit
  running no step during a commit.
- **Metric.** `incremental.circuit.sink_ack_wait` (histogram, seconds, `circuit` tag): one
  measurement per commit of a gated circuit, the time from the step's flush landing to the
  last acknowledgement. This is the number Decided (5) asks for.
- **A failed flush faults an idle circuit.** An auto-stepping circuit's flush runs beside
  the next step and parks its failure for whoever awaits it next: the next step, or a
  commit that waits for delivery. With no checkpointing provider, or between commit
  points, nothing did until the next record arrived — and a flush can now fail for
  reasons that have nothing to do with shutdown (a table version that will not commit, an
  abandoned consumer), so a circuit could sit idle and healthy-looking with its table
  stopped. The pump now waits on the flush along with its inputs when it goes idle, and
  faults the circuit when the flush has failed.
- **Clustered circuits** refuse `ToAcknowledgedOutput` and `CheckpointMaxDelay` at build:
  their processes must commit on the same steps. Consistent with *Out of scope*.
- **Not built here:** stamping deliveries with the leader term, and the delivery lease of
  *The sink gap*. Both belong with step 5.

### As built (sinks and error policy)

Code: `Io/Sinks.cs` (`IUpsertSink`, `IAppendSink`, `SinkDelivery`, `UpsertDelivery`,
`AppendDelivery`, the in-memory sinks), `RecordErrors.cs` (`RecordErrorPolicy`,
`DeadLetter`, `RecordErrors`, `DeadLetterPort`), the guarded loops in the operators,
`Incremental.Hosting` (`AddUpsertSink`, `AddAppendSink`, `AddDeadLetterConsumer`). Tests:
`SinkDeliveryTests.cs`, `RecordErrorPolicyTests.cs`, `HostingSinkTests.cs`.

**Keyed upsert delivery.** The contract — at-least-once, keyed upserts — is delivered on
the consumer side of the acknowledged output, not as a new port:
`handle.DeliverToAsync(sink)` is `ConsumeAsync` with a delegate that collapses the step's
delta, awaits the sink, and returns, which acknowledges the step and lets the gate open.

- **The collapse.** Per key: one live row left is an upsert of it, none is a delete. A row
  that changed (old retracted, new asserted) is one upsert. More than one live row for a
  key — two rows, or one row with weight above 1 — faults the circuit; it is checked within
  the step's delta only, since the delivery keeps no state.
- **Append sinks.** `IAppendSink<T>` receives the inserted records of a step (a weight of
  `n` is `n` records) with the step sequence. A retraction faults the circuit.
- **Failure policy.** A write that throws is retried with a doubling backoff capped at
  `MaxBackoff` (defaults: 5 retries, 100 ms, 5 s). When the retries run out the delivery
  throws `SinkDeliveryException`, which abandons the output and faults the circuit without
  committing. `MaxRetries = null` retries for ever. This is where the text above — "a sink
  that is down stalls its pipeline without faulting it" — and the task of bounding retries
  meet: the default is bounded and faults, the stall is the opt-in. Cancellation is never
  retried.
- **Backpressure.** One batch with the sink, the rest in the output's bounded channel.
- **Not built: the port-side form.** The previous section expected the keyed upsert output
  to be a port implementing `IAcknowledgedOutputPort`. The delivery is on the handle
  instead, because the sink is a runtime object (resolved from a container, often) and the
  declaration is not; the gate is unchanged. `ToTable` is such a port, since its sink — the
  table — is known when the circuit is declared; it is built (shared-tables.md, *Derived
  tables*, *As built*).

**A gap in "keyed upserts converge".** *After a crash* above says that replayed batches
"integrate to the same result and keyed upserts converge". That holds when the replay cuts
the same steps, and for any key that still has a row at the end of the replay step that
covers it. It does not hold for a transient key under auto-stepping. Suppose step 5
delivers an upsert of key `k` and the process dies before the commit; the input that would
have retracted `k` was to be step 6. The restarted circuit may run both as one step, in
which `k`'s insertion and retraction cancel inside the operators, so the step's delta says
nothing about `k` — and the sink keeps the row it was given. Nothing downstream of the
checkpoint knows the row was ever delivered. The row is corrected only when `k` next
changes.

This is recorded, not solved. What would solve it, in rising cost: replaying with the
original step boundaries (they would have to be made durable before delivery, one more
write per step); or a sink that records the sequence on each row and, on restart, is told
the committed step so it can re-derive the rows written above it from a snapshot. Manual
stepping with the same boundaries is exact today, and the crash tests assert it. A derived
table (`ToTable`) does close the gap for itself, by the second route: its checkpoint
records the table version it covers, the table's history is the snapshot, and a restart
undoes the versions written past the checkpoint before it replays (shared-tables.md,
*Derived tables*).

The append form has the mirror-image problem — under auto-stepping a replayed record can
arrive under a different sequence, even one above any the sink has seen — and an exact
answer: on restart, before delivery resumes, the sink drops everything above
`Circuit.CommittedStep`. `InMemoryAppendSink.TruncateAfter` does that and a test relies on
it. A sink that cannot truncate carries the sequence on its records.

**Per-record error policy.** `CircuitOptions.ErrorPolicy`: `Fault` (default, unchanged
behaviour) or `DeadLetter`; `stream.OnError(policy)` overrides it for the operator the
stream carries. Under `DeadLetter` a record whose delegate throws, or that yields a null
key or result, is dropped from the step and reported — record, weight, operator, exception,
step — on `Circuit.DeadLetters`.

- **Consistency under retraction.** Stateless operators (`Select`, `Where`, `SelectMany`,
  `MapValues`, key selectors, `WithEventTime`, `Sum`/`Min`/`Max` selectors, join result
  selectors) drop a record's retraction because the delegate throws for it again; both are
  reported with their weights. This rests on the delegate being deterministic, as
  retraction through a `Select` always has. The task asked for a fault "where a delegate
  throws on the retraction but not the insert". A stateless operator cannot observe that —
  it does not know what it did for the insertion — so no such fault is raised; it is a
  documented requirement instead. The operators that have state use it rather than assume:
  `Aggregate` keeps integrating the group and treats a throwing aggregate as "no result
  for this key" (retracting the last one, by the stored last result), and a remembered
  `Lookup` stores a dropped fact as held with no rows.
- **Null keys.** `GroupBy` (stream, event-time and windowed), `LookupAppendOnly` and keyed
  inputs report a null key as a dead letter. A table join's key comes from its grouping,
  so the `GroupBy` check covers it; a table index selector returning null already means
  "under no value" and is untouched. Under `Fault` a null key faults as it did before,
  with whatever exception the first store to see it throws.
- **Keyed inputs.** The key selector runs on the producer's thread, so the failure is
  carried through the input channel as a keyless entry and reported at drain. Its offset
  is admitted like any record's, so the checkpoint covers it. Circuit-level policy only.
- **Dead-letter output.** One bounded channel (`DeadLetterCapacity`, default 1024) fed by a
  port that collects each worker's drops between steps, so it is backpressure like any
  output and must be read. It is published as a plain output is: after the commit, unless
  the circuit has an acknowledged output. It is not itself acknowledged.
- **Cost.** Under `Fault` an operator's record loop is the code it was; the guarded variant
  is a separate loop or method selected once per step (the joins branch on a local per
  pair). Under `DeadLetter` a record that does not throw allocates nothing; a test compares
  allocation with and without the guard. The perf contracts are unchanged.
- **Metric.** `incremental.circuit.dead_letters` (counter, `circuit` and `operator` tags).
- **`MapValues` returning null** is a dead letter like a null from `Select`. As first built
  the null check saw the keyed row the operator wraps the result in, which is never null,
  and the row went downstream with a null value.
- **`OnError` on the stream `LeftJoin` and `FullJoin`.** These are composed — an inner
  join, an anti-join per outer side with a projection, and a concatenation — and the
  stream they return is the concatenation, which runs no delegate. `OnError` on it now
  sets the policy of the join and of each projection (`NodeDefinition.ErrorPolicyTargets`);
  as first built it set the concatenation's and changed nothing.
- **Not covered, still a fault:** `TopK` ordering, `EvictWhen` and `WindowCloses`
  deadlines, session windows, custom routing hashes, a linear aggregate's `Combine`, the
  `Recurse`-specific join and distinct.

## Source resumption

- **Kafka.** The adapter assigns partitions itself and seeks to
  `circuit.CommittedOffsets(input)` (or the table's `$head` offsets) plus one. Consumer
  group offsets are not used for position. `auto.offset.reset` is an error: if the
  committed offset has aged out, a compacted reference topic is re-read from the start
  through `ReplaceAllAsync`, and a fact topic has lost data, which needs an operator.
- **gRPC snapshot + delta with stable sequence numbers.** The sequence is the offset.
  Resubscribe from committed plus one; if the server no longer holds that position,
  re-snapshot.
- **gRPC without stable sequence numbers.** Every reconnect, failover included, must
  re-snapshot: subscribe to deltas and buffer them, `ReplaceAllAsync` the snapshot, then
  apply the buffer. Unchanged rows create no versions, so downstream work is proportional
  to real differences. **Not solved:** with no way to order a buffered delta against the
  snapshot, a delta older than the snapshot can regress a row until its next change. The
  service contract needs at least a snapshot token comparable with deltas.
- **Append-only sources that cannot replay** (a fact stream over gRPC with no sequence).
  Records between the last commit and the crash are gone. **Not solved here**; put Kafka
  in front of such a source.
- **Files.** A directory is treated as a log: files ordered by name, offset packed from
  (file ordinal, record index). That requires names that sort in arrival order, files
  immutable once visible, and storage both nodes can read. A file that *is* a table is a
  snapshot: `ReplaceAllAsync` whenever its identity changes.
- **CDC through Debezium.** It is a Kafka source. Debezium is at-least-once, so the same
  change can arrive twice at new offsets; row upserts into a table or keyed input absorb
  that. A custom Postgres adapter uses the LSN as the offset and confirms the replication
  slot only up to committed positions. The slot allows one connection, which fences the
  old active as a side effect. Slot survival across a database failover is out of scope.

## Failure modes

| Failure | What happens |
|---------|--------------|
| Active dies mid-commit | The open transaction is aborted by the standby's `InitTransactions`, or completes if it had reached the coordinator's commit decision. Either way the standby reads a whole commit or none of it. |
| Active dies mid-delivery | The checkpoint covering those steps was never taken. The new active replays and redelivers; sinks apply the upserts again. |
| Network partition, both alive | The standby takes over only if it can reach Kafka, and doing so fences the old active's logs. The old active stops sink deliveries when its lease lapses and exits on its next commit. The sink gap above is the remaining exposure. |
| Kafka unavailable | The active cannot commit: table versions stop publishing, pipelines stall at their commit point, inputs back up. The standby cannot fence, so it does not take over. Both recover when Kafka does. Nothing is lost; nothing progresses. |
| Kafka slow (broker election, patching) | Commits stall for seconds. The probe keeps the standby from taking over until `TakeoverCeiling`. |
| Standby lagging | Takeover step 3 takes as long as the lag. Lag is a metric and an alert; a standby that cannot keep up is not a standby. |
| Standby dies | No effect on the active. No failover cover until it has cold-restored, which takes minutes for large tables. |
| Both restart cold | Whichever initialises the producer first becomes active after a full restore; the other is fenced on its first transaction if it also tried, exits, and returns as standby. Downtime is the cold restore. |
| A pipeline's table version was truncated | That pipeline refuses to start and needs a new incarnation. Others are unaffected. |

## Tests

Without Kafka, against the in-memory changelog and table log:

- `ChangelogStorageProvider` passes the existing checkpoint and restart suites unchanged,
  and a provider rebuilt from the log alone equals the one that wrote it.
- A follower fed the log record by record matches the writer's state at every commit,
  including after `RemoveGroup` and across incarnations.
- Commit after delivery: with a consumer that withholds acknowledgements, no checkpoint
  advances; killing the circuit at each point between flush, acknowledgement and commit
  and replaying yields a sink whose final contents match an uninterrupted run.
- Table log: coalesced reads over the durable path equal those from the in-memory tail;
  truncation makes a lagging circuit refuse to start.

Against the local stack (`docker compose --profile connectors up -d`; tests skip when
`localhost:9092` is unreachable, as the Redis tests do):

- **Fencing.** Two log writers on one `transactional.id`: after the second initialises,
  the first's next commit fails and a reader never sees its open transaction. *Built*
  (`KafkaLogTests`), for a checkpoint commit and a table version.
- **Restore after compaction**, with the cleaner forced by small segments and a low dirty
  ratio, equals restore before it. *Built* for the rows topic (`KafkaTableLogTests`) and,
  through a circuit, for the state topic (`KafkaEndToEndTests`); both skip if the
  broker's cleaner has not run within 90 s.
- **Built beyond this list:** group commit (transactions counted), an aborted commit
  isolated from its neighbours, a snapshot restore under a busy writer, range reads and
  trimming, a 200,000-row restore, and the in-process failover: a circuit with a stream
  join, a table join, a lookup, aggregates, a distinct, a keyed input, a derived table and
  four upsert sinks is dropped three times — replaced by its standby, by a cold start, by
  a standby again — and the sinks and the derived table equal an in-memory run.
- **Kill -9 failover**, in the style of `MultiProcessClusterTests`: a host executable run
  twice as real processes, active and standby, with a Kafka source, a shared table, a
  retroactive join and a keyed upsert sink written to a topic. The test kills the active
  mid-stream, waits for the standby to take over, feeds the rest, and asserts the sink's
  final table equals a single-process reference run. It also asserts takeover time
  against a generous bound and that the killed process's term never appears after the
  new one. A variant pauses the active with `SIGSTOP` and resumes it after takeover, to
  prove a zombie cannot commit.
- **CDC**, through the Debezium container: Postgres changes reach a shared table, the
  connector is restarted to force duplicates, and the table matches the database.

Not testable locally: MSK-specific quotas and broker patching behaviour. Those need a
soak run on a real cluster before the failover number is believed.

## Build order

1. **Done.** Commit after delivery in the engine, with acknowledged outputs and
   `CommitAsync`; `CheckpointMaxDelay`; the `sink_ack_wait` metric. See *As built (step 1)*
   under *Commit after delivery*. Step 2 should know: the circuit awaits
   `ICheckpointStorageProvider.CommitAsync` under the step gate, the commit record must
   carry the `$step` entry of the offsets map like any other entry, and the state manifest
   is at format 6.
2. **Done.** `IStateChangelog`, `ChangelogStorage` / `ChangelogStorageProvider` and the
   in-memory changelog; follower mode for stores and for tables; promotion. See *As built
   (step 2)* under *Warm standby*. Step 3 should know: the seam's two failure meanings for
   `CommitAsync`; that the end offset must be the last stable offset and the reader must
   report a position past trailing markers; that a pipeline's partition is
   `StableHash(pipeline id) mod P`; that `Append` copies its spans, so the writer owns the
   batching; and that a storage reads every partition from offset 0 whichever role it
   starts in.
3. **Done.** `Incremental.Kafka`: the log writer, the changelog over `{p}.state`, restore.
   See *As built: `Incremental.Kafka`*.
4. **Done.** Kafka `ITableLog`: both topics, restore, durable range reads, retention by
   `TrimBeforeAsync`. Same section. Step 5 should know: `{p}.leader` exists (one
   partition, compacted) and already holds the term under the key `term`, written by
   `KafkaLog.AcquireWriterAsync`; heartbeats belong in the writer loop (`WriterLoop` in
   `KafkaLog.cs`), one per transaction plus an idle timer, under another key; a fenced
   idle producer learns nothing until it writes, so the heartbeat is also what tells a
   zombie; `InitTransactions` costs about half a second locally; a transaction the broker
   times out is reported as a fence; the standby must not open a table before the active
   has created it (`OpenTableLogAsync` waits); and the takeover order that is tested is
   `log.AcquireWriterAsync()`, each `table.PromoteAsync()`, `storage.PromoteAsync()`, each
   `circuit.Start()`.
5. Leader topic heartbeats, the election, the takeover sequence; the kill -9 test.
6. Kafka source adapter; resumption contracts for gRPC, files and CDC adapters.
7. Active and standby roles in `Incremental.Hosting`.
8. A soak on MSK to replace the estimates above with measurements.

## Out of scope

- Exactly-once to sinks through Kafka transactions.
- Scaling across nodes, more than one standby, cross-region standby.
- Clustered (exchange) circuits: they cannot use shared tables or this provider.
- Changing the Redis provider or its format.
- Replicating the service's pipeline registry.
- State larger than memory. Both nodes hold everything on the heap.
- Schema migration of persisted rows: a changed type still needs a new incarnation or a
  rebuilt table, as with Redis today.
- In-place demotion of a fenced active.

## Not verified

- Whether **MSK Serverless** supports transactions. Its documented limits (120 compacted
  partitions, 8 MiB messages, topic-management rate limits) are confirmed; transaction
  support is not mentioned either way. Assume provisioned MSK until checked.
- Whether **MSK Express brokers** restrict compaction or transactions. Their fixed
  settings (`min.insync.replicas=2`, replication factor 3) suit this design; nothing
  found says compaction is unavailable, and nothing confirms it.
- The exact conditions under which `InitProducerId` returns `CONCURRENT_TRANSACTIONS`,
  and so the worst-case duration of takeover step 2. KIP-98 states the abort-or-await
  behaviour; the timing is unmeasured.
- ~~How `Confluent.Kafka` surfaces fencing.~~ Verified against the local broker:
  `Local_Fenced`, fatal from the transaction calls, not flagged fatal on a later
  `Produce`. See *Fencing, verified* under *As built: `Incremental.Kafka`*. Still not
  verified: that MSK's broker version reports it the same way, and what a producer sees
  when the broker itself aborts a timed-out transaction (the code treats any fatal error
  as a fence, which is safe either way).
- Why `InitTransactions` takes about half a second against an idle local broker, and
  whether it is shorter or longer on MSK. It is the largest part of a takeover measured
  so far.
- That the offset just below a last stable offset is always a transaction marker on MSK
  as it is on Kafka 3.9 (the readers finish a read to the end on that basis). If it were
  not, a read would fall back to the end-of-partition event and be slower, not wrong.
- Behaviour of the group commit under a slow or failing broker: abort-and-retry, the
  split into single-commit transactions and the retriable-commit loop are exercised only
  by an oversized record. A fault-injection run (a proxy that drops or delays) is needed.
- Which Kafka version MSK must run to include the KIP-890 broker-side checks against
  writes from a stale epoch. The design does not rely on them for a well-behaved client.
- KAFKA-13727 (cleaner removing abort markers too early) should be confirmed fixed in the
  chosen broker version, since both compacted topics carry transactional data.
- Every latency figure in this document.

## References

- KIP-98, Exactly Once Delivery and Transactional Messaging —
  <https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging>
- KIP-890, Transactions Server-Side Defense —
  <https://cwiki.apache.org/confluence/display/KAFKA/KIP-890%3A+Transactions+Server-Side+Defense>
- KAFKA-5196, log cleaner and transaction markers (cleaning stops below the last stable
  offset; aborted records are removed) — <https://issues.apache.org/jira/browse/KAFKA-5196>
- KAFKA-13727 — <https://issues.apache.org/jira/browse/KAFKA-13727>
- Amazon MSK quotas (Express partition limits, Serverless limits) —
  <https://docs.aws.amazon.com/msk/latest/developerguide/limits.html>
- Amazon MSK best practices for Standard brokers (partitions per broker) —
  <https://docs.aws.amazon.com/msk/latest/developerguide/bestpractices.html>
- Amazon MSK tiered storage constraints (no compacted topics) —
  <https://docs.aws.amazon.com/msk/latest/developerguide/msk-tiered-storage.html>
- Amazon MSK Express broker read-only configuration —
  <https://docs.aws.amazon.com/msk/latest/developerguide/msk-configuration-express-read-only.html>
- Confluent.Kafka `IProducer` (transactions) and `IAdminClient` (`DeleteRecordsAsync`) —
  <https://docs.confluent.io/platform/current/clients/confluent-kafka-dotnet/_site/api/Confluent.Kafka.IProducer-2.html>,
  <https://docs.confluent.io/platform/current/clients/confluent-kafka-dotnet/_site/api/Confluent.Kafka.IAdminClient.html>
