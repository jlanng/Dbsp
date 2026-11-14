# Design: durability and warm standby

> **Status:** agreed design, 2026-11-09. Build-order steps 1 to 6 are built: commit after
> delivery (with the sink side of keyed upsert delivery and the per-record error policy; see
> the two *As built* sections under *Commit after delivery*); the state changelog seam,
> the changelog provider, the in-memory changelog and follower mode for stores and tables
> (see *As built (step 2)* under *Warm standby*); in `Incremental.Kafka`, the log
> writer, the state changelog and the table log on Kafka (see *As built:
> `Incremental.Kafka`*); and the leader protocol — heartbeats, election, takeover, step
> down, the delivery lease — with a kill -9 test between real processes (see *As built
> (step 5)* under *Leadership and fencing*); and Kafka sources, sinks and Debezium change
> data capture (see *As built (step 6)* under *Source resumption*). The gRPC and file
> adapters and the hosting roles are not.
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
        deadline store        entry = key            → deadline, UTC ticks (see *Event-time state*)
        null = removed (RemoveGroup writes one tombstone per member)

key:    pipeline NUL incarnation NUL $commit         (written last)
value:  commit sequence, leader term, input offsets (including {table}#t, and the
        clock's $watermark entries), manifests
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
| `Produce` refuses one record for a reason of the record's own (over the client's size limit, an unknown partition) | aborts; that commit alone fails with `KafkaLogException`; the rest of the batch runs again |
| `Produce` fails for any other reason — above all `Local_State` | aborts and runs the same batch again, as for a failed commit: no commit is blamed |
| fatal, `Local_Fenced`, or an abort that fails | the writer is finished: every waiting and later commit fails with `KafkaLogFencedException`, `WhenFenced` completes |

The second `Produce` row was added after review. When the *broker* refuses a record
(over the topic's `max.message.bytes`, under the client's limit) the news comes with the
delivery report, while a later commit of the same transaction may still be producing;
from then on every `Produce` throws `Local_State` ("erroneous state"). As first built the
commit that happened to be producing was failed for it. Now it is not: the batch is
retried, then split into single-commit transactions, and the commit with the refused
record fails alone, with the broker's reason. Tested with a 500 KB record against a
60 KB topic limit beside a 400,000-record neighbour.

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
- a producer fenced with a transaction's records in flight (seen in review, when a
  standby took over under a long commit) gets the broker's own words on the error
  handler, fatal: "Producer attempted an operation with an old epoch … transactional
  producer fenced by newer producer instance". Fatal is what the writer checks;
- a producer fenced while idle does not find out until it writes again. Its next
  `BeginTransaction` and `Produce` succeed locally; the fatal error reaches the error
  handler when that transaction first talks to the coordinator, and the commit then
  fails. So `WhenFenced` completes on a zombie's next write, not before. Telling an idle
  zombie it has been deposed is the heartbeat topic's job (step 5).

`InitTransactions` takes 2 to 55 ms against the local broker. As first written this
section said half a second and called it the floor of a takeover. That was wrong: the
half second was the read of the last term that follows it (see *A logical start offset
costs half a second* below). Acquiring the writer — initialise, read the term, commit the
next — now takes 20 to 45 ms.

**Terms.** `AcquireWriterAsync` reads `{p}.leader` (one partition, compacted) for the last
term and commits the next under the key `term`, with the node's id, before it returns.
Reading after `InitTransactions` makes the last term final; committing through the
transactional producer makes the new one unique. Terms never fall along the topic and
every heartbeat carries one, so the read is of the topic's tail only. This is the "write
the new term" of takeover step 4, brought forward because `IStateChangelogWriter.Term`
needs it. Heartbeats, the election and the delivery lease are under *As built (step 5)*.

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

- *A logical start offset costs half a second.* A consumer assigned at
  `Offset.Beginning` must first ask the broker what that is, and a consumer created a
  moment ago does not know the partition's leader yet; librdkafka then waits 500 ms
  before asking again. A consumer assigned at a number starts fetching as soon as it has
  metadata. Found in step 5 by timing the three parts of `AcquireWriterAsync` on the code
  as it was (initialise 3 to 55 ms, read the term 503 to 507 ms, commit it 11 ms). Every
  read now starts from a number: the term from the tail of the leader topic, a table
  restore from each rows partition's start offset, a range read from the index or the
  changes topic's start offset. An in-process takeover went from 550 ms to 30 to 50 ms,
  and the Kafka test suite from about 60 s to about 35 s before the step 5 tests were
  added.

Each `ReadAsync` and each table `FollowAsync` runs its own consumer on its own thread, with
a bounded channel of two batches to the caller. A storage therefore holds `P` consumers
while it follows. One consumer for all partitions is the obvious saving and is not built.
Consumers use `auto.offset.reset=error`, belong to no group and commit no offsets.

*What a reader that is behind may hold* (added after review). On the managed side: the
two batches in the channel, the one being written to it and the one in the caller's
hand, 1,024 records each. On the native side: `KafkaLogOptions.FetchBufferKilobytes`
(`queued.max.messages.kbytes`), default 8 MB a consumer. As first built it was the
client's default of 64 MB, which for twelve state partitions is about 770 MB the GC
cannot see while a restore or a lagging follower is behind. `fetch.queue.backoff.ms` is
set to 10 ms with it, for the reason given under *As built (step 6)*. A test stalls a
follower over an 18 MB partition with a 1 MB buffer and reads both bounds.

*Short reads share one consumer* (added after review). Reading a table's head, the last
term, the oldest retained version and the offset a trim cuts at are brief reads of a
partition's tail. Each used to build a consumer of its own; a standby's lag sample
(every 5 s) therefore built two clients per tracked table, each with its threads, its
connections and, on MSK, its TLS and IAM handshakes. They now run one at a time on a
single consumer the log keeps (`KafkaLog.ShortReadAsync`), which is rebuilt after a read
that fails. The oldest retained version is read again only when the changes topic's
start offset has moved. A test measures lag repeatedly and counts clients built: none.

*Admin calls are retried through one helper* (`KafkaLog.RetryTransientAsync`), for up to
`OperationTimeout`, on the errors that pass. After review the list includes
`Local_NoEnt`: the client's own "Failed to query partition leaders: No leaders found",
which `ListOffsets` and `DeleteRecords` throw as a plain `KafkaException` when the
client's metadata has no leader for a partition yet. Reproduced on the local broker for
just-created topics under load (hundreds of 400 create-then-`ListOffsets` rounds with 6
partitions and 64 in parallel; none on a quiet broker). Unretried, it failed a start, and
during a takeover — a table opened in `PrepareAsync` — it ended the node `Fenced`. Also
added: `BrokerNotAvailable`, `ReplicaNotAvailable`, `KafkaStorageError`, the two leader
epoch errors, `NotController` and `Local_WaitCoord`. Not verified: that a partition in
the middle of a leader election on a multi-broker cluster reports one of these (one
broker here).

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
         key: NUL "$head"                   value: 1 {"epoch":"…","version":7,"partitions":4,"offsets":{"0":41},
                                                       "indexVersion":6,"indexOffset":10}
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
  the active has. So the active must open a new table before the standby can. Two things
  keep it to one epoch (added after review, when two concurrent opens of one new table on
  the writer were found to mint two, leaving a rows topic no standby could open): opens
  of one name in one process share a single open, and the writer reads the head again
  *as the writer* before it mints — a head read made a moment earlier as a reader could
  have missed a table the previous writer had just created.
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
  bounded (8,192 entries, thinned by half when full). So that a process which has only
  restored does not read the whole history to follow from the head, each head record
  also carries the writer's newest index entry — normally the version before and the
  offset it starts at (`indexVersion`, `indexOffset`) — and reading a head seeds the
  index with it. A standby's follower therefore starts one version back from the end.
  Because that offset may have been trimmed since, a read never starts below the topic's
  current start.
- **Retention is `TrimBeforeAsync` and nothing else.** It finds the first offset of the
  version and calls `DeleteRecords`. The offset is always found by looking — the index,
  or a read for the first record at or above the version. As first built a trim past the
  last version took the topic's end offset instead, and a version committed between
  reading "last" and reading "end" (appends are not serialised with trims, and the caller
  may not be the writer) was deleted while the log reported it retained. Tested by
  committing a version from inside the trim. Computing the floor from the pipelines' commit
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

- Takeover by a caught-up standby, in process, once the decision is made: 30 to 50 ms —
  about 20 ms to fence and commit the term, 10 to 25 ms to promote two tables, 1 to 2 ms
  to promote the storage, under 1 ms to start the circuit. (0.55 s as first measured;
  see *A logical start offset costs half a second*.) Now and then a table promotion
  takes 0.25 to 0.4 s instead: its follower stood one record short of the end and had to
  wait for the client's end-of-partition event.
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

- Metrics: none yet (transactions, commits per transaction, commit latency, follower lag).
  `KafkaLog` exposes counters (`TransactionsCommitted`, `TransactionsAborted`,
  `CommitsCompleted`, `RecordsWritten`) but publishes nothing.
- The producer pool and the single multiplexed consumer.
- MSK: `ClientConfig` passes any librdkafka setting through and `ConfigureProducer` /
  `ConfigureConsumer` / `ConfigureAdmin` expose the builders for an OAUTHBEARER refresh
  handler, but nothing has been run against MSK, IAM or TLS.
- A commit larger than the client's queue (`queue.buffering.max.messages`, 100,000 by
  default) is handled by waiting for the queue to drain; one larger than the broker's
  transaction can sensibly hold has no guard beyond `transaction.timeout.ms`. A commit is
  never split, so its duration is also bounded by leadership: see *A long commit is not
  a dead leader* under *As built (step 5)* — a single commit must finish within
  `TakeoverCeiling` (30 s) or the standby takes over under it.

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
  is a tombstone; `RemoveGroup` writes one per member. (Since 2026-11-11 there is a
  format `2`, which puts a layout tag in front of the serializer's output: *As built:
  layouts*, at the end of this document.)
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

### As built (step 5)

Code: `src/Incremental.Kafka` — `KafkaLeadership.cs` (the coordinator, its options and
status types), `LeadershipProbe.cs` (the TCP probe), and in `KafkaLog.cs` the heartbeat,
`ResignAsync` and the lease clock. Test app: `tests/Incremental.Kafka.FailoverApp`. Tests:
`KafkaLeadershipTests.cs` (one process, real broker) and `KafkaFailoverProcessTests.cs`
(real processes).

```csharp
await using var log = await KafkaLog.ConnectAsync(new KafkaLogOptions { …, NodeId = "node-a" });

var leadership = new KafkaLeadership(log, new KafkaLeadershipOptions
{
    ProbeLeader = LeadershipProbe.Tcp(peerHost, peerPort),      // optional hint
    StartLeadingAsync = (term, ct) => { circuit.Start(); /* sources, sinks */ return Task.CompletedTask; },
    StopLeadingAsync = async ct => await circuit.DisposeAsync(), // for StepDownAsync
});
await using var probe = LeadershipProbe.Listen(leadership, myEndpoint);

// Every node builds itself as a standby, whatever it will become.
var tableLog = await log.OpenTableLogAsync<int, Instrument>("instruments");
var instruments = await SharedTable<int, Instrument>.FollowAsync("instruments", i => i.Id, tableLog);
var storage = ChangelogStorage.Follow(log.State);
leadership.Track(instruments, tableLog);
leadership.Track(storage);
var circuit = Build(storage.Pipeline("enrichment"), instruments);   // built, not started

leadership.Changed += status => { if (status.Role is NodeRole.Fenced) Environment.Exit(3); };
await leadership.StartAsync();
```

**Heartbeats are the log writer's.** Every transaction the writer thread commits carries a
record under the key `heartbeat` in `{p}.leader`: `(term, node, at, sequence, resigned)`.
When nothing has been committed for `KafkaLogOptions.HeartbeatInterval` (default 1 s) the
thread commits a transaction holding only that record. So a readable heartbeat means what
the design says — "the active can still commit" — and a writer that was fenced while idle
finds out within one interval, because the heartbeat is a write. Idle heartbeats are
counted in `KafkaLog.IdleHeartbeats`, not in `TransactionsCommitted`. The term record
(key `term`) now carries the node id too. The topic is created with `segment.ms` of one
hour, so the cleaner keeps it to two records plus the active segment; the watch and the
term read start at its tail and never see the rest.

**A long commit is not a dead leader** (added after review). A transaction's heartbeat
is readable only when the transaction commits, and the writer thread cannot commit an
idle heartbeat while a transaction is in flight. As first built, any single commit that
took longer than `TakeoverAfter` less the age of the previous heartbeat therefore read
as silence — and the standby's last check did not help, because the end offset it asks
for is the last stable offset, which an open transaction pins. Reproduced with default
timings: a healthy leader committing 3,000,000 records was fenced 2.5 s in, and its
successor would have had to commit the same work. Two changes:

- *The writer keeps beating inside the transaction.* While it produces a transaction's
  records it adds another heartbeat record every half `HeartbeatInterval`, and it now
  flushes the transaction itself, in slices, beating between them, before it calls
  `CommitTransaction` — which is then one round trip. Nobody can read these records
  until the commit; they are counted in `KafkaLog.InTransactionHeartbeats`. None is
  written after a resignation in the same transaction, which must stay the last
  heartbeat.
- *The standby looks for them.* Once a heartbeat is late (1.25 intervals) it samples the
  leader topic's end with uncommitted records counted (`ListOffsets`, read-uncommitted),
  every quarter interval. An end that has moved since the previous sample is a sign of
  life: only the current writer can append there — a fenced producer's records are
  refused, a dead one writes nothing. It counts against the silence exactly as a probe
  that answers does: the takeover on silence waits, and the ceiling still applies. The
  first sample after a heartbeat is only a baseline, so a leader that died just after
  beginning a transaction buys no time.

So a single commit may now take up to `TakeoverCeiling` (30 s by default), with or
without a probe, and `transaction.timeout.ms` (60 s) stays the broker's limit. Tested in
one process: a commit slowed to 3 s under a 0.8 s timeout leaves the standby a standby
(removing either half fails it), and a leader stopped mid-transaction is still replaced
on time. Limits: the sampling is one small admin request every quarter interval while a
heartbeat is late, and none otherwise; a `CommitTransaction` round trip that itself
stalls is a Kafka stall, which is the probe's case as before; with `TakeoverAfter` at its
minimum of two intervals there is little slack between the baseline sample and the next
in-transaction heartbeat, so three or more is advised. Not run against a large real
commit on MSK.

**One coordinator per process: `KafkaLeadership`.** Roles: `Standby → Promoting → Leader`,
ending in `Fenced` or `Resigned`. Both ends are final: the process exits and its
supervisor starts it again as a standby, as *A fenced active exits* decided. The
coordinator does not exit the process and does not depend on `Incremental.Hosting`; it
raises `Changed`, completes `WhenFinished`, and reports `Status` (role, own term, leader
node and term, heartbeat age, whether the lease is held, follower lag, the last
takeover's timings, the fault). Step 7 wires those into the host.

**Every node starts as a standby.** There is no "start as the leader". A node follows the
logs and builds its circuits, and leads when the election says so; restore and follow
were already one mechanism (step 2). A deployment's first start is the same path: nobody
heartbeats, so after `TakeoverAfter` the node takes over. The one wrinkle is that a
table's topics get their identity from the first writer, so on that first start
`OpenTableLogAsync` cannot complete until the node is the writer. `PrepareAsync` exists
for that: the takeover acquires the writer, then waits in `PrepareAsync` for the build
the application started earlier, which can now finish. A node that should lead at once
is sent `TakeOverAsync`.

**The election**, as decided. A standby tails the leader topic and notes, in local
elapsed time, when it last read a record. It takes over when:

- the silence has reached `TakeoverAfter` (default 3 s) and there is no probe, or the
  probe fails; or
- the silence has reached `TakeoverCeiling` (default 30 s), whatever the probe says; or
- the last record is a resignation; or
- `TakeOverAsync` was called.

Additions to the text:

- *A last check against the broker.* Before acting on silence, the standby asks Kafka
  for the leader topic's end offset. If the request fails, the standby is the one cut off
  from Kafka; taking over would hang in `InitTransactions` and then depose a healthy
  leader the moment the connection returned, so it does not. If the topic has records
  the standby's watch has not delivered, the silence is the watch's. This is not covered
  by a test.
- *The probe answers with a role.* `LeadershipProbe` is one line over TCP — `role term
  node` — written by the process on each connection. A probe is true only when the peer
  answers as `Leader` or `Promoting`. A refused connection (the process is dead), a peer
  that answers as a standby (it was restarted), and no answer within `ProbeTimeout` (it
  is stopped: the kernel still accepts the connection, which is why a bare connect is not
  enough) are all false.
- *Signs of life short of a heartbeat.* See *A long commit is not a dead leader* above.
- *Startup counts as hearing the leader.* A standby's silence clock starts when its watch
  does, so a node started beside a dead leader waits a full `TakeoverAfter`.
- `AutomaticTakeover = false` leaves only the command.

**The takeover**, in the order step 4 tested: `log.AcquireWriterAsync()` (fence, read the
term, commit the next); `PrepareAsync`; `PromoteAsync` on each tracked table, in the order
tracked; `PromoteAsync` on each tracked storage; `StartLeadingAsync(term)`. Then the node
is the leader. If any of it fails — including losing the race for the transactional id —
the node ends `Fenced`, and the coordinator closes the log's writer so that it stops
heartbeating: a node that leads nothing must not look alive to its peer. The same holds
when the leadership is disposed in the middle of a takeover (added after review: it used
to leave the node `Promoting` for ever with its log heartbeating as the writer, so that
the peer waited on a leader that led nothing): if the log is already the writer it is
closed and the node ends `Fenced`. Disposing a *leader's* leadership still leaves the
log the writer, as before. The takeover's timings are kept in `Status.LastTakeover`.

**What a deposed leader learns, and when.** Three ways, fastest first:

1. Its own watch on the leader topic reads a term higher than its own. The coordinator
   closes the log's writer at once (`KafkaLogFencedException`, "node 'b' has taken the
   log over under term 2"). Measured: 30 to 110 ms after a manual takeover began.
2. Its next write fails as fenced. A busy leader's next commit; an idle one's next
   heartbeat, at most `HeartbeatInterval` later. Measured with nothing else running: 23
   to 31 ms after the successor's `AcquireWriterAsync` returned, at 100 ms heartbeats.
3. Its lease lapses (below), which needs no news at all.

Either of the first two makes every later commit fail, so every circuit faults at its
next commit point and none can commit; the role becomes `Fenced`. Circuits are not
faulted from outside — there is no such call in the core, and nothing needs one.

**The commands.** `leadership.TakeOverAsync()` is "take over now": the standby stops
waiting, and the leader, if alive, is fenced under it and replays nothing of its own —
the new leader replays what the old one had not committed. `leadership.StepDownAsync()`
is the planned form, on the leader: it calls `StopLeadingAsync` (dispose the circuits, so
their last commits land), commits a resignation — a heartbeat with `resigned` set — and
closes the writer; a standby that reads the resignation takes over without waiting, and
has nothing to replay. Calls to `StepDownAsync` made together, or after it has
succeeded, share the one step-down: `StopLeadingAsync` runs once, one resignation is
written, and every caller gets the same outcome (added after review; a second concurrent
call used to run `StopLeadingAsync` again and could turn a clean resignation into
`Fenced`). If `StopLeadingAsync` throws, nothing was resigned and the call can be made
again. Both are method calls. Carrying them between nodes (an admin
endpoint) is the host's, step 7.

**The delivery lease** (*The sink gap*, first mitigation) is built.
`leadership.HoldsLease` is true while the node is the writer and its last committed
transaction *began* less than `TakeoverAfter − LeaseMargin` ago (margin: a third of
`TakeoverAfter` unless set; so 2 s by default). The clock starts before the transaction
begins because a standby cannot have seen that heartbeat any earlier, and a standby does
not act until `TakeoverAfter` after it saw it; so the lease ends before the standby
decides, by at least the margin, with no clocks compared. `leadership.Leased(sink)` wraps
an `IUpsertSink` or `IAppendSink` so that every write first waits for the lease. A leader
whose commits stall stops delivering and resumes with its next committed transaction; a
deposed one gets `KafkaLogFencedException`, which the delivery reports like any failed
write. What remains is what the design said remains: a process stopped between the check
and the write. A manual takeover does not wait for the old leader's lease, so there the
window is from the command to the old leader reading the new term.

*The lease clock* (added after review). The lease is elapsed local time, and it only
works if time the holder spends frozen counts against it. `Environment.TickCount64` is
`CLOCK_MONOTONIC` on Linux, which stops while the machine is suspended; a leader that
slept through its lease would have woken still holding it. The lease is now measured on
`CLOCK_BOOTTIME` on Linux (`LeaseClock`), which keeps counting; on Windows `TickCount64`
already does. What no local clock can cover is a hypervisor that pauses a guest *and*
stops its clocks: the guest wakes believing no time has passed. There the lease is no
protection and the window is from the wake-up to the old leader's watch reading the
higher term (milliseconds) or its next heartbeat failing; the term on the delivery is
the defence. This corrects *The sink gap* above, which names "a frozen VM" among the
pauses the lease handles and says it fails only between the check and the write. Not
testable here: neither a suspend nor a paused guest.

**The term on deliveries** (second mitigation) is not added to the core's batches. A sink
that wants it reads `leadership.Term` when it writes; the test app's sink stamps term,
step sequence and node on every record that way, and the tests read the sink both
plainly and with the conditional rule ("refuse a row from an older term than the key
holds").

**Follower lag.** `leadership.MeasureLagAsync()` returns, for each partition of the state
topic, the offsets between the tracked storage's position and the end of committed data
(one `ListOffsets` request for all partitions), and for each tracked table the versions
between the standby's view and the log's head (one short read per table).
`Status.Lag` is that, sampled every `LagSampleInterval` (default 5 s) while the node is a
standby. A caught-up standby reads 0 everywhere, because a follower's position moves past
the transaction marker behind the last record. No metric is published yet.

**Core change.** One: `ChangelogStorage.FollowedOffsets()` and `PartitionCount`, public,
so that lag can be computed outside the core. No seam changed.

**The kill -9 test.** `tests/Incremental.Kafka.FailoverApp` is a console program that is
the wiring above and nothing else. Its pipeline: a keyed input of orders, joined to a
shared `products` table (on Kafka, written by the leader), summed per customer; and the
same records once more as facts, counted per bucket. Three keyed upsert sinks — the
enriched orders, the totals, the counts — are written to one Kafka topic *outside* the
transactional producer, so the sink is as unfenced as a database would be. The counts
are there because upserts forgive: a record lost in mid-run is papered over by the next
upsert of its key, and the final table would still match. A count is right only if every
record was processed exactly once. (Found by a mutation: a successor that skipped 80
records passed the kill -9 test until the counts were added.) The Kafka
source adapter does not exist yet (step 6), so the input is a generator: record `i` is a
pure function of `i`, and `i` is the offset the circuit checkpoints. A step is the next
20 records, a checkpoint every 4 steps, and the table changes only before a step that
begins a checkpoint interval, to a row that depends on the step alone. Those are the two
conditions of step 2's failover test — replay cuts the same steps, and a replayed step
reads the table it read the first time — met by construction rather than by a harness.
The test computes the expected sinks from the same functions without running a circuit
(`FailoverScenario.Expected`), and a run with no failure is checked against it first.

The tests launch the program as real processes and talk to them over stdin and stdout:

| Test | What is done to the leader | What is asserted |
|---|---|---|
| kill -9 | `SIGKILL` mid-stream, default timings, probe on | the standby leads under term 2 within the bound, resumes at a committed checkpoint at or past the last one the dead leader reported, finishes; the sink equals the model read either way; no term-1 record follows a term-2 record |
| paused leader | `SIGSTOP` until the standby leads and has run 40 steps, then `SIGCONT` | the standby took over (the probe got no answer); the woken leader exits as fenced having committed nothing more; restarted, it is the new leader's standby; a third leadership on it starts at the last offset with nothing to do; the sink equals the model under the term rule; anything the zombie wrote late belongs to one step |
| manual takeover, step down | `takeover` sent to the standby; later `stepdown` sent to the new leader | the old leader exits as fenced; the step down hands over in a fraction of `TakeoverAfter` (6 s here) and its successor resumes at exactly the next offset; the sink equals the model |
| simultaneous start | two processes started together, no probe | exactly one finishes the work and leads; the other was fenced and exited (or stayed a standby); restarted, it follows the winner; the sink equals the model |

**Measured failover** (one machine, one local broker, Debug build, three runs of the
suite). kill -9 with the default timings — `TakeoverAfter` 3 s, heartbeat 1 s, the probe
refused at once because the process is gone:

| Part | Two runs | The third |
|---|---|---|
| Detection: kill to the standby deciding | 3.03 s, 3.07 s | 3.02 s |
| Fence: `InitTransactions`, read the term, commit the next | 41 ms, 30 ms | 1.05 s |
| Promote one table | 17 ms, 16 ms | 0.51 s |
| Promote the storage (3 state partitions) | 5 ms | 2 ms |
| Start the circuit and its sinks | 25 ms, 32 ms | 24 ms |
| Decision to leading | 89 ms, 83 ms | 1.59 s |
| Leading to the first sink write | 59 ms, 50 ms | 79 ms |
| **Kill to the first sink write** | **3.18 s, 3.20 s** | **4.69 s** |

Detection is nearly the whole of it: the leader was busy, so its last heartbeat was
milliseconds old when it died, and the standby waited `TakeoverAfter` from there.
Everything after the decision is about 0.15 s — unless the leader died with a transaction
open, which is what the third run caught. Then `InitTransactions` waits about a second
for the coordinator to abort it, and the table's follower, standing just short of an
aborted tail, waits for the client's end-of-partition event (`fetch.wait.max.ms`, 0.5 s)
before it knows it has reached the end. A lower `fetch.wait.max.ms` on the followers
would buy that half second back for more idle fetches; it was left at the default. Other
cases:

- `SIGSTOP`, `TakeoverAfter` 1.5 s, heartbeat 0.3 s: 2.23 to 2.31 s from the stop to the
  standby deciding — the timeout plus a probe that has to time out (0.75 s here) because
  a stopped process accepts the connection and says nothing — and then as above, 2.39
  to 2.45 s from the stop to the first sink write. The woken leader exited as fenced 7
  to 16 ms after `SIGCONT`. In two runs it wrote nothing to the sink after its
  successor's first record; in one it wrote 19 records, all of the one step it had in
  flight when it was stopped. That is the gap the lease cannot close, seen once in
  three; the term rule discarded them and the table was exact.
- Manual takeover: 135 to 185 ms from the command to leading, 186 to 272 ms to the first
  sink write; the old leader knew 88 to 110 ms after the command.
- Step down: 173 to 243 ms from the command to the standby leading, which includes the
  leader running to its next checkpoint and disposing its circuit.
- Simultaneous start: in every run one node led under term 1 and the other was fenced on
  its term commit and exited.

**Deviations and limits.**

- *Simultaneous cold start is settled by fencing, not avoided.* Two standbys with the
  same timeout and no probe decide together; both initialise the transactional id; the
  later wins and the earlier is fenced on its term commit and exits. That is the *Both
  restart cold* row of *Failure modes*. With the TCP probe each sees the other as a
  standby, so the probe does not prevent it either. No jitter was added. The loser may
  have led for a moment — committed a term, even delivered — before reading the higher
  term; the logs are safe by fencing and the sink by the term rule.
- *A leader that is alive and leads nothing blocks failover.* Heartbeats come from the
  log writer, not from the circuits. If every circuit has faulted for a reason of its
  own and the process stays up, the standby hears a healthy leader. The host must step
  down or exit when its circuits are gone (step 7). *Step 7 decided this differently:* a
  pipeline is tenant code and its failure never moves leadership; the service gives
  leadership up by itself only when it cannot write its catalog. See *As built: step 7*.
- *A Kafka outage longer than `transaction.timeout.ms` still ends the active* (steps 3
  and 4): its heartbeat's transaction times out and reports as a fence.
- *Lag sampling builds no clients* (changed after review; it built two per table per
  sample): the head is read on the log's shared short-read consumer.
- *The watch is a third consumer* on each node (one thread), beside the followers.
- Not built: jitter or priority between standbys, more than one standby (nothing stops
  it; nothing was tested), a published lag metric, an admin endpoint for the commands.

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
measured against MSK. (Measured locally in step 5: detection 3.05 s after a kill -9,
steps 2 to 6 about 0.09 s, first sink write 0.06 s later. See *Measured failover* under
*As built (step 5)*.) Step 7 is under a second or two for Kafka sources. The exception is
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

### As built (step 6)

Code: `src/Incremental.Kafka` — `KafkaSource.cs` (the source, its options, lag),
`KafkaRecord.cs` (the record, deserializers, the keyed decoder), `KafkaSinks.cs`
(`KafkaUpsertSink`, `KafkaAppendSink`), `Debezium.cs` (the change-event decoder),
`KafkaConnect.cs` (the Connect REST helper and the Postgres connector configuration).
Tests: `KafkaSourceTests.cs`, `KafkaTableLoaderTests.cs`, `KafkaSinkTests.cs`,
`KafkaSourceResumptionTests.cs`, `DebeziumDecoderTests.cs` (no broker),
`DebeziumCdcTests.cs` (Postgres 17 and Debezium 3.0.8 on the local stack). Of the
adapters this section lists, only Kafka and CDC through Debezium are built; gRPC and
files are not.

```csharp
circuit.Start();                                                   // in StartLeadingAsync
await using var facts = await KafkaSource.StartAsync(circuit, ordersInput, ordersTopic);
await using var accounts = await KafkaSource.StartAsync(circuit, accountsKeyedInput, accountsTopic);
await using var loader = await KafkaSource.StartTableLoaderAsync(customers, cdcTopic, Debezium.Changes<int, Customer>());

await using var sink = new KafkaUpsertSink<string, Total>(new KafkaSinkOptions { Topic = "totals", BootstrapServers = "…" });
_ = circuit.Handle(totals).DeliverToAsync(leadership.Leased(sink), t => t.Key);
```

**One source, three targets.** `KafkaSource` is one consumer on one thread reading one
topic. What differs is where records go: a circuit input (each record with a value is
an insertion; a record with none is passed over), a keyed input (upsert or delete, as a
`KafkaChangeDecoder` reads the record; by default a value is an upsert and a tombstone a
delete), or a shared table's writer (the table loader).

**The position is the checkpoint**, as *Kafka* above specifies. Every record is handed on
through the stamped overloads with its partition and offset, so the circuit commits the
position with the state it produced. A source assigns all the topic's partitions itself,
each at `circuit.CommittedOffsets(input)[p] + 1`; the table loader uses
`table.CommittedOffsets` (the offsets a table records with its versions — "the table's
`$head` offsets" above). The consumer has a `group.id` because the client insists, joins
no group and commits nothing; `auto.offset.reset` is `error`. Particulars:

- Starts are numbers (a logical offset costs half a second; steps 3 to 5). The source
  asks for each partition's first and last offsets once, at start.
- A topic the source has never read starts at `StartFrom`: `Earliest` (default) or
  `Latest`. `Latest` means the end *as it was at the first start* — the start that finds
  nothing committed for any partition. The ends found then are recorded as positions
  (`AdvanceAsync`, below): a table loader commits them before it reports itself started,
  a circuit input takes them through its channel and they are in the first checkpoint. A
  later start resumes from them like any others, and a partition that still has no
  position then — it was empty at the first start, or added since — is read from its
  beginning, because everything in it is new. (Changed after review. As first built "the
  end" was taken afresh at every start for every partition without a position, so a
  partition that had been quiet, or whose records had been read and not yet checkpointed,
  silently skipped whatever it held.) Two limits. Until the first checkpoint after the
  first start nothing is recorded, so a crash in that window starts at the end again. And
  a topic that was *entirely* empty at the first start leaves nothing to record (an
  empty partition has no offset to stamp), so until some record is checkpointed a
  restart is a first start.
- A committed position below the partition's first retained offset is
  `KafkaSourcePositionLostException` at start. So is one past the partition's end, or
  one for a partition the topic does not have: that is another topic (recreated, or
  another cluster).
- *Deviation.* The text has a compacted reference topic re-read from the start through
  `ReplaceAllAsync` when its position has aged out. That is built for the table loader
  and is opt-in (`ReloadTableWhenPositionLost`): the loader reads the topic to the end it
  had, replaces the table's rows with the result and records the end as the position. It
  is not built for a keyed circuit input, whose replay check would drop the re-read
  records (keyed-input snapshot sync does not exist). And it cannot mend a recreated
  topic, whose offsets are below the table's position: that is refused.
- *Offsets that yield nothing are recorded too* (changed after review; this bullet used
  to say they "need nothing"). The committed position was the last record *stamped*: not
  a transaction's marker, not a tombstone on a stream input, not a record the decoder
  passes over, not a table change that changes nothing. On an idle topic those are the
  last offsets, and when retention had removed them the next start compared "committed +
  1" with the topic's new start, reported the position lost — and with
  `ReloadTableWhenPositionLost` reloaded an empty topic and emptied the table. Now:
  a tombstone or a passed-over record on a circuit input is sent as
  `handle.AdvanceAsync(partition, offset)` in the record's place; and when a source has
  stood at the end of a partition, past the last position on record, for
  `CommitIdlePositionAfter` (default 30 s), it records where it stands — for a table,
  `Writer.AdvanceAsync` and a commit, which mints one version with no changes. The delay
  is there because behind every transaction of a busy topic is a marker, and recording
  each would cost a table a second version, and a circuit a second step and commit, per
  transaction. What remains: a process that stops within the delay of the last record
  leaves the position at that record; if it then stays down until retention passes, the
  start reports the position lost as before.
- A source reads the partitions the topic has when it starts. New partitions are seen at
  the next start.

**Core change: `AdvanceAsync`** (added after review, for the two bullets above). A source
had no way to say "I have read up to here" where there was no record to stamp.
`InputHandle<T>.AdvanceAsync(partition, offset)` and the same on `KeyedInputHandle` send
an entry with a position and nothing else through the input's channel; the drain admits
its offset and passes on. It costs the ordinary path nothing: the entry reuses the
rejection's slot with a sentinel, checked only where a rejection already was.
`TableWriter.AdvanceAsync(partition, offset)` stages a position and marks the batch, so
the next `CommitAsync` records it even with no change to publish — the one case in which
a version with no changes is minted. Nothing else about versions changed: followers,
range reads and restores take it as any version. Alongside it,
`ReplaceAllAsync(rows, offsets)` now records its offsets even when the snapshot changed
nothing (a snapshot is taken because the old position was no good; it used to be taken
again at every start until some row changed). shared-tables.md is updated.

**Core change: `RejectAsync`.** "Poison records go through the error policy with their
offset" needed a seam, because deserialization happens in the source, outside any
operator. `InputHandle<T>.RejectAsync(record, error, partition, offset)` and the same on
`KeyedInputHandle` send a record that has no value through the input's channel in its
place. The drain admits its offset like any record's — so the checkpoint covers it and a
replay drops it — and then reports it from `input[name]` under `DeadLetter`, or throws
under `Fault`, which faults the circuit. The cost on the ordinary path is one more
reference in each queued entry and one null check per drained record. `InputPort` now
takes part in `IDeadLetteringInput` as `KeyedInputPort` did. A `KafkaSource` passes a
`KafkaRecord` (topic, partition, offset, key and value bytes, timestamp, headers) as the
record. A table has no dead-letter output: its loader stops on an unreadable record
unless `OnUnreadableTableRecord` is set.

**Starting.** The source's first requests — the topic's partition count, each
partition's first and last offsets — are made through an admin client that shares the
consumer's connections, before the consumer has anything to poll for. Two things were
wrong with that as first built (both found in review). A consumer serves its callbacks
only when polled, the OAUTHBEARER token refresh among them, so with MSK IAM no token was
ever set and the requests timed out: no source could start. The source now polls the
(unassigned) consumer while it waits for each request; a test with an OAUTHBEARER
handler sees it called within the first two seconds of a start that cannot succeed
against the local PLAINTEXT broker. And the offset requests, like `MeasureLagAsync` and
`WhenCaughtUpAsync`, had no retry: a partition between leaders ended the source, and a
source started from `StartLeadingAsync` ended the takeover. They now go through the
log's retry (`KafkaLog.RetryTransientAsync`) for up to `OperationTimeout`. Not verified:
a real SASL/OAUTHBEARER broker, and a multi-broker leader election.

**Backpressure, and two things librdkafka does.** The source writes one record at a time
into the input's bounded channel and waits when it is full, so it stops polling; the
client then stops fetching when it holds `FetchBufferKilobytes`
(`queued.max.messages.kbytes`, set to 16 MB instead of the client's 64). A wait longer
than `PauseAfter` (1 s) pauses the partitions until the write goes through. Measured
with a 76 MB topic, a channel of 256 records and a 2 MB buffer, the circuit not stepping:
the source had read 256 records and the client held at most 3.4 MB (the limit plus the
fetches in flight when it was reached), for as long as the stall lasted; then all 40,000
records arrived once each.

- *Pausing does not free memory.* A paused client keeps what it had fetched until it is
  next polled, then drops it and fetches it again. So the bound is the fetch buffer, with
  or without the pause; the pause only stops fetch requests during a long stall, and
  costs one refetch of the buffer. That is why `PauseAfter` is a second and not
  milliseconds.
- *`fetch.queue.backoff.ms` throttles a small buffer.* When the buffer is full the client
  postpones its next fetch by this long, and the default is 1 s — so a source could
  drain at most one buffer a second. With the 2 MB buffer the 40,000 records took 19 s;
  with the backoff at 10 ms, which the source now sets, 0.3 s.

**The table loader.** Each change is staged with `(partition, offset)`, so the table's
own replay check drops whatever is read twice. A version is committed when the loader
has read everything the client had fetched, or `MaxTableBatch` records (4096): with a
Kafka log behind the table, what arrives during one commit is the next version. 4,300
records at a cap of 500 became 9 versions. `Positions`, `MeasureLagAsync()` and
`WhenCaughtUpAsync()` report progress; for a loader a position counts once its version
has committed. It is the table's writer, so it runs on the leader only:
`StartTableLoaderAsync` refuses a following table, and the tested takeover is
`AcquireWriterAsync`, `table.PromoteAsync()`, then the loader, which read exactly the
records the followed table did not have. A deposed leader's loader ends at its next
commit with the table log's fence. One consequence of shared-tables.md is softened:
offsets ride with versions, so a tail of records that changed nothing is read again
after a restart, harmlessly — but only until the loader has been idle for
`CommitIdlePositionAfter`, when it records where it stands (*Offsets that yield nothing*
above).

**Sinks, and which producer.** `KafkaUpsertSink` (key → row, delete → tombstone) and
`KafkaAppendSink` (one record each). Both stamp `incremental.step` on every record and
`incremental.term` when given `Term`. Both place a keyed record by the Java client's
murmur2, computed here, so placement does not depend on which producer wrote it (checked
against librdkafka's own `murmur2` partitioner). A sink does not create its topic.

> **Decided in step 6: sinks use an idempotent producer of their own by default, and the
> log's transactional producer on request.** The default (`BootstrapServers`) is
> `enable.idempotence` with `acks=all`, not transactional. Reasoning: the delivery
> guarantee is the engine's and is at-least-once either way — a batch is acknowledged
> when every delivery report is in, the checkpoint commits after, and a crash between
> the two delivers the batch again; putting the sink write in a transaction would not
> change that, because it would be a different transaction from the checkpoint's
> (exactly-once to sinks is out of scope). A sink topic belongs to its readers and may
> live on another cluster, and a non-transactional topic asks nothing of them. What the
> default lacks is fencing, so it is to be wrapped in `leadership.Leased` and given
> `Term`. With `KafkaSinkOptions.Log` the batch is instead one commit in the log
> writer's group commit: a deposed leader cannot write the topic at all (*The sink
> gap*: "a Kafka sink written through the transactional producer is fenced outright").
> The price is that the topic is on the log's cluster, a delivery waits for a
> transaction, and readers should use `read_committed`. Tested both ways; the fenced
> case is tested (the batch does not reach the topic and the delivery fails with
> `KafkaLogFencedException`).

For that, a log commit's records may now carry headers and a null key
(`LogRecord`); the log's own records use neither.

*What the step header can and cannot do.* Along a partition the step never falls except
at a restart, where it steps back to the first replayed step. With manual stepping the
replay cuts the same steps, and a reader that skips steps it has seen is exact. An
auto-stepping replay may cut other steps — a record can come back under a different
step, and a replayed step may deliver nothing to this sink at all — so there a reader
gets at-least-once and no more (the resumption test saw 103 and 177 of 6,000 facts
twice in two runs). Making it exact needs the sink to say where a run begins (a run id
and the committed step it resumed from, as headers) so a reader can drop what an
earlier run wrote above it. Not built.

**Debezium.** `Debezium.Changes<TKey, TRow>()` is a `KafkaChangeDecoder` for the JSON
converter's output, with or without `schemas.enable`. `c`, `u` and `r` upsert `after`;
`d` deletes the record key; the tombstone after a delete deletes it again; `m` is
passed over; `t` is unreadable. The key is the primary key from the record key: one
column as a scalar, several as a record. Checked against what the connector really
writes (the decoder tests use captured records):

- Under Postgres's default replica identity a delete's `before` holds the key and
  *defaults* in every other column — empty strings, zero, the epoch — not nulls. Nothing
  here reads `before` for a keyed table. `REPLICA IDENTITY FULL` is needed only to read
  `before` (`Debezium.Events<TRow>()`), or for a table with no primary key, whose
  deletes are keyed from `before` by a `keyOf` selector.
- An update of the primary key arrives as a delete of the old key, its tombstone, and a
  create of the new key.
- `numeric` arrives as bytes unless the connector has `decimal.handling.mode=string`,
  which `DebeziumPostgres.ConnectorConfig` sets. `timestamptz` is ISO-8601 text.
- A second connector on the same slot snapshots every row again. The table read the 50
  duplicate rows and minted no version, because an upsert that changes nothing is not a
  change.

`KafkaConnectClient` (validate, create or update, status, wait until running, restart,
delete) and `DebeziumPostgres.ConnectorConfig` are in the main package: they need
`HttpClient` and nothing else. Not handled: truncates, the TOAST placeholder for an
untouched large column (it arrives as the placeholder text), schema changes, topics
flattened by `ExtractNewRecordState`, and Avro (which plugs in as a Confluent
deserializer through `KafkaDeserializers.From`, untested).

**The CDC test.** Rows are in Postgres before the connector exists (the snapshot); the
connector is validated and registered through the helper; a loader fills a shared table
that a circuit reads through a table `Join` and a `Lookup`, and a second table with a
composite key feeds a keyed input. Then an insert, an update and a delete; a key change;
a connector restart in the middle of sixty statements; and the second snapshot. After
each the table is compared with the database, and the join's sink with what the rows
imply. Run with and without embedded schemas.

**The resumption test.** Two source topics are produced to for about seven seconds while
a circuit reads them — facts into a stream input, accounts with deletes into a keyed
input — with its state on the Kafka changelog and its results in four Kafka sink topics.
The process is dropped at six random moments. Each successor opens a new log (fencing
the last), restores, and starts its sources; the test checks that each source's first
record is at exactly the committed offset plus one. At the end sums and counts per key
are exact, the append topic holds every fact at least once, and the keyed topic holds
every surviving account with its last row. One run: 7,200 records, 211 read again
across the six restarts. The keyed sink may also keep an account the model deleted (*A
gap in "keyed upserts converge"*: the source cannot make an auto-stepping replay cut the
same steps); the test allows that only for keys the model deleted, and saw none in the
runs made.

**Mutation checks.** 27 mutations of the sources, the sinks, the decoder and the core
seam — resume one record late or early, ignore the checkpoint, drop the fetch bound,
never pause, swallow an unreadable record, read a tombstone as nothing, stage table
changes without offsets, lift the batch cap, let a follower load, miss a lost position,
write a delete as an empty value, misplace a key, shift the step header, let a fenced
log's sink carry on, and others. 25 were caught as the tests stood. Two were not. A
sink that returned before its delivery reports were in passed everything, so a test now
has the broker refuse a record and requires the write to fail unacknowledged. A decoder
that ignored `d` events passed the CDC test, because the tombstone that follows a delete
does the same work; the decoder's own tests catch it, and it only matters for a
connector with `tombstones.on.delete=false`.

**Limits.**

- One topic, one consumer and one thread per source.
- No metrics are published; the counters (`RecordsRead`, `RecordsUnreadable`, `Pauses`)
  and `MeasureLagAsync()` are there to be read.
- A source is not leased or fenced: a deposed leader's source reads until its circuit
  faults at the next commit. Reading changes nothing, so nothing depends on it.
- `WhenCaughtUpAsync()` on a circuit input means written to the input, not stepped.
- Not built: gRPC and file adapters, keyed-input snapshot sync, the run headers above.

## Failure modes

| Failure | What happens |
|---------|--------------|
| Active dies mid-commit | The open transaction is aborted by the standby's `InitTransactions`, or completes if it had reached the coordinator's commit decision. Either way the standby reads a whole commit or none of it. |
| Active dies mid-delivery | The checkpoint covering those steps was never taken. The new active replays and redelivers; sinks apply the upserts again. |
| Network partition, both alive | The standby takes over only if it can reach Kafka, and doing so fences the old active's logs. The old active stops sink deliveries when its lease lapses and exits on its next commit. The sink gap above is the remaining exposure. |
| Kafka unavailable | The active cannot commit: table versions stop publishing, pipelines stall at their commit point, inputs back up. The standby cannot fence, so it does not take over. Both recover when Kafka does. Nothing is lost; nothing progresses. (As built: the standby checks that it can reach the leader topic before it tries, so it does not sit in a takeover that would depose the leader on reconnect; and an outage longer than `transaction.timeout.ms` does end the active — see the two *As built* sections.) |
| Kafka slow (broker election, patching) | Commits stall for seconds. The probe keeps the standby from taking over until `TakeoverCeiling`. |
| Standby lagging | Takeover step 3 takes as long as the lag. Lag is a metric and an alert; a standby that cannot keep up is not a standby. |
| Standby dies | No effect on the active. No failover cover until it has cold-restored, which takes minutes for large tables. |
| Both restart cold | Whichever initialises the producer first becomes active after a full restore; the other is fenced on its first transaction if it also tried, exits, and returns as standby. Downtime is the cold restore. (As built it is the one that initialises *last* that leads — that is what fencing means — and the test for it is the simultaneous start.) |
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
  isolated from its neighbours, a read to the end across an aborted tail, a follower's
  position after a commit, a snapshot restore under a busy writer (and the rule behind it
  against 20,000 simulated interleavings, with no broker), range reads and trimming, a
  200,000-row restore, and the in-process failover: a circuit with a stream
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
  prove a zombie cannot commit. *Built* (`KafkaFailoverProcessTests`, with
  `tests/Incremental.Kafka.FailoverApp`), with two differences: the source is a
  deterministic generator whose position is the checkpointed offset, because the Kafka
  source adapter is step 6; and the reference is a model computed from the generator
  rather than a second run, with a no-failure run checked against it. Also built: manual
  takeover, step down, and two processes started together. See *As built (step 5)*.
- **Leadership in one process** (`KafkaLeadershipTests`): heartbeats in every transaction
  and when idle; a standby that waits while it hears the leader and takes over when it
  does not; the probe holding it back until the ceiling; the TCP probe's answers; what a
  deposed leader learns from the term and from its heartbeat; the promotion order; a
  failed takeover that must stop heartbeating; lag figures; the lease lapsing under a
  stalled writer and a leased sink refusing a deposed leader. *Built.*
- **CDC**, through the Debezium container: Postgres changes reach a shared table, the
  connector is restarted to force duplicates, and the table matches the database.
  *Built* (`DebeziumCdcTests`, skipped when Postgres or Kafka Connect is unreachable); the
  duplicates come from a second connector's snapshot, since a restart alone need not
  produce any. See *As built (step 6)*.
- **Sources and sinks** (`KafkaSourceTests`, `KafkaTableLoaderTests`, `KafkaSinkTests`,
  `KafkaSourceResumptionTests`): resumption at the committed offset plus one, per
  partition; refusal when the position is gone; tombstones on a keyed input; unreadable
  records dead-lettered with topic, partition and offset, and their offsets
  checkpointed; a stalled circuit holding the client to its fetch buffer; the table
  loader's versions, lag, takeover by a standby and reload; an upsert sink's topic read
  back by a keyed source in a second circuit, through either producer; the append
  sink's headers; and sources resumed after six crashes against a model. *Built.*
- **After review** (`KafkaSourcePositionTests` and additions to the others): a position
  past markers, tombstones, passed-over records and no-op table changes survives the
  topic losing everything; a busy transactional topic costs a table no extra versions; a
  start at the end is taken once; a source hands on committed records only (an aborted
  and an open transaction in its topic); a source overtaken by retention while it runs
  stops with `KafkaSourcePositionLostException`; the token refresh handler is served
  during start; the transient-error retry; a broker-refused record beside a large
  neighbour; `MaxTransactionRecords`; a stalled follower's bounds; concurrent first
  opens of a table; a trim racing an append; a long commit under a short takeover
  timeout, and a leader stopped mid-transaction; disposal mid-takeover; concurrent
  step-downs; lag sampling without clients. Each fix was checked by putting the defect
  back and seeing its test fail: 22 mutations, 18 killed as the tests were first
  written. Of the four that survived, two were stress tests of just-created topics
  (tables, sources) that the local broker did not make fail on demand — kept as smoke
  tests beside a unit test of the retry, which does fail — and two led to a tighter
  test: a trailing tombstone that the end-of-partition rule had covered for, and a token
  handler that a failed source's close also calls, now timed.

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
5. **Done.** Leader topic heartbeats, the election, the takeover sequence, the manual
   commands, the delivery lease; the kill -9 test. See *As built (step 5)* under
   *Leadership and fencing*. Steps 6 and 7 should know: every node starts as a standby
   and `KafkaLeadership` calls `StartLeadingAsync(term)` when it leads — that is where a
   source adapter connects, at `circuit.CommittedOffsets`; the failover app's generator
   stands where the Kafka source will (`tests/Incremental.Kafka.FailoverApp/Program.cs`,
   `RunAsync`), and swapping it for a real source topic is the test's next form; exact
   sinks need replay to cut the same steps, which the generator gets by construction and
   a real source will not (*A gap in "keyed upserts converge"*); `Fenced` and `Resigned`
   end the process (the host should exit non-zero on `WhenFinished` and let its
   supervisor restart it); the host must step down or exit when its circuits have
   faulted, since heartbeats come from the log writer and not from them; the commands are
   methods and need an endpoint; `Status` and `MeasureLagAsync` are what an admin page
   reads; wrap external sinks in `leadership.Leased`.
6. **Done** for Kafka and for CDC through Debezium: `KafkaSource` into inputs, keyed
   inputs and tables, `KafkaUpsertSink` and `KafkaAppendSink`, the Debezium decoder. See
   *As built (step 6)* under *Source resumption*. Not built: the gRPC and file adapters,
   whose contracts stand as written there. Step 7 should know: a source is started after
   `circuit.Start()` — its factory reads `circuit.CommittedOffsets` — and disposed
   before the circuit, so both belong in `StartLeadingAsync` / `StopLeadingAsync`; a
   table loader starts after its table is promoted, which the takeover has done by then;
   `source.Completion` faults when the source fails (position lost, a read error, the
   circuit's fault) and the host should treat that as it treats a circuit fault;
   `circuit.DeadLetters` must be read when the policy is `DeadLetter`, or unreadable
   records stall the circuit once the channel is full; external sinks go through
   `leadership.Leased`, with `Term = () => leadership.Term`; and the failover app's
   generator can now be replaced by a source topic.
7. **Done**, in `Incremental.Service` rather than `Incremental.Hosting`: a service node
   on `KafkaServiceBackend` is an active or a standby, and the notes addressed to this
   step are answered one by one in *As built: step 7* at the end of this document.
   `Incremental.Hosting`'s `CircuitHost<TDefinition>` still knows nothing of roles: one
   compiled-in circuit that needs a standby is wired by hand, as
   `tests/Incremental.Kafka.FailoverApp` does, or run as a one-pipeline service.
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
  behaviour. Measured once locally (step 5): 1.05 s when the killed leader had a
  transaction open, against 30 to 45 ms when it had not. The worst case, and the figure
  on MSK, are still unknown.
- ~~How `Confluent.Kafka` surfaces fencing.~~ Verified against the local broker:
  `Local_Fenced`, fatal from the transaction calls, not flagged fatal on a later
  `Produce`. See *Fencing, verified* under *As built: `Incremental.Kafka`*. Still not
  verified: that MSK's broker version reports it the same way, and what a producer sees
  when the broker itself aborts a timed-out transaction (the code treats any fatal error
  as a fence, which is safe either way).
- ~~Why `InitTransactions` takes about half a second.~~ It does not: it takes 2 to 55 ms
  locally. The half second was a consumer assigned at a logical offset (*A logical start
  offset costs half a second*). Still not verified: what it takes on MSK, across
  availability zones, and when the old writer died with a transaction open.
- That the probe and the last check against the broker behave on MSK during a broker
  election or a rolling patch as intended: the leader's heartbeats stall, the probe
  answers, the standby waits. Not exercised; it needs a broker that can be made to stall.
- The standby's last check before a takeover (it asks for the leader topic's end offset
  and stands down if it cannot, or if there are records it has not read) has no test.
- The long-commit protection (in-transaction heartbeats, the standby's uncommitted-end
  samples) on MSK with a genuinely large commit: tested locally with a commit slowed by
  a hook. Whether the leader topic's batch is sent promptly while the client's queue is
  full of another partition's records is what a real large commit would show.
- The lease on a suspended host (`CLOCK_BOOTTIME`) and on a paused guest whose clocks
  stop. Neither can be produced here; the second is a stated limit.
- `Local_NoEnt` and the other retried admin errors during a real leader election
  (multi-broker). Seen here only for just-created topics.
- Leadership timing under load on a small machine. A leader starved of CPU for longer
  than `TakeoverAfter` is replaced; that is correct, but how often it happens on the
  chosen instance size is unknown.
- That the offset just below a last stable offset is always a transaction marker on MSK
  as it is on Kafka 3.9 (the readers finish a read to the end on that basis). If it were
  not, a read would fall back to the end-of-partition event and be slower, not wrong.
- Behaviour of the group commit under a slow or failing broker: abort-and-retry and the
  split into single-commit transactions are now exercised by a record the broker refuses
  mid-transaction as well as by one the client refuses; the retriable-commit loop and a
  transient abortable error (a message timeout) are still not. A fault-injection run (a
  proxy that drops or delays) is needed.
- Which Kafka version MSK must run to include the KIP-890 broker-side checks against
  writes from a stale epoch. The design does not rely on them for a well-behaved client.
- KAFKA-13727 (cleaner removing abort markers too early) should be confirmed fixed in the
  chosen broker version, since both compacted topics carry transactional data.
- Sources and sinks on MSK: IAM or TLS through `ClientConfig` and the builder hooks,
  and a source through a broker election. Nothing was run against either. Locally it is
  shown only that a source serves its OAUTHBEARER refresh handler while it starts; a
  source blocked on a full input for longer than a token's life is not polled meanwhile,
  and what the client does when it is polled again was not looked at.
- Whether `fetch.queue.backoff.ms` at 10 ms and a 16 MB fetch buffer are the right
  defaults at production record sizes and rates. They were chosen from one local
  measurement.
- A source's behaviour when the broker fails a fetch for a reason the client does not
  retry: `Consume` then throws and the source ends with the error. Which errors those
  are in practice was not explored.
- Debezium beyond the Postgres connector 3.0.8 with `pgoutput` and the JSON converter:
  other databases, Avro, incremental snapshots, schema changes.
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

## As built: seams for the enrichment service (2026-11-10)

Three additions to `ChangelogStorageProvider`, made for `Incremental.Service`
(enrichment-service.md, *As built*), tested in `tests/Incremental.Tests/ChangelogReleaseTests.cs`:

- `CommittedOffsets`: the last commit's offsets as a copy, in either role and with no
  circuit built. A registry reads the table versions a stopped pipeline's checkpoint still
  needs (`{table}#t`, `{table}#w`) from it.
- `ReleaseStores()`: the typed stores handed out so far are forgotten and what they held
  becomes held bytes again, as if no circuit had been built — after undoing writes since
  the last commit. The next circuit creates its stores from those bytes with its own key
  and value types. This is how state passes between two loads of an assembly, whose types
  are different `Type` objects, and it is the way out the "already created with different
  key/value types" refusal lacked. No circuit may be running on the provider. It writes
  nothing to the log.
- `UseSerializer(serializer)`: replaces the serializer for stores created from now on;
  refused while a typed store exists. One serializer per load of a pipeline's code lets
  its type-metadata cache go with that load.

**A limit of releasing, not fixed.** An entry's record is found in the log by its key's
bytes, and the provider depends on a key serializing back to the bytes it was read from.
A type that is part of an entry key — a weight store's value, an indexed weight store's
value — which gains or loses a member between the generation that wrote the state and the
one that reads it serializes to other bytes: the new generation's writes go to new
records and the old ones are never overwritten or tombstoned, so a restore after that
reads both. A value store's value is not part of a key and may change shape. Nothing
detects the unsafe case.

## As built: step 7 — roles in the service (2026-11-10)

Step 7 is `Incremental.Service` on `KafkaServiceBackend`
(`src/Incremental.Service/KafkaBackend.cs`; enrichment-service.md, *As built*, S2). What
this document asked of it, and where each request went:

| Asked here | Built as |
|---|---|
| Wire `Changed`, `WhenFinished` and `Status` into the host | `INodeLeadership` over `KafkaLeadership`; `ServiceNode` publishes role changes as events and reports `NodeStatus` (role, terms, heartbeat age, lease, takeover timings, the fence's cause, follower lag, the peer's answer) |
| `Fenced` and `Resigned` end the process, non-zero | On `WhenFinished` the node abandons its circuits without draining, sets exit code 3 (fenced) or 4 (resigned) and calls `IHostApplicationLifetime.StopApplication()`. Tested with real processes |
| The commands need carrying between nodes | `ServiceNode.TakeOverAsync` and `StepDownAsync`: refused unless this node, or the peer, is ready (`Force` overrides), each returning a plan. The HTTP endpoint is the admin package's (S3) |
| Step down or exit when the circuits have faulted | **Not done, on purpose.** Pipelines are tenant code: one deterministic bug would hand leadership over and fail the same way on the other node. The node gives leadership up by itself only when its catalog's writer has faulted and cannot be reopened — it steps down to a ready standby, or exits with 5. Every desired-running pipeline being `Failed` is `Unhealthy` in health, and an operator steps the node down by hand |
| Sources start after `circuit.Start()` and stop before the circuit | The supervisor's generation: `Start()`, deliveries, the dead-letter reader, then sources; stop in the reverse order |
| A table loader starts after its table is promoted | Tables are promoted by the registry inside `PrepareAsync`; loaders start at the end of `StartLeadingAsync`, and resume from `table.CommittedOffsets` |
| A faulted source is a faulted circuit | The supervisor watches `source.Completion` with the circuit and the deliveries; any of them failing tears the generation down and restarts it under the backoff policy |
| `circuit.DeadLetters` must be read | Always read, into a ring per pipeline |
| External sinks go through `leadership.Leased`, with the term | Every sink a connector creates is wrapped; the `kafka` connector passes `Term = () => leadership.Term` |
| The failover app's generator replaced by a source topic | `tests/Incremental.Service.FailoverApp`: a Kafka source topic, a table loaded from a topic, Kafka sinks, pipelines from a plugin assembly; `KafkaServiceProcessTests` |

Tables are **not** tracked with `KafkaLeadership.Track(table, log)`: it promotes them one
after another and any one throwing ends the takeover. The service promotes them itself in
`PrepareAsync`, concurrently and each contained, which needed no change to
`KafkaLeadership`. `TakeoverTimings.Tables` is therefore zero for a service node and the
table stage is inside `Prepare`; `ServiceTakeoverTimings` splits it out.

**Four changes to `Incremental.Kafka`, made for this step.**

- *The probe's line has a fourth token* (K1). `LeadershipProbe.Listen(leadership,
  endpoint, detail)` asks the host for a token each time it answers and appends it when it
  holds no space or line break; `LeadershipProbe.QueryAsync(host, port)` returns the whole
  answer as a `LeadershipProbeAnswer` (role, term, node, detail), or null. `Tcp(...)`
  still reads the first token only, so old and new nodes probe each other. The service
  puts `ready` or `notready:{reason}` there, which is how a leader learns whether a ready
  standby exists.
- *A row too large for one record is refused by name* (K2). `KafkaTableLog.AppendAsync`
  builds a version before producing any of it; a change (old row plus new) or a row
  record over `MaxRecordBytes` now throws `KafkaLogException` naming the key, the size and
  the version, and nothing of the version is written. The table's writer still faults —
  any failed append does — but the fault says which row.
- *Short reads no longer wait out each other's long poll.* The log's shared short-read
  consumer (a table's head, the last term) is assigned for one read and unassigned. Each
  read stops at the end of a partition, by which time the client has sent the broker its
  next fetch; with the default `fetch.wait.max.ms` that fetch waits half a second for
  data that will not come, and the client sends one fetch per broker at a time, so the
  next short read — of another partition — began half a second late. A takeover reads
  one head per followed table: with one table (step 5's measurement) it never showed;
  with 21 the table stage took 10.5 s. The short reader now sets `fetch.wait.max.ms` to
  5. It polls only while a read runs. Followers and restores keep the default.
- *The short reader is told a topic's leader before it is first assigned to it*
  (`KafkaLog.AssignForShortRead`). A running consumer assigned a partition of a topic it
  has not seen waits for the client's one-second housekeeping timer to look the leader
  up, so each first short read of a topic took a second — two per table when a node
  opens its tables (the head of the rows topic, the oldest version in the changes
  topic), and one for the successor's read of the leader topic at a step-down. A
  metadata request for the topic, through the consumer's own handle, before the first
  assignment removes the wait: a standby following 21 tables went from 45 s to under 7,
  and a step-down's hand-over from 1.2 s to about 0.3. A new consumer's first assignment
  never paid this, which is why followers, restores and step 5's one-table measurements
  did not show it. The suite in `tests/Incremental.Kafka.Tests` runs half a minute
  faster for it.

**Measured with the service** (two processes, 21 tables, two plugin pipelines, kill -9
mid-stream, local broker): see enrichment-service.md, *As built*, S2 — detection is
`TakeoverAfter`, and everything after the decision totals under a second.

## As built: layouts — typed stores on a follower (2026-11-11)

Measuring the service with large state (enrichment-service.md, *As built*, S6, *Scale*)
showed what *Warm standby* above left open in the service: a standby that holds a
pipeline's state as bytes decodes it at takeover, about 1.3 µs an entry, so a takeover
grew by 2.7 s per million orders in the largest pipeline. A follower may hold typed
stores instead (it always could: `ApplyReplicated` applies to a store that exists), but
only if it can never apply a record another version's types wrote. Nothing in the log
said whose types wrote a record. Now something does.

**The record format gained a format byte.** A value is either

```
1 <serializer output>                      as before
2 <n: one byte> <n bytes of layout> <serializer output>
```

Format 2 is written when the provider has been given a **layout**
(`ChangelogStorageProvider.UseLayout(string)`, allowed only while it holds no typed
store): an opaque identity, at most 255 bytes of UTF-8, of whatever decides the stores'
types. Every value — entries and the commit record — carries it. A provider with no
layout writes format 1, so nothing changes for a circuit that never calls `UseLayout`.
A reader accepts both; a build from before this one refuses format 2 by its format byte,
as it is meant to. So both nodes of a deployment are replaced together before the first
record is written under a layout: stop the standby, replace the leader, start the standby.

**What a follower does with it.** While it holds typed stores:

- a value whose layout is not the provider's is *foreign*: the stores are turned back
  into bytes (`ReleaseStores`) before the record is applied, and the record joins the
  bytes. So does a commit record of another layout.
- a tombstone carries no value and so no layout. One whose key the store's types cannot
  read has the same effect, instead of faulting the follower.
- afterwards no typed store can be created on that follower until `UseLayout` is called
  again (`GetOrCreate` throws): a circuit that was half built when the layout moved does
  not finish on the old types.

Because the check is per record, it does not depend on a marker surviving compaction or
on the follower being close to the end of the log.

**`CommitLayoutAsync()`** writes, on the leader, one transaction holding only a commit
record: the last checkpoint again (same sequence, same offsets, same manifests) under
the provider's layout, with `"announcement": true`. It does nothing when the last commit
record already carries the layout. It exists for liveness, not safety: a follower builds
typed stores when `CommittedLayout` — the layout of the last commit record it applied —
is the one it would build, and a pipeline that has not checkpointed since its layout
changed (it is idle, or it has just been deployed) would otherwise keep its followers on
bytes.

**Going back.** A store created from held bytes keeps them (`_origins`) until its types
have written, and `ReleaseStores` gives those bytes back rather than what the types would
write. On a follower "its types have written" means: a record of its layout was applied
to a store, or a commit record of its layout that is *not* an announcement arrived. A
follower that built a layout's stores on an announcement, and then reads another
announcement because the leader's circuit refused the state and the previous spec was
put back, returns to the very bytes it had. An idle pipeline therefore holds its state
twice on a follower — typed and as the bytes it was read from — until its first
checkpoint under the layout.

*Since S7 of the service* that checkpoint does not wait for a record. A provider that
is keeping such a copy says so (`ICheckpointStorageProvider.HoldsRestoredCopy`), and an
auto-stepping circuit that starts on restored state and has had nothing to read for two
seconds (`Circuit.RestoredCopyGrace`) commits once without a step: the offsets, the
clock and the step marker as it recovered them, under its own manifest. The leader's
copy goes with that commit and a follower's when it reads it. What this gives up is
the last case in which "types that have committed nothing have changed nothing" held
for longer than a moment: a build that ran idle has, after two seconds, committed. The
log is not touched beyond the commit record — every entry is still there as it was last
written. `ChangelogStorageProvider.EstimateSize()` reports the entries held twice
(`EntriesHeldTwice`) for as long as there are any.

**`StoreEpoch`** counts the times the typed stores were given up, by a call or by the
follower. A circuit built on a provider is good to start only while the epoch is what it
was before the circuit was built; the service checks this at a takeover.

**What it does not change.** A leader's `ReleaseStores` still re-serializes what its
types hold, so a member the types do not model is gone from memory there once they have
committed — and from the follower's typed stores at the same moment, since they are the
same types reading the same records. The log keeps an entry's old bytes until that entry
is next written.

Tests: `tests/Incremental.Tests/ChangelogLayoutTests.cs` (seven). The service's use of it
is in enrichment-service.md, *As built*, S6.

## As built: what three brokers and an authorizing broker found (2026-11-11)

Run against the `kafka3` and `kafka-sasl` compose profiles (enrichment-service.md, *As
built*, S6, *Scale and a production-shaped Kafka*, has the runs). Each of these was
invisible on one plaintext broker:

- **A `ListOffsets` answer can carry an error instead of an offset**, per partition, and
  the client does not throw it: a partition whose leader is down and not yet replaced
  comes back with its offset unset (−1001). Every caller read that as an empty
  partition. `KafkaLog.OffsetOf` throws the answer's error, which the admin retry treats
  as passing. Seen as: a takeover with a broker down announcing a term that had been
  used; a takeover after a quorum outage finding no head for the catalog.
- **A writer never announces a term below one it has read** (`KafkaLog.NoteTermSeen`,
  fed by the leadership's watch of the leader topic), and reads the last term again
  until it is not, or until the leader topic's committed end is its end. **A table log
  does not believe a missing head or an older one** on a table it was opened on, and a
  writer does not mint an identity while the rows partitions hold undecided records.
- **Admin retries are short attempts with a metadata refresh between them**
  (`RetryTransientAsync` with an attempt timeout; `LeadersKnownAsync` after a topic is
  created). An admin call that needs a leader takes it from the client's cache, where a
  topic created a moment ago can sit with no leader; the call waited out the whole
  operation timeout and the same call again found the same cache.
- **A consumer's callbacks run only inside `Consume`**, the OAUTHBEARER token refresh
  among them. `AssignedConsumer.ServeCallbacks` is called before a consumer is asked
  for metadata or watermarks (the log's short reader, the service's blob store).
- **The client looks up the coordinator of its `group.id` although nothing joins the
  group**, and reports a refusal through `Consume`. `AssignedConsumer.ConsumeAssigned`
  passes it over; every consume in this library and in the service goes through it.
- **The writer says when it waits.** A commit that keeps timing out, and a transaction
  whose records the brokers have not taken for five seconds and every five after, are
  logged at Warning: that wait is what a standby sees as silence. If an abort itself
  fails, the message says what the transaction was being aborted for.
  `KafkaLogOptions.Logger` is settable, and the service gives the log its own logger
  when the host gave none — before, a host heard none of this.

## As built: event-time state (2026-11-11)

Until this step the watermark and every eviction deadline lived in operator fields. A
circuit restored from a checkpoint started with no watermark and no deadlines, so a late
record could reopen a closed window and overwrite a delivered row, and a key that was
open at the restart and never touched again stayed in the stores, the changelog and the
standby for good. Both are state now.

**The clock.** `WatermarkClock.AppendCheckpoint` adds to every commit's offsets marker
the frozen watermark under `$watermark` and each `WithEventTime` source's max event time
seen under `$watermark#s{n}` (UTC ticks; sources numbered in declaration order; a value
not yet set is left out). The circuit calls it in `CommitCheckpointAsync`, which runs
between steps and after `Freeze()`, so the entries are the clock exactly as the
committed step left it. `Start()` hands the recovered marker to `WatermarkClock.Restore`
before the first step. The per-worker maxima are not kept apart: `Freeze()` takes their
maximum, so the maximum is all that is restored (into worker 0's slot). In a clustered
circuit each process commits the entries under its own `p{n}:` prefix like its other
offsets; they are equal across processes that committed the same step, because the
step-end frames had already merged the observations. `$watermark` is a reserved input
name, as `$step` is — that name and no other: an input called `$watermarks` or
`$watermark-feed` is an input, and its position is restored like any input's. (As first
built, `Start()` skipped every marker entry that *started* with `$watermark`, so such an
input lost its committed position at each restart and applied again whatever its source
re-sent. Nothing is skipped by how a key starts now; the clock's keys cannot be an
input's, because the one is refused as a name and the others contain `#`.) Cost: one
dictionary entry per source per commit, nothing per step.

**The deadlines.** A fourth store kind, `IDeadlineStore<TKey>`
(`IStorageProvider.CreateDeadlineStore`): key → deadline in ticks, with `AddAll` (add the
keys not held, asking for a deadline only for those), `Set`, `Remove` and
`TakeDue(upTo, into)`. `EvictionTracker` — every `EvictWhen` and window eviction in
`Join`, `AntiJoin`, the aggregates, `TopK`, `Min`/`Max`, keyed `Distinct`, the table
`Join`s and the remembered `Lookup` — keeps its keys in one, named
`{node}.deadlines.w{n}`; so does `WindowCloseOperator` (the windows still to announce:
`OnClose`, `WindowCloses`) through the same tracker, and `SessionWindowOperator` (a
session's deadline moves, so it uses `Set` and `Remove`). The stores are created through
`OperatorContext.StateName`, so they are in the state manifest like any other.

A deadline is staged with the step that set it and committed with the checkpoint that
covers that step, like every other store's writes. What each provider does:

| provider | deadlines held as | at a restore |
|---|---|---|
| `InMemoryStorageProvider` | a dictionary and a priority queue; a queue entry whose key has since moved or gone is stale: dropped off the front at every removal, skipped when taken, and the queue rebuilt from the map when stale entries outnumber the live ones, so the queue holds at most twice the map's entries and 64 more | nothing survives (the provider does not checkpoint) |
| `InMemoryCheckpointStorageProvider` | the same, written through, with the deadline each touched key had at the last commit kept beside it for `Recover()` | rolled back to the commit |
| `ChangelogStorageProvider` | the same structure inside a `ChangelogStore`: one record per key (`entry = key → deadline`), a tombstone when the key closes; `HeldInstances` as for the other stores | built from the store's bytes when the circuit creates the store, as every store is; a **follower that holds the store typed applies each record into the map and the queue**, so a warm standby's takeover starts with the order already built — and with a queue the size of the leader's (below) |
| `RedisStorageProvider` | a hash `{prefix}:{store}` (key → exact deadline) and a sorted set `{prefix}:{store}:due` scored by deadline / 1,024 ticks | nothing is read. `TakeDue` asks Redis only once the watermark reaches a remembered lower bound of the committed scores |

The Redis score is coarser than the deadline because a sorted set's score is a double
and a tick count of this century is beyond a double's exact integers; a range read is
therefore a superset, and the hash's exact value decides. The lower bound never passes
an entry whose removal is only staged, since a rollback makes it due again.

**A follower's queue, which nothing takes from.** A leader's queue loses its stale
entries as `TakeDue` dequeues them. A follower never calls `TakeDue`: it applies the
leader's records, a value as `Set` and a tombstone as `Remove`. As first built `Remove`
touched only the map, so a warm standby's queue gained an entry for every deadline
record it applied and lost none — 400,000 entries after 20,000 steps of a windowed
pipeline whose leader held 60, 24 MB that the state's size estimate did not show (it
counted the map's entries for the queue), and a first step after the takeover that
dequeued all of it (36 ms where the steps after took 0.05 to 0.08; the review's
measurement, on a copy of that build). The heap store now keeps the bound itself, at
every write that leaves an entry stale: it drops stale entries off the front — where
they are, when keys go in the order of their deadlines, as a leader's `TakeDue` and a
follower's tombstones make them go — and when stale entries elsewhere outnumber the
live ones it rebuilds the queue from the map, which costs in proportion to the writes
that made them stale. The estimate counts the queue's own array. `DeadlineStoreTests`
holds the bound against a follower that stands by through 3,000 steps of opening and
closing windows (its queue equals the leader's map, not 60,000 entries) and against
removals out of order and keys that only ever move.

**No rebuild at start, and why not.** The alternative to a store was to rebuild each
operator's queue from the keys of its state at `Start()`. That walks every evicting
store at every start — the cost the layout work (above) had just taken out of a
takeover — and the stores have no enumeration to walk with. With the deadlines in a
store, a restore costs what restoring any store costs, and a warm standby pays nothing
at the takeover.

**What a per-record path pays.** `EvictionTracker.Track(key)` appends the key to a
per-step list; `DrainClosed()`, which every evicting operator already called once per
step on every way out, hands the list to the store in one `AddAll` and then takes what
is due into a list the tracker owns. On the heap that is one dictionary probe per delta
row where there was one hash-set probe, and no allocation per step where the old
tracker allocated a list on every step in which something closed. On the changelog a
key costs one more record when it opens and one tombstone when it closes, per operator
that bounds it. On Redis a step costs one `HMGET` for the keys not seen since the last
commit.

**The remembered `Lookup` tracks facts, not keys**, because its state is per fact: the
grouping's policy closes a key, and every fact remembered under it goes then. A fact
that is taken back whole has nothing left to bound, and `EvictionTracker.Forget` gives
its deadline back at once — as first built it did not, and a key whose facts are
corrected (the upsert's shape: retract the old fact, assert the new) kept a deadline,
and a changelog record keyed by the whole fact, for every version the fact ever had
until the key closed: 5,000 deadline entries beside one remembered entry after 5,000
corrections of one order (the review's measurement). It is one entry per fact
remembered now, written when the fact is asserted and tombstoned when it is taken back
or its key closes; a fact held with a weight above one keeps its deadline until the
last of it goes.

**The manifest: format 7.** A circuit with event time adds `eventtime={sources}`. What
happens to an existing checkpoint:

- **A format-6 checkpoint of a circuit that uses event time is refused**
  (`CircuitStateMismatchException`): it holds neither the watermark nor a deadline for
  anything that was open, so resuming it is exactly the fault this step removes. Start
  it from an empty state store and replay its inputs (in the service: `state=reset`).
  The deadlines could be rebuilt from the state's keys, but not the watermark, and a
  rebuilt circuit that admits one late record has already reopened a window.
- **A format-6 checkpoint of a circuit with no event time resumes as it is**, and its
  next commit writes format 7: for such a circuit the two formats hold the same things.
- **A changed number of `WithEventTime` sources is refused**: the committed maxima are
  per source by position. Adding the first source to a circuit that had none, or
  removing the last, is not: there is nothing to misassign.
- Adding `EvictWhen` to an operator that already has state is accepted, as any new
  store is, and starts with no deadline for the keys already there: those close only if
  a record touches them again. That is unchanged, and now the only way to get it.

A checkpointing provider that does not implement `CreateDeadlineStore` is refused at
`Build()` by the interface's default (`NotSupportedException`); a provider that does not
checkpoint gets the heap store from it.

**Tests** (`tests/Incremental.Tests`).

- `EventTimeRestartTests`: one seeded workload run twice, uninterrupted and killed at
  random steps, over six definitions — tumbling, hopping and session windows with
  linear, non-linear and `Min`/`Max` aggregates and `OnClose`; windowed `TopK` and a
  window-sticky regroup; stream joins under `EvictWhen` with two event-time sources
  (`Join`, `LeftJoin`, `AntiJoin`, `Distinct`, `TopK`, `WindowCloses`, `Sum`); a table
  `Lookup` and `Join` under `EvictWhen` — at 1 and 4 workers and both commit cadences,
  on four backings: the in-memory checkpoint provider, the changelog read cold, a
  changelog follower that built its circuit before the records came and is promoted,
  and Redis from a provider that has read nothing. Every batch carries a record 5–20 s
  behind the watermark, so the first step after each restore has one. Asserted: every
  step's output changes equal the uninterrupted run's (and a step run again says what it
  said the first time); the final state is equal entry for entry (by count on the
  in-memory provider, which cannot list itself); the committed offsets, clock included,
  are equal; a cold read of the log equals the state. Then 50 restores in 150 steps on
  ten definition/backing pairs, with the state measured every 25 steps; a late record
  as the first thing after a restore; keys open at a restore and never touched again;
  the two format rules; a provider without deadline stores.
- `DeadlineStoreTests`: the four providers against a dictionary through random adds,
  sets, removes, takes, commits, rollbacks and reopens (cold and by promoting a follower
  that held the store typed); one record per key on the changelog however the key is
  written; no Redis command while nothing can be due.
- Mutations: 23 in the core and the Redis store, all killed by these two classes
  (enrichment-service.md, *As built*, S7, lists them).

**Not done.**

- The deadline of a key is not re-derived when the definition's `closesAt` changes
  under a kept state: a key keeps the deadline it was given when it opened.
- A dead-lettering policy does not cover `closesAt`: one that throws faults the step,
  as before, though now at the end of the operator's step and not at the row.
- The cluster path (several processes) restores the clock per process from its own
  marker; that is reasoned from the code and covered by no test of a clustered restart
  with event time.


## As built: broker failures (2026-11-11)

The review after round two asked why a leader stalled for 30 to 50 s when a broker went
away, why a rolling restart of the brokers moved the leadership in about one run in
five, and what had once left both nodes fenced. Each has a cause that was read from a
log or reproduced. The runs are on the `kafka3` compose profile (three brokers, each
also a controller, replication factor 3, `min.insync.replicas` 2); the tables of
results are in enrichment-service.md, *As built*, S7.

### What the stall was made of

Read from the Kafka client's debug log of a leader (`debug=eos,broker,protocol`, through
`INCREMENTAL_KAFKA_CLIENT` and `INCREMENTAL_LOG_LEVEL=Debug` on the test's node
process) and from the brokers' own logs. One 36 s stall was three things one after
another, and only the first is Kafka's.

1. **The brokers give a dead broker nine seconds.** A broker that is killed stays the
   leader of its partitions, a member of every replica set it was in, and the
   transaction coordinator of the ids it held, until the controller has missed its
   heartbeats for `broker.session.timeout.ms` (9 s). Until then a write with
   `acks=all` to any partition it replicates waits. This is the floor: 7.7 to 10.8 s of
   silence in the sink in the ten kills where the broker lost was not the active
   controller.
2. **If the broker was the active controller, the nine seconds start late.** The other
   controllers elect one of themselves (about three seconds: a two-second fetch timeout
   and an election), and the new controller gives every broker a fresh session — the
   dead one included. 12.6 to 15.1 s in the sink in nineteen of twenty such kills. A cluster whose controllers are not its
   brokers (MSK's are separate) does not have this case for a broker loss.
3. **The client then sat out its own delays.** Three of them, all defaults of
   librdkafka that suit a batch job:
   - `reconnect.backoff.max.ms` is ten seconds. A broker that restarts refuses
     connections for long enough to double the delay up to that; the log shows
     `TxnCoordinator/2: Delaying next reconnect by 9999ms` and then no
     `FindCoordinator` for nine seconds, with the coordinator long since moved.
   - `socket.timeout.ms` is sixty seconds. A broker that stops answering without
     closing its connections — a frozen process, a host or a zone that drops off the
     network — is found out only by a request timing out, one connection at a time:
     the writer waits once for a partition's old leader and once more for the
     coordinator. With one broker paused for a minute (`docker pause`) and the
     defaults, the standby took over at the ceiling in three runs of six and the sink
     was silent for up to 61 s.
   - `topic.metadata.refresh.interval.ms` is five minutes. A client that is idle while
     a broker stops answering learns nothing: a sink's own producer, idle while the
     writer waited, sent its next batch to the old leader and waited in its turn.

`KafkaClientDefaults` is what every client of this package and of the service now
starts from (the log's producer, consumers and admin client, sources, sinks, the blob
store): `reconnect.backoff.ms` 50, `reconnect.backoff.max.ms` 1000, `socket.timeout.ms`
6000, `request.timeout.ms` 5000 (producers; it stays under the socket timeout so that a
broker waiting for a dead replica answers "timed out" and the request is made again on
the same connection), `socket.connection.setup.timeout.ms` 5000,
`topic.metadata.refresh.interval.ms` 2000. A caller's `ClientConfig` is applied after
them and wins. An admin client is not given the socket timeout: its calls carry their
own (a topic's creation the operation timeout, a leader lookup half a second and then
longer), and six seconds cut all of them — a node restarted while the brokers were
between controllers could not create its topics and ended. `EnsureTopicAsync` now also
asks again while the brokers cannot answer.

With these settings, one broker paused for a minute or for twenty seconds: 10.6 to
15.7 s of silence in the sink in nine runs, and nobody took over. (A socket timeout of
twelve seconds, tried first, gave 26 s in two runs of three: twice twelve.)

### What the test environment added

Three things lengthened the stalls of the earlier runs and were the environment's, not
the service's. They are fixed in `docker-compose.yml` and in the tests, and they are
worth knowing before believing a number from a container cluster.

- **A container that is killed and started again came back at a new address.** The
  other brokers' controller clients went on connecting to the old one until
  `socket.connection.setup.timeout.ms` (ten seconds). With the next broker gone the two
  that were left could not reach each other, and the controller quorum had no leader
  for twelve seconds. The brokers now have fixed addresses on a network of their own.
- **A stopped container's name takes five seconds not to resolve**, on the one thread
  a controller talks to its peers on, and the controller listener advertised the
  container's host name. Two surviving controllers took eight seconds to agree. The
  brokers now name each other by address.
- **"The cluster is whole" was believed from one broker.** A killed broker is still
  listed, and still in every replica set, for the nine seconds above; asked through a
  survivor the cluster looked whole the moment the container was started, and the next
  broker was killed while the last was still recovering — two of three down, which is
  not the loss of a broker. Most of the "leadership moved when a broker was killed"
  runs were this. `ClusterWholeAsync` now asks each broker, one at a time.

### The leadership rules

- **A takeover is given up when the leader is heard — until the transactional id is
  taken, and not after.** Between the decision and the moment `InitTransactions`
  returns, the node's watch of the leader topic is still reading. A heartbeat from
  another node calls the acquisition off (`KafkaLeadership.Heard`): the leader was slow,
  its brokers were, and it is committing again. Not on a manual takeover, which is the
  operator's word against the leader's. And not once `InitTransactions` has returned:
  from that moment the leader's epoch is gone, whatever it committed a moment before,
  and the acquisition goes through — reads the term, commits its own, leads
  (`KafkaLog.AcquireWriterUnlessAsync`: the token that calls it off is looked at only
  until then). As first built the token was honoured to the end, so a heartbeat read
  after the fence stopped the takeover there: the standby stood down, the fenced leader
  ended at its next transaction, and nobody led until the standby's wait had passed and
  a silence after it. That is what the third review read in a node's log after a
  two-broker outage, and `KafkaLeadershipTests` now holds the other outcome at both
  points after the fence.
- **A takeover given up for a heard leader holds nothing back.** The silence goes on
  counting from the record that was heard, and the node may take over again as soon as
  that silence is long enough — it does not wait the one to two takeover timeouts of a
  takeover lost to a fence (next but one). The reason is the one case that is left: the
  acquisition is called off between two one-second slices of `InitTransactions`, and a
  request for the id that had already left the client can still land, although the
  client is put away. Whether it did cannot be asked. If it did, the leader's next
  transaction fails and it ends, and the standby answers its silence as it answers any
  dead leader's — one takeover timeout and a takeover, which is the failover the
  design budgets for, not two or three timeouts more.
- **A cancelled or fenced acquisition puts the client away.** `KafkaLog.AcquireAsync`
  replaces the producer and fails with `KafkaLogNotAcquiredException`; the log is a
  reader again. The client must go because a fenced one is finished, and because a
  cancelled `InitTransactions` is not cancelled inside the client: it goes on asking
  and would fence whoever leads when the coordinator answers. The term is committed by
  the acquiring call itself, and the writer thread starts only once the log is the
  writer, so a lost acquisition has nothing to stop.
- **A takeover lost before the node is the writer ends as Standby, not Fenced**
  (`Promotion.Lost`, `BackToStandby`). Nothing has been promoted at that point — the
  acquisition is the first thing a takeover does. A node whose acquisition was fenced
  waits the takeover timeout and a random part of it more before it may try again, so
  that two nodes that fenced each other do not do it in step. "Fenced is final" still
  holds for every node that has been the writer.
- **At the ceiling, a standby asks the brokers before it takes over from a leader that
  answers.** `KafkaLog.ProbeWritePathAsync` looks at the leader topic's partition and
  the partition of `__transaction_state` that holds the log's transactional id (Java's
  `String.hashCode`, as the brokers place it): no leader, a leader that does not
  answer, or fewer replicas in sync *that answer* than `min.insync.replicas`, and
  nobody can commit — the standby waits (`TakeoversHeld`), and for two takeover
  timeouts after the brokers last said so: a leader they held up has to find them again
  before it can be heard, and without that pause the standby took over in the instant
  they returned.

  "That answer" is what the third review's runs added. Who leads a partition and who
  is in sync is metadata, and metadata is the controller's: with no controller quorum
  nothing takes a dead broker off the lists. Two of three brokers killed in one
  command, and the survivor went on reporting `leader 1, isr [1,2,3]` for the whole
  outage (when it answered at all: a description asked of the controller timed out
  "waiting for controller" in two polls of three) — so the probe read "writable", and
  the standby took over at the ceiling into a cluster that could commit nothing, in
  three of the review's four 40-second outages. The lists are no longer believed by
  themselves. The partition is read from any broker's metadata, not the controller's,
  and every broker it names is asked for its own configuration (`DescribeConfigs` on
  the broker resource), which the client sends to that broker and no other: an answer,
  a refusal included, is a broker that is there; a second and a half of silence is not.
  The topics' `min.insync.replicas` is the one thing still asked of the controller, so
  it is asked when the standby starts to watch and kept.
- **A decision to take over is looked at again after the questions it took.** The
  reason for a takeover is found on a silence, and then the peer is probed, the brokers
  are asked and the leader topic's end is read — which takes seconds exactly when
  brokers are only just back and a leader they held up is about to be heard. As first
  built the decision stood on the silence measured before the questions. With the
  broker-by-broker question above taking a second or two, the first runs of it showed
  what that meant: a standby that had held correctly through a whole outage logged
  `taking over (ceiling; leader silent for 00:00:00.0200000)` and fenced a leader it
  had heard twenty milliseconds before, in two runs of six. The silence is measured
  again once the answers are in, and a leader heard meanwhile is a leader.

### A leader does not outlive its transaction timeout

With no quorum of brokers a leader's open transaction cannot be taken or committed. The
client gives its records `transaction.timeout.ms` (`KafkaLogOptions.TransactionTimeout`,
60 s): then they fail, the transaction has to be aborted, and the abort of a
transaction the coordinator never managed to register is refused
(`INVALID_TXN_STATE`), which the client counts as fatal. The writer is finished and the
node ends with exit code 3, with nobody having taken anything over. That is what the
70-second test shows in two runs of three (the service design, S7, *The third
review's findings*, series 2): the standby holds at the ceiling while the leader
answers its probe; the leader's transaction times out at sixty seconds; its abort is
refused — as `INVALID_TXN_STATE` once the brokers were back in one run, as "fenced by
a newer instance" from the one broker left in the other, with no node having taken the
log — and the leader ends; the standby, which no longer has a leader to wait for,
takes over within a fifth of a second and leads when the brokers let it. In the third
run the abort was accepted and the leader kept its term; what decides between them was
not established. An outage shorter than the timeout ends with the same leader — six
runs of six at forty seconds, one of which ran into the timeout because the brokers
took twenty seconds to commit again, and had its abort accepted. A deployment that must ride out longer
ones raises `TransactionTimeout` (the brokers allow up to `transaction.max.timeout.ms`,
fifteen minutes by default); what it pays is that a writer that died with a transaction
open blocks readers of its partitions for that long if nobody takes over, which a
standby does in seconds.

### How both nodes ended fenced

Once in 21 rolling restarts of round two. The log of that run has it, and
`StaleEpochRequestTests` reproduces it on one broker by sending the request itself.

The old leader's transaction had lost a batch to a change of partition leader
(`OUT_OF_ORDER_SEQUENCE_NUMBER`, "requires epoch bump"). The client recovers from that
by asking the coordinator for a new epoch: `InitProducerId` carrying the producer id and
epoch it has (KIP-360). Meanwhile the standby reached the ceiling, took the
transactional id — which makes that request stale — and opened its first transaction,
the one that commits its term. The coordinator of Kafka 3.9 refuses a stale request
only when no transaction is open on the id. When one is, it compares the producer id,
not the epoch, treats the request as a newcomer's, aborts the open transaction and
raises the epoch. The standby's `EndTxn` failed and its abort was refused as fenced; the
old leader's request was answered "concurrent transactions", and its retry was refused
as fenced too. Two fenced nodes, an epoch nobody held, and no leader until a process was
started by hand.

The broker's behaviour is not ours to change. What changed is that the standby was
never the writer when this happened to it, so it is not finished: it stands by again,
and when the silence has lasted it takes the id with a new client, which nothing stale
can touch. In the test the same log acquires at term 1 on its second attempt.

### The supervisor

A node that has been the writer and is fenced, or has resigned, still ends its process
(exit code 3 or 4; 5 when it could not lead). Something has to start it again, or one
takeover later the deployment has one node. That was said in the guide and done by
nothing: the tests restarted nodes by hand. `NodeSupervisor` (in `Incremental.Service`)
is the contract in code — start the process again after every exit but 0 and 2, at once
after 3 and 4, with a doubling delay while it keeps ending within a minute, pass
SIGTERM on and wait — and the example's Kafka mode, the scale rig and the broker tests
run their nodes under it. A platform's own supervisor (systemd `Restart=always`, a
Kubernetes pod, an ECS service) is the same contract and is what production should use.

"At once after 3" is right only if 3 means what it says: another node leads. A takeover
that fails after the node has taken the log — its catalog, its storage or the start of
its circuits failed — also ends the node as `Fenced` (it closes its own writer, or it
would heartbeat for a node that leads nothing), and as first built that too was exit
code 3. Two nodes with the same deterministic failure then took the log from each other
every few seconds for ever: restore the tables, wait out the silence, take the log,
commit a term, fail, exit 3, start again after 200 ms. `LeadershipStatus.TakeoverFailed`
tells the two apart — set when the takeover failed and nobody had fenced the log — and
the service exits with 5 for it, which the supervisor slows down like any process that
keeps failing early.

### Not done, not known

- One kill of the active controller in forty silenced the sink for 26 s, the whole
  time the broker was away, with the leadership not moved. The brokers' logs show their
  own clients waiting out connection set-up timeouts to the dead broker; what the
  service's client was waiting for is not established, and twenty kills made to catch
  it with the client's log on did not show it (enrichment-service.md, S7).

- Nothing was run on MSK. The nine seconds are `broker.session.timeout.ms`, which is
  the cluster's; whether MSK lets it be lowered was not checked.
- A broker that stops answering is still found out one connection at a time. The
  worst case measured is in S7's table; a sink that writes through the log's producer
  (`"producer": "log"`) has one client fewer to find out.
- The write-path probe is as stale as the controller. A standby could instead prove the
  path by writing — a record of its own to the leader topic through a plain producer —
  which was not built: it would be a write outside the fenced producer, and the leader
  topic's end moving is what tells a standby that a leader inside a long transaction is
  alive.
- `InitTransactions` is called in one-second slices so that a takeover can be called
  off. The client documents the call as resumable; a broker that answers slower than a
  second was not tried.
