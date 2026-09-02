# Timely dataflow as a general answer to exchange synchronization

Research note, August 2026, written against primary sources (cited per claim) to answer
two questions: how does Timely Dataflow (Naiad) achieve general non-blocking parallel
execution, and what would Incremental — a DBSP-style step-synchronous engine — need to
adopt to escape the BSP recurrence we measured (every all-to-all exchange waits on the
slowest producer)?

Context this note assumes (measured on this repo, Aug 2026): on the full showcase
pipeline, 42–57% of aggregate worker-step time at 4–8 workers is exchange wait, and the
point-to-point (seq-mailbox) prototype showed the wait is **structural producer skew**
— `T(e+1) = T(e) + max_w segment(w)` — not barrier mechanics.

Sources:

- **[Naiad]** Murray, McSherry, Isaacs, Isard, Barham, Abadi. *Naiad: A Timely Dataflow
  System*, SOSP 2013. <https://dx.doi.org/10.1145/2517349.2522738>
  (PDF: <https://www.microsoft.com/en-us/research/wp-content/uploads/2013/11/naiad_sosp2013.pdf>)
- **[TDbook]** Timely Dataflow mdBook. <https://timelydataflow.github.io/timely-dataflow/>
- **[TDdocs]** Rust `timely` API docs: `progress::reachability`
  (<https://docs.rs/timely/latest/timely/progress/reachability/index.html>),
  `worker::Worker` (<https://docs.rs/timely/latest/timely/worker/struct.Worker.html>)
- **[DBSP]** Budiu, McSherry, Ryzhyk, Tannen. *DBSP: Automatic Incremental View
  Maintenance for Rich Query Languages*, PVLDB 16(7), 2023. <https://arxiv.org/abs/2203.16684>
- **[Falkirk]** Gog et al. *Falkirk Wheel: Rollback Recovery for Dataflow Systems*,
  SoCC 2021. <https://arxiv.org/abs/1503.08877>
- **[Feldera]** Checkpoints & fault tolerance docs (<https://docs.feldera.com/pipelines/fault-tolerance/>)
  and engineering blog (<https://www.feldera.com/blog/fault-tolerance-technical-details>)
- **[Mz]** Materialize: virtual-time blog (<https://materialize.com/blog/virtual-time-consistency-scalability/>),
  formalism (<https://github.com/MaterializeInc/materialize/blob/main/doc/developer/platform/formalism.md>),
  persist design (<https://github.com/MaterializeInc/materialize/blob/main/doc/developer/design/20220330_persist.md>)

---

## 1. The model: timestamps and optional coordination

Timely dataflow is "a directed graph in which stateful vertices send and receive
logically timestamped messages along directed edges" [Naiad §2]. Its three pillars
[Naiad §1]: structured loops with feedback; "stateful dataflow vertices capable of
consuming and producing records **without global coordination**"; and "notifications for
vertices once they have received all records for a given round of input or loop
iteration."

Every message carries `Timestamp : (epoch e, loop counters ⟨c1..ck⟩)`; ingress, egress,
and feedback vertices are the only ones that alter timestamps (append 0, strip, +1
respectively) [Naiad §2.1]. A vertex implements `OnRecv(e, m, t)` (data, any order) and
`OnNotify(t)` (completeness), with the guarantee that "`v.OnNotify(t)` is invoked only
after no further invocations of `v.OnRecv(e,m,t′)` for `t′ ≤ t` will occur" [Naiad §2.2].

The key architectural inversion relative to our engine: **synchronization is something an
operator requests per timestamp** (`NotifyAt`), not something the runtime imposes per
edge. Vertices that never request notification run fully asynchronously: "None of these
operators [Where, Concat, Distinct, Join inside a loop] invokes NotifyAt, and subgraphs
using only these will execute asynchronously (without coordination) on Naiad" — e.g.
"`Distinct` outputs a record as soon as it is seen for the first time" [Naiad §4.2]. The
Rust incarnation states it the same way: workers "are welcome to impose a more
synchronous discipline using progress information … but they are not required to do so"
[TDbook, progress tracking chapter].

## 2. Progress tracking: what replaces the barrier

The scheduler reasons about *pointstamps* `(t ∈ Timestamp, l ∈ Edge ∪ Vertex)`. For each
active pointstamp it keeps an **occurrence count** (outstanding messages/notification
requests bearing it) and a **precursor count** (active pointstamps that
*could-result-in* it, computed from path summaries over the graph). "When an active
pointstamp p's precursor count is zero, there is no other pointstamp in the active set
that could-result-in p, and we say that p is in the **frontier** … The scheduler may
deliver any notification in the frontier" [Naiad §2.3]. Rust timely implements the same
idea in `progress::reachability::Tracker`, propagating "the potential for capabilities to
reach nodes in a directed graph" via per-port path summaries [TDdocs].

Distributed, this becomes a protocol, not a barrier: each worker keeps *local* occurrence
counts and broadcasts `(pointstamp, δ)` updates to all workers, FIFO per sender; the
safety property is that "no local frontier ever moves ahead of the global frontier, taken
across all outstanding events in the system" [Naiad §3.3]. A naive version "would
broadcast every progress update"; Naiad needs two optimizations to make it practical —
projecting pointstamps onto the logical stage/connector graph (not the physical vertex
graph), and accumulating updates in local/cluster-level buffers before broadcasting —
which together "reduce the volume of protocol traffic by one or two orders of magnitude"
[Naiad §3.3, §5.3]. This is the protocol's real cost profile: it is cheap only after
deliberate batching, hierarchy, and even UDP-optimistic delivery plus a modified
eventcount wake-up primitive [Naiad §3.3].

**Does this dodge the BSP recurrence?** For a *single* timestamp, no — and the paper is
explicit. "Before delivering a notification, a Naiad worker must know that there are no
outstanding events **at any worker in the system** with a pointstamp that could-result-in
the pointstamp of the notification" [Naiad §3.3]. For loops: "No iteration can proceed
until all notifications from the previous iteration have been delivered" [Naiad §5.2].
A notification-requiring consumer downstream of a data-dependent exchange still waits,
logically, for the slowest upstream producer of that timestamp. What timely buys instead:

1. **Overlap across timestamps.** "The resulting model supports concurrent execution of
   different epochs and iterations" [Naiad §2]. Waiting for timestamp t's completion is
   not idle time; the worker processes t+1's (or another iteration's) data meanwhile.
   The wait for the slowest producer becomes a *throughput* bound (the pipeline drains at
   the bottleneck's rate), no longer a per-timestamp *latency* sum over exchanges.
2. **Coordination only where requested.** Linear/monotone segments never wait at all
   (§1 above); completeness is paid per notification, not per edge.
3. **No rendezvous mechanics.** Progress flows as asynchronous accumulated deltas;
   nobody parks at a barrier waiting for peers to *arrive* — only notifications wait,
   and only on the *information* that all inputs are complete.

What it does **not** buy: immunity to stragglers. Naiad's own coordination
microbenchmark — a cyclic graph exchanging *no data*, only completeness notifications —
has "median time per iteration … small at 753 µs for 64 computers (512 workers), but the
95th percentile results show the adverse impact of micro-stragglers" [Naiad §5.2]. The
paper devotes §3.5 to this: "transient stalls at a single worker can have a
disproportionate effect on overall performance … we view the resulting micro-stragglers
as the main obstacle to scalability for low-latency workloads", and it *rejects*
speculative/duplicate execution because "Naiad maintains mutable state … we expect the
cost to outweigh the benefits" — its mitigations are avoidance (Nagle/retransmit tuning,
lock backoff granularity, GC tuning), not architecture [Naiad §3.5]. In other words: for
per-step latency, timely faces the same `max_w` term we measured; its answer is to keep
the pipeline full so the term prices throughput, not latency.

## 3. Scheduling: activations, not lockstep

A Naiad worker "is responsible for delivering messages and notifications to vertices in
its partition"; when several actions are runnable it picks (messages before
notifications, to shrink queues) [Naiad §3.2]. `SendBy` to a same-worker vertex may be
delivered *immediately* as a nested call — operator fusion for free — with optional
bounded re-entrancy for cyclic graphs [Naiad §3.2]. Rust timely: `Worker::step`
"gives each dataflow operator a chance to run", `step_or_park(timeout)` parks the thread
when no work exists; operators are queued by *activations* (message arrival, frontier
change, or explicit self-activation via `activator_for`) rather than swept in topological
order [TDdocs, Worker]. Under skew this means a lagging worker delays only consumers of
*its* data at *notified* timestamps; every other worker keeps executing whatever is
runnable — there is no circuit-wide convoy point. The contrast with our
`RunWorkerStep` (every worker sweeps every node, exchanges are unconditional sync
points) is total.

## 4. Iteration

Loop counters in the timestamp plus the frontier machinery *are* the fixed-point
detector: a loop is done for epoch e when no active pointstamp with prefix e remains
inside the loop context, at which point the frontier passes the egress and downstream
notifications deliver [Naiad §2.1, §2.3]. There is no explicit per-iteration vote; our
two-barrier global flag reduction in `RunLoop` is replaced by the same progress protocol
that handles everything else, and different epochs' iterations may overlap. Naiad
even generalizes notification into guarantee-time vs capability-time to support
"bounded staleness" loops that run several iterations ahead of completeness [Naiad §3.1
discussion]. The cost is that every message circulating a loop bears the loop-counter
dimension, and progress state is tracked per (stage, connector) pointstamp — acceptable
in Naiad only because of the projection optimization [Naiad §3.3].

## 5. Consistency and checkpointing in practice

Determinism: timely guarantees that once the frontier passes t, the outputs at t are
final; but *system-level* consistent snapshots are a separate problem, and it is
instructive what each system actually does:

- **Naiad**: stop-the-world. "All processes first pause worker and message delivery
  threads, flush message queues … and finally invoke Checkpoint on each stateful
  vertex"; recovery reverts "all live processes … to the last durable checkpoint"
  [Naiad §3.4]. That is, the flagship asynchronous system checkpoints by *reimposing a
  global barrier* and draining in-flight work.
- **Falkirk Wheel**: fixes exactly that, by assigning "a logical time to each event such
  as a message delivery" and doing selective logical-time rollback — an order of
  magnitude lower response latency than Naiad's batch-tuned fault tolerance [Falkirk,
  abstract]. It is a research design implemented on Naiad; substantial machinery.
- **Rust timely**: no built-in fault tolerance at all (nothing in [TDbook]/[TDdocs]).
- **Materialize**: sidesteps dataflow checkpointing entirely. Inputs are made *definite*
  (durably timestamped in `persist` before use), dataflows are deterministic functions of
  definite inputs and are simply re-derived on failure; virtual time — "events should be
  timestamped prescriptively rather than descriptively" — is the consistency mechanism
  [Mz]. State lives outside the dataflow.
- **Feldera** (the production DBSP engine): checkpoints are "a consistent snapshot of the
  system's state, including computation and the input and output connectors", taken
  between steps — when a checkpoint is requested at an inconvenient input position, the
  runtime "executes steps … until they advance to a point at which a checkpoint is
  possible"; exactly-once adds an input journal and output deduplication by sequence
  number [Feldera]. Step-aligned commits — the same model as ours.

Takeaway: **no timely-family system checkpoints an asynchronously drifting dataflow
in-band**. They stop the world (Naiad), re-derive from definite inputs (Materialize),
align to step boundaries (Feldera), or carry a research-grade rollback design (Falkirk
Wheel). Incremental's step-atomic checkpoint and the 0.2 "batch = one step's delta"
output contract are not what timely would delete; they are what Feldera ships and what
Naiad falls back to.

## 6. The DBSP contrast, and what "adopting timely" can mean here

DBSP is *semantically* synchronous by construction: "all our operators are
'synchronous': they consume and produce data at the same rate", and time "is not the
wall-clock time, but essentially a counter of the sequence of transactions" [DBSP §2,
§1]. Timely/DD attach a partial order of timestamps to data and let execution float;
DBSP fixes a total order of steps and makes each step a transaction. Nothing in DBSP
*requires* the executor to be lockstep, though — synchrony constrains the per-step
input/output relation, not the schedule. That gap between semantics and schedule is
where a general solution lives.

Because Incremental's circuit is **static**, with structured loops, sharded state, and a
fixed all-workers-run-all-nodes discipline, the full timely apparatus collapses:

- *Could-result-in / path summaries* are compile-time facts of the circuit graph. No
  runtime reachability computation is needed [cf. TDdocs reachability — it exists
  because timely builds graphs dynamically].
- *Pointstamps* collapse to `(step, node)` — and inside a Recurse region,
  `(step, node, iteration)`.
- *Occurrence counting* collapses to: consumer of exchange e at step s may merge when
  all W producers have published their (e, s) fragment. That is precisely the
  seq-mailbox prototype already in the tree, generalized from "current step only" to
  "k steps in flight" (k-buffered mailboxes instead of double-buffered).
- *The broadcast protocol* is unnecessary intra-process (shared memory + release/acquire
  seqs), and inter-process it degenerates to the per-(step, node) dense frame sequence
  the cluster transport already ships.

A minimal timely-style adoption is therefore **bounded step pipelining over the existing
circuit**, not a rewrite:

1. Keep step = timestamp = transaction. Allow up to k steps in flight.
2. Per-operator state forces sequentiality *per node per worker* (state after step s
   feeds step s+1), so a worker sweeps nodes in topological order per step as today, but
   different workers may be at different (step, node) positions; exchanges consume by
   completion count, not rendezvous.
3. Outputs commit in step order as the trailing "completed step" counter (the frontier,
   now a single integer) advances — the 0.2 contract is untouched, including
   table-shaped bootstrap.
4. Checkpoint = stop admitting steps, drain the ≤ k in-flight steps, snapshot — Feldera's
   model, cost bounded by k × step time. No Chandy-Lamport.
5. Recurse: keep the intra-step global reduction (it is semantically a global decision;
   even timely's frontier is one), but with seq/counter-based votes so it composes with
   drift; iterations of *different* steps at the same region cannot overlap anyway
   (state sequentiality), which keeps it simple.
6. Sharded-state and stable-routing invariants: unchanged.

Two further timely lessons apply *within* a step and attack the measured skew directly:

- **Selective coordination** [Naiad §4.2]: only "strict" consumers (per-step aggregates,
  window closes) need the *complete* step delta. A consumer whose downstream segment is
  linear (Select/Where/flat joins over deltas — most of a DBSP circuit; DBSP §1 notes
  "almost all relational and Datalog queries are based on linear operators") can merge
  producer fragments *as they arrive*, since linear operators commute with addition.
  That converts "wait for slowest producer, then work" into "work as fragments land" —
  it shortens the critical path itself, which pipelining alone does not.
- **Micro-straggler avoidance** [Naiad §3.5]: Naiad's list — timer granularity, lock
  backoff, GC pauses, packet loss — maps directly onto our step path (e.g. checkpoint
  staging riding the worker step). Timely's authors treat these as things to *remove
  from the path*, not to synchronize around.

## 7. Assessment against the measured evidence

The measured facts were: 42–57% of aggregate worker time is exchange wait on the full
pipeline; the p2p prototype left it nearly intact; the micro-circuit (2 exchanges)
improved at 8 workers. The sources say, consistently, that timely would change this as
follows:

- **Per-step latency: essentially unchanged.** For one timestamp through a
  notification-requiring exchange, timely waits for the slowest producer too [Naiad
  §3.3, §5.2]; its own data-free coordination round costs ~753 µs median at scale with
  straggler-dominated tails [Naiad §5.2] — the same order as our full-pipeline step p50.
  The exception is the linear-segment effect above, which genuinely shortens the path.
- **Throughput / utilization: the real win.** The 42–57% idle wait is exactly what
  concurrent epochs fill [Naiad §2]. With k steps in flight, throughput approaches the
  bottleneck (worker, node) rate instead of the sum of per-exchange maxima.
- **Hot-shard skew: not solved by anyone.** Naiad explicitly declines speculative
  execution because of mutable sharded state [Naiad §3.5] — the same constraint we have.
  Rebalancing/partitioning is the lever, orthogonal to the execution model.
- **Checkpoint/consumer contracts: no conflict.** The production DBSP engine commits at
  step boundaries and drains to checkpoint [Feldera]; Naiad itself pauses the world
  [Naiad §3.4]. Step-atomicity is compatible with pipelined execution via drain.

Ranked by evidence-to-effort, the general solution decomposes as: (1) take skew sources
off the step path (no model change; shortens `max_w` directly); (2) bounded step
pipelining on k-buffered seq mailboxes with a completed-step frontier (the timely
insight, radically simplified by the static circuit; converts residual skew from a
latency tax into a throughput bound); (3) linear-segment fragment streaming (timely's
OnRecv/OnNotify split, expressed through DBSP linearity; shortens the intra-step
critical path); (4) full dynamic operator scheduling with a runtime progress tracker —
the sources give no benefit for a static circuit that (2)+(3) do not already capture,
and Naiad's own protocol needed two orders of magnitude of optimization to be viable
[Naiad §3.3, §5.3]. This note is analysis, not a decision; the ordering is what the
cited evidence supports.
