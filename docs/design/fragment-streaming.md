# Linear-segment fragment streaming

Design sketch, August 2026; v1 shipped 2026-08-31, v2 (production at the head, cross-exchange
forwarding, the emitting join) 2026-09-01 — see "v2" below. Companion to `docs/research/timely-dataflow.md`, which
motivates the mechanism (findings 1 & 3): after the point-to-point exchange and the
land/publish staging work, the remaining multi-worker step cost is ~450 µs/worker of
dependency wait on the *owner chain* — each exchange segment typically has one busy worker
(key ownership rotates per segment), so the segments' work serializes. Pipelining across
steps fills that wait but does not shorten a step; the only lever that shortens the chain
itself is the Timely OnRecv/OnNotify split expressed through DBSP linearity: let work start
on *partial* data wherever the operator's algebra says partial data is safe.

This is the design the handoff asked to sketch before implementing: which operators declare
linearity, how streamable structure is identified at build time, what "partial merge" means
for `IExchangeStepOperator`, and how the fault/poison, phase-stats, and buffer-reuse
contracts extend.

## The model

A **fragment** is a partial delta: one addend of the Z-set sum that makes up a node's delta
for a step. Fragments exist only on the exchange path while a step is in flight; every
boundary the rest of the engine sees — operator `Step` inputs, output ports, checkpoints —
still carries exactly one complete delta per node per step. Fragment streaming changes
*when* work happens inside a step, never *what* a step computes.

Three roles, all per exchange node, all local to the existing sweep:

1. **Chunked publication (producer side).** A worker partitioning its input into an
   exchange may publish it as several fragments instead of one, each release-published
   under a monotonic per-(node, sender) fragment counter, with the existing per-execution
   sequence publication as the completion marker ("no more fragments from me for this
   execution"). Consumers can start on fragment 1 while the producer partitions fragment 2.
   Additionally, the maximal chain of *linear* single-consumer operators feeding the
   exchange is **fused** into the producer loop: the chain runs per chunk, so its work also
   overlaps consumers' work instead of strictly preceding the publication.

2. **Arrival-order consumption (consumer side).** A worker at an exchange consumes
   whichever sender's fragments are available (its own included), in arrival order, rather
   than sender order after a full rendezvous. Each fragment addressed to this worker is
   either accumulated into the merge ("partial merge") or handed to a fragment-consuming
   sink (below). The worker leaves the node when every sender has published completion and
   everything published has been consumed — the same completeness condition as today,
   reached with the merge (and possibly most of the sink's work) already done.

3. **Fragment-consuming sinks.** A stateful operator that is the exchange's sole consumer
   and whose per-step computation distributes over fragment addition may consume fragments
   as they arrive (`OnFragment`) and emit its complete output delta once, at its own sweep
   position (`Complete`). Emission stays at completion — output timing and content are
   exactly today's — but the operator's state/probe work overlaps the producers' tail
   instead of following it. This is where the owner chain actually shortens: the busy
   segment's work is mostly inside these operators.

What this deliberately is *not*: no operator emits anything downstream that a later fragment
could revise (no intermediate retract/insert churn — the v2 emitting join forwards only
per-fragment output that is already final), no dynamic scheduler, no change to step
atomicity, watermarks, checkpoint commit points, outputs, or the cluster path.

## Which operators declare linearity

Linearity here is the DBSP property specialized to our execution: an operator is **linear**
when running it on the fragments of a delta and summing the outputs equals running it on
the summed delta — including any state effects — so it can be applied per record batch in
any grouping. Declared via `IStepOperator.IsLinear` (default false):

| Operator | Declaration | Why |
| --- | --- | --- |
| `MapOperator`, `FilterOperator`, `FlatMapOperator`, `NegateOperator`, `WindowAssignOperator` | linear | Stateless and record-wise; weights pass through. These are the fusable chain links. |
| `PlusOperator` | linear | Record-wise; multi-input, so it never joins a fused chain in v1 (chains are single-input), but the declaration is true and future-proof. |
| `JoinOperator` | fragment sink | Bilinear, not linear: correct under per-fragment *probe-then-integrate* (each fragment probes the opposite store's current state, then integrates into its own), which counts every ΔL×ΔR pair exactly once as long as one thread orders the fragments — the worker does. Output per fragment is final (no retractions), accumulated in the operator and emitted at `Complete`. |
| `LinearAggregateOperator` | fragment sink | The aggregate is a group homomorphism, so fragments fold into the per-key scratch accumulator in any order; the state read/compare/emit/write pass runs once, at `Complete`. |
| `InputOperator` | not declared | Chain heads read its *slot*, which chunks the staged batch naturally; the operator itself is never fused. |
| `WatermarkOperator` | strict (v1) | Within a step it is a linear filter against the frozen watermark plus a commutative max observation, so it *could* stream — deferred, to keep v1's linear set trivially auditable. |
| `DistinctOperator`, `AggregateOperator`, `ExtremumOperator`, `TopKOperator`, `AntiJoinOperator`, session/window-close, snapshot, loop operators | strict | Non-linear (distinct, extremum, topK), user folds over full groups (aggregate), or presence logic (anti-join). They receive the complete merged delta at their sweep position, exactly as today — the partial merge just has it ready sooner. |

`AggregateOperator` and `AntiJoinOperator` could stream their *store writes* per fragment
and defer only the group recompute; that is a v2 note, not v1 — their comment about keeping
the allocating store variants applies to this fence too.

## Build-time identification

All analysis runs in the `Circuit` constructor, after operators are instantiated (worker 0's
instances answer type questions; every worker's are the same types), and only when
`FragmentStreaming` is on — which requires `PointToPointExchange` and therefore already
excludes clusters. Exchanges inside `Recurse` regions are excluded (they keep the current
p2p path): iteration counts are a global per-iteration decision and the flag-reduction
barriers bracket every iteration anyway, so there is no tail to overlap.

Per streamed exchange node `n`:

- **Consumer counts.** One pass over every node's `Inputs` plus every output registration's
  `NodeId` (output ports read `worker.Slots[node]` between steps, so an output is a
  consumer) yields each node's consumer count.
- **Fused chain.** Walk up from `n`'s input while the node is linear, single-input,
  single-consumer, and outside every loop region. The walk's nodes are marked
  `FusedInto = n`; the sweep skips them (their slot gets the operator's canonical empty —
  by construction every linear operator has one), and the chain runs inside `n`'s producer
  loop, per chunk. The chain head input is the slot of the first non-fused node, read whole
  — it is complete by sweep order.
- **Sink.** If `n`'s sole consumer implements `IFragmentSinkOperator`, `n` feeds it
  per-fragment (`OnFragment` with `n`'s input index at the sink) and materializes no merge;
  otherwise `n` accumulates the partial merge and its slot carries it. A sink node's own
  sweep position runs `Complete` instead of `Step`; its non-streamed inputs (e.g. a
  `Grouped()` side of a join, which arrives without an exchange) are passed to `Complete`
  from slots as usual.
- **Production mode (v2).** Where `n`'s fragments get produced is decided from the head `H`
  (`StreamPlan.Produce`):
  - `AfterHead` — the default: right after `H` runs in the sweep, from its now complete
    slot, chunked as below. Production moves as *early* as its input exists, so peers
    consume this worker's fragments while it runs the nodes between `H` and `n`.
  - `Forwarded` — `H` is read by nothing but this chain, and can hand its delta over before
    it is complete: an upstream streamed exchange (`plan[H].ForwardTo = n`) forwards every
    fragment addressed to this worker from its consume loop, through `n`'s chain, into
    `n`'s ring, and completes `n` when it completes itself (cross-exchange streaming); or
    an `IEmittingFragmentSinkOperator` — the join — fed by at least one streamed exchange
    (`_emitInto[H] = n`) hands over each fed fragment's output as it is produced, and its
    `Step` output (the non-streamed inputs' contribution, plus nothing when every input
    streamed) completes `n` right after `H`'s own position. Emission is decided per sink
    node, so every exchange feeding it uses `OnFragmentEmitting`.
  - `AtSelf` — `H` is inside a loop region (a `Recurse` exit feeding an outside grouping):
    it runs once per iteration, so there is no single "after it ran" moment; `n` produces
    from the slot at its own position, exactly v1.

  A head with several readers (the fan-out of an enriched stream into many groupings) is
  `AfterHead` for each exchange it heads, produced one after another from the same slot.

Nothing else changes shape: node ids remain the topological sweep order. A fused chain's
work moves to its head's position (or the exchange's, `AtSelf`), which is safe because
slots stay valid for the whole step and the chain's nodes have no other consumer; every
worker publishes each exchange's completion at a sweep position no later than the
position where any worker waits for it, which is what keeps the mesh deadlock-free under
every mode.

## The protocol

Today's point-to-point slot per (node, sender) publishes `count << 1 | nonempty` once per
execution. Streaming adds a parallel **fragment counter** per (node, sender): cumulative
fragments published, monotonic across executions (so no per-step reset and no packing with
the execution count), release-written after the fragment's Z-sets. The existing sequence
publication becomes the completion marker and keeps its encoding; on a streamed exchange
its nonempty bit is redundant (zero new fragments ⇔ empty) but is published truthfully
anyway — `AwaitPublication` and the empty-skip stay correct and shared.

Fragment data rides a **ring** per (node, sender): `ring[f mod R]` holds fragment `f`'s
per-target Z-set array. `R` bounds the fragments one execution may publish (the producer
sizes chunks up when a delta would exceed it: `chunks = clamp(⌈rows / targetChunk⌉, 1, R)`),
so an execution never laps its own ring; across executions reuse is safe by the step
handoff (below). `targetChunk` and `R` are internal constants (256 rows, 8), overridable
internally for tests. The 1-chunk fast path — a delta at or under `targetChunk` — publishes
exactly one fragment and touches nothing the current path does not; sparse steady-state
executions therefore cost the same as today plus one volatile write.

The consumer keeps a cumulative consumed counter per (node, sender) and polls senders
round-robin, consuming any fragment with `published > consumed`, checking completion via
the sequence slot; when idle it spins with the same escalation and poison poll as
`AwaitPublication`, attributing the wait to `BarrierWaitTicks`.

**Parking (2026-09-01).** Both waits — `AwaitPublication` and the consume loop's idle path
— spin for `CircuitOptions.ExchangeSpinBudget` iterations (default 2048: a short burst of
spins, then yields, a millisecond or two in all) and then park on one circuit-wide exchange
gate, as `SpinBarrier` does, instead of yield-polling a stalled peer forever. Every
publication (sequence or fragment counter) is an `Interlocked.Exchange` followed by a read
of the parked-waiter count, and a waiter registers with an interlocked increment before
re-checking its condition under the gate: two full fences, so one side always sees the
other — a publication either pulses or is seen by the re-check, never lost. Poisoning
pulses unconditionally. The healthy-step fast path pays the fence and one cache-line read
per publication (measured as noise: before/after parity at 20k and 2k orders/s, streaming
and plain p2p); a parked wake-up costs a kernel wake, which is why the budget is longer
than a healthy local wait and `parks/step` is on the phase-stats line. Sweeping the budget
on the paced 20k run (`--spin-budget`): 128 parks 0.36–0.38 times per step, 512 0.02–0.05,
2048 0.00–0.01, 8192 0.00, with step time flat across all four (591–618 µs, run-to-run
noise) — so the default sits at 2048, where a healthy step never parks and a stalled peer's
waiters are asleep within a few milliseconds.

**Coalescing (v2).** Forwarded chunks are small — a fragment addressed to one worker is
about `targetChunk / workers` rows, and an emitting join's per-fragment output smaller
still — and an execution may see `workers × R` of them, so they are not published one to
one. Every chunk is partitioned into the *open* ring slot (appending; the slot is cleared
only when first opened), and the slot is published once it holds `targetChunk` rows, when
the consume loop is about to idle (whatever is there is worth more downstream now than
coalesced later), and at completion. An execution may publish at most `R − 1` fragments
this way; after that the open slot absorbs everything until completion publishes it as the
`R`-th, so an execution never laps its own ring whatever the arrival pattern. Whole-slot
production (`AfterHead`, `AtSelf`, an emitting sink's `Step` output) splits into
`clamp(⌈rows / targetChunk⌉, 1, R − published-this-execution)` chunks that go through the
same open slot.

## "Partial merge" on `IExchangeStepOperator`

The interface grows a streaming face (implemented by `ExchangeOperator<T>`; the interface
keeps `Partition`/`Merge` for the barrier, cluster, single-worker, and in-loop paths):

- `PartitionFragment(chunk, parts, ringIndex)` — partition one chunk into the ring's
  per-target Z-sets. Same routing, same reuse discipline as `Partition`, R rings deep.
- `AccumulateFragment(fragment)` / `CompleteMerge()` — the partial merge: fragments
  addressed to this worker are added into the operator's merge buffer as they arrive;
  `CompleteMerge` returns the accumulated Z-set (or the shared empty when nothing arrived).
  The result is value-identical to `Merge` over the same parts — Z-set addition is
  commutative — though its dictionary insertion order (and thus enumeration order) can
  differ run to run. Z-sets are unordered by contract, so nothing downstream may care;
  batches published to outputs already make no ordering promise.
- Chunk splitting needs one new `IZSet` member (`SplitInto(targets)`), dealing entries into
  reused same-type Z-sets, plus a `CreateLike()` to mint those targets — the runtime moves
  deltas as `object` and cannot name `T`.

## Reuse and lifetime contracts (extends `OutputBuffer.cs`)

The allocation work that landed just before this design (`4f26f58`…`088105d`) rests on one
argument: every consumer of a delta reads it before its producer's next-plus-one execution,
and output ports copy. Streaming keeps every piece of that argument and adds three clauses:

- **Ring fragments** are read by consumers strictly within the same execution of the node
  (the consume loop ends when completion is consumed, and completion follows every
  fragment). The producer touches a ring slot again no earlier than its next execution of
  the node, which is ordered behind every consumer's completion of this one by the step
  handoff (and streaming is off inside loops, so the loop-iteration case never arises).
  Single-buffered rings are therefore safe for the same reason today's single-buffered
  `_mailboxes` are.
- **The partial-merge accumulator** is the node's returned delta and keeps the existing
  double-buffered `OutputBuffer` discipline unchanged — it is *filled* earlier, not
  *retained* longer.
- **Fused operators' output buffers** flip per chunk instead of per step. Safe because each
  chunk is fully consumed (partitioned into the ring, which copies) before the next chunk
  runs on the same thread, and fused nodes have no other consumer. A sink's `OnFragment`
  must copy what it keeps (both sinks do: probe results go to the output buffer, weights
  into stores/scratch) and must not retain the fragment reference past the call.
- **An emitting sink's per-fragment output** (v2) comes from the same double-buffered
  `OutputBuffer` its `Step` uses, flipped per fragment, and is consumed — run through the
  downstream chain and partitioned into the ring — before the operator is called again;
  its `Step` output is consumed the same way right after `Step` returns. The open ring slot
  itself is written across several sweep positions (two feeding exchanges' consume loops,
  then the sink's `Step`), which is safe because nothing reads a slot before its counter is
  published and the counter is published once.

The addendum's warning about **k-step pipelining** stands and is now sharper: with k steps
in flight, "next execution of the producer" no longer implies "previous step fully
consumed", so both the rings and the two-buffer `OutputBuffer` would need to deepen to
k-proportional buffer counts. That is a deliberate cost of that future feature, recorded
here so it is priced in, not discovered.

## Fault, poison, and the step contracts

- The consume and chunk loops poll `_exchangePoisoned` exactly as `AwaitPublication` does;
  a faulted peer's never-coming fragment or completion unwinds via
  `BarrierPoisonedException`, and a worker that faults mid-loop records its exception and
  poisons — the existing order (record, then poison) already guarantees peers never find
  the fault list empty.
- A sink may have integrated some fragments when the step faults. That is within the
  existing contract — a faulted circuit's in-memory state is explicitly unusable and the
  faulting step is never committed — but it does *widen the window*: today a strict
  operator's state is touched only inside its own `Step`; a streamed sink's is touched from
  the exchange's sweep position onward. No observable difference (fault ⇒ terminal either
  way), noted for debugging expectations.
- Step semantics are otherwise untouched: manual `StepAsync` keeps published-on-return,
  staged flushes keep their ≤1-step-late fault surfacing, checkpoints commit the same
  states at the same points, and the empty-skip keeps firing (a streamed exchange with zero
  fragments from every sender yields its canonical empty and the downstream skip
  propagates).

## Phase stats and observability

`BarrierWaitTicks` keeps meaning "synchronization wait" (idle polls in the consume loop
count; fragment work does not). Partition/merge tick attribution moves inside the chunk and
consume loops unchanged in meaning. New per-worker counters (phase-stats only):
`FragmentsPublished`, `FragmentsConsumed`, and sink-fed executions — enough to verify, from
a paced showcase run, that chunking actually engages on the busy segments and how many
fragments a step moves. The `incremental.circuit.barrier_wait` gauge is unaffected.

## Configuration

`CircuitOptions.FragmentStreaming` (experimental, default off), valid only with
`PointToPointExchange = true` (validated at build; cluster is excluded transitively).
Separate from the p2p flag so A/B measurement stays honest: streaming's null hypothesis is
p2p, not the barrier.

## Measured (v1, 2026-08-31)

Torture rig on the full showcase pipeline, 4 workers, Release, back-to-back A/B pairs run
in both orders (this host thermally degrades over a session), ~20k warm steps per run —
`--p2p` vs `--streaming`, post-allocation-work baseline (`088105d`):

| regime | metric | p2p | +streaming |
| --- | --- | --- | --- |
| paced 20k orders/s (~90 records/step) | coordinator µs/step | 950–967 | **911–922** (−4…−6%) |
| | worker span µs/step | 765–790 | **706–730** (−5…−10%) |
| | worker sync-wait µs/step | ~242–262 | ~215–235 (−10%) |
| paced 2k orders/s (~9 records/step) | coordinator µs/step | 699 | **680** (−2.8%) |
| unpaced (saturated, ~17k records/step) | records/s | 361k | 367k (parity) |

Reading: exactly what the model predicts for v1. The win comes from merge and sink work
overlapping producer tails, so it grows with per-step delta size and vanishes into noise
both at trickle deltas (nothing to overlap) and at saturation (throughput is compute-bound
and pipelined across steps by the input channels anyway). The serialized owner chain's
dominant term — strict-operator compute between exchanges — is untouched by design in v1;
shortening it further is the v2 list below, cross-exchange streaming and the early-emitting
join first. Fragment counts confirm the mechanics engage: ~17 fragments published per
worker-step across 27 exchanges at the 20k rate, sinks fed on every data-bearing step.

## Measured (v2, 2026-09-01)

Same rig and discipline (4 workers, Release, `--live-orders 20000 --seconds 20 --warmup 5`,
~25k warm steps per run, alternating runs, `DOTNET_GCHeapCount=4`), three engines
back-to-back: `--p2p`, v1 `--streaming` built from `a0c5bdb` in a worktree, and v2
`--streaming`. Ranges are across 2–6 runs each; the host was in a different thermal state
from the v1 table above, so compare within this table only.

| regime | metric | p2p | v1 streaming | v2 streaming |
| --- | --- | --- | --- | --- |
| paced 20k orders/s (~60 records/step) | coordinator µs/step | 636–652 | 611–636 | **572–591** (−4…−6% vs v1, −9…−11% vs p2p) |
| | worker span µs/step | 545–557 | 512–534 | **469–489** (−6…−8% vs v1, −12…−14% vs p2p) |
| | worker sync-wait µs/step | 180–196 | 170–182 | 134–160 |
| paced 2k orders/s (~9 records/step) | coordinator µs/step | 580–583 | 567 | **525–538** (−5…−7% vs v1, −8…−10% vs p2p) |
| | worker span µs/step | 489–495 | 468 | **420–431** |
| unpaced (saturated) | records/s | 422–435k | 436k | **448k** (+3%) |

One v2 run at 20k landed at 653 µs (compute inflated across the board, immediately after a
Release build on the same host) and is excluded as thermal; every other v2 run beat every
v1 run in its regime. Reading: the win now shows at *trickle* deltas too, where v1 had
nothing to overlap — production after the head and the emitting join move publication
earlier even when there is one fragment per exchange, and the sync-wait column is where it
lands (−20–25% vs v1 at 20k). Fragments per worker-step are unchanged (~17 out at 20k), so
coalescing is doing its job: forwarding did not multiply the fragment count. Saturated
throughput picks up 3% from the shorter critical path, which pipelined input batches
previously hid entirely.

## v2 (2026-09-01): production at the head, forwarding, the emitting join

What shipped, in the terms of the original v2 list:

1. **Cross-exchange streaming** — `Produce = Forwarded` with an exchange head: fragments
   cross the downstream exchange's chain without completing at the upstream one. The
   cumulative counters needed no change; per-edge sub-completion is the open-slot
   coalescing above plus "complete the next exchange when this one completes". The public
   API never puts a linear operator directly on an exchange today (every grouping op is
   strict or a sink), so this path is exercised by a test built on the internal node API
   and is mostly the substrate for (2).
2. **Early-emitting join** — `IEmittingFragmentSinkOperator`, implemented by `JoinOperator`:
   each fed fragment's probe results are forwarded into the next exchange as they are
   produced; `Step` contributes the non-streamed side and completes the downstream
   production. On the showcase pipeline this is the enrichment chain's shape twice over
   (`GroupBy → Join → GroupBy → Join → …`), which is exactly the serialized owner-chain
   segment v1 could not shorten.
3. **Production after the head** — not on the list, but it fell out of unifying (1) and
   (2): every streamed exchange now publishes from its head's position instead of its
   own, so the fan-out groupings off a shared stream publish the moment the stream exists.

Still open, unchanged in scope:

- **Store-write streaming for strict keyed operators** (`AggregateOperator`,
  `AntiJoinOperator`): integrate per fragment, recompute groups at complete.
- **Interleaved sink inputs**: letting a join consume right-side fragments while blocked on
  the left (bilinearity permits any interleaving); sides are still consumed in sweep order.
- **`WatermarkOperator` as a chain link** (see the linearity table).
