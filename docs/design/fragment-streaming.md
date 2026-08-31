# Linear-segment fragment streaming

Design sketch, August 2026. Companion to `docs/research/timely-dataflow.md`, which
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

What this deliberately is *not*: no operator emits early (no intermediate retract/insert
churn downstream), no dynamic scheduler, no change to step atomicity, watermarks,
checkpoint commit points, outputs, or the cluster path.

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

Nothing else changes shape: node ids remain the topological sweep order, and a fused
chain's work moves *later* in the sweep (to the exchange's position), which is safe because
slots stay valid for the whole step and the chain's nodes have no other consumer.

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

## What v2 would add (explicitly out of scope)

1. **Cross-exchange streaming**: fragments flowing through a linear chain *between* two
   exchanges without completing at the first — turns per-hop overlap into pipeline depth.
   Needs per-edge sub-completion tracking; the cumulative-counter protocol already
   accommodates it.
2. **Early-emitting join**: `JoinOperator.OnFragment` output is final, so it could publish
   downstream fragments immediately — the first strict-through-streaming operator. Requires
   (1).
3. **Store-write streaming for strict keyed operators** (`AggregateOperator`,
   `AntiJoinOperator`): integrate per fragment, recompute groups at complete.
4. **Interleaved sink inputs**: letting a join consume right-side fragments while blocked
   on the left (bilinearity permits any interleaving); v1 consumes sides in sweep order.
