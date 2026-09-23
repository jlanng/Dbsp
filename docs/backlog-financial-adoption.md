# Backlog: gaps blocking adoption in financial firms

Findings from a review (2026-09-23) of Incremental through the eyes of a .NET developer at a
financial firm evaluating it. Ordered by how hard each item blocks adoption, not by effort.

**Suggested first three:** keyed-input restart (F4), Kafka connector with transactional sink
(F3 + F6), transport TLS/mTLS (F2) — together they move the evaluation from "can't use it"
to "worth a proof of concept".

## Blockers before any code is written

- [ ] **F1 — Governance and supply chain.** Publish to NuGet with signed packages and an
      SBOM; add `SECURITY.md` (disclosure process), a release/changelog cadence, and a
      stated support/maintenance policy. Single maintainer at 0.x fails most firm OSS
      approval processes regardless of code quality.
- [ ] **F2 — Transport security.** TLS with mutual authentication (mTLS) on the TCP cluster
      transport; "trusted network only" does not pass zero-trust infosec review. Also
      consider encryption at rest for Redis state (currently plain JSON).
- [ ] **F3 — Connectors.** First-party Kafka source (maps onto per-partition offsets) and
      sink; then likely Solace / IBM MQ / CDC (SQL Server, Oracle via Debezium). Without
      these every adopter writes and owns the error-prone glue.

## Correctness and durability

- [x] **F4 — Keyed-input restart story.** After a restore, keyed (upsert) inputs cannot be
      safely re-fed (replaying upserts double-counts). Upserts — amendments, cancels,
      reference-data changes — are the core finance shape. *Done:* the current row per key
      is checkpointed circuit state, so re-feeding is idempotent, and keyed inputs take
      source offsets for exactly-once replay. Follow-up: snapshot sync (`ReplaceAllAsync`,
      retracting keys a full-table re-feed omits), which needs a value-store key scan.
- [ ] **F5 — Coordinated cluster checkpoints.** Processes can recover one checkpoint apart;
      needs a distributed commit (or a recovery step that aligns to the minimum committed
      step). Also: peer reconnection, hot standby/failover, and measured
      rebuild-from-checkpoint time at realistic state sizes.
- [ ] **F6 — Delivery guarantees past the output edge.** Stream outputs are at-most-once
      across a crash. Offer at-least-once with idempotency keys (e.g. step/sequence ids on
      batches) and a transactional sink path for end-to-end exactly-once.
- [ ] **F7 — Schema evolution for persisted state.** A versioned serialization / migration
      path so adding a field to a trade type doesn't mean a new key prefix and a full
      replay of history.

## Finance-specific needs

- [ ] **F8 — Deterministic replay and as-of queries.** Document and test that replaying the
      same input yields identical output (regulatory reproducibility: MiFID II, CAT, model
      validation). Consider bitemporal / as-of support ("what was the exposure at 10:32?").
- [ ] **F9 — Tail-latency reporting.** Publish p50/p99/p999 step latency and GC pause
      behaviour under realistic state, alongside the existing throughput and
      allocation-per-record numbers.
- [ ] **F10 — Off-heap large state.** RocksDB (or similar) provider so a year of trades and
      positions doesn't sit in the managed heap. Already in README Headroom.
- [ ] **F11 — Lineage / explainability.** Tooling to answer "which input rows produced this
      aggregate value?" for debugging and audit.

## Softer turn-offs

- [ ] **F12 — Query surface.** No SQL and pipelines are compiled delegates, so analysts can't
      author them and they can't be config- or runtime-defined. Decide whether this stays
      library-first by design, and say so explicitly.
- [ ] **F13 — Comparison page.** An honest "why not X?" doc against Trill, Kafka Streams,
      Flink, Feldera and Materialize.

## Strengths to keep and lead with

Retractions everywhere (corrections/cancels are native); checked integer sums and
`decimal` guidance; atomic state + offset checkpoints with state manifests; property-based
differential tests; `ILogger` + `Meter`; an honest known-limitations section; and
`examples/Incremental.TradeEnrichment`, which deserves top billing for this audience.
