# Changelog

What each release changed, newest first. The packages are released together under one
version, so one entry covers all of them. This is a 0.x project: the API may change
between minor versions, and an entry says so where it does.

The release workflow takes a release's notes from its section here, and refuses a version
that has none: a heading is `## <version> - <date>`.

## Unreleased

- `SECURITY.md`: how to report a vulnerability, and what is supported.

## 0.2.0 - 2026-11-22

The first release. Four packages on nuget.org, for net8.0 and net10.0:

- **`Dbsp`** — the library. Z-set deltas through incremental operators: joins (inner,
  left, full, anti), `Distinct`, `TopK`, `Count`/`Sum`/`Min`/`Max` and custom aggregates,
  all correct under retraction; event time with watermarks, tumbling, hopping and session
  windows, emit-on-close and eviction of state that is final; recursive circuits;
  multi-worker execution in one process and the same exchange across processes over TCP;
  shared tables; checkpoints of state together with input offsets for exactly-once
  restart; acknowledged outputs and sinks; a per-record error policy with a dead-letter
  output; `ILogger` and `Meter` observability.
- **`Dbsp.Storage.Redis`** — operator state and atomic checkpoints in Redis.
- **`Dbsp.Kafka`** — the state changelog and shared-table logs on Kafka through one
  transactional producer, warm standby with leadership and fencing, sources, sinks and
  Debezium change events.
- **`Dbsp.Hosting`** — Generic Host and dependency-injection integration.

Each package depends on exactly this version of the others: use one version of all of
them.

Persisted formats are at format level 2 (state manifest format 8). Later releases read
what this one wrote; `docs/design/durability-and-standby.md`, *As built: format levels*,
says how two builds share the logs during an upgrade.

Not in this release: the service (`Dbsp.Service` and its four companion packages), which
is in the repository and built by CI. What the library deliberately does not do is under
*Known limitations* in `README.md`.
