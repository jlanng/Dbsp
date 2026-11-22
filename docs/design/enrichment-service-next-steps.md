# Enrichment service: next steps (round five, the last)

> **Carried out.** Round five was built, reviewed and checked on 2026-11-16 (head
> `bbc0f08` at the last critic's check). What was done, what was not, what remains and
> how to merge: [enrichment-service-round5-report.md](enrichment-service-round5-report.md).
> The plan below is kept as it was written.

Saved on 2026-11-15 at commit `0a08a27` of branch `enrichment-service`, to be worked on
later. It is the plan for one more round of work and then a stop. The design is
[enrichment-service.md](enrichment-service.md); the numbers in brackets are items of its
*Open for James* section.

## Where the branch stands

Four rounds of build, review, fix and critique are done. At `0a08a27`, checked
independently of the agents that did the work:

| Check | Result |
|---|---|
| `dotnet build` | 0 warnings, 0 errors |
| `tests/Dbsp.Tests` | 717 total, 716 passed, 1 skipped (the opt-in soak) |
| `tests/Dbsp.Kafka.Tests` | 150 of 150 |
| `tests/Dbsp.Service.Tests` | 260 total, 254 passed, 6 skipped (the three-broker tests) |
| `examples/…/Pipelines.Tests` | 18 of 18 |
| Example smokes | in memory 55 claims, two nodes 70, Kafka with a `kill -9` 69, trade enrichment 13: all hold |
| `benchmarks -- --assert` | all 10 performance contracts hold |
| CI | green on the last push and on a run started by hand, the broker-failure job included |

The last critic found nothing that stops the product being built on this. What it left is
in *From the last critic* below.

## Decisions James has made

Settled; a later round does not reopen them. The design doc does not yet say so: marking
them there is part of this round.

Decided 2026-11-14:

1. The standby stays warm: a plugin's `Build` runs on both nodes [1, 30].
2. The durable Kafka logs stay JSON with zstd, not the dense codec.
3. The admin defaults stay: row data off, code upload off, a caller on loopback may name
   itself [4, 5, 25].
4. Keyed inputs expire by event time only: no processing-time expiry, no cap on keys [69].
5. A `files` input onto what a `files` sink writes stays refused [63].
6. Plugins are written by James's own teams: trusted code in the service's process is the
   model, and there is no sandboxing.
7. MSK authentication will be IAM. Checking anything on AWS (MSK, IAM, real S3) is put
   off: it stays a stated limit, and no script or checklist is built for it now.
8. The work stays a branch. No pull request; James merges.
9. This round is the last: build and review what is listed here, report, stop.
10. For every open item not named here, the built default stands.

Decided 2026-11-15:

11. The writer's transaction timeout stays five minutes [67]. Measured with two of three
    brokers away for 70 s: at 60 s the leadership moved in 4 runs of 6, at five minutes in
    none of 6.
12. A record that arrives behind the watermark is dropped and counted, as built: not
    dead-lettered, not a fault.
13. Production runs on Kubernetes (EKS).

## To build

Each of these is a decision above turned into work.

- **B1. An abstractions package for plugin authors** [11, 20].
  `Dbsp.Service.Abstractions` holds the definition interfaces, the builder
  surface a definition sees and the table definitions; a testing package holds
  `PipelineTestHarness`. A plugin project then compiles without seeing hosting or Kafka
  types. Nothing is published, so the break costs this repository only. Done when the
  example's plugins, the test plugins and `Pipelines.Tests` reference only the new
  packages, a test fails if a plugin-facing type drags in `Dbsp.Kafka` or the
  hosting assemblies, and the plugin unload test still passes when run alone.
- **B2. Paced bulk loads** [61]. A load of five million rows stopped the leader for
  about 2 s in one blocking collection; a standby takes over after 3 s. The table writer
  paces a large load so that no pause comes near `TakeoverAfter`. The timeout is not
  raised. Done when the scale rig at five million rows shows the leader's longest stop
  during the load well under the timeout, with the load's duration before and after.
- **B3. A limit on a pipeline's state** [70]. An optional maximum of state entries per
  pipeline, in the spec; a pipeline that passes it faults — that pipeline only — with a
  reason that names the limit. The growth flag stays. Done when it is in the deploy form,
  the guide and the harness, and a test shows the neighbours of a faulted pipeline
  unaffected.
- **B4. A pluggable record format on the Kafka connectors.** Only the seam: an interface
  the owner's service implements for what its topics carry (Avro, Protobuf, a schema
  registry). JSON stays the built-in. Done when a test plugin or test host supplies a
  second format through it for a source, a keyed source, a table loader and both sink
  kinds, and a record the format cannot read goes the way an unreadable JSON record goes.
- **B5. An S3 object store** for the `files` connector, as an optional package, tested
  against MinIO in the compose stack: listing after a name a page at a time, a write that
  is whole or absent, and the settle rule on the store's own stamps. Done when the files
  connector's tests run against it as they do against a directory, and CI runs them.
  Real S3 stays unverified (decision 7).
- **B6. Rolling upgrades across a change of a persisted format** [31]. Today a build that
  changes the state log's format byte needs both nodes replaced together. A node keeps
  readers for the older formats, and a leader writes a format its standby can read until
  both understand the newer one. Done when a pair of nodes of different format levels
  runs through standby upgrade, takeover and leader upgrade with the sinks equal to the
  model, for the state log and the table logs, and the guide says how an upgrade is
  done. What it means for the state manifest (format 7 refuses older event-time
  checkpoints [36]) is said plainly either way.

## From the last critic

To build, because each can lose or stall records in production:

- **K1. A lagging partition makes honest records late.** The watermark is the newest
  event time seen across an input, with nothing per partition. When one partition of a
  topic falls behind the others by more than the pipeline's open window — a backlog after
  a restart or a takeover, a broker that was away — its records arrive behind the
  watermark and are dropped. The example's topics have one partition, so this has never
  run. Wanted: a watermark that is the least over an input's partitions, with a rule for
  a partition that is idle, kept in the checkpoint and restored; and a test and a smoke
  on a topic of several partitions, with a backlog and a broker loss.
- **K3. One record stamped far in the future stalls a pipeline** [81]. The watermark
  jumps to it, every later record is late, and every open record closes. The only way out
  is a reset. Wanted: a bound on how far one record may move the watermark, with what is
  beyond it treated as a record in error, and a note in the guide for the operator.

Small, to do in passing:

- **K5.** In Kafka mode the example's deploy form offers a feed that is empty there, and
  the dry run accepts it: a pipeline deployed from the template runs and reads nothing.
- **K7.** A takeover an operator asks for writes fail and critical lines on the replaced
  node, and a restarted node says it has never led.
- **K8.** `--headless --kafka` ignores `--prefix`.
- **K9.** The guide carries measurement narrative from each round that an operator does
  not need. Trim the guide; leave the design doc's record as it is.

Looked at, and left:

- **K6. A reused trade id** gives different sink rows depending on how records fall into
  steps [71]. Fix it if a sound rule exists (finality judged by the record's own event
  time, not by the step); otherwise the guide says ids must not be reused within the open
  window, and why.
- **K2. MSK, IAM, TLS, real S3**: put off (decision 7).
- **K4. Memory over hours**: nothing has run longer than about 36 minutes; a node's
  working set is about twice its live heap in the last run. Stays a stated limit.

## Documentation in this round

- *Open for James* gains a *Decided* part with the thirteen decisions above and their
  dates; the items they settle say so in place. *Decide before production* is brought up
  to date.
- The guide gets the Kubernetes recipe: what restarts a node after exit codes 3, 4 and 5
  (at once after 3 and 4, with a growing delay after 5), the probes mapped to the node's
  health endpoint, and a rollout that never stops both nodes at once.
- The guide, the README, `docs/intro.md` and `CLAUDE.md` stay true to what is built.

## How the round is run

The same shape as the four before it, saved as the workflow
`enrichment-service-round5` (`.claude/workflows/enrichment-service-round5.js`, not in
the repository):

1. Six build steps, one after another in the working tree: event time (K1, K3, K6);
   rolling upgrades (B6); tables and state (B2, B3); packages and the record format
   (B1, B4); the S3 store (B5); documentation and the small items.
2. Three review lenses, each followed by a verifier that tries to refute its findings.
3. One fixer for the confirmed findings.
4. A last critic, then a final report to James. No sixth round.

The subtle steps (event time, rolling upgrades, their review, the fixer) run on the
session's model; the rest run on Opus. Every step commits and pushes as it goes and picks
up uncommitted work it finds, so an interruption costs little. If a step returns
nothing — a usage limit, for example — the workflow stops there instead of going on
without it, and can be resumed.

To start it: have the connector stack up (`docker compose --profile connectors up -d`)
and the tree clean, then ask Claude Code to run the saved workflow
`enrichment-service-round5`.
