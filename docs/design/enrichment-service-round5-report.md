# Enrichment service: round five report

Round five, the last, carried out the plan in
[enrichment-service-next-steps.md](enrichment-service-next-steps.md) on branch
`enrichment-service`. Head at the last critic's check: `bbc0f08`, tree clean, equal to
`origin/enrichment-service`. The record of what was built and measured is S9 in
[enrichment-service.md](enrichment-service.md); run records are in [runs/](runs/).

## Where the branch stands

The last critic's verdict: the product can be built on this branch as it stands. Three
qualifications: CI has not produced one green connector-stack run in round five; B2 is
partial; the Kubernetes recipe and the S3 store have never met EKS or Amazon S3
(decision 7).

Checks as the last critic ran them on `bbc0f08`, connector stack up, independently of the
builders:

| Check | Result |
|---|---|
| `dotnet build` | 0 warnings, 0 errors (Debug; Release also builds) |
| `tests/Incremental.Tests` | 765 total, 764 passed, 1 skipped (opt-in soak); 1 min 44 s |
| `tests/Incremental.Kafka.Tests` | 166 total, 164 passed, 2 skipped (the two `*ABrokerLost*`, need three brokers); 4 min 58 s |
| `tests/Incremental.Service.Tests` (`INCREMENTAL_OLDER_BUILD` from `9de8da9`) | 337 total, 329 passed, 8 skipped (six three-broker tests; two admin UI tests, no Chrome); two-build test ran; 4 min 40 s |
| `S3FileConnectorTests` on MinIO | 46 of 46, 0 skipped |
| kafka3 profile | `KafkaPartitionWatermarkTests` 6 of 6; service `*broker*` tests 7 of 7 |
| `examples/…/Pipelines.Tests` | 21 of 21 |
| Example smokes | in memory 54 claims, two nodes 69, Kafka with `kill -9` 69, trade enrichment 13: all hold; Kafka smoke under `--prefix` holds and refuses a used prefix |
| `benchmarks -- --assert` | all 10 performance contracts hold |
| CI | not run since round five began: every push from `d967462` to the head refused by GitHub for billing. Last green run: `9de8da9`, before round five |

Not run by anyone this round: the admin UI browser tests (no Chrome here; last ran in CI
at `56a9d85`), the torture rig, anything on AWS, anything longer than about nine minutes.

## Items

Where the critic's finding differs from the builder's claim, the critic's is taken.

| Item | Status | Evidence |
|---|---|---|
| B1 Abstractions and testing packages | Done | `PluginBoundaryTests` (16), made to fail by a Kafka-typed member; older plugins load through `TypeForwards.cs` (`PluginTests`, fails without it). Critic packed the packages and built the example's plugin and its 21 harness tests outside the repository from PackageReferences only. |
| B2 Paced bulk loads | Partial | `TableLoadPacing`, on by default. Five million rows not shown: every 5M run, paced or not, stopped 3.1-3.8 s in a full gen2 under memory pressure on this machine. At 3M the builder recorded 554-810 ms down to 190-200 ms (141 ms after the review fix); the critic measured 765-1,211 ms down to 470-630 ms, the pacer standing aside on a third of its steps for memory. Pacing about halves the stop; the recorded figures did not reproduce. |
| B3 Limit on a pipeline's state | Done | `circuit.maxStateEntries`; `StateLimitTests`; critic used it through the admin API: limit 0 refused, the limited pipeline Failed with no restarts, neighbours untouched, raise with `state=keep` resumes. Overshoots by up to a second's growth (8,080 against 300 in the critic's run), as stated. Two guide slips (below). |
| B4 Record format seam | Done | `IRecordFormat`, `AddRecordFormat`; `KafkaRecordFormatTests` (source, keyed source, loader, both sink kinds, unreadable records go JSON's way, codecs per generation, plugin unloads). Critic did not write a second format. |
| B5 S3 object store | Done locally | `Incremental.Service.S3`; files connector tests 92 of 92 (46 local, 46 S3 on MinIO). "CI runs them" is true of `ci.yml`, false of history: no CI run has started MinIO. Real S3 unverified (decision 7). Guide's IAM list omits `s3:AbortMultipartUpload`. |
| B6 Rolling upgrades | Done | Format levels (`PersistedFormat`); two real builds (`9de8da9` and head) through standby upgrade, takeovers and the raise, sinks equal to the model in 4 runs; `RollingUpgradeTests`, `FormatLevelTests`, `KafkaFormatLevelTests`. Critic followed the guide's four steps by hand with the example's event-time pipelines: clean. State manifest said plainly: formats 7 and 8 read; format 6 with event time stays refused, needs `state=reset`. |
| K1 Watermark per partition | Done | Least over partitions with an idle rule, checkpointed (manifest format 8, level 2). Backlog on 3 partitions 0 late (951-1,188 of 1,800 before); broker killed or frozen under 6 partitions 0 late. Critic: 75 s backlog over three partitions read with 0 late by hand. Review fixes E1, E3 apply. |
| K3 Future bound | Done | `MaxEventTimeAhead`, default one day in the service: beyond it a dead letter or fault at the port. Tests pass; not exercised by hand. A change of default behaviour. |
| K5 Empty feed on Kafka | Done | Kafka smoke claim holds; `KafkaServiceTests` test passes. |
| K6 Reused trade id | Done (rule after the window; none within) | A key returning after its open window is a new record, same rows over five step layouts; within the window it is an amendment, and the guide says ids must not return within it, and why. Review fix E2 applies. |
| K7 Operator's takeover | Done | No fail/crit line on the replaced node (critic saw this by hand); `previous` term reported (tests only). |
| K8 Smoke `--prefix` | Done | Critic ran under `critk8` (69 hold) and was refused on a used prefix. |
| K9 Guide trimmed | Done | About 90 lines of narrative out; design record untouched. Left a stale figure: smoke claims 55/70 where the smokes check 54/69. |
| K2, K4 | Put off (plan) | Still stated as limits. |
| Docs: Decided part, Before production | Done | D1-D13 with dates at the head of *Open for James*; settled items marked; *Before production* rewritten. |
| Docs: Kubernetes recipe | Done on paper | Supervisor as entry point, probes on `/health`, StatefulSet gated on new `/health/ready?warm=true`, PDB. Not run on any cluster. Gaps: headless Service not given (needs `publishNotReadyAddresses: true`), only `NODE_ID` of the environment shown, `rollout undo` after the raise stalls unless `HoldFormatLevel` was set. |
| Docs: guide, README, intro, CLAUDE.md true | Largely done | `DocSnippetTests` pass; CLAUDE.md suite times match. Slips listed under *What remains*. |
| Run everything, broker job | Partial | Every check run locally, broker job emulated on kafka3 (7 of 7, 2 of 2); CI refused for billing. Builder's "CI green on the first three pushes" is wrong: connector-stack failed on all three (skip count), build-and-test failed on `6b9e537`. |

## What the review confirmed, and what became of it

All seven fixed by the fixer (commits `d138696` to `bbc0f08`), each with a test that
failed before; the critic read the diffs and found no defect.

| Finding | Severity | Outcome |
|---|---|---|
| E1 Level-1 checkpoint resumed moves the watermark to the newest partition | loses data | Fixed: the level-1 key holds the least. Cost kept: after such a resume the future bound is measured from that least. |
| E2 Late version in the same step as the open one deletes the record | contained | Fixed: the port cuts the drain before it. Clustered circuits not covered. |
| E3 Burst after every partition went idle timed by the first partition read | loses data | Fixed: a record takes idle back from all idle partitions until each confirms its end again (588 late in 3 runs before, 0 after). Cost kept: a quiet partition beside busy ones moves the watermark in 1-2 s steps; a record more than about 1 s behind its burst on an idle partition can still be late. |
| S1 Pacing never stops for a steadily committing table | contained | Fixed: a run also ends below 5,000 changes/s; one pace per process. A load slower than that is not paced. |
| C1 Docs promised a backlog after a quiet spell is not late | docs untrue | Fixed with E3; guide, intro and doc comment say what remains. |
| C2 S9's table hid a failure in the service run | record untrue | Fixed: whole suite re-run (329 passed, 0 failed); the old row corrected. |
| C3 CLAUDE.md said the Kafka tests take 1.5 min | docs untrue | Fixed: about five minutes, both suites. |

## What remains

Most important first. Each says what James has to decide.

1. **CI has never run green on round five.** Every `ci.yml` change of the round is
   unexecuted: MinIO's image pull (a community build, `pgsty/minio`, pinned), the
   worktree build of `9de8da9`, the exact skip counts 2 and 6, the smoke's `--prefix`,
   packing three new packages, the broker job's second test line. The admin UI tests have
   not run since `56a9d85`; `app.js`, `forms.js` and `views.js` changed since.
   *Decide:* restore the account's billing, then push and run
   `gh workflow run CI --ref enrichment-service -f broker_tests=true` (about 45 min of
   runner time); expect a fix or two in `ci.yml`. Clear this before relying on the branch.
2. **B2 at five million rows is not shown**, and the 3M figures depend on free memory.
   *Decide:* make the run (`--load-only --rows 5000000`, with and without `--no-pacing`,
   on a machine with about 9 GB free for the node, reading the `gc` lines; half an hour),
   or accept that sizing, not pacing, keeps a load's stop under `TakeoverAfter` (the
   guide's 1.35x container arithmetic can put a node at the collector's threshold during
   a load).
3. **The way back from an upgrade closes about a second after the second node is new**,
   unless `ServiceOptions.HoldFormatLevel` is set; on Kubernetes a `rollout undo` after
   that stalls. The pre-level build `9de8da9` started after a raise calls itself a warm
   standby and would lead with every pipeline NeedsReset (nothing is deployed on it).
   *Decide* before the first production upgrade: an operating rule (roll out with the
   hold, remove it in a second rollout; no build cost) or a raise by operator command
   (a day or two, not planned).
4. **The Kubernetes recipe has not run on any cluster.** *Decide when:* about a day on a
   local cluster (kind) settles rollout, probes and DNS without AWS; add the headless
   Service with `publishNotReadyAddresses: true` and the peer's environment variable.
5. **Event time's protections hold only where `WithEventTime` is declared on the input**,
   on an unclustered circuit; nothing warns at the dry run otherwise (a dry-run note is a
   few hours). Also stated and left: lateness under 5 s is not protected through a
   broker's loss; a quiet partition delays closes by up to 5 s and, beside busy ones,
   makes the watermark step 1-2 s; a deployment held at level 1 pays K1's full restart
   cost at each takeover; the files connector and tenant connectors name no partitions.
   *Decide* whether any of these needs work for your feeds.
6. **The future bound is on by default at one day.** A record further ahead is a dead
   letter or fault where it used to stall; a pipeline whose event time is rightly ahead
   must set `circuit.maxEventTimeAhead`. *Decided by each pipeline's owner.*
7. **Guide corrections** (about fifteen minutes, `docs/enrichment-service.md`): remove
   "Nothing limits a pipeline's state by count"; a pipeline failed by its limit does set
   the node Degraded (`pipeline-failed`); smoke claim counts 54 and 69; CI's skip rule is
   exactly 2 and 6; add `s3:AbortMultipartUpload` to the S3 role; say CI is configured to
   run the S3 tests, not that it does, until it has.
8. **The two-build upgrade test uses no event-time pipeline.** The critic's by-hand run
   with them passed but is not kept. *Decide* whether to add one (a few hours) before a
   second format change.
9. **Two tests failed once each and not again:**
   `TableTests.The_file_loader_snapshots_resyncs_and_handles_unreadable_records` (CI,
   `6b9e537`) and `TableJoinTests.Completing_every_input_ends_an_auto_stepping_circuit_though_its_table_never_completes`
   (fixer's run). Both 5 of 5 alone and in whole runs. Watch them when CI runs.
10. **Put off by decision 7, unchanged:** Amazon S3, MSK and IAM unverified; nothing has
    run for hours (K4). A standby must be upgraded before the leader (format 8); a plugin
    built against the new abstractions does not load on a pre-round-five node, so nodes
    are upgraded before plugins are rebuilt; test projects using the harness must
    reference `Incremental.Service.Testing`.

## How to merge

The work stays a branch (decision 8); James merges. Suggested order:

1. Restore CI billing; push nothing new; run CI on `enrichment-service` with
   `broker_tests=true`. Fix `ci.yml` on the branch if it needs it, until build-and-test,
   perf-contracts, connector-stack and the broker job are green.
2. Make the guide corrections in item 7 on the branch.
3. Merge `enrichment-service` into `main` (a merge commit keeps the round's record; the
   branch is not rebased). Behaviour that changes for an existing deployment: the
   one-day future bound, load pacing on by default, state manifest format 8 after the
   raise, `Confluent.Kafka` loaded by version rule, plugins referencing the new packages.
4. Upgrade running deployments by the guide's *Upgrading the nodes*: standby first, with
   `HoldFormatLevel` set if you want a way back.
