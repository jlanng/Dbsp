# Run records

Records of measured runs that a design document cites, kept here so that a table can be
checked against what produced it. (S7's first broker-failure tables cited a scratch
directory; a later review found it gone.)

## 2026-11-12, broker failures — the third review's step

`docs/design/enrichment-service.md`, *As built*, S7, *The third review's findings*.

- `2026-11-12-broker-failures-series1.jsonl` — one line per run, as
  `KafkaServiceProcessTests` writes it to `broker-runs.jsonl` beside the test assembly
  (`scenario` `no-quorum`: who led afterwards, at which term, why a takeover happened,
  every exit a supervisor saw, the sink's longest silence, the standby's `takeoversHeld`
  counter as read; `scenario` `takeover-broker-down`: the takeover's timings).
- `2026-11-12-broker-failures-series1.measurements.log` — the same runs' lines from
  `measurements.log`.
- `…-series2.jsonl` and `…-series2.measurements.log` — the second series, the same
  way. Its no-quorum records also carry `downForMs` (from the first broker going to
  the command that started them again) and `statusDuringOutageMs` (how long the
  standby took to answer a status request eight seconds before the end of the
  outage); its takeover records carry the broker killed and its roles.
- `run.sh` — how one run was made: the three-broker profile's environment and one
  execution of the named test. The scratch paths in it are the session's; the commands
  are CLAUDE.md's.

The profile: `docker compose --profile kafka3 up -d` (three brokers, each also a
controller, replication factor 3, `min.insync.replicas` 2, Kafka 3.9.0), a 13th-gen
i7-13700H. The runs, in order:

```
run.sh <label> '*Seventy_seconds*' INCREMENTAL_KAFKA_OUTAGE_SECONDS=40                               # two brokers stopped
run.sh <label> '*Seventy_seconds*' INCREMENTAL_KAFKA_OUTAGE_SECONDS=40 INCREMENTAL_KAFKA_FAULT=kill  # two brokers killed
run.sh <label> '*A_takeover_with_one_broker_down*'                                                   # broker shut down, then the leader killed
run.sh <label> '*A_takeover_with_one_broker_down*' INCREMENTAL_KAFKA_FAULT=kill                      # broker and leader killed together
```

Series 1 was made on the build of commit `a1e2eb4` less one change that the series
itself led to (the decision to take over is looked at again after the brokers were
asked), and with the test reading the standby's counter before it restarted the
brokers, which made two of the "40 s" outages 70 s long.

Series 2 was made on the build of commit `4ece9ea` with two changes that were committed
after it: a table's statistics give the log two seconds to say where it ends (as
committed, the two durations are settable by a test; they are what they were), and
the no-quorum test asks the standby for its status during the outage and starts the
brokers again on time. Its runs, in order:

```
3 × run.sh <label> '*Seventy_seconds*' INCREMENTAL_KAFKA_OUTAGE_SECONDS=40                               # two brokers stopped
3 × run.sh <label> '*Seventy_seconds*' INCREMENTAL_KAFKA_OUTAGE_SECONDS=40 INCREMENTAL_KAFKA_FAULT=kill  # two brokers killed
run.sh <label> '*A_takeover_with_one_broker_down*' INCREMENTAL_KAFKA_FAULT=kill INCREMENTAL_KAFKA_BROKER=0   # and 2, 0, 2
run.sh <label> '*Seventy_seconds*'                                                                       # 70 s, stopped
2 × run.sh <label> '*Seventy_seconds*' INCREMENTAL_KAFKA_FAULT=kill                                      # 70 s, killed
```

The third of its runs (`s75d0ddd85fe14700`) spans a suspension of the machine: its
brokers had been started again at 07:50 and its last lines were written at 19:02. The
durations in it are the machine's running time. The session that made the series was
cut off there; the ten runs that follow were made by its script when the machine woke,
and the series was written up from these records by another session on 2026-11-14.

## 2026-11-14, a check of the same step on a cluster just started

`2026-11-14-broker-failures-check.jsonl` and `….measurements.log`: the broker tests of
`KafkaServiceProcessTests` once each with their defaults (`run.sh check-all '*broker*'`),
on the tree as committed (`7dd9d9e`) and the `kafka3` containers removed and started
again a minute before. 7 min 34 s, all passed.

`2026-11-14-j1-state.tsv`: state entries and estimated bytes per pipeline, read from
`GET /admin/api/pipelines` at one, two, five and ten minutes of the in-memory example
(one node, 50 trades a second, the seeded specs), for S8. `before` is the tree at
`a0cbd57` with the `--trade-ids` option added; `after` is S8's tree. `unique`
ran with `--trade-ids 0`, `wrap` with `--trade-ids 20000` (the old default). Two
processes of a Debug build ran side by side.

## 2026-11-14, CI that can be trusted — round four, J3

`docs/design/enrichment-service.md`, *As built*, S8, *CI that can be trusted*.

- `2026-11-14-j3-ci-runs.tsv` — every run of the workflow on the branch from the
  step's first commit (71521ba) to its last that changed code or tests (53d8c83), one
  line per job: run id, commit, what started it, the run's and the job's conclusion,
  when the job started and how many seconds it took. Made with `gh run list` and
  `gh run view --json jobs`.
- `2026-11-14-j3-local.txt` — what ran here: the six loops of the whole service test
  project pinned to two processors (`taskset -c 0,1`, the node processes with it), run
  by run; the broker-free classes under busy loops; the scratch experiments' own
  output (the end of committed data after a commit, a topic asked about after its
  creation, the cost of the first short read of a topic, a leader stopped inside a
  transaction); each node's processor time while one broker of three was down; and
  the two runs of the scale rig. The scripts and the logs behind it were the
  session's scratch directory and are not kept.

## 2026-11-14, what a quorum outage and a broker's loss cost — round four, J4

`docs/design/enrichment-service.md`, *As built*, S8, *What a quorum outage and a
broker's loss cost, and whose time it is*; `docs/design/durability-and-standby.md`, *A
leader does not outlive its transaction timeout*. Each `.jsonl` file holds the lines
`KafkaServiceProcessTests` wrote to `broker-runs.jsonl`, one per run, in the order the
runs were made; a record's `prefix` is the run's topic prefix, by which the other files
name it.

- `2026-11-14-j4-traced-60s.jsonl` — eight outages of 70 s (two of three brokers
  stopped, or killed in one command, turn about) on the 60 s transaction timeout that
  was then the default, the nodes' Kafka client log at `debug=eos,protocol` (the first
  two also `broker`). The test did not yet record the timeout. From the second run on
  the brokers logged to files and `kafka.coordinator.transaction` was at `DEBUG` (an
  override of the profile, below).
- `2026-11-14-j4-traces.txt` — the decisive lines of those eight runs: what the
  writer's client did when its transaction timed out, and what the transaction
  coordinators logged about the transactional id. The table in the engine design is
  read from it. The whole logs were up to 4 GB a node and are not kept.
- `2026-11-14-j4-traced-5min.jsonl` — four outages of 70 s on the five-minute timeout,
  traced the same way.
- `2026-11-14-j4-no-quorum.jsonl` — the runs of record, on the `kafka3` profile as it
  is in the repository, removed and started again before them, no debug logging: six
  outages of 40 s and six of 70 s on the default timeout, then six of 70 s with
  `INCREMENTAL_KAFKA_TRANSACTION_TIMEOUT_SECONDS=60`; stopped and killed turn about.
  Each record carries `transactionTimeoutS`.
- `2026-11-14-j4-takeover-broker-killed.jsonl` — nine takeovers with a broker killed in
  the same moment as the leader's process (`INCREMENTAL_KAFKA_FAULT=kill`,
  `INCREMENTAL_KAFKA_BROKER` 0, 1, 2 in turn), before heartbeats were dated by their
  writing: three with the nodes' log at debug, six without. The fourth
  (`s4ec1feb793df4d9a`) is the takeover that was called off for the dead leader's last
  heartbeat.
- `2026-11-14-j4-takeover-broker-killed-dated.jsonl` — nine more, on the build that
  dates heartbeats.
- `2026-11-14-j4-separate-controllers.jsonl` — on three brokers with three controllers
  of their own (`docker-compose.kafka3-separate-controllers.yml`, beside this file; not
  a profile of the repository): three outages of 40 s, two of 70 s, three takeovers
  with a broker killed.
- `2026-11-14-j4-check.jsonl` — the broker tests once each with their defaults on the
  final build, the profile's containers removed and started again a minute before.
- `2026-11-14-j4-brokers.txt` — the brokers' and controllers' own milestones for these
  runs: when the returning brokers' processes were up, when a controller was elected,
  when it fenced the brokers' old registrations (`session has timed out`), when the
  new ones were accepted and unfenced, and for the four traced runs the leader's last
  commit before the outage and its first after. The two tables of whose time the
  seconds are were read from it.
- `2026-11-14-j4.measurements.log` — the lines of `measurements.log` from the first of
  these runs to the last, with those of the test-suite runs made in between.
- `2026-11-14-j4-run.sh` — how a run was made (the scratch paths are the session's),
  and the override used for the traced runs: the three brokers with
  `logging: { driver: json-file }` — the default journal dropped lines under the
  surviving broker's 4,000 a second — and
  `KAFKA_LOG4J_LOGGERS: "kafka.coordinator.transaction=DEBUG"`.

The machine and the profile are those of the sections above. The runs of record, in
order (`run.sh <label> <filter> [environment]`):

```
3 × ( run.sh … '*Seventy_seconds*' INCREMENTAL_KAFKA_OUTAGE_SECONDS=40 ;                            # stopped
      run.sh … '*Seventy_seconds*' INCREMENTAL_KAFKA_OUTAGE_SECONDS=40 INCREMENTAL_KAFKA_FAULT=kill )
3 × ( run.sh … '*Seventy_seconds*' ; run.sh … '*Seventy_seconds*' INCREMENTAL_KAFKA_FAULT=kill )
2 × ( run.sh … '*A_takeover_with_one_broker_down*' INCREMENTAL_KAFKA_FAULT=kill INCREMENTAL_KAFKA_BROKER=0 ; … =1 ; … =2 )
3 × ( run.sh … '*Seventy_seconds*' INCREMENTAL_KAFKA_TRANSACTION_TIMEOUT_SECONDS=60 ;
      run.sh … '*Seventy_seconds*' INCREMENTAL_KAFKA_TRANSACTION_TIMEOUT_SECONDS=60 INCREMENTAL_KAFKA_FAULT=kill )
```

The run with a longest silence of 72.3 s (`sb5cdd318d216404b`, 70 s on the 60 s
timeout, killed) is not a shorter outage: one record became readable two seconds after
the brokers were started and the next fourteen seconds later, and the test records the
longest silence only.

## 2026-11-15, the check after J4's last change

`docs/design/enrichment-service.md`, S8, *Finished by a second session*. The `kafka3`
profile's containers removed and started nine seconds before the first test; the build
is the one in which a heartbeat younger than the takeover timeout is heard when it is
read.

- `2026-11-15-j4-check.jsonl` — the broker tests of `KafkaServiceProcessTests` once each
  with their defaults (`--filter-method "*broker*"`: six records; 7 of 7 passed in
  7 min 27 s), then `*A_takeover_with_one_broker_down*` three times with
  `INCREMENTAL_KAFKA_FAULT=kill` and `INCREMENTAL_KAFKA_BROKER` 0, 1, 2 (the broker
  killed was the active controller each time).
- `2026-11-15-j4-check.measurements.log` — the lines those runs wrote to
  `measurements.log`.

## 2026-11-15, the fourth review's step

`docs/design/enrichment-service.md`, S8, *The fourth review's findings* and *The early
critic's items*. The `kafka3` profile started for the step; the build is the tree as
that step left it, but for the first three records.

- `2026-11-15-r4-review.jsonl` and `.measurements.log` — in order:
  - three runs of `*Seventy_seconds_without_a_quorum*` with `INCREMENTAL_KAFKA_FAULT=pause`
    and `INCREMENTAL_KAFKA_OUTAGE_SECONDS=45` on the build that held for brokers that do
    not answer and still counted the leader's time from the last hold: the leadership
    moved in all three;
  - one more of the same on the final rule that is **not counted**: another test was
    freezing the same brokers while it ran (the fourth record);
  - five at 45 s and three at 70 s on the final rule: nobody took over;
  - seventeen runs of `*A_takeover_with_one_broker_down*` with
    `INCREMENTAL_KAFKA_BROKER_AFTER_MS` 3050 to 3240 and `INCREMENTAL_KAFKA_BROKER` 0, 1, 2
    (the leader killed first, the broker that many milliseconds later);
  - `*Losing_one_broker_of_three*` with `INCREMENTAL_KAFKA_FAULT=kill`: twice with the
    client traced (`debug=broker`; the first as the admin client was, the second with
    `enable.sparse.connections=false` given as a client setting), three times on the
    build with that setting as the admin clients' default (`standbyBusySeconds` 0), once
    with the setting turned back on by `INCREMENTAL_KAFKA_CLIENT` — the run that fails,
    as it should (`standbyBusySeconds` 5) — and once with `INCREMENTAL_KAFKA_FAULT=pause`.
- `2026-11-15-r4-deadline-cost.txt` — what `EvictWhenFinal` costs per open trade in
  memory and in the state topic: the output of a scratch harness on the core API, and
  the topics' sizes on the broker's disk. One local broker.
- `2026-11-15-r4-service-1000-per-second.jsonl` — the enrichment example on the
  connector stack (`--kafka`, two processes, `--rate 1000`), read every ten seconds from
  both nodes' admin API and the broker's disk: rates, step times, `trade-enrichment`'s
  entries and estimated bytes on each node, the standby's state lag, both nodes'
  working set and live heap, records written and the state topic's bytes.
- `2026-11-15-r4-final-brokers.jsonl` and `.measurements.log` — the broker tests once
  each with their defaults on the step's final build (`--filter-method "*broker*"`: six
  records; 7 of 7 passed in 7 min 14 s). The profile's containers had been up for an
  hour and a half, through everything above.

## 2026-11-15, round five — a watermark per partition (K1)

`docs/design/enrichment-service.md`, *As built*, S9, *Event time where records enter*.

- `2026-11-15-k1-partitions.measurements.log` — the lines `KafkaPartitionWatermarkTests`
  wrote to `measurements.log` beside the test assembly, in the order the runs were
  made, from the first build of the step to the last. A backlog line "declared after a
  `Select`" is the same backlog read by a circuit with one time for the input, and
  the line after it the circuit under test. A broker line's figure in brackets is the
  second circuit of the same run, reading the same topic with one time for the input;
  the lines without one were written before that circuit was added to the test.
  The line of 22:34:06 (130 late, a broker killed) is the build whose source called a
  partition idle when a timer ran out; every later line is after that was mended. The
  six lines from 23:23 on are the build of commit `905ed06`.

The backlog, empty-partition and restart tests ran on the connector stack's one broker
(`docker compose --profile connectors up -d`) until 22:56 and on the `kafka3` profile
after; the broker tests on `kafka3` only:

```
INCREMENTAL_KAFKA_BOOTSTRAP=localhost:9192,localhost:9292,localhost:9392 \
INCREMENTAL_KAFKA_REPLICATION=3 \
INCREMENTAL_KAFKA_BROKERS=incremental-kafka3-1-1,incremental-kafka3-2-1,incremental-kafka3-3-1 \
  dotnet test --project tests/Incremental.Kafka.Tests -- --filter-class '*KafkaPartitionWatermarkTests'
```

A 4-processor cloud VM, 15 GB; Kafka 3.9.0 in containers on the same machine.

## 2026-11-16, round five — rolling upgrades (B6)

`docs/design/enrichment-service.md`, *As built*, S9, *Rolling upgrades across a change
of a persisted format*.

- `2026-11-16-b6-rolling-upgrade.measurements.log` — the lines
  `KafkaServiceProcessTests.The_build_from_before_format_levels_and_this_one_take_turns_until_both_are_this_one`
  wrote to `measurements.log` beside the test assembly, every run of it made in the
  step. The first three lines are the test before its last stage was added (the older
  build started after the raise); each later pair is one run of the whole test.

The older build is commit `9de8da9` — the last before round five, which writes state
manifest format 7 and has no format levels — built in a worktree; the newer one is the
step's own, at each run's time. Both as real processes
(`tests/Incremental.Service.FailoverApp`) on the connector stack's one broker:

```
git worktree add --detach <dir> 9de8da9
dotnet build <dir>/tests/Incremental.Service.FailoverApp
INCREMENTAL_OLDER_BUILD=<dir>/tests/Incremental.Service.FailoverApp/bin/Debug/net10.0/Incremental.Service.FailoverApp.dll \
  dotnet test --project tests/Incremental.Service.Tests -- --filter-method '*The_build_from_before_format_levels*'
```

Without the variable the test skips. A 4-processor cloud VM, 15 GB; Kafka 3.9.0 in a
container on the same machine.

## 2026-11-16, paced bulk loads (B2)

`docs/design/enrichment-service.md`, *As built*, S9, *Paced bulk loads*.

- `2026-11-16-b2-paced-load.tsv` — the measured series: the scale rig's `--load-only`
  at three, four and five million rows, `--no-pacing` (before) and without (after),
  alternated, on the build of the B2 commit. One line per run: the load's time, the
  leader's longest stall during the load (`leader.load.longest-stall`), the collector's
  pauses, the young collections the writer asked for and those it did not ask for
  because memory was above the ceiling, and the machine's total memory at the end of
  the run (it moved: 14.7 to 11.6 GB).
- `2026-11-16-b2-paced-load.gc.log` — those runs' collector lines (the rig's `GcLog`):
  each gen2 with its reason and kind, each pause over 200 ms. The last two pauses of a
  run are the rig's own forced collections after the load.
- `2026-11-16-b2-exploration.txt` — the runs before the pacing was built, what was
  tried, and what the collector said the stops were.

Each run: from `benchmarks/Incremental.Service.Scale/bin/Release/net10.0`,
`dotnet Incremental.Service.Scale.dll run --rows <N> --load-only [--no-pacing] --out <file>`,
on a 4-core machine with the connector stack up.

## 2026-11-16, round five — packages and the record format (B1, B4)

`docs/design/enrichment-service.md`, *As built*, S9, *Packages for plugin authors and a
record format seam*.

- `2026-11-16-b1-b4-checks.txt` — each new test made to fail by the change it guards
  against (a plugin-facing type naming Kafka; the type forwards removed; a codec held by
  a static), the failures as printed, and the suites' counts at the end of the step.
  Case 2 and the two-build test need the build from before format levels and its own
  plugin, as CLAUDE.md says (`git worktree add --detach <dir> 9de8da9`, build its
  `tests/Incremental.Service.FailoverApp` and `tests/Incremental.Service.TestPlugins/Acme.Pipelines`).

## 2026-11-16, round five — the S3 store (B5)

`docs/design/enrichment-service.md`, *As built*, S9, *An S3 object store for the files
connector*.

- `2026-11-16-b5-s3-checks.txt` — the files connector's tests on both stores, each S3
  check made to fail by the change it guards against (the abort of a failed multipart
  write removed; the node's clock in place of the listing's stamps; MinIO unreachable),
  the failures as printed, and the suites' counts at the end of the step, with CI's
  `connector-stack` job emulated here because CI's runs were refused.
  Run with the connector stack up (`docker compose --profile connectors up -d`, which
  now starts MinIO on `localhost:9000`):
  `dotnet test --project tests/Incremental.Service.Tests -- --filter-class "*FileConnectorTests"`.
