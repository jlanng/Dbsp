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
