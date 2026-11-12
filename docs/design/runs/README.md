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
- Later series of the same step have the same names with their own number.
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
