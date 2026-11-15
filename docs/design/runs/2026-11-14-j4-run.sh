#!/bin/bash
# usage: run.sh <label> <filter> [env assignments...]   — one execution of the named broker test(s) on kafka3
S=/tmp/claude-1000/-home-james-code-Incremental/8a6fbf4d-beb9-4ad0-ba23-a56f521a1ee5/scratchpad/r4/j4
cd /home/james/code/Incremental
label="$1"; filter="$2"; shift; shift
export INCREMENTAL_KAFKA_BOOTSTRAP=localhost:9192,localhost:9292,localhost:9392
export INCREMENTAL_KAFKA_REPLICATION=3
export INCREMENTAL_KAFKA_BROKERS=incremental-kafka3-1-1,incremental-kafka3-2-1,incremental-kafka3-3-1
export INCREMENTAL_NODE_LOGS=$S/logs/$label
mkdir -p $S/logs/$label
for kv in "$@"; do export "$kv"; done
start=$(date -u +%Y-%m-%dT%H:%M:%SZ); hm=$(date -u +%H:%M)
echo "=== $(date -u +%H:%M:%S) $label: $filter $@" >> $S/runs.log
dotnet test --no-build --project tests/Incremental.Service.Tests -- --filter-method "$filter" 2>&1 | grep -v "^\[" | grep -v "^$" | grep -v "rdkafka" | tail -25 >> $S/runs.log
for b in 1 2 3; do docker logs --since "$start" incremental-kafka3-$b-1 > $S/logs/$label/broker$b.log 2>&1; docker exec incremental-kafka3-$b-1 sh -c 'cat /opt/kafka/logs/controller.log' 2>/dev/null | awk -v from="$hm" 'substr($2,1,5) >= from' | grep -a "Fencing broker\|Becoming the active\|unfence has been granted\|establishing a new incarnation\|handleBrokerFenced" > $S/logs/$label/controller$b.log; done
echo "=== done $(date -u +%H:%M:%S) $label" >> $S/runs.log

# --- the same against the cluster with controllers of its own ---
# usage: runsep.sh <label> <filter> [env assignments...]   — one execution on the cluster with separate controllers
S=/tmp/claude-1000/-home-james-code-Incremental/8a6fbf4d-beb9-4ad0-ba23-a56f521a1ee5/scratchpad/r4/j4
cd /home/james/code/Incremental
label="$1"; filter="$2"; shift; shift
export INCREMENTAL_KAFKA_BOOTSTRAP=localhost:9592,localhost:9692,localhost:9792
export INCREMENTAL_KAFKA_REPLICATION=3
export INCREMENTAL_KAFKA_BROKERS=kafka3sep-broker-1-1,kafka3sep-broker-2-1,kafka3sep-broker-3-1
export INCREMENTAL_NODE_LOGS=$S/logs/$label
mkdir -p $S/logs/$label
for kv in "$@"; do export "$kv"; done
hm=$(date -u +%H:%M)
echo "=== $(date -u +%H:%M:%S) $label: $filter $@" >> $S/runs.log
dotnet test --no-build --project tests/Incremental.Service.Tests -- --filter-method "$filter" 2>&1 | grep -v "^\[" | grep -v "^$" | grep -v "rdkafka" | tail -25 >> $S/runs.log
for c in 1 2 3; do docker exec kafka3sep-controller-$c-1 sh -c 'cat /opt/kafka/logs/controller.log' 2>/dev/null | awk -v from="$hm" 'substr($2,1,5) >= from' | grep -a "Fencing broker\|Becoming the active\|unfence has been granted\|establishing a new incarnation\|handleBrokerFenced" > $S/logs/$label/controller$c.log; done
echo "=== done $(date -u +%H:%M:%S) $label" >> $S/runs.log

# --- the override of the kafka3 profile used for the traced runs ---
# services:
#   kafka3-1:
#     logging: { driver: json-file }
#     environment:
#       KAFKA_LOG4J_LOGGERS: "kafka.coordinator.transaction=DEBUG"
#   kafka3-2:
#     logging: { driver: json-file }
#     environment:
#       KAFKA_LOG4J_LOGGERS: "kafka.coordinator.transaction=DEBUG"
#   kafka3-3:
#     logging: { driver: json-file }
#     environment:
#       KAFKA_LOG4J_LOGGERS: "kafka.coordinator.transaction=DEBUG"
#   docker compose -f docker-compose.yml -f override.yml --profile kafka3 up -d
