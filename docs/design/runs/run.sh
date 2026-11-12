#!/bin/bash
# usage: run.sh <label> <filter> [env assignments...]   — one execution of the named broker test(s) on kafka3
S=/tmp/claude-1000/-home-james-code-Incremental/10135b76-ee3e-4c6e-ae52-b74a1cf01b22/scratchpad/fix/k3
cd /home/james/code/Incremental
label="$1"; filter="$2"; shift; shift
export INCREMENTAL_KAFKA_BOOTSTRAP=localhost:9192,localhost:9292,localhost:9392
export INCREMENTAL_KAFKA_REPLICATION=3
export INCREMENTAL_KAFKA_BROKERS=incremental-kafka3-1-1,incremental-kafka3-2-1,incremental-kafka3-3-1
export INCREMENTAL_NODE_LOGS=$S/logs
for kv in "$@"; do export "$kv"; done
echo "=== $(date -u +%H:%M:%S) $label: $filter $@" >> $S/runs.log
dotnet test --no-build --project tests/Incremental.Service.Tests -- --filter-method "$filter" 2>&1 | grep -v "^\[" | grep -v "^$" | grep -v "rdkafka" | tail -25 >> $S/runs.log
