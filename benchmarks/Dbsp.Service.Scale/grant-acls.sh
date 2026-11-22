#!/bin/sh
# Grants a service node's principal what it needs on the kafka-sasl compose broker, and
# nothing else: the broker denies whatever no ACL allows.
#
#   grant-acls.sh <node-principal> <topic-prefix> [<rig-principal>]
#
# The node's topics are the deployment's own ({prefix}.state, {prefix}.leader,
# {prefix}.table.*, {prefix}.svc.blobs), the topics its pipelines read ({prefix}.orders,
# {prefix}.in.*) and the ones they write ({prefix}.out.*) — the rig's names; a real
# deployment's source and sink topics are whatever its specs bind. The rig's own
# principal creates those topics, produces the input and reads the sinks.
set -e
NODE=$1
PREFIX=$2
RIG=${3:-}
CONTAINER=${KAFKA_CONTAINER:-dbsp-kafka-sasl-1}
acl() {
    principal=$1
    shift
    docker exec "$CONTAINER" /opt/kafka/bin/kafka-acls.sh --bootstrap-server localhost:19092 --add \
        --allow-principal "User:$principal" "$@" > /dev/null
}

# The deployment's own topics: it creates them, reads their settings, reads and writes
# them, and trims a table's history (DeleteRecords needs Delete).
for own in state leader table. svc.blobs; do
    acl "$NODE" --operation Create --operation Describe --operation DescribeConfigs \
        --operation Read --operation Write --operation Delete \
        --topic "$PREFIX.$own" --resource-pattern-type prefixed
done

# The one transactional id every node of the deployment shares: that is the fence.
acl "$NODE" --operation Describe --operation Write --transactional-id "$PREFIX.writer"

# What its pipelines and table loaders read, and what its sinks write.
acl "$NODE" --operation Describe --operation Read --topic "$PREFIX.orders"
acl "$NODE" --operation Describe --operation Read --topic "$PREFIX.in." --resource-pattern-type prefixed
acl "$NODE" --operation Describe --operation Write --topic "$PREFIX.out." --resource-pattern-type prefixed

# No group: nothing the service creates joins one or commits an offset to Kafka.
# No cluster permission: an idempotent producer needs none on Kafka 3.x (IdempotentWrite
# is implied by Write on a topic). On MSK IAM, kafka-cluster:WriteDataIdempotently is its
# own action and is required.

if [ -n "$RIG" ]; then
    acl "$RIG" --operation All --topic "$PREFIX." --resource-pattern-type prefixed
fi
echo "granted: node principal $NODE on $PREFIX.*${RIG:+, rig principal $RIG}"
