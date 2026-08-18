#!/usr/bin/env bash
# Create the topics the application features need (R1.9).
#
# Auto-creation is off in docker-compose.yml (R0.14 / R1.11), so this is a required
# setup step rather than a convenience — producing to a topic that does not exist fails
# loudly instead of silently creating a 3-partition topic behind your back.
#
# One entry per feature topic; a later spec adds to the array rather than adding a
# second script.
#   order-lifecycle        spec 001 — the prepaid order service and its three consumers
#   order-lifecycle.retry  spec 005 — where a retryable failure waits out its backoff
#   order-lifecycle.dlq    spec 005 — where a message nothing could process ends up
#
# Usage note: REPLICATION_FACTOR is read from the environment and defaults to 3, the
# broker count as of spec 004. MIN_INSYNC_REPLICAS defaults to 2 and closes 004's open
# half of the acks contract (005 D12).
#
# Usage: scripts/create_topics.sh [partitions]

set -euo pipefail

# Feature topics, overridable individually so a host run can point at a scratch topic.
TOPICS=(
  "${ORDER_LIFECYCLE_TOPIC:-order-lifecycle}"
  "${RETRY_TOPIC:-order-lifecycle.retry}"
  "${DLQ_TOPIC:-order-lifecycle.dlq}"
)
PARTITIONS="${1:-3}"
# Replication factor is a property of the TOPIC, not of the cluster (004 D4, R4.4).
# Three brokers means RF 3 is available, not that it is compulsory — creating a topic
# at RF 1 here is exactly how R4.6 is demonstrated:
#   REPLICATION_FACTOR=1 ORDER_LIFECYCLE_TOPIC=rf1-scratch scripts/create_topics.sh
# then stop the node leading one of its partitions and watch that partition go offline
# while order-lifecycle at RF 3 carries on.
REPLICATION_FACTOR="${REPLICATION_FACTOR:-3}"
# The other half of the acks contract, deliberately left open by 004 D8 and closed here
# (005 D12, R5.20). `acks=all` means "every replica currently IN SYNC" — so with this
# unset, an ISR that has shrunk to one member still satisfies it and an acknowledged
# write can exist in exactly one copy. min.insync.replicas is the floor under that.
#
# Guarded against REPLICATION_FACTOR below: a topic whose RF is lower than its
# min.insync.replicas can never be written to at all, which would silently break 004's
# RF-1 scratch-topic demonstration rather than teaching anything.
MIN_INSYNC_REPLICAS="${MIN_INSYNC_REPLICAS:-2}"
CONTAINER="${KAFKA_CONTAINER:-kafka}"
# INTERNAL, not localhost:9092. This script always runs via `docker exec`, and inside a
# container `localhost` is that container. Bootstrapping on the EXTERNAL listener makes the
# broker hand back everyone's advertised EXTERNAL addresses (localhost:9094, localhost:9095),
# which resolve to the wrong container and produce a wall of "node may not be available"
# WARNs. Correct from the host, meaningless from inside. The INTERNAL addresses resolve via
# compose DNS from any container, so the AdminClient reaches all three nodes.
BOOTSTRAP="${KAFKA_INTERNAL_BOOTSTRAP:-kafka:19092}"

# Every node, not just the one we exec into. A topic created at RF 3 while two brokers
# are still starting fails with INVALID_REPLICATION_FACTOR — an error that reads like a
# bad argument rather than a half-started cluster, so it is worth catching here (004 D1).
BROKERS=(kafka kafka-2 kafka-3)
missing=()
for BROKER in "${BROKERS[@]}"; do
  docker compose ps --status running --services 2>/dev/null | grep -qx "$BROKER" || missing+=("$BROKER")
done
if ((${#missing[@]})); then
  echo "not running: ${missing[*]}" >&2
  echo "start the cluster with: docker compose up -d" >&2
  echo "if the quorum was just changed, it needs: docker compose down -v && docker compose up -d" >&2
  exit 1
fi

# A min.insync.replicas above the replication factor makes a topic unwritable, so an
# RF-1 scratch topic gets no floor at all rather than an impossible one.
if ((MIN_INSYNC_REPLICAS > REPLICATION_FACTOR)); then
  echo "min.insync.replicas $MIN_INSYNC_REPLICAS exceeds RF $REPLICATION_FACTOR — not setting it" >&2
  MIN_INSYNC_REPLICAS=""
fi

for TOPIC in "${TOPICS[@]}"; do
  echo "creating topic '$TOPIC' with $PARTITIONS partitions, RF $REPLICATION_FACTOR"

  CREATE_CONFIG=()
  [[ -n "$MIN_INSYNC_REPLICAS" ]] &&
    CREATE_CONFIG=(--config "min.insync.replicas=$MIN_INSYNC_REPLICAS")

  docker exec -i "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --create \
    --if-not-exists \
    --topic "$TOPIC" \
    --partitions "$PARTITIONS" \
    --replication-factor "$REPLICATION_FACTOR" \
    "${CREATE_CONFIG[@]}"

  # --if-not-exists SKIPS an existing topic entirely, --config included. order-lifecycle
  # already exists from spec 004, so without this second pass min.insync.replicas would
  # never reach the one topic the whole feature is about — and closing 004's gap would
  # cost another `docker compose down -v` (005 D12).
  if [[ -n "$MIN_INSYNC_REPLICAS" ]]; then
    docker exec -i "$CONTAINER" /opt/kafka/bin/kafka-configs.sh \
      --bootstrap-server "$BOOTSTRAP" \
      --alter \
      --entity-type topics \
      --entity-name "$TOPIC" \
      --add-config "min.insync.replicas=$MIN_INSYNC_REPLICAS"
  fi

  echo
  docker exec -i "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --describe \
    --topic "$TOPIC"
  echo
done
