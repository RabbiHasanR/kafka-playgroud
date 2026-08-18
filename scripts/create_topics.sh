#!/usr/bin/env bash
# Create the topics the application features need (R1.9).
#
# Auto-creation is off in docker-compose.yml (R0.14 / R1.11), so this is a required
# setup step rather than a convenience — producing to a topic that does not exist fails
# loudly instead of silently creating a 3-partition topic behind your back.
#
# One entry per feature topic; a later spec adds to the array rather than adding a
# second script.
#   order-lifecycle  spec 001 — the prepaid order service and its three consumers
#
# Usage note: REPLICATION_FACTOR is read from the environment and defaults to 3, the
# broker count as of spec 004.
#
# Usage: scripts/create_topics.sh [partitions]

set -euo pipefail

# Feature topics, overridable individually so a host run can point at a scratch topic.
TOPICS=(
  "${ORDER_LIFECYCLE_TOPIC:-order-lifecycle}"
)
PARTITIONS="${1:-3}"
# Replication factor is a property of the TOPIC, not of the cluster (004 D4, R4.4).
# Three brokers means RF 3 is available, not that it is compulsory — creating a topic
# at RF 1 here is exactly how R4.6 is demonstrated:
#   REPLICATION_FACTOR=1 ORDER_LIFECYCLE_TOPIC=rf1-scratch scripts/create_topics.sh
# then stop the node leading one of its partitions and watch that partition go offline
# while order-lifecycle at RF 3 carries on.
REPLICATION_FACTOR="${REPLICATION_FACTOR:-3}"
CONTAINER="${KAFKA_CONTAINER:-kafka}"
BOOTSTRAP="${KAFKA_INTERNAL_BOOTSTRAP:-localhost:9092}"

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

for TOPIC in "${TOPICS[@]}"; do
  echo "creating topic '$TOPIC' with $PARTITIONS partitions, RF $REPLICATION_FACTOR"

  docker exec -i "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --create \
    --if-not-exists \
    --topic "$TOPIC" \
    --partitions "$PARTITIONS" \
    --replication-factor "$REPLICATION_FACTOR"

  echo
  docker exec -i "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --describe \
    --topic "$TOPIC"
  echo
done
