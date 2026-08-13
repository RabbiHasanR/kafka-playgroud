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
# Usage: scripts/create_topics.sh [partitions]

set -euo pipefail

# Feature topics, overridable individually so a host run can point at a scratch topic.
TOPICS=(
  "${ORDER_LIFECYCLE_TOPIC:-order-lifecycle}"
)
PARTITIONS="${1:-3}"
# Replication factor is pinned to 1 by the single-broker environment from spec 000.
# Spec 004 raises it once there is a real cluster.
REPLICATION_FACTOR=1
CONTAINER="${KAFKA_CONTAINER:-kafka}"
BOOTSTRAP="${KAFKA_INTERNAL_BOOTSTRAP:-localhost:9092}"

if ! docker compose ps --status running --services 2>/dev/null | grep -qx kafka; then
  echo "broker is not running — start it with: docker compose up -d" >&2
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
