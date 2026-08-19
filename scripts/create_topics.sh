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
#   order-snapshot         spec 006 — the COMPACTED one: current state per order, plus
#                                     tombstones. The only topic here that is a table
#                                     rather than a log, which is why it is the only one
#                                     carrying extra config (006 D1).
#
# Usage note: REPLICATION_FACTOR is read from the environment and defaults to 3, the
# broker count as of spec 004. MIN_INSYNC_REPLICAS defaults to 2 and closes 004's open
# half of the acks contract (005 D12).
#
# Usage: scripts/create_topics.sh [partitions]

set -euo pipefail

# Feature topics, overridable individually so a host run can point at a scratch topic.
SNAPSHOT_TOPIC="${ORDER_SNAPSHOT_TOPIC:-order-snapshot}"
TOPICS=(
  "${ORDER_LIFECYCLE_TOPIC:-order-lifecycle}"
  "${RETRY_TOPIC:-order-lifecycle.retry}"
  "${DLQ_TOPIC:-order-lifecycle.dlq}"
  "$SNAPSHOT_TOPIC"
)
# ONE partition count for every topic, and that is load-bearing rather than tidy (006 D8).
# The consumers cache folds by partition NUMBER, so order-snapshot-2 and order-lifecycle-2
# share a cache slot. Equal counts plus the same key and partitioner make that collision
# correct — a tombstone evicts exactly the orders whose events filled the slot. Create the
# snapshot topic with a different count and the wrong orders are evicted, silently.
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

# -- spec 006: the compacted topic's cleaner knobs -----------------------------------
# These are DELIBERATELY unrealistic (006 D9). Kafka's production defaults are
# segment.ms=7d and min.cleanable.dirty.ratio=0.5, under which the cleaner skips a
# mostly-clean log and never touches the ACTIVE segment — so compaction is invisible by
# hand and the whole feature would have to be taken on trust. These values roll a segment
# every 10s and clean at 1% garbage, so superseded values disappear while you are watching.
# A real compacted topic pays a large rewrite cost for this; docs/compaction-and-tombstones.md
# prints both sets side by side.
SNAPSHOT_SEGMENT_MS="${SNAPSHOT_SEGMENT_MS:-10000}"
SNAPSHOT_MIN_CLEANABLE_DIRTY_RATIO="${SNAPSHOT_MIN_CLEANABLE_DIRTY_RATIO:-0.01}"
# How long a tombstone survives its own compaction. This is the window in which a lagging
# or restarting consumer can still learn about a delete; past it, a consumer bootstrapping
# from the topic never sees the marker and resurrects state that was deleted.
SNAPSHOT_DELETE_RETENTION_MS="${SNAPSHOT_DELETE_RETENTION_MS:-60000}"
SNAPSHOT_MIN_COMPACTION_LAG_MS="${SNAPSHOT_MIN_COMPACTION_LAG_MS:-0}"

# Per-topic extra config, newline-separated `key=value`. Only the snapshot topic has any;
# a newline delimiter rather than a comma because Kafka config VALUES can contain commas
# (`cleanup.policy=compact,delete` is one setting, not two).
declare -A EXTRA_CONFIG=()
EXTRA_CONFIG["$SNAPSHOT_TOPIC"]="cleanup.policy=compact
segment.ms=$SNAPSHOT_SEGMENT_MS
min.cleanable.dirty.ratio=$SNAPSHOT_MIN_CLEANABLE_DIRTY_RATIO
delete.retention.ms=$SNAPSHOT_DELETE_RETENTION_MS
min.compaction.lag.ms=$SNAPSHOT_MIN_COMPACTION_LAG_MS"

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
  # One list per topic, so what varies between them is data rather than a branch.
  SETTINGS=()
  [[ -n "$MIN_INSYNC_REPLICAS" ]] && SETTINGS+=("min.insync.replicas=$MIN_INSYNC_REPLICAS")
  if [[ -n "${EXTRA_CONFIG[$TOPIC]:-}" ]]; then
    while IFS= read -r SETTING; do
      [[ -n "$SETTING" ]] && SETTINGS+=("$SETTING")
    done <<< "${EXTRA_CONFIG[$TOPIC]}"
  fi

  echo "creating topic '$TOPIC' with $PARTITIONS partitions, RF $REPLICATION_FACTOR"
  ((${#SETTINGS[@]})) && printf '  config: %s\n' "${SETTINGS[@]}"

  CREATE_CONFIG=()
  for SETTING in "${SETTINGS[@]}"; do
    CREATE_CONFIG+=(--config "$SETTING")
  done

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
  # cost another `docker compose down -v` (005 D12). From 006 the same pass is what lets
  # the snapshot topic's cleaner settings be re-tuned without recreating it.
  if ((${#SETTINGS[@]})); then
    # kafka-configs.sh separates settings with commas and wants [a,b] for a list VALUE.
    # Nothing set here has a list value; if one ever does, bracket it at the source above.
    ADD_CONFIG="$(IFS=,; echo "${SETTINGS[*]}")"
    docker exec -i "$CONTAINER" /opt/kafka/bin/kafka-configs.sh \
      --bootstrap-server "$BOOTSTRAP" \
      --alter \
      --entity-type topics \
      --entity-name "$TOPIC" \
      --add-config "$ADD_CONFIG"
  fi

  echo
  docker exec -i "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --describe \
    --topic "$TOPIC"
  echo
done
