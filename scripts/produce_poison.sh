#!/usr/bin/env bash
# Publish a genuinely malformed message to the lifecycle topic (005 R5.2, D11).
#
# HANDLER_FAILURE_MODE=poison makes a *handler* raise, which exercises the routing but
# proves nothing about the decoder — the bytes were fine and the event parsed. This
# writes bytes that cannot become a LifecycleEvent no matter how many times they are
# read, which is the actual definition of a poison message.
#
# What you should see: the consumer logs POISON_MESSAGE then DLQ_PUBLISHED, commits, and
# carries on with the next message. Attempts made: 1. The retry topic is never touched,
# because retrying malformed bytes produces the identical exception every time.
#
# Usage:
#   scripts/produce_poison.sh                    # not JSON at all
#   scripts/produce_poison.sh schema             # valid JSON, wrong shape
#   scripts/produce_poison.sh '<your own bytes>' # anything else
#
# The key is an order id, so the message lands on the same partition that order's real
# events do — which is what makes the block-then-unblock visible on one partition.

set -euo pipefail

TOPIC="${ORDER_LIFECYCLE_TOPIC:-order-lifecycle}"
CONTAINER="${KAFKA_CONTAINER:-kafka}"
BOOTSTRAP="${KAFKA_INTERNAL_BOOTSTRAP:-kafka:19092}"
KEY="${POISON_KEY:-poison-order-1}"

case "${1:-notjson}" in
  # Fails at json.loads — the bytes are not JSON.
  notjson) PAYLOAD='this is not json at all }{' ;;
  # Fails at model_validate — valid JSON, but no order_id, sequence or event_type.
  schema)  PAYLOAD='{"order_id":"poison-order-1","this_field":"is not the schema"}' ;;
  *)       PAYLOAD="$1" ;;
esac

echo "producing poison to '$TOPIC' with key '$KEY':"
echo "  $PAYLOAD"
echo

printf '%s:%s\n' "$KEY" "$PAYLOAD" |
  docker exec -i "$CONTAINER" /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --topic "$TOPIC" \
    --property "parse.key=true" \
    --property "key.separator=:"

echo
echo "now watch a consumer give up on it:"
echo "  docker compose logs -f inventory-consumer | grep -E 'POISON_MESSAGE|DLQ_PUBLISHED'"
