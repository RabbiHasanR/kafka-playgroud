#!/usr/bin/env bash
# Place N prepaid orders against a running order service (R2.31, R2.32).
#
# This exists because a three-way partition split is illegible at four orders and
# obvious at twenty, and typing twenty curl calls teaches nothing.
#
# It is deliberately NOT a load generator (R2.33, 002 D11). No rate control, no
# concurrency, no throughput reporting — one request at a time, printing what the
# broker did with each. The lag and throughput experiments that need a real generator
# belong to a feature that asks for one; this is a typing aid.
#
# Usage:
#   scripts/place_orders.sh [count] [--advance]
#
#   count      how many orders to create (default 10)
#   --advance  also walk each order through PACKED -> SHIPPED -> DELIVERED
#
# Environment:
#   ORDER_SERVICE_URL  default http://localhost:8010

set -euo pipefail

COUNT="${1:-10}"
ADVANCE="no"
for arg in "$@"; do
  [[ "$arg" == "--advance" ]] && ADVANCE="yes"
done

BASE_URL="${ORDER_SERVICE_URL:-http://localhost:8010}"
UNIT_PRICE=1000
QTY=1

if ! [[ "$COUNT" =~ ^[0-9]+$ ]] || ((COUNT < 1)); then
  echo "count must be a positive integer, got '$COUNT'" >&2
  exit 2
fi

if ! curl -fsS "$BASE_URL/health" >/dev/null 2>&1; then
  echo "order service is not answering at $BASE_URL — start it with: docker compose up -d" >&2
  exit 1
fi

# One field out of a JSON object, without depending on jq — every other script here
# runs against a bare shell and this one should too (D11).
json_field() {
  local body="$1" field="$2"
  printf '%s' "$body" | sed -n "s/.*\"$field\":\"\{0,1\}\([^,\"}]*\)\"\{0,1\}.*/\1/p"
}

publish() {
  local order_id="$1" event_type="$2" payload="$3"
  local response
  response=$(curl -fsS -X POST "$BASE_URL/orders/$order_id/events" \
    -H 'content-type: application/json' \
    -d "{\"event_type\":\"$event_type\",\"payload\":$payload}") || return 1
  printf '    seq=%s %-13s -> partition %s offset %s\n' \
    "$(json_field "$response" sequence)" \
    "$event_type" \
    "$(json_field "$response" partition)" \
    "$(json_field "$response" offset)"
}

echo "placing $COUNT order(s) against $BASE_URL (advance=$ADVANCE)"
echo

created=0
for ((i = 1; i <= COUNT; i++)); do
  total=$((QTY * UNIT_PRICE))
  # The payment must equal the item sum or the service returns 422 (R1.14).
  response=$(curl -fsS -X POST "$BASE_URL/orders" \
    -H 'content-type: application/json' \
    -d "{\"customer_id\":\"cust-$i\",
         \"items\":[{\"sku\":\"PEN\",\"qty\":$QTY,\"unit_price\":$UNIT_PRICE}],
         \"payment\":{\"method\":\"CARD\",\"reference\":\"pay-$i\",\"amount\":$total}}") \
    || { echo "order $i failed" >&2; continue; }

  order_id=$(json_field "$response" order_id)
  printf '%-22s ORDER_CREATED -> partition %s offset %s\n' \
    "$order_id" \
    "$(json_field "$response" partition)" \
    "$(json_field "$response" offset)"
  created=$((created + 1))

  if [[ "$ADVANCE" == "yes" ]]; then
    publish "$order_id" PACKED '{}'
    publish "$order_id" SHIPPED '{"carrier":"Pathao","tracking_number":"PT-'"$i"'"}'
    publish "$order_id" DELIVERED '{}'
  fi
done

echo
if [[ "$ADVANCE" == "yes" ]]; then
  echo "created $created order(s), each advanced to DELIVERED — $((created * 4)) events total"
else
  echo "created $created order(s) — $created events total"
fi
