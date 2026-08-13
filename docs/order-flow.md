# The order flow (spec 001)

What this project's order service actually does, end to end.

**In one line:** a prepaid order is placed in a single HTTP call, and three independent
services react to the resulting event without knowing about one another.

The order domain is a familiar backdrop; what is actually being studied is Kafka — that
the message key pins an order's events to one partition and keeps them ordered, and
that three consumer groups on one topic each get every message.

## Synchronous vs. asynchronous

This is the split that matters, and the one Kafka diagrams usually blur.

```
SYNCHRONOUS — the caller is blocked, waiting for this answer
──────────────────────────────────────────────────────────────
  POST /orders
     ├─ validate: total == Σ(qty × unit_price) == payment.amount
     ├─ publish ORDER_CREATED, wait for the broker to acknowledge
     ├─ record the order as CREATED, sequence 1
     └─ 201 { order_id, partition, offset }


ASYNCHRONOUS — consequences; nobody is waiting for these
──────────────────────────────────────────────────────────────
                    order-lifecycle  (3 partitions, key = order_id)
                                │
        ┌───────────────────────┼───────────────────────┐
   inventory-service    notification-service     analytics-service
   group=inventory-     group=notification-      group=analytics-
         service              service                  service
```

**The rule:** if a human or a client is blocked waiting for the output, it is a
synchronous call. Kafka carries facts you are announcing, not answers you need back.

The order service does *not* know the three consumers exist. Adding a fourth is a new
container and no change here at all — that decoupling is the entire reason the event
goes on a topic instead of into three HTTP calls.

## Fan-out: three groups, one topic

Each service joins its **own consumer group**, so Kafka keeps three independent offsets
and every service sees every message.

| | Fan-out (001, here) | Scale-out (002, next) |
|---|---|---|
| Setup | 3 consumers, 3 groups | 3 consumers, 1 group |
| Who gets a message | all of them | exactly one of them |
| Used for | different services reacting | one service going faster |

Stopping one service cannot affect the others, because their offsets were never shared.
That is verifiable — see the walkthrough below.

## Who reacts to what

| Event | inventory | notification | analytics |
|---|---|---|---|
| `ORDER_CREATED` | reserves stock per line item | "Order confirmed" | counts |
| `PACKED` | — | "Packed and waiting for pickup" | counts |
| `SHIPPED` | commits the reservation | "On its way, tracking …" | counts |
| `DELIVERED` | — | "Delivered" | counts |

Inventory is subscribed to the same topic as the others and *receives* all four events.
Caring about only two is a property of its handler map, not its subscription.

The side effects are log lines. Reserving real stock is not what this feature is about.

## The lifecycle, and who guards it

```
CREATED ──PACKED──► PACKED ──SHIPPED──► SHIPPED ──DELIVERED──► DELIVERED
```

The **order service owns this chain**. Asking it to publish `SHIPPED` for an order that
was never packed gets a `409`, and nothing reaches the topic — a real service refuses a
transition its aggregate cannot make, rather than publishing whatever it is handed.

`force: true` bypasses that guard. It exists so the consumers' detection is reachable —
a service that never emits an illegal transition gives them nothing to detect. A forced
event advances the sequence but **not** the recorded state, because the transition it
describes cannot have happened.

## Walkthrough

Start everything and create the topics:

```bash
docker compose up -d --build
scripts/create_topics.sh
docker compose logs -f inventory-consumer notification-consumer analytics-consumer
```

In a second terminal:

```bash
# 1. Place a prepaid order. 2 × 15000 + 1 × 4500 = 34500.
curl -sX POST localhost:8010/orders -H 'content-type: application/json' -d '{
  "customer_id": "cust-1",
  "items": [
    {"sku": "SKU-1", "qty": 2, "unit_price": 15000},
    {"sku": "SKU-2", "qty": 1, "unit_price": 4500}
  ],
  "payment": {"method": "BKASH", "reference": "TRX123", "amount": 34500}
}'
# → 201 {"order_id":"ord-…","state":"CREATED","total_amount":34500,
#        "sequence":1,"partition":2,"offset":0}
```

All three consumer logs now show the same event, each doing different work. Save the id:

```bash
ORDER=ord-...        # paste from the response above
```

```bash
# 2. A wrong total is rejected before anything is published.
curl -sX POST localhost:8010/orders -H 'content-type: application/json' -d '{
  "customer_id": "cust-2",
  "items": [{"sku": "SKU-1", "qty": 1, "unit_price": 15000}],
  "payment": {"method": "CARD", "reference": "TRX999", "amount": 9999}
}'
# → 422  payment.amount 9999 does not equal total_amount 15000

# 3. Skipping a step is refused, and nothing is published.
curl -sX POST localhost:8010/orders/$ORDER/events \
  -H 'content-type: application/json' \
  -d '{"event_type": "SHIPPED", "payload": {"carrier":"Pathao","tracking_number":"PT-1"}}'
# → 409  order ord-… is CREATED; expected PACKED but got SHIPPED

# 4. The real chain, one event at a time. Watch all three logs after each.
curl -sX POST localhost:8010/orders/$ORDER/events \
  -H 'content-type: application/json' -d '{"event_type": "PACKED"}'

curl -sX POST localhost:8010/orders/$ORDER/events \
  -H 'content-type: application/json' \
  -d '{"event_type": "SHIPPED", "payload": {"carrier":"Pathao","tracking_number":"PT-1"}}'

curl -sX POST localhost:8010/orders/$ORDER/events \
  -H 'content-type: application/json' -d '{"event_type": "DELIVERED"}'

# 5. The service's own view of the order.
curl -s localhost:8010/orders/$ORDER
```

### Breaking it on purpose

```bash
# Force an out-of-order event onto the topic.
NEW=$(curl -sX POST localhost:8010/orders -H 'content-type: application/json' -d '{
  "customer_id": "cust-3",
  "items": [{"sku": "SKU-1", "qty": 1, "unit_price": 15000}],
  "payment": {"method": "NAGAD", "reference": "TRX7", "amount": 15000}
}' | grep -o 'ord-[a-f0-9]*')

curl -sX POST localhost:8010/orders/$NEW/events \
  -H 'content-type: application/json' \
  -d '{"event_type":"DELIVERED","force":true}'

docker compose logs --since 1m | grep VIOLATION
```

All three services log an `ILLEGAL_TRANSITION` violation — and **no** sequence gap,
because the forced event still took the next contiguous sequence. The two signals are
independent on purpose. `GET /orders/$NEW` shows the recorded state never left
`CREATED`.

### Proving the fan-out is real

```bash
# Three groups, three independent offsets, one topic.
docker exec -i kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --all-groups

# Stop one service, advance an order, and watch the other two keep up.
docker compose stop notification-consumer
curl -sX POST localhost:8010/orders/$NEW/events \
  -H 'content-type: application/json' -d '{"event_type": "PACKED"}'
docker compose start notification-consumer   # catches up from its own offset
```

## What this deliberately does not do

| Missing | Why |
|---|---|
| A database | Orders live in memory; restarting the service forgets them |
| A transactional outbox | Without a DB there is no dual write to solve — the production fix is to write the event to an `outbox` table in the same transaction as the order, and relay it |
| Deduplication | Delivery is at-least-once. Every event carries an `event_id` for the day it matters (specs 003, 008) |
| Bulk generation | No load generator, so no lag or throughput experiment yet |
| A real payment gateway | Payment is already settled when the order arrives; the webhook-driven variant is a different flow |

Full criteria in [`specs/001-prepaid-order-service/`](../specs/001-prepaid-order-service/requirements.md).
