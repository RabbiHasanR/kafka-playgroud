# Kafka Playground

Single-broker Kafka 4.3.1 in KRaft mode (no ZooKeeper) for hands-on CLI learning.

| What | Where |
|---|---|
| Broker (from host) | `localhost:9092` |
| Broker (from other containers) | `kafka:19092` |
| Kafka UI | http://localhost:8080 |
| CLI tools | `/opt/kafka/bin/` inside the `kafka` container |
| Log segments on disk | `/var/lib/kafka/data/` inside the container |

## Lifecycle

```bash
docker compose up -d          # start
docker compose ps             # status (wait for kafka = healthy)
docker compose logs -f kafka  # broker logs
docker compose down           # stop, keep data
docker compose down -v        # stop, WIPE all topics/messages
```

Get a shell inside the broker — most commands below assume you're here:

```bash
docker exec -it kafka bash
cd /opt/kafka/bin
```

`BS=localhost:9092` is used as shorthand below. Set it once per shell:

```bash
export BS=localhost:9092
```

## Topics

```bash
./kafka-topics.sh --bootstrap-server $BS --create --topic demo --partitions 3 --replication-factor 1
./kafka-topics.sh --bootstrap-server $BS --list
./kafka-topics.sh --bootstrap-server $BS --describe --topic demo
./kafka-topics.sh --bootstrap-server $BS --alter --topic demo --partitions 6   # grow only, never shrink
./kafka-topics.sh --bootstrap-server $BS --delete --topic demo
```

Reading `--describe`: `Leader` is the broker serving that partition, `Replicas` all
brokers holding it, `Isr` the in-sync ones. On a single broker all three are `1`.

Auto-create is **off** — a typo gives you an error instead of a silent empty topic.

## Produce / consume

```bash
./kafka-console-producer.sh --bootstrap-server $BS --topic demo
./kafka-console-consumer.sh --bootstrap-server $BS --topic demo --from-beginning
```

Useful consumer flags:

```bash
--property print.key=true \
--property print.partition=true \
--property print.offset=true \
--property print.timestamp=true \
--max-messages 10 \
--partition 0            # read one partition only
--offset earliest        # with --partition: earliest | latest | <number>
```

Without `--from-beginning` a fresh consumer only sees messages produced *after* it starts.

## Keys and partitioning

```bash
./kafka-console-producer.sh --bootstrap-server $BS --topic demo \
  --property parse.key=true --property key.separator=:
# then type:  user1:logged in
```

Same key always hashes to the same partition — that is Kafka's only ordering
guarantee (ordering holds *within* a partition, never across).

Keyless messages are spread round-robin (sticky-batched), so ordering is not preserved.

## Consumer groups

```bash
./kafka-console-consumer.sh --bootstrap-server $BS --topic demo --group g1

./kafka-consumer-groups.sh --bootstrap-server $BS --list
./kafka-consumer-groups.sh --bootstrap-server $BS --describe --group g1
./kafka-consumer-groups.sh --bootstrap-server $BS --describe --group g1 --members --verbose
```

`--describe` columns: `CURRENT-OFFSET` (committed), `LOG-END-OFFSET` (latest written),
`LAG` = the difference. Lag is the number to watch in production.

One partition is consumed by at most one member of a group. More consumers than
partitions means the extras sit idle. Start/kill members and re-run `--describe`
to watch rebalancing.

## Offsets

The group must have **no active members** to reset:

```bash
./kafka-consumer-groups.sh --bootstrap-server $BS --group g1 --topic demo --reset-offsets --to-earliest --execute
./kafka-consumer-groups.sh --bootstrap-server $BS --group g1 --topic demo --reset-offsets --to-latest --execute
./kafka-consumer-groups.sh --bootstrap-server $BS --group g1 --topic demo --reset-offsets --shift-by -10 --execute
./kafka-consumer-groups.sh --bootstrap-server $BS --group g1 --topic demo --reset-offsets --to-offset 42 --execute
./kafka-consumer-groups.sh --bootstrap-server $BS --group g1 --topic demo --reset-offsets --to-datetime 2026-08-09T00:00:00.000 --execute
```

Swap `--execute` for `--dry-run` to preview.

## Inspecting the raw log

```bash
ls /var/lib/kafka/data/demo-0/
./kafka-dump-log.sh --files /var/lib/kafka/data/demo-0/00000000000000000000.log --print-data-log
```

Shows real record batches, offsets, timestamps, and compression — the physical
form of the "log" abstraction.

Per-partition offset boundaries without consuming:

```bash
./kafka-get-offsets.sh --bootstrap-server $BS --topic demo               # latest
./kafka-get-offsets.sh --bootstrap-server $BS --topic demo --time earliest
```

## Retention and compaction

```bash
./kafka-topics.sh --bootstrap-server $BS --create --topic compacted --partitions 1 --replication-factor 1 \
  --config cleanup.policy=compact \
  --config min.cleanable.dirty.ratio=0.01 \
  --config segment.ms=5000 \
  --config delete.retention.ms=100

./kafka-configs.sh --bootstrap-server $BS --entity-type topics --entity-name demo --describe
./kafka-configs.sh --bootstrap-server $BS --entity-type topics --entity-name demo \
  --alter --add-config retention.ms=60000
```

Compaction keeps only the newest value per key. It runs on *closed* segments, so
you must produce enough to roll a segment (hence the tiny `segment.ms`) before
old values disappear. A key with a `null` value is a tombstone — it deletes the key.

## Other tools

```bash
./kafka-broker-api-versions.sh --bootstrap-server $BS   # connectivity check
./kafka-cluster.sh cluster-id --bootstrap-server $BS
./kafka-producer-perf-test.sh --topic demo --num-records 100000 --record-size 100 --throughput -1 --producer-props bootstrap.servers=$BS
./kafka-consumer-perf-test.sh --bootstrap-server $BS --topic demo --messages 100000
```

## Gotchas

- **`advertised.listeners` is what clients actually connect to.** The broker hands
  this address back on connect, so a client that reaches the broker fine can still
  fail on the next call if the advertised address is wrong. Hence the two listeners:
  `localhost:9092` for the host, `kafka:19092` for containers.
- **Partition count can only grow.** Growing it rehashes keys, so existing keys may
  move to a different partition and their historical ordering is broken.
- **`--from-beginning` is ignored when the group already has committed offsets.**
  Reset the offsets or use a new `--group` name.
- **`docker compose down -v` deletes the volume** and every message with it.

---

# Spec 001 — Prepaid Order Service

A prepaid order is placed in one HTTP call; the service records it and publishes
`ORDER_CREATED` to `order-lifecycle` (3 partitions, keyed by `order_id`). Three
services — inventory, notification, analytics — each consume that event **in their own
consumer group**, so all three see every message. The order is then advanced through
`PACKED → SHIPPED → DELIVERED`, one event at a time, each fanning out again.

Read [docs/order-flow.md](docs/order-flow.md) first — it is one page and covers the
whole flow with runnable commands. Full spec in
[specs/001-prepaid-order-service/](specs/001-prepaid-order-service/requirements.md).

Three things it demonstrates:

- **Key → partition.** Every event is keyed by `order_id`, so one order's events land
  on one partition and stay ordered. Different orders have no ordering guarantee
  between them, and that is correct rather than a limitation.
- **Fan-out by consumer group.** One topic, three group ids, three independent
  offsets. [Spec 002](#spec-002--consumer-groups-rebalancing-and-partition-assignment)
  does the opposite — extra consumers in *one* group, where the messages divide instead
  of duplicating.
- **The synchronous/asynchronous boundary.** `POST /orders` blocks because the caller
  needs an `order_id` back. Everything downstream of the event does not, and happens
  off the log.

## Running it

```bash
docker compose up -d --build            # broker, UI, order service, 3 consumers
./scripts/create_topics.sh              # creates order-lifecycle (auto-create is off)
docker compose logs -f inventory-consumer notification-consumer analytics-consumer
```

From the host instead, four terminals:

```bash
.venv/bin/python -m order_service.producer.app                        # :8010
SERVICE_NAME=inventory    .venv/bin/python -m order_service.consumer.main
SERVICE_NAME=notification .venv/bin/python -m order_service.consumer.main
SERVICE_NAME=analytics    .venv/bin/python -m order_service.consumer.main
```

| Endpoint (`:8010`) | Purpose |
|---|---|
| `POST /orders` | create a prepaid order; `422` if the payment ≠ the item sum |
| `POST /orders/{order_id}/events` | advance it; `409` if the transition is illegal |
| `GET /orders/{order_id}` | the service's own record of the order |

Advancing one order through the chain, with `ORDER` holding the id `POST /orders`
returned. Watch the three consumer logs between each call — every one of them sees
every event, and they land on the same partition because the key is the `order_id`:

```bash
curl -sX POST localhost:8010/orders/$ORDER/events -H 'content-type: application/json' \
  -d '{"event_type":"PACKED"}'

curl -sX POST localhost:8010/orders/$ORDER/events -H 'content-type: application/json' \
  -d '{"event_type":"SHIPPED","payload":{"carrier":"Pathao","tracking_number":"PT-1"}}'

curl -sX POST localhost:8010/orders/$ORDER/events -H 'content-type: application/json' \
  -d '{"event_type":"DELIVERED"}'

curl -s localhost:8010/orders/$ORDER
```

The full walkthrough, including the failure cases, is in
[docs/order-flow.md](docs/order-flow.md).

The order service **guards its own lifecycle** — asking for `SHIPPED` on an unpacked
order is a `409`, not a message on a topic. Pass `"force": true` to bypass the guard
and put a genuinely out-of-order event on the log; all three services will report an
`ILLEGAL_TRANSITION` violation with no accompanying sequence gap.

## Known gaps, all deliberate

| Gap | Closed by |
|---|---|
| Orders held in memory — a restart forgets them | accepted (a real service uses a database) |
| No transactional outbox, so the record and the event cannot be atomic | out of scope; explained in the flow doc |
| Consumer fold state lost on restart | 003 |
| Duplicate processing after a crash (at-least-once) | 003, 008 |
| No deduplication, though every event carries an `event_id` | 003, 008 |

---

# Spec 002 — Consumer Groups, Rebalancing, and Partition Assignment

001 put three **group ids** on one topic and every service saw every message. 002 puts
three **members in one group** and the messages divide. Both run at once, on the same
topic: `notification` scales to three instances while `inventory` and `analytics` stay
single-instance as the control.

Read [docs/consumer-groups.md](docs/consumer-groups.md) — it has the measured numbers.
Full spec in [specs/002-consumer-groups-rebalancing/](specs/002-consumer-groups-rebalancing/requirements.md).

```bash
docker compose up -d --build              # group starts with ONE notification member
./scripts/create_topics.sh                # required after `down -v`
docker compose --profile scale-out up -d  # grow it to three, while watching the logs
./scripts/place_orders.sh 12 --advance    # 12 orders × 4 events, no curl by hand

docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group notification-service --members --verbose
```

Tear down with `docker compose --profile scale-out down` — a plain `down` orphans
`notification-consumer-2` and `-3`.

Four things it demonstrates:

- **Scale-out divides, fan-out duplicates.** 12 of 12 orders went to exactly one group
  member each, while inventory and analytics each received all 12 complete.
- **Ordering survives parallelism.** The key pins an order to a partition and the
  partition to one member, so all four of an order's events are handled by one consumer
  in sequence — 12 of 12, with three consumers running.
- **A rebalance costs whatever it revokes.** Killing one member under the default `range`
  assignor destroyed the folded state of **6 of 6** in-flight orders; under
  `cooperative-sticky` the same scenario cost **3 of 9** — only the partition that moved.
- **The offset is not the memory.** Kafka restored every position perfectly and restored
  no derived state at all. That gap is what spec 003 exists to close.

| Lever | What it causes |
|---|---|
| `CONSUMER_ASSIGNMENT_STRATEGY=cooperative-sticky` | revoke only what moves |
| `CONSUMER_GROUP_PROTOCOL=consumer` | KIP-848 — the broker assigns, not a client |
| `HANDLER_DELAY_SECONDS=12` + a low `CONSUMER_MAX_POLL_INTERVAL_MS` | a live, healthy consumer evicted from its group, then livelocked |
| `STATIC_MEMBERSHIP=1` | a restart that costs **0** rebalances instead of 8 |

## Known gaps, all deliberate

| Gap | Closed by |
|---|---|
| A moved partition loses its fold → false `SEQUENCE_GAP` | 003, fully by 007 |
| Rebalance duration and consumer lag are never measured | needs a load generator, excluded here |
| Partition growth and key rehashing | a topic-level lesson, not a consumer-group one |
| Single broker, RF 1 | 004 |
