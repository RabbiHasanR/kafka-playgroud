# Kafka Playground

A three-broker Kafka 4.3.1 cluster in KRaft mode (no ZooKeeper) for hands-on CLI learning.
It was a single node through spec 003; [spec 004](specs/004-replication-acks-failover/requirements.md)
grew it to three.

| What | Where |
|---|---|
| Brokers (from host) | `localhost:9092`, `localhost:9094`, `localhost:9095` |
| Brokers (from other containers) | `kafka:19092`, `kafka-2:19092`, `kafka-3:19092` |
| Kafka UI | http://localhost:8080 |
| CLI tools | `/opt/kafka/bin/` inside any broker container |
| Log segments on disk | `/var/lib/kafka/data/` inside each container |

Node ids are 1, 2, 3; the container names are `kafka`, `kafka-2`, `kafka-3`. The first keeps
its original name so every `docker exec kafka …` below still works.

## Lifecycle

```bash
docker compose up -d          # start
docker compose ps             # status (wait for kafka = healthy)
docker compose logs -f kafka  # broker logs
docker compose down           # stop, keep data
docker compose down -v        # stop, WIPE all topics/messages
```

**Upgrading from the single-broker era needs `down -v` once.** KRaft writes the controller
quorum into the metadata log at format time, so the old one-voter volume cannot grow into a
three-voter cluster. Run `docker compose down -v && docker compose up -d && ./scripts/create_topics.sh`.
See [docs/replication.md §6](docs/replication.md).

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
| Consumer fold state lost on restart | **closed by 003** |
| Duplicate processing after a crash (at-least-once) | 008 — 003 absorbs it in the *state*, not in the side effect |
| No deduplication, though every event carries an `event_id` | 008 — 003 gets idempotency from `sequence` instead |

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
| Single broker, RF 1 | **closed by 004** |

---

# Spec 003 — Durable Consumer State

001 and 002 both ended with the same false alarm: a consumer that resumed at exactly the
right offset and reported a `SEQUENCE_GAP` for events it had already seen. **Kafka
remembers your position; nothing remembered your memory.** 003 moves the fold into
Postgres, keyed by `(group_id, order_id)` — so it belongs to the *order*, not to whoever
holds the partition — and the false alarm stops.

Read [docs/durable-state.md](docs/durable-state.md) — it has the measured numbers.
Full spec in [specs/003-durable-consumer-state/](specs/003-durable-consumer-state/requirements.md).

```bash
cp .env.example .env                      # POSTGRES_USER / PASSWORD / DB — no defaults given
docker compose up -d --build              # postgres included; schema applied on first boot
./scripts/create_topics.sh                # required after `down -v`
docker compose --profile scale-out up -d  # three notification members

# the memory itself
docker compose exec postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -c "SELECT group_id, order_id, last_sequence, state, handled_count FROM order_fold LIMIT 10;"
```

The same rebalance that 002 recorded, now run twice one variable apart:

```bash
docker compose --profile scale-out up -d && ./scripts/place_orders.sh 9
docker stop notification-consumer-2        # 0 of 3 orders report a gap

STATE_BACKEND=memory docker compose --profile scale-out up -d
docker stop notification-consumer-2        # 4 of 4 do — this is 002's result
```

`STATE_BACKEND` defaults to **`memory`**, so a consumer started with none of 003's settings
still reproduces 001's and 002's recorded experiments. Compose turns it on. The startup
banner names which backend is in force, so no run is ambiguous.

## Levers

| Variable | Default | What it is for |
|---|---|---|
| `STATE_BACKEND` | `memory` | `memory` \| `postgres` |
| `STATE_DB_DSN` | unset | required when the backend is `postgres` |
| `STATE_WRITE_ORDER` | `state_first` | `offset_first` loses data permanently, on purpose |
| `STATE_CRASH_AFTER` | `none` | `state_write` \| `offset_commit` — opens the dual-write window |

The offset commits to Kafka and the fold writes to Postgres, and **no operation covers
both**. `STATE_CRASH_AFTER` makes that gap reachable: crash after the state write and the
event is redelivered, absorbed by the sequence guard, and the handler runs twice —
`handled_count` ends up above `last_sequence`, which is the residue 008 removes.

```bash
# rows that were handled more often than they have events
docker compose exec postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -c "SELECT * FROM order_fold WHERE handled_count > last_sequence;"
```

## Schema

One file, `scripts/state_schema.sql`, applied two ways: mounted into the container's
`/docker-entrypoint-initdb.d/`, and by `./scripts/apply_state_schema.sh`. Both exist
because **the mount runs only when the data volume is empty** — after the first `up`,
editing the schema and running `up` again does nothing at all, silently.

## Known gaps, all deliberate

| Gap | Closed by |
|---|---|
| Duplicate side effects after a crash between the two writes | 008 |
| Offset and state cannot be written atomically | 008 |
| Shared database, not state co-partitioned with the input | 007 |
| Rebuild cost grows with history, not with key count | 007 |
| The producer's own `OrderStore` is still in memory | transactional outbox; no spec claims it |


---

# Spec 004 — Replication, `acks`, and Failover

001, 002 and 003 were all consumer-side lessons running on **one copy of every message**.
Stopping the broker was never an experiment because everything stopped at once. 004 makes the
cluster three nodes, gives every partition three replicas, and turns the producer's `acks` —
hardcoded to `all` since 001 — into one environment variable.

Read [docs/replication.md](docs/replication.md). Full spec in
[specs/004-replication-acks-failover/](specs/004-replication-acks-failover/requirements.md).

```bash
docker compose down -v                    # required once, coming from a single-broker volume
docker compose up -d --build
./scripts/create_topics.sh                # RF 3 by default now

# leader, replicas, and the in-sync set — three different facts
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic order-lifecycle

./scripts/place_orders.sh 5 --advance
docker stop kafka-2                       # a leader dies
./scripts/place_orders.sh 5 --advance     # the producer never noticed
docker start kafka-2                      # Isr returns to three
```

Four things it demonstrates:

- **The ISR is the number that moves.** `Replicas` is fixed at topic creation and does not
  change when a broker dies. `Isr` shrinks within seconds and grows back on restart.
- **Failover needs no operator.** The controller elects a new leader from the ISR and
  librdkafka refreshes its metadata onto it. Nothing restarts, nothing is reconfigured.
- **Replication belongs to the topic, not the cluster.** An RF 1 topic on a healthy
  three-broker cluster still loses a partition when its one node stops, while `order-lifecycle`
  at RF 3 beside it carries on.
- **`acks=all` does not mean all replicas.** It means all replicas *currently in sync* — and an
  ISR that has shrunk to one member satisfies it completely.

| Lever | What it causes |
|---|---|
| `PRODUCER_ACKS=0` | the producer returns before the broker confirms anything, and never reports a loss |
| `PRODUCER_ACKS=1` | the leader's log only — a leader crash before replication still loses the write |
| `REPLICATION_FACTOR=1` | an under-replicated topic, to lose a partition on purpose |

## Known gaps, all deliberate

| Gap | Closed by |
|---|---|
| `acks=all` satisfied by an ISR of one — `min.insync.replicas` not set | **closed by 005** — set to 2, with the producer retry path a refusal needs |
| Unclean leader election and deliberate committed-data loss | not scheduled; the row above is now closed, so this is reachable |
| What `acks` costs in latency, measured | needs a load generator, excluded from this ladder |
| Replica placement, rack awareness, partition reassignment | never claimed |

---

# Spec 005 — Retries, the Dead-Letter Topic, and Poison Messages

Until now a handler could not fail. `runtime.py` said so in the type it declared, and the one
failure it did handle — a message that would not decode — was **logged and committed anyway**,
because the alternative was stalling the partition forever. That is silent data loss, and it
was the right call only because there was nowhere else to put the message.

005 builds the somewhere else, and separates three things that used to look identical.

Read [docs/retries-and-dlq.md](docs/retries-and-dlq.md). Full spec in
[specs/005-retries-dlq-poison-messages/](specs/005-retries-dlq-poison-messages/requirements.md).

```bash
docker compose up -d --build              # no `down -v` this time
./scripts/create_topics.sh                # adds the retry and dead-letter topics

# a message that fails twice and then works
ORDER=$(./scripts/place_orders.sh 1 | grep -oE 'ord-[a-z0-9-]+' | head -1)
HANDLER_FAILURE_MODE=transient HANDLER_FAILURE_ORDERS=$ORDER \
  docker compose up -d --force-recreate inventory-consumer
docker compose logs -f inventory-consumer retry-worker \
  | grep -E 'RETRY_SCHEDULED|RETRY_WAITING|RETRY_SUCCEEDED'

# a message that can never work
./scripts/produce_poison.sh               # not JSON at all
./scripts/produce_poison.sh schema        # valid JSON, wrong shape

# what gave up, and putting it back
docker compose run --rm retry-worker python -m order_service.tools.dlq_replay
docker compose run --rm retry-worker python -m order_service.tools.dlq_replay --publish
```

Four things it demonstrates:

- **A transient failure and a poison message are opposites.** Retrying the first works;
  retrying the second produces the identical exception and spends the budget proving it. So
  classification comes first, and a poison message reaches the dead-letter topic having made
  exactly **one** attempt, never touching the retry topic.
- **In Kafka, giving up in place is not an option.** A partition is read in order, so a
  consumer that keeps retrying offset 847 never commits past it and everything behind it waits.
  The message has to *move*, not wait — which is why the source offset commits immediately.
- **Non-blocking retry buys throughput with ordering.** While a message waits in the retry lane
  the next event for the same order is folded ahead of it, and `SEQUENCE_GAP` fires. That
  warning is correct — the service really has not processed the earlier event yet.
- **Replay reaches every consumer group.** Republishing to `order-lifecycle` delivers to all
  three groups, not only the one that failed. The two that already succeeded absorb it through
  003's sequence guard and log `DUPLICATE_ABSORBED`.

| Lever | What it causes |
|---|---|
| `HANDLER_FAILURE_MODE=transient` | fails `HANDLER_FAILURE_ATTEMPTS` attempts, then succeeds |
| `HANDLER_FAILURE_MODE=poison` | fails every attempt, so the message is dead on arrival |
| `scripts/produce_poison.sh` | genuinely malformed bytes, so the *decoder* fails rather than a handler |
| `RETRY_BACKOFF_SECONDS=120,5` | a long-delayed message ahead of a short one, to watch the retry lane stall |
| `MIN_INSYNC_REPLICAS=2` + `docker compose stop kafka-2 kafka-3` | a write the cluster refuses |

## Known gaps, all deliberate

| Gap | Closed by |
|---|---|
| Head-of-line blocking in the retry lane — one topic, per-message delays | open by design; tiered delay topics are the fix, left unbuilt so the stall is watchable |
| A committed offset no longer means "processed" | 008, where one transaction covers the publication and the commit |
| `SEQUENCE_GAP` warnings while a retry is in flight | inherent to non-blocking retry; the honest signal, not noise |
| Dead letters expire with the topic's default retention | never claimed; no criterion sets retention |
| Nothing alerts on dead-letter depth | out of scope — a dead-letter topic nobody watches is a silent loss bucket |
| Producer retries can reorder without `enable.idempotence` | 008 |
| One worker means one service's backoff holds up the others' | accepted; the fix is a worker per service |
