# Consumer groups, rebalancing, and partition assignment

Spec 001 put **three group ids on one topic** and every service saw every message.
Spec 002 puts **three members in one group** and the messages divide. Same broker, same
topic, same events — the only difference is whether the consumers share a `group.id`.

Both shapes run at once here, which is the point: you can watch them side by side.

```
                          order-lifecycle (3 partitions)
                          ├── partition 0
                          ├── partition 1
                          └── partition 2
                                 │
        ┌────────────────────────┼────────────────────────┐
        │                        │                        │
  inventory-service      analytics-service        notification-service
  1 member → 0,1,2       1 member → 0,1,2         ┌──────────────────┐
                                                  │ notification-1 →0│
      ─── FAN-OUT (001) ───                       │ notification-2 →1│
      every group sees every message              │ notification-3 →2│
                                                  └──────────────────┘
                                                  ─── SCALE-OUT (002) ───
                                                  one message → one member
```

Every number in this document was observed against the broker; the runs are recorded in
[the spec's results table](../specs/002-consumer-groups-rebalancing/tasks.md).

---

## Running it

```bash
docker compose up -d --build     # starts the group with ONE notification member
./scripts/create_topics.sh       # required after `down -v` — auto-create is off
```

Grow the group while watching:

```bash
docker compose --profile scale-out up -d
docker compose logs -f notification-consumer-1 notification-consumer-2 notification-consumer-3
```

Place orders without typing `curl` twenty times:

```bash
./scripts/place_orders.sh 12 --advance    # 12 orders, each walked to DELIVERED
```

See who owns what, from the broker rather than the logs:

```bash
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group notification-service --members --verbose
```

> **Tearing down:** use `docker compose --profile scale-out down`. A plain `down` leaves
> `notification-consumer-2` and `-3` behind as orphans, and if the network was recreated
> in between they fail to start with `network ... not found`. Fix with
> `docker compose --profile scale-out up -d --force-recreate`.

---

## The three things scale-out actually gives you

### 1. Messages divide instead of duplicating

12 orders placed across three members: **12 of 12 handled by exactly one member.** The
same 12 reached inventory and analytics **complete, all 4 events each** — fan-out and
scale-out on one topic, neither disturbing the other.

### 2. Ordering survives it

An order's events are keyed by `order_id`, so `hash(order_id) % 3` always picks the same
partition, and a partition belongs to one member at a time. Therefore:

```
order_id ──hash──> partition ──assignment──> exactly one consumer
```

Of those 12 orders, **12 of 12 had all four events handled by one member in sequence
`[1,2,3,4]`**. Nothing coordinates this — there is no shared lock, no routing table, no
"who owns this order" lookup. **You get ordering and parallelism at the same time**, and
the message key is what buys both.

### 3. The partition count is the ceiling

Start a fourth member on a three-partition topic and it joins successfully holding
nothing:

```
[notification/notification-4] REBALANCE ASSIGNED partitions=[] held=[]
```

`--members --verbose` shows it with `#PARTITIONS 0`. It consumed zero of the next nine
orders. **Scaling consumers past the partition count buys nothing.**

Those nine orders split **1 / 1 / 7** across the three working members. Partitions were
divided perfectly evenly; the *keys* were not. Even assignment is not even load — that is
the hot-key problem, and no assignor fixes it.

---

## What a rebalance costs

A rebalance is Kafka recomputing who reads what. It fires when a member joins, leaves,
crashes, or is evicted. Its *cost* depends entirely on how much gets revoked.

### The consumer's memory is not the consumer's position

This is the distinction the whole feature turns on.

| | what it is | who restores it |
|---|---|---|
| **offset** | where to resume reading | Kafka, perfectly |
| **fold** | what you had accumulated about an order | **nobody** |

Each service folds `(last_sequence, state)` per order in memory. When a partition is
revoked, that fold is discarded ([D6](../specs/002-consumer-groups-rebalancing/design.md)).
Whoever gets the partition next has never seen those orders.

### Watching it happen

Advance six orders to `PACKED`, kill the member owning partition 1, then ship them all:

```
VIOLATION type=SEQUENCE_GAP       order_id=… seq=3 expected=1 observed=3 partition=1 offset=39
VIOLATION type=ILLEGAL_TRANSITION order_id=… seq=3 expected=ORDER_CREATED observed=SHIPPED after None
```

`expected=1` because the fold was empty — the order looked brand new. `after None` because
the state was gone too. Meanwhile the offsets were flawless: p0@29/30, p1@39/40/41, p2@37.
Nothing re-read, nothing skipped.

**Under the default `range` assignor, 6 of 6 orders were affected — including those on
partitions 0 and 2, which never changed owner.** Only 3 needed to fail.

That is not "a rebalance is expensive". That is **eager** rebalancing being expensive.

---

## Three ways to rebalance, measured

Same scenario, three configurations.

### Eager — `range` / `roundrobin` (the client default)

Everyone drops everything, then the assignment is recomputed from scratch.

```
[notification-1] REBALANCE REVOKED  partitions=[0, 1, 2] held=[0, 1, 2]
[notification-1] REBALANCE ASSIGNED partitions=[0]       held=[]          ← lost all folds
```

**Result: 6 of 6 orders gapped**, when only 3 had any reason to be.

### Cooperative — `cooperative-sticky`

Only partitions that genuinely change owner are revoked.

```bash
CONSUMER_ASSIGNMENT_STRATEGY=cooperative-sticky docker compose --profile scale-out up -d --force-recreate
```

```
[notification-3] REBALANCE ASSIGNED partitions=[]  held=[2]    ← kept its fold
[notification-1] REBALANCE ASSIGNED partitions=[0] held=[1]    ← kept its fold
```

**Zero `REVOKED` lines. Result: 3 of 9 orders gapped** — exactly the three on the partition
that actually moved. Partitions 1 and 2 were untouched at 0/3.

### KIP-848 — `group.protocol=consumer`

The broker computes the assignment and pushes each member its share.

```bash
CONSUMER_GROUP_PROTOCOL=consumer CONSUMER_REMOTE_ASSIGNOR=uniform \
  docker compose --profile scale-out up -d --force-recreate
```

```
[notification-1] REBALANCE REVOKED  partitions=[1, 2] held=[0, 1, 2]
[notification-1] REBALANCE ASSIGNED partitions=[]     held=[0]    ← kept partition 0's fold
```

Incremental, like cooperative — it gave up only the two partitions that had to move.

> ⚠️ **You cannot switch an existing group between protocols in place.** Doing so gives
> every member `FATAL: ConsumerGroupHeartbeat fatal error: Broker: The group id does not
> exist` and they exit. Stop the consumers, `kafka-consumer-groups.sh --delete --group
> notification-service`, then start under the new protocol.
>
> Likewise a member joining with a *different assignor* than the group is rejected with
> `INCONSISTENT_GROUP_PROTOCOL` and retries until the group converges.

### The summary

| | revokes | folds lost | orders gapped |
|---|---|---|---|
| `range` (eager, default) | everything from everyone | all | **6 / 6** |
| `cooperative-sticky` | only what moves | only the moved partition's | **3 / 9** |
| KIP-848 `consumer` | only what moves | only the moved partition's | incremental |

---

## Two levers for causing trouble on purpose

001 had `force: true`. 002 has two, for the same reason: without them the behaviour is
unreachable.

### Eviction — a healthy consumer thrown out of its group

```bash
docker compose run --rm --no-deps \
  -e CONSUMER_GROUP_ID=evict-probe -e CONSUMER_INSTANCE_ID=slowpoke \
  -e CONSUMER_SESSION_TIMEOUT_MS=6000 -e CONSUMER_MAX_POLL_INTERVAL_MS=7000 \
  -e HANDLER_DELAY_SECONDS=12 \
  notification-consumer-1
```

```
COMMIT_REJECTED partition=2 offset=0 reason=UNKNOWN_MEMBER_ID "Commit failed: Broker: Unknown member"
consume error: _MAX_POLL_EXCEEDED "Application maximum poll interval (7000ms) exceeded by 375ms"
REBALANCE LOST partitions=[0, 1, 2] held=[2]
REBALANCE ASSIGNED partitions=[0, 1, 2] held=[]     ← rejoins, re-reads offset 0, sleeps, evicted again…
```

**It livelocks forever.** Never committing, it is redelivered the same offset every cycle.
Zero progress, infinite reprocessing. That is at-least-once meeting a slow handler, and it
is the most common real Kafka consumer incident there is.

Nothing was wrong with this consumer. It was working, just slowly — and Kafka cannot tell
"slow" from "gone".

Two timeouts are involved and are constantly confused:

| | measures | enforced by |
|---|---|---|
| `session.timeout.ms` | are heartbeats arriving? | the broker |
| `max.poll.interval.ms` | is the *application* still polling? | **librdkafka itself** |

Heartbeats come from a background C thread, so a slow handler never misses one. The
eviction came entirely from `max.poll.interval.ms`. Under `classic` the client enforces
`max.poll.interval.ms >= session.timeout.ms`, which is why both had to be lowered; under
KIP-848 the session timeout is broker-side and sending it raises.

### Static membership — a restart that costs nothing

```bash
STATIC_MEMBERSHIP=1 docker compose --profile scale-out up -d --force-recreate
docker restart notification-consumer-2
```

Counting `REBALANCE` lines logged by the two members that were *not* restarted:

| | lines | what happened |
|---|---|---|
| dynamic + `docker restart` | **8** | `REVOKED`+`ASSIGNED` twice each — leave, then rejoin |
| static + `docker restart` | **2** | one `ASSIGNED` each, **no `REVOKED`** — folds survived |
| static + `SIGKILL` then `start` | **0** | silence; the coordinator held the partitions |

A rolling deploy of three dynamic instances costs six rebalances. With `group.instance.id`
it costs none. The middle row matters too: even where static membership does not fully
avoid a rebalance, it avoids the **revocation**, so the other members keep their folds.

---

## Environment surface

Every setting defaults to 001's behaviour.

| Variable | Default | Effect |
|---|---|---|
| `CONSUMER_INSTANCE_ID` | hostname | log prefix, so three members are distinguishable |
| `CONSUMER_GROUP_PROTOCOL` | `classic` | `classic` \| `consumer` (KIP-848) |
| `CONSUMER_ASSIGNMENT_STRATEGY` | client default (`range`) | classic only: `range` \| `roundrobin` \| `cooperative-sticky` |
| `CONSUMER_REMOTE_ASSIGNOR` | broker default (`uniform`) | KIP-848 only: `uniform` \| `range` |
| `CONSUMER_SESSION_TIMEOUT_MS` | client default | classic only — raises under KIP-848 |
| `CONSUMER_MAX_POLL_INTERVAL_MS` | client default | both protocols |
| `HANDLER_DELAY_SECONDS` | `0.0` | the eviction lever |
| `STATIC_MEMBERSHIP` | unset | compose-level toggle for `group.instance.id` |

Setting a variable the selected protocol does not accept fails at startup by name, before
the group is joined:

```
CONSUMER_REMOTE_ASSIGNOR cannot be used with CONSUMER_GROUP_PROTOCOL=classic
  — use CONSUMER_ASSIGNMENT_STRATEGY
```

librdkafka would have accepted that one silently and used `range` anyway, so the check is
ours ([D4](../specs/002-consumer-groups-rebalancing/design.md)).

---

## What is deliberately still broken

| Limitation | Why it is left | Closed by |
|---|---|---|
| A moved partition loses its fold, producing false `SEQUENCE_GAP` | It is the evidence that motivates durable state. A local fix deletes the argument | **003**, fully by **007** |
| Duplicate processing after eviction and redelivery | At-least-once behaving as designed | 003, 008 |
| No dedup on `event_id`, though every event carries one | The dedup key exists precisely for this | 003, 008 |
| Rebalance *duration* is never measured, only its shape | Needs a load generator this spec excludes | — |
| Consumer lag under load is not measured | Same | — |
| Partition growth and key rehashing | A topic-level lesson, not a consumer-group one | — |
| Single broker, RF 1 | Partitions, not brokers, are the unit of consumer parallelism | 004 |

**The fold amnesia is a feature of this spec, not a defect.** The honest fixes are durable
state (003) or state co-partitioned with its input (007). Patching it here would remove the
failure that makes both worth building.

---

Related: [specs/002-consumer-groups-rebalancing/](../specs/002-consumer-groups-rebalancing/requirements.md) ·
[DECISIONS.md](../DECISIONS.md) (X9) ·
[concurrency-and-confluent-kafka.md](concurrency-and-confluent-kafka.md) §10 ·
[order-flow.md](order-flow.md)
