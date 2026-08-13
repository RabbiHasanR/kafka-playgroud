# 002 — Prepaid Order Service with Consumer Fan-Out: Design

Implements [requirements.md](requirements.md).
Cross-cutting choices that outlive this feature are recorded in
[../../DECISIONS.md](../../DECISIONS.md) as `X<n>` and referenced from here.
This feature exists because of [X7](../../DECISIONS.md).

## Architecture

```
                     compose network "kafka-playground_default"
  ┌───────────────────────────────────────────────────────────────────────────┐
  │                                                                           │
  │  order-service (FastAPI)                    kafka                         │
  │  :8010                                      :19092                        │
  │   POST /orders ───────────────┐                                           │
  │   POST /orders/{id}/events ───┼── produce ──► order-lifecycle             │
  │   GET  /orders/{id}           │   key=order_id  ├── partition 0           │
  │   GET  /health                │                 ├── partition 1           │
  │   [poll thread] ◄── delivery reports            └── partition 2           │
  │   in-memory: {order_id: Order}                         │                  │
  │                                                        │                  │
  │            ┌───────────────────────────────────────────┤                  │
  │            │                    │                      │                  │
  │  inventory-consumer   notification-consumer   analytics-consumer          │
  │  group=inventory-     group=notification-     group=analytics-            │
  │        service              service                 service               │
  │  ORDER_CREATED,       all four events         all four events             │
  │  SHIPPED                                                                  │
  │                                                                           │
  └───────────────────────────────────────────────────────────────────────────┘
       three groups → three independent offset positions in __consumer_offsets
```

Four new services join the compose stack. The broker is untouched — 000 owns it —
and 001's `producer`, `consumer`, and `order-events` topic are untouched (R2.45).

**The picture to hold onto:** one topic, three arrows out of it, and each arrow has its
own offset. That is fan-out. 003 will add a second arrow into *one* group, and the
messages will divide instead of duplicating.

## Decisions

### D1 — A separate package and a separate topic — *R2.9, R2.45*

`src/order_service/` and the topic `order-lifecycle`. 001 keeps `order_pipeline` and
`order-events`.

The tempting alternative is to extend `order_pipeline/events.py` — one contract, no
duplication. It is rejected because R1.3 fixes 001's event types at six and
`requirements.md` is an approved contract: adding or removing a type there would
contradict it, and `CLAUDE.md` requires stopping rather than auto-amending. A separate
topic also keeps the two features independently runnable, so 001's experiments still
reproduce after 002 exists.

The cost is real duplication — the poll thread, the delivery-report wait, and the
consume loop appear twice in the repository. That is accepted deliberately: the two
copies are owned by different contracts and will diverge (004 rewrites one of them and
not the other). Sharing them now would couple two features that are meant to move
independently.

### D2 — The module layout mirrors 001 — *R2.45*

`producer/` and `consumer/` subpackages, with the same file names where the role is the
same: `app.py`, `kafka_producer.py`, `routes.py`, `main.py`. Reading 002 after 001
should feel like reading the same codebase, so that the differences stand out instead
of the layout.

### D3 — Prepaid: no `PAID` event, and the total is checked at the boundary — *R2.6, R2.14*

Payment settles before the order exists, so it is a field on `ORDER_CREATED`, not a
step in the chain. The lifecycle is `CREATED → PACKED → SHIPPED → DELIVERED`.

The consequence is the interesting part. 001 detects a wrong total *on the consumer*,
by folding `ITEM_ADDED` events and comparing against `PAID`. Here the same error is
impossible to publish: `POST /orders` rejects a payment amount that disagrees with
`Σ(qty × unit_price)` with `422`, before an event exists.

**That relocation is the lesson.** Anything a consumer must detect is something every
consumer must detect — three services here, more later. Validating at the boundary
fixes it once for all of them. A consumer-side check is what you are left with when the
event can already be wrong by the time it reaches you.

Money stays in integer minor units for the same reason as 001's D4: exact arithmetic,
so an equality check has exactly one possible cause of failure.

### D4 — The transition guard lives in the service, with a `force` escape hatch — *R2.21, R2.24, R2.25*

`POST /orders/{order_id}/events` checks the requested `event_type` against the order's
recorded state and returns `409` if it is not the legal successor. This is the sharpest
difference from 001, whose equivalent endpoint publishes whatever it is handed: a real
service owns its aggregate and refuses to emit a transition that cannot have happened.

That guard would make R2.38 and R2.39 unreachable — a producer that never emits an
illegal transition gives the consumers nothing to detect. So `force: true` bypasses it,
defaulted off, exactly the arrangement 001 uses for `unkeyed` and `shuffle` (001 D9).
Default behaviour is what production does; the flag is the lab lever.

**A forced event does not advance the recorded state.** The guard rejected the
transition precisely because it is not one the aggregate can make; pretending otherwise
would put a lie in the order store. The event goes on the topic, the sequence advances,
the order's state does not.

*Rejected:* an environment flag instead of a request flag — it would need a restart
between a clean run and a broken one.

### D5 — Sequence lives on the order, not on the producer — *R2.3, R2.26*

001 holds `{order_id: last_sequence}` inside the producer (D8) because it has no notion
of an order. 002 does: the `Order` record owns its `last_sequence`, and the store hands
out the next one under the same lock that guards the transition check. This is the
aggregate-version pattern, and it is where a real service would keep it — in the same
row it later commits.

A forced publish still takes the next contiguous sequence (R2.26). This keeps the two
consumer-side signals independent: a forced event produces an illegal-transition
violation with **no** sequence gap, so each detector can be observed in isolation. A
gap comes from somewhere else — a consumer restart, which is 001's R1.27 lesson
arriving from the other direction.

**Accepted limitation:** the sequence is assigned before the publish, so a delivery
failure burns a sequence number and the consumers will later see a genuine gap. Same
behaviour as 001, and it is honest — the number was spent whether or not the broker
took the message.

### D6 — Two endpoints, no simulator — *R2.12, R2.19*

`POST /orders` creates; `POST /orders/{order_id}/events` advances. Bulk generation is
deliberately absent: the lag and throughput experiments live in 001, and this feature
is about shape, not volume. `GET /orders/{order_id}` exists so the service's own view
can be compared against what the consumers derived.

Both publishing paths **block on the delivery report** and return the real partition
and offset, following 001's D5 — the route handlers are synchronous `def`, so FastAPI
runs them in a worker thread and the wait cannot stall the event loop. A broker failure
becomes `502`/`504` rather than a silent drop, which matters more here than in 001: a
lost `ORDER_CREATED` is an order that exists for the customer and for nobody else.

### D7 — Fan-out by consumer group, not by topic — *R2.28, R2.29, R2.30*

Three services, three `group.id`s, one topic. Kafka tracks an offset per group, so each
service reads every message and none of them consumes it away from the others.

*Rejected:* a topic per consumer (`order-events-inventory`, …), which is the first
thing most people reach for. It works, and it is wrong here: every new consumer becomes
a change to the producer, which is precisely the coupling Kafka exists to remove. With
one topic, adding a fourth service is a new container and nothing else.

This also makes R2.30 free rather than engineered: stopping one group cannot affect the
others, because their offsets were never shared.

### D8 — One consume runtime, three handler maps — *R2.33, R2.34, R2.35, R2.37*

`consumer/runtime.py` owns the only poll/decode/detect/dispatch/commit loop. A service
is data, not code: a `ServiceSpec` carrying a default group id and a
`dict[EventType, Handler]`. `SERVICE_NAME` selects one from a registry at startup, so
all three run from one image and one entry point.

Three near-identical consume loops is the obvious wrong turn — the third copy is where
the offset-commit bug always gets fixed in two places and not the third.

A service simply omits the event types it does not care about; the runtime skips them
without error, which is what R2.33's "ignore other types" means mechanically.

### D9 — Consumer detection keeps two signals, not three — *R2.38, R2.39*

Per-order `(last_sequence, state)` folded in memory, producing sequence-gap and
illegal-transition violations. 001's third signal, total mismatch, has no counterpart —
D3 moved that check to the API boundary.

Each service folds its own copy. That is not waste: it is what independent services
actually do, and it means one service's amnesia after a restart does not affect the
others' detection.

State is in memory, unpersisted, for the same reason as 001 (X3): 004 is where durable
state arrives, and it needs the failure to motivate it.

### D10 — Manual commit after handling — *R2.32*

`enable.auto.commit=False`, commit after the handler returns. At-least-once, with the
same duplicate-on-crash consequence 001 documents in D12 — not solved here.
Idempotency and deduplication are 004 and 009; a real service would dedupe on
`event_id`, which is why R2.4 puts one on every event even though nothing reads it yet.

### D11 — `event_id` is a UUID4 assigned at construction — *R2.4*

Unique per event, not per order. It exists as the natural dedup key for later specs and
as a stable handle in logs when two events for one order look otherwise identical.

### D12 — Service selection and group ids from the environment — *R2.37, R2.43*

`SERVICE_NAME` picks the registry entry; `CONSUMER_GROUP_ID` defaults to
`<service>-service` and can be overridden, which is what makes a replay-from-earliest
experiment a one-variable change. Broker address and topic follow 001's D15 exactly.

### D13 — The topic script grows a topic list — *R2.9, R2.11*

`scripts/create_topics.sh` iterates over a list rather than creating one hardcoded
topic, so `order-events` and `order-lifecycle` are both created by the same required
setup step. Additive: the script's existing invocation and behaviour for 001 are
unchanged. Auto-creation stays off (R0.14), so a missing topic still fails loudly.

### D14 — Logging: service name first, violations at `WARNING` — *R2.36, R2.41, R2.42*

Every consumed record logs at `INFO` prefixed with the service name, then partition,
offset, key, `order_id`, `sequence`, and `event_type`. Handler output carries a
`[service]` prefix. Violations log at `WARNING` with a stable `VIOLATION` marker, so
`docker compose logs | grep VIOLATION` works across all three services at once, and
`grep '\[inventory\]'` isolates one.

## Module layout

```
src/order_service/
├── config.py                # env-driven settings                    D12
├── events.py                # contract, payloads, transition table   D3, D5, D11
├── producer/
│   ├── app.py               # FastAPI app, lifespan, poll thread     D6
│   ├── kafka_producer.py    # keyed produce + delivery-report wait   D6
│   ├── orders.py            # order store, sequences, guard          D4, D5
│   └── routes.py            # HTTP surface                           D4, D6
└── consumer/
    ├── runtime.py           # the one consume loop + detection       D8, D9, D10
    ├── main.py              # SERVICE_NAME registry, entry point     D8, D12
    ├── inventory.py         # handlers                               D8
    ├── notification.py      # handlers                               D8
    └── analytics.py         # handlers                               D8
scripts/create_topics.sh                                            # D13
docs/order-flow.md                                                  # R2.46
```

`events.py` is imported by both sides of 002 and by neither side of 001.

## HTTP surface

| Endpoint | Success | Failure |
|---|---|---|
| `POST /orders` | `201` + `order_id`, `partition`, `offset` | `422` total mismatch or no items, `502`/`504` broker |
| `POST /orders/{order_id}/events` | `200` + `sequence`, `partition`, `offset` | `404` unknown order, `409` illegal transition, `422` bad payload, `502`/`504` broker |
| `GET /orders/{order_id}` | `200` + state, last sequence, items, total | `404` unknown order |
| `GET /health` | `200` | — |

## Known gaps, by intent

| Gap | Requirement | Closed by |
|---|---|---|
| Order state lost when the service restarts | D5 note | — (accepted; a real service uses a database) |
| No transactional outbox — the DB write and the publish cannot be atomic | D6 | — (out of scope; named in the docs) |
| Consumer fold state lost on restart | D9 | 004 |
| Duplicate processing after a crash | D10 | 004, 009 |
| No consumer-side dedup on `event_id` | D11 | 004, 009 |
| Single broker, RF 1 | D13 | 005 |
| No schema enforcement on the JSON | X2 | later |

## Deferred to later specs

Nothing here may anticipate them: consumer groups and rebalancing (003), durable state
(004), replication (005), dead-letter handling (006), compaction (007), changelog state
stores (008), transactions (009), stream SQL (010).
