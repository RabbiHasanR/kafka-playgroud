# 001 — Prepaid Order Service with Consumer Fan-Out: Design

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

Four services join the compose stack. The broker is untouched — 000 owns it.

**The picture to hold onto:** one topic, three arrows out of it, and each arrow has its
own offset. That is fan-out. 002 will add a second arrow into *one* group, and the
messages will divide instead of duplicating.

## Decisions

### D1 — One package, one topic — *R1.9*

`src/order_service/` publishes to `order-lifecycle`. The package holds both sides of
the feature, and `events.py` is the single contract they share.

*Rejected:* a topic per consumer (see D7), and splitting the producer and the consumers
into separate distributions — they move together, share one event contract, and a split
would buy nothing but two `pip install`s.

### D2 — `producer/` and `consumer/` subpackages — *R1.43*

The command side and the reacting side are different programs with different lifetimes,
different failure modes, and different Kafka clients. Keeping them in sibling packages
under one distribution means one image and one dependency set, while the import path
still says which side you are reading.

### D3 — Prepaid: no `PAID` event, and the total is checked at the boundary — *R1.6, R1.14*

Payment settles before the order exists, so it is a field on `ORDER_CREATED`, not a
step in the chain. The lifecycle is `CREATED → PACKED → SHIPPED → DELIVERED`.

The consequence is the interesting part. A wrong total is impossible to publish:
`POST /orders` rejects a payment amount that disagrees with `Σ(qty × unit_price)` with
`422`, before an event exists. The obvious alternative is to publish it and let each
consumer fold the items and compare — which is a real pattern, and the wrong one here.

**Where the check lives is the lesson.** Anything a consumer must detect is something
*every* consumer must detect — three services here, more later. Validating at the
boundary fixes it once for all of them. Consumer-side detection is what you are left
with when the event can already be wrong by the time it reaches you.

Money is carried in integer minor units so that this check is exact: a float total
would give a mismatch two possible causes — a genuinely wrong amount, or floating-point
drift — and a signal with two causes proves nothing.

*Rejected:* `Decimal` — correct, but it serialises to a string in JSON and adds a
conversion at every boundary for no gain at integer-only arithmetic.

### D4 — The transition guard lives in the service, with a `force` escape hatch — *R1.21, R1.24, R1.25*

`POST /orders/{order_id}/events` checks the requested `event_type` against the order's
recorded state and returns `409` if it is not the legal successor. A real service owns
its aggregate and refuses to emit a transition that cannot have happened; an endpoint
that publishes whatever event type it is handed is a test fixture, not a service.

That guard would make R1.38 and R1.39 unreachable — a service that never emits an
illegal transition gives the consumers nothing to detect. So `force: true` bypasses it,
defaulted off. Default behaviour is what production does; the flag is the lab lever,
and keeping it a *request* flag means a clean call and a broken one are one `curl`
apart with no restart in between.

**A forced event does not advance the recorded state.** The guard rejected the
transition precisely because it is not one the aggregate can make; pretending otherwise
would put a lie in the order store. The event goes on the topic, the sequence advances,
the order's state does not.

### D5 — Sequence lives on the order, not on the producer — *R1.3, R1.26*

The `Order` record owns its `last_sequence`, and the store hands out the next one under
the same lock that guards the transition check. This is the aggregate-version pattern,
and it is where a real service would keep it — in the same row it later commits.

*Rejected:* a counter inside the producer wrapper, keyed by `order_id`. It works, but
it puts the order's version somewhere the order is not, and it would have to be
reconciled the moment orders became durable.

A forced publish still takes the next contiguous sequence (R1.26). This keeps the two
consumer-side signals independent: a forced event produces an illegal-transition
violation with **no** sequence gap, so each detector can be observed in isolation. A
gap comes from somewhere else — a consumer restart, which loses the fold while Kafka
faithfully restores the offset (D9).

**Accepted limitation:** the sequence is assigned before the publish, so a delivery
failure burns a sequence number and the consumers will later see a genuine gap. That is
honest — the number was spent whether or not the broker took the message.

### D6 — Two endpoints, no simulator — *R1.12, R1.19*

`POST /orders` creates; `POST /orders/{order_id}/events` advances. Bulk generation is
deliberately absent by request — this feature is about shape, not volume, and lag and
throughput experiments need a load generator that would obscure it.
`GET /orders/{order_id}` exists so the service's own view can be compared against what
the consumers derived.

Both publishing paths **block on the delivery report** and return the real partition
and offset. Mechanically: the route handlers are synchronous `def`, so FastAPI runs
them in a worker thread and the wait cannot stall the event loop. A broker failure
becomes `502`/`504` rather than a silent drop, which matters: a lost `ORDER_CREATED` is
an order that exists for the customer and for nobody else.

*Rejected:* returning `202 Accepted` on buffer, which is what a high-throughput service
would really do — but the response would be a claim rather than a fact, and R1.17
requires the real partition and offset.

### D7 — Fan-out by consumer group, not by topic — *R1.28, R1.29, R1.30*

Three services, three `group.id`s, one topic. Kafka tracks an offset per group, so each
service reads every message and none of them consumes it away from the others.

*Rejected:* a topic per consumer (`order-lifecycle-inventory`, …), which is the first
thing most people reach for. It works, and it is wrong here: every new consumer becomes
a change to the producer, which is precisely the coupling Kafka exists to remove. With
one topic, adding a fourth service is a new container and nothing else.

This also makes R1.30 free rather than engineered: stopping one group cannot affect the
others, because their offsets were never shared.

### D8 — One consume runtime, three handler maps — *R1.33, R1.34, R1.35, R1.37*

`consumer/runtime.py` owns the only poll/decode/detect/dispatch/commit loop. A service
is data, not code: a `ServiceSpec` carrying a name and a `dict[EventType, Handler]`.
The group id is not on the spec — it is derived from the name by `Settings.group_id_for`
and overridable from the environment (D12), which keeps the spec free of configuration.
`SERVICE_NAME` selects one from a registry at startup, so all three run from one image
and one entry point.

Three near-identical consume loops is the obvious wrong turn — the third copy is where
the offset-commit bug always gets fixed in two places and not the third.

A service simply omits the event types it does not care about; the runtime skips them
without error, which is what R1.33's "ignore other types" means mechanically.

### D9 — Consumer detection keeps two signals, not three — *R1.38, R1.39*

Per-order `(last_sequence, state)` folded in memory, producing sequence-gap and
illegal-transition violations. A third signal — a folded total disagreeing with the
payment — has no place here, because D3 moved that check to the API boundary.

Each service folds its own copy. That is not waste: it is what independent services
actually do, and it means one service's amnesia after a restart does not affect the
others' detection.

State is in memory and unpersisted (X3): 003 is where durable state arrives, and it
needs this failure to motivate it.

### D10 — Manual commit after handling — *R1.32*

`enable.auto.commit=False`, commit after the handler returns. At-least-once, with the
at-least-once: a crash between handling an event and committing its offset redelivers
it. Not solved here — idempotency and deduplication are 003 and 008; a real service
would dedupe on
`event_id`, which is why R1.4 puts one on every event even though nothing reads it yet.

### D11 — `event_id` is a UUID4 assigned at construction — *R1.4*

Unique per event, not per order. It exists as the natural dedup key for later specs and
as a stable handle in logs when two events for one order look otherwise identical.

### D12 — Service selection and group ids from the environment — *R1.37, R1.43*

`SERVICE_NAME` picks the registry entry; `CONSUMER_GROUP_ID` defaults to
`<service>-service` and can be overridden, which is what makes a replay-from-earliest
experiment a one-variable change. Nothing else changes between a host run and a
compose run, which is what R1.44 asks for.

### D13 — The topic script grows a topic list — *R1.9, R1.11*

`scripts/create_topics.sh` iterates over a topic list rather than creating one
hardcoded topic, so adding a topic in a later spec is a one-line change to the array
rather than a second script. Auto-creation stays off (R0.14), so a missing topic still
fails loudly and topic creation stays a deliberate setup step.

### D14 — Logging: service name first, violations at `WARNING` — *R1.36, R1.41, R1.42*

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
docs/order-flow.md                                                  # R1.45
```

`events.py` is the one module both the producer and all three consumers import — the
shared contract, and the only module that may be shared.

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
| Consumer fold state lost on restart | D9 | 003 |
| Duplicate processing after a crash | D10 | 003, 008 |
| No consumer-side dedup on `event_id` | D11 | 003, 008 |
| Single broker, RF 1 | D13 | 004 |
| No schema enforcement on the JSON | X2 | later |

## Deferred to later specs

Nothing here may anticipate them: consumer groups and rebalancing (002), durable state
(003), replication (004), dead-letter handling (005), compaction (006), changelog state
stores (007), transactions (008), stream SQL (009).
