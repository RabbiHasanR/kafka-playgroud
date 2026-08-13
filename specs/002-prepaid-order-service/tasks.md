# 002 — Prepaid Order Service with Consumer Fan-Out: Tasks

Implements [requirements.md](requirements.md) per [design.md](design.md).

Each task cites the requirement IDs it satisfies and, where relevant, the design
decision it follows.

## Shared contract

- [x] **T1** — Add `config.py` reading `KAFKA_BOOTSTRAP_SERVERS`,
  `ORDER_LIFECYCLE_TOPIC`, `SERVICE_NAME`, and `CONSUMER_GROUP_ID` from the
  environment with host-usable defaults and no connection details in source.
  — *R2.43, R2.44* — D12
- [x] **T2** — Define the event model in `events.py`: `event_id`, `order_id`,
  `sequence`, `event_type`, `occurred_at`, `payload`, with `event_id` defaulting to a
  fresh UUID4. — *R2.1, R2.4* — D11
- [x] **T3** — Define the four event types and the payload models: `ORDER_CREATED`
  (`customer_id`, `items[sku, qty, unit_price]`, `total_amount`, `payment[method,
  reference, amount]`) and `SHIPPED` (`carrier`, `tracking_number`), with all money as
  integer minor units. — *R2.2, R2.6, R2.7, R2.8* — D3
- [x] **T4** — Enforce in the `ORDER_CREATED` payload model that `items` is non-empty
  and `total_amount == Σ(qty × unit_price) == payment.amount`, so a mismatch cannot be
  represented as a valid event. — *R2.14, R2.15* — D3
- [x] **T5** — Define the lifecycle transition table mapping each event type to its
  legal predecessor state, forming `CREATED → PACKED → SHIPPED → DELIVERED`, importable
  by both producer and consumer. — *R2.5* — D3
- [x] **T6** — Generalize `scripts/create_topics.sh` to create a list of topics, adding
  `order-lifecycle` with 3 partitions and replication factor 1 alongside 001's
  `order-events`, changing nothing about 001's behaviour. — *R2.9, R2.11, R2.45* — D13

## Producer — order service

- [x] **T7** — Implement `producer/kafka_producer.py`: construct the `Producer`,
  publish with the UTF-8 `order_id` as key using the default partitioner, wait on the
  delivery report, and return the assigned partition and offset — or a named error when
  the broker does not acknowledge within the timeout, distinguishing a missing topic
  from an unreachable broker. — *R2.10, R2.11, R2.18* — D6
- [x] **T8** — Start a daemon `poll()` thread from the FastAPI lifespan and stop it and
  `flush()` with a timeout on shutdown, so delivery callbacks always fire.
  — *R2.17, R2.18* — D6
- [x] **T9** — Implement `producer/orders.py`: the `Order` record, an in-memory store
  under a lock, unique `order_id` assignment, and per-order sequence allocation
  starting at 1 and increasing by exactly 1. — *R2.3, R2.13* — D5
- [x] **T10** — Implement the transition guard in the store: unknown order and illegal
  successor are distinct, named failures, and a `force` reservation bypasses the
  successor check while still taking the next contiguous sequence and leaving the
  recorded state unchanged. — *R2.21, R2.24, R2.25, R2.26* — D4, D5
- [x] **T11** — Implement `POST /orders` as a synchronous `def` handler: validate the
  payment total, create the order as `CREATED`, publish exactly one `ORDER_CREATED` at
  sequence 1, and return `201` with the `order_id`, partition, and offset.
  — *R2.12, R2.13, R2.14, R2.15, R2.16, R2.17* — D3, D6
- [x] **T12** — Implement `POST /orders/{order_id}/events`: `404` for an unknown order,
  `409` naming the current state and the expected event type for an illegal transition,
  `422` for a payload that does not match the event type, and on success publish,
  advance the recorded state, and return the real partition and offset.
  — *R2.19, R2.20, R2.21, R2.22, R2.23* — D4
- [x] **T13** — Implement `GET /orders/{order_id}` reporting the recorded state, last
  sequence, items, and total, plus `GET /health`. — *R2.27*

## Consumer — the three services

- [x] **T14** — Implement `consumer/runtime.py`: subscribe, poll, decode, detect,
  dispatch, commit, with `enable.auto.commit=False`, `auto.offset.reset=earliest`, and
  the offset committed only after the handler returns. — *R2.31, R2.32* — D8, D10
- [x] **T15** — Fold per-order `(last_sequence, state)` in the runtime and record a
  sequence-gap violation when `sequence != last + 1`, including the unknown-order case
  where any sequence other than 1 is a gap. — *R2.38* — D9
- [x] **T16** — Record an illegal-transition violation using the T5 transition table,
  and continue consuming after any violation rather than halting. — *R2.39, R2.40* — D9
- [x] **T17** — Log every consumed record at `INFO` with the service name, partition,
  offset, key, `order_id`, `sequence`, and `event_type`; log every violation at
  `WARNING` with a stable `VIOLATION` marker. — *R2.41, R2.42* — D14
- [x] **T18** — Implement the inventory service: react to `ORDER_CREATED` and `SHIPPED`
  only, logging the stock reservation and its release, and ignore the other two event
  types without error. — *R2.33, R2.36* — D8
- [x] **T19** — Implement the notification service: react to all four event types with
  a distinct customer-facing message per type. — *R2.34, R2.36* — D8
- [x] **T20** — Implement the analytics service: react to all four event types by
  maintaining and logging a count per event type. — *R2.35, R2.36* — D8
- [x] **T21** — Implement `consumer/main.py`: a registry mapping `SERVICE_NAME` to its
  default group id and handler map, so all three services run from one image and one
  entry point. — *R2.28, R2.37* — D8, D12

## Wiring

- [x] **T22** — Add `order-service` and the three consumer services to
  `docker-compose.yml`, gated on broker health, each consumer with its own
  `SERVICE_NAME` and distinct `CONSUMER_GROUP_ID`, changing no broker configuration and
  leaving 001's services untouched. — *R2.28, R2.44, R2.45* — D7, D12

## Tests

- [x] **T23** — Unit-test the contract without a broker: a payment total that disagrees
  with the item sum is rejected, an empty item list is rejected, and a `SHIPPED` payload
  missing its carrier is rejected. — *R2.6, R2.7, R2.14, R2.15*
- [x] **T24** — Unit-test the order store without a broker: sequences start at 1 and
  increase by exactly 1, an unknown order and an illegal successor raise distinct
  errors, a forced reservation bypasses the guard while still taking the next sequence
  and leaving the state unchanged. — *R2.3, R2.20, R2.21, R2.24, R2.26*
- [x] **T25** — Unit-test the consumer fold without a broker: a clean chain produces no
  violations, an out-of-order event produces an illegal-transition violation, and a
  skipped sequence produces a sequence-gap violation. — *R2.38, R2.39, R2.40*
- [x] **T26** — Unit-test the service registry: each service resolves to a distinct
  group id, and inventory's handler map covers `ORDER_CREATED` and `SHIPPED` only while
  the other two cover all four. — *R2.33, R2.34, R2.35, R2.37*

## Verification experiments

Each is run and observed, not merely coded. Tick only after actually running it.

- [x] **T27** — **Fan-out.** Create one order and confirm the same event appears in all
  three consumer logs, each doing different work. — *R2.28, R2.29, R2.36*
- [x] **T28** — **Three groups, one topic.** `kafka-consumer-groups.sh --list` shows
  three distinct groups, and `--describe` shows each with its own offset on
  `order-lifecycle`. — *R2.28, R2.29*
- [x] **T29** — **Validation before the event exists.** `POST /orders` with a payment
  amount disagreeing with the item sum returns `422`, and the topic's end offsets are
  unchanged — nothing was published. — *R2.14*
- [x] **T30** — **The service guards its own transitions.** `SHIPPED` on a freshly
  created order returns `409` naming `CREATED` and `PACKED`, and nothing is published;
  then `PACKED → SHIPPED → DELIVERED` in order each return `200` and each fan out to
  all three services. — *R2.21, R2.23, R2.29*
- [x] **T31** — **Forced out-of-order publish.** The same rejected call with
  `force: true` publishes, and all three services log an illegal-transition violation
  with **no** sequence gap — the two signals are independent. `GET /orders/{id}` shows
  the recorded state did not advance. — *R2.24, R2.26, R2.39*
- [x] **T32** — **Unknown order.** Publishing an event for an `order_id` the service
  never created returns `404` and publishes nothing. — *R2.20*
- [x] **T33** — **Key → partition.** Create ~10 orders, advance each, and confirm with
  `kafka-console-consumer.sh --partition N --property print.key=true` that every event
  for one `order_id` is in exactly one partition. — *R2.10*
- [x] **T34** — **One group stopped, the others unaffected.** Stop the notification
  consumer, advance an order to `DELIVERED`, and confirm inventory and analytics stayed
  current throughout; restart notification and confirm it catches up from its own
  committed offset. — *R2.30, R2.31*
- [x] **T35** — **Sequence gap from amnesia.** Restart a consumer mid-order and confirm
  the next event raises a sequence-gap violation that is indistinguishable from a real
  producer-side gap — 001's state-loss lesson arriving from the other side. — *R2.38*
- [x] **T36** — **001 still works.** Run 001's happy path (`POST /simulate` with 50
  orders) after 002 is up and confirm unchanged behaviour on `order-events`.
  — *R2.45*

## Documentation

- [x] **T37** — Write `docs/order-flow.md`: what is synchronous and what is
  asynchronous, the fan-out diagram, which service reacts to which event, and a
  copy-paste walkthrough from `POST /orders` through `DELIVERED`. — *R2.46*
- [x] **T38** — Add a `README.md` section for 002 covering how to run it, a link to the
  flow document, and the one-line difference between what 001 demonstrates and what 002
  demonstrates. — *R2.46, R2.47*

## Results

Run on 2026-08-13 against the compose stack, all four 002 services plus 001 running.

| Task | Observed |
|---|---|
| T27 | One `ORDER_CREATED` at `partition=1 offset=0` reached all three services; inventory reserved 2 line items, notification sent one message, analytics counted one |
| T28 | `inventory-service`, `notification-service`, `analytics-service` each hold their own offset on `order-lifecycle` |
| T29 | A payment of 9999 against a 15000 total and an empty item list both returned `422`; topic end offsets unchanged at 1 before and after |
| T30 | `SHIPPED` on a `CREATED` order → `409 order … is CREATED; expected PACKED but got SHIPPED`; the real chain then ran `200/200/200` at sequences 2, 3, 4 |
| T31 | Forced `DELIVERED` published at sequence 2; all three logged `ILLEGAL_TRANSITION expected=PACKED`, **no** sequence gap; `GET /orders/{id}` still `CREATED` |
| T32 | Unknown order → `404`, nothing published |
| T33 | 12 orders across 3 partitions (6/3/3); **zero** appeared on more than one partition |
| T34 | With `notification-consumer` stopped, its group lagged 1 while the other two stayed at 0; on restart it caught up to 0 from its own committed offset |
| T35 | That restart raised `SEQUENCE_GAP expected=1 observed=4` — Kafka restored the position, nothing restored the fold |
| T36 | 001's `POST /simulate` with 50 orders published 400 events, 0 failed, 0 new violations; `order-lifecycle` untouched by it |

Inventory logging `PACKED` and `DELIVERED` at `INFO` while producing no handler output
is T18 working, not a miss: it receives every event and reacts to two of them.

## Notes

**T31 is the task that keeps this a Kafka feature.** Without `force`, the producer's
own guard (T10, T12) would make T15 and T16 unreachable — a service that never emits an
illegal transition gives the consumers nothing to detect.

**The order store is deliberately in memory (T9).** Restarting `order-service` forgets
every order, so advancing a pre-restart order returns `404`. Do not "fix" this here; a
real service holds orders in a database and publishes through a transactional outbox,
and neither is in scope.
