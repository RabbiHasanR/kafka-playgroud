# 001 — Prepaid Order Service with Consumer Fan-Out: Tasks

Implements [requirements.md](requirements.md) per [design.md](design.md).

Each task cites the requirement IDs it satisfies and, where relevant, the design
decision it follows.

## Shared contract

- [x] **T1** — Add `config.py` reading `KAFKA_BOOTSTRAP_SERVERS`,
  `ORDER_LIFECYCLE_TOPIC`, `SERVICE_NAME`, and `CONSUMER_GROUP_ID` from the
  environment with host-usable defaults and no connection details in source.
  — *R1.43, R1.44* — D12
- [x] **T2** — Define the event model in `events.py`: `event_id`, `order_id`,
  `sequence`, `event_type`, `occurred_at`, `payload`, with `event_id` defaulting to a
  fresh UUID4. — *R1.1, R1.4* — D11
- [x] **T3** — Define the four event types and the payload models: `ORDER_CREATED`
  (`customer_id`, `items[sku, qty, unit_price]`, `total_amount`, `payment[method,
  reference, amount]`) and `SHIPPED` (`carrier`, `tracking_number`), with all money as
  integer minor units. — *R1.2, R1.6, R1.7, R1.8* — D3
- [x] **T4** — Enforce in the `ORDER_CREATED` payload model that `items` is non-empty
  and `total_amount == Σ(qty × unit_price) == payment.amount`, so a mismatch cannot be
  represented as a valid event. — *R1.14, R1.15* — D3
- [x] **T5** — Define the lifecycle transition table mapping each event type to its
  legal predecessor state, forming `CREATED → PACKED → SHIPPED → DELIVERED`, importable
  by both producer and consumer. — *R1.5* — D3
- [x] **T6** — Write `scripts/create_topics.sh` so it iterates over a topic list,
  creating `order-lifecycle` with 3 partitions and replication factor 1, and confirm a
  missing topic errors rather than being auto-created. — *R1.9, R1.11* — D13

## Producer — order service

- [x] **T7** — Implement `producer/kafka_producer.py`: construct the `Producer`,
  publish with the UTF-8 `order_id` as key using the default partitioner, wait on the
  delivery report, and return the assigned partition and offset — or a named error when
  the broker does not acknowledge within the timeout, distinguishing a missing topic
  from an unreachable broker. — *R1.10, R1.11, R1.18* — D6
- [x] **T8** — Start a daemon `poll()` thread from the FastAPI lifespan and stop it and
  `flush()` with a timeout on shutdown, so delivery callbacks always fire.
  — *R1.17, R1.18* — D6
- [x] **T9** — Implement `producer/orders.py`: the `Order` record, an in-memory store
  under a lock, unique `order_id` assignment, and per-order sequence allocation
  starting at 1 and increasing by exactly 1. — *R1.3, R1.13* — D5
- [x] **T10** — Implement the transition guard in the store: unknown order and illegal
  successor are distinct, named failures, and a `force` reservation bypasses the
  successor check while still taking the next contiguous sequence and leaving the
  recorded state unchanged. — *R1.21, R1.24, R1.25, R1.26* — D4, D5
- [x] **T11** — Implement `POST /orders` as a synchronous `def` handler: validate the
  payment total, create the order as `CREATED`, publish exactly one `ORDER_CREATED` at
  sequence 1, and return `201` with the `order_id`, partition, and offset.
  — *R1.12, R1.13, R1.14, R1.15, R1.16, R1.17* — D3, D6
- [x] **T12** — Implement `POST /orders/{order_id}/events`: `404` for an unknown order,
  `409` naming the current state and the expected event type for an illegal transition,
  `422` for a payload that does not match the event type, and on success publish,
  advance the recorded state, and return the real partition and offset.
  — *R1.19, R1.20, R1.21, R1.22, R1.23* — D4
- [x] **T13** — Implement `GET /orders/{order_id}` reporting the recorded state, last
  sequence, items, and total, plus `GET /health`. — *R1.27*

## Consumer — the three services

- [x] **T14** — Implement `consumer/runtime.py`: subscribe, poll, decode, detect,
  dispatch, commit, with `enable.auto.commit=False`, `auto.offset.reset=earliest`, and
  the offset committed only after the handler returns. — *R1.31, R1.32* — D8, D10
- [x] **T15** — Fold per-order `(last_sequence, state)` in the runtime and record a
  sequence-gap violation when `sequence != last + 1`, including the unknown-order case
  where any sequence other than 1 is a gap. — *R1.38* — D9
- [x] **T16** — Record an illegal-transition violation using the T5 transition table,
  and continue consuming after any violation rather than halting. — *R1.39, R1.40* — D9
- [x] **T17** — Log every consumed record at `INFO` with the service name, partition,
  offset, key, `order_id`, `sequence`, and `event_type`; log every violation at
  `WARNING` with a stable `VIOLATION` marker. — *R1.41, R1.42* — D14
- [x] **T18** — Implement the inventory service: react to `ORDER_CREATED` and `SHIPPED`
  only, logging the stock reservation and its release, and ignore the other two event
  types without error. — *R1.33, R1.36* — D8
- [x] **T19** — Implement the notification service: react to all four event types with
  a distinct customer-facing message per type. — *R1.34, R1.36* — D8
- [x] **T20** — Implement the analytics service: react to all four event types by
  maintaining and logging a count per event type. — *R1.35, R1.36* — D8
- [x] **T21** — Implement `consumer/main.py`: a registry mapping `SERVICE_NAME` to its
  default group id and handler map, so all three services run from one image and one
  entry point. — *R1.28, R1.37* — D8, D12

## Wiring

- [x] **T22** — Add `order-service` and the three consumer services to
  `docker-compose.yml`, gated on broker health, each consumer with its own
  `SERVICE_NAME` and distinct `CONSUMER_GROUP_ID`, changing no broker configuration and
  changing no broker configuration. — *R1.28, R1.44* — D7, D12

## Verification experiments

Each is run and observed, not merely coded. Tick only after actually running it.

- [x] **T27** — **Fan-out.** Create one order and confirm the same event appears in all
  three consumer logs, each doing different work. — *R1.28, R1.29, R1.36*
- [x] **T28** — **Three groups, one topic.** `kafka-consumer-groups.sh --list` shows
  three distinct groups, and `--describe` shows each with its own offset on
  `order-lifecycle`. — *R1.28, R1.29*
- [x] **T29** — **Validation before the event exists.** `POST /orders` with a payment
  amount disagreeing with the item sum returns `422`, and the topic's end offsets are
  unchanged — nothing was published. — *R1.14*
- [x] **T30** — **The service guards its own transitions.** `SHIPPED` on a freshly
  created order returns `409` naming `CREATED` and `PACKED`, and nothing is published;
  then `PACKED → SHIPPED → DELIVERED` in order each return `200` and each fan out to
  all three services. — *R1.21, R1.23, R1.29*
- [x] **T31** — **Forced out-of-order publish.** The same rejected call with
  `force: true` publishes, and all three services log an illegal-transition violation
  with **no** sequence gap — the two signals are independent. `GET /orders/{id}` shows
  the recorded state did not advance. — *R1.24, R1.26, R1.39*
- [x] **T32** — **Unknown order.** Publishing an event for an `order_id` the service
  never created returns `404` and publishes nothing. — *R1.20*
- [x] **T33** — **Key → partition.** Create ~10 orders, advance each, and confirm with
  `kafka-console-consumer.sh --partition N --property print.key=true` that every event
  for one `order_id` is in exactly one partition. — *R1.10*
- [x] **T34** — **One group stopped, the others unaffected.** Stop the notification
  consumer, advance an order to `DELIVERED`, and confirm inventory and analytics stayed
  current throughout; restart notification and confirm it catches up from its own
  committed offset. — *R1.30, R1.31*
- [x] **T35** — **Sequence gap from amnesia.** Restart a consumer mid-order and confirm
  the next event raises a sequence-gap violation that is indistinguishable from a real
  producer-side gap: Kafka restored the position, nothing restored the fold. — *R1.38*

## Documentation

- [x] **T37** — Write `docs/order-flow.md`: what is synchronous and what is
  asynchronous, the fan-out diagram, which service reacts to which event, and a
  copy-paste walkthrough from `POST /orders` through `DELIVERED`. — *R1.45*
- [x] **T38** — Add a `README.md` section covering how to run the feature, a link to
  the flow document, and which parts of the flow are simplifications. — *R1.45, R1.46*

## Results

Run on 2026-08-13 against the compose stack, all four services running. Re-run after
the repository was reduced to this one feature (X8); numbers below are from that run.

| Task | Observed |
|---|---|
| T27 | One `ORDER_CREATED` at `partition=2 offset=10` reached all three services; inventory reserved 2 line items, notification sent one message, analytics counted one |
| T28 | `inventory-service`, `notification-service`, `analytics-service` each hold their own offsets across all 3 partitions of `order-lifecycle` |
| T29 | A payment of 9999 against a 15000 total returned `422`; topic end offsets unchanged at 40 before and after |
| T30 | `SHIPPED` on a `CREATED` order → `409 order … is CREATED; expected PACKED but got SHIPPED`; the real chain then ran `200/200/200` at sequences 2, 3, 4 on one partition |
| T31 | Forced `DELIVERED` published at sequence 2; all three logged `ILLEGAL_TRANSITION expected=PACKED`, **no** sequence gap; `GET /orders/{id}` still `CREATED` with `expected_next_event=PACKED` |
| T32 | Unknown order → `404`, nothing published |
| T33 | 25 orders across 3 partitions (13/6/6); **zero** appeared on more than one partition |
| T34 | With `notification-consumer` stopped, its group lagged 1 while the other two stayed at 0; on restart it caught up to 0 from its own committed offset |
| T35 | That restart raised `SEQUENCE_GAP expected=1 observed=3` — Kafka restored the position, nothing restored the fold |

Inventory logging `PACKED` and `DELIVERED` at `INFO` while producing no handler output
is T18 working, not a miss: it receives every event and reacts to two of them.

## Notes

**T23–T26 and T36 are gone, and the numbers are deliberately not reused.**

T23–T26 were unit tests. This project does not carry a test suite (see `CLAUDE.md`);
correctness is established by the T27–T35 experiments above, run against a real broker.
The requirements they cited stay covered: R1.6, R1.7, R1.14 and R1.15 by T3 and T4;
R1.3, R1.20, R1.21, R1.24 and R1.26 by T9, T10 and T12; R1.38, R1.39 and R1.40 by T15
and T16; R1.33, R1.34, R1.35 and R1.37 by T18–T21.

T36 verified that a second, earlier feature still worked alongside this one. That
feature was removed from the repository (see X8), taking its requirement with it.

**T31 is the task that keeps this a Kafka feature.** Without `force`, the producer's
own guard (T10, T12) would make T15 and T16 unreachable — a service that never emits an
illegal transition gives the consumers nothing to detect.

**The order store is deliberately in memory (T9).** Restarting `order-service` forgets
every order, so advancing a pre-restart order returns `404`. Do not "fix" this here; a
real service holds orders in a database and publishes through a transactional outbox,
and neither is in scope.
