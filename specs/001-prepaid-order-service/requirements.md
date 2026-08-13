# 001 — Prepaid Order Service with Consumer Fan-Out

**Status:** implemented
**Depends on:** [000-foundations](../000-foundations/requirements.md)

## Overview

An order service accepts a **prepaid** order over HTTP, records it, and publishes an
`ORDER_CREATED` event. Three independent services — inventory, notification, analytics
— each consume that event in their own consumer group and do their own work. The order
is then advanced through `PACKED → SHIPPED → DELIVERED`, one event at a time, and each
event fans out to all three again.

**This is a Kafka feature, not an e-commerce feature.** The order domain is a familiar
backdrop; the property under study is that a per-order sequence arrives in the right
order, and that independent services can each react to it without knowing about one
another.

Three mechanisms carry the lesson.

**Key → partition.** Every event is keyed by `order_id`, so one order's events land on
one partition and are therefore ordered. Events for *different* orders have no ordering
guarantee between them, and that is correct rather than a limitation.

**Fan-out by consumer group.** One topic, three group ids. Every service receives every
message and none of them consumes it away from the others. That is the counterpart to
002's scale-out, where extra consumers *in one group* divide the messages between them.
Meeting fan-out first is deliberate — it makes 002 a contrast rather than a variation.

**The synchronous/asynchronous boundary.** `POST /orders` returns an `order_id` because
the caller is blocked waiting for it, so it is an HTTP call and a publish it waits on.
Everything downstream of the event is not the caller's business and happens off the log.

This is the first feature in the repository to contain application code.

## Out of scope

Each is a later feature, or deliberately deferred; none may be built here.

- Bulk or rate-controlled event generation, and the lag and throughput experiments that
  need it — deferred by explicit request
- Multiple consumer instances in one group, rebalancing, partition assignment (002)
- Durable service or consumer state, delivery-semantic hardening (003)
- Multi-broker clusters, replication, `acks` tuning, failover (004)
- Retries, dead-letter topics, poison-message handling (005)
- Log compaction and tombstones (006)
- Local state stores and changelog topics (007)
- Transactions and exactly-once semantics (008)
- Stream processing engines and SQL over streams (009)
- Schema Registry, Avro/Protobuf, schema evolution
- A real database, a transactional outbox, or a real payment gateway
- Consumer-side idempotency or deduplication (003, 008)
- Authentication, TLS, ACLs — the environment stays PLAINTEXT per 000

## User stories

**US-1** — As a developer, I want to place a prepaid order in a single HTTP call and
get an `order_id` back, so that I can see which part of an order flow is synchronous
and why.

**US-2** — As a developer, I want the service to refuse an event that does not follow
its order's current state, so that I can see a real service guarding its own
transitions instead of publishing whatever it is told.

**US-3** — As a developer, I want to override that guard on demand, so that I can put a
genuinely out-of-order event on the topic and watch the consumers detect it.

**US-4** — As a developer, I want three separate services to receive every event
independently, so that I can see fan-out by consumer group rather than assume it.

**US-5** — As a developer, I want each service to react differently to the same event,
so that the value of decoupling is observable rather than asserted.

**US-6** — As a developer, I want to stop one service, advance an order, and restart
it, so that I can confirm one slow or dead consumer group does not hold up the others.

**US-7** — As a developer, I want a short document describing this flow end to end, so
that I can re-read the project later and recognise what it does.

## Acceptance criteria

### Event contract

- **R1.1** — THE SYSTEM SHALL give every event an `event_id`, an `order_id`, a
  `sequence`, an `event_type`, an `occurred_at` timestamp, and an event-type-specific
  `payload`.
- **R1.2** — THE SYSTEM SHALL support exactly the event types `ORDER_CREATED`,
  `PACKED`, `SHIPPED`, and `DELIVERED`.
- **R1.3** — WHEN an event is published for an `order_id` THE SYSTEM SHALL assign it
  the next `sequence` for that `order_id`, starting at 1 and increasing by exactly 1.
- **R1.4** — THE SYSTEM SHALL give each event a globally unique `event_id`.
- **R1.5** — THE SYSTEM SHALL define exactly one legal predecessor state per
  `event_type`, forming the chain `CREATED → PACKED → SHIPPED → DELIVERED`.
- **R1.6** — THE SYSTEM SHALL carry `customer_id`, `items`, `total_amount`, and
  `payment` in the payload of an `ORDER_CREATED` event, where each item has `sku`,
  `qty`, and `unit_price`, and `payment` has `method`, `reference`, and `amount`.
- **R1.7** — THE SYSTEM SHALL carry `carrier` and `tracking_number` in the payload of a
  `SHIPPED` event.
- **R1.8** — THE SYSTEM SHALL express all money as integer minor units.

### Topic and partitioning

- **R1.9** — THE SYSTEM SHALL publish all lifecycle events to a single topic with at
  least 3 partitions, so that partition-level ordering is distinguishable from
  topic-level ordering.
- **R1.10** — WHEN two events share an `order_id` THE SYSTEM SHALL publish them to the
  same partition.
- **R1.11** — WHEN the topic does not exist THE SYSTEM SHALL fail with an explicit
  error rather than creating it implicitly, per R0.14.

### Placing a prepaid order

- **R1.12** — THE SYSTEM SHALL expose an HTTP endpoint that accepts a customer
  identifier, one or more items, and a settled payment, and creates one order.
- **R1.13** — WHEN an order is created THE SYSTEM SHALL assign it a unique `order_id`
  and record its state as `CREATED`.
- **R1.14** — IF the payment amount does not equal `Σ(qty × unit_price)` across the
  items THEN THE SYSTEM SHALL reject the request with `422` and SHALL NOT publish any
  event.
- **R1.15** — IF the request carries no items THEN THE SYSTEM SHALL reject it with
  `422` and SHALL NOT publish any event.
- **R1.16** — WHEN an order is created THE SYSTEM SHALL publish exactly one
  `ORDER_CREATED` event carrying the items, the total, and the payment.
- **R1.17** — WHEN an order has been created THE SYSTEM SHALL return `201` with the
  `order_id` and the partition and offset the broker assigned to its `ORDER_CREATED`
  event.
- **R1.18** — IF the broker does not acknowledge the `ORDER_CREATED` event THEN THE
  SYSTEM SHALL return an error response and SHALL NOT report a partition or offset.

### Advancing an order

- **R1.19** — THE SYSTEM SHALL expose an HTTP endpoint that publishes one event of a
  caller-supplied type for an existing `order_id`.
- **R1.20** — IF the `order_id` is not known THEN THE SYSTEM SHALL return `404` and
  SHALL NOT publish.
- **R1.21** — IF the requested `event_type` is not the legal successor of the order's
  current state THEN THE SYSTEM SHALL return `409` naming both the current state and
  the expected event type, and SHALL NOT publish.
- **R1.22** — IF the payload does not match the requested `event_type` THEN THE SYSTEM
  SHALL return `422` and SHALL NOT publish.
- **R1.23** — WHEN an event has been published THE SYSTEM SHALL advance the order's
  recorded state and return the partition and offset the broker assigned.
- **R1.24** — WHERE the caller requests a forced publish THE SYSTEM SHALL bypass the
  R1.21 transition guard and publish the event, so that an out-of-order event reaches
  the topic and consumer detection can be observed.
- **R1.25** — THE SYSTEM SHALL default the forced-publish mode to off, so that a
  request that does not ask for it gets real-service behaviour.
- **R1.26** — WHEN an event is published under a forced publish THE SYSTEM SHALL still
  assign the next contiguous `sequence`, so that the illegal transition and the
  sequence remain independent signals.
- **R1.27** — THE SYSTEM SHALL expose an HTTP endpoint reporting one order's recorded
  state, last assigned sequence, items, and total.

### Fan-out

- **R1.28** — THE SYSTEM SHALL run three consumer services — inventory, notification,
  and analytics — each with a distinct consumer group identity, all subscribed to the
  one topic.
- **R1.29** — WHEN an event is published THE SYSTEM SHALL deliver it to all three
  services independently.
- **R1.30** — WHILE one service is stopped THE SYSTEM SHALL continue delivering events
  to the other two without delay.
- **R1.31** — WHEN a stopped service is restarted THE SYSTEM SHALL resume it from its
  own group's last committed offset.
- **R1.32** — THE SYSTEM SHALL commit a service's offset only after that service has
  handled the event.

### Per-service behaviour

- **R1.33** — THE SYSTEM SHALL have the inventory service react to `ORDER_CREATED` and
  `SHIPPED` only, and ignore other event types without error.
- **R1.34** — THE SYSTEM SHALL have the notification service react to all four event
  types with a distinct customer-facing message per type.
- **R1.35** — THE SYSTEM SHALL have the analytics service react to all four event types
  by maintaining a count per event type.
- **R1.36** — THE SYSTEM SHALL make each service's reaction observable in its log,
  identifying the service by name.
- **R1.37** — THE SYSTEM SHALL select which service a consumer process runs as from the
  environment, so that all three run from one image and one entry point.

### Consumer-side detection

- **R1.38** — WHEN a consumed event has a `sequence` that is not exactly one greater
  than the last accepted `sequence` for its `order_id` THE SYSTEM SHALL record a
  sequence-gap violation.
- **R1.39** — WHEN a consumed event's `event_type` is not a legal successor of the
  order's current state THE SYSTEM SHALL record an illegal-transition violation.
- **R1.40** — WHEN a violation is recorded THE SYSTEM SHALL continue consuming
  subsequent events rather than halting.
- **R1.41** — THE SYSTEM SHALL log violations at a severity distinguishable from normal
  processing, so that they can be isolated by filtering alone.
- **R1.42** — WHEN an event is consumed THE SYSTEM SHALL log the service name,
  partition, offset, key, `order_id`, `sequence`, and `event_type`.

### Configuration

- **R1.43** — THE SYSTEM SHALL read the broker address, topic name, service name, and
  consumer group identity from environment variables, with no connection details in
  source.
- **R1.44** — THE SYSTEM SHALL run correctly both from the host against
  `localhost:9092` and from inside the compose network against `kafka:19092`, changing
  only environment variables.

### Documentation

- **R1.45** — THE SYSTEM SHALL provide a document describing the flow end to end: what
  happens synchronously, what happens asynchronously, which service reacts to which
  event, and a runnable walkthrough from order creation through delivery.
- **R1.46** — THE SYSTEM SHALL state, in that document, which parts of the flow are
  realistic and which are simplifications a production service would not make.

## Notes

**Why there is no `PAID` event.** Payment settles before the order exists, so it is a
field on `ORDER_CREATED`, not a step in the chain. The consequence is worth naming:
R1.14 puts the total check at the API boundary, where it rejects the request, rather
than leaving each consumer to notice a bad total after the fact. That placement *is*
the lesson — validate what you can before the event exists, because once it is on the
log every consumer inherits it, and there are three of them here.

**Why `force` (R1.24) exists.** Without it, R1.21 would make the consumer-side checks
in R1.38 and R1.39 unreachable — a service that refuses to emit an illegal transition
never produces one to detect. The default is real-service behaviour; the flag is the
lab lever, and it defaults off so a request that does not ask for it gets the honest
path.

**Known limitation, accepted.** Order state lives in the service's memory (there is no
database in scope), so restarting the order service forgets every order and advancing a
pre-restart order returns `404` under R1.20. Consumer fold state is equally volatile,
which is why a consumer restart mid-order produces a sequence-gap violation that cannot
be told apart from a real one. Neither is to be "fixed" here — a real service would
hold orders in Postgres and publish through a transactional outbox, and durable
consumer state is spec 003.
