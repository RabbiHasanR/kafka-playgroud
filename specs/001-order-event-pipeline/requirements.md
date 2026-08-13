# 001 — Ordered Order-Event Pipeline

**Status:** draft — awaiting approval
**Depends on:** [000-foundations](../000-foundations/requirements.md)

## Overview

A single producer publishes order-lifecycle events to a single partitioned topic; a
single consumer folds them into per-order state and reports every ordering violation
it detects.

The purpose is not the order domain. It is to make **partitioning and ordering
observable**: to show that Kafka guarantees order *within a partition only*, that the
message key is what puts related events in the same partition, and that a consumer's
derived state is a separate concern from its committed offset.

Three independent violation signals are required, deliberately overlapping:

| Signal | Detects | Why it is needed |
|---|---|---|
| Illegal state transition | `SHIPPED` before `PAID` | domain-level, intuitive |
| Sequence gap | `seq != prev + 1` | mechanical, needs no domain reasoning |
| Wrong running total | folded sum ≠ paid amount | a true accumulator — cannot be derived from one message |

The running total exists because the first two are *arguable*. A sequence number is
self-describing but not self-validating: `seq: 4` is only wrong relative to a
remembered `3`, and a consumer that has lost its state cannot distinguish a genuine
gap from its own amnesia. A folded total has no such ambiguity — without prior state
there is no total at all.

This is the first feature in the repository to contain application code.

## Out of scope

Each of these is a later feature; none may be built here.

- A realistic prepaid order service and multi-service fan-out (002)
- Multiple consumers, consumer groups, rebalancing, partition assignment (003)
- Durable or external consumer state, delivery-semantic hardening (004)
- Multi-broker clusters, replication, `acks` tuning, failover (005)
- Retries, dead-letter topics, poison-message handling (006)
- Log compaction and tombstones (007)
- Local state stores and changelog topics (008)
- Transactions and exactly-once semantics (009)
- Stream processing engines and SQL over streams (010)
- Schema Registry, Avro/Protobuf, schema evolution
- Authentication, TLS, ACLs — the environment stays PLAINTEXT per 000

## User stories

**US-1** — As a developer, I want to publish order events over HTTP, so that
producing is realistic and I can control what is sent without editing code.

**US-2** — As a developer, I want to see which partition and offset each event landed
at, so that the relationship between key and partition is visible rather than assumed.

**US-3** — As a developer, I want to generate many orders at a controlled rate, so
that I can observe consumer lag and throughput rather than only single messages.

**US-4** — As a developer, I want the consumer to reject events that arrive in the
wrong order, so that ordering is proven by observed failure instead of asserted.

**US-5** — As a developer, I want to deliberately break ordering on demand, so that I
can see violations immediately instead of waiting for one to occur naturally.

**US-6** — As a developer, I want the consumer to maintain a value that can only be
computed by accumulating events, so that the need for consumer-held state is
demonstrable and not a matter of opinion.

**US-7** — As a developer, I want to restart the consumer and see it resume at the
committed offset while having forgotten its folded state, so that I understand
position and derived state are different things.

## Acceptance criteria

### Event contract

- **R1.1** — THE SYSTEM SHALL give every event an `order_id`, a `sequence`, an
  `event_type`, an `occurred_at` timestamp, and an event-type-specific `payload`.
- **R1.2** — WHEN an event is published for an `order_id` THE SYSTEM SHALL assign it
  the next `sequence` for that `order_id`, starting at 1 and increasing by exactly 1.
- **R1.3** — THE SYSTEM SHALL support the event types `ORDER_CREATED`, `ITEM_ADDED`,
  `PAID`, `PACKED`, `SHIPPED`, and `DELIVERED`.
- **R1.4** — THE SYSTEM SHALL carry `sku`, `qty`, and `unit_price` in the payload of
  an `ITEM_ADDED` event, and `amount` in the payload of a `PAID` event.
- **R1.5** — THE SYSTEM SHALL define exactly one legal predecessor set per
  `event_type`, such that any other predecessor is detectable as an illegal
  transition.

### Topic and partitioning

- **R1.6** — THE SYSTEM SHALL publish all order events to a single topic with at
  least 3 partitions, so that partition-level ordering is distinguishable from
  topic-level ordering.
- **R1.7** — WHEN two events share an `order_id` THE SYSTEM SHALL publish them to the
  same partition.
- **R1.8** — THE SYSTEM SHALL NOT guarantee any ordering between events with
  different `order_id`s. (Stated as a requirement because the absence of this
  guarantee is a property to be demonstrated, not a limitation to be worked around.)
- **R1.9** — WHEN the topic does not exist THE SYSTEM SHALL fail with an explicit
  error rather than creating it implicitly, per R0.14.

### Producing

- **R1.10** — THE SYSTEM SHALL expose an HTTP endpoint that publishes exactly one
  event for a caller-supplied `order_id`.
- **R1.11** — THE SYSTEM SHALL expose an HTTP endpoint that generates a
  caller-supplied number of complete order lifecycles at a caller-supplied rate in
  events per second.
- **R1.12** — WHEN an event has been published THE SYSTEM SHALL return the partition
  and offset the broker assigned to it.
- **R1.13** — IF the broker does not acknowledge an event THEN THE SYSTEM SHALL
  return an error response and SHALL NOT report a partition or offset.
- **R1.14** — WHEN the producer shuts down THE SYSTEM SHALL flush buffered events
  before exiting.

### Fault injection

- **R1.15** — WHERE the caller requests unkeyed publishing THE SYSTEM SHALL publish
  events without a message key, so that events for one `order_id` are distributed
  across partitions.
- **R1.16** — WHERE the caller requests shuffled publishing THE SYSTEM SHALL publish
  the events of an order in an order other than ascending `sequence`.
- **R1.17** — THE SYSTEM SHALL default both fault-injection modes to off, so that a
  request that does not ask for them produces a clean stream.

### Consuming and violation detection

- **R1.18** — THE SYSTEM SHALL maintain, per `order_id`, the last accepted
  `sequence`, the current lifecycle state, a running item count, and a running total
  of `qty × unit_price` across all `ITEM_ADDED` events.
- **R1.19** — WHEN a consumed event has a `sequence` that is not exactly one greater
  than the last accepted `sequence` for its `order_id` THE SYSTEM SHALL record a
  sequence-gap violation.
- **R1.20** — WHEN a consumed event's `event_type` is not a legal successor of the
  current lifecycle state for its `order_id` THE SYSTEM SHALL record an
  illegal-transition violation.
- **R1.21** — WHEN a `PAID` event is consumed and its `amount` does not equal the
  running total for its `order_id` THE SYSTEM SHALL record a total-mismatch
  violation.
- **R1.22** — WHEN a violation is recorded THE SYSTEM SHALL continue consuming
  subsequent events rather than halting.
- **R1.23** — THE SYSTEM SHALL record each violation with its type, `order_id`,
  expected value, and observed value.
- **R1.24** — WHEN an event is consumed for an `order_id` with no known state THE
  SYSTEM SHALL treat it as a new order, and SHALL record a sequence-gap violation if
  its `sequence` is not 1.

### Offsets and state

- **R1.25** — THE SYSTEM SHALL commit the offset of an event only after that event
  has been processed.
- **R1.26** — WHEN the consumer restarts THE SYSTEM SHALL resume from the last
  committed offset rather than from the start of the topic.
- **R1.27** — WHEN the consumer restarts THE SYSTEM SHALL NOT restore any per-order
  state accumulated before the restart. (Intentional for this feature: the resulting
  false violations and wrong totals are the evidence that a committed offset is a
  position, not a memory. Spec 004 removes this.)
- **R1.28** — WHERE the consumer is started with a previously unused group identity
  THE SYSTEM SHALL read the topic from its earliest retained offset.

### Observability

- **R1.29** — WHEN an event is consumed THE SYSTEM SHALL log its partition, offset,
  key, `order_id`, `sequence`, and `event_type`.
- **R1.30** — THE SYSTEM SHALL log violations at a severity distinguishable from
  normal processing, so that they can be isolated by filtering alone.
- **R1.31** — THE SYSTEM SHALL report, on demand, the current folded state of every
  known `order_id`, so that state loss across a restart is directly observable.

### Configuration

- **R1.32** — THE SYSTEM SHALL read the broker address, topic name, and consumer
  group identity from environment variables, with no connection details in source.
- **R1.33** — THE SYSTEM SHALL run correctly both from the host against
  `localhost:9092` and from inside the compose network against `kafka:19092`,
  changing only environment variables.

## Notes

R1.27 is a requirement, not a defect. It is the only criterion in this spec that
mandates a shortcoming, and it exists because the shortcoming is the lesson. It must
not be "fixed" within this feature — doing so would remove the evidence that
motivates spec 004.
