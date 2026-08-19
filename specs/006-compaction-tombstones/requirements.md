# 006 — Compaction and Tombstones

**Status:** draft — awaiting approval
**Depends on:** [005-retries-dlq-poison-messages](../005-retries-dlq-poison-messages/requirements.md)

## Overview

Nothing in this repository can delete an order. `order-lifecycle` is append-only, the
consumers' `order_fold` rows grow forever, and there is no HTTP verb that removes
anything. That is not an oversight in 001–005 — it is the gap this rung exists to close,
and closing it requires the one Kafka mechanism the ladder has not met yet.

**A Kafka topic can be a log or a table, and the retention policy is the choice.** Under
`cleanup.policy=delete` — everything so far — a topic is a log: messages age out by time
or size, and the same key appears as many times as it was written. Under
`cleanup.policy=compact` a topic is a table: the log cleaner retains the *latest value
per key* indefinitely and garbage-collects the rest. Replaying a compacted topic
rebuilds current state at a cost proportional to the number of **keys**, not the number
of **events**. That is the mechanism 007 hand-rolls and 009's ksqlDB runs on internally.

**Compaction cannot go on `order-lifecycle`, and understanding why is half the lesson.**
An order is four messages under one key, and each is an *increment*: `PACKED` does not
carry the items, `SHIPPED` does not carry the payment. Compaction would keep only the
newest and discard the rest, leaving a `SHIPPED` event with no creation behind it — a
fold that reports a permanent `SEQUENCE_GAP` and an order that cannot be reconstructed.
Compaction is safe only where each message **replaces** its predecessor rather than
adding to it. So this feature introduces a second topic whose messages are self-contained
snapshots, and leaves the event log exactly as it is.

**A tombstone is a delete, scoped to one key in one topic.** Producing a null value under
a key tells the cleaner to erase that key, and then, after `delete.retention.ms`, to erase
the tombstone itself. That delay is not incidental: it is the window in which a restarting
or lagging consumer still learns about the delete. Remove tombstones immediately and an
offline consumer bootstraps from the log, never sees the marker, and resurrects state that
was deleted days ago.

**Null is legal anywhere; only the cleaner makes it a tombstone.** A null-valued message
can be produced to any topic and every consumer will see `None`. What a compacted topic
adds is the broker-side half — the older values disappear, and eventually so does the
marker. On a `delete` topic the same message is just a message. The mirror rule: a
compacted topic **rejects a null key**, because there is nothing to compact by.

## Out of scope

Each is a later feature or a gap this rung deliberately leaves open; none may be built here.

- **Preventing a deleted order from being resurrected.** Kafka has no cross-topic delete,
  so a tombstone leaves the order's events in `order-lifecycle`, its pending messages in
  the retry topic, and its dead letters in the DLQ. Three paths therefore recreate a
  deleted fold: a replay from earliest (worse once the tombstone has itself been
  compacted away), a retry worker waking up after the delete, and `dlq_replay.py`
  republishing. All three share one root cause — the fold has **two sources with
  independent retention** — and that is 007's subject, where state is rebuilt from the
  changelog alone. This feature documents them; it does not close them.
- **An `ORDER_DELETED` lifecycle event.** It would close the replay path, but needs a new
  `EventType`, a terminal `OrderState`, and a `LEGAL_PREDECESSOR` table that can express
  "from any state" — a change to the shared event contract, out of proportion to this rung.
- **Deriving the fold from the snapshot topic.** The event log stays the source of truth
  here. Rebuilding state from a compacted changelog is 007 (X5).
- **RocksDB or any local state store** (007); **transactions and exactly-once** (008);
  **stream SQL** (009).
- **Tiered delay topics, DLQ depth alerting, unclean leader election** — still open from 005 and 004.
- Any change to the event contract, to the fold's shape, to 002's protocol and membership
  levers, or to the cleanup policy of `order-lifecycle`, `order-lifecycle.retry` or
  `order-lifecycle.dlq`.
- Authentication, TLS, ACLs — the environment stays PLAINTEXT per 000.

## User stories

**US-1** — As a developer, I want a topic that behaves as a table rather than a log, so
that I can see what "the latest value per key survives" means against real messages
instead of reading about it.

**US-2** — As a developer, I want to delete an order over HTTP, so that removal is a
first-class operation rather than something I do with `psql`.

**US-3** — As a developer, I want that delete to travel as a tombstone, so that the
consumers learn about it the same way they learn about everything else — by reading a
message — rather than by a side channel.

**US-4** — As a developer, I want each consumer group to drop its own folded state for a
deleted order, so that "the order is gone" is true in all four places it is recorded and
not only in the producer.

**US-5** — As a developer, I want to watch superseded values and then the tombstone itself
disappear from the topic, so that compaction and `delete.retention.ms` are something I
observe rather than trust.

**US-6** — As a developer, I want a document covering this feature end to end, so that I
can re-read later why the event log is not compacted, what a tombstone does and does not
reach, and which gaps 007 closes.

## Acceptance criteria

### The compacted topic

- **R6.1** — THE SYSTEM SHALL provide a topic dedicated to current per-order state, keyed
  by `order_id`, created with `cleanup.policy=compact` and with the same partition count
  as the lifecycle topic, so that an order's snapshot and its events occupy the same
  partition number.
- **R6.2** — THE SYSTEM SHALL leave the cleanup policy of `order-lifecycle`,
  `order-lifecycle.retry` and `order-lifecycle.dlq` unchanged at `delete`.
- **R6.3** — THE SYSTEM SHALL read the snapshot topic's name, segment roll interval,
  minimum cleanable dirty ratio and tombstone retention from the environment, defaulting
  to values that make compaction observable within a single working session rather than
  to the broker's production defaults.

### Writing the table

- **R6.4** — WHEN a lifecycle event has been acknowledged by the broker THE SYSTEM SHALL
  publish the order's complete current state to the snapshot topic under that order's key,
  such that the message is self-contained and no earlier message is needed to interpret it.
- **R6.5** — IF publishing a snapshot fails THEN THE SYSTEM SHALL log the failure at
  WARNING and SHALL NOT fail the request that produced the lifecycle event, because the
  event log and not the snapshot is the source of truth.

### Deleting an order

- **R6.6** — THE SYSTEM SHALL provide an HTTP endpoint that deletes one order by id and
  publishes a tombstone — that order's key with a null value — to the snapshot topic.
- **R6.7** — IF no order with the given id is known THEN THE SYSTEM SHALL respond `404`
  and SHALL publish nothing.
- **R6.8** — WHEN the tombstone has been acknowledged by the broker THE SYSTEM SHALL
  remove the order from its own store and respond `204`.
- **R6.9** — IF the broker does not acknowledge the tombstone THEN THE SYSTEM SHALL fail
  the request naming the broker error and the partition it applied to, and SHALL leave the
  order in its store, so that a failed delete is retryable rather than half-applied.

### Consuming a tombstone

- **R6.10** — WHEN a consumer receives a message whose value is null THE SYSTEM SHALL
  delete that order's folded state for that consumer group, and SHALL NOT attempt to
  decode the message, route it to the retry topic, or route it to the dead-letter topic.
- **R6.11** — WHEN folded state has been deleted THE SYSTEM SHALL log the deletion under
  its own stable, greppable marker at WARNING or above, naming the order and the topic
  partition the tombstone arrived on.
- **R6.12** — WHEN a consumer receives a message on the snapshot topic whose value is not
  null THE SYSTEM SHALL commit it without altering folded state, so that the fold advances
  only on events.
- **R6.13** — WHEN a tombstone has been handled THE SYSTEM SHALL commit its offset, so a
  delete cannot stall the partition it arrived on.

### Configuration and documentation

- **R6.14** — THE SYSTEM SHALL read every setting this feature introduces from environment
  variables, and SHALL leave every default such that a producer or consumer started with
  none of them behaves as 005 recorded, except that it now also consumes the snapshot topic.
- **R6.15** — THE SYSTEM SHALL provide a document covering the difference between `delete`
  and `compact`, why the lifecycle topic is not compacted, the null-key rule and why a null
  value is only a tombstone under compaction, what `delete.retention.ms` protects against, a
  runnable walkthrough of superseded values and then a tombstone disappearing, and the three
  resurrection paths this feature leaves open with 007 named as where they close. The
  known-gaps tables in `README.md` that name 006 SHALL be updated to match.

## Notes

**Why the snapshot write does not block and the tombstone write does.** A snapshot is
derived state; if it is lost the event log still holds the truth and the next event
rewrites it. A `204` from the delete endpoint, by contrast, is a claim that the delete
landed, and a claim the broker never confirmed would be a lie. The asymmetry is deliberate
and R6.5 and R6.9 are the two halves of it.

**Why the consumers subscribe to the snapshot topic rather than a fourth service reading
it.** The state being deleted belongs to the three existing consumer groups, one row each.
A separate reader would have to delete other groups' rows, which breaks the independence
R3.2 established — a group's memory is its own. Subscribing to both topics keeps each
group deleting only what it owns.

**Why R6.12 exists at all.** Without it, a consumer reading a snapshot would fold it as
though it were an event, and the fold would then have two writers disagreeing about
`last_sequence`. The snapshot topic is written *for* compaction and read only for its
tombstones; saying so as a criterion stops the implementation drifting into 007's
territory by accident.

**What co-partitioning buys here (R6.1).** Both topics are keyed by `order_id` with the
same partitioner and the same partition count, so order X's snapshot and order X's events
land on partition number X-hash alike. The consumers' per-partition fold cache is keyed by
partition number, so a tombstone arriving on `order-snapshot-2` evicts from the same slot
the events on `order-lifecycle-2` populated. This is not incidental — it is the property
007 and 009 both depend on, met here for the first time.

**Criteria count.** 15, at the top of the roughly 12–15 the size budget recorded as
[X11](../../DECISIONS.md) sets.
