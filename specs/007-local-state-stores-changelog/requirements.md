# 007 — Local State Stores and Changelog Topics

**Status:** draft — awaiting approval
**Depends on:** [006-compaction-tombstones](../006-compaction-tombstones/requirements.md)

## Overview

Spec 003 put the consumers' fold in Postgres **knowing it was the wrong long-term answer**.
[X4](../../DECISIONS.md) says so in writing and books itself as *superseded by X5 at 007*.
The reason for choosing it anyway was pedagogic: a database makes the dual-write problem
impossible to miss, because the offset goes to Kafka and the fold goes somewhere else and
no single operation covers both. That lesson has landed. This rung collects the bill.

**State belongs next to the input it was folded from.** A member owning partition 2 needs
partition 2's orders and nothing else, yet `order_fold` is one shared table every process
contends on, holding rows for orders that member does not own the events for. Co-partitioned
state removes the shared server entirely: the instance holding partition 2 holds exactly
partition 2's keys, on its own disk, in a store no other process can even open.

**A local store is worthless without a way to rebuild it, and a compacted topic is that
way.** Local disk is not durable — a container is replaced, a partition moves, a volume is
lost. So every mutation is also written to a compacted **changelog** topic keyed identically
to the state, and a store is rebuilt by replaying its partition of that topic. This is where
006 pays off: compaction retains the latest value per key, so the rebuild costs the **number
of keys**, not the number of events. 003's `PostgresStateStore` refuses to warm its cache on
assignment for exactly this reason — a history-proportional scan at every rebalance is the
cost this feature removes.

**The changelog is per consumer group, and that is forced rather than chosen.** The three
services walk an order's stages at their own pace: inventory may sit at `PACKED` while
notification is already at `SHIPPED`. Compaction retains the latest value **per key**, so one
topic keyed by `order_id` would have the groups overwrite one another and every rebuild would
read whichever group wrote last. `PRIMARY KEY (group_id, order_id)` already encodes this;
the changelog inherits it as one topic per group.

**Local state cannot be shared, and that changes who does the work.** An embedded store takes
an exclusive lock on its directory, so only the process owning a partition may write that
partition's fold. `retry_worker` currently writes folds directly — possible only because a
database accepts connections from anywhere. After this feature the retried message travels to
the owner instead. Work moves to the state rather than state moving to the worker, which is
the practical consequence of co-partitioning and is worth meeting deliberately.

**Both writes become Kafka operations, which sets up 008.** The fold now goes to a topic and
the offset goes to a topic. They are still two operations with a window between them, so
003's `STATE_WRITE_ORDER` and `STATE_CRASH_AFTER` levers keep working and `handled_count`
still outruns `last_sequence`. What changed is that a single transaction can now cover both —
and that is precisely 008.

## Out of scope

Each is a later feature or a gap this rung deliberately leaves open; none may be built here.

- **Making the two writes atomic.** The changelog produce and the offset commit remain
  separate operations, `handled_count` still exceeds `last_sequence`, and a crash in the
  window still duplicates or loses work. Transactions are 008.
- **Closing resurrection by a deliberate replay.** Rebuilding from the changelog stops a
  restart or a rebalance from resurrecting a deleted order. Resetting the group's offsets to
  `earliest` re-reads `order-lifecycle` and re-folds the events, and the changelog tombstone
  only protects within `delete.retention.ms`. Genuine closure needs an `ORDER_DELETED`
  terminal event on the log, which 006 already placed out of scope and this rung does not
  reclaim. 006's README rows are corrected to say this rather than claim full closure.
- **Deriving the fold from `order-snapshot`.** It keeps its 006 role — a tombstone feed —
  and gains no new one. It carries no per-group `last_sequence` and no `handled_count`, so
  bootstrapping from it would merge the three groups' memories and undo R3.2.
- **Adopting Kafka Streams, Faust, or any stream-processing framework.** The mechanism is
  the lesson ([X5](../../DECISIONS.md)). A framework is 009's business, through ksqlDB.
- **Standby replicas, interactive queries, or a state-store HTTP endpoint.** Real Streams
  features, none of them needed to show co-partitioned state and a changelog rebuild.
- **Windowed or aggregate state of any kind** — the fold stays `(last_sequence, state)`.
- Any change to the event contract, to `order-lifecycle`'s cleanup policy, to 002's protocol
  and membership levers, or to 005's retry and dead-letter routing beyond what R7.13 and
  R7.14 require.
- Authentication, TLS, ACLs — the environment stays PLAINTEXT per 000.

## User stories

**US-1** — As a developer, I want each consumer instance to keep its folded state on its own
disk, so that I can see co-partitioned state as directories I can list rather than as rows in
a table everybody shares.

**US-2** — As a developer, I want every fold change written to a compacted changelog topic, so
that local disk stops being a single point of loss and the recovery path is one I can read
with a console consumer.

**US-3** — As a developer, I want a store rebuilt from that changelog when its partition is
assigned, so that a rebalance or a restart restores memory without a shared database and
without a history-proportional scan.

**US-4** — As a developer, I want to see what the rebuild actually cost — how many records it
read for how many keys — so that "proportional to keys, not events" is a number I measured
rather than a claim I accepted.

**US-5** — As a developer, I want a retried message to be folded by the instance that owns its
partition, so that I meet the constraint local state imposes instead of working around it.

**US-6** — As a developer, I want Postgres gone from the repository, so that the assembled
system has one place state lives and 003's shared-database compromise stops being load-bearing.

**US-7** — As a developer, I want a document covering this feature end to end, so that I can
re-read later why the changelog is per group, what a restore costs, and which internal topics
009's ksqlDB will create for the same reasons.

## Acceptance criteria

### The changelog topics

- **R7.1** — THE SYSTEM SHALL provide one changelog topic per consumer group, keyed by
  `order_id`, created with `cleanup.policy=compact` and with the same partition count as the
  lifecycle topic, so that an order's fold and its events occupy the same partition number.
- **R7.2** — THE SYSTEM SHALL leave the cleanup policy of `order-lifecycle`,
  `order-lifecycle.retry` and `order-lifecycle.dlq` unchanged at `delete`, and SHALL leave
  `order-snapshot` in its 006 role — read for its tombstones and never as a source of folded
  state.

### The local store

- **R7.3** — THE SYSTEM SHALL keep each consumer instance's folded state in an embedded store
  on local disk, holding one store per partition that instance currently owns, such that no
  other process can open a store that instance holds.
- **R7.4** — WHEN a fold advances THE SYSTEM SHALL write it to the local store and publish it
  to that consumer group's changelog topic under the order's key.
- **R7.5** — IF an event arrives at or below the stored sequence for its order THEN THE SYSTEM
  SHALL leave the fold unchanged and SHALL count the delivery, so that a redelivery is
  absorbed exactly as R3.11 and R3.13 required of the database.
- **R7.6** — WHEN a tombstone deletes an order's folded state THE SYSTEM SHALL remove it from
  the local store and publish a null value under that order's key to the changelog topic, so
  that a rebuild does not restore it.

### Rebuilding

- **R7.7** — WHEN partitions are assigned to a consumer THE SYSTEM SHALL rebuild each assigned
  partition's store by reading that partition of its changelog topic to the current high
  watermark **before** processing any message from that partition, and SHALL log the rebuild
  under its own stable, greppable marker naming the partition, the records read, the distinct
  keys restored, and the elapsed time.
- **R7.8** — THE SYSTEM SHALL read the changelog during a rebuild without joining a consumer
  group and without committing any offset, so that a rebuild leaves no trace in the group
  coordinator and cannot be confused with consumption.
- **R7.9** — WHERE resuming from a checkpoint is selected THE SYSTEM SHALL rebuild from the
  changelog offset last restored for that partition; otherwise THE SYSTEM SHALL rebuild from
  the beginning of the changelog partition.
- **R7.10** — WHEN partitions are revoked or lost THE SYSTEM SHALL release exactly those
  partitions' stores and SHALL NOT delete their changelog records, so that whoever is assigned
  the partition next can rebuild what this member folded.

### Ordering against the offset

- **R7.11** — THE SYSTEM SHALL confirm the changelog write with the broker before committing
  the offset of the message that produced it, so that the changelog is never behind the
  committed offset; and SHALL keep the write-order and crash-point levers of R3.16 and R3.17
  working against this pair of writes.

### The retry path

- **R7.12** — THE SYSTEM SHALL NOT write folded state from any process other than the one
  holding the partition that state belongs to; the retry worker SHALL instead republish a
  recovered message so that the owning instance folds it.
- **R7.13** — WHEN a republished message is consumed THE SYSTEM SHALL fold it and run handlers
  in the service it was republished for and in no other, and SHALL carry forward the attempt
  count it had reached, so that a republished message can neither re-run work that already
  succeeded nor restart its retry budget.

### Removal, configuration and documentation

- **R7.14** — THE SYSTEM SHALL remove PostgreSQL from the application, its dependencies and
  its environment definition entirely, SHALL retain the in-memory backend unchanged as R3.20
  requires, and SHALL read every setting this feature introduces from environment variables
  with no credential among them.
- **R7.15** — THE SYSTEM SHALL provide a document covering why the changelog is per consumer
  group, what co-partitioned state means on disk, what a rebuild costs and how to measure it,
  the risk a long rebuild poses to group membership, and how the topics built here correspond
  to the internal topics 009's ksqlDB creates. The known-gaps tables in `README.md` that name
  007 SHALL be updated to match, including the correction that a deliberate replay from
  earliest is not closed here.

## Notes

**Why one changelog per group rather than one topic keyed by group and order.** Putting the
group in the key would make compaction correct but would put the group in the *partition
hash*, so an order's fold and its events would land on different partition numbers and the
rebuild in R7.7 could no longer read one partition. Co-partitioning is the property 006 D8
established and 009's joins require; the key has to stay `order_id`, which forces the topic
to carry the group instead.

**Why R7.8 exists.** The obvious way to read a changelog is to subscribe to it. That would
join the rebuilding member to a second group, commit offsets against it, and trigger
rebalances of its own — during a rebalance. Manual assignment with no group id is not an
optimisation here; it is the only shape that terminates.

**Why R7.7 blocks.** A store rebuilt after messages have already been processed is a store
that missed them. Rebuilding inside the assignment, before the loop resumes, is what makes
the fold correct — and it is what Kafka Streams does. R7.15 requires the document to name the
cost: a rebuild long enough to exceed `max.poll.interval.ms` gets the member evicted mid-restore,
which is the reason R7.9's checkpoint mode exists.

**Why R7.12 is phrased as a prohibition.** The constraint is not "the retry worker should be
restructured" — it is that an embedded store physically cannot be opened twice. Stating the
invariant rather than the refactor is what stops a later feature from reintroducing a second
writer with a different justification.

**What is deliberately *not* fixed.** `handled_count` still exceeds `last_sequence`. Both
writes are now Kafka operations, which is the setup for 008's transaction, but they are still
two operations and this rung does not join them.

**Criteria count.** 15, at the top of the roughly 12–15 the size budget recorded as
[X11](../../DECISIONS.md) sets.
