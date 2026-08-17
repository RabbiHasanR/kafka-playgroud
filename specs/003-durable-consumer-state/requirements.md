# 003 — Durable Consumer State

**Status:** draft — awaiting approval
**Depends on:** [002-consumer-groups-rebalancing](../002-consumer-groups-rebalancing/requirements.md)

## Overview

Kafka remembers a consumer's **position**. Nothing yet remembers its **memory**.

The committed offset says "this group has read up to here". It says nothing about what the
consumer *learned* on the way. Each of the three services folds `(last_sequence, state)` per
order to detect ordering violations, and that fold lives in a dict inside the process. It is
destroyed two ways, both routine:

- **A restart.** The offset survives, the fold does not, so the next event for an in-flight
  order reports a sequence gap that never happened. 001 recorded this as T35.
- **A rebalance.** A revoked partition's folds are deliberately discarded, so the member that
  inherits partition 2 has never seen those orders. 002 recorded this as T17 and *required*
  it, in R2.14 and R2.15.

Both were built on purpose and left unfixed on purpose — 001 D9 and 002 D7 each say so in
writing. **This feature is where they are fixed**, and it is the first one that could not
have been understood without the two failures preceding it.

Three mechanisms carry the lesson.

**Position and memory are different things, stored in different places.** The offset stays in
Kafka. The fold moves to Postgres. Once they are separate, it becomes possible to ask what
happens when only one of the two writes lands — which is the whole of the rest of this
feature.

**State keyed by the entity outlives ownership of the partition.** 002 keyed folds by
partition first, because ownership was the thing on display. Here the durable record is keyed
by `(group, order)`, so a partition moving between members does not strand it. The fold stops
belonging to whoever holds the partition and starts belonging to the order.

**The dual-write problem, in plain sight.** The offset commit lands in Kafka and the state
write lands in Postgres, and no single operation covers both. A crash in the gap redelivers an
event that was already applied. This feature does not close that gap — it cannot. It makes the
gap reachable on demand, absorbs it in the state with an idempotent write, and then shows the
part that idempotent writes do *not* absorb: the duplicate side effect. That residue is the
motivation for 008, per [X4](../../DECISIONS.md).

**The durable store is a materialization, not the record.** `order-lifecycle` remains the
source of truth. Every row this feature writes is derived from it and could be rebuilt by
replaying from the earliest offset. Postgres is here so that a rebalance costs nothing rather
than costing a full replay — not because the topic is insufficient.

## Out of scope

Each is a later feature, or deliberately deferred; none may be built here.

- **Persisting the producer's `OrderStore`.** Its amnesia is a *different* problem with a
  *different* fix — see the note below. Deferred by 001 with no spec claiming it.
- **Storing offsets in Postgres alongside the state** so that one transaction covers both.
  It is the natural next thought and it is excluded on purpose: the unclosed gap is what
  gives 008 its payoff. Named in the documentation as the thing not being reached for.
- Local state stores co-partitioned with the input, RocksDB, changelog topics (007, per X5)
- Transactions and exactly-once semantics (008)
- Deduplication by `event_id`. This feature achieves idempotency through the sequence
  already on every event; a dedup table is a second mechanism for an overlapping job and
  belongs where duplicate *side effects* are actually eliminated (008).
- Throughput, lag, or latency measurement of the shared database. The bottleneck X4 names is
  recorded as a qualitative observation; measuring it needs the load generator R2.33 excluded
- Growing the topic's partition count, and the key-rehashing it causes
- Multi-broker clusters, replication, `acks` tuning, failover (004)
- Retries, dead-letter topics, poison-message handling (005)
- Log compaction and tombstones (006)
- Any change to the producer, the event contract, the topic, or its partition count
- Any change to 002's protocol switch, assignor selection, eviction lever, or static membership
- Authentication, TLS, ACLs — the environment stays PLAINTEXT per 000. The database is
  reachable only on the compose network and from the host, with credentials from the
  environment

## User stories

**US-1** — As a developer, I want a restarted consumer to remember the orders it had already
seen, so that the false sequence gap 001 recorded at T35 stops appearing.

**US-2** — As a developer, I want a member that inherits a partition to already know that
partition's orders, so that I can re-run 002's T17 and watch the amnesia be gone rather than
be told it was fixed.

**US-3** — As a developer, I want to see that the durable record is keyed by order and not by
partition, so that I understand *why* moving a partition no longer loses anything.

**US-4** — As a developer, I want to crash a consumer in the window between writing its state
and committing its offset, so that the dual-write problem is something I produce on demand
rather than a paragraph I am asked to believe.

**US-5** — As a developer, I want the redelivered event to leave the state unchanged while a
counter of handled deliveries goes up, so that I can see exactly which half of the problem an
idempotent write solves and which half it does not.

**US-6** — As a developer, I want to run the same crash with the two writes in the opposite
order, so that I learn why state-before-offset is the correct order and what the other order
costs.

**US-7** — As a developer, I want to switch back to in-process state with one environment
variable, so that 001's and 002's recorded experiments stay reproducible and I can put the
before and after side by side.

**US-8** — As a developer, I want a document covering this feature end to end, so that I can
re-read it later and recognise which behaviours are still accepted limitations.

## Acceptance criteria

### Durable fold state

- **R3.1** — THE SYSTEM SHALL persist each consumer service's per-order fold — at minimum the
  last sequence applied and the resulting lifecycle state — outside the consumer process.
- **R3.2** — THE SYSTEM SHALL scope every persisted fold by consumer group, so that the three
  services' memories remain independent of one another exactly as R1.29 requires of their
  offsets.
- **R3.3** — THE SYSTEM SHALL key every persisted fold by order rather than by partition, so
  that it is reachable by whichever member currently holds the partition the order maps to.
- **R3.4** — WHEN a consumer handles an event for an order it holds no cached fold for THE
  SYSTEM SHALL read that order's persisted fold before applying the event.
- **R3.5** — WHEN a consumer applies an event THE SYSTEM SHALL persist the resulting fold
  before committing that event's offset.
- **R3.6** — THE SYSTEM SHALL continue to detect and report genuine sequence-gap and
  illegal-transition violations per R1.38, R1.39, and R1.41.

### Restart and rebalance

- **R3.7** — WHEN a consumer restarts and resumes from its group's last committed offset THE
  SYSTEM SHALL report no sequence-gap violation for an order whose events arrived in order,
  reversing the behaviour 001 recorded at T35.
- **R3.8** — WHEN a member is assigned a partition carrying orders it has never handled THE
  SYSTEM SHALL report no sequence-gap violation for those orders, reversing the behaviour
  R2.15 requires.
- **R3.9** — WHEN partitions are revoked from or lost by a member THE SYSTEM SHALL discard
  only that member's in-process cache for those partitions, and SHALL NOT delete their
  persisted folds.
- **R3.10** — WHILE a member holds a partition THE SYSTEM SHALL be free to serve that
  partition's folds from an in-process cache, and SHALL NOT serve a cached fold for a
  partition it no longer holds.

### Idempotency under at-least-once delivery

- **R3.11** — IF an event's sequence is less than or equal to the persisted last sequence for
  its order THEN THE SYSTEM SHALL leave that order's persisted fold unchanged.
- **R3.12** — WHEN a group's offsets are reset to the earliest and the whole topic is
  re-consumed against populated state THE SYSTEM SHALL leave every persisted fold at the value
  it held before the replay.
- **R3.13** — THE SYSTEM SHALL persist, per order, a count of the deliveries it has handled,
  incremented on every delivery including one whose fold write was a no-op under R3.11.
- **R3.14** — WHEN an already-applied event is redelivered THE SYSTEM SHALL invoke its handler
  again, so that the duplicate side effect is produced rather than hidden.

### The dual-write gap

- **R3.15** — THE SYSTEM SHALL read from the environment a crash point that terminates the
  consumer process in the window between the state write and the offset commit, defaulting to
  no crash.
- **R3.16** — THE SYSTEM SHALL read from the environment which of the state write and the
  offset commit is performed first, defaulting to the state write.
- **R3.17** — WHEN a consumer is terminated between its state write and its offset commit THE
  SYSTEM SHALL, on restart, redeliver that event, leave its persisted fold unchanged per
  R3.11, and increase its handled-delivery count per R3.13.
- **R3.18** — WHILE the offset-first order is selected, IF a consumer is terminated between
  its offset commit and its state write THEN THE SYSTEM SHALL, on restart, resume past that
  event permanently, leaving a persisted fold that is missing an event Kafka considers
  consumed.

### State backend selection

- **R3.19** — THE SYSTEM SHALL select the state backend from the environment, supporting at
  minimum an in-process backend and a durable backend.
- **R3.20** — THE SYSTEM SHALL retain the in-process backend with exactly the behaviour 002
  recorded, so that 001's and 002's experiments remain reproducible without the durable store
  running.
- **R3.21** — IF the durable backend is selected and its store cannot be reached at startup
  THEN THE SYSTEM SHALL fail with an error naming the backend and the address it tried, and
  SHALL NOT join the group.
- **R3.22** — IF the durable store becomes unreachable while consuming THEN THE SYSTEM SHALL
  log it with a stable marker and stop consuming, and SHALL NOT continue against cached state
  alone.
- **R3.23** — WHEN a member joins THE SYSTEM SHALL log which state backend is in effect,
  alongside the protocol and assignor R2.22 already puts in the startup banner.

### Configuration and infrastructure

- **R3.24** — THE SYSTEM SHALL read every setting this feature introduces from environment
  variables, and SHALL NOT embed a connection string or credentials in source or in compose
  defaults.
- **R3.25** — THE SYSTEM SHALL provide idempotent schema creation for the durable store,
  runnable both from the host and from inside the compose network, from a single schema
  definition used by both.
- **R3.26** — THE SYSTEM SHALL run the durable backend both from the host and from inside the
  compose network, changing only environment variables, per R1.44 and R2.35.
- **R3.27** — THE SYSTEM SHALL leave every default such that a consumer started with none of
  this feature's settings behaves as 002 recorded.

### Documentation

- **R3.28** — THE SYSTEM SHALL provide a document covering the difference between a committed
  offset and a folded memory, why the consumer's fold is made durable while the producer's
  order store is not, the dual-write problem, and a runnable walkthrough of the restart and
  rebalance experiments.
- **R3.29** — THE SYSTEM SHALL state, in that document, which observed behaviours remain
  accepted limitations of this feature and which later spec closes each of them.
- **R3.30** — THE SYSTEM SHALL update the known-gaps table in `README.md` to record which rows
  naming 003 this feature closed and which it did not.

## Notes

**Why the producer's `OrderStore` is not persisted here.** The two stores are different kinds
of state and the asymmetry is the point, not an oversight:

| | Producer `OrderStore` | Consumer fold |
|---|---|---|
| Role | **Source of truth** — invents `order_id`, allocates `sequence`, guards transitions | **Derived** — a projection of events already on the topic |
| If lost | Gone; nothing else allocated those sequences | Rebuildable by replaying from the earliest offset |
| Why durable at 003 | — | Not for truth. For *continuity*, so a rebalance is not a full replay |

Making the producer durable introduces the **transactional outbox** problem: the order row and
the publish must land atomically, and cannot. That is a mirror image of this feature's
dual-write with a different fix — an outbox table and a relay, not an idempotent upsert — and
two unsolved dual-writes competing for one document is what would make this two features. A
third option exists and is also not for here: the producer could rebuild its store at startup
by replaying `order-lifecycle`, since every field it needs is in the `ORDER_CREATED` payload.
That is stream–table duality, which is 009's ground.

**R3.13 and R3.14 exist to keep this feature honest.** Without them, "durable state fixes the
amnesia" reads as though it fixed everything. It fixes the state and it does not fix the side
effect: after a crash in the dual-write window, the notification is sent twice and the stored
fold is right both times. A count makes that a number instead of a caveat, and the number is
what 008 will drive to zero.

**R3.15 is this feature's `force` flag.** 001 needed `force: true` because a service that
guards its own transitions never emits an illegal event to detect. 002 needed a handler delay
because a healthy member is never evicted by an honest workload. The same structural problem
appears here: the window between the state write and the offset commit is microseconds wide
and cannot be hit by hand. Without a deliberate crash point, R3.17 is unreachable and the
dual-write problem stays a claim.

**R3.18 is the criterion that justifies R3.5's ordering.** State-before-offset is stated as a
requirement rather than left to implementation because the other order is not merely slower —
it silently loses data, permanently, with no violation logged anywhere. Running it once is
worth more than asserting it.

**R3.20 is a regression guard on recorded results.** 001's and 002's `tasks.md` files contain
observations that only reproduce with in-process state. If this feature removed that backend,
those recorded results would become unverifiable, and a spec whose evidence cannot be re-run
is a spec that has to be trusted.

**On the shared database.** All three notification instances write to one Postgres. That is a
bottleneck and a coupling that co-partitioned local state removes at 007 — X4 chose Postgres
partly to make it visible. It is recorded as an observation in the documentation and
deliberately not measured; measuring it needs the load generator R2.33 excluded from this
ladder.

**Criteria count.** 30 criteria across eight groups. Roughly half of them (R3.11–R3.18) are
about what durable state does *not* fix, which is the proportion this feature deserves.
