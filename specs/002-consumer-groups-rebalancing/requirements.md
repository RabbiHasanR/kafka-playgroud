# 002 — Consumer Groups, Rebalancing, and Partition Assignment

**Status:** draft — awaiting approval
**Depends on:** [001-prepaid-order-service](../001-prepaid-order-service/requirements.md)

## Overview

The notification service runs as **three instances sharing one consumer group**. The
three partitions of `order-lifecycle` are divided between them, so each event is handled
by exactly one instance instead of all three. Inventory and analytics stay as they are —
one instance each, in their own groups — so fan-out and scale-out are visible side by
side on one topic.

**This is the inverse of 001, and that is the point.** 001 put three group ids on one
topic and every service saw every message. Here one group id has three members and the
messages divide. Same broker, same topic, same events; the only difference is whether the
consumers share a `group.id`.

Four mechanisms carry the lesson.

**Partitions are the unit of parallelism.** Three partitions cap the group at three
useful members. A fourth joins successfully and holds nothing. Scaling consumers past the
partition count buys nothing, and that ceiling is a property of the topic, not of the
machines.

**Ordering survives scale-out.** An `order_id` key pins an order to a partition, and a
partition belongs to one member at a time, so one order's events are still handled in
order by a single instance. Ordering and parallelism are not a trade-off here — the key
is what buys both.

**A rebalance is a reassignment, and it costs something.** Members joining or leaving
force the group to recompute who reads what. What that costs depends on *who computes it*
and *how much is revoked* — which is why this feature runs the same experiment under both
the classic client-side protocol and the KIP-848 broker-side protocol available in Kafka
4.x, and under both eager and cooperative assignment.

**Partition ownership is temporary, and state that assumes otherwise breaks.** Each
instance folds order state only for the partitions it holds. A rebalance moves a partition
to an instance that has never seen those orders, and its fold is empty. The resulting
sequence-gap violations are the same amnesia 001 recorded on restart, now caused by a
routine scaling event — and they are the direct motivation for 003.

## Out of scope

Each is a later feature, or deliberately deferred; none may be built here.

- Fixing the fold amnesia a rebalance causes — durable and co-partitioned state is 003
  and 007, and this feature exists partly to motivate them
- Consumer-side idempotency or deduplication (003, 008)
- Growing a topic's partition count and the key-rehashing it causes — a separate lesson,
  excluded by explicit request; it may return later as a numbered experiment
- Rate-controlled bulk generation, throughput measurement, and lag benchmarking — the
  order-placing command in R2.30 is a typing aid and must not grow into a load generator
- Multi-broker clusters, replication, `acks` tuning, failover (004) — partition count,
  not broker count, is what this feature varies
- Retries, dead-letter topics, poison-message handling (005)
- Log compaction and tombstones (006)
- Transactions and exactly-once semantics (008)
- Any change to the producer, the event contract, the topic, or its partition count
- Authentication, TLS, ACLs — the environment stays PLAINTEXT per 000

## User stories

**US-1** — As a developer, I want to add a second and third notification instance to one
group and watch the partitions divide between them, so that I can see scale-out as the
inverse of 001's fan-out rather than take it on trust.

**US-2** — As a developer, I want to add a fourth instance to a three-partition topic and
see it sit idle, so that I learn the partition count is the ceiling on useful consumers.

**US-3** — As a developer, I want to confirm that one order's events are still handled in
order by one instance while three instances are running, so that I can see the message key
buying ordering and parallelism at once.

**US-4** — As a developer, I want each instance to say which partitions it was given and
which were taken away, so that a rebalance is something I read in the logs rather than
infer from missing output.

**US-5** — As a developer, I want to run the same join-and-leave under the classic
protocol and under the KIP-848 protocol, so that I can see who computes the assignment and
what stops while it happens.

**US-6** — As a developer, I want to make a handler slow on purpose and watch a live,
healthy instance get thrown out of its group, so that I recognise the most common consumer
failure in production when I meet it for real.

**US-7** — As a developer, I want to restart an instance with and without a static member
identity and count the rebalances each way, so that I can see what a rolling deploy costs
and what one config line saves.

**US-8** — As a developer, I want to place many orders with one command, so that a
partition split is visible without typing dozens of `curl` calls.

**US-9** — As a developer, I want a document covering this feature end to end, so that I
can re-read it later and recognise which behaviours were accepted limitations.

## Acceptance criteria

### Scale-out within one group

- **R2.1** — THE SYSTEM SHALL run the notification service as three instances sharing one
  consumer group identity, while the inventory and analytics services each remain a single
  instance in their own group.
- **R2.2** — WHILE more than one member of a group is active THE SYSTEM SHALL assign each
  partition of the topic to exactly one member of that group.
- **R2.3** — WHEN an event is published THE SYSTEM SHALL have exactly one member of the
  notification group handle it, however many members are active.
- **R2.4** — WHILE a group has more active members than the topic has partitions THE
  SYSTEM SHALL admit the surplus members to the group holding no partitions, rather than
  rejecting them or failing them.
- **R2.5** — WHILE the notification group has multiple members THE SYSTEM SHALL have every
  event for one `order_id` handled by the same member, for as long as that member holds the
  partition the key maps to.
- **R2.6** — WHILE the notification group is scaled out THE SYSTEM SHALL continue
  delivering every event to the inventory and analytics services independently, per R1.29.

### Membership observability

- **R2.7** — THE SYSTEM SHALL give each consumer process an instance identity distinct
  from both its service name and its group identity.
- **R2.8** — WHEN an event is consumed THE SYSTEM SHALL log the instance identity
  alongside the fields R1.42 already requires, so that three interleaved log streams from
  one group can be told apart by filtering alone.
- **R2.9** — WHEN partitions are assigned to a member THE SYSTEM SHALL log the assigned
  partition list at a severity distinguishable from normal processing, with a stable
  marker.
- **R2.10** — WHEN partitions are revoked from a member THE SYSTEM SHALL log the revoked
  partition list with a stable marker distinguishable from the R2.9 marker.
- **R2.11** — THE SYSTEM SHALL make the current partition-to-member split reportable from
  the broker alone, without reading application logs.

### Rebalance behaviour

- **R2.12** — WHEN a member joins a group THE SYSTEM SHALL redistribute that group's
  partitions across the resulting membership.
- **R2.13** — WHEN a member leaves a group THE SYSTEM SHALL reassign that member's
  partitions to the remaining members.
- **R2.14** — WHEN partitions are revoked from a member THE SYSTEM SHALL discard that
  member's folded order state for exactly those partitions, and SHALL retain it for the
  partitions the member keeps.
- **R2.15** — WHEN a member is assigned a partition for which it holds no folded state THE
  SYSTEM SHALL record the resulting sequence-gap violations per R1.38 rather than
  suppressing them.
- **R2.16** — WHEN a rebalance completes THE SYSTEM SHALL resume each partition from that
  group's last committed offset, per R1.31, regardless of which member now holds it.

### Protocol and assignor selection

- **R2.17** — THE SYSTEM SHALL select the consumer group protocol — the classic
  client-side protocol or the KIP-848 broker-side protocol — from the environment.
- **R2.18** — THE SYSTEM SHALL default to the classic protocol, so that a consumer started
  with no protocol setting behaves exactly as it did in 001.
- **R2.19** — WHERE the classic protocol is selected THE SYSTEM SHALL select the partition
  assignment strategy from the environment, supporting at least `range`, `roundrobin`, and
  `cooperative-sticky`.
- **R2.20** — WHERE the KIP-848 protocol is selected THE SYSTEM SHALL select the
  server-side assignor from the environment, supporting at least `uniform` and `range`.
- **R2.21** — IF a setting is supplied that the selected protocol does not accept THEN THE
  SYSTEM SHALL fail at startup with an error naming both the setting and the selected
  protocol, and SHALL NOT join the group.
- **R2.22** — WHEN a member joins THE SYSTEM SHALL log which protocol and which assignor
  are in effect, so that a run's configuration is recoverable from its own output.

### Slow consumers and eviction

- **R2.23** — THE SYSTEM SHALL read a per-event handler delay from the environment,
  defaulting to no delay.
- **R2.24** — THE SYSTEM SHALL read the maximum interval permitted between polls from the
  environment, so that eviction can be observed in seconds rather than minutes.
- **R2.25** — WHEN a member does not poll within the configured maximum poll interval THE
  SYSTEM SHALL have it removed from the group and its partitions reassigned, while its
  process is still running.
- **R2.26** — IF an offset commit fails because the member no longer owns the partition
  THEN THE SYSTEM SHALL log it with a stable marker distinct from the R1.41 violation
  marker, and SHALL continue consuming.
- **R2.27** — WHEN an evicted member resumes polling THE SYSTEM SHALL rejoin it to the
  group rather than terminating the process.

### Static membership

- **R2.28** — THE SYSTEM SHALL accept an optional static member identity per instance from
  the environment, defaulting to unset.
- **R2.29** — WHILE no static member identity is set THE SYSTEM SHALL treat a restarted
  instance as a new member, redistributing the group's partitions on its departure and
  again on its return.
- **R2.30** — WHILE a static member identity is set and the instance restarts within the
  group's session timeout THE SYSTEM SHALL return that member to its previous partition
  assignment without redistributing the group's partitions.

### Placing volume

- **R2.31** — THE SYSTEM SHALL provide a command that places a caller-specified number of
  orders against a running order service and reports the `order_id`, partition, and offset
  of each.
- **R2.32** — WHERE requested, that command SHALL also advance each order it created
  through `PACKED → SHIPPED → DELIVERED`.
- **R2.33** — THE SYSTEM SHALL keep that command free of rate control, concurrency, and
  throughput reporting, so that it stays a typing aid and does not become the load
  generator this feature excludes.

### Configuration

- **R2.34** — THE SYSTEM SHALL read every setting this feature introduces from environment
  variables, with defaults that leave 001's observed behaviour unchanged.
- **R2.35** — THE SYSTEM SHALL run the scaled-out group both from the host against
  `localhost:9092` and from inside the compose network against `kafka:19092`, changing only
  environment variables, per R1.44.
- **R2.36** — THE SYSTEM SHALL make the second and third notification instances startable
  independently of the first, so that a rebalance can be triggered while the group is
  being watched.

### Documentation

- **R2.37** — THE SYSTEM SHALL provide a document covering scale-out against 001's
  fan-out, what triggers a rebalance, how the two protocols differ in who computes the
  assignment, and a runnable walkthrough of growing and shrinking the group.
- **R2.38** — THE SYSTEM SHALL correct any statement in the existing concurrency reference
  that this feature's protocols make false.
- **R2.39** — THE SYSTEM SHALL state, in that document, which observed behaviours are
  accepted limitations of this feature and which later spec closes each of them.

## Notes

**Why notification is the service that scales.** Duplicate work is *visible* there — three
instances in three groups would mean a customer gets three messages, which is exactly the
mistake scale-out prevents. Inventory and analytics staying single-instance is deliberate:
they are the control group, behaving identically to 001 while the experiment runs beside
them, so any change observed in notification cannot be blamed on the broker or the topic.

**Why one broker is still enough.** Partitions, not brokers, are the unit of consumer
parallelism, and one broker hosts all three partitions of `order-lifecycle` perfectly well.
What a single broker cannot demonstrate is replication and failover, and neither is in
scope here. Nothing in this feature is limited by the broker count.

**Why both protocols.** The classic protocol elects one *client* as group leader and has it
compute and upload the assignment for everyone; KIP-848 moves that computation to the
broker, which pushes each member its own share. Kafka 4.x supports both, this repository's
broker already enables both, and the installed client speaks both. Running the identical
experiment twice, one environment variable apart, is what turns "the new protocol is
better" into an observation. R2.18 keeps `classic` the default so 001's recorded results
stay reproducible.

**The rebalance amnesia is the point, not a defect.** R2.14 and R2.15 together guarantee
that moving a partition between members produces sequence-gap violations. 001 recorded the
same violation after a restart (T35) and accepted it under X3; here it happens during
routine scaling, with no crash involved. Do not "fix" it in this feature — the fix is
durable state in 003 and co-partitioned state in 007, and both read as ceremony if the
failure was never felt.

**R2.33 exists to keep a line 001 drew.** Bulk generation was deferred from 001 by explicit
request, and this feature needs volume only so that a three-way partition split is legible.
The moment the order-placing command grows a rate, a concurrency setting, or a
messages-per-second number, it has become the load generator this spec excluded — and the
lag and throughput experiments that need one belong to a feature that asks for them.

**Partition growth is excluded on purpose.** Growing `order-lifecycle` from 3 partitions to
6 would rehash keys and break an existing order's ordering permanently. That is a real and
worthwhile lesson, it is warned about in `README.md`, and it is about the *topic* rather
than about consumer groups. It was excluded from this feature by explicit request and may
return later as its own numbered experiment.

**Criteria count.** The planning estimate was roughly 24 criteria; this file declares 39.
The growth is in splitting compound statements into separately testable ones — the scope
is unchanged from what was agreed.
