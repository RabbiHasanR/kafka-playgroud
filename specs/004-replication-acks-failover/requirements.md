# 004 — Replication, `acks`, and Failover

**Status:** draft — awaiting approval
**Depends on:** [003-durable-consumer-state](../003-durable-consumer-state/requirements.md)

## Overview

Every spec so far has had **one copy of every message**. One broker, replication factor 1.
Stopping that broker has never been an experiment because there was nothing to observe —
everything stops.

001, 002 and 003 were all consumer-side lessons: how messages divide, what survives a
rebalance, what survives a restart. This one is the first that looks at the **broker side**,
and at the one producer setting that has been hardcoded since 001 — `acks: "all"` — without
anything in the environment able to show what the alternatives cost.

Three mechanisms carry the lesson.

**A partition is replicated; a replica set is not the same as an in-sync replica set.** Three
copies of partition 0 exist, one of them leads, and the ISR is the subset currently caught up.
`kafka-topics --describe` prints all three facts on one line, and stopping a broker changes
two of them.

**`acks` is the producer's half of a durability contract, and the topic holds the other half.**
`acks=all` means all *in-sync* replicas — so its strength depends on a topic setting the
producer cannot see. This feature makes `acks` a lever and shows the range: `0` returns before
the broker has the message at all, `all` waits for the ISR.

**Failover is automatic, and that is the point.** No operator action, no client
reconfiguration: the controller elects a new leader from the ISR and the producer carries on.
What it costs is visible only if the client was told about more than one broker to begin with.

**The cluster replaces the single node rather than sitting beside it.** Spec 000's environment
grows to three brokers permanently; `R0.4` and `R0.17` there are amended accordingly. The
RF 1 vs RF 3 contrast that motivates this feature is a **topic** property, so it stays fully
reachable on the three-broker cluster and does not need a second environment to demonstrate.

## Out of scope

Each is a later feature or deliberately deferred; none may be built here.

- **`min.insync.replicas`.** Named in the documentation as the missing half of the `acks=all`
  contract, and deliberately not set — writing it here means also building the
  `NOT_ENOUGH_REPLICAS` refusal path, which is a second failure mode competing with failover
  for one document.
- **Unclean leader election** and a committed-data-loss demonstration.
- **Rack awareness, replica placement, reassignment** (`kafka-reassign-partitions`).
- Throughput or latency comparison between `acks` values — that needs the load generator
  R2.33 excluded from this ladder. The difference is stated qualitatively.
- Retries, dead-letter topics, poison-message handling (005)
- Log compaction and tombstones (006)
- Local state stores and changelog topics (007); transactions and exactly-once (008)
- Any change to the event contract, the consumer fold, the state store, or 002's protocol,
  assignor, and membership levers
- Growing the topic's partition count
- Authentication, TLS, ACLs — the environment stays PLAINTEXT per 000

## User stories

**US-1** — As a developer, I want three brokers instead of one, so that "the broker died" stops
being the end of the experiment and starts being the beginning of one.

**US-2** — As a developer, I want to see a partition's leader, replicas and ISR change as I stop
and start brokers, so that replication is something I watch rather than something I configure.

**US-3** — As a developer, I want to place orders while a broker is down and have them succeed,
so that automatic failover is a result rather than a claim.

**US-4** — As a developer, I want `acks` to be one environment variable, so that I can run the
same orders at `0` and at `all` and see which one notices a broker dying.

**US-5** — As a developer, I want to create a topic at replication factor 1 on a three-broker
cluster and lose one of its partitions, so that I understand replication as a property of the
topic and not of the cluster.

**US-6** — As a developer, I want a document covering this feature end to end, so that I can
re-read it later and recognise which durability gaps are still open and which spec closes them.

## Acceptance criteria

### The cluster

- **R4.1** — THE SYSTEM SHALL run three Kafka nodes, each combining the broker and controller
  roles in one KRaft cluster, each reachable from the host on its own port and from the compose
  network on the internal listener, per R0.5 and R0.6.
- **R4.2** — THE SYSTEM SHALL replicate every internal topic across all three nodes, per the
  amended R0.17.
- **R4.3** — WHILE any one node is stopped THE SYSTEM SHALL continue to accept produce and
  consume requests for every topic replicated across all three.

### Replication of the feature topic

- **R4.4** — THE SYSTEM SHALL read the feature topic's replication factor from the environment,
  defaulting to 3.
- **R4.5** — WHEN a topic is created or described THE SYSTEM SHALL report, per partition, its
  leader, its full replica set, and its in-sync replica set.
- **R4.6** — IF a partition has a replication factor of 1 and the node holding its only replica
  is stopped THEN THE SYSTEM SHALL report that partition as offline while the cluster's other
  partitions stay available.

### The `acks` contract

- **R4.7** — THE SYSTEM SHALL read the producer's `acks` setting from the environment,
  supporting exactly `0`, `1`, and `all`, defaulting to `all`, and failing at startup with the
  offending value named if given anything else.
- **R4.8** — WHEN the order service starts THE SYSTEM SHALL log the `acks` value in effect,
  alongside the settings R3.23 already puts in the startup banner.
- **R4.9** — WHEN a delivery report carries a broker-side error THE SYSTEM SHALL report the
  error and the topic partition it applied to, rather than an unattributed message.

### Failover

- **R4.10** — WHEN the node leading a partition is stopped THE SYSTEM SHALL elect a new leader
  from that partition's in-sync replicas, and the order service SHALL resume publishing to it
  with no operator action and no restart.
- **R4.11** — THE SYSTEM SHALL give every producer and consumer more than one bootstrap
  address, so that a client can start while any one node is stopped.
- **R4.12** — WHEN a stopped node is restarted THE SYSTEM SHALL return it to the in-sync
  replica set of every partition it holds.

### Configuration and documentation

- **R4.13** — THE SYSTEM SHALL read every setting this feature introduces from environment
  variables, and SHALL leave every default such that a producer or consumer started with none
  of them behaves as 003 recorded, the broker count aside.
- **R4.14** — THE SYSTEM SHALL provide a document covering replication factor versus the
  in-sync replica set, what each `acks` value buys and costs, a runnable failover walkthrough,
  and the one-time `docker compose down -v` the cluster upgrade requires; and SHALL state in it
  that `min.insync.replicas` and unclean leader election remain open, naming where each is
  closed. The known-gaps tables in `README.md` that name 004 SHALL be updated to match.

## Notes

**Why the cluster is permanent rather than optional.** A compose profile holding `kafka-2` and
`kafka-3` back would not work: KRaft writes the controller quorum voters into the metadata log
at format time, so a three-voter quorum with two nodes unstarted elects no controller and the
cluster never comes up. It is three nodes or a re-format, which makes "optional" a fiction. The
one-time cost is a `docker compose down -v` on upgrade — already an established step, since
R0.9 defines it and `create_topics.sh` already has to follow it.

**Why 001–003 stay reproducible.** Nothing recorded in those specs depends on broker count.
Consumer group formation, partition assignment, rebalancing, and the durable fold behave
identically against three brokers. This is what makes R4.13 a cheap guarantee rather than a
constraint on the design.

**R4.11 is load-bearing, not tidiness.** `KAFKA_BOOTSTRAP_SERVERS` is currently one address in
five compose services. Left that way, stopping that one broker prevents every client from
*starting* — and the failure would read as a bug in failover rather than a property of
bootstrap. The lesson only lands if a client can join through whichever node is up.

**R4.6 is why this feature needs no second environment.** Replication factor belongs to the
topic. A scratch topic at RF 1 on a three-broker cluster loses a partition when one node stops,
while `order-lifecycle` at RF 3 does not — the same contrast a single-broker environment would
have shown, with both halves visible at once instead of one `docker compose` invocation apart.

**What `acks=all` does not promise.** With no `min.insync.replicas` set, an ISR that has shrunk
to one member still satisfies `acks=all` — the write is acknowledged by every in-sync replica,
and there is one. That is the gap R4.14 is required to name, and it is the reason
`min.insync.replicas` is listed as out of scope rather than forgotten.

**Criteria count.** 14 across five groups, per the size budget recorded as [X11](../../DECISIONS.md). 003's 30 was
the high-water mark and not the target.
