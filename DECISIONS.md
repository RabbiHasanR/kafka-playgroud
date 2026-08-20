# Cross-cutting decisions

Decisions that span more than one feature, or that a later feature is expected to
supersede.

**This is not a duplicate of `design.md`.** Per-feature decisions and their rejected
alternatives belong in `specs/<feature>/design.md`. Two kinds of decision cannot live
there:

1. **Decisions spanning features** — the Kafka client choice binds specs 001–008; no
   single feature's `design.md` owns it.
2. **History** — `CLAUDE.md` requires `design.md` to be amended to match reality, so
   it always shows only the current state and its superseded reasoning is erased.
   When 007 replaces the state store chosen in 003, only a record like this one shows
   that the replacement was planned rather than a reversal.

**Routing rule:** affects one feature only → `design.md`. Spans features, or will be
revisited later → here, cited from the relevant `design.md`.

Entries are append-only and numbered `X<n>`, distinct from the per-feature `D<n>`
numbering used inside each `design.md`. Revising a decision means adding a new entry
and marking the old one superseded — never editing it in place.

---

## X1 — Kafka client: `confluent-kafka`

**Date:** 2026-08-11 **Status:** accepted **Specs:** 001–008 *(renumbering: X7, X8)*

**Context.** Every feature from 001 onward needs a Python Kafka client, and the later
ones need `acks` tuning (004), idempotent production and transactions (008). Changing
client mid-ladder would mean rewriting the producer and consumer.

**Decision.** `confluent-kafka`, the librdkafka binding.

**Consequences.** It is a synchronous C-backed client, so using it inside async
FastAPI requires care — a background `poll()` thread and synchronous route handlers
(001 D5, D7). In exchange, `acks`, `enable.idempotence`, and the transactional API are
exposed directly and match the Java client's semantics, so 004 and 008 need no client
migration.

**Rejected.** `kafka-python` — pure Python and easier to read, but slower and behind
on broker features that 008 depends on. `aiokafka` — natural async fit, but layers
asyncio complexity on top of Kafka complexity while the goal is to learn Kafka.

---

## X2 — Wire format: JSON, Schema Registry deferred

**Date:** 2026-08-11 **Status:** accepted **Specs:** 001+

**Context.** The event payload format is fixed across every feature that reads or
writes the lifecycle topic.

**Decision.** JSON, with no schema enforcement.

**Consequences.** `kafka-console-consumer.sh` and Kafka UI both show readable
messages, so CLI inspection stays a first-class debugging path throughout the ladder.
The cost is no schema validation and larger messages. Producer and consumer share the
contract by importing one module (`events.py`) rather than by registry enforcement.

**Rejected.** Avro + Schema Registry — the production answer and where this should
eventually go, but it adds a container and a compile step before a single message has
been sent, and it makes every message opaque to the CLI tools that 000 exists to
teach.

---

## X3 — Consumer state at 001 is in memory, and is meant to be lost

**Date:** 2026-08-11 **Status:** amended by X8 **Specs:** 001, superseded in effect by 003

**Context.** A consumer's committed offset is a *position*; its folded per-key state
is a *memory*. These are routinely conflated, and the conflation is invisible until a
restart separates them.

**Decision.** Feature 001 holds folded order state in a plain in-process dict with no
persistence, mandated by requirement R1.27.

**Consequences.** Restarting the consumer produces false sequence violations and
wrong order totals. This is the intended result and must not be "fixed" inside 001 —
doing so removes the evidence that motivates 003. Recorded here because a future
reader will otherwise file it as a defect.

**Rejected.** Starting at 003's durable store — correct code, but the reader never
sees the failure that justifies the machinery, and the machinery then reads as
ceremony.

---

## X4 — Durable state at 003 uses Postgres, knowing 007 replaces it

**Date:** 2026-08-11 **Status:** accepted **Specs:** 003, superseded by X5 at 007

**Context.** Once 001's amnesia is felt, state has to go somewhere durable. The
production-grade answer is a local state store plus a compacted changelog topic
(X5) — so choosing Postgres at 003 is knowingly choosing the weaker option.

**Decision.** Spec 003 uses Postgres anyway.

**Consequences.** Postgres puts the **dual-write problem** in plain sight: the offset
commit and the state write are in two different systems, so no single operation can
make them atomic, and at-least-once duplicates cannot be eliminated — only absorbed
by idempotent upserts. That problem is the direct setup for 008, where state and
offset are both Kafka operations and one transaction covers both. Postgres also
demonstrates the shared-database bottleneck that co-partitioned state removes.

**Rejected.** Going straight to RocksDB + changelog at 003 — fewer steps, but the
payoff at 008 lands only if the dual-write pain was felt first.

---

## X5 — Derived state ends at RocksDB + a compacted changelog topic

**Date:** 2026-08-11 **Status:** accepted **Specs:** 007, supersedes X4

**Context.** The endpoint of the state story, reached after 006 establishes
compaction.

**Decision.** A local RocksDB store for reads, with every mutation also produced to a
compacted changelog topic keyed identically to the state.

**Consequences.** State becomes co-partitioned with input — the instance owning
partition 3 holds exactly partition 3's keys, with no shared database. Rebuild cost
after a restart or a rebalance is proportional to the *number of keys*, not the
number of events, because compaction retains only the latest value per key. This is
the mechanism Kafka Streams uses internally.

Kafka Streams itself is JVM-only, so this is hand-rolled in Python (`rocksdict` plus a
manually produced compacted topic); library maintenance status is worth re-checking on
arrival, as this corner of the ecosystem churns.

**Rejected.** Postgres permanently (see X4) — shared bottleneck, not co-partitioned.
Adopting a framework before hand-rolling it — the mechanism is the lesson.

---

## X6 — ksqlDB is the capstone at 009, deliberately not earlier

**Date:** 2026-08-11 **Status:** accepted **Specs:** 009

**Context.** ksqlDB runs on Kafka Streams and therefore on RocksDB plus compacted
changelog topics — precisely what 006 and 007 build by hand. It could technically be
introduced at any point after 002.

**Decision.** It goes last.

**Consequences.** Met before 007 it is an opaque black box that provisions topics and
state directories for reasons the reader cannot see. Met after, every internal topic
it creates is recognisable and can be matched to the hand-rolled equivalent. It also
covers ground no earlier rung reaches: stream–table duality, windowed aggregation,
and joins with their co-partitioning requirement.

Two caveats to carry into that spec: ksqlDB is under the **Confluent Community
License**, not Apache 2.0; and Confluent's strategic investment has moved toward
Flink, so it is best treated as a vehicle for the concepts rather than a bet on the
tool. Re-check ksqlDB versus Flink SQL on arrival.

**Rejected.** Introducing SQL-over-streams early as a quick win — it would answer the
questions before the reader has learned to ask them.

---

## X7 — The ladder renumbers; a realistic order service takes 002

**Date:** 2026-08-13 **Status:** superseded by X8 the same day **Specs:** all

> **Superseded.** The mechanics lab this entry positions the new feature *against* was
> removed hours later, and the prepaid order service took 001 instead. The numbering
> below is historical; X8 has the current ladder. The reasoning is kept because it
> records why the realistic service was wanted early, which is still why it is 001.

**Context.** 001 is a mechanics lab in an order-domain costume, and says so in its own
overview: *"The purpose is not the order domain."* Its producer accepts a
caller-supplied `event_type` for any order, one process emits all six lifecycle
events, and two flags exist purely to corrupt ordering. Every one of those is right
for what 001 teaches and wrong as a picture of how a service is built.

Reading 001 and asking "is this what a real order flow looks like?" gets the answer
*no* — and that gap is worth closing early, not at the end of the ladder. A reader who
never sees the realistic shape may carry 001's shape into production code.

**Decision.** Insert a new feature at **002** — a prepaid order service with
multi-service consumer fan-out — and shift every reserved mechanics spec down by one.

| Was | Now | Topic |
|---|---|---|
| — | 002 | Prepaid order service + consumer fan-out |
| 002 | 003 | Consumer groups, rebalancing, partition assignment |
| 003 | 004 | Durable consumer state |
| 004 | 005 | Replication, `acks`, failover |
| 005 | 006 | Retries, DLQ, poison messages |
| 006 | 007 | Compaction, tombstones |
| 007 | 008 | Local state stores, changelog topics |
| 008 | 009 | Transactions, exactly-once |
| 009 | 010 | Stream SQL / ksqlDB |

**Consequences.** No directories existed for the reserved numbers, so the shift was a
prose-only edit across `001/`, `README.md`, `DECISIONS.md`, and four source
docstrings. 001's behaviour is unchanged and its requirement IDs stay `R1.x`.

002 landing before 003 is deliberate rather than incidental. 002 fans one topic out to
**three different consumer groups**, where every service sees every message; 003 adds
**more consumers to one group**, where messages are divided between them. Meeting
fan-out first makes the second mechanism land as a contrast instead of a variation.

The cost is that 002 gestures at ground later rungs own — idempotency (004, 009) and
poison-message handling (006) are visible in a realistic service and are deliberately
left unbuilt there.

**On the append-only rule.** X1 and X3–X6 carry `*(renumbered by X7)*` on their
`Specs:` line and have their spec numbers updated in place. Those numbers are
pointers, not reasoning; every body paragraph is untouched, and no decision here was
reversed — only re-indexed. This entry is the record that the shift happened.

**Rejected.** Numbering the realistic service 010 and leaving the ladder alone —
cheaper, and it keeps this file literally append-only, but it puts the one spec that
answers "what does this look like in production?" behind eight rungs of mechanics.
Also rejected: building it as a capstone after 010, for the same reason.

---

## X8 — The mechanics lab is removed; the order service is 001

**Date:** 2026-08-13 **Status:** accepted **Specs:** all, supersedes X7, amends X3

**Context.** X7 added the prepaid order service at 002 and kept the original
`order-event-pipeline` at 001, on the reasoning that the two taught different things
and the lab's fault-injection levers had no counterpart in a realistic service.

That reasoning was sound and was overruled deliberately. Two features covering one
domain, on two topics, with two copies of the poll thread and the consume loop, is a
larger surface than a solo learning repository needs — and every hour spent keeping the
two specs consistent with each other is an hour not spent on the next rung.

**Decision.** Delete `src/order_pipeline/` and `specs/001-order-event-pipeline/`.
Rename `002-prepaid-order-service` to **001**, renumber its criteria `R2.x → R1.x`, and
shift the reserved ladder back up. The result is the pre-X7 numbering with the lab
replaced:

| Spec | Topic |
|---|---|
| 000 | Local Kafka environment |
| **001** | **Prepaid order service + consumer fan-out** |
| 002 | Consumer groups, rebalancing, partition assignment |
| 003 | Durable consumer state |
| 004 | Replication, `acks`, failover |
| 005 | Retries, DLQ, poison messages |
| 006 | Compaction, tombstones |
| 007 | Local state stores, changelog topics |
| 008 | Transactions, exactly-once |
| 009 | Stream SQL / ksqlDB |

X1, X4, X5 and X6 therefore return to the spec numbers they were written with, and the
`*(renumbered by X7)*` markers are gone from them.

**Consequences.** Four demonstrations left the repository with the lab and are not
reachable from 001: unkeyed publishing (why the message key matters at all), shuffled
publishing, rate-controlled bulk generation and the consumer-lag experiment, and the
running-total accumulator. If any of them is wanted later it must be added as a new
numbered experiment against `order-lifecycle`, not recovered from the old spec.

`R2.45` — "leave 001's producer, consumer, topic and event contract unmodified" — had
no meaning once there was nothing to leave alone, so it was dropped rather than
renumbered. Its verification task T36 went with it. `R1.45` and `R1.46` are the former
`R2.46` and `R2.47`.

**Amendment to X3.** That entry says folded consumer state is in memory *mandated by
requirement R1.27*. No such requirement exists in the new 001 — `R1.27` there is the
`GET /orders/{order_id}` endpoint. X3's substance still holds and still motivates 003:
each of the three services folds `(last_sequence, state)` in memory and loses it on
restart, so a restart mid-order produces a sequence-gap violation indistinguishable
from a real one. It is now an accepted limitation recorded in 001 D9, not a mandated
shortcoming. **Read every `R1.x` inside X3 as belonging to the deleted spec.**

**Rejected.** Keeping the lab and living with the duplication — my recommendation at
the time, overruled on the grounds above; the trade is real and this entry is the
record of which way it went. Also rejected: keeping `specs/001-order-event-pipeline/`
as documentation with its code deleted, which would leave three spec documents and 33
ticked tasks describing software that does not exist. Git history at `2ac63da` has all
of it if it is ever wanted back.

---

## X9 — Both rebalance protocols are exercised; `classic` stays the default

**Date:** 2026-08-16 **Status:** accepted **Specs:** 002+

**Context.** Kafka 4.x ships two consumer group protocols. In the **classic** protocol the
coordinator elects one *client* as group leader, and that client computes the partition
assignment and uploads it for every member. Under **KIP-848** (`group.protocol=consumer`,
GA in Kafka 4.0) the *broker* computes the assignment and pushes each member its own
share.

This is not a per-feature choice. Every consumer from 002 to 008 inherits whichever
default is set, and the two protocols accept partly disjoint client configuration —
`partition.assignment.strategy` versus `group.remote.assignor`, and client-side versus
broker-side session timeouts. A default chosen here propagates through every later spec's
consumer config.

The environment already supports both without modification: the broker runs
`KAFKA_GROUP_COORDINATOR_REBALANCE_PROTOCOLS: classic,consumer` (spec 000), and the
installed client is confluent-kafka/librdkafka 2.15.0.

**Decision.** Spec 002 exercises **both**, selected by `CONSUMER_GROUP_PROTOCOL`, and
**`classic` remains the default**.

**Consequences.** The same scale-out experiment runs twice, one environment variable
apart, which is what makes "the new protocol avoids the stop-the-world rebalance" an
observation rather than a claim. Keeping `classic` as the default preserves two things
that would otherwise be invalidated without either of them changing: 001's recorded
experiment results in its `tasks.md`, and the whole of
`docs/concurrency-and-confluent-kafka.md` §10, which describes JoinGroup/SyncGroup and
states in bold that a client computes the assignment. That section is amended by 002 to
scope its claim to the classic protocol rather than being deleted.

The cost is that this repository's default is the protocol Kafka is moving away from. That
is accepted for now on the grounds that the ladder's purpose is to *understand* the
mechanism, and the classic protocol is the one whose failure modes — stop-the-world
revocation, client-side assignors, eager versus cooperative — motivate the redesign.

**When this should flip.** When a later spec's consumers no longer need the classic
failure modes to make their point, or when librdkafka's default itself changes. Flipping
it means a new entry superseding this one, plus a re-run of 001's T27–T39 and 002's
experiments, since both files record protocol-dependent observations.

**Rejected.** Defaulting to `consumer` immediately — more modern and where the ecosystem
is going, but it silently invalidates 001's recorded results and one of this repository's
two reference documents. Also rejected: `classic` only, which would leave the environment's
own `classic,consumer` broker setting unexplained and teach a protocol that Kafka 4.x is
deprecating, with no sight of its replacement.

---

## X10 — The cluster becomes three brokers permanently; 000 is amended

**Date:** 2026-08-18 **Status:** accepted **Specs:** 000, 004+, amends 000 R0.4 and R0.17

**Context.** 004 needs more than one broker. The environment 000 built is a single KRaft
node with every internal topic pinned to replication factor 1, and 001–003 were written
against it. Two shapes were available: an opt-in overlay (`-f docker-compose.cluster.yml`)
leaving 000's environment untouched, or growing `docker-compose.yml` itself.

**Decision.** Three combined broker/controller nodes in `docker-compose.yml`, permanently.

**Consequences.** The contrast that teaches replication is **RF 1 versus RF 3 on a topic**,
not one broker versus three. Replication factor is a topic property, so a scratch topic at
RF 1 on the three-broker cluster loses a partition when its node stops while
`order-lifecycle` at RF 3 does not — both halves observable at once, which the overlay would
have put one `docker compose` invocation apart. The overlay was protecting 000's text, not
the lesson.

Upgrading costs one `docker compose down -v` and a re-run of `create_topics.sh`. This is not
avoidable by a compose profile: KRaft writes the controller quorum voters into the metadata
log at format time, so a three-voter quorum with two nodes unstarted elects no controller and
the cluster never starts. Three nodes or a re-format; "optional" is a fiction.

001–003 stay reproducible — nothing they record depends on broker count. Group formation,
assignment, rebalancing and the durable fold behave identically against three nodes. The
running cost is three JVMs instead of one.

**The amendment to 000.** Two criteria were contradicted and were reworded with the user's
approval rather than rewritten:

| | Was | Now |
|---|---|---|
| R0.4 | "run the broker and controller roles on a single node" | "…on every node" |
| R0.17 | "set the replication factor of all internal topics to 1" | "…to the broker count" |

R0.4's substance was always KRaft combined mode with no ZooKeeper, not the node count, so
the reword restores its intent. R0.17 was pinned to 1 explicitly "so that single-broker
operation does not fail" — that reason expired. Every other criterion in 000 is indifferent
to broker count. 000's overview says which era it was written in.

**Rejected.** The overlay file, for the reasons above. Also rejected: rewriting 000 to
describe a three-broker cluster throughout, which would erase the single-node substrate that
001–003 were built and recorded against.

---

## X11 — Specs are size-budgeted from 004 onward

**Date:** 2026-08-18 **Status:** accepted **Specs:** 004+

**Context.** The specs have grown monotonically: 000 is 262 lines across three files, 003 is
987 — 30 acceptance criteria, a 501-line `design.md`, 231 lines of tasks. Each individual
addition was defensible. The aggregate is a spec that is harder to hold in your head than the
feature it describes, which inverts the reason for writing specs at all.

**Decision.** From 004, each feature gets a budget: **roughly 12–15 acceptance criteria, a
`design.md` under 200 lines, and roughly 12 tasks.** One companion document in `docs/`, whose
length is not budgeted.

**Consequences.** The budget forces a distinction the earlier specs blurred. A *criterion* is
something that can be checked against the running system; a *rationale* is prose explaining
why. 003 promoted a great deal of rationale into criteria and into `design.md` decisions. Under
the budget that material goes to the `docs/` companion, where it is re-read, rather than to
`requirements.md`, where it is approved once and then scanned past.

Some depth is genuinely lost — 003's essays on the dual-write problem earned their length. The
trade is accepted on the grounds that a spec nobody re-reads is not serving as a spec.

Exceeding the budget is allowed and is not an error; it needs a sentence in `design.md` saying
which part of the feature demanded it.

**Rejected.** Retro-fitting the budget to 000–003 — they are the record of what was built and
approved, and shrinking them now would rewrite history to match a rule made afterwards.

---

## X12 — `order-snapshot` is the table, and is what 007 will rebuild state from

**Date:** 2026-08-19 **Status:** accepted **Specs:** 006, 007, cites X5

**Context.** 006 needs a compacted topic, and 007 (X5)
needs a compacted changelog to rebuild a local state store from. These are the same topic,
introduced one rung before the rung that consumes it — so the decision spans two features and
belongs here rather than in either `design.md`, per this file's routing rule.

**Decision.** `order-snapshot`: `cleanup.policy=compact`, keyed by `order_id`, co-partitioned
with `order-lifecycle`, carrying one self-contained snapshot per order and tombstones for
deletes. The order service writes it; the three consumers read it for its tombstones only.

**Consequences.** `order-lifecycle` stays `cleanup.policy=delete` permanently. This is not a
deferral — compacting an event log is destructive, because an order is four messages under one
key and each is an increment rather than a replacement. Compaction would keep only the newest,
leaving a `DELIVERED` event with no creation behind it and breaking every 001–005 experiment
that replays from earliest. The distinction between a log and a table is the reason there are
two topics, and it does not go away at 007.

Co-partitioning is load-bearing from here on. Equal partition counts plus one key and one
partitioner mean order X's snapshot and order X's events share a partition *number*, which is
what makes the consumers' partition-keyed fold cache correct and what 007's co-partitioned
state store and 009's joins both require. `scripts/create_topics.sh` passes one partition count
to both topics for that reason; splitting them would evict the wrong orders, silently.

At 006 this topic is **written but not read for state** — the fold's only source is still the
event log, and R6.12 says so explicitly to stop the implementation drifting into 007 early.
That leaves the fold derived from two topics with independent retention, which is why a deleted
order can be resurrected by a replay, a pending retry, or a DLQ replay. All three are recorded
as open in 006 and close at 007, where the changelog becomes the single source.

**Rejected.** Introducing the compacted topic at 007 alongside RocksDB — it would put
compaction, tombstones, a local store and a rebuild path in one rung, and the tombstone would
arrive as machinery rather than as the answer to "how do I delete an order?". Also rejected:
an `ORDER_DELETED` lifecycle event instead of a tombstone, which would keep deletes on the log
where replays already see them, but needs a new `EventType`, a terminal `OrderState` and a
`LEGAL_PREDECESSOR` table that can express "from any state" — a change to the shared event
contract that 006 is too small to justify. It remains available to 007 if the changelog rebuild
turns out to want it.

---

## X13 — The changelog is one compacted topic per consumer group

**Date:** 2026-08-20 **Status:** accepted **Specs:** 007, amends X12, confirms X5

**Context.** [X5](#x5--derived-state-ends-at-rocksdb--a-compacted-changelog-topic) promised
"a compacted changelog topic keyed identically to the state".
[X12](#x12--order-snapshot-is-the-table-and-is-what-007-will-rebuild-state-from) then said
`order-snapshot` *was* that topic. Building 007 showed the two cannot both be true.

**Decision.** Three topics, `order-fold.<group_id>`, each `cleanup.policy=compact`, keyed by
`order_id`, co-partitioned with `order-lifecycle`. `order-snapshot` keeps exactly its 006 role
— the producer's table, read by the consumers for its tombstones and nothing else.

**Consequences.** The fold is per `(group_id, order_id)` because the three services walk an
order's stages at their own pace, and compaction retains the latest value **per key**. One
topic keyed by `order_id` would have the groups overwrite one another, and every rebuild would
read whichever group wrote last. Putting the group in the key instead fixes compaction and
breaks partitioning — the key is what the partitioner hashes, so an order's fold and its events
would land on different partition numbers and the single-partition rebuild would be impossible.
The group therefore goes in the topic name. This is what Kafka Streams does, one changelog per
store per `application.id`, and 009's ksqlDB will create the same shape.

X5's `rocksdict` choice was checked rather than assumed on arrival, as X5 asked: version 0.3.29
publishes a `cp313` manylinux wheel that installs into `python:3.13-slim`, and the exclusive
directory lock the design depends on is real. No fallback was needed.

**Consequence that reaches backwards.** 005's `R5.9` says the retry worker "SHALL select the
handler … and SHALL fold the result into that service's consumer-group state". An embedded
store cannot be opened by a process that does not own the partition, so a worker holding no
partitions can fold nothing. R7.12 states the prohibition and the worker became a scheduler
that republishes instead. R5.9's *intent* is intact — the right service's handler runs and the
right group's state is folded — but the location moved, and R5.9's wording has not been
amended, because `requirements.md` is not amended without the user's say-so.

**Rejected.** Keying one topic `group_id|order_id` and producing with an explicit partition to
restore co-partitioning — it works, but the rebuild then reads all three groups' folds and
discards two-thirds, and the explicit partition is a footgun the moment anyone forgets it.
Reusing `order-snapshot`, per X12 — it carries no per-group `last_sequence` and no
`handled_count`, so bootstrapping from it merges three memories R3.2 requires to stay separate.
