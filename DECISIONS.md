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

**Date:** 2026-08-11 **Status:** accepted **Specs:** 001–008

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
writes `order-events`.

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

**Date:** 2026-08-11 **Status:** accepted **Specs:** 001, superseded in effect by 003

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
