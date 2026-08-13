# 001 — Ordered Order-Event Pipeline: Tasks

Implements [requirements.md](requirements.md) per [design.md](design.md).

Each task cites the requirement IDs it satisfies and, where relevant, the design
decision it follows.

## Shared contract

- [x] **T1** — Add the project scaffold and `config.py`, reading
  `KAFKA_BOOTSTRAP_SERVERS`, `ORDER_EVENTS_TOPIC`, and `CONSUMER_GROUP_ID` from the
  environment with host-usable defaults and no connection details in source.
  — *R1.32, R1.33* — D15
- [x] **T2** — Define the event model in `events.py`: `order_id`, `sequence`,
  `event_type`, `occurred_at`, `payload`; the six event types; and the `ITEM_ADDED`
  (`sku`, `qty`, `unit_price`) and `PAID` (`amount`) payloads, with all money as
  integer minor units. — *R1.1, R1.3, R1.4* — D4
- [x] **T3** — Define the lifecycle transition table mapping each `event_type` to its
  legal predecessor states, in `events.py`, importable by both producer and consumer.
  — *R1.5* — D10
- [x] **T4** — Write `scripts/create_topics.sh` creating `order-events` with 3
  partitions and replication factor 1, and confirm a missing topic errors rather than
  being auto-created. — *R1.6, R1.9* — D14

## Producer

- [x] **T5** — Implement `kafka_producer.py`: construct the `Producer`, publish with
  the UTF-8 `order_id` as message key using the default partitioner, wait on the
  delivery report, and return the assigned partition and offset — or an error when
  the broker does not acknowledge within the timeout. — *R1.7, R1.12, R1.13* — D2, D5
- [x] **T6** — Start a daemon `poll()` thread from the FastAPI lifespan, and stop it
  and `flush()` with a timeout on shutdown. — *R1.14* — D7
- [x] **T7** — Maintain per-`order_id` sequence counters under a lock, assigning the
  next contiguous value starting at 1 on each publish. — *R1.2* — D8
- [x] **T8** — Implement `POST /orders/{order_id}/events` as a synchronous `def`
  handler returning the real partition and offset, and `504` on delivery timeout.
  — *R1.10, R1.12* — D5
- [x] **T9** — Implement the simulator: generate complete valid lifecycles including
  a variable number of `ITEM_ADDED` events, and pace publishing to a requested
  events-per-second rate. — *R1.11* — D6
- [x] **T10** — Add the `unkeyed` and `shuffle` fault-injection flags, both defaulting
  to off, composable, publishing with a null key and in permuted sequence order
  respectively. — *R1.15, R1.16, R1.17* — D9
- [x] **T11** — Implement `POST /simulate` as a background job returning a summary
  with counts of published and failed events, plus `GET /health`. — *R1.11* — D6

## Consumer

- [x] **T12** — Implement `OrderState` (`last_sequence`, `state`, `item_count`,
  `total`) and the pure fold `(state, event) → (new_state, violations)` in
  `state.py`, accumulating item count and `Σ(qty × unit_price)`. — *R1.18* — D11
- [x] **T13** — Detect sequence-gap violations when `sequence != last_sequence + 1`,
  including the unknown-`order_id` case where any `sequence` other than 1 is a
  violation. — *R1.19, R1.24*
- [x] **T14** — Detect illegal-transition violations using the T3 transition table.
  — *R1.20* — D10
- [x] **T15** — Detect total-mismatch violations by comparing a `PAID` event's
  `amount` against the folded running total. — *R1.21* — D4
- [x] **T16** — Record each violation with its type, `order_id`, expected value, and
  observed value, and continue consuming rather than halting. — *R1.22, R1.23*
- [x] **T17** — Implement the consume loop with `enable.auto.commit=False`,
  `auto.offset.reset=earliest`, committing each record's offset only after it has
  been folded. — *R1.25, R1.26, R1.28* — D12
- [x] **T18** — Hold folded state only in process memory, with no persistence and no
  restore path on startup. — *R1.27* — D11, X3
- [x] **T19** — Log every consumed record at `INFO` with partition, offset, key,
  `order_id`, `sequence`, and `event_type`; log every violation at `WARNING` with a
  stable `VIOLATION` marker. — *R1.29, R1.30* — D16
- [x] **T20** — Expose `GET /state` from a threaded HTTP server dumping the folded
  state of every known `order_id`. — *R1.31* — D13

## Wiring

- [x] **T21** — Add `producer` and `consumer` services to `docker-compose.yml`,
  gated on broker health, with `KAFKA_BOOTSTRAP_SERVERS=kafka:19092`, changing no
  broker configuration. — *R1.33* — D15

## Verification experiments

Each is run and observed, not merely coded. Tick only after actually running it.

- [x] **T23** — **Key → partition.** Publish ~20 orders, then read each partition
  with `kafka-console-consumer.sh --partition N --property print.key=true` and
  confirm every event for a given `order_id` appears in exactly one partition.
  — *R1.7*
- [x] **T24** — **Happy path.** `POST /simulate` with 50 orders; confirm 50 complete
  lifecycles, zero violations, and correct totals at `GET /state`.
  — *R1.11, R1.18, R1.21*
- [x] **T25** — **Unkeyed run.** Simulate with `unkeyed=true`; confirm one order's
  events land on multiple partitions and sequence-gap violations appear. This is the
  demonstration of *why* the key exists. — *R1.15, R1.19*
- [x] **T26** — **Shuffled run.** Simulate with `shuffle=true`; confirm
  illegal-transition violations appear even though every event is correctly keyed —
  partition ordering cannot repair a producer that emits out of order.
  — *R1.16, R1.20*
- [x] **T27** — **No global ordering.** Show events from different `order_id`s
  interleaving in an order that does not match production time, and record that this
  is correct behaviour rather than a fault. — *R1.8*
- [x] **T28** — **Offset survival.** Stop the consumer mid-stream and restart it;
  confirm via `kafka-consumer-groups.sh --describe` that it resumes at the committed
  offset instead of replaying. Kafka remembered the *position*. — *R1.26*
- [x] **T29** — **State amnesia.** Capture `GET /state`, restart the consumer
  mid-order, and capture it again; confirm both symptoms — a false sequence violation
  indistinguishable from a real one, and a wrong order total, because the earlier
  `ITEM_ADDED` events sit before the committed offset and are never re-read.
  — *R1.27, R1.31*
- [x] **T30** — **Replay from zero.** Restart the consumer with a fresh
  `CONSUMER_GROUP_ID` so it reads from earliest; confirm the totals come out correct
  again, then record the two limits that break this at scale — startup cost growing
  with the topic, and `retention.ms` eventually deleting the inputs needed to
  re-fold. — *R1.28*
- [x] **T31** — **Missing topic.** Produce to a non-existent topic and confirm an
  explicit error rather than silent auto-creation. — *R1.9*
- [x] **T32** — **Broker unreachable.** Stop the broker and confirm the single-event
  endpoint returns an error and reports no partition or offset. — *R1.13*
- [x] **T33** — **Lag visible.** Simulate at a rate above consumer throughput;
  confirm growing lag in `kafka-consumer-groups.sh --describe` and in Kafka UI at
  `localhost:8080`. — *R1.11*

## Documentation

- [x] **T34** — Document running the pipeline from the host and from inside compose,
  the experiment list above and what each one proves, and the `grep VIOLATION`
  filter. — *R1.30, R1.33*

## Notes

**T22 is gone, and the number is deliberately not reused.** It was a unit test of the
fold function. This project does not carry a test suite (see `CLAUDE.md`); correctness
is established by the T23–T33 experiments, which exercise the same code against a real
broker. The requirements it cited stay covered: R1.18 by T12, R1.19 and R1.24 by T13,
R1.20 by T14, R1.21 by T15.

**Pending process change, not a task.** `CLAUDE.md`'s *Layout* section needs a rule
recording `DECISIONS.md` and the routing rule between it and `design.md`, or the
convention will not hold. It cites no requirement, so it is deliberately not a
checkbox here — `.claude/tools/spec-status.sh` requires every task to cite one.

**`.claude/tools/spec-status.sh` checks requirement↔task traceability only.**
`DECISIONS.md` is outside its scope and is not machine-checked.

**T21 was verified statically, not by running the containers.** `docker compose
config` validates, both services are gated on `service_healthy`, and the diff to
`docker-compose.yml` is purely additive — no broker setting changed. The images were
**not** built and the services were **not** started, so `000-foundations` T12
(a container-side client reaching `kafka:19092`) remains unticked. Run
`docker compose up -d --build producer consumer` to close it.

Every T23–T33 experiment was run from the host against `localhost:9092`, which
exercises R1.33's host half but not its container half.

**T18 must not be "improved".** It mandates a shortcoming (R1.27, X3). Adding
persistence here would remove the evidence that motivates spec 004.
