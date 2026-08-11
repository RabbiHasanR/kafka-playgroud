# Kafka Playground

Single-broker Kafka 4.3.1 in KRaft mode (no ZooKeeper) for hands-on CLI learning.

| What | Where |
|---|---|
| Broker (from host) | `localhost:9092` |
| Broker (from other containers) | `kafka:19092` |
| Kafka UI | http://localhost:8080 |
| CLI tools | `/opt/kafka/bin/` inside the `kafka` container |
| Log segments on disk | `/var/lib/kafka/data/` inside the container |

## Lifecycle

```bash
docker compose up -d          # start
docker compose ps             # status (wait for kafka = healthy)
docker compose logs -f kafka  # broker logs
docker compose down           # stop, keep data
docker compose down -v        # stop, WIPE all topics/messages
```

Get a shell inside the broker — most commands below assume you're here:

```bash
docker exec -it kafka bash
cd /opt/kafka/bin
```

`BS=localhost:9092` is used as shorthand below. Set it once per shell:

```bash
export BS=localhost:9092
```

## Topics

```bash
./kafka-topics.sh --bootstrap-server $BS --create --topic demo --partitions 3 --replication-factor 1
./kafka-topics.sh --bootstrap-server $BS --list
./kafka-topics.sh --bootstrap-server $BS --describe --topic demo
./kafka-topics.sh --bootstrap-server $BS --alter --topic demo --partitions 6   # grow only, never shrink
./kafka-topics.sh --bootstrap-server $BS --delete --topic demo
```

Reading `--describe`: `Leader` is the broker serving that partition, `Replicas` all
brokers holding it, `Isr` the in-sync ones. On a single broker all three are `1`.

Auto-create is **off** — a typo gives you an error instead of a silent empty topic.

## Produce / consume

```bash
./kafka-console-producer.sh --bootstrap-server $BS --topic demo
./kafka-console-consumer.sh --bootstrap-server $BS --topic demo --from-beginning
```

Useful consumer flags:

```bash
--property print.key=true \
--property print.partition=true \
--property print.offset=true \
--property print.timestamp=true \
--max-messages 10 \
--partition 0            # read one partition only
--offset earliest        # with --partition: earliest | latest | <number>
```

Without `--from-beginning` a fresh consumer only sees messages produced *after* it starts.

## Keys and partitioning

```bash
./kafka-console-producer.sh --bootstrap-server $BS --topic demo \
  --property parse.key=true --property key.separator=:
# then type:  user1:logged in
```

Same key always hashes to the same partition — that is Kafka's only ordering
guarantee (ordering holds *within* a partition, never across).

Keyless messages are spread round-robin (sticky-batched), so ordering is not preserved.

## Consumer groups

```bash
./kafka-console-consumer.sh --bootstrap-server $BS --topic demo --group g1

./kafka-consumer-groups.sh --bootstrap-server $BS --list
./kafka-consumer-groups.sh --bootstrap-server $BS --describe --group g1
./kafka-consumer-groups.sh --bootstrap-server $BS --describe --group g1 --members --verbose
```

`--describe` columns: `CURRENT-OFFSET` (committed), `LOG-END-OFFSET` (latest written),
`LAG` = the difference. Lag is the number to watch in production.

One partition is consumed by at most one member of a group. More consumers than
partitions means the extras sit idle. Start/kill members and re-run `--describe`
to watch rebalancing.

## Offsets

The group must have **no active members** to reset:

```bash
./kafka-consumer-groups.sh --bootstrap-server $BS --group g1 --topic demo --reset-offsets --to-earliest --execute
./kafka-consumer-groups.sh --bootstrap-server $BS --group g1 --topic demo --reset-offsets --to-latest --execute
./kafka-consumer-groups.sh --bootstrap-server $BS --group g1 --topic demo --reset-offsets --shift-by -10 --execute
./kafka-consumer-groups.sh --bootstrap-server $BS --group g1 --topic demo --reset-offsets --to-offset 42 --execute
./kafka-consumer-groups.sh --bootstrap-server $BS --group g1 --topic demo --reset-offsets --to-datetime 2026-08-09T00:00:00.000 --execute
```

Swap `--execute` for `--dry-run` to preview.

## Inspecting the raw log

```bash
ls /var/lib/kafka/data/demo-0/
./kafka-dump-log.sh --files /var/lib/kafka/data/demo-0/00000000000000000000.log --print-data-log
```

Shows real record batches, offsets, timestamps, and compression — the physical
form of the "log" abstraction.

Per-partition offset boundaries without consuming:

```bash
./kafka-get-offsets.sh --bootstrap-server $BS --topic demo               # latest
./kafka-get-offsets.sh --bootstrap-server $BS --topic demo --time earliest
```

## Retention and compaction

```bash
./kafka-topics.sh --bootstrap-server $BS --create --topic compacted --partitions 1 --replication-factor 1 \
  --config cleanup.policy=compact \
  --config min.cleanable.dirty.ratio=0.01 \
  --config segment.ms=5000 \
  --config delete.retention.ms=100

./kafka-configs.sh --bootstrap-server $BS --entity-type topics --entity-name demo --describe
./kafka-configs.sh --bootstrap-server $BS --entity-type topics --entity-name demo \
  --alter --add-config retention.ms=60000
```

Compaction keeps only the newest value per key. It runs on *closed* segments, so
you must produce enough to roll a segment (hence the tiny `segment.ms`) before
old values disappear. A key with a `null` value is a tombstone — it deletes the key.

## Other tools

```bash
./kafka-broker-api-versions.sh --bootstrap-server $BS   # connectivity check
./kafka-cluster.sh cluster-id --bootstrap-server $BS
./kafka-producer-perf-test.sh --topic demo --num-records 100000 --record-size 100 --throughput -1 --producer-props bootstrap.servers=$BS
./kafka-consumer-perf-test.sh --bootstrap-server $BS --topic demo --messages 100000
```

## Gotchas

- **`advertised.listeners` is what clients actually connect to.** The broker hands
  this address back on connect, so a client that reaches the broker fine can still
  fail on the next call if the advertised address is wrong. Hence the two listeners:
  `localhost:9092` for the host, `kafka:19092` for containers.
- **Partition count can only grow.** Growing it rehashes keys, so existing keys may
  move to a different partition and their historical ordering is broken.
- **`--from-beginning` is ignored when the group already has committed offsets.**
  Reset the offsets or use a new `--group` name.
- **`docker compose down -v` deletes the volume** and every message with it.











---

# Spec 001 — Ordered Order-Event Pipeline

A FastAPI producer publishes order lifecycle events to `order-events` (3 partitions,
keyed by `order_id`); a single consumer folds them into per-order state and reports
every ordering violation it finds. Full spec in
[specs/001-order-event-pipeline/](specs/001-order-event-pipeline/).

The pipeline detects broken ordering three independent ways:

| Signal | Fires when | Why it exists |
|---|---|---|
| `SEQUENCE_GAP` | `sequence != last + 1` | mechanical — no domain knowledge needed |
| `ILLEGAL_TRANSITION` | e.g. `SHIPPED` before `PACKED` | domain-level and intuitive |
| `TOTAL_MISMATCH` | folded item total ≠ `PAID` amount | a true accumulator — **cannot** be computed from one message |

The third is the one that settles the argument. Sequence numbers are
self-*describing* but not self-*validating*: `seq: 4` is only wrong relative to a
remembered `3`. A running total has no such ambiguity — with no prior state there is
no total at all.

## Running it

Install (Python 3.11+):

```bash
python3 -m venv .venv && .venv/bin/pip install -e ".[dev]"
```

Start the broker and create the topic — auto-creation is off, so this is required:

```bash
docker compose up -d kafka
./scripts/create_topics.sh
```

From the host, in two terminals:

```bash
.venv/bin/python -m order_pipeline.producer.app     # :8000
.venv/bin/python -m order_pipeline.consumer.main    # :8001
```

Or inside compose, where the only thing that changes is the broker address
(`kafka:19092` instead of `localhost:9092`):

```bash
docker compose up -d --build producer consumer
```

| Endpoint | Purpose |
|---|---|
| `POST /orders/{order_id}/events` | publish one event; returns the **real** partition and offset |
| `POST /simulate` | generate N lifecycles at a given rate; returns a job id |
| `GET /simulate/{job_id}` | published / failed counts for a run |
| `GET /state` (port 8001) | the consumer's folded state — the amnesia experiment reads this |

Watching for violations is one filter:

```bash
grep VIOLATION consumer.log          # host
docker compose logs consumer | grep VIOLATION
```

## The experiments

Each one is a task in [tasks.md](specs/001-order-event-pipeline/tasks.md). Results
below are from an actual run, not predictions.

**T23 — the key decides the partition.** 20 orders, 160 events → 40/64/56 across the
three partitions, and **zero** orders spanning more than one. Uneven by design: that
is murmur2 hashing, not a bug.

**T24 — happy path.** 50 orders, 400/400 published, zero violations, every total
matching at `PAID`.

**T25 — unkeyed.** 140 violations of all three types. Two surprises worth knowing:

- Null-key messages do **not** automatically scatter. librdkafka's
  `sticky.partitioning.linger.ms` (default 10 ms) keeps a burst of them on one
  partition for batching, which silently keeps an unkeyed order ordered. The producer
  sets it to `0` so the fault injection actually injects a fault.
- Even scattered, a consumer that **keeps up in real time** may still see events in
  the right order and report nothing. The violations appear once the consumer is
  behind and drains each partition in batches. *Losing the guarantee is not the same
  as losing the ordering* — which is exactly why this class of bug reaches production.

**T26 — shuffled but correctly keyed.** All 8 events of an order on one partition,
144 violations anyway. Kafka preserved the order it was given, including a wrong one.
Partition ordering is faithful, not corrective.

**T27 — no global ordering.** Timestamps are monotonic *within* every partition and
non-monotonic across the topic. That is the guarantee boundary, working as designed.

**T28 — offsets survive.** Killed the consumer, restarted it: resumed at the committed
offsets (242/321/441) and re-consumed **0** records. Kafka remembered the position.

**T29 — state does not survive.** Same restart, and all 90 orders of folded state were
gone. An order with 3 items totalling 850 came back as `item_count: 0, total: 0`, and
the next `PAID` event raised three violations — every one of them false. Nothing was
wrong with the data.

This is the point of the whole feature: **a committed offset is a position, not a
memory.** And note the consumer cannot tell its own amnesia from a real producer bug —
that ambiguity is why durable state matters, and spec 003 fixes it.

**T30 — replay from zero.** A fresh `CONSUMER_GROUP_ID` re-read all 1320 records from
earliest and the totals came back correct. This genuinely works, and it breaks for
exactly two reasons: startup cost grows with the topic, and `retention.ms` eventually
deletes the inputs you would need to re-fold. Hence compaction (006) and durable
state (003).

**T31 / T32 — failing loudly.** Producing to a missing topic gives a named error, not
silent auto-creation. With the broker stopped the endpoint returns `504` and reports
no partition or offset — and is careful *not* to blame a missing topic for what is
really an unreachable broker.

**T33 — lag.** A 16,000-event burst pushed lag to 12,911 before draining to zero.
Visible in `kafka-consumer-groups.sh --describe` and in Kafka UI at `localhost:8080`.

## Known gaps, all deliberate

| Gap | Closed by |
|---|---|
| Folded state lost on consumer restart (R1.27) | 003 |
| Producer sequence counters lost on restart | accepted |
| Duplicate processing after a crash (at-least-once) | 003, 008 |
| Single broker, RF 1, no failover | 004 |

Do not "fix" the first one inside 001 — it is the evidence that motivates 003.
