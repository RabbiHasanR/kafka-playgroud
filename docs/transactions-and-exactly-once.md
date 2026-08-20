# Transactions and exactly-once

Companion to [spec 008](../specs/008-transactions-exactly-once/requirements.md).

Two mechanisms live here and they are constantly confused with each other. An **idempotent
producer** stops one producer duplicating or reordering its own retries. A **transaction**
makes a consumer's writes and its offset commit land together or not at all. The first is
free and always on. The second costs latency and is off by default.

Neither of them makes a handler run exactly once. That sentence is the whole reason this
document exists.

---

## Part 1 — Idempotence, and why reordering is the worse half

### What `retries` was doing before this

Every producer here has set `acks=all` and `retries` since 005. Neither implies the other,
and together they allow two failures.

**Duplication.** The broker writes the record, the acknowledgement is lost coming back,
librdkafka retries, and the record is written twice. Note that `acks=all` makes this *more*
likely rather than less — more hops, more chances to lose the ack.

**Reordering.** `max.in.flight.requests.per.connection` defaults to 5. Batch 1 fails and is
retried; batches 2 and 3 already succeeded. Partition order is now `2, 3, 1`.

This project absorbs duplication already — the sequence guard from 003 exists for exactly
that, and `handled_count` counts what it absorbed. It has **no defence against reordering**,
and reordering attacks the one invariant everything else rests on.

### Why reordering hurts *here* specifically

The domain is an ordered lifecycle: `ORDER_CREATED → PACKED → SHIPPED → DELIVERED`.

- `is_legal_transition` sees a `SHIPPED` arrive before its `PACKED` and logs
  `VIOLATION type=ILLEGAL_TRANSITION`. Nothing was wrong with the data. You would be
  debugging a fiction.
- The sequence guard sees sequence 3 before sequence 2 and reports `SEQUENCE_GAP`. Same
  fiction, different marker.
- Worst, on the **changelog**: the fold for sequence 3 is written, then the retried fold for
  sequence 2 lands *after* it under the same key. Compaction retains the latest value per
  key — which is now the older fold. `restore()` then rebuilds that faithfully. The
  corruption is silent, durable, survives a rebuild, and no marker in this repository
  reports it.

That last one is why `PRODUCER_IDEMPOTENCE` defaults to `true` even on the at-least-once
path.

### What it actually does

The producer gets a **producer id** and a **per-partition sequence number**. The broker
deduplicates a retry it has already seen and *rejects* a sequence that arrives out of order.
Requirements: `acks=all`, `retries > 0`, `max.in.flight <= 5` — all already true here.

### What it does not do

- **It does not survive a producer restart.** A new process gets a new producer id, so a
  duplicate spanning a crash is not caught.
- **It does not span partitions or topics.** The guarantee is per producer, per partition.
- **It does nothing at the HTTP boundary.** A client whose `POST /orders` times out *after*
  the broker accepted the write will retry and produce a genuinely new event with a new
  `event_id`. Idempotence cannot see that. The fixes are a client-supplied idempotency key
  or a transactional outbox, and no spec in this repository claims either.

Turn it off to watch the before:

```bash
PRODUCER_IDEMPOTENCE=false docker compose up -d --force-recreate order-service
```

---

## Part 2 — What one transaction covers

### The defect

Until 008 the consume loop did this:

```
handler runs  →  produce changelog record  →  commit offset
                                          ↑
                                    crash here
```

Two operations. 003 put a lever in that window on purpose (`STATE_CRASH_AFTER`), and the
residue was a number: `handled_count` above `last_sequence`.

The reason it could not be fixed at 003 is that the offset went to Kafka and the fold went
to Postgres, and nothing spans two systems. **007 is what made 008 possible** — moving the
fold onto a compacted changelog topic meant both writes became Kafka operations.

### The fix

```
begin_transaction()
  ├─ produce changelog record(s)
  ├─ produce retry / dead-letter record(s)
  └─ send_offsets_to_transaction(offsets, consumer_group_metadata())
commit_transaction()
```

Everything in that block lands or none of it does. The offsets go through the *producer*,
not `consumer.commit()` — committing against the consumer would put the offset outside the
transaction, which is the exact defect being fixed.

### One producer, not two

The largest change in this feature is not the API. It is that `LocalStateStore` and
`FailureRouter` each built their own `Producer` and now share one, built in `main.py`.

**Every write inside one transaction must come from one producer instance.** That is the
constraint, and it is why `transactions.build_producer` exists. The sharing is unconditional
rather than switched on with the guarantee, so there is only one wiring shape to get right.

### The identity, and the setting it promotes

Under `exactly_once` the producer carries a `transactional.id`, here
`<group_id>-<CONSUMER_INSTANCE_ID>`. The broker uses it to **fence**: when a producer with a
known identity reappears, its epoch is bumped and the previous holder's writes start being
rejected. That is what stops a zombie — an evicted member whose process is still alive and
still mid-batch — from committing on top of its replacement.

It must be **stable across restarts**. A random identity per start fences nothing, because
the zombie holds a different one.

The consequence is that `CONSUMER_INSTANCE_ID`, a log field since 002, is now
correctness-critical. Two members sharing a value fence *each other* in a loop, each bumping
the epoch the other just took:

```
PRODUCER_FENCED — exiting; is CONSUMER_INSTANCE_ID unique?
```

The process exits rather than retrying. A fenced producer cannot un-fence itself: its epoch
is behind for good, so retrying is a spin, not a recovery.

### Why v2 and not per-partition identities

Two protocols exist. **v1** gives every partition its own `transactional.id`, so producers
are created and destroyed inside the rebalance callback. **v2** (KIP-447) keeps one producer
per instance and fences through the consumer group's generation, carried by
`consumer_group_metadata()`.

v2 is used here because 007 D6 already made `_on_assign` blocking — it rebuilds state stores
before consuming. Adding a set of `init_transactions()` round trips to that callback would
make the eviction risk materially worse, on the exact path that is already the slowest.

### Batching, and why the interval is checked on empty polls

One transaction per message is the pathological setting: every message pays a full two-phase
commit. `TRANSACTION_COMMIT_INTERVAL_MESSAGES` (default 100) and
`TRANSACTION_COMMIT_INTERVAL_MS` (default 200) bound it, whichever arrives first.

The time bound is checked on the `poll()`-returned-nothing branch too. Without that, a
transaction opened by the last message of a burst stays open through the lull, and every
`read_committed` reader downstream stalls at the last stable offset for as long as the quiet
lasts. Kafka Streams checks on the same schedule at a 100 ms default.

Set the message count to 1 for the crash demonstrations.

---

## Part 3 — The part configuration does not buy

### RocksDB is outside the transaction

The transaction covers the changelog records and the offsets. Both are Kafka operations. The
**local RocksDB write is not**, and nothing can roll a disk write back.

So an aborted transaction leaves the local store holding folds the changelog never received.
The store is now *ahead* of the truth — and the sequence guard cannot save you, because the
guard absorbs a fold that is too **old**, not one that is too **new**. An uncommitted fold
would silently swallow the redelivery that was meant to redo the work.

The repair is not a rollback. It is a rebuild:

1. `store.discard(partitions)` — close the `Rdict`, delete the directory and its checkpoint.
2. `store.restore(partitions)` — replay the changelog, which under `read_committed` holds
   only what actually committed.
3. `seek()` each partition back to its committed offset — which is exactly where the aborted
   transaction began, so nothing needs to track that separately.

Only the partitions the transaction *wrote to*. Rebuilding untouched ones would pay a full
restore for nothing.

This is what Kafka Streams does when it wipes state stores on unclean shutdown, and it is the
same reason.

### Two settings follow from this

**`isolation.level=read_committed` on the restore reader is correctness, not preference.** A
rebuild at `read_uncommitted` replays aborted folds and reintroduces precisely the corruption
the transaction was bought to prevent — via the mechanism meant to repair it.

**`STATE_REBUILD=checkpoint` is refused under `exactly_once`,** and the process exits 2. A
checkpoint asserts the store already matches the changelog up to a recorded offset. An
aborted batch makes that false, and the checkpoint would have the rebuild skip exactly the
records that repair it. Refused rather than silently downgraded: a lever that quietly stops
meaning what it says is worse than one that will not start.

### What `read_committed` costs

- **Latency.** A consumer cannot read past the **last stable offset** — the first offset of
  the oldest open transaction. An open transaction blocks readers behind it. The commit
  interval is the lever.
- **Non-contiguous offsets.** Commit and abort markers are real records occupying real
  offsets. Consumer lag arithmetic and `endOffset - currentOffset` go slightly off, and gaps
  in the offset sequence are normal rather than a sign of loss.

---

## Part 4 — What is still at-least-once afterwards

Be precise about this. The feature's name promises more than Kafka delivers.

### A handler still runs twice

A redelivery after an abort runs the handler again. `notification.py` prints its customer
message a second time. What the transaction guarantees is that **nothing which ran twice was
ever committed twice** — the aborted fold is discarded, so the durable state reflects one
application.

The `README.md` line saying 008 drives `handled_count` to zero is true of the **durable
fold** and not of handler execution. That correction is why R8.15 exists.

### External side effects are not transactional

The notification message stands in for an email. Kafka cannot un-send an email. Anything
reaching outside Kafka — an HTTP call, a payment, a push notification — needs its own
idempotency at the far end. A transaction is not a distributed transaction.

### The retry worker

It reads `read_committed`, so it never schedules a retry for a message whose transaction
aborted. But its own cycle — consume, republish, commit — is still two operations, the same
shape the service consumers just had wrapped. A crash between the republish and the commit
still redelivers, and the message is scheduled twice.

Left open deliberately: a second transactional producer with its own identity lifecycle
would add surface without showing a mechanism this feature has not already shown.

### The HTTP boundary

Covered in Part 1. Unclaimed by any spec.

---

## Watching it

The clearest demonstration in the feature — an aborted transaction does **not** un-write its
records. They are physically on the topic, and the abort is a marker over them:

```bash
# One transaction per message, crash inside the open transaction.
PROCESSING_GUARANTEE=exactly_once \
  TRANSACTION_COMMIT_INTERVAL_MESSAGES=1 \
  STATE_CRASH_AFTER=transaction_open \
  docker compose up -d --force-recreate inventory-consumer

# Then read the changelog both ways and compare.
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:19092 \
  --topic order-fold.inventory-service --from-beginning \
  --isolation-level read_committed

docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:19092 \
  --topic order-fold.inventory-service --from-beginning \
  --isolation-level read_uncommitted
```

The record is absent from the first and present in the second.

---

## Looking ahead to 009

ksqlDB and Kafka Streams supply all of this behind one setting
(`processing.guarantee=exactly_once_v2`). They create the changelog topics, they hold the
transactional identities, they wipe and rebuild state stores on unclean shutdown, and they
batch the commits.

That is worth meeting **after** this rather than instead of it. The setting is one line; what
it is doing underneath is this document.
