# Compaction and Tombstones

Companion to [spec 006](../specs/006-compaction-tombstones/requirements.md).

Until this feature, nothing in this repository could delete an order. The consumers'
`order_fold` rows grew forever, `order-lifecycle` was append-only, and there was no HTTP
verb that removed anything.

That gap is not really about a missing endpoint. It is about a property no topic here had
yet: the ability to behave as a **table** rather than a **log**.

---

## 1. One topic, two possible meanings

`cleanup.policy` decides which one a topic is.

| | `delete` (the default) | `compact` |
|---|---|---|
| What ages out | whole segments, by `retention.ms` / `retention.bytes` | superseded values, by key |
| How long data lives | a fixed window | the latest value per key, indefinitely |
| A key appears | as many times as it was written | effectively once |
| Replay cost | proportional to the **number of events** | proportional to the **number of keys** |
| It models | a diary | a table |

A compacted topic converges to a snapshot of current state per key. That is why Kafka uses
it for its own `__consumer_offsets`, and why Kafka Streams backs every state store with one.

Both can be combined — `cleanup.policy=compact,delete` compacts by key *and* still ages
data out. Nothing here uses that.

---

## 2. Why `order-lifecycle` is not compacted

This is the part worth slowing down for, because "turn on compaction" looks like a
one-line change and is a data-loss bug.

An order is four messages under one key:

```
key=ord-abc  {"event_type":"ORDER_CREATED","sequence":1,"payload":{customer, items, payment}}
key=ord-abc  {"event_type":"PACKED",       "sequence":2}
key=ord-abc  {"event_type":"SHIPPED",      "sequence":3,"payload":{carrier, tracking}}
key=ord-abc  {"event_type":"DELIVERED",    "sequence":4}
```

Each is an **increment**. `PACKED` does not carry the items. `SHIPPED` does not carry the
payment. Compaction keeps only the newest value per key, so it would leave:

```
key=ord-abc  {"event_type":"DELIVERED","sequence":4}
```

A `DELIVERED` event with no creation behind it. `apply_event()` folds sequence 4 against
`last_sequence=0`, reports a permanent `SEQUENCE_GAP`, and the order can never be
reconstructed. Every experiment in specs 001–005 that replays from earliest breaks.

**The rule underneath:**

> Compaction is safe only where a message **replaces** its predecessor.
> It is destructive where a message **adds** to it.

Events add. Snapshots replace. So spec 006 introduces a second topic instead of changing
the first.

---

## 3. The two topics

```
order-lifecycle  (cleanup.policy=delete)     order-snapshot  (cleanup.policy=compact)
key = order_id                               key = order_id
value = one LifecycleEvent                   value = the WHOLE order, or null
an event log — the source of truth           a table — derived, and disposable
```

Both are keyed by `order_id`, use the same partitioner, and have the same partition count.
That makes them **co-partitioned**: order X's snapshot lands on the same partition *number*
as order X's events.

This is load-bearing, not tidiness. The consumers cache folds by partition number, so
`order-snapshot-2` and `order-lifecycle-2` share a cache slot. Equal counts make that
collision correct — a tombstone evicts exactly the orders whose events filled the slot.
Create the snapshot topic with a different partition count and the wrong orders get evicted,
silently. `scripts/create_topics.sh` passes one `PARTITIONS` value to both for that reason.

The snapshot value is deliberately the *entire* order — customer, items, total, payment,
state — and not a trimmed `{state}`. A consumer may legitimately see this message and
nothing else about the order, because compaction discarded everything earlier. Trimming it
would halve the bytes and destroy the property the topic exists to demonstrate.

---

## 4. Tombstones

A **tombstone** is a message with a key and a `null` value. It means "this key no longer
exists in this topic." The log cleaner erases every earlier value for that key, and then,
after `delete.retention.ms`, erases the tombstone too.

```
DELETE /orders/ord-abc
   └─ produce(topic="order-snapshot", key="ord-abc", value=None)
```

### Null is legal anywhere; only the cleaner makes it a tombstone

You can produce a null value to *any* topic. Kafka accepts it and every consumer sees
`msg.value() is None`. What a compacted topic adds is the broker-side half.

| | `compact` | `delete` |
|---|---|---|
| Can produce `value=null` | yes | yes |
| Consumer sees `None` | yes | yes |
| Older values for that key erased | **yes** | no |
| The null message itself eventually disappears | **yes**, after `delete.retention.ms` | no — only when its segment ages out |
| "Tombstone" is the right word | yes | not really |

Put a null on `order-lifecycle` and your consumers could still act on it — that half is
application convention. But nothing would ever clean it up. It would be a null message
cosplaying as a tombstone.

### The mirror rule: a compacted topic rejects a null *key*

There is nothing to compact by. This is why `_produce_keyed` types its key as `str` and not
`str | None`, and why keying by `order_id` is a precondition of this feature rather than a
detail of it.

### Why `delete.retention.ms` exists

The tombstone does not vanish the moment it has done its work. It lingers, by default for
24 hours.

That delay is the window in which a slow, restarting, or newly-bootstrapping consumer can
still *learn about the delete*. Remove tombstones immediately and a consumer that was
offline reads the compacted topic, never sees the marker, and resurrects state that was
deleted days ago. The window is the only thing standing between compaction and silent
resurrection — which is also the subject of §7.

---

## 5. What the consumers do with one

The null check sits **before** decoding, and that ordering is the whole trick:

```python
# runtime.py, _handle_message
if message.value() is None:          # ← FIRST
    self._handle_tombstone(message, partition)
    return
```

Without it, `_decode` calls `raw.decode("utf-8")` on `None`, raises `AttributeError`, and
spec 005 classifies that as a non-retryable failure and routes the message to the
dead-letter topic. **Every delete would become a dead letter.** 005's failure path is not
modified here, only bypassed.

`_handle_tombstone` then deletes that group's row — a real `DELETE FROM order_fold`, not a
`deleted_at` flag. The row that compaction erased from the topic must not survive in the
table, or "deleted" stops meaning deleted in the one place this feature is about.

Each of the three consumer groups deletes only its own row. That is why the existing
consumers subscribe to the snapshot topic rather than a fourth service reading it: a
separate reader would have to delete other groups' rows, undoing the independence spec 003
established.

Non-null messages on the snapshot topic are committed and otherwise ignored. The fold's only
source is the event log. Folding a snapshot too would give it two writers disagreeing about
`last_sequence` — that merge is spec 007's job.

---

## 6. Making it observable

Kafka's production defaults make compaction invisible by hand:

| Setting | Kafka default | Here | Why |
|---|---|---|---|
| `segment.ms` | 7 days | 10 s | the cleaner never touches the **active** segment |
| `min.cleanable.dirty.ratio` | 0.5 | 0.01 | it skips a log that is less than half garbage |
| `delete.retention.ms` | 24 hours | 60 s | how long the tombstone lingers |
| `min.compaction.lag.ms` | 0 | 0 | unchanged |

**These values are deliberately unrealistic.** A real compacted topic pays a large rewrite
cost for a 10-second segment roll. They exist so the walkthrough below finishes while you
are still watching it. Everything is overridable:

```bash
SNAPSHOT_SEGMENT_MS=60000 scripts/create_topics.sh
```

The broker's `log.cleaner.enable=true` is already the default; `docker-compose.yml` states
it anyway, because from this feature on there is a topic whose correctness depends on the
cleaner actually running.

### Walkthrough

```bash
docker compose up -d && scripts/create_topics.sh

# 1. Confirm the policy and that both topics agree on partition count.
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 \
  --describe --topic order-snapshot

# 2. Create an order and walk it through its lifecycle.
scripts/place_orders.sh
ORDER=ord-...        # from the output
for E in PACKED SHIPPED DELIVERED; do
  curl -s -X POST localhost:8010/orders/$ORDER/events \
    -H 'content-type: application/json' -d "{\"event_type\":\"$E\"}" >/dev/null
done

# 3. Watch the table. Several values for one key — compaction has not run yet.
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:19092 --topic order-snapshot --from-beginning \
  --property print.key=true --property print.value=true --timeout-ms 5000

# 4. Wait past segment.ms, then re-read. Only the newest value per key survives.
sleep 20 && (repeat step 3)

# 5. Delete the order.
curl -i -X DELETE localhost:8010/orders/$ORDER          # 204

# 6. The tombstone is visible as a null value, and every consumer reports it.
docker compose logs --since 1m | grep TOMBSTONE

# 7. The fold is gone from all three groups.
docker exec -it postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -c "SELECT group_id, order_id FROM order_fold WHERE order_id = '$ORDER';"

# 8. Wait past delete.retention.ms and re-read: the tombstone has erased itself too.
sleep 90 && (repeat step 3)
```

Steps 4 and 8 are the two halves worth seeing. Step 4 is compaction; step 8 is the tombstone
completing its own removal.

---

## 7. What a tombstone does not reach

**Kafka has no cross-topic delete.** A tombstone erases one key in one topic. After
deleting an order:

| Where | State |
|---|---|
| `order-snapshot` | key erased, then the tombstone itself | 
| `order_fold` (all 3 groups) | rows deleted |
| `order-lifecycle` | **all four events still there**, until `retention.ms` |
| `order-lifecycle.retry` | **still there** — a pending retry will still run |
| `order-lifecycle.dlq` | **still there** — replayable by hand |

So a deleted order can come back, three ways:

1. **A replay from earliest.** A new consumer group re-folds the four events and recreates
   the row. Worse if `delete.retention.ms` has passed: the tombstone is gone, so nothing
   ever corrects it.
2. **The retry lane.** `retry_worker.py` holds messages for up to 150 seconds by default.
   Delete an order while one of its events is waiting, and the worker wakes up, runs the
   handler, and recreates the fold.
3. **DLQ replay.** `dlq_replay.py` republishes to `order-lifecycle`, which re-folds into all
   three groups.

These are **open by design**, not oversights. They share one root cause: the fold has **two
sources with independent retention**, and a table derived from two logs that expire on
different clocks will eventually disagree. Patching each path individually would hide that.

Spec 007 removes the second source — state is rebuilt from a compacted changelog alone, one
source, one retention policy — and that is where all three close.

The alternative considered and rejected here was emitting an `ORDER_DELETED` *event* to
`order-lifecycle` alongside the tombstone. It would close path 1 properly, and it is what a
production system usually does: the log records that the delete happened, the table records
that the order is gone. It was rejected for this rung because it needs a new `EventType`, a
terminal `OrderState`, and a `LEGAL_PREDECESSOR` table that can express "from any state" —
a change to the shared event contract, out of proportion to the lesson.

---

## 8. Still open

| Gap | Where it closes |
|---|---|
| A replay, a pending retry, or a DLQ replay resurrects a deleted order | 007 |
| The fold is derived from two topics | 007 |
| A lost snapshot write is repaired only by the next event — and `DELIVERED` has none | accepted; the event log is the truth |
| The cleaner settings here are unrealistic | accepted; production values in §6 |
| Head-of-line blocking in the retry lane; DLQ depth alerting | still open from 005 |
| Unclean leader election | still open from 004 |
