# 006 — Compaction and Tombstones: Design

Implements [requirements.md](requirements.md). Every decision cites the criteria it
serves. Cross-cutting choices live in [DECISIONS.md](../../DECISIONS.md): the client (X1),
the wire format (X2), and the endpoint this rung is a step toward (X5).

## Architecture

```
                         POST /orders, POST /orders/{id}/events
                                        │
                            ┌───────────┴───────────┐
                            │     order-service     │
                            └───────────┬───────────┘
              event (blocks on ack)     │     snapshot (fire-and-forget)
                            ▼           │           ▼
        order-lifecycle  (delete)       │   order-snapshot  (compact)
        key=order_id                    │   key=order_id
        value=LifecycleEvent            │   value=full order │ null
        3 partitions, RF 3              │   3 partitions, RF 3
                            │           │           │
                            └─────┬─────┴───────────┘
                                  ▼
              inventory / notification / analytics   ← one group each,
              subscribed to BOTH topics                 both topics
                                  │
                                  ▼
                    Postgres order_fold  (group_id, order_id)
                        events  → upsert the fold
                        null    → DELETE the row

        DELETE /orders/{id} ──► tombstone to order-snapshot (blocks on ack) ──► 204
```

The two topics are **co-partitioned**: same key, same partitioner, same partition count,
so order X's snapshot and order X's events share a partition number (D8).

## Decisions

### D1 — Compaction goes on a new topic; the event log is untouched — *R6.1, R6.2*

An order is four messages under one key, each an increment. `PACKED` does not carry the
items; `SHIPPED` does not carry the payment. Compaction retains the newest value per key
and discards the rest, so compacting `order-lifecycle` would leave a `SHIPPED` event with
no creation behind it — `apply_event()` folds it against `last_sequence=0`, reports a
permanent `SEQUENCE_GAP`, and the order becomes unreconstructable. Every experiment in
001–005 that replays from earliest would break.

The rule underneath: **compaction is safe only where a message replaces its predecessor,
never where it adds to it.** `order-snapshot` satisfies that by construction (D2).

**Rejected.** `compact` or `compact,delete` on `order-lifecycle` — destroys the fold,
above. One topic carrying both events and snapshots, split by a header — the cleaner does
not read headers, so it would compact the events too.

### D2 — The snapshot value is the whole order, and is derived not stored — *R6.4*

`Order.as_snapshot()` on the existing frozen dataclass, built from `as_dict()` so the two
views cannot drift. Self-containment is the criterion: a consumer that has seen *only*
the newest snapshot for a key knows the customer, the items, the total, the payment and
the state. No `sequence` field is folded from it (D7).

**Rejected.** Publishing only on create and delete — then a key has one value and
compaction never has anything to remove. A trimmed `{state}`-only snapshot — not
self-contained, which is the property being taught.

### D3 — The snapshot write does not block; the tombstone write does — *R6.5, R6.8, R6.9*

A snapshot is derived state: if it is lost, `order-lifecycle` still holds the truth and
the next event rewrites it, so failing a `201` over it would trade the authoritative
write for the derived one. It is produced with a delivery callback that logs at WARNING
and nothing waits on it.

A `204` from the delete endpoint is a claim that the delete landed. That one waits, reusing
the `threading.Event` + delivery-callback pattern already in `publish_and_wait`, and
reuses `_describe_delivery_error` so R6.9's "name the partition" comes for free from 004.

### D4 — The order leaves the store only after the broker acknowledges — *R6.6, R6.7, R6.8, R6.9*

`DELETE /orders/{order_id}`: `404` if unknown (checked before anything is produced, R6.7),
then tombstone, then `OrderStore.remove()`, then `204`. This mirrors `create_order`, which
already registers only after the ack so a delivery failure leaves no order behind. Here the
same ordering means a failed delete leaves the order intact and retryable rather than
half-applied — removed locally but still present in every consumer's fold.

**Rejected.** Removing first and compensating on failure — a compensating re-add would
have to reconstruct state the failure may have already changed.

### D5 — The null check precedes `_decode` — *R6.10, R6.13*

Today `_decode` calls `raw.decode("utf-8")` on the value; `None` raises `AttributeError`,
which is caught and re-raised as `NonRetryableError`, which 005 routes to the dead-letter
topic. **Every tombstone would become a dead letter.** So `_handle_message` gains a first
branch — `if message.value() is None:` → `_handle_tombstone()` → delete, log, commit,
return — before decode, before the failure path, before the handler dispatch. 005's
routing is not modified, only bypassed.

### D6 — `StateStore.delete()`, and the row is really deleted — *R6.10, R6.11*

One method added to the protocol and both backends. `PostgresStateStore` runs
`DELETE FROM order_fold WHERE group_id = %s AND order_id = %s` and evicts the cache entry;
`MemoryStateStore` pops from `_folds` and `_handled`. No schema migration: nothing is added
to `state_schema.sql`.

The marker is `TOMBSTONE order_id=… partition=… offset=…` at WARNING, matching the
`VIOLATION` / `DUPLICATE_ABSORBED` / DLQ markers so `grep TOMBSTONE` suffices (R6.11).

**Rejected.** A `deleted_at` column — every read then filters on it, the row that
compaction erased from the topic survives in the table, and "deleted" stops meaning
deleted in the one place the feature is about.

### D7 — Both topics, one subscription, dispatch on `message.topic()` — *R6.10, R6.12*

`run()` subscribes to `[order_lifecycle_topic, order_snapshot_topic]`. A non-null message
on the snapshot topic is logged and committed with no fold write (R6.12) — the fold's only
source is the event log. Without that rule the fold would have two writers disagreeing
about `last_sequence`, which is precisely the merge 007 exists to do properly.

**Rejected.** A fourth consumer service reading the snapshot topic — it would have to
delete the other three groups' rows, breaking the independence R3.2 established. Each
group deleting only what it owns is why the subscription goes here.

### D8 — Co-partitioning is what makes the partition-keyed cache correct — *R6.1*

`MemoryStateStore._folds` and `PostgresStateStore._cache` are keyed by `int` partition, so
`order-lifecycle-2` and `order-snapshot-2` share a cache slot. Equal partition counts, the
same key and the same `consistent_random` partitioner make that **correct**: a tombstone on
`order-snapshot-2` evicts from the slot the events on `order-lifecycle-2` filled. It is
load-bearing, not incidental — an `order-snapshot` created with a different partition count
would silently evict the wrong orders' cache entries. Postgres is unaffected below the
cache; its key is `(group_id, order_id)`.

The same argument covers `_forget()` on revoke, which drops by partition number and so
releases both topics' partition *n* together.

### D9 — Cleaner knobs are tuned for observation, not for production — *R6.3*

Stock defaults (`segment.ms=7d`, `min.cleanable.dirty.ratio=0.5`) mean nothing visible
happens by hand: the cleaner skips a mostly-clean log and never touches the active segment.
`create_topics.sh` sets aggressive values on `order-snapshot` alone, from the environment,
so the walkthrough completes in minutes. **These are deliberately unrealistic** — a real
compacted topic pays a large rewrite cost for them — and the companion doc says so next to
the production defaults.

The script's `TOPICS` array becomes name → per-topic extra config, because
`min.insync.replicas` must not be the only thing that varies any more. The existing
`--if-not-exists` plus `kafka-configs.sh --alter` two-pass is reused unchanged; it already
exists because `--if-not-exists` skips `--config` on a topic that is already there.

### D10 — Defaults preserve 005, with one stated exception — *R6.14*

Every new variable defaults to 005's behaviour. The one exception, named here as R6.14
requires: a consumer started with no new settings now also subscribes to `order-snapshot`,
because a subscription that had to be switched on would leave the default configuration
unable to see a delete at all.

### D11 — Resurrection is left open, deliberately — *R6.15*

A tombstone erases one key in one topic. The order's events stay in `order-lifecycle`, its
pending messages in the retry topic, its dead letters in the DLQ, so a replay from earliest,
a waking retry worker, or `dlq_replay.py` will each recreate the fold. One root cause: the
fold has **two sources with independent retention**. Papering over it per-path would hide
that. 007 removes the second source.

## Environment surface

| Variable | Default | Read by | Criteria |
|---|---|---|---|
| `ORDER_SNAPSHOT_TOPIC` | `order-snapshot` | producer, consumers, `create_topics.sh` | R6.1, R6.4 |
| `SNAPSHOT_SEGMENT_MS` | `10000` | `create_topics.sh` | R6.3 |
| `SNAPSHOT_MIN_CLEANABLE_DIRTY_RATIO` | `0.01` | `create_topics.sh` | R6.3 |
| `SNAPSHOT_DELETE_RETENTION_MS` | `60000` | `create_topics.sh` | R6.3 |
| `SNAPSHOT_MIN_COMPACTION_LAG_MS` | `0` | `create_topics.sh` | R6.3 |

Broker-level: `KAFKA_LOG_CLEANER_ENABLE=true` (already the default) and
`KAFKA_LOG_CLEANER_BACKOFF_MS` are set explicitly on the compose anchor so the cleaner's
existence is visible in the environment rather than assumed.

## Known gaps, by intent

| Gap | Status |
|---|---|
| A replay from earliest resurrects a deleted order | open by design (D11); closed at 007 |
| A pending retry recreates a deleted fold | open by design (D11); closed at 007 |
| `dlq_replay.py` can republish a deleted order | open by design (D11); closed at 007 |
| The tombstone outlives the fold delete by `delete.retention.ms` only | inherent to compaction; the doc names the window |
| The snapshot topic's cleaner settings are unrealistic | accepted (D9); production values named in the doc |
| A lost snapshot write is never repaired until the next event | accepted (D3); the event log is the truth |
| The fold is still derived from two topics | inherent to D7; 007 makes it one |
| Everything still open from 004 and 005 | unchanged |

## Deferred to later specs

Local state stores and compacted changelog topics (007, [X5](../../DECISIONS.md)),
transactions and exactly-once (008), stream SQL (009).

## Budget

Within the [X11](../../DECISIONS.md) budget: 15 criteria, this file under 200 lines.
