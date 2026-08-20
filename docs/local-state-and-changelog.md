# Local state stores and changelog topics

> Spec [007](../specs/007-local-state-stores-changelog/requirements.md). Companion to
> [durable-state.md](durable-state.md) (003, whose Postgres backend this replaces) and
> [compaction-and-tombstones.md](compaction-and-tombstones.md) (006, whose compacted
> topic this one depends on).

## What changed, in one sentence

The consumers' folded state moved out of a shared Postgres table and onto **each
instance's own disk**, one embedded store per partition it owns, made recoverable by a
**compacted changelog topic** keyed exactly like the state.

```
003                                  007
─────────────────────────────        ─────────────────────────────────────────────
        ┌───────────┐                inventory-consumer   → order-fold.inventory-service
        │  Postgres │                  /var/lib/order-state/inventory-service/{0,1,2}/
        │ order_fold│                notification-1       → order-fold.notification-service
        └─────▲─────┘                  …/notification-service/0/     ← only what it owns
   ┌──────────┼──────────┐            notification-2
   │          │          │              …/notification-service/1/
inventory notification analytics      notification-3
   ×5 processes, one table              …/notification-service/2/
```

Postgres was never the intended endpoint. [X4](../DECISIONS.md) says so in writing and
books itself as *superseded by X5 at 007*. It was chosen at 003 because a database makes
the dual-write problem impossible to miss — the offset goes to Kafka, the fold goes
somewhere else, and no single operation covers both.

## Why the changelog is per consumer group

This is the question worth getting right, because the wrong answer is very appealing.

The three services fold the **same order** at their **own pace**. Inventory may still be
at `PACKED` while notification is already at `SHIPPED` — that is the whole point of
running three groups on one topic.

Compaction retains **the latest value per key**. So a single topic keyed by `order_id`:

```
order-fold          key=order-42
  offset 11  ← inventory writes    {last_sequence: 2, state: PACKED}
  offset 12  ← notification writes {last_sequence: 4, state: SHIPPED}
  ─── the cleaner runs ───
  offset 12 survives.  Every group rebuilding order-42 now reads SHIPPED.
```

Inventory's memory has been overwritten by notification's. This is exactly what
`PRIMARY KEY (group_id, order_id)` prevented in the Postgres table, so the group has to be
in the compaction key somehow.

**The obvious fix is the wrong one.** Keying the topic `group_id|order_id` makes
compaction correct — and breaks everything else, because the key is also what the
partitioner hashes:

| | compaction key | partition of order-42's fold | rebuild |
|---|---|---|---|
| `order_id`, one topic | order | same as its events ✓ | groups clobber each other ✗ |
| `group\|order`, one topic | (group, order) ✓ | **different** from its events ✗ | must read every partition ✗ |
| `order_id`, one topic **per group** | (group, order) ✓ | same as its events ✓ | one partition ✓ |

So the group goes in the **topic name**, never in the key. Kafka Streams does the same
thing for the same reason — one changelog per store per `application.id`.

## Co-partitioning, as something you can list

```
$ docker exec notification-consumer-1 ls -R /var/lib/order-state
/var/lib/order-state/notification-service:
0/  0.ckpt
```

That instance owns partition 0 and holds partition 0's keys. Nothing else. `order-42`'s
events are on `order-lifecycle-0`, its fold is on `order-fold.notification-service-0`, and
both are handled by this process — because all the topics have the same partition count,
the same key, and the same partitioner.

The store is a real RocksDB directory with a real lock:

```
IO error: lock hold by current process ... /var/lib/order-state/.../0/LOCK
```

That error is the feature, not a nuisance. **Only the process that owns a partition can
write that partition's state.** It is why the retry worker had to change (below).

## The write path, and the one ordering that matters

```
handler returns
  → store[order_id] = fold          local, immediate
  → produce(changelog, key, fold)   async, buffered
  → store.flush()                   ← blocks until the broker has it
  → consumer.commit(offset)
```

The flush is not an optimisation. It holds the invariant **changelog ≥ committed offset**:
a rebuild may know more than the offset says, never less. Reverse them and a crash leaves
the group committed past a fold that no rebuild can reproduce — silently.

`STATE_WRITE_ORDER=offset_first` deliberately does exactly that, which is why the lever
exists:

```bash
docker compose up -d --force-recreate \
  -e STATE_WRITE_ORDER=offset_first notification-consumer-1
```

Both writes are now Kafka operations. That is the setup for **008**: one transaction can
cover a produce and an offset commit, and cannot cover a produce and a database write.
Until then `handled_count` still outruns `last_sequence` and the residue is still visible:

```
DUPLICATE_ABSORBED order_id=… seq=4 stored_seq=4 handled=5
```

## The restore path

On assignment, before a single message is processed:

```
REBALANCE ASSIGNED partitions=[2] held=[]
RESTORED partition=2 records=1180 keys=94 from=0 to=1180 mode=full ms=412
```

`records` versus `keys` is this entire feature stated as two numbers. 1180 changelog
records compacted down to 94 live orders — the rebuild costs **keys**, not history. On a
`delete` topic that number could only grow.

Three details worth knowing:

**It blocks.** The rebuild runs inside the rebalance callback. A store rebuilt *after*
messages were processed is a store that missed them. The cost is that a long restore can
exceed `max.poll.interval.ms` and get the member evicted mid-rebuild — a real Kafka
Streams operational problem, and the reason the checkpoint mode exists.

**It joins no group.** The reader only ever calls `assign()`, never `subscribe()`. A
subscription would join a *second* consumer group and rebalance it, during a rebalance.
It commits nothing and leaves nothing in the coordinator.

**Tombstones are replayed as deletes.** A null value on the changelog removes the key
rather than restoring it, which is what stops every rebuild resurrecting every deleted
order.

### `full` versus `checkpoint`

```bash
# the default: replay the whole partition, every assignment
docker compose restart notification-consumer-1
docker compose logs notification-consumer-1 | grep RESTORED
#  RESTORED partition=0 records=1180 keys=94 from=0 to=1180 mode=full ms=412

# resume from where this store was left
STATE_REBUILD=checkpoint docker compose up -d --force-recreate notification-consumer-1
docker compose logs notification-consumer-1 | grep RESTORED
#  RESTORED partition=0 records=0 keys=0 from=1180 to=1180 mode=checkpoint ms=8
```

`full` is the default even though Kafka Streams checkpoints, because under `checkpoint` a
warm restart reads nothing and the number the feature exists to show is visible exactly
once. The checkpoint lives in `<partition>.ckpt` **beside** the store rather than inside
it — a store that will not open would otherwise take its own recovery marker down with it.

## Why the retry worker stopped writing state

It used to do this:

```python
store = self._stores[spec.name]      # a connection to the shared database
store.save(partition, order_id, ...)  # write another group's fold from here
```

That was only ever possible because Postgres accepts connections from anywhere. An
embedded store does not — the owning consumer holds the lock. So the worker became a pure
**scheduler**: it does the waiting, and when a message is due it puts it back on the topic
it came from.

```
retry topic ──due?──► republish to order-lifecycle ──► the owner folds it
                      headers: x-retry-target, x-attempt
```

Two headers make that safe, and both are load-bearing:

| header | what breaks without it |
|---|---|
| `x-retry-target` | the source topic fans out to **all three** groups, so the two that already handled the message successfully run their handlers again |
| `x-attempt` | the message arrives as a fresh attempt 1, fails again with the lever still armed, is scheduled again — and ping-pongs between the two topics forever |

The general principle is worth more than the mechanism: **with local state, work travels
to the state, not state to the worker.** Any design that wants a second process to update
a partition's state is a design that has not noticed the lock yet.

## What this does *not* close

**A deliberate replay from earliest still resurrects a deleted order.** Rebuilding from the
changelog fixes the restart and the rebalance, because the store's only source is now the
changelog. Resetting the group's offsets re-reads `order-lifecycle` and re-folds the
events, and the changelog tombstone only protects for `delete.retention.ms`. Genuinely
closing it needs an `ORDER_DELETED` terminal event on the log, which
[006](../specs/006-compaction-tombstones/requirements.md) placed out of scope and 007 did
not reclaim.

**The two writes are still two writes.** The changelog produce and the offset commit are
separate operations with a window between them. That is 008.

## Where this shows up again at 009

ksqlDB runs on Kafka Streams, which runs on RocksDB plus compacted changelog topics —
precisely what is hand-rolled here. When 009 starts and creates topics like:

```
_confluent-ksql-default_query_CTAS_ORDERS_5-Aggregate-GroupBy-repartition
_confluent-ksql-default_query_CTAS_ORDERS_5-Aggregate-Aggregate-Materialize-changelog
```

every part of that name is now readable: a repartition topic exists because a join needs
co-partitioning, and a `-changelog` topic exists because a materialised table is a local
RocksDB store that has to be recoverable. That recognition is why
[X6](../DECISIONS.md) put ksqlDB last.
