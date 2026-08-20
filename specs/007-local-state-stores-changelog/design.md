# 007 — Local State Stores and Changelog Topics: Design

Implements [requirements.md](requirements.md). Every decision cites the criteria it serves.
Cross-cutting choices live in [DECISIONS.md](../../DECISIONS.md): the client (X1), the wire
format (X2), the Postgres compromise this rung ends (X4), the endpoint it reaches (X5), and
the correction to X12 that D1 records as **X13**.

## Architecture

```
        order-lifecycle (delete)          order-snapshot (compact)
        key=order_id                      key=order_id
        value=LifecycleEvent              value=order │ null   ← tombstones only
              │                                 │
              └──────────────┬──────────────────┘
                             ▼
              inventory / notification / analytics     one group each,
                             │                          subscribed to both
              fold ──────────┤
                             ▼
        ┌────────────────────────────────────────────────────────┐
        │  LocalStateStore                                       │
        │    STATE_DIR/<group_id>/<partition>/       the store   │
        │    STATE_DIR/<group_id>/<partition>.ckpt   the mark    │
        └──────────┬──────────────────────────▲─────────────────┘
                   │ produce (async)          │ replay to high watermark
                   ▼                          │ on assignment
        order-fold.<group_id>  (compact, key=order_id, co-partitioned)
              value = {last_sequence, state, last_event_id, handled_count} │ null

        retry-worker ──backoff──► republish to order-lifecycle
                                   headers: x-retry-target, x-attempt
```

Six topics, one partition count. Co-partitioning was load-bearing from 006 D8 and is now
what makes `restore()` a single-partition read.

## Decisions

### D1 — One changelog topic per consumer group, keyed by `order_id` — *R7.1, R7.2*

The topic name carries the group; the key does not. `${STATE_CHANGELOG_PREFIX}.<group_id>`
gives `order-fold.notification-service`, derived from the same `group_id_for()` the store
and the offsets already use, so overriding `CONSUMER_GROUP_ID` moves the changelog with the
group rather than stranding it.

**Rejected.** *One topic keyed `order_id`* — compaction retains the latest value per key, so
the three groups would overwrite each other and every rebuild would read whichever group
wrote last. The groups genuinely differ: inventory can sit at `PACKED` while notification is
at `SHIPPED`. *One topic keyed `group_id|order_id`* — compaction becomes correct, but the
group enters the partition hash, an order's fold and its events land on different partition
numbers, and R7.7's single-partition replay is no longer possible. *Reusing `order-snapshot`*
— this is what [X12](../../DECISIONS.md) claimed 007 would do, and it is wrong: the snapshot
carries no per-group `last_sequence` and no `handled_count`, so bootstrapping from it merges
three memories R3.2 requires to stay separate. X13 records the correction; `order-snapshot`
keeps its 006 role unchanged.

### D2 — One store per owned partition, not one store per instance — *R7.3*

`STATE_DIR/<group_id>/<partition>/`. Partition first makes revocation a `close()` rather than
a range delete, makes `held()` a directory listing, and puts the exclusive lock at the same
granularity as ownership, so one partition moving cannot disturb the others. It is also what
Kafka Streams does — one store per task. Co-partitioned state becomes something `ls` prints.

**Rejected.** One store per instance keyed `<partition>|<order_id>` — revocation turns into a
prefix scan and delete, and the property being taught stops being visible on disk.

### D3 — The engine is `rocksdict`; the fallback was not needed — *R7.3*

[X5](../../DECISIONS.md) names `rocksdict` and asked for its maintenance status to be
re-checked on arrival. It was: **0.3.29 publishes a `cp313` manylinux 2.28 wheel**, which
installs into `python:3.13-slim` without a source build, so the stdlib `sqlite3` fallback this
section previously held open was never used.

The property the whole feature rests on is enforced by the engine rather than by convention —
a second open of a held directory fails outright:

```
IO error: lock hold by current process ... /var/lib/order-state/<group>/0/LOCK
```

That is what makes R7.12 a statement about physics rather than about discipline, and it is why
D10 had to change the retry worker instead of documenting a rule for it. The store still sits
behind the `StateStore` Protocol, so the engine remains swappable — it is simply not swapped.

### D4 — The changelog write is asynchronous; the flush before the commit is not — *R7.4, R7.11*

`save()` writes the local store, then `produce()`s the new fold and returns. The producer is
flushed before the offset commit, which is what R7.11's ordering actually costs: one flush per
committed message, not one round-trip per fold. The invariant it buys is **changelog ≥
committed offset** — a rebuild can be ahead of the offset, never behind it.

003's two levers survive unchanged in name and meaning. `STATE_WRITE_ORDER=offset_first` still
commits before the fold is durable and still loses it permanently on a crash;
`STATE_CRASH_AFTER=state_write` now fires after the flush. What changed is that both writes are
Kafka operations, which is exactly the pair 008's transaction covers.

**Rejected.** Blocking on each delivery report — a broker round-trip per event, where the
tombstone path in 006 D3 blocks only because a `204` is a claim. No flush at all — the local
store would lead the changelog, and a rebuild after a crash would silently regress state.

### D5 — The sequence guard moves from one SQL statement into Python — *R7.5*

003's `_UPSERT_FOLD` did guard, upsert and count atomically because a shared table can have
concurrent writers. Locally it becomes read-modify-write, which is safe for one reason:
**a partition has exactly one writer**, the invariant 002 established and R7.12 now states
outright. `handled_count` moves into the stored value and keeps counting every delivery, so
R3.13's residue survives as the number 008 drives to zero.

The read-through cache goes away entirely. After a restore the store *is* the warm copy, so
`PostgresStateStore`'s lazy-load-on-miss and its `forget()`-drops-the-cache contract both
disappear rather than being ported.

### D6 — `restore()` joins the Protocol and is called from `_on_assign` — *R7.7*

One method added, and `MemoryStateStore.restore()` is a documented no-op — that backend *is*
002's amnesia and R3.20 requires it to keep producing it.

`_on_assign` receives up to 2×N `TopicPartition`s because the subscription spans
`order-lifecycle` and `order-snapshot`, while the store is keyed by partition **number** alone
(006 D8). So the callback deduplicates to a set of numbers before restoring. Blocking inside
the rebalance callback is correct and deliberate: a store rebuilt after messages were already
processed is a store that missed them.

### D7 — The restore reader assigns, never subscribes — *R7.8*

A second `Consumer`, `enable.auto.commit=false`, a `group.id` distinct from the service's
(librdkafka requires the property to construct one) that is **never joined** because the reader
only ever calls `assign()`. Assignment without subscription performs no group join, so a
rebuild triggers no rebalance and leaves nothing in the coordinator. It seeks, reads to the
high watermark captured at the start, applies each record — null value means delete — and
closes.

The evidence line is `RESTORED partition=… records=… keys=… ms=…` at WARNING, alongside
`REBALANCE`, `VIOLATION` and `TOMBSTONE`, because `records` versus `keys` is the entire
feature stated as two numbers.

**Rejected.** Subscribing the reader — it would join a second group and rebalance it, during a
rebalance. Reusing the service's own consumer — it is mid-callback and cannot be reassigned.

### D8 — The checkpoint is a file beside the store, and `full` is the default — *R7.9*

`<partition>.ckpt` holds the changelog offset the store was last brought up to, written on a
clean `forget()` or `close()`. `STATE_REBUILD=checkpoint` seeks to it; `full` ignores it and
replays the partition from the beginning.

`full` is the default even though Streams checkpoints, because under `checkpoint` a warm
restart reads almost nothing and the feature's central number is visible exactly once, on a
cold start. The lever exists so the contrast can be measured rather than described.

**Rejected.** Keeping the mark inside the store — a store that will not open then loses its own
recovery marker. Committing offsets on the changelog — that is D7's rejected group join.

### D9 — Revocation closes; only a tombstone deletes — *R7.10*

`forget(partitions)` flushes the producer, writes the checkpoint, and closes the handle. The
directory stays, so a sticky reassignment back to this instance is nearly free under
`checkpoint`. `delete(order_id)` is the opposite and unchanged from 006 D6 — a tombstone is an
instruction to destroy — and now also publishes a null to the changelog so a rebuild does not
restore what was deleted.

### D10 — Retry-worker republishes, and two headers carry what a republished message loses — *R7.12, R7.13*

`FailureRouter.to_source()` is a third caller of the existing `_publish`, which already sends
original bytes plus headers. `retry_worker._succeed()` calls it instead of `store.save()`, and
the worker's `build_store` import, its `_stores` map and its `StateStore` dependency all go.

Two headers, because a republished message is otherwise indistinguishable from a fresh one:

- `x-retry-target` names the service the retry was for. `_handle_message` gains a branch of the
  same shape as 006's snapshot branch — log, commit, return — for a message targeted at
  another service. Without it, a republished event re-runs handlers in the two groups that
  already succeeded.
- The attempt count is read back and passed into `failures.maybe_fail`. Without it the message
  arrives as attempt 1 with the failure lever still armed, fails again, and ping-pongs between
  the two topics forever.

**Accepted cost.** The event log now contains messages the producer never wrote. **Rejected**
alternative: the consumers subscribe to the retry topic themselves, which keeps the log pristine
but puts the backoff wait inside the consume loop — the one thing 005 exists to keep out of it.

### D11 — Postgres leaves entirely, and R3.25 is retired — *R7.14*

`PostgresStateStore`, the four SQL constants, `redact_dsn`, `psycopg`, `state_schema.sql`,
`apply_state_schema.sh`, the `postgres` service, its volume, its healthcheck, its `depends_on`
entries, `STATE_DB_DSN` and the three `POSTGRES_*` credentials all go. `StateBackend.POSTGRES`
becomes `LOCAL`; `MemoryStateStore` is untouched.

**R3.25 — "idempotent schema creation for the durable store" — becomes unsatisfiable and is
retired here**, with the user's approval on record. Every other 003 criterion survives the
swap: R3.22's stop-on-unavailable, R3.23's backend banner, R3.24's no-credentials rule, R3.26's
host-or-compose parity and R3.27's defaults all hold against the new backend.

### D12 — What this rung does not close — *R7.15*

Rebuilding from the changelog stops a **restart or a rebalance** resurrecting a deleted order.
It does not stop a deliberate offset reset to `earliest`, which re-reads `order-lifecycle` and
re-folds the events; the changelog tombstone only protects within `delete.retention.ms`.
006's README rows are corrected to say so. Genuine closure needs the `ORDER_DELETED` terminal
event 006 placed out of scope, and this rung does not reclaim it.

## Environment surface

| Variable | Default | Read by | Criteria |
|---|---|---|---|
| `STATE_BACKEND` | `memory` (compose sets `local`) | consumers | R7.14 |
| `STATE_DIR` | `/var/lib/order-state` | consumers | R7.3 |
| `STATE_CHANGELOG_PREFIX` | `order-fold` | consumers, `create_topics.sh` | R7.1 |
| `STATE_REBUILD` | `full` | consumers | R7.9 |
| `FOLD_SEGMENT_MS` | `10000` | `create_topics.sh` | R7.1 |
| `FOLD_MIN_CLEANABLE_DIRTY_RATIO` | `0.01` | `create_topics.sh` | R7.1 |
| `FOLD_DELETE_RETENTION_MS` | `60000` | `create_topics.sh` | R7.1 |

Separate from the `SNAPSHOT_*` knobs rather than shared with them, because the two tombstone
windows protect different things: the snapshot's is how long a lagging consumer can still learn
of a delete (006), the changelog's is how long a rebuild is still told not to restore it (R7.6).

Removed: `STATE_DB_DSN`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`,
`POSTGRES_HOST_PORT`. Each consumer container gains its own named volume at `STATE_DIR` — five
volumes, because a shared one would reintroduce exactly the contention this feature removes.

## Known gaps, by intent

| Gap | Status |
|---|---|
| An offset reset to `earliest` still resurrects a deleted order | open by design (D12); needs `ORDER_DELETED` |
| A rebuild longer than `max.poll.interval.ms` evicts the member mid-restore | inherent; `STATE_REBUILD=checkpoint` is the mitigation, the doc names it |
| The changelog produce and the offset commit are still two operations | 008 |
| `handled_count` still exceeds `last_sequence` | 008 (D5) |
| The event log carries republished messages the producer never wrote | accepted (D10) |
| A lost volume with no changelog tombstone left restores a deleted order | inherent to `delete.retention.ms` |
| Everything still open from 004 and 005 | unchanged |

## Deferred to later specs

Transactions and exactly-once (008), stream SQL over the topics built here (009,
[X6](../../DECISIONS.md)).

## Budget

Criteria are within the [X11](../../DECISIONS.md) budget at 15. This file exceeds the 200-line
guidance, and the tasks list runs to 13 rather than roughly 12, because the feature is a
**replacement rather than an addition**: removing Postgres from code, dependencies, scripts and
compose is close to a third of the diff and carries no criteria of its own beyond R7.14.
