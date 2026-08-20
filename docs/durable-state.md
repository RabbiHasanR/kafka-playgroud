# Durable consumer state

> Spec [003](../specs/003-durable-consumer-state/requirements.md). Companion to
> [order-flow.md](order-flow.md) (001) and [consumer-groups.md](consumer-groups.md) (002).
>
> **The Postgres backend this describes was replaced at
> [007](../specs/007-local-state-stores-changelog/requirements.md).** That was always the
> plan ([X4](../DECISIONS.md)): a database makes the dual-write problem impossible to miss,
> which is why it was chosen knowing it was the weaker option. The distinction this
> document draws — position versus memory — is unchanged and is why the rung exists. Where
> the memory *lives* is now [local-state-and-changelog.md](local-state-and-changelog.md),
> and `scripts/state_schema.sql` and `scripts/apply_state_schema.sh` no longer exist.

## The distinction the whole feature rests on

**Kafka remembers your position. Nothing remembers your memory.**

A committed offset says *"this group has read up to here."* It says nothing about what the
consumer **learned** on the way. Those are two different things, stored in two different
places, and the difference is invisible until something restarts.

|  | Position | Memory |
|---|---|---|
| What it is | the next offset to read | the folded `(last_sequence, state)` per order |
| Who stores it | Kafka, in `__consumer_offsets` | nobody, until this spec |
| Survives a restart | yes | **no**, before 003 |
| Survives a rebalance | yes — it belongs to the *group* | **no**, before 003 — it belonged to the *process* |

001 and 002 both left the right-hand column empty on purpose, and both said so in writing.
The consequence was a violation that never happened:

```
VIOLATION type=SEQUENCE_GAP order_id=ord-f20a44f238d5 seq=3 expected=1 observed=3
```

The consumer resumed at exactly the right offset. It just had no idea it had already seen
events 1 and 2 for that order.

## Two ways the memory was lost

**A restart.** The process dies, the offset survives, the fold does not (001 T35).

**A rebalance.** This one is worse, because nothing crashed. A partition is revoked from one
member and handed to another; the receiving member has never seen those orders, so its fold is
empty. 002 required this in R2.14 and R2.15 — a *routine scaling event* producing violations
indistinguishable from real corruption.

Both are fixed here by one change of shape.

## The load-bearing decision: key by the order, not by the partition

002 keyed folds **partition first**, because partition ownership was the lesson:

```python
self._folds: dict[int, dict[str, OrderFold]] = {}   # partition → order → fold
```

Carrying that into a table would have recreated the failure durably — a partition moving would
strand rows behind a key nobody looks under any more. So the partition leaves the key entirely:

```sql
PRIMARY KEY (group_id, order_id)
```

The fold stops belonging to *whoever holds the partition* and starts belonging to *the order*.
That is the entire mechanism. `group_id` is in the key so the three services' memories stay as
independent as their offsets are — inventory's row for an order and notification's row for the
same order are different rows.

The in-process cache stays, but it is now genuinely a cache. On a revocation:

```python
def forget(self, partitions):
    for partition in set(partitions):
        self._cache.pop(partition, None)   # and NO delete
```

**That missing `DELETE` is the whole difference between 002 and 003 at a rebalance.** The same
callback fires, the same partitions are dropped, and the record survives because the record was
never in the process.

### Why a cache is safe here

A partition belongs to exactly one member at a time — which is what 002 spent a whole feature
establishing — so the owner's cached folds cannot go stale. Nobody else is writing those orders.

The exception is eviction: `on_lost` fires for a member that may already have been replaced, and
a zombie can finish a handler and write after its replacement moved on. The sequence guard below
makes that write a no-op, which is why the guard is load-bearing and not a nicety.

### Why the read is lazy

State is read on cache miss, one order at a time — never warmed on assignment. Warming would
cost a scan proportional to *history* at every rebalance. That cost is exactly what spec 007
removes with a compacted changelog, and paying it here would hide the problem 007 solves.

## The dual-write problem

Once the fold is durable, there are **two writes to two systems**:

```
  handler runs
       │
       ├──①  fold  ──► Postgres
       │
       └──②  offset ──► Kafka
```

No operation covers both. Anything can happen between ① and ②, and that gap cannot be closed
here — only chosen. The order is a lever, and the default is not a preference:

| Order | Crash in the gap | Result |
|---|---|---|
| **state → offset** (default) | state written, offset not | event redelivered, write absorbed, **handler runs twice** |
| offset → state | offset committed, state not | event never redelivered, **fold permanently missing it** |

`STATE_WRITE_ORDER=offset_first` exists so the second row can be watched rather than believed.
It ends with a consumer that is working perfectly, logging nothing unusual, and permanently
wrong — the only `WARNING` lines in the observed run were two ordinary rebalance messages.

### Absorbing the duplicate

Every event already carries a monotonic per-order `sequence`, so it is already an idempotency
token. No dedup table, no new identifier — one statement:

```sql
last_sequence = GREATEST(order_fold.last_sequence, EXCLUDED.last_sequence),
state         = CASE WHEN EXCLUDED.last_sequence > order_fold.last_sequence
                     THEN EXCLUDED.state ELSE order_fold.state END,
handled_count = order_fold.handled_count + 1
```

The asymmetry is the design. **Fold columns move only on a higher sequence. `handled_count`
increments every time.** One statement under autocommit is its own transaction — no
read-modify-write, no `SELECT … FOR UPDATE`, no application lock. The guard lives in the
database, where concurrency is the database's problem.

A full replay of the topic against populated state is therefore a no-op. Observed: 229 records
re-consumed, **0 fold changes, 0 violations, 229 duplicates absorbed.**

### What this does *not* fix

```
DUPLICATE_ABSORBED order_id=ord-1ecb94d6c169 seq=1 stored_seq=1 handled=2
```

`last_sequence` is 1. `handled_count` is 2. One event, two deliveries — and **the customer got
two notifications.** Durable state fixed the state; it did not fix the side effect, and it
cannot, because the handler already ran before the redelivery was recognisable.

That gap between "the state is correct" and "the work happened once" is not a caveat here, it is
a number in a column. Spec 008 is where it goes to zero.

### A redelivery is not a violation

Durable memory introduces a false positive that neither 001 nor 002 could produce. A redelivered
event has a sequence *behind* the stored one, so the fold logic sees it as out of order and
reports a gap and an illegal transition. Nothing was wrong with the data.

So a delivery at or behind the stored sequence is diagnosed as a duplicate **before** folding,
and reported as `DUPLICATE_ABSORBED` rather than `VIOLATION`. Otherwise this feature would have
removed one class of false positive and quietly introduced another, and `grep VIOLATION` — which
001 designed as the whole filtering story — would be useless on any consumer that has ever been
redelivered anything.

Detection power is unchanged, in both directions:

- a forced illegal transition is still reported;
- a consumer that **genuinely** lost an event still reports the gap when the next one arrives,
  while a consumer that did not lose it reports nothing about the same event.

## Failing loudly

| Failure | Response | Why |
|---|---|---|
| Commit rejected — partition moved (002) | log, **continue** | a membership fact; the new owner redoes the work |
| Store unreachable at startup | log, **exit 2, never join** | a member that joins and then dies has already cost a rebalance |
| Store unreachable mid-run | log `STATE_STORE_UNAVAILABLE`, **exit 1** | continuing would commit offsets for events whose state was never written — the `offset_first` failure, arriving by accident |

The startup error names the address and **redacts the password**; an unredacted DSN in an error
line puts the credential into every log aggregator that ever sees it.

## Running it

```bash
cp .env.example .env          # set POSTGRES_USER / PASSWORD / DB — no defaults are supplied
docker compose up -d                          # postgres included; schema applied on first boot
docker compose --profile scale-out up -d      # three notification members
```

The schema is one file, `scripts/state_schema.sql`, used two ways. It is mounted into the
container's `/docker-entrypoint-initdb.d/` **and** applied by `scripts/apply_state_schema.sh`.
Both exist because the mount runs *only when the data volume is empty* — after the first `up`,
editing the schema and running `up` again does nothing at all, silently.

Inspect the memory directly:

```bash
docker compose exec postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -c "SELECT group_id, order_id, last_sequence, state, handled_count FROM order_fold
      ORDER BY updated_at DESC LIMIT 10;"

# the residue: rows handled more often than they have events
  -c "SELECT * FROM order_fold WHERE handled_count > last_sequence;"
```

### The environment surface

| Variable | Default | Effect |
|---|---|---|
| `STATE_BACKEND` | `memory` | `memory` \| `postgres` |
| `STATE_DB_DSN` | unset | required when the backend is `postgres` |
| `STATE_WRITE_ORDER` | `state_first` | `state_first` \| `offset_first` |
| `STATE_CRASH_AFTER` | `none` | `none` \| `state_write` \| `offset_commit` |

**The default is `memory`, deliberately.** A consumer started with none of these behaves exactly
as 002 recorded, so 001's and 002's experiments stay reproducible without a database. Compose
turns it on. The startup banner names the backend in force, which is what stops that default
from being a trap:

```
[inventory/host-crash-lab] state_backend=postgres write_order=state_first crash_after=none
```

### Watching the fix work

```bash
# 003: the inheriting member reports nothing
docker compose --profile scale-out up -d
scripts/place_orders.sh 9
docker stop notification-consumer-2

# 002's behaviour, one variable apart
STATE_BACKEND=memory docker compose --profile scale-out up -d
```

Observed side by side on the same rebalance: **0 of 3 orders reported a gap on Postgres, 4 of 4
on memory.**

## Accepted limitations

| Limitation | Closed by |
|---|---|
| Duplicate side effects after a crash in the dual-write window (`handled_count > last_sequence`) | 008 |
| Offset and state cannot be written atomically | 008 |
| State is in a shared database, not co-partitioned with the input | 007 |
| Rebuild cost grows with history rather than with key count | 007 |
| No dedup on `event_id`; idempotency rides on `sequence` | 008 |
| The producer's `OrderStore` is still lost on restart | — (transactional outbox; no spec claims it) |
| The shared-database bottleneck is described, never measured | — (needs a load generator this ladder excludes) |

### The thing you would reach for, and why we are not

The obvious way to close the dual-write gap is to **store the offset in Postgres too**, in the
same transaction as the state, and `seek()` to it on assignment. One transaction, both writes,
no duplicates.

It is deliberately not done here. That unclosed gap is what gives spec 008 its payoff, where the
state and the offset both become *Kafka* operations under one transaction — and the argument for
that only lands if the problem was felt first. Spec 007 removes a different half of the same
discomfort by making state local and co-partitioned.
