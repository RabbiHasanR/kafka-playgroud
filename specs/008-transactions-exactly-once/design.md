# 008 — Transactions and Exactly-Once Semantics: Design

Implements [requirements.md](requirements.md). Every decision cites the criteria it serves.
Cross-cutting choices live in [DECISIONS.md](../../DECISIONS.md): the client (X1), the Postgres
compromise that made the dual-write visible (X4), the changelog endpoint this rung joins to the
offset (X5, X13), and the exactly-once protocol D2 records as **X14**.

## Architecture

```
  ┌─ producer process (FastAPI) ──────────────────────────────────────────┐
  │  Producer  enable.idempotence=true      → order-lifecycle             │
  │            (NOT transactional — D2)     → order-snapshot              │
  └───────────────────────────────────────────────────────────────────────┘

  ┌─ consumer process (×3 services) ──────────────────────────────────────┐
  │                                                                       │
  │   Consumer  isolation.level=read_committed                            │
  │      │  subscribe: order-lifecycle, order-snapshot                    │
  │      ▼                                                                │
  │   ServiceConsumer ──── handler ────┐                                  │
  │      │                             │                                  │
  │      │  _commit(msg)               │ save(fold)                       │
  │      ▼                             ▼                                  │
  │   CommitStrategy            LocalStateStore ──► RocksDB (local disk)  │
  │      ├ DirectCommitter               │           ▲ outside the txn!   │
  │      │    consumer.commit()          │           └── discard+restore  │
  │      │                               │               on abort (D6)    │
  │      └ TransactionalCommitter        │                                │
  │           begin / send_offsets / commit                               │
  │                     │                │                                │
  │                     ▼                ▼                                │
  │   ┌──────────────────────────────────────────────────┐                │
  │   │  ONE Producer per process (D1)                   │                │
  │   │    transactional.id=<group_id>-<instance>  (D3)  │                │
  │   └───┬───────────────────────────────┬──────────────┘                │
  │       │ FailureRouter                 │ changelog                     │
  └───────┼───────────────────────────────┼───────────────────────────────┘
          ▼                               ▼
   order-lifecycle.retry          order-fold.<group_id>
   order-lifecycle.dlq            (compact, co-partitioned)
          │
          └─► retry-worker  read_committed, NOT transactional (open gap)
```

The shape of the change is not the transaction API — it is **producer ownership**. Everything
below follows from one producer where there were two.

## Decisions

### D1 — One producer per consumer process, shared by the store and the router — *R8.4*

`LocalStateStore` and `FailureRouter` each construct a `Producer` today. Every write inside one
transaction must come from one producer instance, so they must share. The producer is built in
`main.py` beside the store and the router — the same place, for the same stated reason: one
owner, constructed before the group is joined, closed in one place.

**The sharing is unconditional, not EOS-only.** Both current configs hardcode `acks=all`, and
`state.py`'s extra `partitioner=consistent_random` is librdkafka's default, so nothing is
compromised by merging them. Building one producer on both paths avoids a second construction
path that only the at-least-once run would exercise. `client.id` becomes `<group_id>-<instance>`
rather than the store's `<group_id>-changelog`.

**Rejected.** *A producer per component, shared only under EOS* — two wiring shapes, one of
which is untested on the default path. *Keeping construction inside the components and passing
a flag* — the components would each need to know about transactions, which is exactly the
coupling D4 exists to avoid.

### D2 — Exactly-once v2: one producer per instance, fenced by group metadata — *R8.3, R8.6*

Two protocols are available. **v1** gives each partition its own `transactional.id`, so
producers are created and destroyed inside `_on_assign`/`_on_revoke`. **v2** (KIP-447) keeps one
producer per instance and fences through the consumer group's generation, carried by
`consumer_group_metadata()` into `send_offsets_to_transaction`. The broker is 4.3.1, so v2 is
available; this feature uses it, and [X14](../../DECISIONS.md) records the choice.

v2 is chosen because per-partition producers would put producer construction inside the
rebalance callback that 007 D6 already made blocking for restore. A rebalance would then cost a
restore *and* a set of `init_transactions()` round trips, and the eviction risk R7.7's note
already warns about would get materially worse.

**Rejected.** *v1* — for the reason above, and because its per-partition identity is the thing
KIP-447 was written to remove. *No fencing at all* (a random identity per start) — the restarted
instance could not fence the zombie it replaced, which is the entire purpose of the identity.

### D3 — The identity is `<group_id>-<instance>`, and that promotes `CONSUMER_INSTANCE_ID` — *R8.3, R8.5*

`transactional.id` must be **stable across restarts** or fencing is decorative. Both halves are
already resolved in the process: `group_id_for()` gives the group, `CONSUMER_INSTANCE_ID` gives
the member. The consequence is that a setting which has been a log field since 002 becomes
correctness-critical — two members sharing one value will fence each other in a loop, each
bumping the epoch the other just took.

R8.5 is what makes that loud. `KafkaError._FENCED` / `_INVALID_PRODUCER_EPOCH` on any
transactional call is logged as `PRODUCER_FENCED` naming the identity, and the process exits 2.
Retrying is wrong by construction: a fenced producer can never un-fence itself.

**Rejected.** *Deriving from the hostname* — Compose gives containers stable names, but a
`docker compose up --scale` run does not, and the failure would be silent. *Deriving from a
UUID persisted to `STATE_DIR`* — survives restarts but not a lost volume, which is precisely
when a zombie is most likely to exist.

### D4 — `_commit()` stays the single seam; a `CommitStrategy` sits behind it — *R8.6, R8.7*

`_commit(message)` is already called from all five places an offset advances — the snapshot
branch, the not-ours retry branch, the tombstone path, `_persist_and_commit`, and
`_route_failure`. Rather than branch on the guarantee at five sites, the method delegates to one
injected object:

| | `DirectCommitter` | `TransactionalCommitter` |
|---|---|---|
| `note(message)` | `consumer.commit(message)` | record offset+1, mark the partition touched, `begin_transaction()` if none open |
| `maybe_commit()` | nothing | commit if the interval is reached |
| `abort(reason)` | nothing | `abort_transaction()`, return the touched partitions |

This is deliberately the same shape as `StateStore` at 003 D2 — one protocol, two
implementations, selected by one environment variable, with the pre-feature behaviour kept
intact as the control.

`TransactionalCommitter` needs the `Consumer` for `consumer_group_metadata()`, so it is
constructed inside `ServiceConsumer` (which owns the consumer) from the producer `main.py`
passes in. The producer crosses the boundary; the strategy does not.

### D5 — The interval is checked on empty polls too — *R8.8*

`TRANSACTION_COMMIT_INTERVAL_MESSAGES` or `TRANSACTION_COMMIT_INTERVAL_MS`, whichever arrives
first. The time check runs on the `message is None` branch of the poll loop as well, not only
after a message: without it a transaction opened by the last message of a burst stays open until
traffic resumes, and every `read_committed` reader downstream stalls at the last stable offset
for as long as the lull lasts. Kafka Streams checks on the same schedule for the same reason,
at a 100 ms default; 200 ms is used here because nothing in this repository is latency-bound and
a slower interval makes the LSO stall easier to watch.

`TRANSACTION_COMMIT_INTERVAL_MESSAGES=1` gives one transaction per message, which is what the
crash lever in D8 wants and what the throughput comparison measures against.

### D6 — An abort discards and rebuilds the partitions it touched, then seeks to committed — *R8.9, R8.11*

The transaction covers the changelog produce and the offsets. It does not cover the RocksDB
write, which has already happened and cannot be rolled back. So `abort()` returns the partitions
the transaction wrote to and the loop:

1. `store.discard(partitions)` — close the `Rdict`, delete the directory and its checkpoint.
2. `store.restore(partitions)` — rebuild from the changelog, which under `read_committed`
   contains only committed records.
3. `consumer.seek()` each partition to its **committed** offset — which is exactly where the
   aborted transaction began, so no start offset needs tracking.

**Only the touched partitions, not all held ones.** The transaction knows which partitions it
produced to; rebuilding partitions it never wrote would pay a full restore for nothing. The
existing `_rewind` helper is reused for step 3.

**A rebalance aborts too, and takes a shortcut (R8.9).** `_on_revoke` and `_on_lost` abort any
open transaction before releasing partitions, because submitting an offset for a partition the
member no longer owns is the failure `COMMIT_REJECTED` already exists to report — and inside a
transaction it would fence the producer rather than merely warn. No discard-and-rebuild is
needed there: `_forget()` is already releasing those stores, and whoever is assigned them next
rebuilds from the changelog under R7.7. Partitions the member *keeps* are discarded and rebuilt
per the three steps above.

**Rejected.** *Wiping all held partitions* — simpler, and it makes an abort cost proportional to
assignment rather than to work done. *Leaving the store dirty and relying on the sequence guard*
— the guard absorbs a fold that is too *old*, not one that is too *new*; an uncommitted fold is
ahead of the changelog and would silently swallow the redelivery that was supposed to redo it.
*Committing instead of aborting on revoke* — the offsets are for partitions already gone.

### D7 — Checkpoint rebuild is refused under EOS; `offset_first` is warned and ignored — *R8.12, R8.13*

`STATE_REBUILD=checkpoint` asserts the store already matches the changelog up to a recorded
offset. An aborted batch makes that false, and D6's rebuild has nothing to rebuild *to*. The
combination is refused at startup with both settings named, alongside 002's existing
protocol/setting validation, so a doomed process never joins the group (R3.21's rule).

`STATE_WRITE_ORDER` is a different case: under a transaction there is no order between the two
writes, so `offset_first` is not dangerous, only meaningless. It is logged as ignored rather
than refused — refusing would make the 003 lever look broken when it is merely subsumed.

### D8 — The crash point is `transaction_open` — *R8.13*

`StateCrashPoint` gains one member, firing after the fold is written and the offset noted and
**before** `commit_transaction()`. `os._exit(1)` for the reason 003 D5 already gives. What it
demonstrates: group lag stays non-zero, the changelog under `read_committed` has no record for
that event, the same record *is* visible under `read_uncommitted`, and on restart the redelivery
produces a fold with `handled_count == last_sequence` — the residue at zero.

`state_write` and `offset_commit` keep their 003 meanings on the at-least-once path and are
inert under EOS, where the states they name do not exist.

### D9 — Idempotence is one setting on every producer, defaulting on — *R8.1, R8.2*

`PRODUCER_IDEMPOTENCE=true` is the default and applies in all three places a producer is built.
It is kept as a setting rather than hardcoded so the pre-008 behaviour stays reachable as the
control R8.2 requires. Enabling transactions forces idempotence on regardless; the setting is
therefore only meaningful on the at-least-once path, and the startup line reports what is
actually in effect rather than what was asked for.

`acks` is untouched — `PRODUCER_ACKS` keeps its 004 role on the lifecycle producer, and the
changelog and failure producers keep hardcoding `acks=all` for the reason 007 gives.

### D10 — Isolation is one setting read by every consumer — *R8.10*

`CONSUMER_ISOLATION_LEVEL=read_committed` by default, applied in `build_consumer_config()`, in
the retry worker, and in `_changelog_reader()`. The restore reader is the one that matters for
correctness: at `read_uncommitted` a rebuild replays aborted folds and reintroduces exactly the
corruption D6 exists to repair. It is nonetheless left settable, because setting it to
`read_uncommitted` on a console consumer is the clearest demonstration in the feature — the
aborted records are physically on the topic, and a transaction is a marker over them rather
than their absence.

### D11 — What this rung does not close — *R8.15*

The retry worker gets `read_committed` and nothing else: it consumes, republishes and commits,
which is the same defect being fixed in the service consumers, left open by the scope decision
in `requirements.md`. The `POST /orders` boundary gets idempotence and no more. Handler
execution is still at-least-once, and `notification.py`'s customer message still prints twice on
a redelivery. The companion document names all three, and corrects the `README.md` line claiming
this rung drives `handled_count` to zero — it drives the **durable fold's** residue to zero.

## Environment surface

| Variable | Default | Read by | Criteria |
|---|---|---|---|
| `PROCESSING_GUARANTEE` | `at_least_once` | consumers | R8.3, R8.14 |
| `PRODUCER_IDEMPOTENCE` | `true` | all producers | R8.1, R8.2 |
| `CONSUMER_ISOLATION_LEVEL` | `read_committed` | consumers, retry worker, restore reader | R8.10 |
| `TRANSACTION_COMMIT_INTERVAL_MESSAGES` | `100` | consumers | R8.8 |
| `TRANSACTION_COMMIT_INTERVAL_MS` | `200` | consumers | R8.8 |
| `TRANSACTION_TIMEOUT_MS` | `60000` | the shared producer | R8.3 |
| `STATE_CRASH_AFTER` | gains `transaction_open` | consumers | R8.13 |

No broker settings change: `transaction.state.log.replication.factor` and
`transaction.state.log.min.isr` were pinned by 000 T5 and re-pinned by 004 T4, and
`TRANSACTION_TIMEOUT_MS` sits well under the broker's 15-minute `transaction.max.timeout.ms`.

## Known gaps, by intent

| Gap | Status |
|---|---|
| The retry worker's republish and commit are still two operations | open by design (D11) |
| A handler still runs twice on a redelivery; external side effects repeat | inherent — Kafka cannot cover them |
| `POST /orders` retried by the client still produces a second event | open; needs an idempotency key or an outbox, unclaimed |
| An abort pays a full restore for every partition it touched | inherent to state outside the transaction (D6) |
| `read_committed` stalls consumers at the LSO while a transaction is open | inherent; the interval is the lever |
| Everything still open from 004, 005 and 006 | unchanged |

## Deferred to later specs

Stream SQL over these topics (009, [X6](../../DECISIONS.md)), which supplies this guarantee as
one configuration line and is worth meeting only after the mechanism underneath is known.

## Budget

Criteria are at 15, the top of the [X11](../../DECISIONS.md) range. This file exceeds the
200-line guidance because the feature is two mechanisms rather than one — an idempotent producer
and a transactional cycle — plus a state-recovery rule (D6) that neither of them implies and
that no amount of configuration provides.
