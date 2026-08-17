# 003 — Durable Consumer State: Design

Implements [requirements.md](requirements.md).
Cross-cutting choices that outlive this feature are recorded in
[../../DECISIONS.md](../../DECISIONS.md) as `X<n>` and referenced from here.

This feature is the one [X4](../../DECISIONS.md) reserves at 003, and it adds **no new `X`
entry**. X4 already fixes the store (Postgres), already predicts its replacement at 007, and
already states the consequence this design is built around — that at-least-once duplicates
"cannot be eliminated, only absorbed by idempotent upserts". Nothing below contradicts it, so
every decision here is per-feature and numbered `D<n>`.

## Architecture

```
                     compose network "kafka-playground_default"
  ┌────────────────────────────────────────────────────────────────────────────────┐
  │                                                                                │
  │  order-service (FastAPI) :8010          kafka :19092                           │
  │   OrderStore: IN MEMORY (D12)  ─ produce ─►  order-lifecycle                    │
  │                                              ├── p0  ├── p1  ├── p2             │
  │                                                    │                            │
  │        ┌───────────────────────────────────────────┼──────────────┐             │
  │        │                     │                     │              │             │
  │  inventory-consumer   analytics-consumer    notification-consumer-1,-2,-3       │
  │  group=inventory-     group=analytics-      group=notification-service          │
  │        service              service         (3 members, 1 partition each)       │
  │        │                     │                     │                            │
  │        │  ①  offset ─────────┴─────────────────────┴──► __consumer_offsets      │
  │        │                                                    (KAFKA)             │
  │        │  ②  fold                                                               │
  │        └──────────────────────┬─────────────────────────────────────────┐       │
  │                               ▼                                         │       │
  │                     postgres :5432  ── order_fold ──────────────────────┘       │
  │                     PRIMARY KEY (group_id, order_id)   ← NOT partition (D1)     │
  │                                                                                │
  └────────────────────────────────────────────────────────────────────────────────┘

   ① and ② are two writes to two systems with no operation covering both.
   That gap is this feature's subject, not its accident (D4, D5).
```

**The picture to hold onto:** 001 and 002 had one arrow leaving each consumer — the offset.
003 draws a second one, to a different system. Everything interesting in this feature is a
consequence of there being two arrows and no way to draw a box around both.

The producer, `events.py`, `order-lifecycle`, its three partitions, `create_topics.sh`, and
every setting 002 introduced are **untouched**.

## Decisions

### D1 — The durable record is keyed by `(group_id, order_id)`, and partition is not in the key — *R3.1, R3.2, R3.3, R3.7, R3.8*

002 D6 keyed folds **partition first**, because partition ownership was the thing on display
and a revocation had to be one `del`. Carrying that shape into the table would recreate,
durably, the exact failure this feature exists to remove: a partition moving between members
would strand its rows behind a key the new owner does not look under.

So the partition leaves the key entirely:

```sql
PRIMARY KEY (group_id, order_id)
```

`group_id` is in the key because R3.2 requires the three services' memories to stay
independent, exactly as their offsets are. One table, three logical namespaces — inventory's
row for `ord-abc` and notification's row for `ord-abc` are different rows, and one service
falling behind or replaying cannot disturb the other. This is 001 D7's fan-out property
surviving the move into shared storage, and it is the reason the group id must be part of the
key rather than a filter applied afterwards.

**The partition is not stored at all**, not even as a non-key column. It is derivable from
`order_id` by the same hash the producer used, so storing it duplicates a fact — and a
duplicated fact is one that can disagree. After a partition-count change the stored number
would be silently wrong while the derived one stayed right, which is the worst shape for a
column that exists only for convenience.

*Rejected:* `(group_id, partition, order_id)`. It makes "which orders live on partition 2" a
cheap query, and it costs the entire feature — after a rebalance the new owner queries under a
partition it now holds and finds rows written under an ownership that has moved on.

### D2 — A `StateStore` protocol with two backends; `apply_event()` is untouched — *R3.6, R3.19, R3.20*

The consume loop does not learn SQL. A new module `consumer/state.py` defines:

```python
class StateStore(Protocol):
    def load(self, partition: int, order_id: str) -> OrderFold | None: ...
    def save(self, partition: int, order_id: str,
             fold: OrderFold, event_id: str) -> SaveOutcome: ...
    def forget(self, partitions: Iterable[int]) -> None: ...
    def held(self) -> list[int]: ...
    def close(self) -> None: ...
```

**Amended during implementation, twice.** `OrderFold` lives in `state.py`, not in
`runtime.py` as first written: `PostgresStateStore` builds one from a database row, so it
needs the class at runtime and the import can only point one way — `runtime` → `state`.
And `save()` takes `event_id` alongside the fold, because the table records which event
last advanced a row (001 D11) while `apply_event()` does not track it; passing it beside
the fold is what keeps that function untouched, which R3.6 depends on. `held()` was added
for the rebalance log line, which used to read `self._folds` directly.

with `MemoryStateStore` (002's `dict[int, dict[str, OrderFold]]`, lifted out of
`ServiceConsumer` unchanged) and `PostgresStateStore`. `ServiceConsumer._folds` and
`_drop_folds` disappear into calls on an injected store.

`SaveOutcome` is a frozen dataclass carrying `applied: bool` and `handled_count: int` —
enough for the runtime to log a redelivery without knowing why it was one.

**The existing `apply_event()` is reused verbatim.** It already takes `OrderFold | None` and
returns a new fold plus a violation list — precisely the shape a store-backed loop needs, and
the reason this is a small change to `runtime.py` rather than a rewrite. `OrderFold` also
stays as it is: `handled_count` is store bookkeeping and deliberately not a field on the
domain fold, so the pure function has no opinion about persistence.

Lifting the memory backend out **verbatim** is what R3.20 is asking for. 001's and 002's
recorded results only reproduce against that code path; a "tidied up while moving it" version
would make every earlier observation unverifiable, and a spec whose evidence cannot be re-run
has to be trusted instead of checked.

### D3 — The cache stays, and it is now genuinely a cache — *R3.4, R3.9, R3.10*

`PostgresStateStore` keeps 002's partition-keyed dict in front of the table. It is safe for a
reason worth stating: **a partition belongs to exactly one member at a time**, so the owner's
cached folds cannot go stale — nobody else is writing those orders. The cache is not an
optimisation bolted on; it is licensed by the ownership property 002 spent a whole feature
establishing.

`forget()` therefore drops cache entries and **issues no `DELETE`**. That one line is the
whole difference between 002 and 003 at a rebalance: the same callback runs, the same
partitions are dropped, and the record survives because the record was never in the process.

Reads are **lazy, on cache miss**, not warmed on assignment. Warming would mean selecting
every order ever seen on that partition at every rebalance — an unbounded cost that grows with
history, which is exactly the cost 007 removes with a compacted changelog. Paying it here
would hide the problem 007 solves. Lazy reads cost one `SELECT` per order on first touch.

**Where the ownership argument breaks, and what covers it.** `_on_lost` fires for a member
that has already been replaced — 002's eviction experiment produces exactly that. A zombie
member can finish a handler and write *after* the new owner has moved the order forward. The
upsert's sequence guard (D6) makes that write a no-op, which is why the guard is load-bearing
rather than a nicety: it is the only thing standing between an evicted member and the state of
a partition it no longer holds.

### D4 — The state write goes before the offset commit, and the order is a lever — *R3.5, R3.16, R3.18*

| Order | Crash in the gap | Result |
|---|---|---|
| **state → offset** (default) | state written, offset not committed | event redelivered; fold write is a no-op (D6); handler runs twice |
| offset → state | offset committed, state not written | event never redelivered; **fold is permanently missing it** |

The second is not "slower" or "less safe in some edge case" — it loses data forever and logs
nothing while doing it. That is why R3.5 states the ordering as a requirement and R3.16 makes
it switchable: an ordering that is merely asserted is an ordering nobody has watched fail.
Running experiment 5 once is worth more than the paragraph above.

The default is state-first, so a consumer configured with none of this feature's settings
behaves correctly rather than merely compatibly.

### D5 — The crash lever, and why it must be `os._exit()` — *R3.15, R3.17*

`STATE_CRASH_AFTER` selects a point at which the process dies immediately: `none` (default),
`state_write`, or `offset_commit`. This is 003's counterpart to 001's `force: true` and 002's
`HANDLER_DELAY_SECONDS`, and it exists for the same structural reason — the window between the
two writes is microseconds wide and cannot be hit by hand, so without a deliberate crash point
R3.17 and R3.18 are unreachable and the dual-write problem stays a claim.

**It must be `os._exit(1)`, not `sys.exit()` or an exception.** Anything that unwinds the
stack runs the `finally` in `run()`, which calls `Consumer.close()` — a *graceful* departure
that leaves the group cleanly. That is a shutdown, not a crash, and it would produce a
politer rebalance than the failure being simulated. `os._exit()` skips `finally`, `atexit`,
and buffer flushing, which is what a `SIGKILL` or a segfault actually does.

The lever is per-process and off by default, so it is set on one instance while the other two
stay honest — the same shape as 002's eviction experiment.

### D6 — Idempotency comes from the sequence already on the event — *R3.11, R3.12, R3.13*

Every event carries a monotonic per-order `sequence` (R1.3). That is already an idempotency
token, so this feature adds no dedup table and no new identifier. The whole mechanism is one
statement:

```sql
WITH previous AS (
    SELECT last_sequence FROM order_fold
     WHERE group_id = %(group_id)s AND order_id = %(order_id)s
), upserted AS (
    INSERT INTO order_fold (group_id, order_id, last_sequence, state, last_event_id, handled_count)
    VALUES (%(group_id)s, %(order_id)s, %(sequence)s, %(state)s, %(event_id)s, 1)
    ON CONFLICT (group_id, order_id) DO UPDATE SET
        last_sequence = GREATEST(order_fold.last_sequence, EXCLUDED.last_sequence),
        state         = CASE WHEN EXCLUDED.last_sequence > order_fold.last_sequence
                             THEN EXCLUDED.state ELSE order_fold.state END,
        last_event_id = CASE WHEN EXCLUDED.last_sequence > order_fold.last_sequence
                             THEN EXCLUDED.last_event_id ELSE order_fold.last_event_id END,
        handled_count = order_fold.handled_count + 1,
        updated_at    = now()
    RETURNING last_sequence, handled_count
)
SELECT upserted.last_sequence, upserted.handled_count,
       COALESCE(previous.last_sequence, 0) AS previous_sequence
  FROM upserted LEFT JOIN previous ON true;
```

**The `previous` CTE was added during implementation**, and the first version was wrong
without it. `applied` was derived as "the row now carries the sequence we sent" — which is
also true of an *exact* redelivery, because `GREATEST(n, n)` is `n`. The one case the
feature exists to detect was the one case it would have missed. Inside `DO UPDATE` the
qualified name `order_fold.last_sequence` is the old value, but in `RETURNING` it is the
new one, so the pre-write sequence is otherwise unreachable. Every CTE in one statement
sees the same snapshot, so `previous` reads the row as it stood before the upsert — and it
is **still one statement**, which is the part the paragraph below depends on.

Two things happen in that one statement, and their asymmetry is the design:

- The **fold columns** move only when the incoming sequence is higher. A replay loses to
  itself and changes nothing (R3.11), which makes a full replay-from-earliest a no-op
  (R3.12).
- **`handled_count` increments unconditionally** (R3.13). It counts deliveries, not
  advances.

`applied` is derived by the store: the returned `last_sequence` equals the sequence we sent
exactly when our write won.

One statement matters. Under autocommit it is its own transaction, so there is no
read-modify-write, no `SELECT … FOR UPDATE`, no application lock, and no window in which two
members could interleave. The guard is in the database, where concurrency is the database's
problem.

**The sequence is authoritative even under `force`.** 001's escape hatch advances the sequence
while refusing to advance the recorded state, so a forced event is still strictly increasing
and still guarded correctly here. Idempotency-by-sequence does not quietly depend on the
producer being well behaved in the one way it is allowed not to be.

*Rejected:* a `processed_events(event_id)` table. It is the textbook answer, it gives R1.4's
`event_id` its first reader, and it costs a second mechanism doing an overlapping job plus an
unbounded table needing a retention policy this repository has no reason to design. Where
duplicate *side effects* are actually eliminated is 008; a dedup table here would look like it
had already solved that.

### D7 — `handled_count` is how this feature admits what it did not fix — *R3.13, R3.14*

Durable state fixes the **state**. It does not fix the **side effect**: after a crash in the
dual-write window the customer is notified twice and the stored fold is correct both times.

Without a number, that is a caveat in a document that nobody re-reads. With one, experiment 4
ends with `last_sequence=3, handled_count=4` on a row that received three events — the residue
of the dual-write problem, on screen, in one query. That number is what 008 exists to drive to
zero.

It lives on the same row as the fold, written by the same statement, on purpose: a separate
counter table would be a *second* dual-write inside the feature about dual-writes.

The runtime also logs a `DUPLICATE_ABSORBED` line when `SaveOutcome.applied` is false. No
requirement mandates that marker — it is an observability aid for R3.13, in the same shape as
002's `COMMIT_REJECTED`, and it is named here so it is not mistaken for scope nobody asked for.

### D8 — The backend defaults to `memory`, and the banner is what stops that being a trap — *R3.19, R3.20, R3.23, R3.27*

`STATE_BACKEND` is `memory` (default) or `postgres`.

Defaulting to `memory` is uncomfortable — this feature's whole point is off unless asked for —
and it is what R3.27 and R3.20 jointly require. A consumer started from the host with no new
settings must behave as 002 recorded, or 001's and 002's experiments stop reproducing. Compose
sets `STATE_BACKEND=postgres` explicitly for all five consumers, so the assembled system runs
the durable path; only a bare `python -m order_service.consumer.main` gets the old one.

The trap this could set — running an experiment on the wrong backend and reporting it
confidently — is closed by R3.23: the startup banner names the backend in force, beside the
protocol and assignor 002 already put there. The same reasoning as 002 D4, and the same fix.

### D9 — Failing loudly beats degrading quietly — *R3.21, R3.22*

**At startup** (R3.21): `PostgresStateStore` connects and verifies its table before the
`Consumer` is constructed, and raises `StateStoreUnavailable` naming the backend and a
**redacted** DSN. Redaction is not politeness — an unredacted DSN in an error line puts the
password in every log aggregator that ever sees it.

Constructing before the client mirrors 002 D4's ordering: a process that cannot honour R3.5
must never join the group, because a member that joins and then dies has already caused a
rebalance.

**Mid-run** (R3.22): a store failure is logged with a stable `STATE_STORE_UNAVAILABLE` marker
and then re-raised, ending the consume loop and exiting non-zero.

This is the deliberate opposite of 002 D8, where a rejected commit is survived and consumption
continues. The two look inconsistent and are not:

| Failure | Response | Why |
|---|---|---|
| Commit rejected — partition moved (002 D8) | log, continue | a membership fact; the new owner will redo the work |
| State store unreachable (D9) | log, stop | R3.5 can no longer be honoured; continuing would commit offsets for events whose state was never written — silent, permanent, and exactly R3.18's failure mode arriving by accident |

### D10 — psycopg 3, raw SQL, one connection, autocommit — *R3.24*

`psycopg[binary]` v3. No ORM and no query builder: the upsert-with-guard in D6 **is** the
idempotency mechanism, and any layer that generates it hides the one thing worth reading.

One connection per consumer process, `autocommit=True`. The consume loop is single-threaded —
established in 001 D5 and `docs/concurrency-and-confluent-kafka.md` — so a pool would be three
moving parts guarding against concurrency that does not exist. Autocommit is correct rather
than lazy: every write is exactly one statement, so an explicit `BEGIN`/`COMMIT` would wrap a
transaction the database was already giving us.

*Rejected:* SQLAlchemy Core — familiar, and it would put a translation layer over the four
lines that carry the lesson. A connection pool — see above; it returns at 007 only if the
store stops being per-process.

### D11 — One DDL file, two entry points — *R3.25*

`scripts/state_schema.sql` holds the schema, with `CREATE TABLE IF NOT EXISTS`. It is used
twice:

- mounted into the Postgres container at `/docker-entrypoint-initdb.d/`, so a first
  `docker compose up` needs no extra step;
- applied by `scripts/apply_state_schema.sh` for host runs, re-runs, and the case below.

Both paths, one file — the compose schema and the host schema cannot drift, which is the same
reasoning as 002 D2's YAML anchor.

**The caveat that makes the second entry point necessary:** `/docker-entrypoint-initdb.d/` runs
**only when the data volume is empty**. Editing the DDL and running `docker compose up` again
does nothing at all, silently. `apply_state_schema.sh` is the answer, and the README section
must say so, because "I changed the schema and nothing happened" is otherwise a half-hour.

### D14 — A redelivery is not a violation — *R3.6, R3.14*

**Added during implementation, from what T21 showed.** Durable memory introduces a false
positive that neither 001 nor 002 could produce: a redelivered event has a sequence at or
*behind* the stored one, so `apply_event()` reports both a `SEQUENCE_GAP`
(`expected=2 observed=1`) and an `ILLEGAL_TRANSITION` (`ORDER_CREATED after CREATED`).

Nothing was wrong with the data. We saw it twice, which is what at-least-once delivery is
entitled to do. Reporting that under 001's `VIOLATION` marker would mean this feature
removed one class of false positive and quietly introduced another — and R3.6 asks for
*genuine* violations, which this is not.

So the consume loop compares the incoming sequence against the stored fold **before**
folding, and suppresses the diagnosis when the event has already been applied. The handler
still runs and the delivery is still counted (R3.14); only the violation lines are
withheld, replaced by `DUPLICATE_ABSORBED`.

This costs nothing in detection power, which T26 checks in both directions: a forced
illegal transition is still reported, and a consumer that *genuinely* lost an event still
reports the gap when the next one arrives — while a consumer that did not lose it reports
nothing about the same event.

*Rejected:* leaving the violations in and explaining them in the documentation. It makes
`grep VIOLATION` — which 001 R1.41 designed as the whole filtering story — useless on any
consumer that has ever been redelivered anything.

### D15 — One image tag for every service that builds from this repository

**Added during implementation, after it produced a false result.** Each compose service
carried a bare `build: .`, so compose derived an image name *per service* — and
`docker compose build` skips services behind a profile. `notification-consumer-2` and `-3`
therefore kept running spec 002's code while `-1` ran 003's, in the same consumer group,
with nothing to show for it but a missing banner line. The first T19 run reported four
sequence gaps that meant nothing.

Every service now shares `image: kafka-playground:local` through one anchor, so a stale
member is not expressible. This also makes the Dockerfile's opening claim — *"one image for
the order service and all three consumers"* — true, which it had not been since 002 split
notification into three services.

It is recorded as a decision rather than a fix because the failure mode is invisible: two
members of one group silently running different specifications is exactly the kind of thing
that makes an experiment lie, in a repository whose entire output is observations.

### D12 — What the table deliberately does not contain

| Not stored | Why |
|---|---|
| `partition` | derivable from the key; a stored copy can disagree (D1) |
| consumer offsets | X4 — moving them here would close the dual-write gap that 008 needs open; named in the docs as the thing not being reached for |
| per-event history | the topic is the event log. This row is a *fold* — one row per order, not per event. Storing events would make Postgres a worse Kafka |
| the producer's orders | `OrderStore` stays in memory. A projection is made durable; the source of truth stays the log (requirements, Notes) |

### D13 — Credentials come from the environment and compose fails without them — *R3.24, R3.26*

`docker-compose.yml` builds one DSN in a YAML anchor and merges it into all five consumers:

```yaml
x-state-db-env: &state-db-env
  STATE_BACKEND: postgres
  STATE_DB_DSN: postgresql://${POSTGRES_USER:?set POSTGRES_USER in .env}:${POSTGRES_PASSWORD:?set POSTGRES_PASSWORD in .env}@postgres:5432/${POSTGRES_DB:?set POSTGRES_DB in .env}
```

The `:?` form is chosen over `:-default`: a default password *is* a hardcoded credential, just
one with a friendlier shape, and the repository rule says environment variables only. A
missing value fails at `docker compose up` with a message naming the variable. `.env` is
already gitignored and `.env.example` already whitelisted; this feature adds the example file.

A single DSN rather than five discrete settings keeps psycopg's own parser in charge and gives
compose one string to build instead of five to keep consistent.

**Host and compose differ by that one string** (R3.26). The Postgres container publishes
`5432` on the host exactly as the broker publishes `9092` — through
`${POSTGRES_HOST_PORT:-5432}`, because a developer machine very often already runs Postgres
there and a port clash should cost one `.env` line rather than the ability to inspect the
database from outside compose. The *container* side never moves, so `STATE_DB_DSN` is
unaffected. A host-run consumer uses `@localhost:<host port>` and a compose-run consumer
uses `@postgres:5432`, with nothing else changing
— the same shape R1.44 and R2.35 established for `localhost:9092` versus `kafka:19092`. Both
reach the same database, so an experiment can be started in compose and inspected with `psql`
from the host.

## Schema

`scripts/state_schema.sql`:

```sql
CREATE TABLE IF NOT EXISTS order_fold (
    group_id       text        NOT NULL,
    order_id       text        NOT NULL,
    last_sequence  integer     NOT NULL,
    state          text,
    last_event_id  text        NOT NULL,
    handled_count  integer     NOT NULL DEFAULT 0,
    updated_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (group_id, order_id)
);
```

`state` is nullable and `text` rather than an enum: it mirrors `OrderFold.state`, which is
`OrderState | None`, and a database enum would be a second copy of `events.py`'s transition
vocabulary needing a migration every time the lifecycle grows. The application owns the
contract (X2's reasoning, applied to storage).

The one query that reads and the one that writes are both primary-key operations, so no
secondary index is added. The `handled_count > last_sequence` inspection experiment 4 needs is
a full scan of a table with tens of rows.

## Module layout

```
src/order_service/
├── config.py                # + STATE_BACKEND, STATE_DB_DSN, the two levers   D5, D8, D13
├── events.py                # UNCHANGED
├── producer/                # UNCHANGED — OrderStore stays in memory          D12
└── consumer/
    ├── state.py             # new: StateStore protocol, Memory + Postgres     D2, D3, D6
    ├── runtime.py           # folds → injected store; write order; crash lever D2, D4, D5
    ├── main.py              # builds the store, closes it on exit             D8, D9
    ├── inventory.py         # UNCHANGED
    ├── notification.py      # UNCHANGED
    └── analytics.py         # UNCHANGED
scripts/
├── create_topics.sh         # UNCHANGED
├── place_orders.sh          # UNCHANGED
├── state_schema.sql         # new                                             D11
└── apply_state_schema.sh    # new                                             D11
docs/
└── durable-state.md         # new                                             R3.28
docker-compose.yml           # + postgres service, state env anchor            D11, D13
.env.example                 # new                                             D13
pyproject.toml               # + psycopg[binary]                               D10
```

The three handler modules being untouched is again worth noting, for the same reason 002
noted it: **durability is not visible from inside a handler.** A service reacting to an event
cannot tell whether its memory will survive the next rebalance. Only the loop around it
changes — which is why the fix lands in one file plus one new module.

## Environment surface

Every default leaves 002's observed behaviour unchanged (R3.27).

| Variable | Default | Effect |
|---|---|---|
| `STATE_BACKEND` | `memory` | `memory` \| `postgres` (D8) |
| `STATE_DB_DSN` | unset | required when backend is `postgres`; empty = unset (D13) |
| `STATE_WRITE_ORDER` | `state_first` | `state_first` \| `offset_first` (D4) |
| `STATE_CRASH_AFTER` | `none` | `none` \| `state_write` \| `offset_commit` (D5) |

Compose sets `STATE_BACKEND=postgres` and `STATE_DB_DSN` for all five consumers; the two
levers stay unset there and are supplied per-experiment.

Every 001 and 002 variable keeps its meaning. `STATE_DB_DSN` follows the existing
`_blank_is_unset` validator, because compose interpolation yields `""` rather than removing a
variable — the same trap 002 D10 hit with `group.instance.id`.

## Known gaps, by intent

| Gap | Requirement | Closed by |
|---|---|---|
| Duplicate side effects after a crash in the dual-write window | R3.14, D7 | 008 |
| Offset and state cannot be written atomically | D4, X4 | 008 |
| State is in a shared database, not co-partitioned with the input | X4 | 007 |
| Rebuild cost grows with history, not with key count | D3 | 007 (compacted changelog) |
| The shared-DB bottleneck is described, never measured | out of scope | — (needs the load generator R2.33 excludes) |
| No dedup on `event_id`; idempotency rides on `sequence` | D6 | 008 |
| Producer `OrderStore` still lost on restart | D12 | — (outbox; no spec claims it) |
| Single broker, RF 1 | 000 | 004 |

## Deferred to later specs

Nothing here may anticipate them: replication and `acks` (004), dead-letter handling (005),
compaction and tombstones (006), local state stores and changelog topics (007), transactions
and exactly-once (008), stream SQL (009).
