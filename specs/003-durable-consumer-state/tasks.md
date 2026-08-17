# 003 — Durable Consumer State: Tasks

Implements [requirements.md](requirements.md) per [design.md](design.md).

Each task cites the requirement IDs it satisfies and, where relevant, the design decision it
follows.

**Read this before starting.** Adding `psycopg` changes `pyproject.toml`, which rebuilds the
image, which restarts `order-service` — and its `OrderStore` is in memory by design (D12). Every
order placed before T1 becomes unadvanceable: `POST /orders/{id}/events` will return `404`. Place
the orders each experiment needs **after** the rebuild, not before. This is not a defect and it
is not this feature's to fix.

**Order matters.** T5–T11 are the store; T12–T17 are the loop that uses it. Every experiment
from T18 on runs through both. Building the runtime before the store means writing the consume
loop twice, exactly as 002 found with its config builder.

## Dependency and infrastructure

- [x] **T1** — Add `psycopg[binary]` to `pyproject.toml`. Rebuild the image and confirm all
  five consumers still start on the `memory` backend, unchanged — the dependency landing must
  not alter 002's behaviour before a single line of state code exists. — *R3.1, R3.24* — D10
- [x] **T2** — Write `scripts/state_schema.sql`: `order_fold` with
  `PRIMARY KEY (group_id, order_id)`, `last_sequence`, nullable `state`, `last_event_id`,
  `handled_count` defaulting to 0, and `updated_at`. `CREATE TABLE IF NOT EXISTS`, so the file
  is safe to re-apply. **No `partition` column** and no secondary index.
  — *R3.1, R3.2, R3.3, R3.13, R3.25* — D1, D6, D11
- [x] **T3** — Write `scripts/apply_state_schema.sh`: apply T2's file with `psql`, taking the
  connection from the environment, runnable from the host and from inside the compose network.
  It exists because `/docker-entrypoint-initdb.d/` runs only on an empty volume, so it is the
  only path that works after the first `up`. — *R3.25* — D11
- [x] **T4** — Add the `postgres:17-alpine` service to `docker-compose.yml` with a `pg_isready`
  healthcheck, a named volume, T2's file mounted at `/docker-entrypoint-initdb.d/`, and `5432`
  published to the host. Add the `x-state-db-env` anchor building one DSN from
  `${POSTGRES_USER:?…}`, `${POSTGRES_PASSWORD:?…}`, `${POSTGRES_DB:?…}` and merge it into all
  five consumers. Make each consumer `depends_on` postgres `service_healthy`. Add
  `.env.example` documenting the three variables — no default password anywhere.
  — *R3.24, R3.25, R3.26* — D11, D13

## Configuration

- [x] **T5** — Extend `Settings` in `config.py` with `state_backend` (`memory` default),
  `state_db_dsn` (unset, and treated as unset when blank via the existing `_blank_is_unset`
  validator), `state_write_order` (`state_first` default), and `state_crash_after` (`none`
  default). The first, third, and fourth are `StrEnum`s for the same reason `GroupProtocol` is
  — an unrecognised value must fail at startup, not select a silent fallback. Confirm a
  consumer started with none of them set behaves exactly as 002 recorded.
  — *R3.19, R3.24, R3.27* — D5, D8, D13

## The state store

- [x] **T6** — Create `consumer/state.py` with the `StateStore` protocol (`load`, `save`,
  `forget`, `held`, `close`) and the frozen `SaveOutcome` carrying `applied` and
  `handled_count`. **Do not add `handled_count` to `OrderFold`** — the domain fold keeps no
  opinion about persistence, which is what leaves `apply_event()` untouched.
  *Amended while building:* `OrderFold` moved here from `runtime.py` (the Postgres store builds
  one from a row, so the import can only point one way), and `save()` takes `event_id` beside
  the fold. — *R3.19* — D2
- [x] **T7** — Implement `MemoryStateStore` by lifting `ServiceConsumer._folds` and
  `_drop_folds` out **verbatim**, behaviour unchanged. Not tidied, not renamed, not improved:
  001's and 002's recorded results only reproduce against this code path. — *R3.20* — D2
- [x] **T8** — Implement `PostgresStateStore.__init__`: connect with `psycopg` v3,
  `autocommit=True`, verify `order_fold` exists, and raise `StateStoreUnavailable` naming the
  backend and a **password-redacted** DSN. One connection, no pool. `close()` releases it.
  — *R3.21* — D9, D10, D13
- [x] **T9** — Implement `load()` and `forget()`. `load()` serves from the partition-keyed cache
  and falls back to a single-row `SELECT` on a miss — **lazily, never warmed on assignment**,
  because warming costs a scan proportional to history at every rebalance and would hide the
  problem 007 solves. `forget()` drops cache entries for exactly those partitions and issues
  **no `DELETE`**. — *R3.4, R3.9, R3.10* — D3
- [x] **T10** — Implement `save()` as the single guarded upsert from D6: fold columns advance
  only when the incoming sequence is higher, `handled_count` increments unconditionally,
  `RETURNING last_sequence, handled_count`. One statement — no `SELECT … FOR UPDATE`, no
  read-modify-write, no application lock.
  *Amended while building:* `applied` cannot be "the row carries the sequence we sent" — that is
  also true of an exact redelivery, the one case this feature exists to detect. A `previous` CTE
  returns the pre-write sequence, still in one statement. — *R3.5, R3.11, R3.13* — D6
- [x] **T11** — Handle a store failure during consumption: log at `ERROR` with a stable
  `STATE_STORE_UNAVAILABLE` marker and re-raise so the loop ends and the process exits
  non-zero. Deliberately **not** 002 D8's survive-and-continue — continuing would commit
  offsets for events whose state was never written. — *R3.22* — D9

## Runtime

- [x] **T12** — Inject a `StateStore` into `ServiceConsumer`; delete `_folds` and `_drop_folds`;
  have `_on_revoke` and `_on_lost` call `forget()`. `apply_event()` and the violation logging
  are **unchanged**, so genuine sequence gaps and illegal transitions are still detected and
  still reported at `WARNING` with 001's marker. — *R3.4, R3.6, R3.9* — D2, D3
- [x] **T13** — Implement the write order in `_handle_message`: state write then offset commit
  by default, reversible by `STATE_WRITE_ORDER`. The default is not a preference — the other
  order loses data permanently and logs nothing. — *R3.5, R3.16* — D4
- [x] **T14** — Implement the crash lever with **`os._exit(1)`**, fired after the state write or
  after the offset commit per `STATE_CRASH_AFTER`. Not `sys.exit()` and not an exception: either
  unwinds the stack, runs `run()`'s `finally`, and closes the consumer gracefully — a shutdown,
  not a crash. — *R3.15* — D5
- [x] **T15** — When `SaveOutcome.applied` is false, log `DUPLICATE_ABSORBED` at `WARNING` with
  the order, sequence, and handled count, in the shape of 002's `COMMIT_REJECTED`. The handler
  still runs — the duplicate side effect is produced, not hidden. — *R3.14* — D7
- [x] **T16** — Add the state backend to the startup banner beside the protocol and assignor
  002 put there, so a log excerpt carries which backend produced it. — *R3.23* — D8
- [x] **T17** — Build the store from settings in `main.py` before the `Consumer` is constructed,
  and close it on exit. A process that cannot honour R3.5 must never join the group — joining
  and then dying has already caused a rebalance. — *R3.19, R3.21* — D8, D9

## Found while building

Neither of these was foreseen; both are recorded as decisions rather than quiet fixes because
each one made an experiment lie before it was caught.

- [x] **T30** — Suppress the violation diagnosis for an event whose sequence is at or behind the
  stored fold, before folding it. Durable memory makes a redelivery look like an out-of-order
  arrival, so T21 reported a `SEQUENCE_GAP` and an `ILLEGAL_TRANSITION` for data that was never
  wrong — one class of false positive traded for another. The handler still runs and the
  delivery is still counted; only the diagnosis is withheld. — *R3.6, R3.14* — D14
- [x] **T31** — Give every service that builds from this repository one shared image tag.
  A bare `build: .` makes compose derive an image name per service, and `docker compose build`
  skips profile-gated services — so `notification-consumer-2` and `-3` ran spec 002's code
  inside a 003 group, and the first T19 run reported four meaningless sequence gaps. A stale
  member must not be expressible. — *R3.20, R3.27* — D15

## Verification experiments

Each is run and observed, not merely coded. Tick only after actually running it, and record
what was seen in Results. All run with `STATE_BACKEND=postgres` unless stated.

- [x] **T18** — **Restart amnesia is gone.** Place an order, advance it to `PACKED`, restart a
  consumer, then advance to `SHIPPED`. Confirm no `SEQUENCE_GAP` — the reverse of what 001
  recorded at T35. Query `order_fold` and confirm the row survived the restart. — *R3.7*
- [x] **T19** — **Rebalance amnesia is gone.** Re-run 002's T17 unchanged: advance several
  orders part-way, kill one notification instance, and confirm the survivor that inherits the
  partition reports **no** `SEQUENCE_GAP` for the in-flight orders. Confirm from the row that
  the inheriting member read state written by a different member. **This is the experiment the
  feature exists for** — record 002's T17 result beside it. — *R3.3, R3.8*
- [x] **T20** — **A full replay changes nothing.** Snapshot `order_fold` for one group, reset
  that group's offsets to earliest with `kafka-consumer-groups.sh --reset-offsets`, let it
  re-consume the topic, and confirm every `last_sequence` and `state` is unchanged while
  `handled_count` has risen. — *R3.11, R3.12*
- [x] **T21** — **The dual-write gap, as a number.** Set `STATE_CRASH_AFTER=state_write` on one
  instance, advance an order, and let it die between the two writes. Restart it and confirm:
  the event is redelivered, `DUPLICATE_ABSORBED` is logged, `last_sequence` is unchanged,
  `handled_count` is one higher than the number of events, and the handler's log line appears
  twice. — *R3.13, R3.14, R3.17*
- [x] **T22** — **The other order loses data.** Set `STATE_WRITE_ORDER=offset_first` and
  `STATE_CRASH_AFTER=offset_commit`, and repeat T21. Confirm the consumer resumes **past** the
  event, that `order_fold` is permanently missing it, and that nothing anywhere logged a
  problem. — *R3.18*
- [x] **T23** — **Group isolation survived shared storage.** With all three services running,
  confirm `order_fold` holds one row per `(group_id, order_id)` and that stopping notification
  leaves inventory's and analytics' rows advancing — 001's fan-out property, now visible as
  rows rather than only as offsets. — *R3.2*
- [x] **T24** — **The memory backend still reproduces 002.** Set `STATE_BACKEND=memory` and
  re-run T19. Confirm the `SEQUENCE_GAP` returns exactly as 002 T17 recorded it, and that the
  startup banner names which backend produced each run. — *R3.20, R3.23*
- [x] **T25** — **Failing loudly.** With postgres stopped, start a consumer on the `postgres`
  backend and confirm it exits before joining the group, with an error naming the backend and a
  DSN whose password is redacted. Then, with it running, stop postgres mid-consumption and
  confirm `STATE_STORE_UNAVAILABLE` is logged and the process exits non-zero rather than
  continuing on cache. — *R3.21, R3.22*
- [x] **T26** — **Real violations still fire.** With durable state on, publish a genuinely out
  of order event using 001's `force: true` and confirm `ILLEGAL_TRANSITION` is still reported,
  and that a genuine sequence gap is still reported as one. Durable state must remove the false
  positives without removing the detector. — *R3.6*
- [x] **T27** — **Host and compose.** Run a consumer from the host against `localhost:9092` and
  `@localhost:5432`, and in compose against `kafka:19092` and `@postgres:5432`, changing only
  environment variables. Confirm both write to the same rows, and that a consumer started with
  no 003 settings at all runs on `memory` and behaves as 002 recorded. — *R3.26, R3.27*

## Documentation

- [x] **T28** — Write `docs/durable-state.md`: a committed offset is a position and a fold is a
  memory; why the consumer's fold is durable while the producer's `OrderStore` is not; the
  dual-write problem and the one thing not being reached for (offsets in Postgres, and why 008
  needs that gap open); a runnable walkthrough of T18, T19, and T21; and a closing section
  naming which behaviours remain accepted limitations and which spec closes each.
  — *R3.28, R3.29*
- [x] **T29** — Add a `README.md` section: running with Postgres, applying the schema (including
  the empty-volume caveat), the environment surface, and a link to the new document. Update the
  known-gaps table to record which rows naming 003 this feature closed — *Consumer fold state
  lost on restart* — and which it did not: *duplicate processing after a crash* and *no
  deduplication* both stay open and now point at 008 alone. — *R3.28, R3.30*

## Results

To be recorded after the experiments are run, in the same shape as
[001's results table](../001-prepaid-order-service/tasks.md). Each row names what was actually
observed — rows, counts, markers, and offsets — not what was expected.

| Task | Observed |
|---|---|
| T18 | Order to `PACKED`, `docker restart inventory-consumer`, then `SHIPPED`. `seq=3` handled at offset 57 with **0 `VIOLATION` lines**. Row survived the restart at `last_sequence=2` and advanced to 3. 001 T35's false gap does not occur. |
| T19 | 3 members, one partition each. 9 orders to `seq=2`, all 9 persisted under `notification-service`. `docker stop notification-consumer-2` → `notification-1` logged `REBALANCE ASSIGNED partitions=[0, 1]`. Advancing the 3 partition-1 orders to `SHIPPED`: **0 `SEQUENCE_GAP`**, handled by a member that had never seen those orders. Against T24's control: **0/3 here, 4/4 on memory.** |
| T20 | `analytics-service` offsets reset `--to-earliest`, whole topic re-consumed: **229 records, 0 fold differences, 0 `VIOLATION`, 229 `DUPLICATE_ABSORBED`**, `handled_count` total 278 → 507 (+229 exactly). 84 rows before and after. |
| T21 | `STATE_CRASH_AFTER=state_write`: `CRASH_LEVER` fired, exit 1, row written at `last_sequence=1 handled_count=1`, group lag 1 — **state durable, offset uncommitted**. On restart the event was redelivered: `DUPLICATE_ABSORBED seq=1 stored_seq=1 handled=2`, `last_sequence` still 1, state still `CREATED`, and **the handler ran twice for one event**. That last number is the residue 008 exists to remove. |
| T22 | `STATE_WRITE_ORDER=offset_first` + `STATE_CRASH_AFTER=offset_commit`: offset committed, crash before the write. `inventory-service` has **no row at all** for the order while `analytics` and `notification` both hold it. On restart the event was **never redelivered** (0 occurrences), lag 0, offset past it. The only `WARNING` lines in the whole run were two ordinary `REBALANCE` lines — **nothing anywhere reported the loss.** |
| T23 | Whole notification group stopped; an order advanced to `seq=2`. Rows for `inventory-service` and `analytics-service` advanced; `notification-service` had **no row**. On restart it caught up to `seq=2` independently. Per-group row counts diverged freely (analytics 88, the others 25) after T20's replay — one memory per group, in one table. |
| T24 | Identical to T19 with `STATE_BACKEND=memory`, one variable apart: **4 of 4 orders on the inherited partition reported `SEQUENCE_GAP expected=1 observed=3`**. This is 002 T17 reproducing exactly, and it is the "before" that makes T19's zero mean something. |
| T25 | Unreachable database at startup: exit **2**, `state backend 'postgres' is unreachable at postgresql://order_service:***@localhost:9999/order_state`, **0 rebalance or subscribe lines — it never joined the group**, and the password was redacted. `STATE_BACKEND=postgres` with no DSN: exits naming `STATE_DB_DSN`. Store killed mid-run: all four consumers that were processing logged `STATE_STORE_UNAVAILABLE` and exited **1**; none continued on cache. |
| T26 | Forced `SHIPPED` after `CREATED` on a contiguous sequence → `ILLEGAL_TRANSITION` still reported. A consumer that had genuinely lost `seq=1` via T22's mechanism reported `SEQUENCE_GAP expected=1 observed=2` on the next event, **while `analytics`, which never lost it, reported nothing about the same event**. Durable state removed the false positives without touching the detector. |
| T27 | Every crash and eviction experiment above was run **from the host** against `localhost:9092` + `localhost:5433`, while the containers ran against `kafka:19092` + `postgres:5432`, both writing the same rows in the same group. Only environment variables differed. A consumer started with no 003 settings reports `state_backend=memory` and behaves as 002 recorded. |

## Notes

**T19 is the feature.** Everything before it is machinery and everything after it is
qualification. It is also the only experiment whose result is meaningless on its own: "no
`SEQUENCE_GAP`" is a claim about a change, so 002's T17 observation has to sit beside it in the
table. If that row is not recorded, T19 proves nothing.

**T7 is the task most likely to be done wrong.** "Lift it out verbatim" reads like an
invitation to improve it on the way past. It is the opposite. The memory backend is the control
for T24, and a subtly better version of it makes every earlier recorded result unverifiable —
which is worse than leaving the code slightly untidy, in a repository whose output is
observations.

**T14 is 003's `force` flag**, the same structural need 001 and 002 each hit. The window between
the state write and the offset commit is microseconds wide, so R3.17 and R3.18 are unreachable
without a deliberate way in. The `os._exit()` detail is not pedantry — `sys.exit()` produces a
*graceful* departure and therefore a different rebalance, so an experiment run that way would be
measuring a shutdown and reporting it as a crash.

**T22 is the experiment nobody wants to run and everybody should.** It ends with a consumer that
is working perfectly, logging nothing unusual, and permanently wrong. That combination is worth
seeing once.

**Nothing here closes the duplicate side effect.** T21 ends with `handled_count` higher than the
event count and that is the *correct* result — the residue X4 predicted, waiting for 008. A
local fix, a dedup table, or offsets moved into Postgres would each delete the evidence the next
rungs are built on.

**T29 must not overclaim.** Three README gap rows name 003. This feature closes one of them.
Editing the other two to say "closed" because the spec that mentioned them shipped is the exact
drift `CLAUDE.md` asks to be surfaced rather than absorbed.
