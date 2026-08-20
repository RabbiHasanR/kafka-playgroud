# 007 — Local State Stores and Changelog Topics: Tasks

Implements [requirements.md](requirements.md) per [design.md](design.md).

Each task cites the requirement IDs it satisfies and, where relevant, the design decision it
follows.

**`docker compose down -v` is not needed, but a rebuild is.** The KRaft quorum is untouched and
the three changelog topics are new rather than reconfigured, so existing orders and topics
survive. The image gains a dependency (T4), so `docker compose build` is required, and T3 adds
five named volumes that `up -d` creates on its own.

**Order matters in three places.** T1 must land before anything produces to or consumes from a
changelog, because auto-creation is off. T4–T7 build the store and must precede T8's wiring.
T11's removals come last so that an intermediate checkout still starts.

**T3 and T9 together are the cutover.** Between them the consumers still resolve
`STATE_BACKEND=postgres`, which no longer exists as an enum member — so those two land in the
same change or the containers do not start.

## Topics and configuration

- [x] **T1** — In `scripts/create_topics.sh`, add the three per-group changelog topics —
  `${STATE_CHANGELOG_PREFIX:-order-fold}.<group_id>` for the three default group ids — to the
  `TOPICS` array with the same `PARTITIONS` and `REPLICATION_FACTOR` as `order-lifecycle`,
  carrying `cleanup.policy=compact` plus `segment.ms`, `min.cleanable.dirty.ratio`,
  `delete.retention.ms` and `min.compaction.lag.ms` from new `FOLD_*` variables. Reuse the
  existing `EXTRA_CONFIG` map and the `--if-not-exists` plus `kafka-configs.sh --alter`
  two-pass unchanged. Extend the header comment's per-topic list with the three entries and a
  line saying why the changelog is per group rather than per order.
  — *R7.1, R7.2* — D1
- [x] **T2** — In `config.py`: rename `StateBackend.POSTGRES` to `LOCAL`, delete the
  `state_db_dsn` field and its entry in the blank-is-unset validator, and add `state_dir`,
  `state_changelog_prefix` and a `StateRebuild` enum (`FULL` default, `CHECKPOINT`) with
  `state_rebuild`. Add a `changelog_topic_for(group_id)` helper next to `group_id_for()` so the
  application and `create_topics.sh` cannot derive different names. Document each in the
  `Attributes` block in the style of the existing entries, and rewrite `StateBackend`'s
  docstring, which describes Postgres.
  — *R7.1, R7.3, R7.9, R7.14* — D1
- [x] **T3** — In `docker-compose.yml`: replace the `x-state-db-env` anchor with `x-state-env`
  carrying `STATE_BACKEND`, `STATE_DIR`, `STATE_CHANGELOG_PREFIX` and `STATE_REBUILD`; give each
  of the five consumer containers its **own** named volume mounted at `STATE_DIR`, with a
  comment saying a shared volume would reintroduce the contention this feature removes; delete
  the `postgres` service, the `postgres-data` volume and the `postgres` entry in the
  `x-consumer-deps` anchor. Rewrite the anchor's spec-003 header comment.
  — *R7.3, R7.14* — D2, D11

## The local store

- [x] **T4** — In `consumer/state.py`, delete `PostgresStateStore`, `_UPSERT_FOLD`,
  `_DELETE_FOLD`, `_SELECT_FOLD`, `_VERIFY_SCHEMA`, `redact_dsn` and the `psycopg` import. Add
  `LocalStateStore`: one `Rdict` per owned partition under `STATE_DIR/<group_id>/<partition>/`,
  a `confluent_kafka.Producer` for the changelog, and `load`/`save`/`delete`/`held`/`close`.
  `save()` applies the sequence guard in Python against the local value, keeps `handled_count`
  in the stored JSON, writes locally, then produces the fold to the changelog without waiting.
  `delete()` removes locally and produces a null under the order's key. No read-through cache —
  after a restore the store is the warm copy. Wrap engine and producer errors in the existing
  `StateStoreUnavailable`, and update the module docstring, which is entirely about Postgres.
  Expose `flush()` so the caller can force the changelog ahead of an offset commit; `save()`
  itself never waits on a delivery report.
  — *R7.3, R7.4, R7.5, R7.6, R7.11* — D2, D3, D4, D5
- [x] **T5** — Add `LocalStateStore.restore(partitions)`: for each partition, a second
  `Consumer` with auto-commit off and a throwaway `group.id` that is **assigned, never
  subscribed**, seeked, and read to the high watermark captured at the start, applying each
  record into the store and treating a null value as a delete. Log
  `RESTORED partition=… records=… keys=… ms=…` at WARNING. Raise `StateStoreUnavailable` if the
  replay cannot complete, so a member that could not rebuild does not process messages against
  a half-built store.
  — *R7.7, R7.8* — D6, D7
- [x] **T6** — Add the per-partition checkpoint to `LocalStateStore`:
  `STATE_DIR/<group_id>/<partition>.ckpt` holding the changelog offset the store was brought up
  to, written when a partition is released or the store is closed, and honoured by `restore()`
  only under `STATE_REBUILD=checkpoint`. Under `full` it is ignored and the partition replays
  from the beginning. Note in the docstring why `full` is the default.
  — *R7.9* — D8
- [x] **T7** — Add `restore(partitions)` to the `StateStore` Protocol, and implement
  `MemoryStateStore.restore()` as a documented no-op — that backend is 002's amnesia and spec
  003's memory-backend criterion requires it to keep producing it. Add `LocalStateStore.forget(partitions)` releasing exactly
  those partitions: flush the producer, write each checkpoint, close each handle, and leave the
  directory and the changelog records alone, in contrast to `delete()` directly above it.
  — *R7.7, R7.10* — D6, D9

## Wiring

- [x] **T8** — In `consumer/runtime.py`, have `_on_assign` call `store.restore()` over the
  **deduplicated** partition numbers of the assignment — the subscription spans two topics while
  the store is keyed by number alone — and log membership before restoring so the two lines read
  in order. Call `store.flush()` in `_persist_and_commit` immediately before `_commit`, so the
  changelog is never behind the committed offset, and leave `STATE_WRITE_ORDER` and
  `STATE_CRASH_AFTER` switching the same two points they switch today. Update the module
  docstring and `_forget`'s docstring, both of which describe the Postgres backend and its
  cache.
  — *R7.7, R7.10, R7.11* — D4, D6
- [x] **T9** — In `consumer/main.py`, have `build_store` return `LocalStateStore` for
  `StateBackend.LOCAL`, passing the settings and group id; drop the `STATE_DB_DSN` check and the
  `PostgresStateStore` import.
  — *R7.3, R7.14* — D11
- [x] **T10** — Add `FailureRouter.to_source(origin, message, service, attempt)` to
  `consumer/dlq.py`, reusing `_publish` and stamping the existing attempt header plus a new
  `x-retry-target`. In `consumer/retry_worker.py`, have `_succeed()` call it instead of
  `store.load`/`store.save`, and remove the `build_store` import, the `_stores` map and every
  `StateStore` reference. In `consumer/runtime.py`, decode headers with the existing
  `decode_headers()` and add a branch skipping a message whose `x-retry-target` names another
  service — same shape as the snapshot-topic branch — and pass the carried attempt number into
  `failures.maybe_fail` instead of the literal `1`.
  — *R7.12, R7.13* — D10

## Removal and records

- [x] **T11** — Drop `psycopg[binary]` from `pyproject.toml` and add `rocksdict`; delete
  `scripts/state_schema.sql` and `scripts/apply_state_schema.sh`; remove the `POSTGRES_*` block
  from `.env.example` and add the `STATE_*` and `FOLD_*` levers this feature introduces.
  — *R7.14* — D11
- [x] **T12** — Write `docs/local-state-and-changelog.md`: why the changelog is per consumer
  group and what would break if it were not; co-partitioned state as directories on disk; the
  write path and the flush-before-commit invariant; the restore path, what `records` versus
  `keys` in the `RESTORED` line means, and a runnable walkthrough comparing `full` against
  `checkpoint`; the `max.poll.interval.ms` risk a long restore carries; why a retried message
  must travel to the partition's owner; and a closing section mapping these topics onto the
  internal `-changelog` topics 009's ksqlDB creates.
  — *R7.15* — D7, D8, D10
- [x] **T13** — Append `X13` to `DECISIONS.md` recording that the changelog is one compacted
  topic per consumer group, that this amends [X12](../../DECISIONS.md)'s claim that
  `order-snapshot` is what 007 rebuilds from, and that X5's `rocksdict` choice was confirmed
  against a real wheel. Update `README.md`: add the three topics and the `STATE_*` levers
  wherever the topic list and environment surface appear, and correct the 006 known-gaps rows
  that name 007 — a restart and a rebalance no longer resurrect a deleted order, but a
  deliberate replay from earliest still does, and closing that needs the `ORDER_DELETED` event
  006 placed out of scope.
  — *R7.2, R7.15* — D1, D12
