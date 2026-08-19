# 006 — Compaction and Tombstones: Tasks

Implements [requirements.md](requirements.md) per [design.md](design.md).

Each task cites the requirement IDs it satisfies and, where relevant, the design decision it
follows.

**No `docker compose down -v` needed.** Nothing here changes the KRaft quorum, and
`order-snapshot` is a new topic rather than a reconfiguration of an existing one — so orders
placed before this feature survive it and the producer's in-memory `OrderStore` is undisturbed.
The broker-side cleaner settings in T2 do need a `docker compose up -d` to take effect.

**Order matters in two places.** T1 must land before anything produces to or consumes from
`order-snapshot`, because auto-creation is off. And T8–T9 (the consumer's null branch) should land
before or with T7 (the delete endpoint): a tombstone published while the consumers still decode
every value would be classified as undecodable and routed to the dead-letter topic, per D5. Doing
T7 first is recoverable — the dead letters are readable and `dlq_replay.py` need not be run — but
it produces a misleading DLQ, so T8–T9 first is the intended order.

**T3–T7 are the producer side, T8–T9 the consumer side.** Between T7 and T9 the snapshot topic
accumulates values nothing reads, which is a fine intermediate state and not an error.

## Topics and configuration

- [x] **T1** — Restructure `scripts/create_topics.sh` so each topic carries its own extra config
  rather than the array applying `min.insync.replicas` uniformly. Add
  `${ORDER_SNAPSHOT_TOPIC:-order-snapshot}` with the same `PARTITIONS` argument and
  `REPLICATION_FACTOR` as `order-lifecycle` — equal partition counts are load-bearing, not
  cosmetic (D8) — carrying `cleanup.policy=compact` plus `segment.ms`,
  `min.cleanable.dirty.ratio`, `delete.retention.ms` and `min.compaction.lag.ms` from the
  environment with D9's defaults. Leave the other three topics at `cleanup.policy=delete` and
  keep their `min.insync.replicas` behaviour unchanged. Reuse the existing `--if-not-exists`
  plus `kafka-configs.sh --alter` two-pass. Extend the header comment's per-topic list with the
  new entry and a line saying why this one is compacted.
  — *R6.1, R6.2, R6.3* — D1, D8, D9
- [x] **T2** — Set `KAFKA_LOG_CLEANER_ENABLE: "true"` and `KAFKA_LOG_CLEANER_BACKOFF_MS` on the
  shared Kafka anchor in `docker-compose.yml`, with a comment noting that the first is already
  the broker default and is stated so the cleaner's existence is visible in the environment
  rather than assumed. Add `ORDER_SNAPSHOT_TOPIC` to the shared application environment anchor
  so the producer and all three consumers resolve the same value.
  — *R6.1, R6.3, R6.14* — D9
- [x] **T3** — Add `order_snapshot_topic: str = "order-snapshot"` to `Settings` in `config.py`
  under a `-- spec 006` heading, documented in the class docstring's `Attributes` block in the
  style of the existing entries. No other setting: the cleaner knobs are read by
  `create_topics.sh` from the shell, not by the application.
  — *R6.1, R6.14* — D10

## The producer side

- [x] **T4** — Add `Order.as_snapshot()` to `producer/orders.py`, returning the order's complete
  current state built on top of the existing `as_dict()` so the two views cannot drift, and
  `OrderStore.remove(order_id) -> Order`, raising `UnknownOrder` and taking the existing
  `_lock`. Document on `as_snapshot` that self-containment is a requirement rather than a
  convenience — a consumer holding only the newest snapshot for a key must know the customer,
  items, total, payment and state.
  — *R6.4, R6.8* — D2
- [x] **T5** — Refactor `LifecycleEventProducer._produce` in `producer/kafka_producer.py` into a
  keyed produce taking `topic`, `key: str` and `value: bytes | None`, with the existing
  `BufferError` and `KafkaException` handling unchanged, and have the current event path call
  it. This is the DRY step that makes the tombstone the same call with `value=None`; no
  behaviour changes.
  — *R6.4, R6.6* — D3
- [x] **T6** — Add `publish_snapshot(order_id, snapshot)` to the producer: fire-and-forget, with
  a delivery callback that logs failures at WARNING through `_describe_delivery_error` and that
  nothing waits on. Add `publish_tombstone(order_id)`: `value=None`, blocking on its delivery
  report using the same `threading.Event` and outcome-dict pattern as `publish_and_wait`, and
  raising `DeliveryFailed` / `DeliveryTimeout` so the route layer's existing `502` / `504`
  translation applies unchanged. Note in the docstrings why one blocks and the other does not.
  — *R6.4, R6.5, R6.6, R6.9* — D3
- [x] **T7** — In `producer/routes.py`, call `publish_snapshot` after the successful
  `_publish(...)` in both `create_order` and `publish_event`, using the `Order` returned by
  `register`/`commit` so the snapshot reflects the state after the event. Add
  `DELETE /orders/{order_id}` returning `204`: `404` before producing anything if the order is
  unknown, then the tombstone, then `OrderStore.remove()` — in that order, so a broker failure
  leaves the order intact and retryable rather than half-applied. Extend the module docstring's
  order-of-operations list with the delete path.
  — *R6.4, R6.6, R6.7, R6.8, R6.9* — D3, D4

## The consumer side

- [x] **T8** — Add `delete(partition, order_id)` to the `StateStore` protocol in
  `consumer/state.py` and to both backends: `PostgresStateStore` runs
  `DELETE FROM order_fold WHERE group_id = %s AND order_id = %s` as a module-level constant
  alongside `_UPSERT_FOLD` and evicts the cache entry, raising `StateStoreUnavailable` on a
  `psycopg.Error` as its neighbours do; `MemoryStateStore` pops from `_folds` and `_handled`.
  No change to `scripts/state_schema.sql`.
  — *R6.10* — D6
- [x] **T9** — In `consumer/runtime.py`, subscribe `run()` to both the lifecycle and snapshot
  topics and name both in the startup banner. Add a first branch to `_handle_message`:
  `if message.value() is None:` → `_handle_tombstone(message)` → `store.delete(...)`, log
  `TOMBSTONE order_id=… partition=… offset=…` at WARNING, commit, return — placed **before**
  `_decode`, because `_decode` currently turns a null value into a `NonRetryableError` and 005
  would route every tombstone to the dead-letter topic. Commit a non-null message arriving on
  the snapshot topic without touching the fold, so the fold's only source stays the event log.
  Update the module docstring to say the loop now reads two topics and why only one of them
  folds.
  — *R6.10, R6.11, R6.12, R6.13* — D5, D7

## Documentation

- [x] **T10** — Write `docs/compaction-and-tombstones.md`: `delete` versus `compact` as log
  versus table; why `order-lifecycle` is not compacted, with the four-message worked example;
  the replaces-not-adds rule; null values legal anywhere but only tombstones under compaction,
  and the mirror rule that a compacted topic rejects a null key; `delete.retention.ms` and the
  resurrection it prevents; D9's observation-tuned settings printed next to Kafka's production
  defaults; a runnable walkthrough of superseded values disappearing and then the tombstone
  itself; and a closing section on the three resurrection paths D11 leaves open, naming 007 as
  where they close.
  — *R6.15* — D9, D11
- [x] **T11** — Update `README.md`: link the existing "Retention and compaction" section to this
  spec and to T10's document, add `order-snapshot` and the `DELETE /orders/{id}` endpoint
  wherever the topics and the HTTP surface are listed, and update any known-gaps row naming 006
  to point at the gaps `design.md` records as open.
  — *R6.15*
- [x] **T12** — Add a `X12` entry to `DECISIONS.md` recording that `order-snapshot` is
  introduced here as a compacted table and is the topic 007 will rebuild local state from,
  citing X5 — because the routing rule puts a decision that spans features there rather than in
  a single `design.md`, and because 007 replacing the Postgres fold will otherwise erase the
  reasoning that the replacement was planned.
  — *R6.1* — D1, D11
