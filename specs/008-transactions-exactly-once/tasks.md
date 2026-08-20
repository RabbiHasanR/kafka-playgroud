# 008 — Transactions and Exactly-Once Semantics: Tasks

Implements [requirements.md](requirements.md) per [design.md](design.md).

Each task cites the requirement IDs it satisfies and, where relevant, the design decision it
follows.

**No new topics and no broker changes, so `docker compose down -v` is not needed.** The
transaction state log was pinned by 000 T5 and 004 T4 and is inherited untouched. The image
gains no dependency — `confluent_kafka` already exposes the transactional API — but the source
changes, so `docker compose build` is required. T13's variables are additive and every one has a
default, so an un-updated `.env` still starts on the at-least-once path.

**Order matters in three places.** T1 must land first: every later task reads settings or enum
members it adds. T2 and T3 build the module T4–T7 import from. T14's documentation comes last so
it describes what was actually built.

**T4, T5 and T6 together are the cutover.** `LocalStateStore.__init__` and
`FailureRouter.__init__` both gain a required producer parameter, and `main.py` is the only
caller that can supply it — so those three land in the same change or the consumers do not
start.

**Fourteen tasks rather than the [X11](../../DECISIONS.md) twelve.** The feature is two
mechanisms — an idempotent producer and a transactional cycle — and the second requires moving
producer ownership out of two components that have owned their own since 005 and 007.

## Configuration

- [x] **T1** — In `config.py`, add `ProcessingGuarantee` (`at_least_once`, `exactly_once`) and
  `IsolationLevel` (`read_committed`, `read_uncommitted`) as `StrEnum`s beside the existing
  ones; add `processing_guarantee`, `producer_idempotence`, `consumer_isolation_level`,
  `transaction_commit_interval_messages`, `transaction_commit_interval_ms` and
  `transaction_timeout_ms` with the defaults from the design's environment surface; add
  `TRANSACTION_OPEN` to `StateCrashPoint`; and add a `transactional_id_for(group_id)` helper
  beside the existing `group_id_for()`. Extend the `Settings` docstring in the established
  style. — *R8.12, R8.13, R8.14* — D3, D7, D8

- [x] **T2** — In a new `consumer/transactions.py`, add `build_producer(settings, group_id,
  instance)` returning the one `Producer` a consumer process owns: `acks=all` and
  `partitioner=consistent_random` as `LocalStateStore` hardcoded them, `enable.idempotence` from
  `PRODUCER_IDEMPOTENCE`, `client.id=<group_id>-<instance>`, and — only under
  `exactly_once` — `transactional.id` from `transactional_id_for()` plus
  `transaction.timeout.ms`. Add a `ProducerFenced` exception and a `_fenced(exc)` predicate
  recognising `_FENCED` and `_INVALID_PRODUCER_EPOCH`. Module docstring explains why one
  producer replaces two and why the identity must be stable. — *R8.1, R8.3, R8.5* — D1, D2, D3

- [x] **T3** — In `consumer/transactions.py`, add a `CommitStrategy` Protocol with
  `note(message)`, `maybe_commit()`, `abort(reason)` and `close()`, and two implementations.
  `DirectCommitter` wraps today's `consumer.commit(message=..., asynchronous=False)` including
  its `COMMIT_REJECTED` handling, and is a no-op for the other three. `TransactionalCommitter`
  calls `init_transactions()` once, opens a transaction lazily on the first `note()`, records
  `offset + 1` per `TopicPartition` and the partitions written to, commits through
  `send_offsets_to_transaction(offsets, consumer.consumer_group_metadata())` followed by
  `commit_transaction()` when either interval is reached, and returns the touched partitions
  from `abort()`. Any fenced error raises `ProducerFenced`. — *R8.6, R8.7, R8.8, R8.9* — D4, D5

## The store and the router

- [x] **T4** — In `consumer/state.py`, remove `LocalStateStore`'s own `Producer` construction and
  take one as a constructor argument. Add `isolation.level` from settings to
  `_changelog_reader()`. Add `discard(partitions)` — close the `Rdict`, delete the partition
  directory and its checkpoint file — to the `StateStore` Protocol, implementing it on
  `LocalStateStore` and as a dict eviction on `MemoryStateStore`. Leave `flush()` as it is;
  T9 decides when it is called. Amend the module docstring's closing paragraph, which currently
  says both writes are still uncovered. — *R8.4, R8.10, R8.11* — D1, D6, D10

- [x] **T5** — In `consumer/dlq.py`, remove `FailureRouter`'s own `Producer` construction and take
  one as a constructor argument; `close()` no longer owns it. Update the module docstring's
  opening paragraph about blocking on delivery reports to say that under a transaction the
  ordering is the transaction's rather than the flush's. — *R8.4* — D1

- [x] **T6** — In `consumer/main.py`, build the producer with `build_producer()` before the store
  and the router — before the group is joined, so a doomed process never joins it — pass it to
  `build_store()`, `FailureRouter` and `ServiceConsumer`, and flush and close it in the existing
  `finally` alongside them. Exit 2 on a startup `ProducerFenced` or a refused setting
  combination. — *R8.4, R8.12* — D1, D7

## The consume loop

- [x] **T7** — In `consumer/runtime.py`, add `isolation.level` to `build_consumer_config()`;
  construct the `CommitStrategy` in `ServiceConsumer.__init__` from the injected producer and the
  consumer it owns; have `_commit(message)` delegate to `strategy.note(message)` and call
  `strategy.maybe_commit()` after each handled message and on the `message is None` branch of the
  poll loop. Extend the startup log block with one line carrying the guarantee, the
  transactional identity, the isolation level and both commit intervals. — *R8.6, R8.8, R8.10,
  R8.14* — D4, D5, D10

- [x] **T8** — In `consumer/runtime.py`, add the abort path: catch `ProducerFenced` in `run()`,
  log `PRODUCER_FENCED` with the identity and exit 1 without retrying; and on a transaction
  abort, `store.discard()` then `store.restore()` the returned partitions and `_rewind` each to
  its committed offset. Have `_on_revoke` and `_on_lost` abort any open transaction before
  `_forget()`, taking no discard-and-rebuild for partitions being released. — *R8.5, R8.9,
  R8.11* — D3, D6

- [x] **T9** — In `consumer/runtime.py`, branch `_persist_and_commit` on the guarantee: under
  `exactly_once` write the fold, fire the `transaction_open` crash point, then `_commit` —
  no `flush()`, because `commit_transaction()` is what makes the record durable, and no
  write-order branch, because a transaction has no order between the two writes. Log
  `STATE_WRITE_ORDER=offset_first` as ignored under the guarantee rather than refusing it. Leave
  the at-least-once path and both 003 crash points exactly as they are, and amend the docstring's
  table to say which column applies to which guarantee. — *R8.13* — D7, D8

- [x] **T10** — In `consumer/retry_worker.py`, add `isolation.level` to the worker's consumer
  configuration, and record in the module docstring that the worker's own
  republish-then-commit stays at-least-once and why. — *R8.10* — D10, D11

## The producer process

- [x] **T11** — In `producer/kafka_producer.py`, add `enable.idempotence` from
  `PRODUCER_IDEMPOTENCE` to the producer configuration, leaving `acks` and the retry settings on
  their 004 and 005 behaviour. Extend the module docstring with why reordering rather than
  duplication is what this buys here, and why this producer is not transactional. — *R8.1, R8.2*
  — D9

## Environment and records

- [x] **T12** — In `docker-compose.yml`, add the six new variables to the consumer service
  environment through the existing anchor, defaulting to the at-least-once path so an unchanged
  `.env` behaves as it does today; add `CONSUMER_ISOLATION_LEVEL` to the retry worker and
  `PRODUCER_IDEMPOTENCE` to the producer service. — *R8.14*

- [x] **T13** — In `.env.example`, add a `spec 008` block documenting all six variables in the
  established commentary style, including that `CONSUMER_INSTANCE_ID` becomes correctness-critical
  under `exactly_once` and that `TRANSACTION_COMMIT_INTERVAL_MESSAGES=1` is the per-message
  setting the crash lever wants. — *R8.14* — D3, D5

- [x] **T14** — Write `docs/transactions-and-exactly-once.md`: why reordering is the worse half of
  what idempotence prevents and what it does to the sequence guard and the changelog; what one
  transaction covers and what it does not; why the local store must be discarded and rebuilt
  after an abort; what `read_committed` costs at the last stable offset and why control records
  make offsets non-contiguous; and what stays at-least-once afterwards. Update the known-gaps
  rows in `README.md` that name 008 — including the correction that this rung drives
  `handled_count` to `last_sequence` for the durable fold but does not stop a handler running
  twice — and append `X14` to `DECISIONS.md` recording the exactly-once v2 choice and its
  rejected alternative. — *R8.15* — D2, D11
