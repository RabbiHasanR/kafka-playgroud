# 005 — Retries, Dead-Letter Topic, and Poison Messages: Tasks

Implements [requirements.md](requirements.md) per [design.md](design.md).

Each task cites the requirement IDs it satisfies and, where relevant, the design decision it
follows.

**No `docker compose down -v` this time.** 004 needed one because KRaft fixes the controller
quorum at format time. Nothing here changes the quorum, and D12's `kafka-configs.sh --alter`
puts `min.insync.replicas` onto the existing `order-lifecycle` without recreating it — so orders
placed before this feature survive it, and the producer's in-memory `OrderStore` is not
disturbed.

**Order matters.** T1 must land first: every later task produces to or consumes from topics that
do not exist until it runs, and `docker-compose.yml` has broker-side auto-creation off. T2–T4 are the new building blocks,
T5–T6 wire them into the existing loop, T7–T9 add the worker that consumes what T6 starts
producing. Doing T6 before T7–T9 leaves messages accumulating in the retry topic with nothing
reading them, which is a fine intermediate state and not an error.

**One criterion is delivered by the absence of code.** R5.15 — nothing consumes the dead-letter
topic — is satisfied by T9 *not* subscribing the worker to it and by T11's tool being run by
hand. It is cited at T11 because that is where the choice is made visible, per D10.

## Topics and configuration

- [x] **T1** — Extend `scripts/create_topics.sh`: add `${RETRY_TOPIC:-order-lifecycle.retry}` and
  `${DLQ_TOPIC:-order-lifecycle.dlq}` to the `TOPICS` array with the same partition count and
  replication factor as `order-lifecycle`, and add a `MIN_INSYNC_REPLICAS` variable defaulting to
  2, passed as `--config min.insync.replicas=…` on create. Follow the create loop with a
  `kafka-configs.sh --alter --entity-type topics --add-config` pass over every topic, because
  `--if-not-exists` skips a topic that already exists and the config would otherwise never reach
  the `order-lifecycle` 004 created. Extend the header comment's per-topic list.
  — *R5.5, R5.12, R5.20* — D12
- [x] **T2** — Add spec 005's settings to `config.py` under their own heading: `retry_topic`,
  `dlq_topic`, `retry_max_attempts` (3), `retry_backoff_seconds` (`"30,120"`, parsed to a list),
  `producer_retries`, `producer_retry_backoff_ms`, `producer_message_timeout_ms`, and a
  `HandlerFailureMode(StrEnum)` with `NONE`/`TRANSIENT`/`POISON` plus `handler_failure_orders` and
  `handler_failure_attempts`. Docstring each in the style of `ProducerAcks`, reusing the existing
  `_blank_is_unset` validator for the optional strings. Every default must leave a process started
  without them behaving as 004 recorded.
  — *R5.10, R5.19, R5.21, R5.23* — D8, D11, D13

## The failure contract

- [x] **T3** — Add `src/order_service/consumer/errors.py`: `RetryableError` and
  `NonRetryableError`, and `classify(exc) -> type[RetryableError] | type[NonRetryableError]`
  returning `NonRetryableError` only for the declared non-retryable cases and `RetryableError`
  for everything else, including exceptions of neither type.
  — *R5.1, R5.3* — D2
- [x] **T4** — Add `src/order_service/consumer/dlq.py`: a `FailureRouter` wrapping one
  `confluent_kafka.Producer`, with `to_retry(...)` and `to_dead_letter(...)` that build D7's
  header set, key the message by `order_id`, publish the **original bytes** rather than a
  re-serialised event, and block until the broker acknowledges — raising if it does not, so the
  caller does not commit. Timestamps ISO-8601 UTC via `events.utc_now()`.
  — *R5.5, R5.6, R5.13* — D3, D7

## The consume loop

- [x] **T5** — In `runtime.py`, drop the `Handler` comment declaring that raising is not part of
  the contract and say that a handler may raise. Change `_handle_message`'s decode branch: instead
  of logging and committing, it raises `NonRetryableError` carrying the decode error, so a
  malformed message takes the same route as a schema violation.
  — *R5.2, R5.4* — D2
- [x] **T6** — Add the failure branch to `ServiceConsumer`. Wrap the handler call; on an
  exception, classify it (T3), and either publish to the retry topic with `x-attempt=1` and a
  due time one backoff ahead, or — when non-retryable — publish straight to the dead-letter topic
  with `x-attempts-made=0`. Commit the source offset only after the publication is acknowledged,
  and **skip the fold write entirely** so the fold does not advance. Log `RETRY_SCHEDULED`,
  `POISON_MESSAGE` and `DLQ_PUBLISHED` at WARNING with the `key=value` shape the existing
  `VIOLATION` and `DUPLICATE_ABSORBED` lines use. Inject the `FailureRouter` through
  `__init__` the way `StateStore` already is, and build it in `main.py`.
  — *R5.6, R5.7, R5.11, R5.12, R5.14, R5.16* — D3, D6, D9

## The retry worker

- [x] **T7** — Add `src/order_service/consumer/retry_worker.py`: an entry point that subscribes
  to the retry topic alone under group `retry-worker`, builds one `ServiceSpec` per entry in
  `SERVICE_REGISTRY` and one `PostgresStateStore` per service **keyed by that service's group id**,
  and dispatches each message to the handler of the service named in `x-service`, folding into
  that service's store. Reuse `apply_event` and the commit path rather than duplicating them;
  reject a message whose `x-service` is unknown by routing it to the dead-letter topic.
  — *R5.9* — D1, D5
- [x] **T8** — Add the due-time gate and attempt accounting to the worker. On a message whose
  `x-retry-at` is in the future: `seek()` back to its offset, `pause()` that partition, record
  when to resume, and continue the loop — resuming due partitions at the top of each iteration
  and never calling `time.sleep()`, so `poll()` keeps the member alive. On a failed attempt,
  increment `x-attempt` and republish with the backoff at that index (reusing the last entry when
  the list is shorter); when `x-attempt` reaches `RETRY_MAX_ATTEMPTS`, publish to the dead-letter
  topic with `x-attempts-made` set and log `RETRY_EXHAUSTED`.
  — *R5.8, R5.10, R5.12, R5.16* — D4, D8, D9
- [x] **T9** — Add a `retry-worker` service to `docker-compose.yml` from the existing
  `x-consumer-image` and `x-kafka-bootstrap` anchors, running
  `python -m order_service.consumer.retry_worker`, with the same Postgres and broker
  `depends_on` conditions the three consumers use. It subscribes to the retry topic only — the
  dead-letter topic must appear in no service's subscription.
  — *R5.9* — D1

## The levers and the tools

- [x] **T10** — Add `src/order_service/consumer/failures.py`: a `maybe_fail(event, attempt)`
  driven by `HANDLER_FAILURE_MODE`, raising `RetryableError` for `transient` until
  `handler_failure_attempts` is exceeded and `NonRetryableError` for `poison`, only for orders
  named in `HANDLER_FAILURE_ORDERS`, and doing nothing under `none`. Call it at the top of the
  handler dispatch in both `runtime.py` and the worker. Add `scripts/produce_poison.sh`, which
  writes raw non-JSON bytes to the lifecycle topic with `kafka-console-producer` so T5's decode
  branch is exercised by a genuinely malformed message.
  — *R5.2, R5.19* — D11
- [x] **T11** — Add `src/order_service/tools/dlq_replay.py`: an `argparse` CLI that reads the
  dead-letter topic with `enable.auto.commit=False`, prints each message's key, original topic,
  partition, offset, attempts and error, and republishes to `x-original-topic` preserving the key
  **only** when `--publish` is passed — reporting and exiting otherwise. Support `--service` to
  restrict to one group's failures and `--limit` to bound a run. It commits nothing and is never
  invoked by `docker-compose.yml`, which is what makes the dead-letter topic terminal.
  — *R5.15, R5.17, R5.18* — D10

## The producer's half

- [x] **T12** — Pass `retries`, `retry.backoff.ms` and `message.timeout.ms` from T2's settings
  into `LifecycleEventProducer.__init__`, and extend the startup banner in `producer/app.py` —
  which already carries brokers, topic and `acks` — with the three values in effect. The existing
  `_describe_delivery_error` already names the partition, so a `NOT_ENOUGH_REPLICAS` refusal
  surfaces through 004 D7's path unchanged.
  — *R5.21, R5.22* — D12

## Configuration and documentation

- [x] **T13** — Document every T2 and T1 variable in `.env.example` under a spec-005 heading,
  commented out at their defaults, using the plain `${VAR:-default}` shape since none is a
  credential. Add 005's section to `README.md` following the shape of the 003 and 004 sections,
  with its own known-gaps table, and close the `acks=all satisfied by an ISR of one` rows in
  `README.md` and `docs/replication.md` that name 005.
  — *R5.23, R5.24* — D13
- [x] **T14** — Write `docs/retries-and-dlq.md`: the retryable/non-retryable distinction and why
  an unclassified exception is retryable; why a poison message blocks a partition in Kafka
  specifically; what non-blocking retry costs in ordering, including why `SEQUENCE_GAP` warnings
  appear while a retry is in flight; D7's header contract; a runnable walkthrough of a transient
  failure recovering at attempt 2 and a poison message reaching the dead-letter topic with zero
  attempts; the manual replay path and why nothing drains the topic automatically; and an explicit
  statement that tiered delay topics, dead-letter depth alerting, dead-letter retention and
  unclean leader election remain open, naming where each is closed.
  — *R5.24* — D2, D6, D10, D14

## Bounding replay

- [x] **T15** — Replace `dlq_replay.py`'s `subscribe()` and idle-timeout loop with a bounded
  read: resolve the dead-letter topic's partitions via `list_topics`, capture each one's high
  watermark with `get_watermark_offsets(cached=False)` *before* publishing anything, `assign()`
  them at their beginning offsets, and stop once every partition has yielded its recorded
  watermark minus one. Treat an empty partition as complete before the loop starts, and keep the
  idle timeout only as a stall guard. Correct the module docstring, which claims the tool refuses
  to automate the replay loop — until this task it did not.
  — *R5.17* — D10
- [x] **T16** — Skip republishing a dead letter whose `x-error-class` is non-retryable unless
  `--include-poison` is passed, listing it with an `excluded — non-retryable` note and tallying
  the exclusions separately from the republished count. The exclusion applies to publication
  only; the listing is unchanged.
  — *R5.25* — D10
