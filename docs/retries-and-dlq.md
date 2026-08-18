# Retries, the Dead-Letter Topic, and Poison Messages

Companion to [spec 005](../specs/005-retries-dlq-poison-messages/requirements.md).

Until this feature, a handler could not fail. `runtime.py` said so in the type it declared:

```python
#: Raising is not part of the handler contract at this spec.
Handler = Callable[[LifecycleEvent], None]
```

And the one failure the loop did handle showed what the absence cost:

```python
except (ValueError, UnicodeDecodeError) as exc:
    logger.error("undecodable message at %s[%d]@%d: %s", ...)
    self._commit(message)      # ← the message is gone. forever. silently.
    return
```

That was not carelessness. Committing was the *only* option, because the alternative was
worse. This document is about why, and about what had to be built to get a third option.

---

## 1. Why a bad message is worse in Kafka than in a queue

In RabbitMQ or SQS, messages are handed out individually. A consumer that cannot process one
rejects it, and the broker hands out the next. A bad message blocks **itself**.

Kafka has no such thing. A partition is an ordered log, and a consumer group's position in it
is **one number per partition**. There is no "skip this one and remember to come back".

```
partition 1:  [845][846][847][848][849][850] ...
                          ↑
              committed offset = 847
              846 was processed. 848 was not. There is no way to say
              "847 failed but 848 succeeded" — the offset is one number.
```

So a consumer that retries offset 847 forever never commits past 847. Restart it and it reads
847 again. Every message behind it on that partition waits, indefinitely. One bad message
poisons the whole partition — which is where the name comes from.

That leaves exactly three options, and 005 is about choosing the third:

| | Option | Cost |
|---|---|---|
| 1 | retry in place forever | the partition stops. Lag grows without bound. |
| 2 | log and commit | the message is destroyed, silently. **This is what 001–004 did.** |
| 3 | move it somewhere else, then commit | the partition advances and the message survives |

---

## 2. A transient failure and a poison message are opposites

They look identical in a stack trace and need opposite treatment.

| Transient — retrying works | Poison — retrying never works |
|---|---|
| database connection dropped | the bytes are not JSON |
| downstream returned `503` | JSON is valid, but not a `LifecycleEvent` |
| lock timeout, network reset | `total_amount` disagrees with the item sum |
| `NOT_ENOUGH_REPLICAS` | a field the schema requires is missing |

Retrying a poison message three times produces three identical exceptions, three backoff
waits, and no new information. Never retrying a transient one throws away work that would have
succeeded 200ms later. So classification happens **first**, in one place:

```python
# order_service/consumer/errors.py
def classify(exc: BaseException) -> FailureKind:
    if isinstance(exc, NonRetryableError):
        return NonRetryableError
    return RetryableError
```

### Why an unrecognised exception is retryable

The two mistakes are not symmetric:

- A **handler bug** misfiled as retryable costs the attempt budget and reaches the dead-letter
  topic anyway. You lose ~2 minutes and gain a slightly noisier log.
- A **real outage** misfiled as poison is discarded on its first attempt, with no second chance,
  during exactly the incident where you can least afford it.

The cheaper mistake is the default.

---

## 3. The path a failure takes

```
order-lifecycle ──► inventory / notification / analytics   (3 groups)
                          │
          ┌───────────────┴───────────────┐
    RetryableError                  NonRetryableError
    (attempt 1 spent)               (attempt 1 spent, and it was the only one)
          │                                │
          ▼                                │
  order-lifecycle.retry                    │
   x-service, x-attempt=2, x-retry-at      │
          │                                │
          ▼                                │
     retry-worker  ── waits until due, then runs                    │
          │            SERVICE_REGISTRY[x-service]'s handler        │
          │                                                         │
     success → fold + commit                                        │
     fail, attempt < 3 → back to the retry topic, x-attempt=3       │
     fail, attempt = 3 ────────────────────┬─────────────────────────┘
                                           ▼
                                 order-lifecycle.dlq
                                  x-consumer-group=inventory-service
```

**Attempt 1 is spent inline**, by the main consumer, with no delay. A handler that succeeds
normally never touches the retry topic. `RETRY_MAX_ATTEMPTS=3` therefore means one inline
attempt and two in the worker — which is why `RETRY_BACKOFF_SECONDS` has two entries, not three.

**A poison message never touches the retry topic at all.** It reaches the dead-letter topic
having made exactly one attempt, and `x-attempts-made` says `1`.

---

## 4. What non-blocking retry actually costs

This is the part worth understanding, because it is a real trade and not a free win.

The main consumer commits the source offset **as soon as the message is safely on the retry
topic** — not when the retry succeeds. That is what keeps the partition moving. It has two
consequences, both permanent until spec 008.

### A committed offset stops meaning "processed"

From 005 onward, "committed" means *"no longer this partition's problem"*. The message may
still be in the retry lane, or in the dead-letter topic. Consumer lag is no longer a complete
picture of outstanding work — you have to look at the retry topic too.

### Ordering is lost for the message that failed

```
t=0    ord-42 seq 3  → handler fails → moved to the retry topic, offset committed
t=1    ord-42 seq 4  → arrives on the main topic → folded normally
                       fold.last_sequence is 2, so seq 4 is a GAP → warning
t=30   ord-42 seq 3  → retry worker succeeds → fold advances to 3
```

You will see this in the logs and it is **correct**:

```
inventory/... VIOLATION type=SEQUENCE_GAP order_id=ord-42 seq=4 expected=3 observed=4
```

The service really has not processed seq 3 yet. The fold is deliberately **not** advanced when
a handler fails — advancing it would record work that never happened, and would make the
eventual retry look like a duplicate to 003's sequence guard, which would silently discard it.

The warning is the visible price of not stalling the partition. Blocking retry would have
preserved the ordering and stalled everything behind it instead. **You pick one.**

---

## 5. The header contract

The headers are the entire value of the dead-letter topic. Without them it is a pile of bytes
nobody can act on.

| Header | Retry | DLQ | Meaning |
|---|:-:|:-:|---|
| `x-service` | ✓ | ✓ | the routing key — `inventory`, `notification`, `analytics` |
| `x-consumer-group` | ✓ | ✓ | which group gave up |
| `x-original-topic` / `-partition` / `-offset` | ✓ | ✓ | proves *which* message this was |
| `x-original-timestamp` | ✓ | ✓ | the broker timestamp of the original |
| `x-attempt` | ✓ | — | the attempt this publication schedules |
| `x-retry-at` | ✓ | — | ISO-8601 UTC; earliest that attempt may run |
| `x-attempts-made` | — | ✓ | `1` for poison, `RETRY_MAX_ATTEMPTS` for exhausted |
| `x-error-class` / `x-error-message` | ✓ | ✓ | what went wrong |
| `x-failed-at` | ✓ | ✓ | ISO-8601 UTC |

`x-original-offset` is the one that matters most: it is what makes a replay *provable* rather
than approximate. To read them from the CLI:

```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic order-lifecycle.dlq \
  --from-beginning --property print.headers=true --property print.key=true
```

---

## 6. The producer's half — closing 004's open gap

Spec 004 ended with a hole it named on purpose: with no `min.insync.replicas`, `acks=all` means
"every replica *currently in sync*" — and if the in-sync set has shrunk to one, one replica
satisfies it. An acknowledged write could exist in exactly one copy.

005 sets it to **2**, because honouring a refusal needs the producer-side retry path this
feature is about. `NOT_ENOUGH_REPLICAS` is a *retryable* error: librdkafka keeps trying until
`message.timeout.ms` runs out, and only then does the delivery report fail.

```bash
docker compose stop kafka-2 kafka-3       # ISR shrinks to 1, below the floor of 2
./scripts/place_orders.sh 1               # the write is REFUSED, not silently accepted
docker compose start kafka-2              # ISR back to 2 — writes resume, no restart
```

| Setting | librdkafka name | What it does |
|---|---|---|
| `PRODUCER_RETRIES` | `retries` | how many times a failed produce is retried |
| `PRODUCER_RETRY_BACKOFF_MS` | `retry.backoff.ms` | wait between those retries |
| `PRODUCER_MESSAGE_TIMEOUT_MS` | `message.timeout.ms` | **the binding one** — total time across all retries |

`message.timeout.ms` is librdkafka's name for what the Java client calls
`delivery.timeout.ms`. It caps the total, so `retries` alone cannot keep a message in flight
past it.

> **Producer retries can reorder messages.** Without `enable.idempotence`, a retried batch can
> land *behind* a batch produced after it — breaking the per-key ordering 001 depends on. This
> is not fixed here. It is spec **008**'s, where idempotent production makes the broker
> deduplicate and re-sequence on the way in.

`scripts/create_topics.sh` both passes `min.insync.replicas` at creation **and** `ALTER`s it
onto topics that already exist — because `--if-not-exists` skips an existing topic entirely,
config and all, and `order-lifecycle` was created back at 004. Without the second pass, closing
this gap would have cost another `docker compose down -v`.

---

## 7. The dead-letter topic is terminal, and that is the feature

**Nothing consumes it.** Not a service, not the retry worker, not a compose entry. Check:

```bash
grep -rn "DLQ_TOPIC" docker-compose.yml src/    # no subscribe() anywhere
```

A process that drains the dead-letter topic on a loop is an unbounded retry topic wearing a
different name: the message that could not be processed comes back, fails, and returns. You
would have rebuilt the problem this feature exists to solve, and lost the one property that
made the topic worth having — that it is terminal, and someone has to look.

So replay is a tool you run by hand, and it **reports** by default:

```bash
# look. changes nothing.
docker compose run --rm retry-worker python -m order_service.tools.dlq_replay

  poison-order-1    service=inventory  group=inventory-service  attempts=1  origin=order-lifecycle-1@94
      NonRetryableError: undecodable message: Expecting value: line 1 column 1 (char 0)

# only what one service gave up on
... python -m order_service.tools.dlq_replay --service inventory

# actually put them back — after fixing the cause
... python -m order_service.tools.dlq_replay --publish
```

The intended sequence is **read what failed → fix the cause → replay**. Replaying a message
whose cause was not fixed sends it straight back to the dead-letter topic. That round trip is
worth doing once deliberately; it is the demonstration of why auto-replay is wrong.

### Replay reaches every consumer group

Replay publishes to `x-original-topic` — `order-lifecycle`. All **three** groups read that
topic, so all three see the message again, not only the one that failed.

The two that already succeeded absorb it through 003's sequence guard:

```
notification/... DUPLICATE_ABSORBED order_id=ord-42 seq=3 stored_seq=5 handled=2
```

The fold does not move, because seq 3 is below the stored 5. But the *handler ran again* — that
is what `handled` counts. This is the at-least-once cost 003 recorded, arriving from a new
direction, and spec **008** is where it goes to zero.

---

## 8. Waiting without being evicted

The retry worker never calls `time.sleep()`. Sleeping past `max.poll.interval.ms` gets the
member evicted, its partition reassigned, and the message redelivered — the exact failure 002's
`HANDLER_DELAY_SECONDS` lever exists to demonstrate. A retry implemented with `sleep` would
loop forever.

Instead, a message that is not yet due gets its partition **paused** and its offset **sought
back to**:

```python
self._consumer.pause([TopicPartition(topic, partition)])
self._consumer.seek(tp)          # ← without this, the message is SKIPPED on resume
self._deferred[(topic, partition)] = due_at
```

The `seek` is load-bearing and easy to leave out. Pausing stops the fetch, but the client's read
position has already moved past the message. On resume it would be skipped, and the retry
silently dropped rather than delayed.

`poll()` keeps being called every iteration throughout — returning nothing for the paused
partition and everything for the others — so the member stays alive in its group.

---

## 9. Making all of it happen on demand

A failure path nobody can trigger is a failure path nobody has seen.

```bash
# fails attempts 1 and 2, succeeds on attempt 3 — watch it recover in the worker
HANDLER_FAILURE_MODE=transient HANDLER_FAILURE_ORDERS=ord-42 \
  docker compose up -d --force-recreate inventory-consumer retry-worker

# fails every attempt — dead on arrival, one attempt made, retry topic untouched
HANDLER_FAILURE_MODE=poison HANDLER_FAILURE_ORDERS=ord-42 \
  docker compose up -d --force-recreate inventory-consumer

# a genuinely malformed message — the DECODER fails, not a handler
./scripts/produce_poison.sh            # not JSON at all
./scripts/produce_poison.sh schema     # valid JSON, wrong shape
```

The lever and the script are not redundant. `HANDLER_FAILURE_MODE=poison` makes a *handler*
raise, which exercises the routing but proves nothing about the decoder — the bytes were fine
and the event parsed. `produce_poison.sh` writes bytes that can never become a `LifecycleEvent`,
which is the actual definition.

### The markers to grep for

```bash
docker compose logs -f inventory-consumer retry-worker \
  | grep -E 'RETRY_SCHEDULED|RETRY_WAITING|RETRY_SUCCEEDED|RETRY_EXHAUSTED|POISON_MESSAGE|DLQ_PUBLISHED'
```

| Marker | Where | Means |
|---|---|---|
| `RETRY_SCHEDULED` | both | moved to the retry topic; names the attempt and when it is due |
| `RETRY_WAITING` | worker | partition paused until this message is due |
| `RETRY_SUCCEEDED` | worker | a later attempt worked; the fold has advanced |
| `RETRY_EXHAUSTED` | worker | attempts spent; going to the dead-letter topic |
| `POISON_MESSAGE` | both | non-retryable; going to the dead-letter topic with 1 attempt |
| `DLQ_PUBLISHED` | both | it is in the dead-letter topic now |
| `FAILURE_PUBLISH_FAILED` | both | the *publication* failed; nothing committed, will be re-read |

---

## 10. What is still open

| Gap | Where it is closed |
|---|---|
| **Head-of-line blocking in the retry lane** | open by design — see below |
| A committed offset no longer means "processed" | **008** — one transaction covers publication and commit |
| `SEQUENCE_GAP` while a retry is in flight | inherent to non-blocking retry; the honest signal |
| Producer retries can reorder | **008** — `enable.idempotence` |
| `DUPLICATE_ABSORBED` on replay | **008** — exactly-once |
| Dead letters expire with the topic's default retention | never claimed; no criterion sets retention |
| Nothing alerts on dead-letter depth | out of scope — see below |
| One worker means one service's backoff holds up the others' | accepted; the fix is a worker per service |
| Unclean leader election, committed-data loss | still open from 004 |

### Head-of-line blocking, one lane over

One retry topic carrying per-message delays can invert:

```
retry partition 0:  [ A: due t+120s ][ B: due t+30s ][ C: due t+30s ]
                       ↑ the worker waits here — B and C sit behind it for 90s
```

Watch it happen:

```bash
RETRY_BACKOFF_SECONDS=120,5 docker compose up -d --force-recreate retry-worker
```

The production fix is **tiered delay topics**: one topic per delay value, so every message in a
topic waits the same span from its own produce time and the head is always the soonest due.
That would be `order-lifecycle.retry.30s` and `order-lifecycle.retry.120s`.

It is deliberately not built. Keeping it open is what makes "this is why tiered topics exist" an
observation rather than a claim. It is softened by the retry topic having three partitions —
three independent lanes, and only the partition whose head is not due gets paused.

### Nobody is watching the dead-letter topic

There is no alert on its depth, and in production that is the difference between a dead-letter
topic and a silent data-loss bucket. `messages_in > 0` on `order-lifecycle.dlq` should page
someone. Here, you look:

```bash
docker exec kafka /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 --topic order-lifecycle.dlq
```
