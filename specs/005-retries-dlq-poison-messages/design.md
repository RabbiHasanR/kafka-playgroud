# 005 — Retries, Dead-Letter Topic, and Poison Messages: Design

Implements [requirements.md](requirements.md). Decisions are numbered `D<n>` and cite the
criteria they satisfy. Cross-feature decisions live in [DECISIONS.md](../../DECISIONS.md).

**On the size budget.** [X11](../../DECISIONS.md) budgets a feature at roughly 12–15 criteria;
this one has 24. The overrun is D12 — closing 004's deferred `min.insync.replicas` gap adds a
producer-side lesson on top of a consumer-side feature that was already two mechanisms (a retry
topic and a dead-letter topic) plus a replay tool. X11 permits the overrun with this sentence,
which covers this file's length for the same reason.

## Architecture

Two new topics, one new process. The three service consumers are unchanged in shape — they gain
a failure branch, not a new loop.

```
order-lifecycle ──► inventory / notification / analytics   (3 groups, unchanged)
                          │
          ┌───────────────┴───────────────┐
    RetryableError                  NonRetryableError
    (attempt 1 spent)               (poison — 0 attempts)
          │                                │
          ▼                                │
  order-lifecycle.retry                    │
   x-service, x-attempt=2, x-retry-at      │
          │                                │
          ▼                                │
     retry-worker  ── one container, group `retry-worker`,
          │            subscribes to the retry topic only
     due? → SERVICE_REGISTRY[x-service] → that service's handler
          │                                │
     success → fold + commit               │
     attempt 3 fails ──────────────┬───────┘
                                   ▼
                         order-lifecycle.dlq
                          x-consumer-group=inventory-service
                                   │
                                   ▼
                    dlq_replay (manual, reports by default)
                                   ▼
                           order-lifecycle
```

## Decisions

### D1 — One retry topic, one worker, routing by header — *R5.5, R5.9, R5.12*

A shared retry topic would be wrong if the three *service* consumers subscribed to it: a message
only inventory failed on would reach notification and analytics, which already succeeded on it.
They do not subscribe. `retry-worker` is a single consumer group that owns the retry topic and
dispatches on the `x-service` header through the existing `SERVICE_REGISTRY` in
`consumer/main.py`. One topic, one dict lookup, no fan-out.

It is a **separate process** because waiting is the one thing the main consumers must never do,
and a separate process is the only arrangement in which its waiting cannot reach them.

Rejected: a retry topic per service — routing on `msg.topic()` instead of a header, no cheaper,
and six topics once D14's tiers arrive. Also rejected: a worker per service — full lane isolation
for three more containers on top of the six already in compose.

### D2 — Classification is one function, and the unknown case is retryable — *R5.1, R5.2, R5.3, R5.4*

`consumer/errors.py` declares `RetryableError` and `NonRetryableError`, and `classify(exc)`
returns one of them for any exception. The two mistakes are not symmetric: a handler bug
misfiled as retryable costs three attempts and reaches the dead-letter topic anyway, while a real
outage misfiled as poison is discarded on the first attempt. The cheaper mistake is the default.

`Handler` in `runtime.py` loses its *"raising is not part of the handler contract"* comment. The
decode branch of `_handle_message`, which today logs and commits, raises `NonRetryableError`
instead — the same classification a schema violation gets, since both mean the bytes are wrong.

### D3 — The message moves before the offset commits; the offset commits before the retry runs — *R5.6, R5.7, R5.14*

Two writes to two systems again, exactly 003 D4's shape, and the ordering argument is the same.
Publish to the retry (or dead-letter) topic, **wait for the broker's acknowledgement**, then
commit the source offset. Commit first and a crash in the window loses the message with nothing
logged.

If the publication itself fails, the source offset is not committed and the message is
redelivered, spending its attempt again. At-least-once, unchanged.

*Amended during implementation:* "not committed" is not sufficient on its own — the client's
read position has already advanced, so the message would be skipped for the rest of the session
and only reappear after a restart. Both loops therefore log `FAILURE_PUBLISH_FAILED` and **seek
back** to the message, so the next `poll()` redelivers it. While the broker is unreachable this
spins, once per handler duration, with an ERROR line each time; a visible spin was preferred to
an in-session silent gap.

The commit does **not** wait for the retry to be attempted — that is what R5.7 buys, and it is
why a committed offset stops meaning "processed" from this feature onward.

### D4 — Waiting is `pause` + `poll` + `seek`, never `sleep` — *R5.8*

`time.sleep()` in a poll loop is the footgun 002's `handler_delay_seconds` lever exists to
demonstrate: sleep past `max.poll.interval.ms` and the broker evicts the member, the partition is
reassigned, the message is redelivered, and the worker loops forever. So the worker never sleeps.

On reading a message that is not yet due it **pauses the partition, then seeks back to that
message's offset**, and records when to resume. The seek is load-bearing: without it the
client's fetch position has already advanced and the message would be skipped on resume.
`poll()` keeps being called every iteration — returning nothing, which is the point — so the
member stays alive, and partitions whose due time has passed are resumed at the top of each
iteration.

*Amended during implementation:* this decision originally said seek-then-pause. Seeking a
partition that is actively fetching races with the fetcher, and a paused partition has no
fetcher to race with, so the order is reversed.

### D5 — The worker holds one state store per service, keyed by that service's group — *R5.9, R5.11*

`PostgresStateStore` takes a `group_id` (003 R3.2), so the worker builds three of them —
`inventory-service`, `notification-service`, `analytics-service` — and picks by `x-service`. A
retry that succeeds therefore lands in exactly the rows the main consumer would have written.
Storing under a `retry-worker` group instead would split each service's memory across two keys.

*Amended during implementation:* `PostgresStateStore`'s read-through cache is licensed by "a
partition belongs to exactly one member at a time" (003), and **this worker breaks that
invariant** — it is a second reader of orders the main consumer is still advancing, so a cached
fold here can be stale. The worker therefore calls `store.forget([partition])` immediately
before every `load`, turning each read into a real one. That uses 003's existing API and needs
no change to it; a `cache=False` constructor flag was rejected as a change to an approved
feature for one caller's benefit.

### D6 — A failed handler leaves the fold unchanged — *R5.11*

The fold means "what this service has actually processed". Advancing it on failure would record
work that never happened, and would make the eventual retry look like a duplicate to the sequence
guard — silently discarding it.

The visible consequence is new `SEQUENCE_GAP` warnings: the next event for that order arrives on
the main topic while the failed one is still in the retry lane, and 001's R1.38 fires correctly.
That warning is the price of R5.7 rather than a defect, and R5.24's document must say so.

### D7 — The header contract — *R5.5, R5.13*

| Header | Retry | DLQ | Meaning |
|---|:-:|:-:|---|
| `x-service` | ✓ | ✓ | routing key (D1); `inventory` \| `notification` \| `analytics` |
| `x-consumer-group` | ✓ | ✓ | the group that gave up |
| `x-original-topic` / `-partition` / `-offset` | ✓ | ✓ | proves which message this was |
| `x-original-timestamp` | ✓ | ✓ | broker timestamp of the original |
| `x-attempt` | ✓ | — | 1-based, the attempt this publication schedules |
| `x-retry-at` | ✓ | — | ISO-8601 UTC; earliest the next attempt may run |
| `x-attempts-made` | — | ✓ | 1 for poison (attempt 1 was spent inline), `RETRY_MAX_ATTEMPTS` when exhausted |
| `x-error-class` / `x-error-message` | ✓ | ✓ | what went wrong |
| `x-failed-at` | ✓ | ✓ | ISO-8601 UTC |

All values are UTF-8 ASCII. Timestamps are ISO-8601 rather than epoch millis to match
`occurred_at` in `events.py` and stay readable under `--property print.headers=true`. The message
key stays `order_id` throughout, so an order's retries and its dead letters keep co-partitioning.

Without these the dead-letter topic is a pile of bytes nobody can act on; `x-original-offset` is
what makes a replay provable rather than approximate.

### D8 — Attempt 1 is inline; attempts 2 and 3 are the worker's — *R5.10*

`RETRY_MAX_ATTEMPTS=3` counts the *first* try. The main consumer spends attempt 1 with no delay,
so a handler that succeeds normally never touches the retry topic. `RETRY_BACKOFF_SECONDS=30,120`
supplies attempts 2 and 3, indexed by `x-attempt - 1`; a list shorter than the attempt budget
reuses its last entry rather than failing.

### D9 — Four markers, WARNING or above — *R5.16*

`RETRY_SCHEDULED`, `RETRY_EXHAUSTED`, `POISON_MESSAGE`, `DLQ_PUBLISHED`, following the
`VIOLATION` / `COMMIT_REJECTED` / `DUPLICATE_ABSORBED` convention already in `runtime.py`: a
stable uppercase token first, then `key=value` fields, so one `grep` answers the question.

### D10 — The dead-letter topic is terminal; replay is a separate tool that reports by default — *R5.15, R5.17, R5.18*

Nothing in `docker-compose.yml` consumes the dead-letter topic. `order_service/tools/dlq_replay.py`
is run by hand, reads with `enable.auto.commit=False` and commits nothing, prints what it would
republish, and publishes only under an explicit flag. `--service` narrows it to one group's
failures via `x-service`.

A process that drains the topic on a loop is an unbounded retry topic wearing a different name.
The topic's value is that it is terminal and someone has to look, so the default is to look.

Republishing goes to `x-original-topic`, which means **all three groups** re-consume it. The two
that already succeeded absorb it through 003's sequence guard and log `DUPLICATE_ABSORBED` — the
at-least-once cost 003 recorded, arriving from a new direction, and driven to zero at 008.

### D11 — The failure lever is environment-driven, plus one genuinely malformed message — *R5.19*

`HANDLER_FAILURE_MODE` (`none` | `transient` | `poison`) with `HANDLER_FAILURE_ORDERS` naming
which orders and `HANDLER_FAILURE_ATTEMPTS` saying how many attempts `transient` fails before
succeeding. A `StrEnum` for the reason `GroupProtocol` and `ProducerAcks` are: an unrecognised
value must fail at startup, not select a behaviour quietly. This is 005's counterpart to 002's
`handler_delay_seconds` and 003's `state_crash_after`.

Simulating a *decode* failure this way would be dishonest, so `scripts/produce_poison.sh` writes
raw non-JSON bytes with `kafka-console-producer`. That exercises D2's decode branch with a real
malformed message rather than a handler pretending to be one.

### D12 — `min.insync.replicas` is altered onto the topic, not only passed at creation — *R5.20, R5.21, R5.22*

`create_topics.sh` passes `--config min.insync.replicas=$MIN_INSYNC_REPLICAS` on create **and**
follows with `kafka-configs.sh --alter --add-config`, because `--if-not-exists` silently skips an
existing topic and the config would never land on the `order-lifecycle` that 004 already created.
Without the alter, closing this gap would cost another `docker compose down -v`.

`PRODUCER_RETRIES`, `PRODUCER_RETRY_BACKOFF_MS` and `PRODUCER_MESSAGE_TIMEOUT_MS` map to
librdkafka's `retries`, `retry.backoff.ms` and `message.timeout.ms`. The last is the binding one:
it bounds the *total* time including retries, and is librdkafka's spelling of the Java client's
`delivery.timeout.ms`. With two brokers stopped, a write fails `NOT_ENOUGH_REPLICAS`, is retried
until that timeout, then surfaces through 004 D7's delivery-failure path, which already names the
partition. The banner line joins R4.8's `acks` line in `producer/app.py`, for 004 D6's reason.

**Producer retries can reorder.** Without `enable.idempotence`, a retried batch can land behind a
later one. Named in the document and left to 008, which is where idempotent production belongs.

### D13 — Defaults preserve 004, with one stated exception — *R5.23*

Every variable D8, D11 and D12 introduce defaults to the behaviour 004 recorded. The exception is
unavoidable and is written into R5.23: a handler that fails now retries, where before this feature
a handler could not fail at all. `min.insync.replicas` defaults to 2 rather than unset, which is
the gap being closed and therefore a deliberate change in behaviour, not a regression.

### D14 — Head-of-line blocking in the retry lane is left open — *R5.24*

One retry topic carrying per-message delays can invert: a message due in 120s sits at the head of a
partition and stalls messages behind it that were due in 30s. The fix is **tiered delay topics** —
one topic per delay value, so every message in a topic waits the same span from its own produce
time and the head is always the soonest due.

Not built, for 004 D8's reason: keeping it open makes the stall observable, which is the only way
"this is why tiered topics exist" lands as an observation rather than a claim. Softened by the
retry topic having 3 partitions — three independent lanes, and D4 pauses only the partition whose
head is not due.

## Environment surface

| Variable | Default | Read by | Criteria |
|---|---|---|---|
| `RETRY_TOPIC` | `order-lifecycle.retry` | consumers, worker | R5.5 |
| `DLQ_TOPIC` | `order-lifecycle.dlq` | consumers, worker, replay | R5.12 |
| `RETRY_MAX_ATTEMPTS` | `3` | consumers, worker | R5.10 |
| `RETRY_BACKOFF_SECONDS` | `30,120` | consumers, worker | R5.10 |
| `HANDLER_FAILURE_MODE` | `none` | consumers, worker | R5.19 |
| `HANDLER_FAILURE_ORDERS` | *(unset)* | consumers, worker | R5.19 |
| `HANDLER_FAILURE_ATTEMPTS` | `2` | consumers, worker | R5.19 |
| `MIN_INSYNC_REPLICAS` | `2` | `create_topics.sh` | R5.20 |
| `PRODUCER_RETRIES` | `3` | producer | R5.21 |
| `PRODUCER_RETRY_BACKOFF_MS` | `100` | producer | R5.21 |
| `PRODUCER_MESSAGE_TIMEOUT_MS` | `30000` | producer | R5.21, R5.22 |

No credentials; all eleven are non-sensitive.

## Known gaps, by intent

| Gap | Status |
|---|---|
| Head-of-line blocking in the retry lane | open by design (D14) — tiered topics named in the doc |
| A committed offset no longer means "processed" | inherent to R5.7 (D3); closed at 008 |
| `SEQUENCE_GAP` warnings while a retry is in flight | inherent to D6; the honest signal, not noise |
| Dead-letter messages expire with the topic's default retention | never claimed; no criterion sets retention |
| Nothing alerts on dead-letter depth | out of scope; named in the doc as the operational half |
| Producer retries can reorder without idempotence | open (D12); closed at 008 |
| One worker means one service's backoff holds the others' | accepted (D1); fix is a worker per service |
| Unclean leader election, committed-data loss | still open from 004 |

## Deferred to later specs

**008** owns all three of this feature's open costs at once: a transaction covering the retry
publication and the source offset commit closes D3's window, `enable.idempotence` closes D12's
reordering, and exactly-once removes the `DUPLICATE_ABSORBED` D10's replay produces.
