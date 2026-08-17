# Threads, the event loop, and how confluent-kafka really sends and receives

A short reference for the concurrency machinery this repo sits on. Every concept is
anchored to code in this repository, so read it with the files open.

**Part I (§1–7)** is the producer: a web service that is *called*, and publishes.
**Part II (§8–14)** is the consumer: a bare process that *calls*, and reads. §2–4 are
generic, and §15–16 recap both.

---

> **PART I — THE PRODUCER**

## 1. The whole picture first

One `POST /orders/{order_id}/events` request touches four different execution contexts:

```
                  ┌──────────────── producer process ────────────────┐
   HTTP request   │                                                  │
   ──────────────▶│  [uvicorn event loop]   ← 1 thread, never blocks │
                  │          │                                       │
                  │          │ route is `def`, so hand it off        │
                  │          ▼                                       │
                  │  [threadpool worker]    ← anyio pool             │
                  │      produce()  ──────┐                          │
                  │      done.wait() ⏸    │  (only enqueues)         │
                  │                       ▼                          │
                  │              [librdkafka queue]  ← C memory      │
                  │                       │                          │
                  │              [C broker threads] ─────────────────┼──▶ Kafka
                  │                       │                          │
                  │              delivery report ◀───────────────────┼─── ack
                  │                       │                          │
                  │  [poll thread] ── poll(0.1) fires the callback   │
                  │          │                                       │
                  │          └─▶ done.set() ⏵ worker wakes           │
                  │                       │                          │
   HTTP response ◀│───────────────────────┘                          │
                  └──────────────────────────────────────────────────┘
```

Four contexts: **event loop**, **threadpool worker**, **librdkafka's C threads**, and
**our poll thread**. Everything below zooms into one of those boxes.

---

## 2. OS thread, Python thread, process

A `threading.Thread` **is** a real OS thread — the kernel schedules it, it can land on
another CPU core. What limits it is the **GIL**: only one thread runs Python bytecode at
a time.

The part people miss: the GIL is **released** during I/O (socket reads, `sleep`) and
inside C extension code. That is the entire reason this works:

```python
# src/order_service/producer/kafka_producer.py:62-64
self._poll_thread = threading.Thread(
    target=self._poll_loop, name="order-service-poll", daemon=True
)
self._poll_thread.start()
```

`_poll_loop` spends its life inside `self._producer.poll(0.1)` — C code, GIL released.
It costs almost no Python execution time yet keeps running while request threads work.

`daemon=True` means the interpreter will not wait for it at exit; we shut it down
explicitly in `stop()` instead, because we want `flush()` to happen first.

**A process** is a different beast: separate memory, its own GIL, no shared objects.
Which is why this line matters:

```python
# src/order_service/producer/orders.py:111
self._orders: dict[str, Order] = {}
```

This dict is process-local, and each `Order` carries its own `last_sequence` (**D5** —
the sequence lives on the order, not on the producer). Run two order-service processes
and neither sees the other's orders; restart one and a pre-restart order is gone
entirely, so advancing it returns `404`. Deliberate, and the reason a restart loses
orders.

| | shares memory | GIL | good for |
|---|---|---|---|
| Thread | yes | shared | blocking I/O, waiting |
| Process | no | its own | CPU-bound work |
| C-extension thread | yes (C side) | runs without it | librdkafka's networking |

---

## 3. Thread pool

Creating a thread per unit of work is expensive and unbounded — 10,000 requests would
mean 10,000 threads. A **pool** keeps a fixed set of threads pulling from a work queue:
creation cost paid once, concurrency capped.

FastAPI uses anyio's threadpool (~40 workers by default) for every `def` route. You never
see it in this codebase — that is the point, it is implicit.

The naive alternative is `ThreadingHTTPServer` from the standard library, which is
**thread-per-request** — no pool, no cap:

```python
server = ThreadingHTTPServer((host, port), handler)
threading.Thread(target=server.serve_forever, daemon=True).start()
```

Fine for a debug endpoint hit by a human; behind real traffic it is a memory bomb. The
order service uses FastAPI's pooled threads instead, which is why nothing like this
appears in `src/`.

---

## 4. Event loop: `def` vs `async def`

The event loop is **one thread running one task at a time**. It switches tasks only at an
`await`. That is not a limitation — while task A waits on a socket, the loop runs B, C, D.
Thousands of concurrent waits, one thread.

The rule that follows:

| Route style | Where it runs | Blocking allowed? |
|---|---|---|
| `async def` | on the event loop | **no** — freezes every request in the process |
| `def` | handed to the threadpool | yes — it blocks only its own worker |

So this signature is a decision, not a typo:

```python
# src/order_service/producer/routes.py:204-209
@router.post("/orders/{order_id}/events", response_model=PublishEventResponse)
def publish_event(                              # ← `def`, not `async def`
    order_id: str,
    body: PublishEventRequest,
    request: Request,
) -> PublishEventResponse:
```

Because underneath it does this:

```python
# src/order_service/producer/kafka_producer.py:130, 145
done = threading.Event()
...
if not done.wait(wait):     # blocks the calling thread
```

`threading.Event.wait()` blocks hard. On the event loop it would stall the whole process;
on a threadpool worker it stalls one worker. That pairing — blocking wait + synchronous
route — **is decision D6**.

The async escape hatch exists:

```python
result = await asyncio.to_thread(producer.publish_and_wait, event)
```

…but `to_thread` just pushes the work onto the same threadpool. Same cost, extra
indirection. D6 skipped it because nothing else in the handler is async.

---

## 5. How confluent-kafka connects to the broker

`confluent-kafka` is a thin Python binding over **librdkafka**, a C library (decision
**X1**). The Python objects are handles; the actual protocol work happens in C.

```python
# src/order_service/producer/kafka_producer.py:42-51
self._producer = Producer(
    {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "acks": "all",
        "partitioner": "consistent_random",
        "client.id": "order-service-producer",
    }
)
```

**This constructor does no network I/O on your thread, and never blocks.** It builds
config and starts librdkafka's own threads. What happens next, all in C, without any
Python involvement:

1. An internal `rdk:main` thread contacts a `bootstrap.servers` address, negotiates
   protocol versions (**ApiVersions**), and fetches **cluster metadata** — the full
   broker list, topics, partitions, and which broker leads each one.
2. It opens a **dedicated thread and TCP connection per broker** it needs.
   Bootstrap is only a starting point; the client talks to leaders directly afterwards.
3. Those threads handle batching, compression, retries, reconnects, and metadata refresh
   on their own timers (default every 5 minutes, plus on demand after a leader change).

Because it is asynchronous, a wrong broker address does not raise at construction — it
surfaces much later as a timeout. The one place we force a real round-trip:

```python
# src/order_service/producer/kafka_producer.py:96-98
metadata = self._producer.list_topics(timeout=timeout)
topic = metadata.topics.get(self._settings.order_lifecycle_topic)
return topic is not None and topic.error is None
```

That is a genuine metadata request with a timeout, which is why `publish_and_wait` calls
it to distinguish "topic missing" from "broker unreachable".

---

## 6. The internal queue: why `produce()` returns instantly

```python
# src/order_service/producer/kafka_producer.py:174-179
self._producer.produce(
    topic=self._settings.order_lifecycle_topic,
    key=event.order_id.encode("utf-8"),
    value=event.model_dump_json().encode("utf-8"),
    on_delivery=on_delivery,
)
```

`produce()` copies the message into librdkafka's **in-memory queue in C** and returns.
No network I/O happens on your thread. The C threads pick it up, batch it, send it, and
the broker's ack arrives later.

Three consequences, each visible in the code:

**(a) The queue is finite.** Produce faster than the network drains and it fills:

```python
# src/order_service/producer/kafka_producer.py:180-181
except BufferError as exc:
    raise DeliveryFailed(f"producer queue is full: {exc}") from exc
```

**(b) Callbacks only fire while you call `poll()`.** librdkafka will not call into Python
from its own C threads — it queues the delivery reports and hands them over only when your
code asks. That is what this loop is for:

```python
# src/order_service/producer/kafka_producer.py:100-103
def _poll_loop(self) -> None:
    while not self._poll_stop.is_set():
        self._producer.poll(0.1)
```

`poll(0.1)` = "serve pending callbacks, wait up to 100 ms if there are none". It runs on
its own thread so callbacks fire regardless of request traffic (decision **D6**), owned by
the app lifespan:

```python
# src/order_service/producer/app.py:29-30, 45
producer = LifecycleEventProducer(settings)
producer.start()          # poll thread up before the first request
...
producer.stop()           # flush, then stop the thread
```

**(c) Exiting without flushing loses the tail.** Whatever is still in the queue dies with
the process:

```python
# src/order_service/producer/kafka_producer.py:74
remaining = self._producer.flush(flush_timeout)
```

`flush()` blocks until the queue drains — and it polls internally, so callbacks fire
during it too.

---

## 7. Why the poll thread is not optional

Follow the deadlock if you delete it:

1. Request thread calls `produce()`, then `done.wait(5.0)`.
2. Broker receives the message and sends the ack. **The message is fine.**
3. librdkafka queues the delivery report and waits for someone to call `poll()`.
4. Nobody does — the only other thread is blocked in `done.wait()`.
5. Five seconds pass. `on_delivery` never ran, so `done` was never set.
6. The client returns **504 delivery timeout** for a message that was successfully
   published.

Blocking on the delivery report is only safe because a thread is servicing callbacks.
They are one decision (**D6**) with two halves, and dropping either half hangs the
request.

A bulk endpoint would sidestep the whole issue by not waiting per event — one broker
round-trip per message caps throughput at roughly one message per round-trip, which is
why load generators fire and forget and tally the reports on the callback instead. The
order service has no such endpoint; it publishes one event per request and waits, and
that is the right trade when a human is holding the other end.

---

> **PART II — THE CONSUMER**

## 8. The whole picture first, again

The consumer is the same library with the arrows reversed — and one box fewer:

```
                  ┌──────────────── consumer process ────────────────┐
                  │                                                  │
                  │  [main thread]  ← the only Python thread         │
                  │       │                                          │
                  │       │  while self._running:                    │
                  │       ▼                                          │
                  │    poll(1.0) ──── pops one message ──┐           │
                  │       │                              │           │
                  │       ▼                              │           │
                  │    handler(event)                    │           │
                  │       │                              │           │
                  │    commit() ─────────────────────────┼───────────┼──▶ Kafka
                  │                                      │           │
                  │                          [librdkafka fetch queue]│
                  │                                      ▲           │
                  │              [C broker threads] ─────┘           │
                  │                    fetching continuously ◀───────┼─── records
                  │              [rdk:main] heartbeats, rebalances   │
                  └──────────────────────────────────────────────────┘
```

Compare with §1. **No event loop. No threadpool. No dedicated poll thread.** Three
contexts instead of four: your main thread, `rdk:main`, and the broker threads.

Everything below is [`ServiceConsumer`](../src/order_service/consumer/runtime.py#L147)
and its entry point, [`main.py`](../src/order_service/consumer/main.py).

---

## 9. Why there is no FastAPI here

The producer is **reactive**: nothing happens until someone calls it, so it needs a
socket, a router, and status codes. The consumer is **proactive** — nobody calls it, it
calls Kafka. Its entire life is one loop:

```python
# src/order_service/consumer/runtime.py:198-205
while self._running:
    message = self._consumer.poll(1.0)
    if message is None:
        continue
    if message.error():
        self._handle_error(message)
        continue
    self._handle_message(message)
```

No inbound socket, no request, no response. A web framework would add an event loop, an
ASGI server, and a port binding around a `while` statement. **The broker is the
consumer's web server**: consumer groups already provide work distribution (partition
assignment), backpressure (you poll at your own pace), retry (uncommitted offsets are
redelivered), and liveness (group membership).

This also settles §7. The producer needs a *separate* poll thread because its Python
thread is busy blocking in `done.wait()`. **The consumer's main loop already is the poll
loop** — same requirement, satisfied by the structure rather than by an extra thread.

| | producer | consumer |
|---|---|---|
| who initiates | an HTTP caller | this process |
| Python threads | event loop + ~40 workers + poll thread | **one** |
| who calls `poll()` | a dedicated background thread | the main loop itself |
| lifecycle owner | FastAPI lifespan | `signal.signal` in `main()` |

---

## 10. What `subscribe()` really does

```python
# src/order_service/consumer/runtime.py:186
self._consumer.subscribe([topic])
```

**This call performs no network I/O and returns in microseconds.** It records the
subscription and signals `rdk:main`. That is all.

Everything real happens afterwards on C threads, while your loop is already spinning.
The producer's startup is steps 1–2; the consumer adds four more.

**This table describes the `classic` group protocol**, which is the default and what
spec 001 uses throughout. Kafka 4.0 added a second protocol that replaces steps 4–5
entirely — see *"Two protocols"* below.

| # | Request | Who answers | Purpose |
|---|---|---|---|
| 1 | ApiVersions | bootstrap broker | negotiate protocol versions |
| 2 | Metadata | bootstrap broker | brokers, topics, partitions, leaders — then **cached** |
| 3 | **FindCoordinator** | bootstrap broker | which broker owns this `group.id` |
| 4 | **JoinGroup** | coordinator | get a `member.id` + generation; one member is elected leader |
| 5 | **SyncGroup** | coordinator | the elected *client* computes the assignment and uploads it |
| 6 | **OffsetFetch** | coordinator | where did this group leave off? |
| 7 | Fetch | **partition leaders** | the actual records, long-polled continuously |

Steps 3–6 are consumer-only. Three details worth keeping:

**The coordinator is chosen by hashing the group id.** Kafka computes
`murmur2(group.id) % 50` to pick a partition of the internal `__consumer_offsets` topic;
that partition's leader is the group's coordinator, and it stores both membership and
committed offsets. Because each service has its own `group.id`, each gets independent
state — which is what makes **D7**'s fan-out work and why stopping one service cannot
affect the others.

**Under the classic protocol the broker does not compute the assignment — a client
does.** In step 4 the coordinator elects one member as leader; in step 5 that member runs
the assignor and uploads the result for everyone. With one consumer per group and three
partitions, that member is assigned all three.

### Two protocols, as of Kafka 4.0

Spec 002 exercises both, selected by `CONSUMER_GROUP_PROTOCOL` (**X9**). The difference is
exactly *who runs the assignor*, and it changes what a rebalance costs.

| | `classic` (default) | `consumer` — KIP-848 |
|---|---|---|
| Who computes the assignment | an elected **client** | the **broker** |
| Handshake | JoinGroup + SyncGroup (steps 4–5) | ConsumerGroupHeartbeat |
| Assignor knob | `partition.assignment.strategy` | `group.remote.assignor` |
| Session timeout | client-side `session.timeout.ms` | **broker-side** — sending it raises |
| Revocation | eager by default; `cooperative-sticky` opts out | incremental by design |

The classic protocol's default assignors (`range`, `roundrobin`) are **eager**: every
member surrenders every partition before the new assignment is computed, even partitions
that were never going to move. `cooperative-sticky` and KIP-848 revoke only what actually
changes hands.

Measured in 002: under `range`, killing one member cost **6 of 6** in-flight orders their
folded state; under `cooperative-sticky` the same scenario cost **3 of 9** — exactly the
orders on the one partition that changed owner.

Two operational notes that are easy to meet the hard way: an existing group **cannot be
switched between protocols in place** (every member gets `ConsumerGroupHeartbeat fatal
error: Broker: The group id does not exist` and exits — delete the group first), and a
member joining with a different assignor than the rest of the group is rejected with
`INCONSISTENT_GROUP_PROTOCOL` until the group converges.

Full detail and the runnable comparisons: [consumer-groups.md](consumer-groups.md).

**`auto.offset.reset` only applies at step 6, and only once.** If OffsetFetch returns
`-1` (no committed offset for this group), the client sends a `ListOffsets` request and
starts at `earliest`. Once anything has been committed, the setting is never consulted
again — which is why changing `CONSUMER_GROUP_ID` to an unused value replays the topic
from the beginning (**D12**).

```python
# src/order_service/consumer/runtime.py:156-168
self._consumer = Consumer(
    {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": self._group_id,          # → steps 3-6 exist at all (D7)
        "enable.auto.commit": False,         # → §13 (D10)
        "auto.offset.reset": "earliest",     # → consulted only at step 6
        "client.id": f"order-service-{spec.name}",
    }
)
```

Like the producer, none of this blocks or raises. Start with the broker down and both
`Consumer(...)` and `subscribe(...)` still succeed — you find out from `poll()`.

---

## 11. The fetch queue — §6 in reverse

The producer's `produce()` appends to a C-side queue that broker threads drain
*outward*. The consumer is the mirror: broker threads fill a C-side queue *inward*, and
`poll()` drains it.

```
PRODUCER                              CONSUMER
your thread                           C broker thread
    │ produce()                           │ Fetch response arrives
    ▼                                     ▼
┌─────────────┐                     ┌─────────────┐
│ send queue  │  ← C memory →       │ fetch queue │
└─────────────┘                     └─────────────┘
    │ C broker thread                     │ poll()
    ▼ batches & sends                     ▼ your Python thread
  broker                                your handler
```

The broker threads keep Fetch requests permanently in flight; they do not wait for you.
Each response is decompressed and split into a **per-partition queue**, and those are
forwarded into one queue that `poll()` reads. With three partitions you have three fetch
queues feeding one loop, which is exactly why every log line carries `partition=`:

```python
# src/order_service/consumer/runtime.py:247-256
logger.info(
    "[%s] partition=%d offset=%d key=%s order_id=%s seq=%d type=%s", ...
)
```

Ordering is guaranteed **within** a partition, never across them.

Prefetching is aggressive: by the time your loop asks for message #1, thousands may
already be in memory. The knobs mirror each other exactly:

| | producer | consumer |
|---|---|---|
| queue depth | `queue.buffering.max.messages` (100 000) | `queued.min.messages` (100 000) |
| queue bytes | `queue.buffering.max.kbytes` (~1 GB) | `queued.max.messages.kbytes` (64 MB) |
| batch timing | `linger.ms` (5 ms) | `fetch.wait.max.ms` (500 ms) |
| batch trigger | `batch.num.messages` (10 000) | `fetch.min.bytes` (1) |
| per-request cap | `message.max.bytes` (1 MB) | `max.partition.fetch.bytes` (1 MB) |

**The one real asymmetry is what "full" means.**

Producer-full pushes back *into your code* — the `BufferError` of §6(a), which becomes a
`502`. Consumer-full pushes back *out to the broker*: librdkafka simply stops issuing
Fetch requests for that partition until you drain some. No exception, nothing to handle,
which is why the consume loop has no equivalent branch. A slow consumer is not an error;
it is a consumer that fetches less often.

---

## 12. What `poll(1.0)` actually does

Given §11, `poll()` is much less than it looks:

> **Pop one item off the local C queue, waiting up to 1.0 s for one to appear — and
> serve any pending callbacks.**

It does **not** send a Fetch. It does **not** send a heartbeat. `rdk:main` and the broker
threads already did both. `poll()` is the handoff point between C threads and Python.

| | thread | does |
|---|---|---|
| `rdk:main` | background, C | metadata refresh, group state machine, heartbeats, timers |
| `rdk:broker-N` | background, C | socket I/O — Fetch requests, responses, filling the queue |
| **`poll()`** | **your Python thread** | dequeues one message; runs callbacks |

Two consequences carried straight over from §2 and §6(b):

**It releases the GIL.** The binding wraps the call in `Py_BEGIN_ALLOW_THREADS`, so that
one-second wait costs no Python execution time — the same property that makes the
producer's poll thread nearly free.

**Callbacks run on your thread, inside `poll()`.** A `rebalance_cb` or `on_commit` is
queued by the C threads and invoked only when you poll — identical to `on_delivery` in
§6(b). librdkafka never calls into Python from its own threads.

Three return shapes, and the loop handles each:

| return | meaning | line |
|---|---|---|
| `None` | queue empty for 1.0 s — **not** "topic is empty" | 200 |
| `Message` with `.error()` | a signal, not data | 202 |
| `Message` with data | the real thing | 205 |

The `1.0` is also the shutdown granularity. `main()` traps signals and only flips a
boolean:

```python
# src/order_service/consumer/main.py:65-70
def shutdown(signum: int, _frame: FrameType | None) -> None:
    logger.info("[%s] signal %d received, shutting down", spec.name, signum)
    consumer.stop()          # sets self._running = False, nothing more

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)
```

A signal handler interrupts the main thread at an arbitrary bytecode boundary, so doing
real work there — closing a socket, committing an offset — risks doing it mid-message.
Setting a flag is safe; the loop notices within one second and unwinds normally.

**The heartbeat nuance.** In the *Java* client heartbeats piggyback on `poll()`, so a
slow handler gets you evicted. In librdkafka, `rdk:main` heartbeats on its own timer
regardless. Instead the client enforces **`max.poll.interval.ms`** (default 5 min): if
your application has not polled within that window, librdkafka leaves the group itself
and surfaces `_MAX_POLL_EXCEEDED`. Academic while handlers only log — but a handler that
made a real network call would meet it.

---

## 13. `commit()` is the consumer's delivery report

This is the symmetry that makes both halves one story.

```python
# src/order_service/consumer/runtime.py:276
self._consumer.commit(message=message, asynchronous=False)
```

An `OffsetCommit` to the **coordinator** (not the partition leader), appending a record
to `__consumer_offsets`. `asynchronous=False` blocks until the broker acknowledges — the
mirror of the producer's blocking `done.wait()`. Kafka stores "where to resume", i.e.
`offset + 1`; passing `message=` lets the client do that arithmetic.

**The placement is the decision (D10).** It is the last statement of `_handle_message`,
after the handler has run:

| | crash before commit | crash after commit |
|---|---|---|
| handler ran | redelivered → **handled twice** | fine |
| handler did not run | redelivered → handled once ✓ | would be **lost** |

Committing last makes the failure mode duplicates, never loss — **at-least-once**.
Committing first would give at-most-once. This is also why `enable.auto.commit=False`
matters: the auto-committer fires on a 5-second timer and would happily commit offsets
for messages still sitting in the fetch queue, unhandled.

Which yields the rule that ties §11 and §13 together:

> **The queue is never the record. The log on disk is.**

Suppose 5 000 messages are buffered, you have handled 12, and the process is `SIGKILL`ed.
The other 4 988 vanish from memory — but they were never committed, so the next start
refetches them. Nothing is lost. The same applies during a rebalance, which purges the
queue for revoked partitions.

One cost worth naming: a synchronous commit per message is one network round-trip per
event, and that is this loop's throughput ceiling — the exact counterpart of the
producer's per-event `done.wait()` in §7. Fine when you are reading log lines; a
production consumer would commit every N messages and accept a wider redelivery window.

The two error paths bracket this nicely. A message that will not decode is logged and
**committed anyway**:

```python
# src/order_service/consumer/runtime.py:234-244
except (ValueError, UnicodeDecodeError) as exc:
    logger.error("[%s] undecodable message at %s[%d]@%d: %s", ...)
    self._consumer.commit(message=message, asynchronous=False)
    return
```

Not committing would re-read the same poison pill forever, blocking every good message
behind it on that partition. And a *detected violation* does not stop anything either —
`apply_event` records it and the fold advances regardless (**D9**), so one bad event
produces one warning rather than an unending cascade.

---

## 14. Why `close()` is not optional

§7 walked a deadlock; this is its counterpart, and it is a *distributed* failure rather
than a local one.

```python
# src/order_service/consumer/runtime.py:206-209
finally:
    self._consumer.close()
    logger.info("[%s] consumer closed", self._spec.name)
```

`close()` commits pending offsets, sends **LeaveGroup**, and tears down the threads. In a
`finally`, so it runs on clean exit and on a fatal exception alike.

Skip it — `SIGKILL`, or a container that never handled `SIGTERM` — and:

1. The process dies without leaving the group.
2. The coordinator still believes the member is alive.
3. It waits out `session.timeout.ms` (45 s by default) before declaring it dead.
4. Only then does it rebalance. **Nobody consumes those partitions for that whole
   window.**

That is the entire reason `main.py` traps `SIGTERM` and not just `SIGINT`: `SIGINT` is
Ctrl-C from `docker compose up`, but `SIGTERM` is what `docker compose down` sends. Miss
it and Docker `SIGKILL`s after ten seconds — no commit, no LeaveGroup, a stalled group.

The exit codes are deliberate too:

```python
# src/order_service/consumer/main.py:59-61, 74-76
except KeyError as exc:
    logger.error("%s", exc)
    sys.exit(2)              # bad SERVICE_NAME — restarting will not help
...
except KafkaException as exc:
    logger.error("[%s] fatal kafka error: %s", spec.name, exc)
    sys.exit(1)              # broker fault — restarting might
```

Fail fast on configuration, fail safe on data: a bad `SERVICE_NAME` exits immediately,
while a bad *message* is logged, committed, and skipped. Opposite policies, on purpose —
config errors never fix themselves, and one malformed record should not take down a
service.

---

> **BOTH HALVES**

## 15. Picking the right tool

| Situation | Use | Why |
|---|---|---|
| CPU-bound work (hashing, parsing millions of rows) | process | the GIL blocks real thread parallelism |
| Blocking call in a sync framework | thread / threadpool | GIL is released while waiting |
| Many concurrent network waits | event loop | one thread, thousands of pending awaits |
| C library with its own threads | let it run, service its callbacks | it does not need yours |
| Blocking call inside `async def` | `asyncio.to_thread` — or just use `def` | never block the loop |
| A process that is *called* | web framework | it needs a socket, routes, status codes |
| A process that *calls* | a bare loop | the broker already provides the plumbing |

---

## 16. Recap

| Term | One line | Where in this repo |
|---|---|---|
| Main thread | The thread the interpreter starts in; runs the event loop under uvicorn | [app.py](../src/order_service/producer/app.py) |
| OS thread | Real kernel thread; `threading.Thread` creates one | [kafka_producer.py:62](../src/order_service/producer/kafka_producer.py#L62) |
| GIL | One Python bytecode at a time; released during I/O and in C code | why the poll thread is nearly free |
| Process | Separate memory and GIL; nothing shared | [`_orders`](../src/order_service/producer/orders.py#L111) resets per process (D5) |
| Thread pool | Fixed reusable threads on a work queue; caps concurrency | anyio, behind every `def` route |
| Event loop | One thread, one task at a time, switches at `await` | uvicorn |
| `def` route | Runs in the threadpool — may block | [routes.py:204](../src/order_service/producer/routes.py#L204) (D6) |
| `async def` route | Runs on the loop — must never block | not used for publishing here |
| librdkafka send queue | C-side buffer `produce()` appends to and returns | [`_produce`](../src/order_service/producer/kafka_producer.py#L167) |
| `Producer.poll()` | Serves queued callbacks into Python | [`_poll_loop`](../src/order_service/producer/kafka_producer.py#L100) (D6) |
| `flush()` | Blocks until the send queue drains; call before exit | [`stop`](../src/order_service/producer/kafka_producer.py#L68) (D6) |
| `rdk:main` | C thread: metadata, group state machine, heartbeats | never visible from Python |
| `rdk:broker-N` | C thread per connection: the actual socket I/O | one per broker |
| Consumer group | `group.id`; owns an offset per partition, so groups are independent | [`group_id_for`](../src/order_service/config.py#L31) (D7) |
| Coordinator | The broker that owns a group's membership and offsets | picked by hashing `group.id` |
| Rebalance | Members join/leave → partitions reassigned, by an elected client under `classic` or by the broker under KIP-848 | `JoinGroup` + `SyncGroup`; §10 |
| Eager vs cooperative | Whether a rebalance revokes everything or only what moves — and therefore how much folded state is lost | [consumer-groups.md](consumer-groups.md) |
| librdkafka fetch queue | C-side buffer the broker threads fill and `poll()` drains | §11 |
| `Consumer.poll()` | Dequeues one message; runs callbacks; releases the GIL | [`run`](../src/order_service/consumer/runtime.py#L179) |
| `commit()` | Makes consumption durable — the consumer's delivery report | [`_handle_message`](../src/order_service/consumer/runtime.py#L276) (D10) |
| `close()` | Commits, sends LeaveGroup, stops the threads; call before exit | [`run`](../src/order_service/consumer/runtime.py#L206) |

**The producer sentence to keep:** `produce()` hands a message to a C-side queue drained
by threads you do not control, and its result only reaches your Python code while
somebody calls `poll()` — so the poll thread, not the broker, is what makes a blocking
publish return.

**The consumer sentence to keep:** the C threads run Kafka whether you poll or not —
connecting, heartbeating, fetching, rebalancing — so `poll()` is just your Python thread
walking over to ask "anything for me?", and `commit()`, not `poll()`, is what makes
having read it mean something.

**And the pair:** `produce()` and `poll()` are both *buffer* operations; the delivery
report and the offset commit are the *acknowledgements*. Confuse the two on either side
and you get a silent data-loss bug that only shows up under failure.

---

Related: [specs/001-prepaid-order-service/design.md](../specs/001-prepaid-order-service/design.md)
(D5, D6, D7, D8, D9, D10, D12) · [DECISIONS.md](../DECISIONS.md) (X1, X3) ·
[order-flow.md](order-flow.md)
