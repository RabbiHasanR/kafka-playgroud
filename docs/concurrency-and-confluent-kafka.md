# Threads, the event loop, and how confluent-kafka really sends

A short reference for the concurrency machinery this repo's producer sits on. Every
concept is anchored to code in this repository, so read it with the files open.

---

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
# src/order_service/producer/kafka_producer.py:81-83
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
# src/order_service/producer/orders.py:154
self._orders: dict[str, Order] = {}
```

This dict is process-local, and each `Order` carries its own `last_sequence`. Run two
order-service processes and neither sees the other's orders; restart one and it hands
out sequence 1 again for an order it has forgotten. That is **D5** plus the accepted
in-memory limitation — deliberate, and the reason a restart loses orders.

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
# src/order_service/producer/routes.py:256-261
@router.post("/orders/{order_id}/events", response_model=PublishEventResponse)
def publish_event(                              # ← `def`, not `async def`
    order_id: str,
    body: PublishEventRequest,
    request: Request,
) -> PublishEventResponse:
```

Because underneath it does this:

```python
# src/order_service/producer/kafka_producer.py:158, 173
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
indirection. D5 skipped it because nothing else in the handler is async.

---

## 5. How confluent-kafka connects to the broker

`confluent-kafka` is a thin Python binding over **librdkafka**, a C library (decision
**X1**). The Python objects are handles; the actual protocol work happens in C.

```python
# src/order_service/producer/kafka_producer.py:59-70
self._producer = Producer(
    {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "acks": "all",
        "partitioner": "consistent_random",
        "sticky.partitioning.linger.ms": 0,
        "client.id": "order-pipeline-producer",
    }
)
```

**This constructor does not connect.** It builds config and starts librdkafka's own
threads. What happens next, all in C, without any Python involvement:

1. An internal "main" thread contacts a `bootstrap.servers` address and fetches **cluster
   metadata** — the full broker list, topics, partitions, and which broker leads each one.
2. It opens a **dedicated thread and TCP connection per broker** it learned about.
   Bootstrap is only a starting point; the client talks to leaders directly afterwards.
3. Those threads handle batching, compression, retries, reconnects, and metadata refresh
   on their own timers.

Because it is lazy and asynchronous, a wrong broker address does not raise at construction
— it surfaces much later as a timeout. The one place we force a real round-trip:

```python
# src/order_service/producer/kafka_producer.py:119-121
metadata = self._producer.list_topics(timeout=timeout)
topic = metadata.topics.get(self._settings.order_events_topic)
return topic is not None and topic.error is None
```

That is a genuine metadata request with a timeout, which is why `publish_and_wait` calls
it to distinguish "topic missing" from "broker unreachable".

---

## 6. The internal queue: why `produce()` returns instantly

```python
# src/order_service/producer/kafka_producer.py:207-216
self._producer.produce(
    topic=self._settings.order_events_topic,
    key=event.order_id.encode("utf-8") if keyed else None,
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
# src/order_service/producer/kafka_producer.py:214-215
except BufferError as exc:
    raise DeliveryFailed(f"producer queue is full: {exc}") from exc
```

**(b) Callbacks only fire while you call `poll()`.** librdkafka will not call into Python
from its own C threads — it queues the delivery reports and hands them over only when your
code asks. That is what this loop is for:

```python
# src/order_service/producer/kafka_producer.py:123-126
def _poll_loop(self) -> None:
    while not self._poll_stop.is_set():
        self._producer.poll(0.1)
```

`poll(0.1)` = "serve pending callbacks, wait up to 100 ms if there are none". It runs on
its own thread so callbacks fire regardless of request traffic (decision **D6**), owned by
the app lifespan:

```python
# src/order_service/producer/app.py:39-40, 55
producer = LifecycleEventProducer(settings)
producer.start()          # poll thread up before the first request
...
producer.stop()           # flush, then stop the thread
```

**(c) Exiting without flushing loses the tail.** Whatever is still in the queue dies with
the process:

```python
# src/order_service/producer/kafka_producer.py:93
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

## 8. Picking the right tool

| Situation | Use | Why |
|---|---|---|
| CPU-bound work (hashing, parsing millions of rows) | process | the GIL blocks real thread parallelism |
| Blocking call in a sync framework | thread / threadpool | GIL is released while waiting |
| Many concurrent network waits | event loop | one thread, thousands of pending awaits |
| C library with its own threads | let it run, service its callbacks | it does not need yours |
| Blocking call inside `async def` | `asyncio.to_thread` — or just use `def` | never block the loop |

---

## 9. Recap

| Term | One line | Where in this repo |
|---|---|---|
| Main thread | The thread the interpreter starts in; runs the event loop under uvicorn | [app.py](../src/order_service/producer/app.py) |
| OS thread | Real kernel thread; `threading.Thread` creates one | [kafka_producer.py:81](../src/order_service/producer/kafka_producer.py#L81) |
| GIL | One Python bytecode at a time; released during I/O and in C code | why the poll thread is nearly free |
| Process | Separate memory and GIL; nothing shared | [`_orders`](../src/order_service/producer/orders.py#L154) resets per process (D5) |
| Thread pool | Fixed reusable threads on a work queue; caps concurrency | anyio, behind every `def` route |
| Event loop | One thread, one task at a time, switches at `await` | uvicorn |
| `def` route | Runs in the threadpool — may block | [routes.py:257](../src/order_service/producer/routes.py#L257) (D6) |
| `async def` route | Runs on the loop — must never block | not used for publishing here |
| librdkafka queue | C-side buffer `produce()` appends to and returns | [`_produce`](../src/order_service/producer/kafka_producer.py#L197) |
| `poll()` | Serves queued callbacks into Python | [`_poll_loop`](../src/order_service/producer/kafka_producer.py#L123) (D6) |
| `flush()` | Blocks until the queue drains; call before exit | [`stop`](../src/order_service/producer/kafka_producer.py#L87) (D6) |

**The one sentence to keep:** `produce()` hands a message to a C-side queue drained by
threads you do not control, and its result only reaches your Python code while somebody
calls `poll()` — so the poll thread, not the broker, is what makes a blocking publish
return.

---

Related: [specs/001-prepaid-order-service/design.md](../specs/001-prepaid-order-service/design.md)
(D5, D6) · [DECISIONS.md](../DECISIONS.md) (X1)
