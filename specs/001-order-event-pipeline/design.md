# 001 — Ordered Order-Event Pipeline: Design

Implements [requirements.md](requirements.md).
Cross-cutting choices that outlive this feature are recorded in
[../../DECISIONS.md](../../DECISIONS.md) as `X<n>` and referenced from here.

## Architecture

```
                     compose network "kafka-playground_default"
  ┌──────────────────────────────────────────────────────────────────────┐
  │                                                                      │
  │  producer (FastAPI)                        kafka                     │
  │  :8000                                     :19092                    │
  │   POST /orders/{id}/events ──┐                                        │
  │   POST /simulate ────────────┼── produce ──► order-events             │
  │   GET  /health               │   key=order_id   ├── partition 0       │
  │                              │                  ├── partition 1       │
  │   [poll thread] ◄── delivery reports            └── partition 2       │
  │                                                        │              │
  │  consumer                                              │              │
  │  :8001                       ◄──── subscribe ──────────┘              │
  │   GET /state  (dump folded state)                                     │
  │   in-memory: {order_id: OrderState}                                   │
  │                                                                      │
  └──────────────────────────────────────────────────────────────────────┘
       group "order-processor" → offsets in __consumer_offsets (broker)
```

Two new services join the existing compose stack. The broker is untouched — `000`
owns it.

Two stores exist and are deliberately kept distinct in the reader's mind:
**committed offsets** live in the broker and survive everything; **folded per-order
state** lives in the consumer's process memory and survives nothing.

## Decisions

### D1 — `confluent-kafka` as the client — *R1.7, R1.12, R1.13*

See [X1](../../DECISIONS.md). Binds every spec from here to 009, so it is recorded
cross-cutting rather than here.

### D2 — Key is the raw `order_id`, default partitioner — *R1.7, R1.8, R1.15*

The message key is `order_id` encoded UTF-8; the partition is left to librdkafka's
default `consistent_random` partitioner (murmur2 hash of the key, matching the Java
client). Nothing custom — the point is to observe Kafka's own behaviour, and a custom
partitioner would obscure which guarantee comes from where.

A null key is what makes R1.15 work: with no key the partitioner picks randomly, so
one order's events scatter and the sequence gaps appear on their own.

**`sticky.partitioning.linger.ms=0` is set, and this was not obvious.** By default
librdkafka keeps *all* null-key messages produced within a 10 ms window on a single
partition, to improve batching. Running T25 with the default, every unkeyed order was
produced inside that window and landed **entirely on one partition** — perfectly
ordered, zero violations, fault injection injecting nothing. Setting the linger to
zero forces a genuine per-message random choice. It has no effect on keyed messages.

The lesson is worth more than the setting: *"no key" does not imply "scattered".*
Modern clients deliberately batch null-key messages onto one partition, so a burst of
unkeyed writes can stay accidentally ordered — right up until traffic patterns change.

*Rejected:* an explicit partition number per message — would produce the same
observable result while hiding the hashing step that is the actual lesson.

### D3 — JSON on the wire — *R1.1*

See [X2](../../DECISIONS.md). Chosen so `kafka-console-consumer.sh` stays readable
against the same topic; Avro and Schema Registry are deferred.

### D4 — Money as integer minor units — *R1.4, R1.21*

`unit_price` and `amount` are integers in the smallest currency unit (paisa/cents).
`total = Σ(qty × unit_price)` is then exact integer arithmetic, so R1.21's equality
check cannot produce a false violation from float representation error.

This matters more than it looks: a float total would make the total-mismatch signal
untrustworthy, and an untrustworthy signal teaches nothing. Floats would introduce a
second possible cause for every mismatch.

*Rejected:* `Decimal` — correct, but it serialises to a string in JSON and adds a
conversion at every boundary for no gain at integer-only arithmetic.

### D5 — Blocking delivery report on the single-event endpoint — *R1.12, R1.13*

`Producer.produce()` only buffers; the broker's acknowledgement arrives later on a
delivery callback. For `POST /orders/{id}/events` the handler waits for that callback
before responding, so the response can carry the true partition and offset, and a
broker failure becomes an HTTP error rather than a silent drop.

Mechanically: the route is a **synchronous `def`**, not `async def`, so FastAPI runs
it in a worker thread and the wait cannot block the event loop. The delivery callback
sets a `threading.Event` carrying the result; the handler waits on it with a timeout
and returns `504` if the timeout expires.

This is the produce-side latency/guarantee tradeoff made visible at the API boundary,
and it is the same tradeoff `acks` controls in spec 005.

**Missing topics are diagnosed, not left as timeouts.** librdkafka treats an unknown
topic as *retriable* and keeps retrying until the message timeout, so R1.9's "explicit
error" would otherwise surface as an opaque `504`. On timeout the producer therefore
checks cluster metadata and, if the topic is genuinely absent, raises a named error
instead. If the metadata call *itself* fails the broker is unreachable — a different
fault — and the plain timeout is reported, so a down broker is never misreported as a
missing topic.

*Rejected:* returning `202 Accepted` immediately on buffer — faster, and what a
high-throughput service would really do, but the response would be a claim rather
than a fact, and R1.12 requires the real partition and offset.

### D6 — `/simulate` does **not** block per event — *R1.11, R1.12*

R1.12 is read as scoping to the single-event endpoint of R1.10. `/simulate` exists to
generate load (R1.11) and per-event blocking would cap throughput at one round-trip
per event, defeating the lag experiment. It runs as a background task, paces itself
to the requested events-per-second, and returns a job summary immediately; delivery
errors are counted and surfaced in the logs and the job summary rather than per
event.

*If this reading of R1.12 is wrong, this is the point to say so* — the alternative is
a second requirement distinguishing the two endpoints.

Delivery counts are therefore reported by `GET /simulate/{job_id}` rather than in the
`POST` response, which returns `202` with the job id. `GET /simulate` lists every run
the process has started.

### D7 — Background poll thread owned by the app lifespan — *R1.13, R1.14*

Delivery callbacks only fire while someone calls `poll()`. A daemon thread started in
the FastAPI `lifespan` runs `producer.poll(0.1)` in a loop, so callbacks fire
promptly regardless of request activity, and D5's waiting handler is always released
by someone. On shutdown the lifespan stops the thread and calls `flush()` with a
timeout, satisfying R1.14.

`Producer` is thread-safe, so the poll thread and request threads share one instance.

*Rejected:* calling `flush()` inside each request — it works, but it drains the whole
buffer including other requests' messages, coupling unrelated calls' latency.

### D8 — Producer-side sequence counters, in memory — *R1.2*

The producer holds `{order_id: last_sequence}` under a lock and assigns the next
value on publish. Sequences are therefore contiguous per order for the lifetime of
the producer process.

**Known limitation:** restarting the producer resets the counters, so events for an
order that spans a restart will restart at 1 and the consumer will correctly report a
violation. This is acceptable here — orders are created and completed within one
simulation run — and it is a second, independent instance of exactly the lesson in
R1.27: derived state in memory does not survive.

*Rejected:* deriving the sequence from a consumer-side read of the topic — circular,
and it would make the producer depend on the consumer.

### D9 — Fault injection as request flags, defaulted off — *R1.15, R1.16, R1.17*

`/simulate` takes `unkeyed: bool = False` and `shuffle: bool = False`. `unkeyed`
publishes with `key=None`; `shuffle` emits an order's events in a permuted sequence
order. They compose — enabling both is a legitimate and instructive combination.

Keeping these as request parameters rather than environment settings means a clean
run and a broken run are one HTTP call apart, with no restart in between.

### D10 — Explicit lifecycle transition table — *R1.5, R1.20*

A module-level mapping from `event_type` to its set of legal predecessor states, in
`events.py`, shared by producer (to generate valid lifecycles) and consumer (to
validate them). One definition, two users — no drift between what is produced and
what is checked.

`ITEM_ADDED` is legal only from `CREATED` and repeatable; the rest form the linear
chain `CREATED → PAID → PACKED → SHIPPED → DELIVERED`.

### D11 — Consumer state in a plain dict — *R1.18, R1.27*

`{order_id: OrderState}` where `OrderState` carries `last_sequence`, `state`,
`item_count`, and `total`. No persistence, by requirement. The fold logic lives in
`consumer/state.py` as a pure function of `(current_state, event) → (new_state,
violations)`, so it is unit-testable without a broker and survives being re-hosted on
a durable store in spec 004.

### D12 — Manual commit after processing — *R1.22, R1.25*

`enable.auto.commit=False`; the consumer commits after each record is folded. This is
**at-least-once**: a crash between processing and committing re-delivers the record,
which will then be re-folded and inflate the running total.

That duplicate-processing consequence is not fixed here — it is the subject of spec
004, and 009 resolves it properly. Recorded so it is not mistaken for an oversight.

`auto.offset.reset=earliest` satisfies R1.28 for a fresh group id.

### D13 — Consumer exposes `GET /state` — *R1.31*

The consumer runs a minimal HTTP server in a thread exposing the folded state table.
Restart amnesia is then a direct before/after comparison of two `curl` outputs rather
than an inference from log volume.

*Rejected:* a `SIGUSR1` handler dumping to the log — no extra dependency and quite
elegant, but harder to diff and awkward inside a container.

### D14 — Topic created by an explicit script — *R1.6, R1.9*

`scripts/create_topics.sh` creates `order-events` with 3 partitions and replication
factor 1, invoking the CLI inside the broker container. Auto-creation is off (R0.14),
so this is a required setup step and a deliberate one.

Replication factor 1 is forced by the single-broker environment from 000; spec 005
raises it.

### D15 — Configuration through environment variables — *R1.32, R1.33*

`config.py` reads `KAFKA_BOOTSTRAP_SERVERS`, `ORDER_EVENTS_TOPIC`, and
`CONSUMER_GROUP_ID`. Compose sets the bootstrap to `kafka:19092`; a host-run process
defaults to `localhost:9092`. Nothing else changes between the two, satisfying R1.33.

### D16 — Violations logged at `WARNING`, records at `INFO` — *R1.29, R1.30*

Every consumed record logs partition, offset, key, `order_id`, `sequence`, and
`event_type` at `INFO`. Every violation logs its type, `order_id`, expected, and
observed at `WARNING` with a stable `VIOLATION` marker, so
`docker compose logs consumer | grep VIOLATION` is the whole filtering story.

## Module layout

```
src/order_pipeline/
├── config.py              # env-driven settings                        D15
├── events.py              # event schema, types, transition table      D4, D10
├── producer/
│   ├── app.py             # FastAPI app, lifespan, poll thread         D7
│   ├── kafka_producer.py  # produce + delivery-report wait             D5, D8
│   ├── simulator.py       # lifecycle generation, rate pacing, faults  D6, D9
│   └── routes.py          # HTTP surface                               D5, D6
└── consumer/
    ├── main.py            # subscribe/poll/commit loop                 D12
    ├── state.py           # pure fold + violation detection            D11
    └── http.py            # GET /state                                 D13
scripts/create_topics.sh                                               # D14
```

`events.py` is imported by both sides — it is the shared contract, and the only
module that may be.

## Known gaps, by intent

| Gap | Requirement | Closed by |
|---|---|---|
| Folded state lost on consumer restart | R1.27 | 004 |
| Producer sequence counters lost on restart | D8 | — (accepted) |
| Duplicate processing after a crash | D12 | 004, 009 |
| Single broker, RF 1, no failover | D14 | 005 |
| No schema enforcement on the JSON | D3 | later |

## Deferred to later specs

Nothing in this feature may anticipate them: the prepaid order service and
multi-service fan-out (002), consumer groups and rebalancing (003), durable state
(004), replication (005), dead-letter handling (006), compaction (007), changelog
state stores (008), transactions (009), stream SQL (010).
