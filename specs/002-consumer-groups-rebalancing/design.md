# 002 — Consumer Groups, Rebalancing, and Partition Assignment: Design

Implements [requirements.md](requirements.md).
Cross-cutting choices that outlive this feature are recorded in
[../../DECISIONS.md](../../DECISIONS.md) as `X<n>` and referenced from here.
This feature is the one [X8](../../DECISIONS.md) reserves at 002.

Client behaviour asserted below was probed against the installed
confluent-kafka/librdkafka **2.15.0**, not assumed; the probe results are quoted where
they shaped a decision.

## Architecture

```
                     compose network "kafka-playground_default"
  ┌───────────────────────────────────────────────────────────────────────────┐
  │                                                                           │
  │  order-service (FastAPI) :8010          kafka :19092                      │
  │   POST /orders ──────────── produce ──► order-lifecycle                   │
  │   POST /orders/{id}/events   key=order_id  ├── partition 0                │
  │        ▲                                   ├── partition 1                │
  │        │                                   └── partition 2                │
  │  scripts/place_orders.sh N                        │                       │
  │                                                   │                       │
  │        ┌──────────────────────────────────────────┤                       │
  │        │                    │                     │                       │
  │  inventory-consumer   analytics-consumer   ┌──────┴──────────────────┐    │
  │  group=inventory-     group=analytics-     │ group=notification-     │    │
  │        service              service        │        service          │    │
  │  1 member → p0,p1,p2  1 member → p0,p1,p2  │  notification-consumer-1│ p0 │
  │                                            │  notification-consumer-2│ p1 │
  │      ── FAN-OUT (001, unchanged) ──        │  notification-consumer-3│ p2 │
  │                                            └─────────────────────────┘    │
  │                                                ── SCALE-OUT (002) ──      │
  └───────────────────────────────────────────────────────────────────────────┘
    3 groups → 3 independent offset positions, exactly as in 001
    1 of those groups → 3 members dividing the SAME 3 partitions between them
```

**The picture to hold onto:** 001 drew three arrows out of the topic and each carried
every message. 002 splits one of those arrows into three strands that carry a third each.
Both shapes are on screen at once, on one topic, and the only difference between them is
whether the consumers share a `group.id`.

The producer, `events.py`, `order-lifecycle`, its 3 partitions, and
`scripts/create_topics.sh` are **untouched**. Every change in this feature is on the
consumer side or in compose.

## Decisions

### D1 — Notification scales; inventory and analytics are the control — *R2.1, R2.3, R2.6*

Notification runs as three instances in one group. The other two services keep exactly
their 001 shape.

Notification is the right one to split because duplicate work is *visible* there — three
notification instances in three groups would mean the customer gets three messages, which
is precisely the mistake scale-out exists to prevent. Inventory reserving stock three
times would be equally wrong but reads as an abstract log line; a duplicate customer
message does not need explaining.

Keeping inventory and analytics single-instance is not laziness. They are the experimental
control: while notification's partitions move around, those two keep consuming every event
exactly as 001 recorded. Any behaviour change observed during a rebalance therefore cannot
be blamed on the broker, the topic, or the producer — only two of the twelve variables in
the picture are moving.

*Rejected:* scaling all three services. Three times the containers and three times the log
noise, for no mechanism the single scaled group does not already show — and it destroys
the control group that makes the observations attributable.

### D2 — Three explicit compose services, not `docker compose up --scale` — *R2.1, R2.28, R2.36*

`notification-consumer` becomes `notification-consumer-1`, `-2`, `-3`, generated from one
YAML anchor so the shared configuration is written once. `-2` and `-3` carry
`profiles: ["scale-out"]`, so a plain `docker compose up` starts the group with **one**
member and the second and third are brought in deliberately, while the logs are being
watched.

`--scale` is the idiomatic answer and it is wrong for all three of this feature's needs:

| Need | `--scale` | three services |
|---|---|---|
| Stable name to `docker restart` | container ids only | `notification-consumer-2` |
| Distinct `group.instance.id` per instance | impossible — replicas share one env block | one env value each |
| Grow the group mid-experiment | `--scale n=3` recreates the service | `--profile scale-out up -d` |

The second row is fatal on its own: static membership (D10) requires each instance to have
its *own* stable identity, and `--scale` gives every replica the same environment. The
identity would have to come from the container hostname, which changes on recreation —
which is exactly the thing static membership exists to prevent.

*Rejected:* `--scale` with hostname-derived ids, for the reason above; and a fourth
permanent instance to demonstrate R2.4's idle member, which is a one-off `docker compose
run` rather than a container that idles forever in every developer's stack.

### D3 — Instance identity is explicit, not inferred — *R2.7, R2.8*

Each consumer process reads `CONSUMER_INSTANCE_ID` from the environment and prefixes it
into every log line, alongside the service name 001's D14 already puts there:

```
[notification/notification-2] partition=1 offset=57 key=… order_id=… seq=2 type=PACKED
```

Three members of one group produce three interleaved streams that are otherwise
character-for-character identical — same service name, same group, same handlers. Without
an instance token, `docker compose logs` for the group is unreadable and the central claim
of R2.3 ("exactly one member handled it") cannot be checked by eye at all.

It defaults to the hostname when unset, so a host run still produces something distinct
without configuration. It is deliberately **not** the same value as `group.instance.id`
(D10) — one is a logging concern that is always present, the other is a protocol-level
identity that is off by default, and conflating them would mean you could not have
readable logs without also changing the group's rebalance semantics.

### D4 — The protocol switch is validated by us, because librdkafka only guards one direction — *R2.17, R2.19, R2.20, R2.21, R2.22*

`CONSUMER_GROUP_PROTOCOL` selects `classic` or `consumer`, and
`_build_consumer_config()` assembles a different config dict for each. This is not
cosmetic plumbing: the two protocols accept **different, partly disjoint** settings.
Probed against 2.15.0:

| Setting | under `classic` | under `consumer` (KIP-848) |
|---|---|---|
| `partition.assignment.strategy` | required knob | **rejected** — "not supported … Use `group.remote.assignor`" |
| `group.remote.assignor` | **silently accepted and ignored** | the knob |
| `session.timeout.ms` | client-side knob | **rejected** — "It is defined broker side" |
| `heartbeat.interval.ms` | client-side knob | **rejected** — same |
| `max.poll.interval.ms` | accepted, `>= session.timeout.ms` | accepted, unconstrained |
| `group.instance.id` | accepted | accepted |
| `enable.auto.commit` | accepted | accepted |

**The second row is the entire justification for validating this ourselves.** librdkafka
raises `_INVALID_ARG` for a classic-only setting under the new protocol, but accepts a
KIP-848-only setting under the classic protocol without a word. So a run configured with
`CONSUMER_GROUP_PROTOCOL=classic` and `CONSUMER_REMOTE_ASSIGNOR=uniform` would start
cleanly, join happily, and use `range` — and the experiment's conclusion would be about an
assignor that was never in effect. A silent no-op is the worst possible failure in a
repository whose output *is* observations.

So R2.21 is enforced in our code, before `Consumer(...)` is constructed, naming both the
setting and the protocol. R2.22 then logs the protocol and the assignor actually in force
at join time, so a log excerpt pasted into `tasks.md` carries its own configuration.

Two consequences of the table worth stating plainly, because both bite later:

- **`session.timeout.ms` must be sent under `classic` and must not be sent under
  `consumer`.** It is not merely optional in the second case; it raises. So it is a
  conditional key, not a value with a default.
- **`max.poll.interval.ms >= session.timeout.ms` is enforced by the client under
  `classic`.** Probed: `max.poll.interval.ms=15000` alone fails against the 45 s default
  session timeout. The eviction lever (D9) therefore has to lower *both* under classic,
  and only one under KIP-848. This asymmetry is real and is the reason
  `CONSUMER_SESSION_TIMEOUT_MS` exists as a setting at all.

*Rejected:* letting librdkafka's own errors surface. They are good errors in the direction
it checks — and there is no error at all in the direction that silently changes the
meaning of an experiment.

### D5 — Rebalance callbacks log and manage folds; they never call `assign()` — *R2.9, R2.10, R2.14*

`subscribe()` gains `on_assign`, `on_revoke`, and `on_lost`. Each logs its partition list
with a stable marker and adjusts the fold store. **None of them calls `assign()`,
`unassign()`, `incremental_assign()`, or `incremental_unassign()`.**

This is what keeps one callback body correct across all four protocol/assignor
combinations. The alternative is branching: eager assignors need `assign`/`unassign`,
cooperative assignors and KIP-848 need the `incremental_*` pair, and calling the wrong one
is an error rather than a degradation. The client documents the way out
(`Consumer.incremental_assign`, 2.15.0):

> Note that if you do not call `incremental_assign` in your `on_assign` handler, this will
> be done automatically and start offsets will be [the committed ones].

So doing nothing is not an oversight — it is the supported way to say "the default
assignment, please", and it is the only formulation that is correct under every protocol
this feature switches between.

`on_lost` is given its own callback rather than being folded into `on_revoke`. The
distinction matters exactly once, and it is the case D8 is about: partitions that were
*lost* may already be owned by another member, so committing against them fails. Logging
them under a different marker is what makes the eviction experiment (E8) readable.

*Rejected:* explicit assignment with a protocol branch — four code paths, three of which
are wrong at any moment, in the one function that runs during every membership change.

### D6 — Folds are keyed by partition first — *R2.14, R2.15*

```python
self._folds: dict[str, OrderFold]              # 001
self._folds: dict[int, dict[str, OrderFold]]   # 002: partition → order_id → fold
```

R2.14 requires that a revoked partition's state be discarded *and* that the retained
partitions' state survive. With 001's flat dict that is not expressible — the fold for an
order gives no indication of which partition it came from, so honouring the requirement
would mean either dropping everything on every rebalance (wrong, and it would make
cooperative assignment indistinguishable from eager) or keeping everything (also wrong,
and it would hide R2.15's amnesia behind stale state that happens to still be correct).

Keying by partition makes revocation `del self._folds[p]` and makes the shape of the
correct answer visible in the data structure: **an instance holds state for exactly the
partitions it owns.** That is co-partitioned state, arrived at here by necessity, and it is
the same shape [X5](../../DECISIONS.md) reaches deliberately at 007 with RocksDB and a
changelog topic. Meeting it as a `dict` first is the point.

### D7 — The rebalance amnesia is preserved, and is a requirement — *R2.15*

When a partition moves to an instance that has never held it, that instance has no fold for
those orders, so the next event for a part-way order reports `SEQUENCE_GAP`. R2.15 requires
this to be *recorded*, not suppressed.

001 saw the same violation after a restart (T35) and accepted it under
[X3](../../DECISIONS.md). What 002 adds is that no crash is involved: scaling a service up
by one — a routine, intentional, healthy operation — corrupts every in-flight order's
derived state on the partition that moved. That is a much harder fact to argue away than a
restart, and it is the strongest available motivation for 003.

Suppressing it would be easy and is forbidden. There is no honest local fix: the instance
genuinely does not know what it never saw, and the only real answers are durable state
(003) or state that migrates with the partition (007).

### D8 — A commit that lost its partition is marked and survived, not fatal — *R2.26, R2.27*

001's `_handle_message` ends with an unguarded synchronous `commit()`. Under scale-out that
call can now fail for a reason 001 could not produce: the member was evicted or the
partition was revoked mid-handler, so the offset belongs to somebody else.

The commit is wrapped, and the failure logs at `WARNING` with its own stable marker —
deliberately **not** 001's `VIOLATION` marker, because `grep VIOLATION` means "the data
stream was wrong" and this means "our group membership changed underneath us". Two
different diagnoses must stay greppable apart, or E8's output cannot be read.

The process then continues polling and rejoins the group (R2.27). Exiting would be the
easy choice and it would destroy the experiment: the entire lesson of E8 is that the
process is **alive and healthy** while the group has given up on it. A consumer that dies
on eviction can never demonstrate that.

Nothing here retries the handler or deduplicates. The redelivery that follows is
at-least-once behaving exactly as 001's D10 described, and absorbing it is 003 and 008.

### D9 — The eviction lever: a handler delay plus a lowered poll interval — *R2.23, R2.24, R2.25*

`HANDLER_DELAY_SECONDS` makes every handler sleep before returning; `CONSUMER_MAX_POLL_INTERVAL_MS`
and `CONSUMER_SESSION_TIMEOUT_MS` shrink the window the broker allows. Both default to
values that leave 001's behaviour untouched (no delay; the client defaults).

This is 002's counterpart to 001's `force` flag, and it exists for the same structural
reason: **without it the requirement is unreachable.** R2.25 describes a member being
removed from its group while its process is alive and healthy. `docker stop` cannot produce
that — it kills the process. Nothing in an honest workload produces it either, on a
handler whose entire body is a log line. The delay is the only way to reach the state, and
it is the single most common real consumer incident there is: a handler that calls a slow
API, does not poll, and is declared dead by a broker that cannot distinguish "slow" from
"gone".

The lever is per-instance environment, so one member can be made slow while its two peers
stay healthy — which is what makes the partitions visibly move *to* somewhere rather than
just stop.

Under `classic` both timeouts must be lowered together (D4's `max.poll.interval.ms >=
session.timeout.ms` constraint); under `consumer` only the poll interval may be set, since
the session timeout is broker-side. The config builder handles the difference so the
experiment is the same two environment variables in both runs.

### D10 — Static membership is opt-in, per instance, and empty means off — *R2.28, R2.29, R2.30*

`CONSUMER_INSTANCE_ID_STATIC` sets `group.instance.id` when non-empty and is omitted from
the config entirely when unset. In compose it is wired through Compose's `:+` form:

```yaml
CONSUMER_INSTANCE_ID_STATIC: ${STATIC_MEMBERSHIP:+notification-1}
```

so `STATIC_MEMBERSHIP=1 docker compose up -d` turns it on for all three instances with
distinct values, and a plain `up` leaves them dynamic. **An empty string must be read as
unset**, because Compose interpolation yields `""` rather than removing the variable — a
`group.instance.id` of `""` would be a real, empty, and shared identity, which is worse
than either intended state.

Off by default is the right default twice over: it preserves 001's behaviour per R2.34,
and it means the experiment's baseline (E9's "restart without it") is what you get without
doing anything.

Probed: `group.instance.id` is accepted under **both** protocols, so E9 runs unchanged
under either — one of the few knobs in D4's table that needs no branch.

### D11 — `place_orders.sh` is bash, and deliberately not clever — *R2.31, R2.32, R2.33*

A loop over `curl` against `POST /orders`, printing `order_id`, partition, and offset per
line, with an optional flag to walk each order through `PACKED → SHIPPED → DELIVERED`.
No `jq` dependency — the two fields needed are extracted with `sed`, so the script runs
against a bare shell like every other script in `scripts/`.

It exists because a three-way partition split is illegible at four orders and obvious at
twenty, and typing twenty `curl` calls teaches nothing. R2.33 forbids it growing rate
control, concurrency, or a messages-per-second figure — at that point it is the load
generator 001 deferred by explicit request and this spec's "Out of scope" excludes, and the
lag and throughput experiments it would enable belong to a feature that asks for them.

*Rejected:* a Python generator sharing the producer's client. It would be shorter and it
would immediately invite `--rate`, `--threads`, and a summary table.

### D12 — `classic` stays the default; the new protocol is opt-in — *R2.18*

Recorded as [X9](../../DECISIONS.md) because it binds every consumer from here to 008, not
just this feature. Summary: 001's recorded results and the whole of
`docs/concurrency-and-confluent-kafka.md` §10 describe classic behaviour, and silently
flipping the default would invalidate both without a line of either changing. The full
reasoning, and the conditions under which the default should flip, are in X9.

## Module layout

```
src/order_service/
├── config.py                # + protocol, assignor, timeouts, delay, ids   D3, D4, D9, D10
├── events.py                # UNCHANGED
├── producer/                # UNCHANGED — all four modules
└── consumer/
    ├── runtime.py           # + config builder, rebalance callbacks,       D4, D5, D6,
    │                        #   partition-keyed folds, guarded commit      D8, D9
    ├── main.py              # + instance id in the startup banner          D3
    ├── inventory.py         # UNCHANGED
    ├── notification.py      # UNCHANGED
    └── analytics.py         # UNCHANGED
scripts/
├── create_topics.sh         # UNCHANGED
└── place_orders.sh          # new                                          D11
docs/
├── consumer-groups.md       # new                                          R2.37
├── concurrency-and-confluent-kafka.md   # §10 amended                      R2.38
└── order-flow.md            # UNCHANGED
docker-compose.yml           # notification ×3 via anchor, scale-out profile D2, D10
```

The three handler modules being untouched is a result worth noting: **scale-out is not
visible from inside a handler.** A service that reacts to an event cannot tell whether it
is one of one or one of three, and that is exactly why the group id is the only thing that
had to change.

## Environment surface

Every setting defaults to 001's behaviour (R2.34).

| Variable | Default | Effect |
|---|---|---|
| `CONSUMER_INSTANCE_ID` | hostname | log prefix only (D3) |
| `CONSUMER_GROUP_PROTOCOL` | `classic` | `classic` \| `consumer` (D4, X9) |
| `CONSUMER_ASSIGNMENT_STRATEGY` | `range` | classic only: `range` \| `roundrobin` \| `cooperative-sticky` |
| `CONSUMER_REMOTE_ASSIGNOR` | unset | KIP-848 only: `uniform` \| `range` |
| `CONSUMER_SESSION_TIMEOUT_MS` | unset → client default | classic only; must be ≤ max poll interval |
| `CONSUMER_MAX_POLL_INTERVAL_MS` | unset → client default | both protocols (D9) |
| `HANDLER_DELAY_SECONDS` | `0.0` | the eviction lever (D9) |
| `CONSUMER_INSTANCE_ID_STATIC` | unset | `group.instance.id`; empty = unset (D10) |

`SERVICE_NAME`, `CONSUMER_GROUP_ID`, `KAFKA_BOOTSTRAP_SERVERS`, and
`ORDER_LIFECYCLE_TOPIC` keep their 001 meanings unchanged.

## Known gaps, by intent

| Gap | Requirement | Closed by |
|---|---|---|
| Folded state is lost when a partition is reassigned | R2.15, D7 | 003, fully by 007 |
| Duplicate handling after eviction and redelivery | D8 | 003, 008 |
| No consumer-side dedup on `event_id` | 001 D11 | 003, 008 |
| A rebalance's *duration* is never measured, only its shape | D5 | — (needs a load generator this spec excludes) |
| Consumer lag under load is not measured | R2.33 | — (same) |
| Partition growth and key rehashing | out of scope | — (excluded by request; may become an experiment) |
| Single broker, RF 1 | 000 | 004 |

## Deferred to later specs

Nothing here may anticipate them: durable state (003), replication (004), dead-letter
handling (005), compaction (006), changelog state stores (007), transactions (008), stream
SQL (009).
