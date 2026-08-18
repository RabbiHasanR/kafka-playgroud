# 004 — Replication, `acks`, and Failover: Design

Implements [requirements.md](requirements.md). Decisions are numbered `D<n>` and cite the
criteria they satisfy. Cross-feature decisions live in [DECISIONS.md](../../DECISIONS.md).

## Architecture

Three combined broker/controller nodes form one KRaft cluster. `order-lifecycle` keeps its
three partitions and gains three replicas of each, spread so that no node holds two copies of
the same partition — the controller does this placement itself.

```
                 kafka            kafka-2          kafka-3
 host port       9092              9094             9095
 node id            1                 2                3
 ─────────────────────────────────────────────────────────
 partition 0   LEADER            follower         follower
 partition 1   follower          LEADER           follower
 partition 2   follower          follower         LEADER

 ISR per partition = {1,2,3} while healthy; shrinks when a node stops.
```

Nothing about the application's shape changes. The producer keys by `order_id` and the
consumers fold exactly as 003 left them; the only Python touched is where `acks` comes from and
how a delivery failure is reported.

## Decisions

### D1 — Three combined nodes in `docker-compose.yml`, permanently — *R4.1, R4.2, R4.3*

Per [X10](../../DECISIONS.md). Each node runs `broker,controller`, so the quorum is the three
brokers themselves rather than a separate controller tier — the shape 000 already used, with
the node count raised.

The three services differ only in node id, ports, advertised host and volume, so the shared
body goes in a YAML anchor (`x-kafka-broker`) the way `x-consumer-image` and `x-notification-env`
already do. Rejected: `deploy.replicas`, which forbids `container_name` and would leave no
stable name for `docker stop kafka-2` — the same reason 002 rejected `--scale`.

Internal topics move to RF 3, with the transaction state log at `min.insync.replicas` 2. This
is the amended R0.17.

### D2 — The first node keeps `container_name: kafka` — *R4.1*

`README.md`, `scripts/create_topics.sh` and every documented CLI invocation run
`docker exec kafka /opt/kafka/bin/…`. Renaming it to `kafka-1` for symmetry would break all of
them for a cosmetic gain. Node ids are 1/2/3 and the hostnames are `kafka`, `kafka-2`,
`kafka-3` — asymmetric on purpose, and cheaper than the alternative.

Host ports 9092 / 9094 / 9095. 9093 is the controller listener *inside* each container and is
never published; separate containers mean the internal 19092 and 9093 do not clash.

### D3 — Bootstrap is one anchor listing all three brokers — *R4.11*

`KAFKA_BOOTSTRAP_SERVERS` is currently the literal `kafka:19092` in five compose services. It
becomes an `x-kafka-bootstrap` anchor holding `kafka:19092,kafka-2:19092,kafka-3:19092`.

This is load-bearing. A client only needs one reachable address to discover the rest of the
cluster from metadata — but if that one address is the node that is down, the client cannot
start at all, and the failure looks like broken failover rather than a bootstrap list of one.
The host-side default in `config.py` moves the same way: `localhost:9092,localhost:9094,localhost:9095`.

### D4 — Replication factor is a topic argument, not a broker default — *R4.4, R4.5, R4.6*

`create_topics.sh` reads `REPLICATION_FACTOR` from the environment, defaulting to 3, and keeps
passing `--replication-factor` explicitly. The broker's `KAFKA_DEFAULT_REPLICATION_FACTOR` is
set to 3 as well, but nothing relies on it.

Explicit beats inherited here because R4.6 is the point: `REPLICATION_FACTOR=1
ORDER_LIFECYCLE_TOPIC=rf1-scratch ./scripts/create_topics.sh` produces an under-replicated topic
on a healthy three-broker cluster, and stopping one node takes a partition of it offline while
`order-lifecycle` is untouched. Replication is a property of the topic, and the script is where
that is visible.

### D5 — `acks` becomes a `StrEnum` setting; the producer stops hardcoding it — *R4.7, R4.13*

`ProducerAcks` joins `GroupProtocol`, `StateBackend`, `StateWriteOrder` and `StateCrashPoint` in
`config.py`, with members `NONE = "0"`, `LEADER = "1"`, `ALL = "all"` and a default of `ALL`.

A `StrEnum` rather than a plain string for the reason the other four are: librdkafka accepts
several spellings and silently ignores nothing, so an unrecognised value would otherwise select
a behaviour quietly. Pydantic rejects it at startup naming the value, which is R4.7's second
clause. `LifecycleEventProducer.__init__` passes `settings.producer_acks` where `"all"` is
hardcoded today, and the default keeps every existing run byte-identical (R4.13).

### D6 — The `acks` banner is the producer's, not the consumer's — *R4.8*

R4.8 points at "the startup banner R3.23 already puts in place", which is the *consumer*
banner in `runtime.py`. `acks` is a producer setting and is meaningless on a consumer, so the
line goes in the producer's lifespan log at `producer/app.py`, which already prints brokers and
topic. The criterion is satisfied by the analogous banner rather than the literal one, and this
paragraph is the record of that reading.

### D7 — A delivery failure names the partition it was for — *R4.9*

`DeliveryFailed` is currently raised as `str(err)`, which under `acks=all` on a degraded cluster
produces a bare librdkafka string with no indication of which partition refused. The delivery
callback already receives `msg`, so the error carries the topic partition alongside the error
itself. This is a small change and it is the difference between a failover experiment that
teaches something and one that prints noise.

### D8 — `min.insync.replicas` is deliberately not set — *R4.14*

With no `min.insync.replicas`, `acks=all` means "every replica currently in the ISR" — and if
the ISR has shrunk to one, a single replica satisfies it. `acks=all` alone therefore does not
guarantee more than one copy of an acknowledged write.

Setting it here would be one line, and is rejected: honouring it means also building the
`NOT_ENOUGH_REPLICAS` refusal path, its retry behaviour, and a second staged failure — a whole
second lesson competing with failover for one document. It is named in `docs/replication.md` as
the open half of the contract rather than quietly left out.

### D9 — Upgrading costs one `docker compose down -v` — *R4.14*

The existing `kafka-data` volume holds a metadata log formatted with a single-voter quorum.
KRaft fixes the controller quorum at format time, so the cluster cannot grow into it; the volume
must go. R0.9 already defines `down -v` as the reset, and `create_topics.sh` already has to
follow it, so this adds a documented step rather than a new concept.

This is also why the two extra brokers are not behind a compose profile: a three-voter quorum
with two nodes unstarted elects no controller and nothing comes up. "Optional cluster" is not a
reachable state.

### D10 — R4.5, R4.10 and R4.12 are satisfied by the broker, and we build nothing — *R4.5, R4.10, R4.12*

Leader election from the ISR, the producer's transparent reconnection to a new leader, and a
restarted node rejoining the ISR are all things Kafka does on its own. There is no code, no
config beyond D1, and no task that implements them.

They are still criteria rather than prose because they are what the feature is *for*: each one
is checkable against the running system with `kafka-topics --describe` and
`scripts/place_orders.sh`, and each one would fail visibly if D1's replication or D3's bootstrap
list were wrong. The reporting side of R4.5 is already built — `create_topics.sh` describes
every topic it creates, and that output is where leader, replicas and ISR are read.

Recording this explicitly is the alternative to leaving three criteria untraceable and letting a
later reader assume something was forgotten.

## Environment surface

| Variable | Default | Read by | Criterion |
|---|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | all three brokers | producer, consumers | R4.11 |
| `REPLICATION_FACTOR` | `3` | `scripts/create_topics.sh` | R4.4, R4.6 |
| `PRODUCER_ACKS` | `all` | producer | R4.7, R4.8 |

No credentials and no new secrets; the three added variables are all non-sensitive.

## Known gaps, by intent

| Gap | Status |
|---|---|
| `acks=all` satisfied by an ISR of one | open by design (D8) — named in the doc |
| Unclean leader election, committed-data loss | out of scope; needs D8 first |
| No measurement of what `acks` costs in latency | needs the load generator R2.33 excluded from this ladder |
| Replica placement, rack awareness, reassignment | never claimed |
| Producer `OrderStore` still in memory | 001 D9; unrelated to broker durability |

## Deferred to later specs

`min.insync.replicas` and the `NOT_ENOUGH_REPLICAS` path sit naturally with **005** (retries,
DLQ, poison messages), which builds the retry machinery a refused write needs. Exactly-once
production, where `acks=all` becomes mandatory rather than a choice, is **008**.
