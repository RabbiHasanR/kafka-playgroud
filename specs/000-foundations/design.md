# 000 — Foundations: Design

Implements [requirements.md](requirements.md).

## Architecture

```
host                          docker network "kafka-playground_default"
─────────────────────         ────────────────────────────────────────
localhost:9092  ───────────►  kafka:9092   (EXTERNAL listener)
localhost:8080  ───────────►  kafka-ui:8080
                              kafka-ui ──► kafka:19092  (INTERNAL listener)
                              kafka    ──► kafka:9093   (CONTROLLER listener, self)

volume kafka-playground_kafka-data ──► /var/lib/kafka/data
```

Two containers: `kafka` (broker + controller) and `kafka-ui`. One named volume.

## Decisions

### D1 — KRaft, not ZooKeeper — *R0.1, R0.4*

ZooKeeper is removed entirely in Kafka 4.x, so a ZK-based setup would teach a dead
architecture. One node carries `process.roles=broker,controller` with itself as the
sole quorum voter (`KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093`).

*Rejected:* ZooKeeper mode — legacy, extra container, not available in 4.x.

### D2 — Three listeners — *R0.5, R0.6, R0.7*

The broker replies to every connection with its **advertised** address, and the
client reconnects to that. A single listener therefore cannot serve both host and
container clients: `localhost` is wrong inside a container, and the compose service
name is wrong on the host.

| Listener | Bind | Advertised | Used by |
|---|---|---|---|
| `EXTERNAL` | `0.0.0.0:9092` | `localhost:9092` | host clients, port-mapped |
| `INTERNAL` | `0.0.0.0:19092` | `kafka:19092` | other compose services, inter-broker |
| `CONTROLLER` | `0.0.0.0:9093` | — | KRaft quorum only |

All three are PLAINTEXT; this is a local learning environment (see Out of scope).
`INTERNAL` is the inter-broker listener, which matters once this grows to a cluster.

*Rejected:* single listener on `localhost:9092` — the standard "works from host,
breaks from container" failure. Documented as a gotcha in `README.md` rather than
hidden.

### D3 — Named volume for log dirs — *R0.8, R0.9*

`kafka-data:/var/lib/kafka/data`. A named volume survives `down` and is removed by
`down -v`, which maps exactly onto the two requirements.

*Rejected:* bind mount to a host path — Docker creates missing host directories as
root, which is precisely the ownership problem this project already hit.

### D4 — Healthcheck gates the UI — *R0.2, R0.3, R0.13*

`kafka-broker-api-versions.sh` against `localhost:9092` is the cheapest true
readiness probe: it requires the broker to be serving the client protocol, not just
to have an open port. `kafka-ui` uses `depends_on: condition: service_healthy`, so it
never starts against a broker that would refuse it.

`start_period: 20s` covers first-boot storage formatting without counting failures.

### D5 — Official `apache/kafka` image — *R0.10, R0.11*

Ships the CLI tools at `/opt/kafka/bin/`, so `docker exec -it kafka bash` is the whole
toolchain — nothing installed on the host. Log segments are readable at
`/var/lib/kafka/data/<topic>-<partition>/` for `kafka-dump-log.sh`.

Pinned to `4.3.1` — the latest 4.3 patch. Pinned rather than `latest` so the
environment does not change under an unrelated `docker compose pull`.

*Rejected:* `confluentinc/cp-kafka` — heavier, and its value (Schema Registry,
Connect) is out of scope. Revisit when a spec needs Avro.

### D6 — Fail-loud configuration — *R0.14, R0.15, R0.16*

| Setting | Value | Reason |
|---|---|---|
| `auto.create.topics.enable` | `false` | A mistyped topic name errors instead of silently creating an empty topic and producing a mystery "consumer sees nothing" |
| `num.partitions` | `3` | Default topics show real partition-distribution behaviour rather than the degenerate single-partition case |
| `group.initial.rebalance.delay.ms` | `0` | Default 3 s makes consumer-group experiments feel broken |

### D7 — Replication factor 1 on internal topics — *R0.17*

`__consumer_offsets`, the transaction state log, and the share-coordinator state topic
all default to a replication factor above 1 and fail to create on a single broker.
Each is pinned to 1, along with `transaction.state.log.min.isr`.

`group.coordinator.rebalance.protocols: classic,consumer` enables both the old and the
new (KIP-848) rebalance protocols, so group behaviour can be compared.

### D8 — Documentation split — *R0.18, R0.19*

`README.md` is a lookup table — the flags that get re-searched constantly
(`--property print.partition=true`, the offset-reset invocations) — plus a Gotchas
section for failure modes whose symptom does not resemble the cause.

Per-experiment observations belong in `notes/`, created as the work happens. Not
pre-stubbed: empty numbered files are clutter.

## Risks

| Risk | Mitigation |
|---|---|
| Port 9092 or 8080 already bound on the host | Compose fails loudly at `up`; change the host side of the mapping |
| `down -v` run by habit, losing an experiment | Called out explicitly in `README.md` lifecycle and Gotchas |
| Single broker teaches nothing about replication | Accepted for this spec; a 3-broker topology is a future spec |
| `kafka-ui:latest` is unpinned and may drift | Accepted — the UI is an optional convenience (`WHERE` clause in R0.12), not load-bearing |
