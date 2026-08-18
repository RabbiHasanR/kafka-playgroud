# 004 — Replication, `acks`, and Failover: Tasks

Implements [requirements.md](requirements.md) per [design.md](design.md).

Each task cites the requirement IDs it satisfies and, where relevant, the design decision it
follows.

**Read this before starting.** T3 changes the KRaft controller quorum, which the existing
`kafka-data` volume cannot grow into (D9). Between T3 and T5 the cluster must be recreated with
`docker compose down -v`, which discards every topic and every message, and the producer's
`OrderStore` is in memory by design — so any order placed before that point becomes
unadvanceable and `POST /orders/{id}/events` returns `404`. Place the orders an experiment needs
**after** T5, not before. This is spec 000's documented `down -v` reset behaving as specified, not a defect.

**Order matters.** T1–T5 leave a working three-broker cluster with topics on it; T6–T9 are the
producer changes that run against it. Doing the code first means testing `acks` against a
single broker, where every value behaves the same and nothing is learned.

**Three criteria have no dedicated task, by design.** Leader election, producer reconnection
and ISR rejoin are behaviour Kafka provides once the replicas exist, so they are delivered by
T2 and T3 building the cluster rather than by code of their own, and cited there. D10 records
why they are criteria anyway. Their reporting half — leader, replicas and ISR on one line —
comes from T5.

## Infrastructure

- [x] **T1** — Extract the existing `kafka` service's shared configuration into an
  `x-kafka-broker` YAML anchor in `docker-compose.yml` — image, listener security map, inter-broker
  listener name, healthcheck, learning-friendly tweaks, `CLUSTER_ID`. Leave the per-node values
  (node id, quorum voters, listeners, advertised listeners, volume, ports) on the service. This is
  a pure refactor: a plain `up` must still start one working broker before T2 adds anything.
  — *R4.1* — D1
- [x] **T2** — Add `kafka-2` and `kafka-3` from the T1 anchor: node ids 2 and 3, hostnames
  `kafka-2` / `kafka-3`, their own named volumes, host ports 9094 and 9095, and advertised
  listeners `INTERNAL://<hostname>:19092` plus `EXTERNAL://localhost:<host port>`. The first node
  keeps `container_name: kafka` — every documented `docker exec kafka …` depends on it.
  This task and T3 are also what deliver leader election and ISR rejoin: there is no code for
  them, only replicas for the controller to elect from.
  — *R4.1, R4.10, R4.12* — D1, D2, D10
- [x] **T3** — Set `KAFKA_CONTROLLER_QUORUM_VOTERS` to `1@kafka:9093,2@kafka-2:9093,3@kafka-3:9093`
  on all three nodes. Raise `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR`,
  `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR` and
  `KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR` to 3, set
  `KAFKA_TRANSACTION_STATE_LOG_MIN_ISR` to 2, and add `KAFKA_DEFAULT_REPLICATION_FACTOR: 3`.
  Replace the "Single-broker musts" comment block, which no longer describes the file.
  — *R4.2, R4.3* — D1
- [x] **T4** — Add an `x-kafka-bootstrap` anchor holding
  `kafka:19092,kafka-2:19092,kafka-3:19092` and use it in place of the literal `kafka:19092` in
  every service that talks to the cluster — `order-service`, `inventory-consumer`, the three
  `notification-consumer-*`, `analytics-consumer`, and `kafka-ui`. Make each consumer and the
  order service `depends_on` all three brokers being healthy.
  — *R4.11* — D3
- [x] **T5** — Change `scripts/create_topics.sh` to read `REPLICATION_FACTOR` from the
  environment, defaulting to **3**, replacing the pinned `REPLICATION_FACTOR=1` and the comment
  saying spec 004 would raise it. Keep the existing `--describe` output, which is what reports
  leader, replicas and ISR. Update the broker-running precondition check so it does not report a
  healthy cluster when only some nodes are up.
  — *R4.4, R4.5, R4.6* — D4

## The producer

- [x] **T6** — Add `ProducerAcks(StrEnum)` to `config.py` with `NONE = "0"`, `LEADER = "1"`,
  `ALL = "all"`, docstring in the style of `GroupProtocol` and `StateBackend`, and a
  `producer_acks` field on `Settings` defaulting to `ALL`. Change the
  `kafka_bootstrap_servers` default to `localhost:9092,localhost:9094,localhost:9095` so a
  host-run client also survives one node being down.
  — *R4.7, R4.11, R4.13* — D3, D5
- [x] **T7** — Replace the hardcoded `"acks": "all"` in `LifecycleEventProducer.__init__` with
  `settings.producer_acks`. Confirm the default leaves the producer's configuration identical to
  what it was before this feature.
  — *R4.7, R4.13* — D5
- [x] **T8** — Extend the order service's startup banner in `producer/app.py` — which already
  logs brokers and topic — with the `acks` value in effect. This is the producer's analogue of
  the consumer banner spec 003 established; D6 records why it is not the same line.
  — *R4.8* — D6
- [x] **T9** — Carry the topic partition into delivery failures: the delivery callback already
  receives `msg`, so `DeliveryFailed` should name the topic and partition alongside the broker's
  error instead of raising a bare `str(err)`.
  — *R4.9* — D7

## Configuration and documentation

- [x] **T10** — Document `REPLICATION_FACTOR` and `PRODUCER_ACKS` in `.env.example` under a
  spec-004 heading, both commented out at their defaults. Neither is a credential, so both use
  the plain `${VAR:-default}` shape rather than 003's `${VAR:?…}` form.
  — *R4.13* — D4, D5
- [x] **T11** — Write `docs/replication.md`: replication factor versus the in-sync replica set,
  what each `acks` value buys and costs, a runnable failover walkthrough using
  `scripts/place_orders.sh` and `kafka-topics --describe`, the RF-1-scratch-topic contrast from
  D4, the one-time `down -v`, and an explicit statement that `min.insync.replicas` and unclean
  leader election remain open with 005 and 008 named as where they land.
  — *R4.14* — D4, D8, D9
- [x] **T12** — Add the spec 004 section to `README.md` following the shape of the 002 and 003
  sections, including the `down -v` upgrade note and the three-broker `docker exec` targets. Add
  004's own known-gaps table, and close the `Single broker, RF 1 | 004` rows in the 002 and 003
  gap tables.
  — *R4.14* — D9
