# 000 — Foundations: Tasks

Implements [requirements.md](requirements.md) per [design.md](design.md).

## Environment

- [x] **T1** — Define the `kafka` service on `apache/kafka:4.3.1` with
  `process.roles=broker,controller` and a single-node quorum voter. — *R0.1, R0.4* — D1
- [x] **T2** — Configure the EXTERNAL / INTERNAL / CONTROLLER listener triple with
  matching advertised addresses and security-protocol map. — *R0.5, R0.6, R0.7* — D2
- [x] **T3** — Map host port 9092 to the EXTERNAL listener. — *R0.5*
- [x] **T4** — Attach the `kafka-data` named volume at `/var/lib/kafka/data`. — *R0.8, R0.9, R0.11* — D3
- [x] **T5** — Pin replication factor 1 on the offsets, transaction-state, and
  share-coordinator internal topics; set `transaction.state.log.min.isr=1`. — *R0.17* — D7
- [x] **T6** — Set `auto.create.topics.enable=false`, `num.partitions=3`,
  `group.initial.rebalance.delay.ms=0`. — *R0.14, R0.15, R0.16* — D6
- [x] **T7** — Add a healthcheck running `kafka-broker-api-versions.sh` with a 20 s
  start period. — *R0.2, R0.3* — D4
- [x] **T8** — Define the `kafka-ui` service on host port 8080, pointed at
  `kafka:19092`, gated on `service_healthy`. — *R0.12, R0.13* — D4

## Verification

- [x] **T9** — Bring the stack up and confirm `kafka` reaches healthy. — *R0.1, R0.3*
- [x] **T10** — Confirm the broker answers an API-versions request on
  `localhost:9092`. — *R0.5*
- [x] **T11** — Create a 3-partition topic and confirm `--describe` reports leader,
  replicas, and ISR per partition. — *R0.10, R0.15*
- [ ] **T12** — Confirm a container-side client resolves and connects on
  `kafka:19092`. — *R0.6* — *(kafka-ui connects successfully, which exercises this;
  not yet verified independently)*
- [ ] **T13** — Confirm topics and messages survive `down` followed by `up -d`. — *R0.8*
- [ ] **T14** — Confirm a produce to a non-existent topic errors instead of
  auto-creating. — *R0.14*
- [ ] **T15** — Confirm `kafka-dump-log.sh` reads a segment under
  `/var/lib/kafka/data/`. — *R0.11*

## Documentation

- [x] **T16** — Write the CLI reference covering topics, produce/consume, keys and
  partitioning, consumer groups, offset resets, log inspection, and
  retention/compaction. — *R0.18*
- [x] **T17** — Document the `advertised.listeners` failure mode, one-way partition
  growth, and `--from-beginning` versus committed offsets. — *R0.19*

## Notes

T12–T15 are unticked because they have not actually been run — the configuration
supports them but the behaviour was never observed. Ticking them requires running
them.
