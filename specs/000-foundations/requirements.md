# 000 — Foundations: Local Kafka Environment

**Status:** implemented (written retroactively)
**Depends on:** none

## Overview

A Kafka cluster running locally in Docker, reachable from both the host and other
containers, with the full CLI toolset available for hands-on exploration of topics,
partitions, keys, consumer groups, offsets, and log segments.

It was a single node when this spec was written, and specs 001–003 were built against
that. Spec 004 grew it to three nodes; R0.4 and R0.17 carry the amendment, and every
other criterion here is unaffected by the broker count.

This spec is the substrate every later feature builds on. It deliberately covers
*environment*, not application behaviour.

## Out of scope

- Multi-broker clusters, replication, failover — **004**
- Schema Registry, Kafka Connect, Kafka Streams (future spec)
- Authentication, TLS, ACLs — this is a local learning environment on PLAINTEXT
- Any Python client code (future spec)

## User stories

**US-1** — As a developer, I want a Kafka broker running with one command, so that I
can start experimenting without cluster setup work.

**US-2** — As a developer, I want to reach the broker from both my host machine and
from other containers, so that neither host-run tools nor containerised services
need a different topology.

**US-3** — As a developer, I want messages and topics to survive a restart, so that
an experiment spanning several sessions is not lost.

**US-4** — As a developer, I want to run every Kafka CLI tool without installing
anything locally, so that the environment stays self-contained.

**US-5** — As a developer, I want to see topics, partitions, and consumer lag
visually, so that I can confirm what the CLI reported.

**US-6** — As a developer, I want mistakes to fail loudly, so that I learn the real
behaviour instead of debugging silent no-ops.

## Acceptance criteria

### Broker availability

- **R0.1** — WHEN `docker compose up -d` is run THE SYSTEM SHALL start a Kafka broker
  in KRaft mode with no ZooKeeper dependency.
- **R0.2** — WHILE the broker is starting THE SYSTEM SHALL report container health as
  unhealthy until the broker answers an API-versions request.
- **R0.3** — WHEN the broker has completed startup THE SYSTEM SHALL report container
  health as healthy within 60 seconds of `up`.
- **R0.4** — THE SYSTEM SHALL run the broker and controller roles on every node.
  *(amended by 004: was "on a single node". The criterion protects KRaft combined mode
  with no ZooKeeper, not the node count — see [X10](../../DECISIONS.md).)*

### Connectivity

- **R0.5** — WHEN a client on the host connects to `localhost:9092` THE SYSTEM SHALL
  accept the connection and advertise an address the host can resolve.
- **R0.6** — WHEN a client in another compose service connects to `kafka:19092` THE
  SYSTEM SHALL accept the connection and advertise an address that container can
  resolve.
- **R0.7** — THE SYSTEM SHALL keep controller traffic on a listener separate from
  both client listeners.

### Durability

- **R0.8** — WHEN `docker compose down` is followed by `docker compose up -d` THE
  SYSTEM SHALL retain all topics and their messages.
- **R0.9** — WHEN `docker compose down -v` is run THE SYSTEM SHALL discard all topic
  data. (Explicit, so data loss is never a surprise.)

### Tooling

- **R0.10** — THE SYSTEM SHALL make the Kafka CLI tools available inside the broker
  container at `/opt/kafka/bin/`.
- **R0.11** — THE SYSTEM SHALL expose the on-disk log segments inside the broker
  container at `/var/lib/kafka/data/` for inspection with `kafka-dump-log.sh`.
- **R0.12** — WHERE a web UI is included THE SYSTEM SHALL serve it on host port 8080
  and connect it to the broker over the internal listener.
- **R0.13** — THE SYSTEM SHALL start the UI only after the broker reports healthy.

### Learning ergonomics

- **R0.14** — IF a client references a topic that does not exist THEN THE SYSTEM SHALL
  return an error rather than creating the topic implicitly.
- **R0.15** — WHEN a topic is created without an explicit partition count THE SYSTEM
  SHALL give it 3 partitions.
- **R0.16** — WHEN a consumer group forms THE SYSTEM SHALL complete the initial
  rebalance without an artificial delay.
- **R0.17** — THE SYSTEM SHALL set the replication factor of all internal topics to the
  broker count, so that operation does not fail on under-replication.
  *(amended by 004: was pinned to 1 for single-broker operation — see
  [X10](../../DECISIONS.md).)*

### Documentation

- **R0.18** — THE SYSTEM SHALL provide a command reference covering topic management,
  produce/consume, keys and partitioning, consumer groups, offset resets, log-segment
  inspection, and retention/compaction.
- **R0.19** — THE SYSTEM SHALL document the `advertised.listeners` failure mode, the
  irreversibility of partition growth, and the interaction between `--from-beginning`
  and committed group offsets.
