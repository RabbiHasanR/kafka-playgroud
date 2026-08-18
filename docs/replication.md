# Replication, `acks`, and Failover

Spec [004](../specs/004-replication-acks-failover/requirements.md).

Every spec before this one ran on **one copy of every message**. One broker, replication
factor 1. Stopping the broker was never an experiment because there was nothing to observe —
everything stopped at once.

This is the first feature that looks at the broker side, and at the one producer setting that
had been hardcoded since 001.

---

## 1. Three things, not one

The word "replication" hides three separate settings that people routinely conflate.

| | What it is | Where it is set | Who owns it |
|---|---|---|---|
| **Broker count** | How many nodes exist | `docker-compose.yml` | the cluster |
| **Replication factor** | How many copies of each partition | `--replication-factor` at topic creation | **the topic** |
| **`acks`** | How many copies must confirm before a write is "done" | producer config | **the producer** |

The middle row is the one worth pausing on. **Replication factor belongs to the topic, not to
the cluster.** A three-broker cluster does not make your topics replicated — it makes
replication *available*. You can, and in §5 you will, create an RF 1 topic on a perfectly
healthy three-broker cluster and watch it lose a partition.

---

## 2. Replicas versus the in-sync replica set

```
$ docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --describe --topic order-lifecycle

Topic: order-lifecycle  PartitionCount: 3  ReplicationFactor: 3
  Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
  Partition: 1  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
  Partition: 2  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
```

Three facts on each line, and they are not the same fact:

- **Replicas** — the copies that are *supposed* to exist. Fixed at creation. Does not change
  when a broker dies.
- **Isr** — the in-sync replica set: the copies that are actually caught up *right now*.
  Shrinks within seconds of a broker dying, grows back when it returns.
- **Leader** — the one replica handling all reads and writes for that partition. Always a
  member of the ISR.

Note that the controller spread the leaders across all three nodes rather than piling them on
one. Leadership is balanced per partition, which is why losing one broker costs you a third of
the leadership rather than all of it.

**The ISR is the number that matters**, and it is the only one of the three that moves.

---

## 3. What each `acks` value buys

One environment variable, `PRODUCER_ACKS`, read by the order service.

| Value | Producer waits for | Loses data when | Cost |
|---|---|---|---|
| `0` | nothing at all | almost anything | none — and no delivery report worth reading |
| `1` | the leader's local log | the leader dies before followers catch up | one network round trip |
| `all` | every replica **currently in the ISR** | see the caveat below | slowest, and the default |

`acks=0` is worth running once precisely because of how *fine* everything looks. The producer
returns immediately, reports no errors, and notices nothing when a broker dies mid-run. The
absence of a complaint is not evidence that the write landed.

### The caveat this feature does not close

**`acks=all` does not mean "all replicas". It means "all replicas currently in sync".**

If two of three brokers are down, the ISR for a partition is `{leader}` — one member. A write
acknowledged by that one replica has satisfied `acks=all` completely, and it exists in exactly
one copy. `acks=all` alone therefore guarantees nothing about how many copies of your
acknowledged data exist.

The setting that closes this is `min.insync.replicas` on the topic: set it to 2 and the broker
*refuses* the write with `NOT_ENOUGH_REPLICAS` rather than accepting it into a degraded ISR.
Durability is a contract with two halves, and the producer only holds one of them.

**It is deliberately not set here** (004 D8) — honouring a refusal means building the retry
path that answers it, which is spec **005**'s subject. Exactly-once production, where
`acks=all` stops being a choice, is **008**.

---

## 4. Failover, start to finish

```bash
docker compose up -d --build
./scripts/create_topics.sh                       # RF 3 by default now
./scripts/place_orders.sh 5 --advance

# who leads what
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic order-lifecycle

# kill a leader — pick a node that leads a partition above
docker stop kafka-2

# within a few seconds: a new Leader, and Isr down to two members
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic order-lifecycle

# the service never restarted and was never reconfigured
./scripts/place_orders.sh 5 --advance

docker start kafka-2                             # Isr returns to three
```

Three things are worth noticing, and none of them are things this repository built:

1. **Leadership moved without anyone asking.** The controller elected a new leader from the
   ISR. No operator action, no client restart.
2. **The producer followed it.** librdkafka refreshed its metadata, found the new leader, and
   carried on. The order service did not know anything happened.
3. **`Replicas` never changed; `Isr` did.** The cluster still intends three copies. It
   temporarily has two.

### Why the bootstrap list has three addresses

`KAFKA_BOOTSTRAP_SERVERS` names all three brokers, and that is load-bearing rather than tidy.

A client only needs *one* reachable address — it discovers the rest of the cluster from
metadata immediately after connecting. But bootstrap happens at **startup**, before any
metadata exists. With a single-entry list pointing at `kafka`, stopping `kafka` means every
consumer and the order service cannot start at all, and the failure looks like broken failover
rather than what it is.

To see it: stop `kafka` (not `kafka-2`) and restart a consumer. It joins through `kafka-2`.

---

## 5. Replication is a topic property — the proof

The sharpest demonstration in this feature, and it needs no second cluster:

```bash
# an under-replicated topic on a perfectly healthy 3-broker cluster
REPLICATION_FACTOR=1 ORDER_LIFECYCLE_TOPIC=rf1-scratch ./scripts/create_topics.sh

# each partition has exactly one replica, on one node
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic rf1-scratch

# stop whichever node leads one of its partitions
docker stop kafka-3

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic rf1-scratch
#   Leader: none   Isr:            ← that partition is simply gone
```

Meanwhile `order-lifecycle` at RF 3, on the same cluster, at the same moment, is fine.

That contrast is the whole lesson. The cluster was never the thing protecting your data — the
topic's replication factor was. Three brokers with RF 1 topics buys you nothing at all.

---

## 6. The upgrade cost, once

The cluster is **not** optional and there is no single-broker mode to go back to.

KRaft writes the controller quorum voters into the metadata log when a node first formats its
volume. The old `kafka-data` volume was formatted with a one-voter quorum and cannot grow into
a three-voter one, so the upgrade is:

```bash
docker compose down -v          # discards every topic and message
docker compose up -d --build
./scripts/create_topics.sh
```

This is also why the two extra brokers are not behind a compose profile: a three-voter quorum
with two nodes unstarted elects no controller and nothing comes up. "Optional cluster" is not a
reachable state — it is three nodes or a re-format.

The producer's `OrderStore` is in memory (001 D9), so `down -v` also strips any in-flight
order's ability to advance. Place the orders an experiment needs *after* the reset.

Running cost: three JVMs instead of one.

---

## 7. What is still open

| Gap | Where it closes |
|---|---|
| `acks=all` satisfied by an ISR of one — `min.insync.replicas` not set | **005**, with the retry path a refusal needs |
| Unclean leader election, and losing committed data on purpose | not scheduled; needs the row above first |
| What `acks` costs in latency, measured | needs a load generator, excluded from this ladder |
| Replica placement, rack awareness, `kafka-reassign-partitions` | never claimed |
| Producer `OrderStore` still in memory | 001 D9 — a source-of-truth problem, unrelated to broker durability |

The consumer side is untouched by this feature. Groups, rebalancing, and the durable fold
behave exactly as [consumer-groups.md](consumer-groups.md) and [durable-state.md](durable-state.md)
record — none of it depends on how many brokers there are.
