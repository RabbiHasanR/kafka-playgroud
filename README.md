# Kafka Playground

Single-broker Kafka 4.3.1 in KRaft mode (no ZooKeeper) for hands-on CLI learning.

| What | Where |
|---|---|
| Broker (from host) | `localhost:9092` |
| Broker (from other containers) | `kafka:19092` |
| Kafka UI | http://localhost:8080 |
| CLI tools | `/opt/kafka/bin/` inside the `kafka` container |
| Log segments on disk | `/var/lib/kafka/data/` inside the container |

## Lifecycle

```bash
docker compose up -d          # start
docker compose ps             # status (wait for kafka = healthy)
docker compose logs -f kafka  # broker logs
docker compose down           # stop, keep data
docker compose down -v        # stop, WIPE all topics/messages
```

Get a shell inside the broker — most commands below assume you're here:

```bash
docker exec -it kafka bash
cd /opt/kafka/bin
```

`BS=localhost:9092` is used as shorthand below. Set it once per shell:

```bash
export BS=localhost:9092
```

## Topics

```bash
./kafka-topics.sh --bootstrap-server $BS --create --topic demo --partitions 3 --replication-factor 1
./kafka-topics.sh --bootstrap-server $BS --list
./kafka-topics.sh --bootstrap-server $BS --describe --topic demo
./kafka-topics.sh --bootstrap-server $BS --alter --topic demo --partitions 6   # grow only, never shrink
./kafka-topics.sh --bootstrap-server $BS --delete --topic demo
```

Reading `--describe`: `Leader` is the broker serving that partition, `Replicas` all
brokers holding it, `Isr` the in-sync ones. On a single broker all three are `1`.

Auto-create is **off** — a typo gives you an error instead of a silent empty topic.

## Produce / consume

```bash
./kafka-console-producer.sh --bootstrap-server $BS --topic demo
./kafka-console-consumer.sh --bootstrap-server $BS --topic demo --from-beginning
```

Useful consumer flags:

```bash
--property print.key=true \
--property print.partition=true \
--property print.offset=true \
--property print.timestamp=true \
--max-messages 10 \
--partition 0            # read one partition only
--offset earliest        # with --partition: earliest | latest | <number>
```

Without `--from-beginning` a fresh consumer only sees messages produced *after* it starts.

## Keys and partitioning

```bash
./kafka-console-producer.sh --bootstrap-server $BS --topic demo \
  --property parse.key=true --property key.separator=:
# then type:  user1:logged in
```

Same key always hashes to the same partition — that is Kafka's only ordering
guarantee (ordering holds *within* a partition, never across).

Keyless messages are spread round-robin (sticky-batched), so ordering is not preserved.

## Consumer groups

```bash
./kafka-console-consumer.sh --bootstrap-server $BS --topic demo --group g1

./kafka-consumer-groups.sh --bootstrap-server $BS --list
./kafka-consumer-groups.sh --bootstrap-server $BS --describe --group g1
./kafka-consumer-groups.sh --bootstrap-server $BS --describe --group g1 --members --verbose
```

`--describe` columns: `CURRENT-OFFSET` (committed), `LOG-END-OFFSET` (latest written),
`LAG` = the difference. Lag is the number to watch in production.

One partition is consumed by at most one member of a group. More consumers than
partitions means the extras sit idle. Start/kill members and re-run `--describe`
to watch rebalancing.

## Offsets

The group must have **no active members** to reset:

```bash
./kafka-consumer-groups.sh --bootstrap-server $BS --group g1 --topic demo --reset-offsets --to-earliest --execute
./kafka-consumer-groups.sh --bootstrap-server $BS --group g1 --topic demo --reset-offsets --to-latest --execute
./kafka-consumer-groups.sh --bootstrap-server $BS --group g1 --topic demo --reset-offsets --shift-by -10 --execute
./kafka-consumer-groups.sh --bootstrap-server $BS --group g1 --topic demo --reset-offsets --to-offset 42 --execute
./kafka-consumer-groups.sh --bootstrap-server $BS --group g1 --topic demo --reset-offsets --to-datetime 2026-08-09T00:00:00.000 --execute
```

Swap `--execute` for `--dry-run` to preview.

## Inspecting the raw log

```bash
ls /var/lib/kafka/data/demo-0/
./kafka-dump-log.sh --files /var/lib/kafka/data/demo-0/00000000000000000000.log --print-data-log
```

Shows real record batches, offsets, timestamps, and compression — the physical
form of the "log" abstraction.

Per-partition offset boundaries without consuming:

```bash
./kafka-get-offsets.sh --bootstrap-server $BS --topic demo               # latest
./kafka-get-offsets.sh --bootstrap-server $BS --topic demo --time earliest
```

## Retention and compaction

```bash
./kafka-topics.sh --bootstrap-server $BS --create --topic compacted --partitions 1 --replication-factor 1 \
  --config cleanup.policy=compact \
  --config min.cleanable.dirty.ratio=0.01 \
  --config segment.ms=5000 \
  --config delete.retention.ms=100

./kafka-configs.sh --bootstrap-server $BS --entity-type topics --entity-name demo --describe
./kafka-configs.sh --bootstrap-server $BS --entity-type topics --entity-name demo \
  --alter --add-config retention.ms=60000
```

Compaction keeps only the newest value per key. It runs on *closed* segments, so
you must produce enough to roll a segment (hence the tiny `segment.ms`) before
old values disappear. A key with a `null` value is a tombstone — it deletes the key.

## Other tools

```bash
./kafka-broker-api-versions.sh --bootstrap-server $BS   # connectivity check
./kafka-cluster.sh cluster-id --bootstrap-server $BS
./kafka-producer-perf-test.sh --topic demo --num-records 100000 --record-size 100 --throughput -1 --producer-props bootstrap.servers=$BS
./kafka-consumer-perf-test.sh --bootstrap-server $BS --topic demo --messages 100000
```

## Gotchas

- **`advertised.listeners` is what clients actually connect to.** The broker hands
  this address back on connect, so a client that reaches the broker fine can still
  fail on the next call if the advertised address is wrong. Hence the two listeners:
  `localhost:9092` for the host, `kafka:19092` for containers.
- **Partition count can only grow.** Growing it rehashes keys, so existing keys may
  move to a different partition and their historical ordering is broken.
- **`--from-beginning` is ignored when the group already has committed offsets.**
  Reset the offsets or use a new `--group` name.
- **`docker compose down -v` deletes the volume** and every message with it.










