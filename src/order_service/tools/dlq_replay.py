"""Read the dead-letter topic, and put messages back only when told to (005 D10).

Nothing consumes the dead-letter topic as part of normal operation (R5.15). This is the
tool that does it on purpose, and its default is to **report**, not to publish — because
a process that drains the topic on a loop is an unbounded retry topic wearing a different
name, and the topic's whole value is that it is terminal and someone has to look.

The intended sequence is: read what failed, fix the cause, then replay.

Replay publishes to ``x-original-topic``, which means all three consumer groups see the
message again — not only the one that gave up. The two that already succeeded absorb it
through 003's sequence guard and log ``DUPLICATE_ABSORBED``. That is the at-least-once
cost 003 recorded arriving from a new direction, and 008 is where it goes to zero.

Two things keep a replay from becoming the loop this tool exists to avoid.

**A run is bounded by a snapshot taken before the first publish** (R5.17). Each partition's
high watermark is recorded up front and the run stops there. Republishing provokes failures
that land back in this topic within milliseconds, so a tool that read to the *live* end of
the topic would read its own consequences and republish them, forever — which is exactly
what the first version of this file did. Messages a run causes belong to the next run,
when a human has chosen to look again.

**Non-retryable dead letters are not republished by default** (R5.25). Non-retryable means
the same bytes deterministically produce the same failure, so replaying them unchanged is
guaranteed to refill the topic — one message in, one dead letter per consumer group out.
They are still listed and counted, because hiding them would defeat the point of a tool
whose job is to show what is in there. ``--include-poison`` replays them anyway, which is
how the poison round trip is watched deliberately.

Neither guard helps a *transient* failure whose cause has not been fixed. Nothing here can
know whether it has been; that judgement is the human's, and is the reason ``--publish``
exists at all.

Usage::

    # look, change nothing
    docker compose run --rm retry-worker python -m order_service.tools.dlq_replay

    # only what inventory gave up on
    ... python -m order_service.tools.dlq_replay --service inventory

    # actually put them back
    ... python -m order_service.tools.dlq_replay --service inventory --publish
"""

import argparse
import logging
import sys
from dataclasses import dataclass

from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Message,
    Producer,
    TopicPartition,
)

from order_service.config import Settings, get_settings
from order_service.consumer.dlq import (
    HDR_ATTEMPTS_MADE,
    HDR_CONSUMER_GROUP,
    HDR_ERROR_CLASS,
    HDR_ERROR_MESSAGE,
    HDR_ORIGINAL_OFFSET,
    HDR_ORIGINAL_PARTITION,
    HDR_ORIGINAL_TOPIC,
    HDR_SERVICE,
    decode_headers,
)
from order_service.consumer.errors import NonRetryableError

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("dlq_replay")

#: Required to build a Consumer, and otherwise unused: this tool assigns partitions
#: explicitly rather than subscribing, so it never joins the group and has no position to
#: record. "What is in the dead-letter topic" must not depend on who looked at it last.
INSPECT_GROUP = "dlq-replay-inspector"

#: A stall guard, not the stop condition. The run ends when every partition reaches the
#: watermark recorded for it; this only bounds the wait if one of them goes quiet first.
_STALL_TIMEOUT_SECONDS = 5.0

#: How long to wait for topic metadata and watermarks.
_METADATA_TIMEOUT_SECONDS = 10.0

#: Error classes whose messages replay cannot fix. Matched against the ``x-error-class``
#: header, which carries the concrete exception's name — a future subclass of
#: NonRetryableError would need adding here to be recognised.
NON_RETRYABLE_ERROR_CLASSES = frozenset({NonRetryableError.__name__})


@dataclass(frozen=True)
class Snapshot:
    """The dead-letter topic as it stood before the run published anything.

    Attributes:
        assignments: One partition per entry, positioned at its earliest live offset.
        end_offsets: Partition to the high watermark read at snapshot time. A partition
            holding nothing appears in neither field.
    """

    assignments: list[TopicPartition]
    end_offsets: dict[int, int]

    @property
    def message_count(self) -> int:
        """Return how many messages the run will read, across all partitions."""
        return sum(
            end - tp.offset
            for tp in self.assignments
            if (end := self.end_offsets[tp.partition]) > tp.offset
        )


def build_consumer(settings: Settings) -> Consumer:
    """Build a consumer that reads the dead-letter topic without recording a position."""
    return Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": INSPECT_GROUP,
            # Partitions are assigned at explicit offsets below, so there is no reset to
            # fall back on — and nothing is ever committed for a later run to resume from.
            "enable.auto.commit": False,
            "client.id": "dlq-replay",
        }
    )


def snapshot_topic(consumer: Consumer, topic: str) -> Snapshot:
    """Record where every partition of ``topic`` starts and ends, right now.

    Reading the watermarks *before* publishing anything is what makes a run terminate:
    the messages a replay provokes land at or beyond the offsets recorded here, and are
    therefore not part of this run.

    Args:
        consumer: The consumer to query the cluster through.
        topic: The topic to snapshot.

    Returns:
        The partitions to read and the offset each one stops at.

    Raises:
        KafkaException: If the topic's metadata cannot be read.
    """
    metadata = consumer.list_topics(topic=topic, timeout=_METADATA_TIMEOUT_SECONDS)
    described = metadata.topics.get(topic)
    if described is None or described.error is not None:
        raise KafkaException(f"cannot read metadata for {topic}: {described}")

    assignments: list[TopicPartition] = []
    end_offsets: dict[int, int] = {}
    for partition in sorted(described.partitions):
        low, high = consumer.get_watermark_offsets(
            TopicPartition(topic, partition),
            timeout=_METADATA_TIMEOUT_SECONDS,
            cached=False,
        )
        # An empty partition is complete before the loop starts, so it is left out of
        # both lists rather than becoming something the run waits on.
        if high <= low:
            continue
        assignments.append(TopicPartition(topic, partition, low))
        end_offsets[partition] = high

    return Snapshot(assignments=assignments, end_offsets=end_offsets)


def describe(message: Message, headers: dict[str, str]) -> str:
    """Render one dead letter as a single readable line."""
    key = message.key().decode("utf-8", "replace") if message.key() else "<null>"
    return (
        f"  {key:<24} "
        f"service={headers.get(HDR_SERVICE, '?'):<13} "
        f"group={headers.get(HDR_CONSUMER_GROUP, '?'):<22} "
        f"attempts={headers.get(HDR_ATTEMPTS_MADE, '?'):<2} "
        f"origin={headers.get(HDR_ORIGINAL_TOPIC, '?')}"
        f"-{headers.get(HDR_ORIGINAL_PARTITION, '?')}"
        f"@{headers.get(HDR_ORIGINAL_OFFSET, '?')}\n"
        f"      {headers.get(HDR_ERROR_CLASS, '?')}: "
        f"{headers.get(HDR_ERROR_MESSAGE, '<no message>')}"
    )


def replay(settings: Settings, args: argparse.Namespace) -> int:
    """Read the dead-letter topic and, if asked, republish what it holds.

    Args:
        settings: Resolved environment settings.
        args: Parsed command-line arguments.

    Returns:
        A process exit code.
    """
    consumer = build_consumer(settings)
    snapshot = snapshot_topic(consumer, settings.dlq_topic)
    consumer.assign(snapshot.assignments)
    producer = Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "acks": "all",
            "partitioner": "consistent_random",
            "client.id": "dlq-replay",
        }
    )

    verb = "replaying" if args.publish else "inspecting (nothing will be published)"
    logger.info("%s %s", verb, settings.dlq_topic)
    logger.info(
        "snapshot: %d message(s) across %d partition(s)",
        snapshot.message_count,
        len(snapshot.assignments),
    )
    if args.service:
        logger.info("restricted to service=%s", args.service)
    if args.include_poison:
        logger.info("including non-retryable dead letters")
    logger.info("")

    # Partition to the offset it stops at; emptied as each one reaches its watermark.
    pending = dict(snapshot.end_offsets)
    seen = 0
    matched = 0
    excluded = 0
    published = 0
    try:
        while pending:
            message = consumer.poll(_STALL_TIMEOUT_SECONDS)
            if message is None:
                logger.warning(
                    "stalled %.0fs short of the snapshot on partition(s) %s — "
                    "reporting what was read",
                    _STALL_TIMEOUT_SECONDS,
                    sorted(pending),
                )
                break
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("consume error: %s", message.error())
                break

            end = pending.get(message.partition())
            if end is None:
                # Past the watermark recorded for this partition: caused by this run's
                # own publishing, and the next run's to deal with.
                continue
            in_scope = message.offset() < end
            if message.offset() + 1 >= end:
                del pending[message.partition()]
            if not in_scope:
                continue

            seen += 1
            headers = decode_headers(message)
            if args.service and headers.get(HDR_SERVICE) != args.service:
                continue

            matched += 1
            logger.info("%s", describe(message, headers))

            # Decided even on a read-only run, so a dry run reports what --publish would
            # actually do rather than what it would like to.
            if (
                headers.get(HDR_ERROR_CLASS) in NON_RETRYABLE_ERROR_CLASSES
                and not args.include_poison
            ):
                excluded += 1
                logger.info(
                    "      excluded — non-retryable; --include-poison to replay anyway"
                )
            elif args.publish:
                target = headers.get(HDR_ORIGINAL_TOPIC)
                if not target:
                    logger.warning("      skipped — no %s header", HDR_ORIGINAL_TOPIC)
                    continue
                # Headers are deliberately NOT carried back. The replayed message must
                # look exactly like the original to every consumer, or a service would
                # see failure metadata on a message it is processing for the first time.
                producer.produce(topic=target, key=message.key(), value=message.value())
                published += 1
                logger.info("      → republished to %s", target)

            if args.limit and matched >= args.limit:
                logger.info("\nstopping at --limit %d", args.limit)
                break
    finally:
        remaining = producer.flush(30.0)
        consumer.close()

    logger.info("\n%d dead letter(s) read, %d matched", seen, matched)
    if excluded:
        logger.info("%d excluded as non-retryable", excluded)
    if args.publish:
        if remaining:
            logger.error("%d message(s) were NOT delivered — rerun to finish", remaining)
            return 1
        logger.info("%d republished to their original topics", published)
        logger.info(
            "every consumer group re-reads them; the ones that already succeeded "
            "will log DUPLICATE_ABSORBED"
        )
    elif matched:
        logger.info("nothing was published — pass --publish to put these back")
    return 0


def main() -> None:
    """Parse arguments and run the replay tool."""
    parser = argparse.ArgumentParser(
        prog="dlq_replay",
        description=(
            "Inspect the dead-letter topic, and republish its messages to the topic "
            "they came from. Reports and changes nothing unless --publish is given."
        ),
    )
    parser.add_argument(
        "--publish",
        action="store_true",
        help="actually republish. Without it this tool only reports (R5.17).",
    )
    parser.add_argument(
        "--service",
        help="only messages this service gave up on, e.g. inventory (R5.18)",
    )
    parser.add_argument(
        "--include-poison",
        action="store_true",
        help=(
            "republish non-retryable dead letters too. They are excluded by default "
            "because replaying them unchanged fails identically (R5.25)."
        ),
    )
    parser.add_argument(
        "--limit", type=int, default=0, help="stop after this many matching messages"
    )
    args = parser.parse_args()

    settings = get_settings()
    if args.service and args.service not in {"inventory", "notification", "analytics"}:
        parser.error(f"unknown service {args.service!r}")
    sys.exit(replay(settings, args))


if __name__ == "__main__":
    main()
