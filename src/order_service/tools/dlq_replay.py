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

Replaying a message whose cause was **not** fixed sends it straight back to the
dead-letter topic. That is the loop this tool refuses to automate.

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

from confluent_kafka import Consumer, KafkaError, Message, Producer

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

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("dlq_replay")

#: A throwaway group, so reading the topic never records a position. Committing would
#: make "what is in the dead-letter topic" depend on who looked at it last.
INSPECT_GROUP = "dlq-replay-inspector"

#: How long to wait for a message before deciding the topic is drained.
_IDLE_TIMEOUT_SECONDS = 5.0


def build_consumer(settings: Settings) -> Consumer:
    """Build a consumer that reads the dead-letter topic without recording a position."""
    return Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": INSPECT_GROUP,
            # Never commits, always starts at the beginning: this tool answers "what is
            # in there", which must not depend on previous runs of itself.
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "client.id": "dlq-replay",
        }
    )


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
    consumer.subscribe([settings.dlq_topic])
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
    if args.service:
        logger.info("restricted to service=%s", args.service)
    logger.info("")

    seen = 0
    matched = 0
    published = 0
    try:
        while True:
            message = consumer.poll(_IDLE_TIMEOUT_SECONDS)
            if message is None:
                break
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("consume error: %s", message.error())
                break

            seen += 1
            headers = decode_headers(message)
            if args.service and headers.get(HDR_SERVICE) != args.service:
                continue

            matched += 1
            logger.info("%s", describe(message, headers))

            if args.publish:
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
        "--limit", type=int, default=0, help="stop after this many matching messages"
    )
    args = parser.parse_args()

    settings = get_settings()
    if args.service and args.service not in {"inventory", "notification", "analytics"}:
        parser.error(f"unknown service {args.service!r}")
    sys.exit(replay(settings, args))


if __name__ == "__main__":
    main()
