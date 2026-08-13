"""The consumer: subscribe, fold, detect violations, commit.

Two things here are the whole lesson of spec 001.

**Offsets are committed after processing, never before (R1.25, D12).**
That makes this *at-least-once*: a crash between folding an event and committing its
offset means the event is re-delivered and re-folded on restart, inflating the
running total. That duplicate is not a bug to be patched here — it is the subject of
spec 004, and spec 009 resolves it properly.

**The folded state is never persisted (R1.27, X3).**
Kafka restores the committed offset on restart because the broker stores it. Nothing
restores the per-order totals, because nothing stored them. The consumer resumes in
exactly the right *place* with none of its *memory* — and the false violations that
follow are the evidence that those are two different things.
"""

import json
import logging
import signal
import sys
from types import FrameType

from confluent_kafka import Consumer, KafkaError, KafkaException, Message

from order_pipeline.config import Settings, get_settings
from order_pipeline.consumer.http import start_state_server
from order_pipeline.consumer.state import OrderStateStore
from order_pipeline.events import OrderEvent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
)

logger = logging.getLogger("order_pipeline.consumer")


class OrderEventConsumer:
    """Consumes order events and folds them into in-memory state."""

    def __init__(self, settings: Settings) -> None:
        """Initialise the consumer.

        Args:
            settings: Resolved runtime settings.
        """
        self._settings = settings
        self._store = OrderStateStore()
        self._running = False
        self._consumer = Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "group.id": settings.consumer_group_id,
                # Offsets are committed by hand, after processing (R1.25).
                "enable.auto.commit": False,
                # A group id with no committed offsets starts at the earliest
                # retained message (R1.28) — which is what makes the
                # replay-from-zero experiment work.
                "auto.offset.reset": "earliest",
                "client.id": "order-pipeline-consumer",
            }
        )

    @property
    def store(self) -> OrderStateStore:
        """Return the folded state store.

        Returns:
            The consumer's :class:`OrderStateStore`.
        """
        return self._store

    def stop(self) -> None:
        """Ask the consume loop to exit after the current iteration."""
        self._running = False

    def run(self) -> None:
        """Subscribe and consume until stopped.

        Raises:
            KafkaException: If the broker reports a fatal error.
        """
        topic = self._settings.order_events_topic
        self._consumer.subscribe([topic])
        self._running = True
        logger.info(
            "consuming topic=%s group=%s brokers=%s",
            topic,
            self._settings.consumer_group_id,
            self._settings.kafka_bootstrap_servers,
        )

        try:
            while self._running:
                message = self._consumer.poll(1.0)
                if message is None:
                    continue
                if message.error():
                    self._handle_error(message)
                    continue
                self._handle_message(message)
        finally:
            # Leaves the group cleanly and flushes any pending commit.
            self._consumer.close()
            logger.info("consumer closed")

    def _handle_error(self, message: Message) -> None:
        """Log a broker-reported error on a polled message.

        Args:
            message: The message carrying the error.

        Raises:
            KafkaException: If the error is fatal.
        """
        error = message.error()
        if error is not None and error.code() == KafkaError._PARTITION_EOF:
            return
        if error is not None and error.fatal():
            raise KafkaException(error)
        logger.error("consume error: %s", error)

    def _handle_message(self, message: Message) -> None:
        """Fold one message and commit its offset.

        The offset is committed only after the fold has been applied (R1.25). A
        message that cannot be parsed is logged and its offset committed anyway —
        retry and dead-letter handling is spec 006, and stalling here would block
        the partition.

        Args:
            message: The message to process.
        """
        raw = message.value()
        key = message.key()
        try:
            event = OrderEvent.model_validate(json.loads(raw.decode("utf-8")))
        except (ValueError, UnicodeDecodeError) as exc:
            logger.error(
                "undecodable message at %s[%d]@%d: %s",
                message.topic(),
                message.partition(),
                message.offset(),
                exc,
            )
            self._consumer.commit(message=message, asynchronous=False)
            return

        # R1.29 — partition, offset and key are logged for every record so the
        # key-to-partition mapping is visible without opening Kafka UI.
        logger.info(
            "partition=%d offset=%d key=%s order_id=%s seq=%d type=%s",
            message.partition(),
            message.offset(),
            key.decode("utf-8") if key is not None else "<null>",
            event.order_id,
            event.sequence,
            event.event_type,
        )

        violations = self._store.apply(event)
        for violation in violations:
            # R1.30 — WARNING and a stable marker, so `grep VIOLATION` is the whole
            # filtering story.
            logger.warning(
                "%s partition=%d offset=%d",
                violation.as_log_fields(),
                message.partition(),
                message.offset(),
            )

        self._consumer.commit(message=message, asynchronous=False)


def main() -> None:
    """Run the consumer and its state server until interrupted."""
    settings = get_settings()
    consumer = OrderEventConsumer(settings)
    server = start_state_server(
        consumer.store, settings.consumer_state_host, settings.consumer_state_port
    )

    def shutdown(signum: int, _frame: FrameType | None) -> None:
        logger.info("signal %d received, shutting down", signum)
        consumer.stop()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        consumer.run()
    except KafkaException as exc:
        logger.error("fatal kafka error: %s", exc)
        sys.exit(1)
    finally:
        server.shutdown()


if __name__ == "__main__":
    main()
