"""Kafka producer wrapper for the order service: keyed publishing, delivery reports.

``Producer.produce()`` only appends to librdkafka's internal queue; the broker's
acknowledgement arrives later on a delivery callback, and callbacks only fire while
somebody calls ``poll()``. Hence the background poll thread — without it a caller
waiting on a delivery report would wait forever.

"""

import logging
import threading
from dataclasses import dataclass

from confluent_kafka import KafkaException, Producer

from order_service.config import Settings
from order_service.events import LifecycleEvent

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DeliveryResult:
    """Where the broker actually put a message (R1.17, R1.23)."""

    partition: int
    offset: int


class DeliveryFailed(Exception):
    """The broker rejected the message or reported an error (R1.18)."""


class DeliveryTimeout(Exception):
    """No delivery report arrived within the configured timeout (R1.18)."""


class LifecycleEventProducer:
    """Publishes lifecycle events keyed by ``order_id``."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._producer = Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "acks": "all",
                # murmur2 hash of the key, matching the Java client. Every event for
                # one order therefore lands on one partition (R1.10).
                "partitioner": "consistent_random",
                "client.id": "order-service-producer",
            }
        )
        self._poll_stop = threading.Event()
        self._poll_thread: threading.Thread | None = None

    # -- lifecycle -------------------------------------------------------------

    def start(self) -> None:
        """Start the background thread that services delivery callbacks (D6)."""
        if self._poll_thread is not None:
            return
        self._poll_stop.clear()
        self._poll_thread = threading.Thread(
            target=self._poll_loop, name="order-service-poll", daemon=True
        )
        self._poll_thread.start()
        logger.info("producer poll thread started")

    def stop(self, flush_timeout: float = 10.0) -> None:
        """Flush buffered messages and stop the poll thread.

        Args:
            flush_timeout: Seconds to wait for the buffer to drain.
        """
        remaining = self._producer.flush(flush_timeout)
        if remaining:
            logger.warning("shutdown flush left %d message(s) undelivered", remaining)
        self._poll_stop.set()
        if self._poll_thread is not None:
            self._poll_thread.join(timeout=5.0)
            self._poll_thread = None
        logger.info("producer stopped")

    def topic_exists(self, timeout: float = 5.0) -> bool:
        """Report whether the configured topic exists on the broker.

        Args:
            timeout: Seconds to wait for cluster metadata.

        Returns:
            ``True`` if the broker knows the topic.

        Raises:
            KafkaException: If cluster metadata could not be fetched at all — an
                unreachable broker is a different failure from a missing topic.
        """
        metadata = self._producer.list_topics(timeout=timeout)
        topic = metadata.topics.get(self._settings.order_lifecycle_topic)
        return topic is not None and topic.error is None

    def _poll_loop(self) -> None:
        """Serve delivery callbacks until stopped."""
        while not self._poll_stop.is_set():
            self._producer.poll(0.1)

    # -- publishing ------------------------------------------------------------

    def publish_and_wait(
        self, event: LifecycleEvent, *, timeout: float | None = None
    ) -> DeliveryResult:
        """Publish an event and block until the broker acknowledges it.

        Must not run on an event loop — the route handlers are synchronous ``def``
        for exactly this reason (D6).

        Args:
            event: The event to publish.
            timeout: Seconds to wait for the delivery report. Defaults to the
                configured ``delivery_timeout_seconds``.

        Returns:
            The partition and offset the broker assigned.

        Raises:
            DeliveryFailed: If the broker reported an error, or the topic is missing.
            DeliveryTimeout: If no delivery report arrived in time.
        """
        wait = (
            timeout if timeout is not None else self._settings.delivery_timeout_seconds
        )
        done = threading.Event()
        outcome: dict[str, object] = {}

        def on_delivery(err: object, msg: object) -> None:
            if err is not None:
                outcome["error"] = err
            else:
                outcome["result"] = DeliveryResult(
                    partition=msg.partition(),  # type: ignore[attr-defined]
                    offset=msg.offset(),  # type: ignore[attr-defined]
                )
            done.set()

        self._produce(event, on_delivery=on_delivery)

        if not done.wait(wait):
            # librdkafka treats an unknown topic as retriable, so a missing topic
            # surfaces as a plain timeout. R1.11 asks for an explicit error — but only
            # when metadata actually says it is missing; a failing metadata call means
            # an unreachable broker, which is a different fault.
            try:
                topic_missing = not self.topic_exists()
            except KafkaException:
                topic_missing = False
            if topic_missing:
                raise DeliveryFailed(
                    f"topic '{self._settings.order_lifecycle_topic}' does not exist "
                    "and auto-creation is disabled — run scripts/create_topics.sh"
                )
            raise DeliveryTimeout(
                f"no delivery report for {event.order_id} seq {event.sequence} "
                f"within {wait}s"
            )
        if "error" in outcome:
            raise DeliveryFailed(str(outcome["error"]))
        return outcome["result"]  # type: ignore[return-value]

    def _produce(self, event: LifecycleEvent, *, on_delivery: object) -> None:
        """Enqueue an event for delivery, keyed by ``order_id``.

        Raises:
            DeliveryFailed: If librdkafka refused to enqueue the message.
        """
        try:
            self._producer.produce(
                topic=self._settings.order_lifecycle_topic,
                key=event.order_id.encode("utf-8"),
                value=event.model_dump_json().encode("utf-8"),
                on_delivery=on_delivery,  # type: ignore[arg-type]
            )
        except BufferError as exc:
            raise DeliveryFailed(f"producer queue is full: {exc}") from exc
        except KafkaException as exc:
            raise DeliveryFailed(str(exc)) from exc
