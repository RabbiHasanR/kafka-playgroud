"""Kafka producer wrapper for the order service: keyed publishing, delivery reports.

What is deliberately *not* here is the sequence counter. The sequence belongs to the
order, so it lives on the :class:`~order_service.producer.orders.Order` record and is
allocated under the same lock that guards the transition check (D5) — the
aggregate-version pattern, and where a real service would keep it: in the row it later
commits.

**Publishing is asynchronous even when it looks synchronous.** ``Producer.produce()``
only appends to librdkafka's internal queue; the broker's acknowledgement arrives later
on a delivery callback, and callbacks only fire while somebody calls ``poll()``. Hence
the background poll thread — without it a caller waiting on a delivery report would
wait forever.
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
    """Where the broker actually put a message (R1.17, R1.23).

    Attributes:
        partition: Partition the broker assigned.
        offset: Offset within that partition.
    """

    partition: int
    offset: int


class DeliveryFailed(Exception):
    """The broker rejected the message or reported an error (R1.18)."""


class DeliveryTimeout(Exception):
    """No delivery report arrived within the configured timeout (R1.18)."""


class LifecycleEventProducer:
    """Publishes lifecycle events keyed by ``order_id``."""

    def __init__(self, settings: Settings) -> None:
        """Initialise the producer.

        Args:
            settings: Resolved runtime settings.
        """
        self._settings = settings
        self._producer = Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                # Wait for all in-sync replicas. On the single broker of spec 000 that
                # is just one; spec 004 makes this setting meaningful.
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

        Auto-creation is disabled (R0.14, R1.11), so a missing topic is a setup error
        worth naming rather than letting it surface as a delivery timeout.

        Args:
            timeout: Seconds to wait for cluster metadata.

        Returns:
            ``True`` if the broker knows the topic.

        Raises:
            KafkaException: If cluster metadata could not be fetched at all — an
                unreachable broker is a different failure from a missing topic and
                must not be reported as one.
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
        """Publish an event and wait for the broker to acknowledge it.

        Blocks until the delivery callback fires, so the caller can report the real
        partition and offset and turn a broker failure into an error rather than a
        silent drop. That matters: a lost ``ORDER_CREATED`` is an
        order that exists for the customer and for nobody else.

        Callers must not run this on an event loop — the route handlers are
        synchronous ``def`` for exactly this reason (D6).

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
            # librdkafka treats an unknown topic as retriable and keeps retrying until
            # the message timeout, so a missing topic surfaces as a plain timeout.
            # R1.11 asks for an explicit error, so name the real cause — but only when
            # metadata actually says the topic is missing. If the metadata call itself
            # fails the broker is unreachable, which is a different fault and must not
            # be reported as a missing topic.
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

        Args:
            event: The event to publish.
            on_delivery: Callback invoked when the delivery report arrives.

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
