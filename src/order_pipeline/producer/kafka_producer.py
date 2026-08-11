"""Kafka producer wrapper: keyed publishing, delivery reports, sequence counters.

Two behaviours here are worth understanding rather than skimming.

**Publishing is asynchronous even when it looks synchronous.**
``Producer.produce()`` only appends to librdkafka's internal queue and returns; the
broker's acknowledgement arrives later on a delivery callback, and callbacks only
fire while somebody calls ``poll()``. That is why :class:`OrderEventProducer` runs a
background poll thread (D7) — without it a caller waiting on a delivery report would
wait forever.

**The message key is the whole ordering story.**
The key is the UTF-8 ``order_id``; librdkafka hashes it (murmur2, matching the Java
client) to pick a partition, so every event for one order lands on one partition and
is therefore ordered. Publish with ``keyed=False`` and the partitioner picks at
random instead — which is exactly how the R1.15 fault injection breaks ordering.
"""

import logging
import threading
from collections.abc import Callable
from dataclasses import dataclass

from confluent_kafka import KafkaException, Producer

from order_pipeline.config import Settings
from order_pipeline.events import OrderEvent

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DeliveryResult:
    """Where the broker actually put a message (R1.12).

    Attributes:
        partition: Partition the broker assigned.
        offset: Offset within that partition.
    """

    partition: int
    offset: int


class DeliveryFailed(Exception):
    """The broker rejected the message or reported an error (R1.13)."""


class DeliveryTimeout(Exception):
    """No delivery report arrived within the configured timeout (R1.13)."""


class OrderEventProducer:
    """Publishes order events, assigning per-order sequence numbers.

    Sequence counters live in process memory (D8). Restarting the producer resets
    them, which makes an order spanning a restart restart at sequence 1 — the same
    "derived state does not survive" lesson the consumer teaches from the other side
    of the pipe.
    """

    def __init__(self, settings: Settings) -> None:
        """Initialise the producer.

        Args:
            settings: Resolved runtime settings.
        """
        self._settings = settings
        self._producer = Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                # Wait for all in-sync replicas. On the single broker of spec 000
                # that is just one; spec 004 makes this setting meaningful.
                "acks": "all",
                # murmur2 hash of the key, random when the key is null (D2).
                "partitioner": "consistent_random",
                # Disable sticky partitioning for null-key messages. By default
                # librdkafka keeps *all* null-key messages produced within a 10 ms
                # window on one partition to improve batching — which silently
                # keeps an unkeyed order together and makes the R1.15 fault
                # injection fail to inject anything. Zero forces a genuine
                # per-message random choice. Has no effect on keyed messages.
                "sticky.partitioning.linger.ms": 0,
                "client.id": "order-pipeline-producer",
            }
        )
        self._sequences: dict[str, int] = {}
        self._lock = threading.Lock()
        self._poll_stop = threading.Event()
        self._poll_thread: threading.Thread | None = None

    # -- lifecycle -------------------------------------------------------------

    def start(self) -> None:
        """Start the background thread that services delivery callbacks (D7)."""
        if self._poll_thread is not None:
            return
        self._poll_stop.clear()
        self._poll_thread = threading.Thread(
            target=self._poll_loop, name="kafka-producer-poll", daemon=True
        )
        self._poll_thread.start()
        logger.info("producer poll thread started")

    def stop(self, flush_timeout: float = 10.0) -> None:
        """Flush buffered messages and stop the poll thread (R1.14).

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

        Auto-creation is disabled (R0.14, R1.9), so a missing topic is a setup
        error worth naming rather than letting it surface as a delivery timeout.

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
        topic = metadata.topics.get(self._settings.order_events_topic)
        return topic is not None and topic.error is None

    def flush(self, timeout: float = 10.0) -> int:
        """Block until buffered messages are delivered.

        Args:
            timeout: Seconds to wait for the buffer to drain.

        Returns:
            The number of messages still undelivered when the timeout expired.
        """
        return self._producer.flush(timeout)

    def _poll_loop(self) -> None:
        """Serve delivery callbacks until stopped."""
        while not self._poll_stop.is_set():
            self._producer.poll(0.1)

    # -- sequence assignment ---------------------------------------------------

    def next_sequence(self, order_id: str) -> int:
        """Return the next sequence number for an order (R1.2).

        Args:
            order_id: The order to advance.

        Returns:
            The next sequence, starting at 1 and increasing by exactly 1.
        """
        with self._lock:
            nxt = self._sequences.get(order_id, 0) + 1
            self._sequences[order_id] = nxt
            return nxt

    def peek_sequence(self, order_id: str) -> int:
        """Return the last sequence assigned to an order without advancing it.

        Args:
            order_id: The order to inspect.

        Returns:
            The last assigned sequence, or 0 if the order is unknown.
        """
        with self._lock:
            return self._sequences.get(order_id, 0)

    # -- publishing ------------------------------------------------------------

    def publish(
        self,
        event: OrderEvent,
        *,
        keyed: bool = True,
        on_delivery: Callable[[object, object], None] | None = None,
    ) -> None:
        """Publish an event without waiting for its delivery report.

        Used by the simulator, where blocking per event would cap throughput at one
        broker round-trip per message and defeat the lag experiment (D6).

        Args:
            event: The event to publish.
            keyed: When ``False``, publish with a null key so the partitioner picks
                at random and the order's events scatter (R1.15).
            on_delivery: Optional callback for the delivery report, used by the
                simulator to tally successes and failures. Defaults to logging
                errors only.

        Raises:
            DeliveryFailed: If the message could not be enqueued.
        """
        self._produce(
            event, keyed=keyed, on_delivery=on_delivery or self._log_delivery_error
        )

    def publish_and_wait(
        self, event: OrderEvent, *, keyed: bool = True, timeout: float | None = None
    ) -> DeliveryResult:
        """Publish an event and wait for the broker to acknowledge it.

        Blocks until the delivery callback fires, so the caller can report the real
        partition and offset (R1.12) and turn a broker failure into an error rather
        than a silent drop (R1.13). Callers must not run this on an event loop —
        the route handler is a synchronous ``def`` for exactly this reason (D5).

        Args:
            event: The event to publish.
            keyed: When ``False``, publish with a null key (R1.15).
            timeout: Seconds to wait for the delivery report. Defaults to the
                configured ``delivery_timeout_seconds``.

        Returns:
            The partition and offset the broker assigned.

        Raises:
            DeliveryFailed: If the broker reported an error.
            DeliveryTimeout: If no delivery report arrived in time.
        """
        wait = timeout if timeout is not None else self._settings.delivery_timeout_seconds
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

        self._produce(event, keyed=keyed, on_delivery=on_delivery)

        if not done.wait(wait):
            # librdkafka treats an unknown topic as retriable and keeps retrying
            # until the message timeout, so a missing topic surfaces as a plain
            # timeout. R1.9 asks for an explicit error, so name the real cause —
            # but only when metadata actually says the topic is missing. If the
            # metadata call itself fails the broker is unreachable, which is a
            # different fault and must not be reported as a missing topic.
            try:
                topic_missing = not self.topic_exists()
            except KafkaException:
                topic_missing = False
            if topic_missing:
                raise DeliveryFailed(
                    f"topic '{self._settings.order_events_topic}' does not exist "
                    "and auto-creation is disabled — run scripts/create_topics.sh"
                )
            raise DeliveryTimeout(
                f"no delivery report for {event.order_id} seq {event.sequence} "
                f"within {wait}s"
            )
        if "error" in outcome:
            raise DeliveryFailed(str(outcome["error"]))
        return outcome["result"]  # type: ignore[return-value]

    def _produce(self, event: OrderEvent, *, keyed: bool, on_delivery: object) -> None:
        """Enqueue an event for delivery.

        Args:
            event: The event to publish.
            keyed: Whether to attach the ``order_id`` as the message key.
            on_delivery: Callback invoked when the delivery report arrives.

        Raises:
            DeliveryFailed: If librdkafka refused to enqueue the message.
        """
        try:
            self._producer.produce(
                topic=self._settings.order_events_topic,
                key=event.order_id.encode("utf-8") if keyed else None,
                value=event.model_dump_json().encode("utf-8"),
                on_delivery=on_delivery,  # type: ignore[arg-type]
            )
        except BufferError as exc:
            raise DeliveryFailed(f"producer queue is full: {exc}") from exc
        except KafkaException as exc:
            raise DeliveryFailed(str(exc)) from exc

    @staticmethod
    def _log_delivery_error(err: object, msg: object) -> None:
        """Log a failed fire-and-forget delivery.

        Args:
            err: The librdkafka error, or ``None`` on success.
            msg: The message the report refers to.
        """
        if err is not None:
            logger.error("delivery failed: %s", err)
