"""Kafka producer wrapper for the order service: keyed publishing, delivery reports.

``Producer.produce()`` only appends to librdkafka's internal queue; the broker's
acknowledgement arrives later on a delivery callback, and callbacks only fire while
somebody calls ``poll()``. Hence the background poll thread — without it a caller
waiting on a delivery report would wait forever.

From 008 the producer is **idempotent** by default (R8.1). It is not, and will not be,
*transactional*: a transaction covers a consume-process-produce cycle, and there is no
consumed offset here to fold into one. What idempotence buys is that ``retries`` can no
longer duplicate or — the part that matters — reorder. What nothing here buys is safety at
the HTTP boundary: a client whose request times out after the broker accepted the write
retries and produces a genuinely new event with a new ``event_id``. That gap needs an
idempotency key or a transactional outbox, and no spec claims either.

From 006 this class writes to two topics. Lifecycle events go to ``order-lifecycle`` and
block on their delivery report; snapshots and tombstones go to the compacted
``order-snapshot``, where the snapshot does *not* block and the tombstone does (006 D3).
A tombstone is simply a keyed message with ``value=None``.
"""

import json
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


def _describe_delivery_error(err: object, msg: object) -> str:
    """Render a delivery-report error with the topic partition it applied to (R4.9).

    Args:
        err: The ``KafkaError`` handed to the delivery callback.
        msg: The message the report is for. It carries the topic and partition even
            when delivery failed, though the partition is ``UNASSIGNED`` if the failure
            happened before one was chosen.

    Returns:
        A single line naming the error and, where known, its topic partition.
    """
    try:
        topic = msg.topic()  # type: ignore[attr-defined]
        partition = msg.partition()  # type: ignore[attr-defined]
    except (AttributeError, TypeError):
        return str(err)
    if topic is None or partition is None or partition < 0:
        return f"{err} (partition not yet assigned)"
    return f"{err} [{topic}-{partition}]"


class LifecycleEventProducer:
    """Publishes lifecycle events keyed by ``order_id``."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._producer = Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                # 004 D5: was hardcoded "all" from 001 until this feature made it a
                # lever. The default is unchanged, so every earlier run is reproducible.
                "acks": settings.producer_acks.value,
                # 005 D12/R5.21 — the producer's own retry path, which until now was
                # entirely librdkafka's defaults. It matters from this spec on because
                # min.insync.replicas gives the broker a reason to REFUSE a write that
                # acks=all alone would have accepted: NOT_ENOUGH_REPLICAS is retryable,
                # and how long we keep trying is now a decision rather than a default.
                "retries": settings.producer_retries,
                "retry.backoff.ms": settings.producer_retry_backoff_ms,
                # The binding one. It caps the TOTAL time including every retry, and is
                # librdkafka's name for what the Java client calls delivery.timeout.ms —
                # so `retries` alone cannot keep a message in flight past this.
                "message.timeout.ms": settings.producer_message_timeout_ms,
                # 008 R8.1 — the retry settings above are exactly what makes this
                # necessary. Without it, a retry after a lost acknowledgement writes the
                # record twice, and a retry while later batches are already in flight
                # writes them OUT OF ORDER. The second is the one that hurts here: this
                # domain is an ordered lifecycle, so a SHIPPED that overtakes a PACKED
                # makes every consumer report a violation that never happened.
                #
                # Off is what 001–007 ran as, and is kept reachable as the control (R8.2).
                "enable.idempotence": settings.producer_idempotence,
                # murmur2 on the key, as Java does: one order, one partition (R1.10).
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

    def topic_exists(self, topic: str | None = None, timeout: float = 5.0) -> bool:
        """Report whether a topic exists on the broker.

        Args:
            topic: Which topic to check. Defaults to the lifecycle topic.
            timeout: Seconds to wait for cluster metadata.

        Returns:
            ``True`` if the broker knows the topic.

        Raises:
            KafkaException: If cluster metadata could not be fetched at all — an
                unreachable broker is a different failure from a missing topic.
        """
        name = topic or self._settings.order_lifecycle_topic
        metadata = self._producer.list_topics(timeout=timeout)
        found = metadata.topics.get(name)
        return found is not None and found.error is None

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
                ``producer_delivery_wait_seconds``, which tracks the producer's
                own ``message.timeout.ms`` so the caller never gives up first.

        Returns:
            The partition and offset the broker assigned.

        Raises:
            DeliveryFailed: If the broker reported an error, or the topic is missing.
            DeliveryTimeout: If no delivery report arrived in time.
        """
        return self._publish_blocking(
            topic=self._settings.order_lifecycle_topic,
            key=event.order_id,
            value=event.model_dump_json().encode("utf-8"),
            what=f"{event.order_id} seq {event.sequence}",
            timeout=timeout,
        )

    def publish_snapshot(self, order_id: str, snapshot: dict[str, object]) -> None:
        """Publish an order's current state to the compacted topic, without waiting.

        Fire-and-forget on purpose (006 D3, R6.5). A snapshot is *derived* state: if this
        write is lost, ``order-lifecycle`` still holds the truth and the next event for
        the order rewrites it. Blocking here — or failing the caller's ``201`` over it —
        would trade the authoritative write for the derived one.

        The cost is named in the spec's known-gaps table rather than hidden: an order that
        has reached ``DELIVERED`` has no next event, so a snapshot lost on its last write
        stays stale until a tombstone or a replay corrects it.

        Args:
            order_id: The order this snapshot is for; also the compaction key.
            snapshot: The self-contained state from :meth:`Order.as_snapshot`.
        """

        def on_delivery(err: object, msg: object) -> None:
            if err is not None:
                logger.warning(
                    "snapshot delivery failed for %s: %s",
                    order_id,
                    _describe_delivery_error(err, msg),
                )

        try:
            self._produce_keyed(
                topic=self._settings.order_snapshot_topic,
                key=order_id,
                value=json.dumps(snapshot).encode("utf-8"),
                on_delivery=on_delivery,
            )
        except DeliveryFailed as exc:
            # Enqueueing failed outright — still not the caller's problem (R6.5).
            logger.warning("snapshot not enqueued for %s: %s", order_id, exc)

    def publish_tombstone(
        self, order_id: str, *, timeout: float | None = None
    ) -> DeliveryResult:
        """Publish a tombstone for one order and block until the broker acknowledges.

        The opposite choice from :meth:`publish_snapshot`, and deliberately so (006 D3):
        a ``204`` from the delete endpoint is a claim that the delete landed, and a claim
        the broker never confirmed would be a lie. So this one waits, and the route layer
        translates the two exceptions into ``502`` and ``504`` exactly as it already does
        for a lifecycle event.

        Args:
            order_id: The order to erase; the key the tombstone is written under.
            timeout: Seconds to wait for the delivery report. Defaults as
                :meth:`publish_and_wait` does.

        Returns:
            The partition and offset the tombstone landed on.

        Raises:
            DeliveryFailed: If the broker reported an error, or the topic is missing.
            DeliveryTimeout: If no delivery report arrived in time.
        """
        return self._publish_blocking(
            topic=self._settings.order_snapshot_topic,
            key=order_id,
            value=None,
            what=f"tombstone for {order_id}",
            timeout=timeout,
        )

    def _publish_blocking(
        self,
        *,
        topic: str,
        key: str,
        value: bytes | None,
        what: str,
        timeout: float | None = None,
    ) -> DeliveryResult:
        """Produce one keyed message and block until its delivery report arrives.

        Shared by :meth:`publish_and_wait` and :meth:`publish_tombstone` so the wait, the
        missing-topic diagnosis and the partition-naming error text exist once.

        Args:
            topic: Where the message goes.
            key: The message key.
            value: The serialised body, or ``None`` for a tombstone.
            what: How to name this message in a timeout error.
            timeout: Seconds to wait; defaults to ``producer_delivery_wait_seconds``.

        Returns:
            The partition and offset the broker assigned.

        Raises:
            DeliveryFailed: If the broker reported an error, or the topic is missing.
            DeliveryTimeout: If no delivery report arrived in time.
        """
        # 005 D12: derived from message.timeout.ms, NOT the 10s delivery_timeout_seconds
        # this used to read. Giving up before librdkafka does turns every slow delivery
        # into a ghost write and hides the NOT_ENOUGH_REPLICAS report R5.22 asks for.
        wait = (
            timeout
            if timeout is not None
            else self._settings.producer_delivery_wait_seconds
        )
        done = threading.Event()
        outcome: dict[str, object] = {}

        def on_delivery(err: object, msg: object) -> None:
            if err is not None:
                # R4.9: a bare str(err) on a degraded cluster says "Broker: Not enough
                # in-sync replicas" and nothing about WHICH partition refused, which is
                # the one fact a failover experiment needs (004 D7).
                outcome["error"] = _describe_delivery_error(err, msg)
            else:
                outcome["result"] = DeliveryResult(
                    partition=msg.partition(),  # type: ignore[attr-defined]
                    offset=msg.offset(),  # type: ignore[attr-defined]
                )
            done.set()

        self._produce_keyed(
            topic=topic, key=key, value=value, on_delivery=on_delivery
        )

        if not done.wait(wait):
            # A missing topic looks like a timeout, so ask metadata which it was (R1.11).
            try:
                topic_missing = not self.topic_exists(topic)
            except KafkaException:
                topic_missing = False
            if topic_missing:
                raise DeliveryFailed(
                    f"topic '{topic}' does not exist "
                    "and auto-creation is disabled — run scripts/create_topics.sh"
                )
            raise DeliveryTimeout(f"no delivery report for {what} within {wait}s")
        if "error" in outcome:
            raise DeliveryFailed(str(outcome["error"]))
        return outcome["result"]  # type: ignore[return-value]

    def _produce_keyed(
        self,
        *,
        topic: str,
        key: str,
        value: bytes | None,
        on_delivery: object,
    ) -> None:
        """Enqueue one keyed message, which may be a tombstone.

        ``value=None`` is what makes a message a tombstone (006 D3). Note that null is
        legal on *any* topic and every consumer will see it — what a compacted topic adds
        is the broker-side half: the key's older values are erased, and after
        ``delete.retention.ms`` so is the marker. The mirror rule is why ``key`` is
        ``str`` and not optional: a compacted topic rejects a null key outright, because
        there is nothing to compact by.

        Args:
            topic: Where the message goes.
            key: The partitioning and compaction key. Always ``order_id`` here, which is
                what co-partitions the two topics (006 D8).
            value: The serialised body, or ``None`` for a tombstone.
            on_delivery: Callback invoked by the poll thread with the broker's report.

        Raises:
            DeliveryFailed: If librdkafka refused to enqueue the message.
        """
        try:
            self._producer.produce(
                topic=topic,
                key=key.encode("utf-8"),
                value=value,
                on_delivery=on_delivery,  # type: ignore[arg-type]
            )
        except BufferError as exc:
            raise DeliveryFailed(f"producer queue is full: {exc}") from exc
        except KafkaException as exc:
            raise DeliveryFailed(str(exc)) from exc
