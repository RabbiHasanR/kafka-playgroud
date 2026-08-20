"""The process that does the waiting, so the service consumers never have to (005 D1, D4).

One container, one consumer group, one topic. Every message on the retry topic carries an
``x-service`` header naming the service whose handler failed, so routing is a dict lookup
against the same :data:`~order_service.consumer.main.SERVICE_REGISTRY` the three service
consumers are built from — no second copy of what a service is.

**Why this is a separate process.** Waiting out a backoff is the one thing the main
consumers must never do: a partition is read in order, so a consumer that waits holds up
every message behind it. Moving the wait into its own process is what makes the retry
non-blocking; nothing this loop does can reach the three consume loops.

**Why waiting is not sleeping.** ``time.sleep()`` past ``max.poll.interval.ms`` gets the
member evicted, the partition reassigned, and the message redelivered — the failure 002's
``handler_delay_seconds`` lever exists to demonstrate. So a message that is not yet due
has its partition **paused** and its offset **sought back to**, and the loop keeps calling
``poll()``, which returns nothing for a paused partition and everything for the others.

**What it stopped doing at 007.** It used to run the handler and fold the result into
that service's state itself, which a shared database allowed. An embedded store does not:
only the process holding a partition can write that partition's fold (R7.12). So the
worker is now purely a **scheduler** — it does the waiting, and when a message is due it
republishes it to the topic it came from, where the instance that owns the partition runs
the handler and folds through the ordinary path. Everything downstream of that (a further
failure, the attempt budget, the dead-letter route) is the consumer's existing logic, not
a second copy here.

**Known gap (D14).** One topic carrying per-message delays can invert: a message due in
120s sits at the head of a partition and holds up messages behind it that are due in 30s.
Tiered delay topics are the fix and are deliberately not built, so the stall is watchable.
The topic's three partitions are three independent lanes, which softens it.
"""

import logging
import signal
import sys
from datetime import datetime
from types import FrameType

from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Message,
    TopicPartition,
)

from order_service.config import Settings, get_settings
from order_service.consumer.dlq import (
    HDR_ATTEMPT,
    HDR_RETRY_AT,
    HDR_SERVICE,
    FailurePublishFailed,
    FailureRouter,
    Origin,
    decode_headers,
)
from order_service.consumer.errors import NonRetryableError
from order_service.consumer.main import SERVICE_REGISTRY
from order_service.consumer.runtime import (
    ConsumerConfigError,
    ServiceSpec,
    build_consumer_config,
    key_of,
)
from order_service.events import LifecycleEvent, utc_now

logger = logging.getLogger("order_service.retry_worker")

#: One group for the whole worker. Offsets are tracked per topic-partition regardless, and
#: a group per service would mean a consumer per service — which is D1's rejected shape.
RETRY_WORKER_GROUP = "retry-worker"


class RetryWorker:
    """Runs later attempts on behalf of whichever service asked for them."""

    def __init__(
        self,
        settings: Settings,
        specs: dict[str, ServiceSpec],
        router: FailureRouter,
    ) -> None:
        """Build the worker.

        Args:
            settings: Resolved environment settings.
            specs: Every service this worker can route to, by name. Still needed even
                though handlers no longer run here (R7.12): a message naming no known
                service is a poison message this worker must recognise.
            router: Where a message goes when it is due, and where it goes when nothing
                can be done with it.

        Raises:
            ConsumerConfigError: If the settings are incompatible with the selected group
                protocol (R2.21).
        """
        self._settings = settings
        self._specs = specs
        self._router = router
        self._running = False
        #: Partitions paused until their head message is due, by (topic, partition).
        self._deferred: dict[tuple[str, int], datetime] = {}
        self._consumer = Consumer(
            build_consumer_config(
                settings,
                group_id=settings.consumer_group_id or RETRY_WORKER_GROUP,
                client_id=f"retry-worker-{settings.instance_label}",
            )
        )

    def stop(self) -> None:
        """Ask the loop to exit after the current iteration."""
        self._running = False

    def run(self) -> None:
        """Subscribe to the retry topic and process due messages until stopped.

        The dead-letter topic is **not** subscribed to, here or anywhere. That absence is
        what makes it terminal (R5.15, D10).

        Raises:
            KafkaException: If the broker reports a fatal error.
        """
        topic = self._settings.retry_topic
        self._consumer.subscribe([topic], on_revoke=self._on_revoke, on_lost=self._on_revoke)
        self._running = True
        logger.info(
            "[retry-worker] consuming topic=%s group=%s services=%s",
            topic,
            self._settings.consumer_group_id or RETRY_WORKER_GROUP,
            ",".join(sorted(self._specs)),
        )
        logger.info(
            "[retry-worker] max_attempts=%d backoff_seconds=%s dlq=%s",
            self._settings.retry_max_attempts,
            self._settings.retry_backoff_schedule,
            self._settings.dlq_topic,
        )

        try:
            while self._running:
                self._resume_due()
                message = self._consumer.poll(1.0)
                if message is None:
                    continue
                if message.error():
                    self._handle_error(message)
                    continue
                self._handle_message(message)
        finally:
            self._consumer.close()
            logger.info("[retry-worker] consumer closed")

    # -- the due-time gate (D4) ------------------------------------------------------

    def _resume_due(self) -> None:
        """Resume every partition whose head message has become due."""
        now = utc_now()
        for key, due_at in list(self._deferred.items()):
            if due_at > now:
                continue
            del self._deferred[key]
            topic, partition = key
            try:
                self._consumer.resume([TopicPartition(topic, partition)])
            except KafkaException as exc:
                logger.warning("[retry-worker] could not resume %s: %s", key, exc)

    def _defer(self, message: Message, due_at: datetime) -> None:
        """Pause this partition and rewind, so the message is re-read when it is due.

        The rewind is load-bearing. Pausing alone stops the fetch, but the client's read
        position has already moved past this message — on resume it would be skipped, and
        the retry would be silently dropped rather than delayed.

        Pause happens **before** the seek: seeking a partition that is actively fetching
        races with the fetcher, and a paused partition has no fetcher to race with.
        """
        topic, partition = message.topic(), message.partition()
        tp = TopicPartition(topic, partition, message.offset())
        try:
            self._consumer.pause([TopicPartition(topic, partition)])
            self._consumer.seek(tp)
        except KafkaException as exc:
            logger.warning(
                "[retry-worker] could not defer %s-%d@%d: %s",
                topic,
                partition,
                message.offset(),
                exc,
            )
            return
        self._deferred[(topic, partition)] = due_at
        logger.info(
            "[retry-worker] RETRY_WAITING key=%s partition=%d offset=%d due=%s",
            key_of(message),
            partition,
            message.offset(),
            due_at.isoformat(),
        )

    def _on_revoke(self, _consumer: Consumer, partitions: list[TopicPartition]) -> None:
        """Forget deferrals for partitions this member no longer owns."""
        for tp in partitions:
            self._deferred.pop((tp.topic, tp.partition), None)

    # -- processing ------------------------------------------------------------------

    def _handle_error(self, message: Message) -> None:
        """Log a broker-reported error, raising only on a fatal one.

        Raises:
            KafkaException: If the error is fatal.
        """
        error = message.error()
        if error is not None and error.code() == KafkaError._PARTITION_EOF:
            return
        if error is not None and error.fatal():
            raise KafkaException(error)
        logger.error("[retry-worker] consume error: %s", error)

    def _handle_message(self, message: Message) -> None:
        """Run one later attempt, or defer it until it is due."""
        headers = decode_headers(message)
        origin = Origin.from_headers(headers, message)
        attempt = _int_header(headers, HDR_ATTEMPT, default=2)
        service = headers.get(HDR_SERVICE, "")

        due_at = _time_header(headers, HDR_RETRY_AT)
        if due_at is not None and due_at > utc_now():
            self._defer(message, due_at)
            return

        spec = self._specs.get(service)
        if spec is None:
            # Not a routing failure we can fix by trying again: without a service name
            # there is no handler to run, on this attempt or any other.
            self._give_up(
                message,
                origin=origin,
                service=service or "<unset>",
                attempt=attempt,
                exc=NonRetryableError(
                    f"x-service header {service!r} names no registered service "
                    f"(known: {', '.join(sorted(self._specs))})"
                ),
                marker="POISON_MESSAGE",
            )
            return

        try:
            event = LifecycleEvent.model_validate_json(message.value())
        except (ValueError, TypeError) as exc:
            self._give_up(
                message,
                origin=origin,
                service=service,
                attempt=attempt,
                exc=NonRetryableError(f"undecodable message: {exc}"),
                marker="POISON_MESSAGE",
            )
            return

        self._release(message, spec, event, origin, attempt)

    def _release(
        self,
        message: Message,
        spec: ServiceSpec,
        event: LifecycleEvent,
        origin: Origin,
        attempt: int,
    ) -> None:
        """Put a due message back where the partition's owner will read it (R7.12, R7.13).

        The handler is deliberately **not** run here. Running it here and republishing
        would do the work twice; running it here and folding locally is what 007 made
        impossible. So this loop's whole contribution is the waiting.

        A failed republish is not committed, so the message is redelivered and the wait is
        served again — at-least-once, exactly as a failed retry publication already
        behaved (R5.6).
        """
        try:
            topic = self._router.to_source(
                message,
                origin=origin,
                service=spec.name,
                group_id=self._settings.group_id_for(spec.name),
                attempt=attempt,
            )
        except FailurePublishFailed as exc:
            logger.error(
                "[retry-worker] RETRY_RELEASE_FAILED service=%s order_id=%s attempt=%d "
                "reason=%s — not committing, will be redelivered",
                spec.name,
                event.order_id,
                attempt,
                exc,
            )
            return

        logger.warning(
            "[retry-worker] RETRY_RELEASED service=%s order_id=%s seq=%d attempt=%d "
            "topic=%s",
            spec.name,
            event.order_id,
            event.sequence,
            attempt,
            topic,
        )
        self._commit(message)

    def _give_up(
        self,
        message: Message,
        *,
        origin: Origin,
        service: str,
        attempt: int,
        exc: BaseException,
        marker: str,
    ) -> None:
        """Publish to the dead-letter topic and commit (R5.12, R5.14, R5.16)."""
        logger.warning(
            "[retry-worker] %s service=%s key=%s attempts=%d origin=%s-%d@%d error=%s: %s",
            marker,
            service,
            key_of(message),
            attempt,
            origin.topic,
            origin.partition,
            origin.offset,
            type(exc).__name__,
            exc,
        )
        try:
            self._router.to_dead_letter(
                message,
                origin=origin,
                service=service,
                group_id=self._settings.group_id_for(service),
                attempts_made=attempt,
                error=exc,
            )
        except FailurePublishFailed as publish_error:
            self._rewind(message, publish_error)
            return

        logger.warning(
            "[retry-worker] DLQ_PUBLISHED service=%s key=%s topic=%s",
            service,
            key_of(message),
            self._settings.dlq_topic,
        )
        self._commit(message)

    def _rewind(self, message: Message, exc: BaseException) -> None:
        """Nothing was moved, so nothing may be committed — re-read instead (D3)."""
        logger.error(
            "[retry-worker] FAILURE_PUBLISH_FAILED key=%s offset=%s — not committing: %s",
            key_of(message),
            message.offset(),
            exc,
        )
        try:
            self._consumer.seek(
                TopicPartition(message.topic(), message.partition(), message.offset())
            )
        except KafkaException as seek_error:
            logger.error("[retry-worker] could not rewind: %s", seek_error)

    def _commit(self, message: Message) -> None:
        """Commit one offset, surviving the loss of the partition it belongs to."""
        try:
            self._consumer.commit(message=message, asynchronous=False)
        except KafkaException as exc:
            logger.warning(
                "[retry-worker] COMMIT_REJECTED partition=%d offset=%d reason=%s",
                message.partition(),
                message.offset(),
                exc.args[0] if exc.args else exc,
            )


def _int_header(headers: dict[str, str], name: str, *, default: int) -> int:
    """Return an integer header, falling back when it is missing or malformed."""
    try:
        return int(headers[name])
    except (KeyError, ValueError):
        return default


def _time_header(headers: dict[str, str], name: str) -> datetime | None:
    """Return an ISO-8601 header as a datetime, or ``None`` if absent or malformed.

    A malformed due time reads as "due now" rather than as an error: running the attempt
    early is a smaller mistake than stalling the message forever.
    """
    try:
        return datetime.fromisoformat(headers[name])
    except (KeyError, ValueError):
        return None


def main() -> None:
    """Run the retry worker until interrupted."""
    settings = get_settings()

    specs = {name: factory() for name, factory in SERVICE_REGISTRY.items()}

    # No state stores here since 007. The worker holds no partition, so it may write no
    # fold (R7.12); it republishes and the owning consumer does both.
    router = FailureRouter(settings)
    try:
        worker = RetryWorker(settings, specs, router)
    except ConsumerConfigError as exc:
        logger.error("[retry-worker] %s", exc)
        router.close()
        sys.exit(2)

    def shutdown(signum: int, _frame: FrameType | None) -> None:
        logger.info("[retry-worker] signal %d received, shutting down", signum)
        worker.stop()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        worker.run()
    except KafkaException as exc:
        logger.error("[retry-worker] fatal kafka error: %s", exc)
        sys.exit(1)
    finally:
        router.close()


if __name__ == "__main__":
    main()
