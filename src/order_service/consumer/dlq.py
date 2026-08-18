"""Where a failed message goes, and what it carries with it (005 D3, D7).

Two destinations behind one object. :meth:`FailureRouter.to_retry` sends a message that
may yet succeed to the retry topic with a due time; :meth:`FailureRouter.to_dead_letter`
sends one that will not to the terminal topic. Both publish the **original bytes** rather
than a re-serialised event, because a message that failed to *decode* has no event to
serialise — and because a replay should put back exactly what was produced.

Both block until the broker acknowledges. That is not politeness: the caller commits the
source offset immediately afterwards, and committing an offset for a message that was
never durably moved anywhere loses it silently (D3).

The headers are the whole value of the dead-letter topic. Without them it is a pile of
bytes nobody can act on; ``x-original-offset`` is what makes a replay provable rather
than approximate.
"""

import logging
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta

from confluent_kafka import KafkaException, Message, Producer

from order_service.config import Settings
from order_service.events import utc_now

logger = logging.getLogger(__name__)

#: Routing and provenance, on every message this module publishes (R5.13, D7).
HDR_SERVICE = "x-service"
HDR_CONSUMER_GROUP = "x-consumer-group"
HDR_ORIGINAL_TOPIC = "x-original-topic"
HDR_ORIGINAL_PARTITION = "x-original-partition"
HDR_ORIGINAL_OFFSET = "x-original-offset"
HDR_ORIGINAL_TIMESTAMP = "x-original-timestamp"
HDR_ATTEMPT = "x-attempt"
HDR_RETRY_AT = "x-retry-at"
HDR_ATTEMPTS_MADE = "x-attempts-made"
HDR_ERROR_CLASS = "x-error-class"
HDR_ERROR_MESSAGE = "x-error-message"
HDR_FAILED_AT = "x-failed-at"

#: Headers ride with every copy of the message, so a stack trace's worth of text would be
#: paid for on each hop. The class name plus the first line is what identifies a failure.
_MAX_ERROR_CHARS = 500


class FailurePublishFailed(RuntimeError):
    """The retry or dead-letter publication was not acknowledged (R5.6).

    Raised so the caller does **not** commit: the message is then redelivered from the
    source topic and tried again, which is at-least-once behaving exactly as before.
    """


def decode_headers(message: Message) -> dict[str, str]:
    """Return a message's headers as a string dict, ignoring undecodable ones.

    Args:
        message: The polled message.

    Returns:
        Header name to value. Empty if the message carries none.
    """
    decoded: dict[str, str] = {}
    for name, value in message.headers() or []:
        if value is None:
            continue
        try:
            decoded[name] = value.decode("utf-8")
        except UnicodeDecodeError:
            continue
    return decoded


def _message_timestamp(message: Message) -> str:
    """Return a message's broker timestamp as ISO-8601 UTC, or ``unknown``."""
    try:
        _kind, milliseconds = message.timestamp()
    except (AttributeError, TypeError):
        return "unknown"
    if not milliseconds or milliseconds < 0:
        return "unknown"
    return datetime.fromtimestamp(milliseconds / 1000, tz=utc_now().tzinfo).isoformat()


@dataclass(frozen=True)
class Origin:
    """Where a message was first published, carried across every hop (R5.13).

    A message on the retry topic is not where it came from, so the retry worker must read
    provenance out of the headers rather than off the message in its hand. That is the
    only reason this is a type and not four keyword arguments.
    """

    topic: str
    partition: int
    offset: int
    timestamp: str

    @classmethod
    def from_message(cls, message: Message) -> "Origin":
        """Build the origin of a message being failed for the first time."""
        return cls(
            topic=message.topic() or "unknown",
            partition=message.partition() if message.partition() is not None else -1,
            offset=message.offset() if message.offset() is not None else -1,
            timestamp=_message_timestamp(message),
        )

    @classmethod
    def from_headers(cls, headers: dict[str, str], message: Message) -> "Origin":
        """Recover the origin a previous hop recorded, falling back to the message.

        The fallback matters when something lands on the retry topic without this
        module's headers — a hand-produced message, say. It is then treated as
        originating there, which is wrong but bounded, rather than crashing the worker.
        """
        if HDR_ORIGINAL_TOPIC not in headers:
            return cls.from_message(message)
        return cls(
            topic=headers[HDR_ORIGINAL_TOPIC],
            partition=int(headers.get(HDR_ORIGINAL_PARTITION, -1)),
            offset=int(headers.get(HDR_ORIGINAL_OFFSET, -1)),
            timestamp=headers.get(HDR_ORIGINAL_TIMESTAMP, "unknown"),
        )


def describe_error(exc: BaseException) -> tuple[str, str]:
    """Return the exception's class name and a truncated message, for headers."""
    text = str(exc).replace("\n", " ").strip() or "<no message>"
    if len(text) > _MAX_ERROR_CHARS:
        text = f"{text[:_MAX_ERROR_CHARS]}…"
    return type(exc).__name__, text


class FailureRouter:
    """Publishes failed messages to the retry and dead-letter topics (D3, D7)."""

    def __init__(self, settings: Settings) -> None:
        """Build the router's producer.

        Args:
            settings: Resolved environment settings, for the topic names and brokers.
        """
        self._settings = settings
        self._producer = Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                # Hardcoded, unlike the order producer's PRODUCER_ACKS lever (004 D5).
                # A dead letter is the *record* that a message could not be processed;
                # publishing it at acks=0 could lose the evidence while the consumer
                # commits the offset regardless, which is the one outcome this whole
                # feature exists to prevent.
                "acks": "all",
                "partitioner": "consistent_random",
                "client.id": "order-service-failure-router",
            }
        )

    def close(self, flush_timeout: float = 10.0) -> None:
        """Flush anything still buffered."""
        remaining = self._producer.flush(flush_timeout)
        if remaining:
            logger.warning("failure router flush left %d message(s) unsent", remaining)

    def to_retry(
        self,
        message: Message,
        *,
        origin: Origin,
        service: str,
        group_id: str,
        attempt: int,
        error: BaseException,
    ) -> datetime:
        """Publish a message for a later attempt, and report when that attempt is due.

        Args:
            message: The message that failed; its key and value are republished as-is.
            origin: Where the message was first published.
            service: Which service's handler failed — the retry worker's routing key.
            group_id: The consumer group that failed.
            attempt: The 1-based attempt this publication schedules. The main consumer
                spends attempt 1 inline, so its first publication schedules attempt 2.
            error: What went wrong, for the headers.

        Returns:
            The time the scheduled attempt becomes due.

        Raises:
            FailurePublishFailed: If the broker did not acknowledge the publication.
        """
        backoff = self._settings.backoff_for_attempt(attempt)
        due_at = utc_now() + timedelta(seconds=backoff)
        error_class, error_message = describe_error(error)
        self._publish(
            topic=self._settings.retry_topic,
            message=message,
            headers={
                **self._provenance(origin, service, group_id),
                HDR_ATTEMPT: str(attempt),
                HDR_RETRY_AT: due_at.isoformat(),
                HDR_ERROR_CLASS: error_class,
                HDR_ERROR_MESSAGE: error_message,
                HDR_FAILED_AT: utc_now().isoformat(),
            },
        )
        return due_at

    def to_dead_letter(
        self,
        message: Message,
        *,
        origin: Origin,
        service: str,
        group_id: str,
        attempts_made: int,
        error: BaseException,
    ) -> None:
        """Publish a message nothing could process to the terminal topic (R5.12).

        Args:
            message: The message that failed; its key and value are republished as-is.
            origin: Where the message was first published.
            service: Which service gave up.
            group_id: Which consumer group gave up — the header a human reads first.
            attempts_made: How many attempts were actually spent. One for a poison
                message, ``retry_max_attempts`` for an exhausted one.
            error: The last thing that went wrong.

        Raises:
            FailurePublishFailed: If the broker did not acknowledge the publication.
        """
        error_class, error_message = describe_error(error)
        self._publish(
            topic=self._settings.dlq_topic,
            message=message,
            headers={
                **self._provenance(origin, service, group_id),
                HDR_ATTEMPTS_MADE: str(attempts_made),
                HDR_ERROR_CLASS: error_class,
                HDR_ERROR_MESSAGE: error_message,
                HDR_FAILED_AT: utc_now().isoformat(),
            },
        )

    @staticmethod
    def _provenance(origin: Origin, service: str, group_id: str) -> dict[str, str]:
        """Return the headers every hop carries, whatever its destination."""
        return {
            HDR_SERVICE: service,
            HDR_CONSUMER_GROUP: group_id,
            HDR_ORIGINAL_TOPIC: origin.topic,
            HDR_ORIGINAL_PARTITION: str(origin.partition),
            HDR_ORIGINAL_OFFSET: str(origin.offset),
            HDR_ORIGINAL_TIMESTAMP: origin.timestamp,
        }

    def _publish(
        self, *, topic: str, message: Message, headers: dict[str, str]
    ) -> None:
        """Publish one message and block until the broker acknowledges it.

        No background poll thread, unlike ``LifecycleEventProducer`` (001 D6): this is
        only ever called from a consume loop that is already blocked on the result, so
        ``flush()`` is both the wait and the callback pump.

        Raises:
            FailurePublishFailed: If the publication errored or was not acknowledged in
                time.
        """
        outcome: dict[str, object] = {}
        done = threading.Event()

        def on_delivery(err: object, _msg: object) -> None:
            if err is not None:
                outcome["error"] = str(err)
            done.set()

        encoded = [(name, value.encode("utf-8")) for name, value in headers.items()]
        try:
            self._producer.produce(
                topic=topic,
                key=message.key(),
                value=message.value(),
                headers=encoded,
                on_delivery=on_delivery,
            )
        except (BufferError, KafkaException) as exc:
            raise FailurePublishFailed(f"could not enqueue for {topic}: {exc}") from exc

        remaining = self._producer.flush(self._settings.delivery_timeout_seconds)
        if remaining or not done.is_set():
            raise FailurePublishFailed(
                f"no delivery report from {topic} within "
                f"{self._settings.delivery_timeout_seconds}s"
            )
        if "error" in outcome:
            raise FailurePublishFailed(f"{topic} rejected the message: {outcome['error']}")
