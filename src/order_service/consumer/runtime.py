"""The one consume loop, shared by all three services.

A service is data, not code: a :class:`ServiceSpec` carrying a name, a consumer group,
and a ``dict[EventType, Handler]``. ``SERVICE_NAME`` picks one at startup, so all three
run from one image and one entry point (D8, R2.37). Three near-identical poll loops is
the obvious wrong turn — the third copy is where the offset-commit bug gets fixed in
two places and not the third.

**Each service is its own consumer group, and that is the whole lesson (D7).** Kafka
tracks an offset per group, so all three read every message and none of them consumes
it away from the others. Stopping one cannot affect the others because their offsets
were never shared. Spec 003 adds a second consumer to *one* group, where the messages
divide instead of duplicating — meeting fan-out first is what makes that a contrast.

**Offsets are committed after the handler returns, never before (R2.32).** That makes
this at-least-once: a crash between handling an event and committing its offset means
the event is redelivered and rehandled. Not solved here — deduplication on ``event_id``
is 004 and 009.

**The fold is not persisted.** Each service keeps its own per-order
``(last_sequence, state)`` in memory and loses it on restart, which is 001's R1.27
lesson arriving from the other side: the offset comes back, the memory does not, and
the sequence-gap violation that follows is indistinguishable from a real one.
"""

import json
import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from enum import StrEnum

from confluent_kafka import Consumer, KafkaError, KafkaException, Message

from order_service.config import Settings
from order_service.events import (
    EXPECTED_NEXT_EVENT,
    RESULTING_STATE,
    EventType,
    LifecycleEvent,
    OrderState,
    is_legal_transition,
)

logger = logging.getLogger(__name__)

#: What a service does with one event. Handlers log; they do not return anything, and
#: raising is not part of the contract at this spec — poison-message handling is 006.
Handler = Callable[[LifecycleEvent], None]


@dataclass(frozen=True)
class ServiceSpec:
    """One consumer service (R2.28, R2.37).

    Attributes:
        name: Short service name, used in logs and to derive the group id.
        handlers: Which event types this service reacts to, and how. Types absent
            from the map are skipped without error — that is what "ignores the other
            events" means mechanically (R2.33).
    """

    name: str
    handlers: Mapping[EventType, Handler]


class ViolationType(StrEnum):
    """The kinds of ordering violation a service can detect."""

    SEQUENCE_GAP = "SEQUENCE_GAP"
    ILLEGAL_TRANSITION = "ILLEGAL_TRANSITION"


@dataclass(frozen=True)
class Violation:
    """A single detected violation.

    Attributes:
        type: Which check failed.
        order_id: The order the failing event belongs to.
        sequence: Sequence of the failing event.
        expected: What the service expected to see.
        observed: What it actually saw.
    """

    type: ViolationType
    order_id: str
    sequence: int
    expected: str
    observed: str

    def as_log_fields(self) -> str:
        """Render the violation as a single log line body.

        Returns:
            A stable, greppable representation.
        """
        return (
            f"VIOLATION type={self.type} order_id={self.order_id} "
            f"seq={self.sequence} expected={self.expected} observed={self.observed}"
        )


@dataclass(frozen=True)
class OrderFold:
    """What a service has accumulated about one order.

    Attributes:
        order_id: The order this describes.
        last_sequence: Sequence of the most recent event applied.
        state: Lifecycle state after that event.
    """

    order_id: str
    last_sequence: int = 0
    state: OrderState | None = None


def apply_event(
    current: OrderFold | None, event: LifecycleEvent
) -> tuple[OrderFold, list[Violation]]:
    """Fold one event into a service's view of an order, reporting violations.

    A pure function of ``(fold, event) -> (fold, violations)``, so it is testable
    without a broker and can be re-hosted on a durable store in 004 by changing only
    where the state comes from and goes to.

    The event is applied even when it violates an expectation, so consumption continues
    (R2.40) and one bad event does not poison every later one.

    Args:
        current: The service's existing fold for this order, or ``None`` if unseen.
        event: The event to apply.

    Returns:
        The updated fold and the violations this event triggered.
    """
    fold = current if current is not None else OrderFold(order_id=event.order_id)
    violations: list[Violation] = []

    # -- sequence contiguity (R2.38) -------------------------------------------
    # For an unseen order last_sequence is 0, so the expected sequence is 1 and
    # anything else is a gap. This is the same code path that fires after a restart,
    # which is precisely why the two are indistinguishable.
    expected_sequence = fold.last_sequence + 1
    if event.sequence != expected_sequence:
        violations.append(
            Violation(
                type=ViolationType.SEQUENCE_GAP,
                order_id=event.order_id,
                sequence=event.sequence,
                expected=str(expected_sequence),
                observed=str(event.sequence),
            )
        )

    # -- lifecycle legality (R2.39) --------------------------------------------
    if not is_legal_transition(event.event_type, fold.state):
        expected_event = EXPECTED_NEXT_EVENT.get(fold.state)
        violations.append(
            Violation(
                type=ViolationType.ILLEGAL_TRANSITION,
                order_id=event.order_id,
                sequence=event.sequence,
                expected=(
                    str(expected_event)
                    if expected_event is not None
                    else "nothing (lifecycle complete)"
                ),
                observed=f"{event.event_type} after {fold.state}",
            )
        )

    updated = replace(
        fold,
        last_sequence=event.sequence,
        state=RESULTING_STATE[event.event_type],
    )
    return updated, violations


class ServiceConsumer:
    """Runs one service against the lifecycle topic."""

    def __init__(self, spec: ServiceSpec, settings: Settings) -> None:
        """Initialise the consumer.

        Args:
            spec: The service to run.
            settings: Resolved runtime settings.
        """
        self._spec = spec
        self._settings = settings
        self._group_id = settings.group_id_for(spec.name)
        self._folds: dict[str, OrderFold] = {}
        self._running = False
        self._consumer = Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                # Its own group — this is what makes it fan-out rather than
                # scale-out (D7).
                "group.id": self._group_id,
                # Offsets are committed by hand, after the handler runs (R2.32).
                "enable.auto.commit": False,
                # A group with no committed offsets starts at the earliest retained
                # message, so a fresh group id replays the whole topic.
                "auto.offset.reset": "earliest",
                "client.id": f"order-service-{spec.name}",
            }
        )

    @property
    def group_id(self) -> str:
        """Return the consumer group this service joined.

        Returns:
            The resolved group id.
        """
        return self._group_id

    def stop(self) -> None:
        """Ask the consume loop to exit after the current iteration."""
        self._running = False

    def run(self) -> None:
        """Subscribe and consume until stopped.

        Raises:
            KafkaException: If the broker reports a fatal error.
        """
        topic = self._settings.order_lifecycle_topic
        self._consumer.subscribe([topic])
        self._running = True
        logger.info(
            "[%s] consuming topic=%s group=%s brokers=%s handling=%s",
            self._spec.name,
            topic,
            self._group_id,
            self._settings.kafka_bootstrap_servers,
            ",".join(sorted(str(t) for t in self._spec.handlers)) or "<nothing>",
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
            logger.info("[%s] consumer closed", self._spec.name)

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
        logger.error("[%s] consume error: %s", self._spec.name, error)

    def _handle_message(self, message: Message) -> None:
        """Decode, detect, dispatch, and commit one message.

        A message that cannot be parsed is logged and its offset committed anyway —
        retry and dead-letter handling is spec 006, and stalling here would block the
        partition for this service.

        Args:
            message: The message to process.
        """
        raw = message.value()
        key = message.key()
        try:
            event = LifecycleEvent.model_validate(json.loads(raw.decode("utf-8")))
        except (ValueError, UnicodeDecodeError) as exc:
            logger.error(
                "[%s] undecodable message at %s[%d]@%d: %s",
                self._spec.name,
                message.topic(),
                message.partition(),
                message.offset(),
                exc,
            )
            self._consumer.commit(message=message, asynchronous=False)
            return

        # R2.42 — the service name comes first so three interleaved log streams stay
        # readable, and partition/offset/key make the routing visible without opening
        # Kafka UI.
        logger.info(
            "[%s] partition=%d offset=%d key=%s order_id=%s seq=%d type=%s",
            self._spec.name,
            message.partition(),
            message.offset(),
            key.decode("utf-8") if key is not None else "<null>",
            event.order_id,
            event.sequence,
            event.event_type,
        )

        updated, violations = apply_event(self._folds.get(event.order_id), event)
        self._folds[event.order_id] = updated
        for violation in violations:
            # R2.41 — WARNING and a stable marker, so `grep VIOLATION` across all
            # three services is the whole filtering story.
            logger.warning(
                "[%s] %s partition=%d offset=%d",
                self._spec.name,
                violation.as_log_fields(),
                message.partition(),
                message.offset(),
            )

        handler = self._spec.handlers.get(event.event_type)
        if handler is not None:
            handler(event)

        # R2.32 — only now, after the handler has run.
        self._consumer.commit(message=message, asynchronous=False)
