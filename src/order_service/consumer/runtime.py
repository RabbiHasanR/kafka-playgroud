"""The one consume loop, shared by all three services.

A service is data, not code: a :class:`ServiceSpec` carrying a name and a
``dict[EventType, Handler]``. ``SERVICE_NAME`` picks one at startup, so all three run
from one image and one entry point (D8, R1.37).

Each service is its own consumer group (D7). Kafka tracks an offset per group, so all
three read every message and stopping one cannot affect the others.

Offsets commit after the handler returns (R1.32), so delivery is at-least-once. Where
the fold *lives* is 003's subject: a :class:`~order_service.consumer.state.StateStore` is
injected, so the same loop runs against 002's in-process dict or against Postgres, and
the difference is one environment variable (003 D2, D8).

With the durable store the two writes go to two systems — the fold to Postgres, the
offset to Kafka — and nothing covers both. The order between them is deliberate and
switchable (003 D4), and the window between them can be opened on purpose (003 D5).
"""

import json
import logging
import os
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from enum import StrEnum

from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Message,
    TopicPartition,
)

from order_service.config import (
    GroupProtocol,
    Settings,
    StateCrashPoint,
    StateWriteOrder,
)
from order_service.consumer.state import OrderFold, StateStore, StateStoreUnavailable
from order_service.events import (
    EXPECTED_NEXT_EVENT,
    RESULTING_STATE,
    EventType,
    LifecycleEvent,
    OrderState,
    is_legal_transition,
)

logger = logging.getLogger(__name__)

#: Raising is not part of the handler contract at this spec.
Handler = Callable[[LifecycleEvent], None]

#: What each protocol uses when its setting is left unset; shown in the banner (R2.22).
DEFAULT_ASSIGNOR = {
    GroupProtocol.CLASSIC: "range (client default)",
    GroupProtocol.CONSUMER: "uniform (broker default)",
}


class ConsumerConfigError(ValueError):
    """A consumer configuration the selected group protocol cannot accept (R2.21)."""


def validate_protocol_settings(settings: Settings) -> None:
    """Reject settings the selected group protocol does not accept (R2.21).

    librdkafka guards only one of the two directions. It raises ``_INVALID_ARG`` for a
    classic-only setting under ``group.protocol=consumer``, but **silently accepts and
    ignores** ``group.remote.assignor`` under ``classic`` — so a run configured with an
    assignor that never took effect would start cleanly and produce an observation about
    something that was not happening. In a repository whose output is observations, that
    is the worst available failure, so both directions are checked here (D4).

    Raises:
        ConsumerConfigError: If a setting is incompatible with the selected protocol.
    """
    protocol = settings.consumer_group_protocol

    # Each entry pairs the offending value with the remedy for that setting.
    if protocol is GroupProtocol.CONSUMER:
        incompatible = {
            "CONSUMER_ASSIGNMENT_STRATEGY": (
                settings.consumer_assignment_strategy,
                "use CONSUMER_REMOTE_ASSIGNOR",
            ),
            "CONSUMER_SESSION_TIMEOUT_MS": (
                settings.consumer_session_timeout_ms,
                "the session timeout is broker-side under this protocol",
            ),
        }
    else:
        incompatible = {
            "CONSUMER_REMOTE_ASSIGNOR": (
                settings.consumer_remote_assignor,
                "use CONSUMER_ASSIGNMENT_STRATEGY",
            ),
        }

    problems = [
        f"{name} cannot be used with CONSUMER_GROUP_PROTOCOL={protocol} — {remedy}"
        for name, (value, remedy) in sorted(incompatible.items())
        if value is not None
    ]
    if problems:
        raise ConsumerConfigError("; ".join(problems))


def assignor_in_effect(settings: Settings) -> str:
    """Return the assignor actually in force, for the startup banner (R2.22)."""
    if settings.consumer_group_protocol is GroupProtocol.CLASSIC:
        chosen = settings.consumer_assignment_strategy
    else:
        chosen = settings.consumer_remote_assignor
    return chosen or DEFAULT_ASSIGNOR[settings.consumer_group_protocol]


def build_consumer_config(
    settings: Settings, group_id: str, client_id: str
) -> dict[str, object]:
    """Assemble the librdkafka config for the selected group protocol (D4).

    The two protocols take partly disjoint settings, so this is a branch rather than a
    dict of values: ``partition.assignment.strategy`` and ``session.timeout.ms`` are
    rejected under KIP-848, and ``group.remote.assignor`` does nothing under classic.
    Anything left unset is omitted entirely so the client or broker default applies.

    Raises:
        ConsumerConfigError: If the settings are incompatible with the protocol.
    """
    validate_protocol_settings(settings)
    protocol = settings.consumer_group_protocol

    config: dict[str, object] = {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        # One group per service (D7); 002's notification instances share one.
        "group.id": group_id,
        # Committed by hand, after the handler runs (R1.32).
        "enable.auto.commit": False,
        # No committed offsets means a fresh group id replays the whole topic.
        "auto.offset.reset": "earliest",
        "client.id": client_id,
        "group.protocol": protocol.value,
    }

    if protocol is GroupProtocol.CLASSIC:
        strategy = settings.consumer_assignment_strategy
        if strategy is not None:
            config["partition.assignment.strategy"] = strategy
        if settings.consumer_session_timeout_ms is not None:
            config["session.timeout.ms"] = settings.consumer_session_timeout_ms
    elif settings.consumer_remote_assignor is not None:
        config["group.remote.assignor"] = settings.consumer_remote_assignor

    # Half the eviction lever (D9); under classic the session timeout drops with it.
    if settings.consumer_max_poll_interval_ms is not None:
        config["max.poll.interval.ms"] = settings.consumer_max_poll_interval_ms

    # Static membership (D10), off unless an id is set. Accepted by both protocols.
    if settings.consumer_instance_id_static is not None:
        config["group.instance.id"] = settings.consumer_instance_id_static

    return config


@dataclass(frozen=True)
class ServiceSpec:
    """One consumer service (R1.28, R1.37).

    Event types absent from ``handlers`` are skipped without error (R1.33).
    """

    name: str
    handlers: Mapping[EventType, Handler]


class ViolationType(StrEnum):
    """The kinds of ordering violation a service can detect."""

    SEQUENCE_GAP = "SEQUENCE_GAP"
    ILLEGAL_TRANSITION = "ILLEGAL_TRANSITION"


@dataclass(frozen=True)
class Violation:
    """A single detected violation."""

    type: ViolationType
    order_id: str
    sequence: int
    expected: str
    observed: str

    def as_log_fields(self) -> str:
        """Render the violation as a stable, greppable log line body."""
        return (
            f"VIOLATION type={self.type} order_id={self.order_id} "
            f"seq={self.sequence} expected={self.expected} observed={self.observed}"
        )


def apply_event(
    current: OrderFold | None, event: LifecycleEvent
) -> tuple[OrderFold, list[Violation]]:
    """Fold one event into a service's view of an order, reporting violations.

    The event is applied even when it violates an expectation, so consumption
    continues (R1.40).

    Returns:
        The updated fold and the violations this event triggered.
    """
    fold = current if current is not None else OrderFold(order_id=event.order_id)
    violations: list[Violation] = []

    # -- sequence contiguity (R1.38) -------------------------------------------
    # An unseen order has last_sequence 0, so anything but 1 is a gap.
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

    # -- lifecycle legality (R1.39) --------------------------------------------
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

    def __init__(
        self, spec: ServiceSpec, settings: Settings, store: StateStore
    ) -> None:
        """Build a consumer for one service.

        Args:
            spec: The service to run.
            settings: Resolved environment settings.
            store: Where folded state lives (003 D2). Injected rather than constructed
                here so ``main.py`` can fail on an unreachable database *before* this
                consumer exists, and so the backend is one substitution rather than a
                branch inside the loop.

        Raises:
            ConsumerConfigError: If the settings are incompatible with the selected
                group protocol (R2.21) — raised before the client is constructed, so
                the process never joins the group.
        """
        self._spec = spec
        self._settings = settings
        self._group_id = settings.group_id_for(spec.name)
        self._instance = settings.instance_label
        self._store = store
        self._running = False
        self._consumer = Consumer(
            build_consumer_config(
                settings,
                group_id=self._group_id,
                client_id=f"order-service-{spec.name}-{self._instance}",
            )
        )

    @property
    def group_id(self) -> str:
        """Return the consumer group this service joined."""
        return self._group_id

    @property
    def instance(self) -> str:
        """Return this process's identity within its group (R2.7)."""
        return self._instance

    def stop(self) -> None:
        """Ask the consume loop to exit after the current iteration."""
        self._running = False

    def run(self) -> None:
        """Subscribe and consume until stopped.

        Raises:
            KafkaException: If the broker reports a fatal error.
        """
        topic = self._settings.order_lifecycle_topic
        # D5 — the callbacks only log and manage folds; the client assigns (R2.16).
        self._consumer.subscribe(
            [topic],
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
            on_lost=self._on_lost,
        )
        self._running = True
        # R2.22 — protocol and assignor in the banner, so logs are self-describing.
        logger.info(
            "[%s/%s] consuming topic=%s group=%s brokers=%s handling=%s",
            self._spec.name,
            self._instance,
            topic,
            self._group_id,
            self._settings.kafka_bootstrap_servers,
            ",".join(sorted(str(t) for t in self._spec.handlers)) or "<nothing>",
        )
        logger.info(
            "[%s/%s] protocol=%s assignor=%s static_member=%s max_poll_interval_ms=%s",
            self._spec.name,
            self._instance,
            self._settings.consumer_group_protocol,
            assignor_in_effect(self._settings),
            self._settings.consumer_instance_id_static or "<dynamic>",
            self._settings.consumer_max_poll_interval_ms or "<client default>",
        )
        # R3.23 — which backend produced this run's observations (003 D8).
        logger.info(
            "[%s/%s] state_backend=%s write_order=%s crash_after=%s",
            self._spec.name,
            self._instance,
            self._settings.state_backend,
            self._settings.state_write_order,
            self._settings.state_crash_after,
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
        except StateStoreUnavailable as exc:
            # R3.22 — unlike a rejected commit, a dead state store must stop the loop.
            logger.error(
                "[%s/%s] STATE_STORE_UNAVAILABLE backend=%s reason=%s",
                self._spec.name,
                self._instance,
                self._settings.state_backend,
                exc,
            )
            raise
        finally:
            self._consumer.close()
            logger.info("[%s] consumer closed", self._spec.name)

    # -- rebalance callbacks (D5) ---------------------------------------------------
    # WARNING, not INFO: R2.9 wants these to stand out from per-record lines.

    def _on_assign(self, _consumer: Consumer, partitions: list[TopicPartition]) -> None:
        """Log partitions gained. Does not assign — the client does (D5)."""
        self._log_membership("ASSIGNED", partitions)

    def _on_revoke(self, _consumer: Consumer, partitions: list[TopicPartition]) -> None:
        """Log partitions given up and release exactly their state (R2.14, R3.9)."""
        self._log_membership("REVOKED", partitions)
        self._forget(partitions)

    def _on_lost(self, _consumer: Consumer, partitions: list[TopicPartition]) -> None:
        """Log partitions lost to an eviction and release exactly their state.

        Separate from ``_on_revoke`` because these partitions may already belong to
        another member, so committing against them fails — which is what makes the
        eviction experiment readable.

        This is also where 003's sequence guard earns its keep: an evicted member can
        finish a handler and write *after* its replacement has moved the order on. That
        write loses to the guard rather than corrupting the new owner's state (003 D3).
        """
        self._log_membership("LOST", partitions)
        self._forget(partitions)

    def _log_membership(self, event: str, partitions: list[TopicPartition]) -> None:
        """Log one membership change with a stable marker (R2.9, R2.10)."""
        changed = sorted(tp.partition for tp in partitions)
        logger.warning(
            "[%s/%s] REBALANCE %s partitions=%s held=%s",
            self._spec.name,
            self._instance,
            event,
            changed or "[]",
            self._store.held() or "[]",
        )

    def _forget(self, partitions: list[TopicPartition]) -> None:
        """Release this member's in-process state for exactly these partitions.

        **What this means now depends on the backend, and that is the whole feature.**

        On the memory backend it is 002 unchanged: the folds are gone, so whoever
        receives these partitions next starts with no memory of their orders and reports
        R2.15's sequence-gap violations.

        On the durable backend the same call drops a *cache* and issues no ``DELETE``
        (R3.9). The record survives because the record was never in the process, so the
        member that inherits the partition reads it and reports nothing (R3.8).
        """
        self._store.forget(tp.partition for tp in partitions)

    def _handle_error(self, message: Message) -> None:
        """Log a broker-reported error on a polled message.

        Raises:
            KafkaException: If the error is fatal.
        """
        error = message.error()
        if error is not None and error.code() == KafkaError._PARTITION_EOF:
            return
        if error is not None and error.fatal():
            raise KafkaException(error)
        logger.error(
            "[%s/%s] consume error: %s", self._spec.name, self._instance, error
        )

    def _handle_message(self, message: Message) -> None:
        """Decode, detect, dispatch, and commit one message.

        An unparseable message is logged and its offset committed anyway; stalling
        here would block the partition for this service.
        """
        raw = message.value()
        key = message.key()
        partition = message.partition()
        try:
            event = LifecycleEvent.model_validate(json.loads(raw.decode("utf-8")))
        except (ValueError, UnicodeDecodeError) as exc:
            logger.error(
                "[%s/%s] undecodable message at %s[%d]@%d: %s",
                self._spec.name,
                self._instance,
                message.topic(),
                partition,
                message.offset(),
                exc,
            )
            self._commit(message)
            return

        # R1.42, R2.8 — service and instance first, so streams stay greppable.
        logger.info(
            "[%s/%s] partition=%d offset=%d key=%s order_id=%s seq=%d type=%s",
            self._spec.name,
            self._instance,
            partition,
            message.offset(),
            key.decode("utf-8") if key is not None else "<null>",
            event.order_id,
            event.sequence,
            event.event_type,
        )

        # 003 — from the store, so unheld partitions arrive with history (R3.4, R3.8).
        current = self._store.load(partition, event.order_id)

        # D14 — a redelivery is a repeat, not a violation (R3.6, R3.14).
        redelivered = current is not None and event.sequence <= current.last_sequence

        updated, violations = apply_event(current, event)
        for violation in [] if redelivered else violations:
            # R1.41 — WARNING and a stable marker, so `grep VIOLATION` suffices.
            logger.warning(
                "[%s/%s] %s partition=%d offset=%d",
                self._spec.name,
                self._instance,
                violation.as_log_fields(),
                partition,
                message.offset(),
            )

        handler = self._spec.handlers.get(event.event_type)
        if handler is not None:
            handler(event)

        # R2.23 — the eviction lever (002 D9): sleep here and the commit below fails.
        if self._settings.handler_delay_seconds > 0:
            time.sleep(self._settings.handler_delay_seconds)

        self._persist_and_commit(message, event, updated, partition)

    def _persist_and_commit(
        self,
        message: Message,
        event: LifecycleEvent,
        fold: OrderFold,
        partition: int,
    ) -> None:
        """Write the fold and commit the offset, in the configured order (003 D4).

        Two writes, two systems, and no operation covering both. Which one goes first
        decides *how* that gap fails:

        ==================  ==========================================================
        ``state_first``     crash in the gap → the event is redelivered, the fold write
        (default)           is absorbed by the sequence guard, the handler runs twice
        ``offset_first``    crash in the gap → the event is never redelivered and the
                            fold is **permanently** missing it, silently
        ==================  ==========================================================

        The default is not a preference (R3.5). The lever exists so the other outcome
        can be watched rather than asserted (R3.16, R3.18).

        Raises:
            StateStoreUnavailable: If the state store fails. Deliberately not caught
                here — see :meth:`run` (R3.22).
        """
        if self._settings.state_write_order is StateWriteOrder.OFFSET_FIRST:
            self._commit(message)
            self._crash_if_configured(StateCrashPoint.OFFSET_COMMIT, event)
            self._save_fold(event, fold, partition)
            return

        self._save_fold(event, fold, partition)
        self._crash_if_configured(StateCrashPoint.STATE_WRITE, event)
        # R1.32 — only now, after the handler has run and the state is durable.
        self._commit(message)

    def _save_fold(
        self, event: LifecycleEvent, fold: OrderFold, partition: int
    ) -> None:
        """Persist one fold and report a redelivery the guard absorbed (R3.14, D7).

        Raises:
            StateStoreUnavailable: If the write fails.
        """
        outcome = self._store.save(partition, event.order_id, fold, event.event_id)
        if outcome.applied:
            return
        # The fold did not move, but the handler already ran; 008 drives this to zero.
        logger.warning(
            "[%s/%s] DUPLICATE_ABSORBED order_id=%s seq=%d stored_seq=%d handled=%d",
            self._spec.name,
            self._instance,
            event.order_id,
            event.sequence,
            fold.last_sequence,
            outcome.handled_count,
        )

    def _crash_if_configured(
        self, point: StateCrashPoint, event: LifecycleEvent
    ) -> None:
        """Kill the process here if this is the configured crash point (003 D5).

        **``os._exit`` and not ``sys.exit``.** Anything that unwinds the stack runs
        :meth:`run`'s ``finally``, which closes the consumer — a *graceful* departure
        from the group. That is a shutdown, not a crash, and it would produce a politer
        rebalance than the failure being simulated. ``os._exit`` skips ``finally``,
        ``atexit``, and buffer flushing, which is what a ``SIGKILL`` actually does.
        """
        if self._settings.state_crash_after is not point:
            return
        logger.critical(
            "[%s/%s] CRASH_LEVER point=%s order_id=%s seq=%d — exiting hard",
            self._spec.name,
            self._instance,
            point,
            event.order_id,
            event.sequence,
        )
        # Flush by hand: os._exit does not, and the line above is the evidence.
        for handler in logging.getLogger().handlers:
            handler.flush()
        os._exit(1)

    def _commit(self, message: Message) -> None:
        """Commit one offset, surviving the loss of the partition it belongs to.

        Under scale-out this can fail for a reason 001 could not produce: the member was
        evicted or the partition was revoked mid-handler, so the offset is somebody
        else's now. That is logged under its own marker — deliberately not 001's
        ``VIOLATION``, which means "the data was wrong" rather than "our membership
        changed underneath us" — and consumption continues so the member rejoins
        (R2.26, R2.27, D8).
        """
        try:
            self._consumer.commit(message=message, asynchronous=False)
        except KafkaException as exc:
            logger.warning(
                "[%s/%s] COMMIT_REJECTED partition=%d offset=%d reason=%s",
                self._spec.name,
                self._instance,
                message.partition(),
                message.offset(),
                exc.args[0] if exc.args else exc,
            )
