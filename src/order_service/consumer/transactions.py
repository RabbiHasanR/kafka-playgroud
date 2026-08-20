"""The one producer a consumer process owns, and how its offsets get committed (008).

Until this feature a consumer process ran **two** producers. ``LocalStateStore`` built one
for the changelog; ``FailureRouter`` built another for the retry and dead-letter topics.
That was fine while each write stood alone. It stops being fine the moment a transaction
has to cover a write *and* the offset of the message that caused it, because **every write
inside one transaction must come from one producer instance** (R8.4, D1).

So the producer moves out of both components and is built here, once, in ``main.py``, and
handed to whoever needs to write. The sharing is unconditional rather than switched on with
the guarantee: both components already hardcoded ``acks=all``, and the changelog's
``partitioner=consistent_random`` is librdkafka's default anyway, so nothing is given up —
and a second wiring shape that only one path exercises is a second thing to get wrong.

**Why the identity must be stable.** Under ``exactly_once`` the producer carries a
``transactional.id``. The broker uses it to fence: when a producer with a known identity
reappears, its epoch is bumped and the previous holder's writes start being rejected. That
is what stops a zombie — an evicted member whose process is still alive and still mid-batch
— from committing on top of its replacement. A fresh random identity each start would fence
nothing, because the zombie holds a different one. :meth:`Settings.transactional_id_for`
derives it from the group and the instance, which is what makes ``CONSUMER_INSTANCE_ID``
correctness-critical from here on (D3).

**Why fencing is fatal and not retried.** A fenced producer cannot un-fence itself. Its
epoch is behind and every transactional call it makes will keep failing, so retrying is a
spin, not a recovery. :class:`ProducerFenced` exists to carry that to the top of the loop,
where the process exits and lets the container restart with a fresh epoch (R8.5).

**What a transaction covers, and what it does not.** It covers the changelog records, the
retry and dead-letter records, and the offsets — all Kafka operations, which is only true
because 007 moved the fold out of Postgres. It does **not** cover the local RocksDB write,
which is why an abort returns the partitions it touched: the caller has to discard and
rebuild them, because nothing here can roll a disk write back (D6).
"""

import logging
import time
from typing import Protocol

from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer, TopicPartition

from order_service.config import ProcessingGuarantee, Settings

logger = logging.getLogger(__name__)

#: Errors that mean this producer's epoch is behind and will never catch up.
_FENCING_CODES = frozenset(
    {
        KafkaError._FENCED,
        KafkaError.INVALID_PRODUCER_EPOCH,
        KafkaError.PRODUCER_FENCED,
    }
)


class ProducerFenced(Exception):
    """Another producer took this one's transactional identity (R8.5).

    Raised instead of retried. The epoch is gone; every subsequent transactional call
    fails the same way, so the only recovery is a new process.
    """


def is_fenced(exc: KafkaException) -> bool:
    """Return whether a Kafka error means this producer has been fenced.

    Args:
        exc: The exception raised by a transactional call.

    Returns:
        True if the error names a fencing or epoch condition.
    """
    error = exc.args[0] if exc.args else None
    return isinstance(error, KafkaError) and error.code() in _FENCING_CODES


def build_producer(settings: Settings, group_id: str, instance: str) -> Producer:
    """Build the one producer this consumer process writes everything through (R8.1, R8.3).

    Args:
        settings: Resolved environment settings.
        group_id: The consumer group this process belongs to. Names the changelog topic
            and half the transactional identity.
        instance: This member's label, the other half of the identity.

    Returns:
        A producer, transactional if ``PROCESSING_GUARANTEE=exactly_once``.
    """
    config: dict[str, object] = {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        # Hardcoded rather than following PRODUCER_ACKS, as both components it replaces
        # did: these are the records that make local disk recoverable and that stop a
        # failed message being dropped, and losing either silently is the failure the
        # whole path exists to prevent.
        "acks": "all",
        # The SAME partitioner as the order producer. Equal partition counts plus one key
        # and one partitioner are what put order X's fold and order X's events on the same
        # partition NUMBER, which is what makes restoring one partition from one changelog
        # partition correct (006 D8, 007 D1).
        "partitioner": "consistent_random",
        "client.id": f"{group_id}-{instance}",
        # R8.1 — the broker deduplicates a retried produce and REJECTS an out-of-order
        # sequence. The second half is the one that matters here: this domain is an ordered
        # lifecycle, and a reordered fold on a compacted changelog is corruption that
        # survives a rebuild.
        "enable.idempotence": settings.producer_idempotence,
    }

    if settings.exactly_once:
        # Forced on regardless of PRODUCER_IDEMPOTENCE — librdkafka rejects the
        # combination, and a transaction without it would be meaningless anyway.
        config["enable.idempotence"] = True
        config["transactional.id"] = settings.transactional_id_for(group_id)
        config["transaction.timeout.ms"] = settings.transaction_timeout_ms

    return Producer(config)


class CommitStrategy(Protocol):
    """How a consumer's offsets reach the broker (008 D4).

    One protocol, two implementations, chosen by one environment variable — the same
    shape ``StateStore`` has carried since 003 D2, and for the same reason: the
    pre-feature behaviour has to stay runnable as the control.
    """

    def note(self, message: Message) -> None:
        """Record that ``message`` has been fully handled."""
        ...

    def maybe_commit(self) -> None:
        """Commit if this strategy's interval has been reached."""
        ...

    def abort(self, reason: str) -> frozenset[int]:
        """Discard uncommitted work, returning the partitions it had written to."""
        ...

    def close(self) -> None:
        """Release anything held, committing nothing."""
        ...


class DirectCommitter:
    """At-least-once: commit each offset as soon as its message is handled.

    007's behaviour, lifted out of ``ServiceConsumer._commit`` unchanged so that it stays
    the control the transactional path is measured against (R8.2).
    """

    def __init__(self, consumer: Consumer, service: str, instance: str) -> None:
        """Store what the rejection log line needs.

        Args:
            consumer: The consumer whose offsets are being committed.
            service: Service name, for the log marker.
            instance: Member label, for the log marker.
        """
        self._consumer = consumer
        self._service = service
        self._instance = instance

    def note(self, message: Message) -> None:
        """Commit one offset, surviving the loss of the partition it belongs to.

        Under scale-out this can fail for a reason 001 could not produce: the member was
        evicted or the partition was revoked mid-handler, so the offset is somebody
        else's now. That is logged under its own marker — deliberately not 001's
        ``VIOLATION``, which means "the data was wrong" rather than "our membership
        changed underneath us" — and consumption continues so the member rejoins
        (R2.26, R2.27, 002 D8).
        """
        try:
            self._consumer.commit(message=message, asynchronous=False)
        except KafkaException as exc:
            logger.warning(
                "[%s/%s] COMMIT_REJECTED partition=%d offset=%d reason=%s",
                self._service,
                self._instance,
                message.partition(),
                message.offset(),
                exc.args[0] if exc.args else exc,
            )

    def maybe_commit(self) -> None:
        """Nothing to do — :meth:`note` already committed."""

    def abort(self, reason: str) -> frozenset[int]:
        """Nothing to abort; no work is ever uncommitted for longer than one message."""
        return frozenset()

    def close(self) -> None:
        """Nothing to release."""


class TransactionalCommitter:
    """Exactly-once: the fold, the failure route and the offsets land together (R8.6–R8.9).

    A transaction opens lazily on the first handled message and closes when either
    interval in D5 is reached. Offsets go through
    ``send_offsets_to_transaction`` rather than ``consumer.commit()`` — committing
    against the consumer would put the offset *outside* the transaction, which is the
    exact defect being fixed.

    The group metadata handed along with the offsets is what makes one producer per
    instance safe under rebalancing (KIP-447, D2): the broker checks this member's
    generation before accepting, so a member that has been evicted cannot commit for
    partitions it no longer holds.
    """

    def __init__(
        self,
        producer: Producer,
        consumer: Consumer,
        settings: Settings,
        service: str,
        instance: str,
    ) -> None:
        """Initialise transactions and start with none open.

        ``init_transactions()`` fences any previous holder of this identity and recovers
        whatever it left open, so it must happen before the first message is handled.

        Raises:
            ProducerFenced: If another producer already holds this identity.
        """
        self._producer = producer
        self._consumer = consumer
        self._settings = settings
        self._service = service
        self._instance = instance

        self._transactional_id = settings.transactional_id_for(
            settings.group_id_for(service)
        )
        self._open = False
        self._messages = 0
        self._opened_at = 0.0
        self._touched: set[int] = set()
        #: (topic, partition) to where consumption resumes, accumulated by `note`.
        self._offsets: dict[tuple[str, int], int] = {}

        try:
            producer.init_transactions()
        except KafkaException as exc:
            self._raise_if_fenced(exc)
            raise

    @property
    def touched(self) -> frozenset[int]:
        """Return the partitions the open transaction has written to."""
        return frozenset(self._touched)

    def note(self, message: Message) -> None:
        """Open a transaction if none is, and record this message's offset in it.

        The offset recorded is ``offset + 1`` — where consumption resumes, not where this
        message was — because that is what a committed offset means to Kafka.

        Accumulated per ``(topic, partition)`` rather than read back from
        ``Consumer.position()``. The position reflects what was *delivered* to this
        process, which is only the same thing as what was *handled* while every branch of
        the consume loop keeps calling this method. Recording it here means the offsets
        submitted are exactly the messages that reached the end of their handling, with no
        dependency on that invariant holding somewhere else.
        """
        self._begin_if_needed()
        self._offsets[(message.topic(), message.partition())] = message.offset() + 1
        self._touched.add(message.partition())
        self._messages += 1

    def maybe_commit(self) -> None:
        """Commit if either interval has been reached (R8.8, D5).

        Called after each handled message *and* on an empty poll: a transaction opened by
        the last message of a burst would otherwise stay open through the lull, holding
        every ``read_committed`` reader downstream at the last stable offset.
        """
        if not self._open:
            return
        by_count = self._messages >= self._settings.transaction_commit_interval_messages
        elapsed_ms = (time.monotonic() - self._opened_at) * 1000
        if by_count or elapsed_ms >= self._settings.transaction_commit_interval_ms:
            self.commit()

    def commit(self) -> None:
        """Submit the offsets and commit, closing the transaction.

        Raises:
            ProducerFenced: If this producer's identity has been taken.
            KafkaException: If the commit fails for any other reason.
        """
        if not self._open:
            return
        try:
            self._producer.send_offsets_to_transaction(
                [
                    TopicPartition(topic, partition, offset)
                    for (topic, partition), offset in self._offsets.items()
                ],
                self._consumer.consumer_group_metadata(),
            )
            self._producer.commit_transaction()
        except KafkaException as exc:
            self._raise_if_fenced(exc)
            raise
        finally:
            self._reset()

    def abort(self, reason: str) -> frozenset[int]:
        """Abort the open transaction and report what it had written to (R8.9, R8.11).

        The returned partitions are the caller's problem, not this class's: their local
        stores hold folds the changelog never received, and only the caller can discard
        and rebuild them.

        Args:
            reason: Why the abort happened, for the log line.

        Returns:
            The partitions the aborted transaction produced to. Empty if none was open.
        """
        if not self._open:
            return frozenset()
        touched = self.touched
        logger.warning(
            "[%s/%s] TRANSACTION_ABORTED partitions=%s messages=%d reason=%s",
            self._service,
            self._instance,
            sorted(touched) or "[]",
            self._messages,
            reason,
        )
        try:
            self._producer.abort_transaction()
        except KafkaException as exc:
            # An abort that itself fails leaves the broker to time the transaction out.
            # Worth a line, not worth stopping for — unless it is a fencing error, which
            # means this process must not continue at all.
            self._raise_if_fenced(exc)
            logger.error(
                "[%s/%s] abort_transaction failed: %s",
                self._service,
                self._instance,
                exc,
            )
        finally:
            self._reset()
        return touched

    def close(self) -> None:
        """Abort anything still open, committing nothing.

        A transaction open at shutdown covers work whose offsets were never submitted, so
        aborting is the honest close: the messages are redelivered to whoever comes next.
        """
        self.abort("shutdown")

    # -- internals ------------------------------------------------------------------

    def _begin_if_needed(self) -> None:
        """Open a transaction if none is open.

        Raises:
            ProducerFenced: If this producer's identity has been taken.
        """
        if self._open:
            return
        try:
            self._producer.begin_transaction()
        except KafkaException as exc:
            self._raise_if_fenced(exc)
            raise
        self._open = True
        self._messages = 0
        self._opened_at = time.monotonic()

    def _reset(self) -> None:
        """Return to having no transaction open."""
        self._open = False
        self._messages = 0
        self._opened_at = 0.0
        self._touched.clear()
        self._offsets.clear()

    def _raise_if_fenced(self, exc: KafkaException) -> None:
        """Convert a fencing error into :class:`ProducerFenced`.

        Raises:
            ProducerFenced: If ``exc`` names a fencing or epoch condition.
        """
        if not is_fenced(exc):
            return
        raise ProducerFenced(
            f"transactional.id {self._transactional_id} was taken by another "
            f"producer: {exc}"
        ) from exc


def build_commit_strategy(
    producer: Producer,
    consumer: Consumer,
    settings: Settings,
    service: str,
    instance: str,
) -> CommitStrategy:
    """Return the commit strategy the configured guarantee calls for (R8.6).

    Args:
        producer: The process's one producer.
        consumer: The consumer whose offsets are being committed.
        settings: Resolved environment settings.
        service: Service name, for log markers.
        instance: Member label, for log markers.

    Returns:
        ``TransactionalCommitter`` under ``exactly_once``, otherwise ``DirectCommitter``.

    Raises:
        ProducerFenced: If transactions cannot be initialised because the identity is
            already held.
    """
    if settings.processing_guarantee is ProcessingGuarantee.EXACTLY_ONCE:
        return TransactionalCommitter(producer, consumer, settings, service, instance)
    return DirectCommitter(consumer, service, instance)


def isolation_level(settings: Settings) -> str:
    """Return the isolation level every consumer in this process reads with (R8.10).

    One function rather than three call sites reading the same setting, because the
    changelog restore reader getting a different value from the consume loop is a silent
    correctness bug: a rebuild at ``read_uncommitted`` replays aborted folds.
    """
    return settings.consumer_isolation_level.value
