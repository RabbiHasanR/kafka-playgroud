"""Where a consumer's folded per-order state lives (specs 003 and 007).

Kafka remembers a consumer's **position** — the committed offset. Nothing in 001 or 002
remembered its **memory** — the per-order fold — so a restart or a rebalance produced
sequence-gap violations that never happened. This module is where that memory goes.

Two backends behind one protocol (003 D2):

``MemoryStateStore``
    002's behaviour, lifted out unchanged. It is the control for every experiment that
    compares before and after, so it is deliberately **not** improved on the way past.

``LocalStateStore``
    The durable one, since 007. An embedded RocksDB store on the instance's own disk,
    **one per owned partition**, made durable by a compacted changelog topic keyed
    identically to the state. It replaced ``PostgresStateStore``, which held the same
    folds in one shared table (X4, X5).

Three things changed at 007 and each is load-bearing.

**State is co-partitioned with input.** The instance holding partition 2 holds exactly
partition 2's keys, in ``<state_dir>/<group_id>/2/``, behind an exclusive lock no other
process can take. There is no shared server, so there is no contention and no way to read
another member's state by accident.

**Local disk is not durable, and the changelog is what makes it recoverable.** Every
mutation is also produced to ``<prefix>.<group_id>``, compacted and keyed by ``order_id``.
:meth:`LocalStateStore.restore` rebuilds one partition by replaying that topic's partition
of the same number — costing the number of **keys**, not the number of **events**, which
is the whole point of 006 having established compaction first.

**The changelog is per consumer group.** Compaction retains the latest value per key and
the three services fold the same order at their own pace, so one topic keyed by
``order_id`` would have them overwrite one another. The group therefore goes in the topic
name; putting it in the key would put it in the partition hash and break co-partitioning.

From 008 those two writes can be one. Both are Kafka operations — which is what 007's
move out of Postgres bought — so under ``PROCESSING_GUARANTEE=exactly_once`` the changelog
record and the offset go into a single transaction, and the producer that carries them is
no longer built here: it is the process's one producer, shared with the failure router,
because every write inside one transaction must come from one instance (R8.4).

What the transaction does **not** cover is the RocksDB write, which is a disk operation and
cannot be rolled back. So an abort has a repair rather than a rollback: :meth:`discard`
deletes the affected partitions and :meth:`restore` replays them from the changelog's
committed records. That is why the restore reader reads ``read_committed``, and why
``STATE_REBUILD=checkpoint`` is refused under the guarantee (R8.11, R8.12).
"""

import json
import logging
import shutil
import time
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Message,
    Producer,
    TopicPartition,
)
from rocksdict import Rdict

from order_service.config import Settings, StateRebuild
from order_service.consumer.transactions import isolation_level
from order_service.events import OrderState

logger = logging.getLogger(__name__)

#: How long a restore waits for the next changelog record before declaring the replay
#: stalled. Same shape as ``dlq_replay``'s guard: a rebuild that cannot finish must fail
#: loudly, because the alternative is folding against a half-built store.
_RESTORE_STALL_SECONDS = 10.0

#: How long ``get_watermark_offsets`` waits. A rebuild cannot start without knowing where
#: the partition ends, so this failing is fatal to the assignment rather than skippable.
_WATERMARK_TIMEOUT_SECONDS = 10.0


@dataclass(frozen=True)
class OrderFold:
    """What a service has accumulated about one order.

    Lives here rather than in ``runtime.py`` because the durable store builds one from a
    stored value, so the store needs the class at runtime and the import can only point
    one way.

    Deliberately carries no ``handled_count``: that is the store's bookkeeping, and
    keeping it off the domain fold is what leaves ``apply_event()`` with no opinion about
    persistence.
    """

    order_id: str
    last_sequence: int = 0
    state: OrderState | None = None


@dataclass(frozen=True)
class SaveOutcome:
    """What a store did with one write.

    Attributes:
        applied: Whether the fold advanced. ``False`` means the incoming event was at or
            below the stored sequence — a redelivery, absorbed (R3.11).
        handled_count: Deliveries handled for this order, including the ones whose fold
            write was a no-op. ``handled_count`` exceeding the event count is the
            dual-write problem's residue, as a number (R3.13).
    """

    applied: bool
    handled_count: int


class StateStoreUnavailable(RuntimeError):
    """The durable store cannot be reached or is not usable (R3.21, R3.22)."""


class StateStore(Protocol):
    """Where one consumer keeps its folded state."""

    def load(self, partition: int, order_id: str) -> OrderFold | None:
        """Return the fold for one order, or ``None`` if it has never been seen."""
        ...

    def save(
        self, partition: int, order_id: str, fold: OrderFold, event_id: str
    ) -> SaveOutcome:
        """Record the fold for one order and report what happened.

        ``event_id`` is which event produced this fold. It is passed alongside rather
        than carried on :class:`OrderFold` so that ``apply_event()`` stays untouched
        (R3.6) — the fold is what the service knows, and which event last advanced it is
        what the store records about it (001 D11).
        """
        ...

    def delete(self, partition: int, order_id: str) -> bool:
        """Erase one order's fold entirely, and report whether there was one.

        Distinct from :meth:`forget`, which releases a *partition* without destroying
        anything durable. This is the durable delete a tombstone asks for (R6.10).
        """
        ...

    def restore(self, partitions: Iterable[int]) -> None:
        """Rebuild state for exactly these partitions before any of them is consumed.

        Called from the assignment callback, and blocking on purpose: a store rebuilt
        *after* messages have been processed is a store that missed them (R7.7).
        """
        ...

    def flush(self) -> None:
        """Make every write so far durable, before the caller commits an offset (R7.11)."""
        ...

    def forget(self, partitions: Iterable[int]) -> None:
        """Release in-process state for exactly these partitions."""
        ...

    def discard(self, partitions: Iterable[int]) -> None:
        """Destroy state for these partitions, so the next restore rebuilds it (R8.11).

        Unlike :meth:`forget`, which releases a store believing it to be correct, this
        throws the contents away. An aborted transaction leaves the store holding folds
        the changelog never received, and no transaction can roll back a local disk
        write — so the only repair is to delete and replay.
        """
        ...

    def held(self) -> list[int]:
        """Return the partitions this store currently holds state for, for logging."""
        ...

    def close(self) -> None:
        """Release any resources held by the store."""
        ...


class MemoryStateStore:
    """002's in-process fold store, unchanged (R3.20, D2).

    Partition first, then order: an instance holds state for exactly the partitions it
    owns, so a revocation is one ``pop`` and the shape of the data structure says what
    co-partitioned state means.

    **It has no sequence guard, and that is not an oversight.** Adding one would change
    the behaviour 002 recorded — there, a redelivered event overwrote the fold with its
    lower sequence and the *next* real event reported a gap. That observation is the
    "before" half of this feature's evidence, so this class has to keep producing it.
    """

    def __init__(self) -> None:
        self._folds: dict[int, dict[str, OrderFold]] = {}
        self._handled: dict[tuple[int, str], int] = {}

    def load(self, partition: int, order_id: str) -> OrderFold | None:
        """Return the fold held for one order on one partition."""
        return self._folds.get(partition, {}).get(order_id)

    def save(
        self, partition: int, order_id: str, fold: OrderFold, event_id: str
    ) -> SaveOutcome:
        """Overwrite the fold — last write wins, exactly as in 002.

        ``event_id`` is accepted and ignored: this backend keeps no record beyond the
        fold, which is what makes it 002's control.
        """
        del event_id
        self._folds.setdefault(partition, {})[order_id] = fold
        handled = self._handled.get((partition, order_id), 0) + 1
        self._handled[(partition, order_id)] = handled
        return SaveOutcome(applied=True, handled_count=handled)

    def delete(self, partition: int, order_id: str) -> bool:
        """Drop one order's fold and its delivery count (R6.10).

        Returns:
            ``True`` if a fold was actually removed.
        """
        removed = self._folds.get(partition, {}).pop(order_id, None)
        self._handled.pop((partition, order_id), None)
        return removed is not None

    def restore(self, partitions: Iterable[int]) -> None:
        """Do nothing, deliberately (R3.20).

        This backend **is** 002's amnesia. Rebuilding here would erase the "before" half
        of every experiment 003 and 007 are measured against, so the one thing this
        method must not do is work.
        """
        del partitions

    def flush(self) -> None:
        """Do nothing — there is nothing to make durable, which is the point (R3.20)."""

    def forget(self, partitions: Iterable[int]) -> None:
        """Forget everything about these partitions (R2.14).

        Whoever receives them next starts with no memory of their orders, which is what
        produced 002's sequence-gap violations. On this backend that is still true.
        """
        for partition in set(partitions):
            self._folds.pop(partition, None)
            for key in [k for k in self._handled if k[0] == partition]:
                del self._handled[key]

    def discard(self, partitions: Iterable[int]) -> None:
        """Drop these partitions' folds (R8.11).

        Identical to :meth:`forget` on this backend, because there is nothing on disk to
        delete and nothing to replay afterwards — this store's restore is a no-op. Kept
        distinct so the Protocol has one meaning for both backends.
        """
        self.forget(partitions)

    def held(self) -> list[int]:
        """Return the partitions holding folds."""
        return sorted(self._folds)

    def close(self) -> None:
        """Nothing to release."""


@dataclass(frozen=True)
class _Record:
    """One order's row in the local store: the fold plus the store's own bookkeeping.

    Split from :class:`OrderFold` for the reason 003 gave — the fold is what the service
    knows, and ``last_event_id`` / ``handled_count`` are what the store records *about*
    it. Keeping them apart is what leaves ``apply_event()`` with no opinion about
    persistence.
    """

    fold: OrderFold
    last_event_id: str
    handled_count: int

    def encode(self) -> bytes:
        """Return the changelog value and the stored value — deliberately the same bytes.

        One encoding, so a restore cannot disagree with a write about what was stored.
        """
        return json.dumps(
            {
                "last_sequence": self.fold.last_sequence,
                "state": str(self.fold.state) if self.fold.state is not None else None,
                "last_event_id": self.last_event_id,
                "handled_count": self.handled_count,
            }
        ).encode("utf-8")

    @classmethod
    def decode(cls, order_id: str, raw: bytes) -> "_Record":
        """Rebuild a record from stored or replayed bytes.

        Raises:
            StateStoreUnavailable: If the value is not the shape this module wrote. A
                changelog that cannot be decoded is not a message-level failure and must
                not be routed like one — it means the store is unusable (R3.22).
        """
        try:
            payload = json.loads(raw)
            state = payload["state"]
            return cls(
                fold=OrderFold(
                    order_id=order_id,
                    last_sequence=int(payload["last_sequence"]),
                    state=OrderState(state) if state is not None else None,
                ),
                last_event_id=payload["last_event_id"],
                handled_count=int(payload["handled_count"]),
            )
        except (ValueError, TypeError, KeyError) as exc:
            raise StateStoreUnavailable(
                f"undecodable stored fold for {order_id}: {exc}"
            ) from exc


class LocalStateStore:
    """Folded state on local disk, made durable by a compacted changelog (007 D2–D5).

    One :class:`~rocksdict.Rdict` per **owned partition**, under
    ``<state_dir>/<group_id>/<partition>/``. Partition first, and at the level of a whole
    store rather than a key prefix, because that is what makes ownership physical: the
    engine takes an exclusive lock on the directory, so a second process cannot open a
    partition this instance holds. Releasing a partition is a ``close()``; listing what
    is held is a directory listing.

    There is **no read-through cache**, unlike the Postgres backend this replaced. After
    :meth:`restore` the store *is* the warm copy, so a cache would be a second copy of
    something already in memory-mapped local files.
    """

    def __init__(self, settings: Settings, group_id: str, producer: Producer) -> None:
        """Open the store root and take the producer the changelog is written through.

        Args:
            settings: Resolved environment settings.
            group_id: The consumer group whose memory this is. It names both the
                directory and the changelog topic, so the three services' memories stay
                independent exactly as their offsets are (R3.2).
            producer: The process's one producer, built and owned by ``main.py`` (R8.4,
                008 D1). This class built its own until 008. It cannot any more: under a
                transaction the changelog write and the offset have to be covered
                together, and every write inside one transaction must come from one
                producer instance — which the failure router also writes through.

        Raises:
            StateStoreUnavailable: If the state directory cannot be created.
        """
        self._settings = settings
        self._group_id = group_id
        self._topic = settings.changelog_topic_for(group_id)
        self._root = Path(settings.state_dir) / group_id
        try:
            self._root.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise StateStoreUnavailable(
                f"state backend 'local' cannot use {self._root}: {exc}"
            ) from exc

        self._stores: dict[int, Rdict] = {}
        #: Partition to the changelog offset the store has been brought up to. Seeded by
        #: restore, advanced by delivery reports, written to the checkpoint on release.
        self._position: dict[int, int] = {}

        # 008 D1 — not built here any more. `acks=all` and the shared partitioner moved
        # to `transactions.build_producer` unchanged; what changed is who owns it.
        self._producer = producer

    @property
    def group_id(self) -> str:
        """Return the consumer group this store holds the memory of."""
        return self._group_id

    @property
    def changelog_topic(self) -> str:
        """Return the topic this store's mutations are written to."""
        return self._topic

    # -- reads and writes -----------------------------------------------------------

    def load(self, partition: int, order_id: str) -> OrderFold | None:
        """Return the fold for one order from the local store.

        A plain local read: after :meth:`restore` every key this partition owns is
        already here, so a miss means the order has genuinely never been seen.

        Raises:
            StateStoreUnavailable: If the partition's store cannot be read.
        """
        record = self._read(partition, order_id)
        return None if record is None else record.fold

    def save(
        self, partition: int, order_id: str, fold: OrderFold, event_id: str
    ) -> SaveOutcome:
        """Write the fold under the sequence guard, then publish it to the changelog.

        The guard is read-modify-write here rather than 003's single atomic ``UPSERT``,
        and that is safe for exactly one reason: **a partition has one writer**. That is
        the invariant 002 established and R7.12 now states outright — and it is why the
        retry worker republishes instead of writing folds of its own.

        The changelog write does **not** wait for a delivery report. R7.11's ordering is
        paid once per committed message by :meth:`flush`, not once per fold.

        Raises:
            StateStoreUnavailable: If the store or the changelog cannot be written.
        """
        previous = self._read(partition, order_id)
        previous_sequence = 0 if previous is None else previous.fold.last_sequence
        handled_count = 1 if previous is None else previous.handled_count + 1

        # "Applied" means the fold ADVANCED, not that the write succeeded — same
        # distinction the SQL guard drew with GREATEST/CASE (003 D6).
        applied = fold.last_sequence > previous_sequence
        if applied or previous is None:
            record = _Record(
                fold=fold, last_event_id=event_id, handled_count=handled_count
            )
        else:
            record = _Record(
                fold=previous.fold,
                last_event_id=previous.last_event_id,
                handled_count=handled_count,
            )

        self._write(partition, order_id, record.encode())
        return SaveOutcome(applied=applied, handled_count=handled_count)

    def delete(self, partition: int, order_id: str) -> bool:
        """Erase one order locally and tombstone it on the changelog (R6.10, R7.6).

        The changelog half is what stops the delete being undone by the next rebuild.
        Note the contrast with :meth:`forget` below: that releases a partition without
        destroying anything, because a rebalance must not destroy a memory. This is the
        opposite — a tombstone is an instruction to destroy one.

        Args:
            partition: The partition owning this order's store.
            order_id: The order to erase.

        Returns:
            ``True`` if the order was actually present. ``False`` means the tombstone
            arrived for an order this group had never folded, which is normal on a
            replay and is not an error.

        Raises:
            StateStoreUnavailable: If the store or the changelog cannot be written.
        """
        store = self._store_for(partition)
        try:
            existed = store.get(order_id) is not None
            if existed:
                del store[order_id]
        except Exception as exc:  # noqa: BLE001 — the engine raises bare Exception
            raise StateStoreUnavailable(f"deleting {order_id}: {exc}") from exc

        # Published even when nothing was present, so that a member which never folded
        # this order still records the delete for whoever rebuilds from here.
        self._publish(partition, order_id, None)
        return existed

    def flush(self, timeout: float = 30.0) -> None:
        """Block until every buffered changelog record is acknowledged (R7.11).

        Called immediately before the offset commit, which is what keeps the invariant
        **changelog >= committed offset**: a rebuild may be ahead of the offset, never
        behind it. The reverse order would let a crash leave the offset past a fold that
        no rebuild can reproduce.

        Raises:
            StateStoreUnavailable: If anything is still unsent when the timeout expires.
        """
        remaining = self._producer.flush(timeout)
        if remaining:
            raise StateStoreUnavailable(
                f"{remaining} changelog record(s) unacknowledged after {timeout:.0f}s — "
                f"refusing to commit past state that cannot be rebuilt"
            )

    # -- rebuilding -----------------------------------------------------------------

    def restore(self, partitions: Iterable[int]) -> None:
        """Rebuild each partition's store from its changelog partition (R7.7, R7.8).

        Blocking, inside the assignment callback, and that is the design: the loop must
        not process a message for a partition whose memory is still being read back.

        Raises:
            StateStoreUnavailable: If a partition cannot be replayed to its watermark.
                Deliberately fatal — folding against a half-built store would report
                sequence gaps that never happened, which is the exact bug this whole
                line of features exists to remove.
        """
        for partition in sorted(set(partitions)):
            self._restore_one(partition)

    def _restore_one(self, partition: int) -> None:
        """Replay one changelog partition into one store, and log what it cost."""
        started = time.monotonic()
        reader = self._changelog_reader()
        try:
            self._await_metadata(reader, partition)
            target = TopicPartition(self._topic, partition)
            try:
                _low, high = reader.get_watermark_offsets(
                    target, timeout=_WATERMARK_TIMEOUT_SECONDS, cached=False
                )
            except KafkaException as exc:
                raise StateStoreUnavailable(
                    f"cannot read the end of {self._topic}-{partition}: {exc}"
                ) from exc

            start = self._start_offset(partition, low=_low)
            # Opened whether or not there is anything to replay. An owned partition has a
            # store, and the lock on it, from the moment it is assigned — otherwise an
            # empty changelog leaves the member owning partitions it holds no store for,
            # and `held()` reports [] while the rebalance log says otherwise (R7.3).
            self._store_for(partition)

            records, keys = 0, 0
            if start < high:
                # assign(), never subscribe(): a subscription would join a SECOND group
                # and rebalance it — during a rebalance (007 D7).
                reader.assign([TopicPartition(self._topic, partition, start)])
                records, keys = self._drain(reader, partition, start=start, high=high)
        finally:
            reader.close()

        self._position[partition] = high
        # WARNING, alongside REBALANCE / VIOLATION / TOMBSTONE, because `records` versus
        # `keys` is this entire feature stated as two numbers (R7.7).
        logger.warning(
            "[%s] RESTORED partition=%d records=%d keys=%d from=%d to=%d mode=%s ms=%d",
            self._group_id,
            partition,
            records,
            keys,
            start,
            high,
            self._settings.state_rebuild,
            int((time.monotonic() - started) * 1000),
        )

    def _drain(
        self, reader: Consumer, partition: int, *, start: int, high: int
    ) -> tuple[int, int]:
        """Read one assigned partition to its watermark, applying every record.

        Returns:
            The number of records read, and the number of keys **left live** by them.
            A key whose last record was a tombstone is not counted: it was replayed, but
            it was not restored, and R7.7 asks for the second number. The gap between the
            two is the compaction ratio — the whole point of the changelog being a table.

        Raises:
            StateStoreUnavailable: If the replay stalls short of the watermark.
        """
        store = self._store_for(partition)
        records, live = 0, set()
        next_offset = start
        while next_offset < high:
            message = reader.poll(_RESTORE_STALL_SECONDS)
            if message is None:
                raise StateStoreUnavailable(
                    f"restoring {self._topic}-{partition} stalled at offset "
                    f"{next_offset} of {high}"
                )
            error = message.error()
            if error is not None:
                if error.code() == KafkaError._PARTITION_EOF:  # noqa: SLF001
                    continue
                raise StateStoreUnavailable(
                    f"restoring {self._topic}-{partition}: {error}"
                )

            next_offset = message.offset() + 1
            records += 1
            order_id = _key_of(message)
            try:
                if message.value() is None:
                    # A tombstone on the changelog: the delete, replayed. Without this
                    # branch every rebuild would resurrect every deleted order.
                    if store.get(order_id) is not None:
                        del store[order_id]
                    live.discard(order_id)
                else:
                    store[order_id] = bytes(message.value())
                    live.add(order_id)
            except StateStoreUnavailable:
                raise
            except Exception as exc:  # noqa: BLE001 — the engine raises bare Exception
                raise StateStoreUnavailable(
                    f"restoring {self._topic}-{partition} at offset "
                    f"{message.offset()}: {exc}"
                ) from exc

        return records, len(live)

    def _start_offset(self, partition: int, *, low: int) -> int:
        """Return the changelog offset a rebuild begins at (R7.9).

        Under ``full`` the store is destroyed and replayed from the beginning of the
        partition, so the cost is visible on every assignment. Under ``checkpoint`` the
        existing store is kept and only the delta is applied.
        """
        if self._settings.state_rebuild is StateRebuild.CHECKPOINT:
            checkpoint = self._read_checkpoint(partition)
            if checkpoint is not None:
                return max(checkpoint, low)
        self._destroy(partition)
        return low

    def _await_metadata(self, reader: Consumer, partition: int) -> None:
        """Load the changelog's metadata before anything asks about its offsets.

        ``get_watermark_offsets`` answers from librdkafka's **local** metadata cache, so a
        consumer that has not fetched metadata yet fails it with ``_UNKNOWN_PARTITION``
        (a local error, value -190) rather than asking the broker. That happens routinely
        on a cold start, when the consumers reach their first rebalance before the broker
        is serving topic metadata — and without this call it kills the process over a race
        that resolves itself a second later.

        ``list_topics`` is a real metadata request, so it both settles the race and turns
        a genuinely missing topic into an error that says which one and what to run.

        Raises:
            StateStoreUnavailable: If metadata cannot be fetched, or the topic really does
                not have this partition.
        """
        try:
            metadata = reader.list_topics(
                topic=self._topic, timeout=_WATERMARK_TIMEOUT_SECONDS
            )
        except KafkaException as exc:
            raise StateStoreUnavailable(
                f"cannot fetch metadata for {self._topic}: {exc}"
            ) from exc

        topic_metadata = metadata.topics.get(self._topic)
        if topic_metadata is None or topic_metadata.error is not None:
            reason = "unknown" if topic_metadata is None else topic_metadata.error
            raise StateStoreUnavailable(
                f"changelog topic {self._topic} is not available ({reason}) — "
                f"auto-creation is off, so run scripts/create_topics.sh"
            )
        if partition not in topic_metadata.partitions:
            raise StateStoreUnavailable(
                f"{self._topic} has no partition {partition} "
                f"(it has {sorted(topic_metadata.partitions)}) — the changelog must have "
                f"the same partition count as the lifecycle topic"
            )

    def _changelog_reader(self) -> Consumer:
        """Build the assign-only consumer a rebuild reads through (R7.8).

        ``group.id`` is required to construct a ``Consumer`` at all, so it is set to a
        throwaway that is never joined: the reader only ever calls ``assign()``, which
        performs no group join, commits nothing, and leaves nothing in the coordinator.

        **The isolation level is a correctness setting here, not a preference** (R8.10,
        008 D10). At ``read_uncommitted`` a rebuild replays folds belonging to
        transactions that aborted — which is precisely the corruption the transaction was
        bought to prevent, reintroduced by the mechanism meant to repair it.
        """
        return Consumer(
            {
                "bootstrap.servers": self._settings.kafka_bootstrap_servers,
                "group.id": f"{self._group_id}-restore-{uuid.uuid4().hex[:8]}",
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
                "enable.partition.eof": True,
                "isolation.level": isolation_level(self._settings),
            }
        )

    # -- lifecycle ------------------------------------------------------------------

    def forget(self, partitions: Iterable[int]) -> None:
        """Release exactly these partitions, destroying nothing (R3.9, R7.10).

        Flush first, then checkpoint, then close. The directory and every changelog
        record stay where they are, so the member assigned this partition next can
        rebuild what this one folded — which is the entire difference between 002's
        rebalance and this one.
        """
        wanted = sorted(set(partitions))
        if not wanted:
            return
        # Before the checkpoints: a checkpoint claims the changelog holds everything up
        # to that offset, and an unflushed produce would make that a lie.
        self._producer.flush(30.0)
        for partition in wanted:
            store = self._stores.pop(partition, None)
            if store is None:
                continue
            self._write_checkpoint(partition)
            store.close()

    def discard(self, partitions: Iterable[int]) -> None:
        """Delete these partitions' stores outright, checkpoints included (R8.11).

        The counterpart to :meth:`forget`, and deliberately not a variant of it. ``forget``
        releases a store believing it correct and leaves it on disk for whoever holds the
        partition next. This throws it away, because an aborted transaction leaves the
        store holding folds the changelog never received and there is no way to write a
        disk record back out of existence.

        Nothing is flushed on the way out — flushing would push records the transaction
        just abandoned — and the checkpoint goes with the directory, because a checkpoint
        for a store that no longer exists would have the next restore skip the records
        that rebuild it.

        Raises:
            StateStoreUnavailable: If a directory cannot be removed. Consuming against a
                store that could not be repaired is worse than stopping.
        """
        for partition in sorted(set(partitions)):
            store = self._stores.pop(partition, None)
            if store is not None:
                store.close()
            self._position.pop(partition, None)
            path = self._path_for(partition)
            checkpoint = self._checkpoint_path(partition)
            try:
                shutil.rmtree(path, ignore_errors=False)
                checkpoint.unlink(missing_ok=True)
            except OSError as exc:
                raise StateStoreUnavailable(
                    f"could not discard partition {partition} at {path}: {exc}"
                ) from exc
            logger.warning(
                "[%s] STORE_DISCARDED partition=%d — rebuilding from %s",
                self._group_id,
                partition,
                self._topic,
            )

    def held(self) -> list[int]:
        """Return the partitions this instance currently holds a store for."""
        return sorted(self._stores)

    def close(self) -> None:
        """Flush, checkpoint and close every open partition.

        The producer is **not** closed here: since 008 it belongs to ``main.py``, which
        also writes the failure router through it (008 D1).
        """
        self.forget(list(self._stores))

    # -- internals ------------------------------------------------------------------

    def _store_for(self, partition: int) -> Rdict:
        """Open, or return, the store for one partition.

        Raises:
            StateStoreUnavailable: If the directory cannot be opened — most often
                because another process already holds its lock, which is the constraint
                R7.12 exists to state.
        """
        store = self._stores.get(partition)
        if store is not None:
            return store
        path = self._path_for(partition)
        try:
            store = Rdict(str(path))
        except Exception as exc:  # noqa: BLE001 — the engine raises bare Exception
            raise StateStoreUnavailable(
                f"state backend 'local' cannot open {path}: {exc}"
            ) from exc
        self._stores[partition] = store
        return store

    def _read(self, partition: int, order_id: str) -> _Record | None:
        """Return one stored record, or ``None``."""
        store = self._store_for(partition)
        try:
            raw = store.get(order_id)
        except Exception as exc:  # noqa: BLE001 — the engine raises bare Exception
            raise StateStoreUnavailable(f"reading {order_id}: {exc}") from exc
        return None if raw is None else _Record.decode(order_id, bytes(raw))

    def _write(self, partition: int, order_id: str, value: bytes) -> None:
        """Write one value locally and publish the same bytes to the changelog."""
        store = self._store_for(partition)
        try:
            store[order_id] = value
        except Exception as exc:  # noqa: BLE001 — the engine raises bare Exception
            raise StateStoreUnavailable(f"writing {order_id}: {exc}") from exc
        self._publish(partition, order_id, value)

    def _publish(self, partition: int, order_id: str, value: bytes | None) -> None:
        """Enqueue one changelog record, which may be a tombstone.

        Raises:
            StateStoreUnavailable: If it cannot be enqueued even after draining.
        """

        def on_delivery(err: object, msg: Message) -> None:
            if err is not None:
                # Not raised here — this runs on the producer's thread. flush() is what
                # turns an unacknowledged changelog into a refusal to commit.
                logger.error(
                    "[%s] CHANGELOG_DELIVERY_FAILED order_id=%s reason=%s",
                    self._group_id,
                    order_id,
                    err,
                )
                return
            self._position[msg.partition()] = max(
                self._position.get(msg.partition(), 0), msg.offset() + 1
            )

        try:
            self._producer.produce(
                topic=self._topic,
                key=order_id.encode("utf-8"),
                value=value,
                on_delivery=on_delivery,
            )
        except BufferError:
            # The local queue is full: serve delivery reports and try once more, rather
            # than dropping a record that makes the local store unrecoverable.
            self._producer.flush(30.0)
            try:
                self._producer.produce(
                    topic=self._topic,
                    key=order_id.encode("utf-8"),
                    value=value,
                    on_delivery=on_delivery,
                )
            except (BufferError, KafkaException) as exc:
                raise StateStoreUnavailable(
                    f"changelog for {order_id} could not be enqueued: {exc}"
                ) from exc
        except KafkaException as exc:
            raise StateStoreUnavailable(
                f"changelog for {order_id} could not be enqueued: {exc}"
            ) from exc

        # Serves delivery callbacks without blocking, so _position advances as the
        # broker acknowledges rather than only at the next flush.
        self._producer.poll(0)

    def _path_for(self, partition: int) -> Path:
        """Return the directory holding one partition's store."""
        return self._root / str(partition)

    def _checkpoint_path(self, partition: int) -> Path:
        """Return the file holding one partition's restore mark (007 D8).

        Beside the store rather than inside it: a store that will not open would
        otherwise take its own recovery marker down with it.
        """
        return self._root / f"{partition}.ckpt"

    def _read_checkpoint(self, partition: int) -> int | None:
        """Return the offset this partition was last brought up to, if it is recorded."""
        path = self._checkpoint_path(partition)
        try:
            return int(path.read_text().strip())
        except (OSError, ValueError):
            # Absent or unreadable is not an error — it means "replay from the start",
            # which is always correct, only slower.
            return None

    def _write_checkpoint(self, partition: int) -> None:
        """Record how far this partition's store has been brought up to."""
        position = self._position.get(partition)
        if position is None:
            return
        try:
            self._checkpoint_path(partition).write_text(f"{position}\n")
        except OSError as exc:
            # Never fatal: a lost checkpoint costs a full replay, which is the default
            # anyway. Losing the partition over it would be the worse trade.
            logger.warning(
                "[%s] could not write checkpoint for partition %d: %s",
                self._group_id,
                partition,
                exc,
            )

    def _destroy(self, partition: int) -> None:
        """Close and erase one partition's store, so a full replay starts empty."""
        store = self._stores.pop(partition, None)
        if store is not None:
            store.close()
        path = self._path_for(partition)
        if not path.exists():
            return
        try:
            Rdict.destroy(str(path))
        except Exception as exc:  # noqa: BLE001 — the engine raises bare Exception
            raise StateStoreUnavailable(
                f"cannot reset {path} for a full rebuild: {exc}"
            ) from exc


def _key_of(message: Message) -> str:
    """Return a changelog record's key as text.

    Raises:
        StateStoreUnavailable: If the key is null. A compacted topic rejects null keys at
            the broker, so this means the topic is not the one this store thinks it is.
    """
    key = message.key()
    if key is None:
        raise StateStoreUnavailable(
            f"null key on {message.topic()}-{message.partition()} at offset "
            f"{message.offset()} — is that topic compacted?"
        )
    return bytes(key).decode("utf-8")
