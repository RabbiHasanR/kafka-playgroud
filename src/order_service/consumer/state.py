"""Where a consumer's folded per-order state lives (spec 003).

Kafka remembers a consumer's **position** — the committed offset. Nothing in 001 or 002
remembered its **memory** — the per-order fold — so a restart or a rebalance produced
sequence-gap violations that never happened. This module is where that memory goes.

Two backends behind one protocol (D2):

``MemoryStateStore``
    002's behaviour, lifted out unchanged. It is the control for every experiment that
    compares before and after, so it is deliberately **not** improved on the way past.

``PostgresStateStore``
    The durable one. Keyed by ``(group_id, order_id)`` and **never by partition** (D1) —
    which is the whole reason a partition can move between members without its orders
    losing their history.

The offset still commits to Kafka and the fold now writes to Postgres, so there are two
writes to two systems and no operation covering both. That gap is this feature's subject:
:meth:`PostgresStateStore.save` absorbs it in the state with a guarded upsert, and
``handled_count`` counts what the guard does *not* absorb — the duplicate side effect,
which is 008's to remove (X4).
"""

import re
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Protocol

import psycopg

from order_service.events import OrderState


@dataclass(frozen=True)
class OrderFold:
    """What a service has accumulated about one order.

    Lives here rather than in ``runtime.py`` because ``PostgresStateStore`` builds one
    from a database row, so the store needs the class at runtime and the import can only
    point one way.

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

    def forget(self, partitions: Iterable[int]) -> None:
        """Release in-process state for exactly these partitions."""
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

    def forget(self, partitions: Iterable[int]) -> None:
        """Forget everything about these partitions (R2.14).

        Whoever receives them next starts with no memory of their orders, which is what
        produced 002's sequence-gap violations. On this backend that is still true.
        """
        for partition in set(partitions):
            self._folds.pop(partition, None)
            for key in [k for k in self._handled if k[0] == partition]:
                del self._handled[key]

    def held(self) -> list[int]:
        """Return the partitions holding folds."""
        return sorted(self._folds)

    def close(self) -> None:
        """Nothing to release."""


#: The whole idempotency mechanism, in one statement (D6).
#:
#: The asymmetry is the design: the fold columns advance **only** when a higher sequence
#: arrives, so a redelivery loses to itself and changes nothing (R3.11) and a full replay
#: from earliest is a no-op (R3.12) — while ``handled_count`` increments **every** time,
#: because it counts deliveries rather than advances (R3.13).
#:
#: One statement under autocommit is its own transaction, so there is no
#: read-modify-write, no ``SELECT … FOR UPDATE``, no application lock, and no window for
#: two members to interleave. The guard lives in the database, where concurrency is the
#: database's problem — which is also what makes a write from an evicted zombie member
#: harmless (D3).
#: The ``previous`` CTE is what makes "did this advance?" answerable. Inside
#: ``DO UPDATE`` the qualified name ``order_fold.last_sequence`` is the *old* value, but
#: in ``RETURNING`` it is the *new* one, so the pre-write sequence is otherwise
#: unreachable — and without it an exact redelivery (same sequence, ``GREATEST(n, n)``)
#: is indistinguishable from a fresh apply. Every CTE in one statement sees the same
#: snapshot, so ``previous`` reads the row as it stood before the upsert.
#:
#: It is still **one statement**, which is the part that matters: the guard stays atomic
#: without a transaction, a row lock, or a read-modify-write.
_UPSERT_FOLD = """
    WITH previous AS (
        SELECT last_sequence
          FROM order_fold
         WHERE group_id = %(group_id)s AND order_id = %(order_id)s
    ), upserted AS (
        INSERT INTO order_fold (
            group_id, order_id, last_sequence, state, last_event_id, handled_count
        )
        VALUES (%(group_id)s, %(order_id)s, %(sequence)s, %(state)s, %(event_id)s, 1)
        ON CONFLICT (group_id, order_id) DO UPDATE SET
            last_sequence = GREATEST(order_fold.last_sequence, EXCLUDED.last_sequence),
            state         = CASE WHEN EXCLUDED.last_sequence > order_fold.last_sequence
                                 THEN EXCLUDED.state ELSE order_fold.state END,
            last_event_id = CASE WHEN EXCLUDED.last_sequence > order_fold.last_sequence
                                 THEN EXCLUDED.last_event_id
                                 ELSE order_fold.last_event_id END,
            handled_count = order_fold.handled_count + 1,
            updated_at    = now()
        RETURNING last_sequence, handled_count
    )
    SELECT upserted.last_sequence,
           upserted.handled_count,
           COALESCE(previous.last_sequence, 0) AS previous_sequence
      FROM upserted LEFT JOIN previous ON true
"""

_SELECT_FOLD = """
    SELECT last_sequence, state
      FROM order_fold
     WHERE group_id = %(group_id)s AND order_id = %(order_id)s
"""

#: Cheapest possible proof that the schema is there before the group is joined.
_VERIFY_SCHEMA = "SELECT 1 FROM order_fold LIMIT 1"


def redact_dsn(dsn: str) -> str:
    """Return a connection string safe to put in a log line or an error.

    An unredacted DSN in a startup error puts the password into every log aggregator
    that ever sees it, so R3.21's "name the address it tried" has to stop short of the
    credential. Handles both URL and ``key=value`` forms.

    Args:
        dsn: The connection string to redact.

    Returns:
        The same string with any password replaced by ``***``.
    """
    redacted = re.sub(r"(://[^:/?#@]+:)[^@]*(@)", r"\1***\2", dsn)
    return re.sub(r"(password\s*=\s*)(\S+)", r"\1***", redacted)


class PostgresStateStore:
    """Durable fold state, with a read-through cache in front of it (D1, D3, D6).

    The cache is licensed by a property 002 spent a whole feature establishing: a
    partition belongs to exactly one member at a time, so the owner's cached folds cannot
    go stale — nobody else is writing those orders.

    Reads are lazy, on cache miss, and never warmed on assignment. Warming would cost a
    scan proportional to history at every rebalance, which is precisely the cost 007
    removes with a compacted changelog; paying it here would hide the problem 007 solves.
    """

    def __init__(self, dsn: str, group_id: str) -> None:
        """Connect, and prove the schema is usable before anyone joins a group.

        Args:
            dsn: libpq connection string.
            group_id: The consumer group whose memory this is. Part of the primary key,
                so the three services' memories stay independent (R3.2).

        Raises:
            StateStoreUnavailable: If the database cannot be reached or ``order_fold``
                does not exist.
        """
        self._dsn = dsn
        self._group_id = group_id
        self._cache: dict[int, dict[str, OrderFold]] = {}
        safe = redact_dsn(dsn)
        try:
            self._conn = psycopg.connect(dsn, autocommit=True)
        except psycopg.Error as exc:
            raise StateStoreUnavailable(
                f"state backend 'postgres' is unreachable at {safe}: {exc}"
            ) from exc
        try:
            with self._conn.cursor() as cursor:
                cursor.execute(_VERIFY_SCHEMA)
        except psycopg.Error as exc:
            self._conn.close()
            raise StateStoreUnavailable(
                f"state backend 'postgres' at {safe} has no usable order_fold table "
                f"({exc}) — run scripts/apply_state_schema.sh"
            ) from exc

    @property
    def group_id(self) -> str:
        """Return the consumer group this store holds the memory of."""
        return self._group_id

    def load(self, partition: int, order_id: str) -> OrderFold | None:
        """Return the fold for one order, reading through the cache on a miss.

        Raises:
            StateStoreUnavailable: If the read fails.
        """
        cached = self._cache.get(partition, {}).get(order_id)
        if cached is not None:
            return cached

        try:
            with self._conn.cursor() as cursor:
                cursor.execute(
                    _SELECT_FOLD, {"group_id": self._group_id, "order_id": order_id}
                )
                row = cursor.fetchone()
        except psycopg.Error as exc:
            raise StateStoreUnavailable(f"reading {order_id}: {exc}") from exc

        if row is None:
            return None

        last_sequence, state = row
        fold = OrderFold(
            order_id=order_id,
            last_sequence=last_sequence,
            state=OrderState(state) if state is not None else None,
        )
        self._cache.setdefault(partition, {})[order_id] = fold
        return fold

    def save(
        self, partition: int, order_id: str, fold: OrderFold, event_id: str
    ) -> SaveOutcome:
        """Write the fold under the sequence guard and count the delivery.

        Raises:
            StateStoreUnavailable: If the write fails or returns nothing.
        """
        params = {
            "group_id": self._group_id,
            "order_id": order_id,
            "sequence": fold.last_sequence,
            "state": str(fold.state) if fold.state is not None else None,
            "event_id": event_id,
        }
        try:
            with self._conn.cursor() as cursor:
                cursor.execute(_UPSERT_FOLD, params)
                row = cursor.fetchone()
        except psycopg.Error as exc:
            raise StateStoreUnavailable(f"writing {order_id}: {exc}") from exc

        if row is None:
            raise StateStoreUnavailable(f"writing {order_id}: upsert returned no row")

        _stored_sequence, handled_count, previous_sequence = row
        # "Applied" means the fold ADVANCED, not that the write succeeded. Comparing
        # against the stored sequence instead would call an exact redelivery a fresh
        # apply — GREATEST(n, n) is n, so the row looks untouched either way, and the
        # one case this feature exists to detect would be the one it missed.
        applied = fold.last_sequence > previous_sequence
        if applied:
            self._cache.setdefault(partition, {})[order_id] = fold
        return SaveOutcome(applied=applied, handled_count=handled_count)

    def forget(self, partitions: Iterable[int]) -> None:
        """Drop the cache for these partitions, and issue no ``DELETE`` (R3.9).

        This one line is the whole difference between 002 and 003 at a rebalance: the
        same callback fires and the same partitions are dropped, but the record survives
        because the record was never in the process.
        """
        for partition in set(partitions):
            self._cache.pop(partition, None)

    def held(self) -> list[int]:
        """Return the partitions currently cached."""
        return sorted(self._cache)

    def close(self) -> None:
        """Release the connection."""
        self._conn.close()
