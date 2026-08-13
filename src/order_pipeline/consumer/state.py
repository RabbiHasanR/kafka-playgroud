"""Per-order folded state and violation detection.

The fold is a **pure function** of ``(current_state, event) -> (new_state,
violations)``. That keeps it inspectable in isolation, and it means spec 004
can re-host the same logic on a durable store by changing only where the state comes
from and goes to — not how it is computed.

Three independent violation signals are produced here, and the overlap is the point:

* **Sequence gap** — mechanical, needs no domain knowledge.
* **Illegal transition** — domain-level, uses the shared transition table.
* **Total mismatch** — a true accumulator. This is the one that cannot be faked:
  a consumer with no prior state has no total at all, so the folded value is the
  only thing that can disagree with the ``PAID`` amount.

Note that the sequence check cannot distinguish a genuine producer-side gap from
the consumer's own amnesia after a restart. That ambiguity is not a flaw to be
engineered away — it is the evidence that a committed offset is a position and not
a memory (R1.27).
"""

import threading
from dataclasses import dataclass, replace
from enum import StrEnum

from order_pipeline.events import (
    LEGAL_PREDECESSORS,
    RESULTING_STATE,
    EventType,
    OrderEvent,
    OrderState,
    is_legal_transition,
)


class ViolationType(StrEnum):
    """The kinds of ordering violation the consumer can detect."""

    SEQUENCE_GAP = "SEQUENCE_GAP"
    ILLEGAL_TRANSITION = "ILLEGAL_TRANSITION"
    TOTAL_MISMATCH = "TOTAL_MISMATCH"


@dataclass(frozen=True)
class Violation:
    """A single detected violation (R1.23).

    Attributes:
        type: Which check failed.
        order_id: The order the failing event belongs to.
        sequence: Sequence of the failing event.
        expected: What the consumer expected to see.
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
    """The consumer's accumulated knowledge about one order (R1.18).

    Attributes:
        order_id: The order this describes.
        last_sequence: Sequence of the most recent event applied.
        state: Lifecycle state after that event.
        item_count: Number of ``ITEM_ADDED`` events folded in.
        total: Running sum of ``qty * unit_price`` in integer minor units.
    """

    order_id: str
    last_sequence: int = 0
    state: OrderState | None = None
    item_count: int = 0
    total: int = 0

    def as_dict(self) -> dict[str, object]:
        """Return a JSON-serialisable view of this fold.

        Returns:
            The fold's fields, with the state rendered as a plain string.
        """
        return {
            "order_id": self.order_id,
            "last_sequence": self.last_sequence,
            "state": str(self.state) if self.state is not None else None,
            "item_count": self.item_count,
            "total": self.total,
        }


def apply_event(
    current: OrderFold | None, event: OrderEvent
) -> tuple[OrderFold, list[Violation]]:
    """Fold one event into an order's state, reporting any violations.

    The event is applied even when it violates an expectation, so that consumption
    continues rather than halting (R1.22) and a single bad event does not poison
    every later one.

    Args:
        current: The order's existing fold, or ``None`` if the order is unknown.
        event: The event to apply.

    Returns:
        The updated fold and the violations this event triggered.
    """
    fold = current if current is not None else OrderFold(order_id=event.order_id)
    violations: list[Violation] = []

    # -- sequence contiguity (R1.19, R1.24) ------------------------------------
    # For an unknown order last_sequence is 0, so the expected sequence is 1 and
    # anything else is a gap. This is the same code path that fires after a
    # consumer restart, which is precisely why the two are indistinguishable.
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

    # -- lifecycle legality (R1.20) --------------------------------------------
    if not is_legal_transition(event.event_type, fold.state):
        violations.append(
            Violation(
                type=ViolationType.ILLEGAL_TRANSITION,
                order_id=event.order_id,
                sequence=event.sequence,
                expected=f"{event.event_type} after one of "
                f"{_legal_predecessors_label(event.event_type)}",
                observed=f"{event.event_type} after {fold.state}",
            )
        )

    # -- accumulate (R1.18) ----------------------------------------------------
    item_count = fold.item_count
    total = fold.total
    if event.event_type is EventType.ITEM_ADDED:
        item = event.as_item_added()
        item_count += 1
        total += item.line_total

    # -- total agreement at payment (R1.21) ------------------------------------
    if event.event_type is EventType.PAID:
        paid = event.as_paid()
        if paid.amount != total:
            violations.append(
                Violation(
                    type=ViolationType.TOTAL_MISMATCH,
                    order_id=event.order_id,
                    sequence=event.sequence,
                    expected=str(total),
                    observed=str(paid.amount),
                )
            )

    updated = replace(
        fold,
        last_sequence=event.sequence,
        state=RESULTING_STATE[event.event_type],
        item_count=item_count,
        total=total,
    )
    return updated, violations


def _legal_predecessors_label(event_type: EventType) -> str:
    """Render an event type's legal predecessor states for a log message.

    Args:
        event_type: The event type to describe.

    Returns:
        A comma-separated list of legal predecessor states.
    """
    return ", ".join(
        sorted(str(state) for state in LEGAL_PREDECESSORS[event_type])
    )


class OrderStateStore:
    """In-memory store of every order's fold.

    **This deliberately has no persistence** (R1.27, X3). Restarting the consumer
    loses every fold while Kafka faithfully restores the committed offset — which
    is the entire point of the feature. Do not add a durable backing store here;
    spec 004 does that, and it needs this failure to motivate it.
    """

    def __init__(self) -> None:
        """Initialise an empty store."""
        self._folds: dict[str, OrderFold] = {}
        self._violations: list[Violation] = []
        # The consume loop writes; the /state HTTP server reads from its own thread.
        self._lock = threading.Lock()

    def apply(self, event: OrderEvent) -> list[Violation]:
        """Fold an event into the store.

        Args:
            event: The event to apply.

        Returns:
            The violations this event triggered.
        """
        with self._lock:
            updated, violations = apply_event(self._folds.get(event.order_id), event)
            self._folds[event.order_id] = updated
            self._violations.extend(violations)
        return violations

    def snapshot(self) -> dict[str, object]:
        """Return the current state of every known order (R1.31).

        Returns:
            Order folds, violation count, and recent violations.
        """
        with self._lock:
            return {
                "order_count": len(self._folds),
                "violation_count": len(self._violations),
                "orders": [fold.as_dict() for fold in self._folds.values()],
                "recent_violations": [
                    {
                        "type": str(v.type),
                        "order_id": v.order_id,
                        "sequence": v.sequence,
                        "expected": v.expected,
                        "observed": v.observed,
                    }
                    for v in self._violations[-50:]
                ],
            }
