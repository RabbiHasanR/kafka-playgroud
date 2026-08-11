"""Unit tests for the pure fold (T22).

No broker involved — that is the payoff of keeping ``apply_event`` a pure function
of ``(state, event) -> (state, violations)``.
"""

import pytest

from order_pipeline.consumer.state import (
    OrderFold,
    ViolationType,
    apply_event,
)
from order_pipeline.events import EventType, OrderEvent, OrderState


def event(
    sequence: int,
    event_type: EventType,
    payload: dict | None = None,
    order_id: str = "ord-1",
) -> OrderEvent:
    """Build an event for a test case.

    Args:
        sequence: Sequence number to assign.
        event_type: The event's type.
        payload: Event-type-specific payload.
        order_id: Order the event belongs to.

    Returns:
        The constructed event.
    """
    return OrderEvent(
        order_id=order_id,
        sequence=sequence,
        event_type=event_type,
        payload=payload or {},
    )


def fold_all(events: list[OrderEvent]) -> tuple[OrderFold | None, list]:
    """Fold a list of events in order.

    Args:
        events: Events to apply, in delivery order.

    Returns:
        The final fold and every violation raised along the way.
    """
    state: OrderFold | None = None
    violations: list = []
    for evt in events:
        state, new = apply_event(state, evt)
        violations.extend(new)
    return state, violations


def happy_path() -> list[OrderEvent]:
    """Build a clean two-item lifecycle totalling 700.

    Returns:
        The order's events in lifecycle order.
    """
    return [
        event(1, EventType.ORDER_CREATED),
        event(2, EventType.ITEM_ADDED, {"sku": "A", "qty": 2, "unit_price": 100}),
        event(3, EventType.ITEM_ADDED, {"sku": "B", "qty": 1, "unit_price": 500}),
        event(4, EventType.PAID, {"amount": 700}),
        event(5, EventType.PACKED),
        event(6, EventType.SHIPPED),
        event(7, EventType.DELIVERED),
    ]


def test_happy_path_produces_no_violations() -> None:
    """A correctly ordered lifecycle folds cleanly."""
    state, violations = fold_all(happy_path())

    assert violations == []
    assert state is not None
    assert state.state is OrderState.DELIVERED
    assert state.item_count == 2
    assert state.total == 700
    assert state.last_sequence == 7


def test_sequence_gap_is_detected() -> None:
    """A skipped sequence raises exactly one gap violation."""
    events = happy_path()
    del events[2]  # drop sequence 3, so 4 follows 2

    _, violations = fold_all(events)

    gaps = [v for v in violations if v.type is ViolationType.SEQUENCE_GAP]
    assert len(gaps) == 1
    assert gaps[0].expected == "3"
    assert gaps[0].observed == "4"


def test_unknown_order_not_starting_at_one_is_a_violation() -> None:
    """An unseen order whose first event is not sequence 1 is flagged (R1.24).

    This is the same code path that fires after a consumer restart, which is why a
    genuine gap and the consumer's own amnesia are indistinguishable.
    """
    _, violations = apply_event(None, event(4, EventType.PACKED))

    gaps = [v for v in violations if v.type is ViolationType.SEQUENCE_GAP]
    assert len(gaps) == 1
    assert gaps[0].expected == "1"
    assert gaps[0].observed == "4"


def test_illegal_transition_is_detected() -> None:
    """Shipping before packing is flagged even with contiguous sequences."""
    events = [
        event(1, EventType.ORDER_CREATED),
        event(2, EventType.PAID, {"amount": 0}),
        event(3, EventType.SHIPPED),  # legal only after PACKED
    ]

    _, violations = fold_all(events)

    illegal = [v for v in violations if v.type is ViolationType.ILLEGAL_TRANSITION]
    assert len(illegal) == 1
    assert illegal[0].sequence == 3


def test_total_mismatch_is_detected() -> None:
    """A PAID amount disagreeing with the folded total is flagged."""
    events = happy_path()
    events[3] = event(4, EventType.PAID, {"amount": 999})

    _, violations = fold_all(events)

    mismatches = [v for v in violations if v.type is ViolationType.TOTAL_MISMATCH]
    assert len(mismatches) == 1
    assert mismatches[0].expected == "700"
    assert mismatches[0].observed == "999"


def test_lost_state_produces_a_wrong_total() -> None:
    """Folding only the tail of an order yields a wrong total, not a warning.

    This is the mechanical form of the restart-amnesia experiment (T29): the items
    sit before the consumer's committed offset, so they are never re-read and the
    total can only come out low. No sequence check can recover them — the value is
    simply gone.
    """
    events = happy_path()
    resumed_from_paid = events[3:]

    state, violations = fold_all(resumed_from_paid)

    assert state is not None
    assert state.total == 0  # the two ITEM_ADDED events were never seen
    mismatches = [v for v in violations if v.type is ViolationType.TOTAL_MISMATCH]
    assert len(mismatches) == 1
    assert mismatches[0].expected == "0"
    assert mismatches[0].observed == "700"


def test_folding_continues_after_a_violation() -> None:
    """A violation does not stop later events from being applied (R1.22)."""
    events = [
        event(1, EventType.ORDER_CREATED),
        event(5, EventType.ITEM_ADDED, {"sku": "A", "qty": 1, "unit_price": 250}),
        event(6, EventType.ITEM_ADDED, {"sku": "B", "qty": 1, "unit_price": 250}),
    ]

    state, violations = fold_all(events)

    assert violations  # the jump from 1 to 5 was flagged
    assert state is not None
    assert state.item_count == 2
    assert state.total == 500


@pytest.mark.parametrize(
    ("qty", "unit_price"),
    [(1, 1), (3, 333), (7, 99_99), (2, 1_000_000)],
)
def test_totals_are_exact_integers(qty: int, unit_price: int) -> None:
    """Integer minor units keep the total exact for any input (D4).

    Floats would make a total-mismatch violation ambiguous between broken ordering
    and representation drift, which would make the signal useless.
    """
    events = [
        event(1, EventType.ORDER_CREATED),
        event(2, EventType.ITEM_ADDED, {"sku": "A", "qty": qty, "unit_price": unit_price}),
        event(3, EventType.PAID, {"amount": qty * unit_price}),
    ]

    state, violations = fold_all(events)

    assert violations == []
    assert state is not None
    assert state.total == qty * unit_price
