"""Broker-free unit tests for spec 002 (T23–T26).

Everything here runs without Kafka. The event contract, the order store, and the
consumer fold are all pure enough to test directly, which is why they were kept that
way — a test that needs a broker is a test that does not get run.
"""

import pytest
from pydantic import ValidationError

from order_service.consumer import analytics, inventory, notification
from order_service.consumer.main import SERVICE_REGISTRY, build_spec
from order_service.consumer.runtime import ViolationType, apply_event
from order_service.config import Settings
from order_service.events import (
    EventType,
    LifecycleEvent,
    OrderCreatedPayload,
    OrderItem,
    OrderState,
    PaymentInfo,
    PaymentMethod,
    ShippedPayload,
)
from order_service.producer.orders import (
    IllegalTransition,
    OrderStore,
    UnknownOrder,
    new_order_id,
)

ITEMS = [
    OrderItem(sku="SKU-1", qty=2, unit_price=15_000),
    OrderItem(sku="SKU-2", qty=1, unit_price=4_500),
]
TOTAL = 34_500


def _payment(amount: int = TOTAL) -> PaymentInfo:
    """Build a settled payment for a given amount.

    Args:
        amount: Amount paid in integer minor units.

    Returns:
        A :class:`PaymentInfo`.
    """
    return PaymentInfo(method=PaymentMethod.BKASH, reference="TRX123", amount=amount)


def _created_payload() -> OrderCreatedPayload:
    """Build a valid ``ORDER_CREATED`` payload.

    Returns:
        A payload whose total agrees with its items and its payment.
    """
    return OrderCreatedPayload(
        customer_id="cust-1", items=ITEMS, total_amount=TOTAL, payment=_payment()
    )


def _event(order_id: str, sequence: int, event_type: EventType, **payload: object):
    """Build a lifecycle event.

    Args:
        order_id: The order the event belongs to.
        sequence: The event's sequence number.
        event_type: The event's type.
        **payload: Payload fields, if the type needs any.

    Returns:
        The constructed :class:`LifecycleEvent`.
    """
    if event_type is EventType.ORDER_CREATED and not payload:
        body = _created_payload().model_dump(mode="json")
    elif event_type is EventType.SHIPPED and not payload:
        body = {"carrier": "Pathao", "tracking_number": "PT-1"}
    else:
        body = dict(payload)
    return LifecycleEvent(
        order_id=order_id, sequence=sequence, event_type=event_type, payload=body
    )


# -- T23: the event contract -------------------------------------------------


def test_total_must_equal_item_sum() -> None:
    """A total that disagrees with the items is rejected (R2.14)."""
    with pytest.raises(ValidationError, match="does not equal the item sum"):
        OrderCreatedPayload(
            customer_id="cust-1",
            items=ITEMS,
            total_amount=TOTAL + 1,
            payment=_payment(TOTAL + 1),
        )


def test_payment_must_equal_total() -> None:
    """A payment that disagrees with the total is rejected (R2.14)."""
    with pytest.raises(ValidationError, match="does not equal total_amount"):
        OrderCreatedPayload(
            customer_id="cust-1",
            items=ITEMS,
            total_amount=TOTAL,
            payment=_payment(TOTAL - 500),
        )


def test_order_needs_at_least_one_item() -> None:
    """An empty item list is rejected (R2.15)."""
    with pytest.raises(ValidationError):
        OrderCreatedPayload(
            customer_id="cust-1", items=[], total_amount=0, payment=_payment(0)
        )


def test_shipped_payload_needs_carrier_and_tracking() -> None:
    """A ``SHIPPED`` payload missing its carrier is rejected (R2.7)."""
    with pytest.raises(ValidationError):
        ShippedPayload(carrier="", tracking_number="PT-1")
    with pytest.raises(ValidationError):
        LifecycleEvent(
            order_id="ord-1",
            sequence=2,
            event_type=EventType.SHIPPED,
            payload={"carrier": "Pathao"},
        )


def test_every_event_gets_a_unique_id() -> None:
    """Each event carries its own ``event_id`` (R2.4)."""
    first = _event("ord-1", 2, EventType.PACKED)
    second = _event("ord-1", 3, EventType.SHIPPED)
    assert first.event_id != second.event_id


# -- T24: the order store ----------------------------------------------------


def _store_with_order() -> tuple[OrderStore, str]:
    """Build a store holding one freshly created order.

    Returns:
        The store and the order's id.
    """
    store = OrderStore()
    order_id = new_order_id()
    store.register(order_id, _created_payload())
    return store, order_id


def test_registered_order_starts_at_created_sequence_one() -> None:
    """A new order is ``CREATED`` at sequence 1 (R2.3, R2.13)."""
    store, order_id = _store_with_order()
    order = store.get(order_id)
    assert order is not None
    assert order.state is OrderState.CREATED
    assert order.last_sequence == 1
    assert order.total_amount == TOTAL


def test_sequences_increase_by_exactly_one() -> None:
    """Each reservation takes the next contiguous sequence (R2.3)."""
    store, order_id = _store_with_order()
    assert store.reserve(order_id, EventType.PACKED) == 2
    store.commit(order_id, EventType.PACKED)
    assert store.reserve(order_id, EventType.SHIPPED) == 3
    store.commit(order_id, EventType.SHIPPED)
    assert store.reserve(order_id, EventType.DELIVERED) == 4


def test_unknown_order_is_its_own_error() -> None:
    """Reserving against an unknown order raises ``UnknownOrder`` (R2.20)."""
    store = OrderStore()
    with pytest.raises(UnknownOrder):
        store.reserve("ord-nope", EventType.PACKED)


def test_illegal_transition_names_current_and_expected() -> None:
    """An out-of-order event is refused with both states named (R2.21)."""
    store, order_id = _store_with_order()
    with pytest.raises(IllegalTransition) as excinfo:
        store.reserve(order_id, EventType.SHIPPED)
    error = excinfo.value
    assert error.current is OrderState.CREATED
    assert error.requested is EventType.SHIPPED
    assert error.expected is EventType.PACKED


def test_completed_lifecycle_expects_nothing() -> None:
    """A delivered order refuses everything, with no expected successor (R2.21)."""
    store, order_id = _store_with_order()
    for event_type in (EventType.PACKED, EventType.SHIPPED, EventType.DELIVERED):
        store.reserve(order_id, event_type)
        store.commit(order_id, event_type)
    with pytest.raises(IllegalTransition) as excinfo:
        store.reserve(order_id, EventType.PACKED)
    assert excinfo.value.expected is None


def test_force_bypasses_the_guard_but_still_takes_a_sequence() -> None:
    """A forced reservation publishes out of order without a gap (R2.24, R2.26)."""
    store, order_id = _store_with_order()
    sequence = store.reserve(order_id, EventType.SHIPPED, force=True)
    assert sequence == 2


def test_force_does_not_advance_the_recorded_state() -> None:
    """A forced illegal event leaves the aggregate where it was (R2.24, D4)."""
    store, order_id = _store_with_order()
    store.reserve(order_id, EventType.SHIPPED, force=True)
    order = store.commit(order_id, EventType.SHIPPED, force=True)
    assert order.state is OrderState.CREATED
    assert order.last_sequence == 2


def test_force_on_a_legal_transition_still_advances() -> None:
    """``force`` is a bypass, not a veto — a legal event advances as normal."""
    store, order_id = _store_with_order()
    store.reserve(order_id, EventType.PACKED, force=True)
    order = store.commit(order_id, EventType.PACKED, force=True)
    assert order.state is OrderState.PACKED


# -- T25: the consumer fold --------------------------------------------------


def test_clean_chain_raises_no_violations() -> None:
    """The happy path folds without complaint (R2.38, R2.39)."""
    fold = None
    chain = [
        (1, EventType.ORDER_CREATED),
        (2, EventType.PACKED),
        (3, EventType.SHIPPED),
        (4, EventType.DELIVERED),
    ]
    for sequence, event_type in chain:
        fold, violations = apply_event(fold, _event("ord-1", sequence, event_type))
        assert violations == []
    assert fold is not None
    assert fold.state is OrderState.DELIVERED
    assert fold.last_sequence == 4


def test_out_of_order_event_is_an_illegal_transition_with_no_gap() -> None:
    """A forced publish produces one signal, not two (R2.26, R2.39)."""
    fold, _ = apply_event(None, _event("ord-1", 1, EventType.ORDER_CREATED))
    fold, violations = apply_event(fold, _event("ord-1", 2, EventType.SHIPPED))
    assert [v.type for v in violations] == [ViolationType.ILLEGAL_TRANSITION]
    assert violations[0].expected == str(EventType.PACKED)


def test_skipped_sequence_is_a_gap() -> None:
    """A missing sequence is detected independently of the transition (R2.38)."""
    fold, _ = apply_event(None, _event("ord-1", 1, EventType.ORDER_CREATED))
    _, violations = apply_event(fold, _event("ord-1", 5, EventType.PACKED))
    assert [v.type for v in violations] == [ViolationType.SEQUENCE_GAP]
    assert violations[0].expected == "2"
    assert violations[0].observed == "5"


def test_unseen_order_starting_above_one_is_a_gap() -> None:
    """Consumer amnesia looks exactly like a real gap (R2.38, D9)."""
    _, violations = apply_event(None, _event("ord-1", 7, EventType.PACKED))
    types = {v.type for v in violations}
    assert ViolationType.SEQUENCE_GAP in types
    assert ViolationType.ILLEGAL_TRANSITION in types


def test_folding_continues_after_a_violation() -> None:
    """A violation does not stop the fold advancing (R2.40)."""
    fold, _ = apply_event(None, _event("ord-1", 1, EventType.ORDER_CREATED))
    fold, _ = apply_event(fold, _event("ord-1", 9, EventType.DELIVERED))
    assert fold.last_sequence == 9
    assert fold.state is OrderState.DELIVERED


# -- T26: the service registry -----------------------------------------------


def test_every_service_resolves_to_a_distinct_group() -> None:
    """The three services never share a consumer group (R2.28, R2.37)."""
    settings = Settings(consumer_group_id=None)
    groups = {settings.group_id_for(name) for name in SERVICE_REGISTRY}
    assert groups == {"inventory-service", "notification-service", "analytics-service"}


def test_inventory_handles_two_event_types_only() -> None:
    """Inventory ignores ``PACKED`` and ``DELIVERED`` (R2.33)."""
    spec = build_spec(inventory.SERVICE_NAME)
    assert set(spec.handlers) == {EventType.ORDER_CREATED, EventType.SHIPPED}


def test_notification_and_analytics_handle_every_event_type() -> None:
    """Both react to all four types (R2.34, R2.35)."""
    for name in (notification.SERVICE_NAME, analytics.SERVICE_NAME):
        spec = build_spec(name)
        assert set(spec.handlers) == set(EventType)


def test_unknown_service_name_is_named_loudly() -> None:
    """A typo in ``SERVICE_NAME`` lists the valid options (R2.37)."""
    with pytest.raises(KeyError, match="unknown SERVICE_NAME"):
        build_spec("warehouse")


def test_analytics_counters_are_per_process() -> None:
    """Two built analytics services do not share a counter (D8)."""
    first = build_spec(analytics.SERVICE_NAME)
    second = build_spec(analytics.SERVICE_NAME)
    assert first.handlers[EventType.PACKED] is not second.handlers[EventType.PACKED]


def test_handlers_run_without_error_on_their_event_types() -> None:
    """Every registered handler accepts an event of its own type (R2.36)."""
    for name in SERVICE_REGISTRY:
        spec = build_spec(name)
        for sequence, event_type in enumerate(spec.handlers, start=1):
            spec.handlers[event_type](_event("ord-1", sequence, event_type))
