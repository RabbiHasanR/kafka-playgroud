"""Unit tests for the shared event contract (specs/001, R1.5, R1.14)."""

import pytest

from order_service.events import (
    EventType,
    LifecycleEvent,
    OrderCreatedPayload,
    OrderState,
    is_legal_transition,
    validate_payload,
)


class TestIsLegalTransition:
    """R1.5 — each event type has exactly one legal predecessor state."""

    @pytest.mark.parametrize(
        ("event_type", "current"),
        [
            (EventType.ORDER_CREATED, None),
            (EventType.PACKED, OrderState.CREATED),
            (EventType.SHIPPED, OrderState.PACKED),
            (EventType.DELIVERED, OrderState.SHIPPED),
        ],
    )
    def test_legal_successor_is_accepted(
        self, event_type: EventType, current: OrderState | None
    ) -> None:
        assert is_legal_transition(event_type, current) is True

    def test_illegal_successor_is_rejected(self) -> None:
        assert is_legal_transition(EventType.SHIPPED, OrderState.CREATED) is False


class TestOrderCreatedPayloadValidation:
    """R1.14 — total_amount must agree with both the items and the payment."""

    def test_matching_totals_are_accepted(self, make_item, make_payment) -> None:
        items = [make_item(qty=2, unit_price=100)]
        payload = OrderCreatedPayload(
            customer_id="cust-1",
            items=items,
            total_amount=200,
            payment=make_payment(amount=200),
        )
        assert payload.total_amount == 200

    def test_total_disagreeing_with_items_is_rejected(self, make_item, make_payment) -> None:
        items = [make_item(qty=2, unit_price=100)]
        with pytest.raises(ValueError, match="does not equal the item sum"):
            OrderCreatedPayload(
                customer_id="cust-1",
                items=items,
                total_amount=999,
                payment=make_payment(amount=999),
            )

    def test_payment_disagreeing_with_total_is_rejected(self, make_item, make_payment) -> None:
        items = [make_item(qty=2, unit_price=100)]
        with pytest.raises(ValueError, match="does not equal total_amount"):
            OrderCreatedPayload(
                customer_id="cust-1",
                items=items,
                total_amount=200,
                payment=make_payment(amount=150),
            )


class TestValidatePayload:
    """Payloads validate against the model registered for their event type."""

    def test_event_type_without_a_schema_returns_none(self) -> None:
        assert validate_payload(EventType.PACKED, {}) is None

    def test_malformed_shipped_payload_is_rejected(self) -> None:
        with pytest.raises(ValueError):
            validate_payload(EventType.SHIPPED, {"carrier": "DHL"})  # missing tracking_number

    def test_well_formed_shipped_payload_is_accepted(self) -> None:
        result = validate_payload(
            EventType.SHIPPED, {"carrier": "DHL", "tracking_number": "1Z1"}
        )
        assert result is not None


class TestLifecycleEvent:
    """The envelope validates its own payload shape and exposes typed accessors."""

    def test_valid_order_created_event_constructs(self, make_item, make_payment) -> None:
        payload = OrderCreatedPayload(
            customer_id="cust-1",
            items=[make_item()],
            total_amount=100,
            payment=make_payment(amount=100),
        )
        event = LifecycleEvent(
            order_id="ord-1",
            sequence=1,
            event_type=EventType.ORDER_CREATED,
            payload=payload.model_dump(),
        )
        assert event.order_id == "ord-1"

    def test_payload_not_matching_event_type_is_rejected(self) -> None:
        with pytest.raises(ValueError):
            LifecycleEvent(
                order_id="ord-1",
                sequence=1,
                event_type=EventType.SHIPPED,
                payload={"carrier": "DHL"},  # missing tracking_number
            )

    def test_as_order_created_returns_typed_payload(self, make_item, make_payment) -> None:
        payload = OrderCreatedPayload(
            customer_id="cust-1",
            items=[make_item()],
            total_amount=100,
            payment=make_payment(amount=100),
        )
        event = LifecycleEvent(
            order_id="ord-1",
            sequence=1,
            event_type=EventType.ORDER_CREATED,
            payload=payload.model_dump(),
        )
        assert event.as_order_created().customer_id == "cust-1"

    def test_as_order_created_rejects_wrong_event_type(self) -> None:
        event = LifecycleEvent(order_id="ord-1", sequence=2, event_type=EventType.PACKED)
        with pytest.raises(ValueError, match="is not ORDER_CREATED"):
            event.as_order_created()

    def test_as_shipped_returns_typed_payload(self) -> None:
        event = LifecycleEvent(
            order_id="ord-1",
            sequence=3,
            event_type=EventType.SHIPPED,
            payload={"carrier": "DHL", "tracking_number": "1Z1"},
        )
        assert event.as_shipped().carrier == "DHL"

    def test_as_shipped_rejects_wrong_event_type(self) -> None:
        event = LifecycleEvent(order_id="ord-1", sequence=2, event_type=EventType.PACKED)
        with pytest.raises(ValueError, match="is not SHIPPED"):
            event.as_shipped()
