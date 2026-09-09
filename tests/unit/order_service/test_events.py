"""Unit tests for the shared event contract (specs/001, R1.5, R1.14)."""

import pytest

from order_service.events import (
    EventType,
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
