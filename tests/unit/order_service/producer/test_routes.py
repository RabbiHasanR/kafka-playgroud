"""Unit tests for the order service HTTP routes.

The producer is mocked; ``store`` is a real :class:`OrderStore`. Each test
that expects a route to reject a request also asserts the producer was
*never called* — the ordering guarantees documented in ``routes.py`` (nothing
spent on a rejected request) are the point of testing this file at all.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from fastapi import HTTPException

from order_service.events import EventType, PaymentMethod
from order_service.producer.kafka_producer import DeliveryFailed, DeliveryResult, DeliveryTimeout
from order_service.producer.orders import OrderStore, UnknownOrder
from order_service.producer.routes import (
    CreateOrderRequest,
    PublishEventRequest,
    create_order,
    delete_order,
    publish_event,
)

if TYPE_CHECKING:
    from unittest.mock import MagicMock

    from tests.unit.order_service.producer.conftest import FakeRequest


class TestCreateOrder:
    def test_success_publishes_then_registers_then_snapshots(
        self,
        fake_request: FakeRequest,
        mock_producer: MagicMock,
        make_item,
        make_payment,
    ) -> None:
        mock_producer.publish_and_wait.return_value = DeliveryResult(partition=0, offset=1)
        body = CreateOrderRequest(
            customer_id="cust-1",
            items=[make_item(qty=1, unit_price=100)],
            payment=make_payment(amount=100, method=PaymentMethod.CARD),
        )

        response = create_order(body, fake_request)

        assert response.state == "CREATED"
        assert response.sequence == 1
        assert response.partition == 0
        assert response.offset == 1
        mock_producer.publish_and_wait.assert_called_once()
        mock_producer.publish_snapshot.assert_called_once()

    def test_payment_total_mismatch_is_rejected_before_publishing(
        self, fake_request: FakeRequest, mock_producer: MagicMock, make_item, make_payment
    ) -> None:
        body = CreateOrderRequest(
            customer_id="cust-1",
            items=[make_item(qty=1, unit_price=100)],
            payment=make_payment(amount=50),  # disagrees with the item sum
        )

        with pytest.raises(HTTPException) as exc_info:
            create_order(body, fake_request)

        assert exc_info.value.status_code == 422
        mock_producer.publish_and_wait.assert_not_called()

    def test_delivery_timeout_becomes_504(
        self, fake_request: FakeRequest, mock_producer: MagicMock, make_item, make_payment
    ) -> None:
        mock_producer.publish_and_wait.side_effect = DeliveryTimeout("no report")
        body = CreateOrderRequest(
            customer_id="cust-1",
            items=[make_item(qty=1, unit_price=100)],
            payment=make_payment(amount=100),
        )

        with pytest.raises(HTTPException) as exc_info:
            create_order(body, fake_request)

        assert exc_info.value.status_code == 504

    def test_delivery_failed_becomes_502(
        self, fake_request: FakeRequest, mock_producer: MagicMock, make_item, make_payment
    ) -> None:
        mock_producer.publish_and_wait.side_effect = DeliveryFailed("rejected")
        body = CreateOrderRequest(
            customer_id="cust-1",
            items=[make_item(qty=1, unit_price=100)],
            payment=make_payment(amount=100),
        )

        with pytest.raises(HTTPException) as exc_info:
            create_order(body, fake_request)

        assert exc_info.value.status_code == 502


class TestPublishEvent:
    def test_unknown_order_is_404(
        self, fake_request: FakeRequest, mock_producer: MagicMock
    ) -> None:
        body = PublishEventRequest(event_type=EventType.PACKED)

        with pytest.raises(HTTPException) as exc_info:
            publish_event("no-such-order", body, fake_request)

        assert exc_info.value.status_code == 404
        mock_producer.publish_and_wait.assert_not_called()

    def test_invalid_payload_is_422_before_a_sequence_is_reserved(
        self, fake_request: FakeRequest, mock_producer: MagicMock, registered_order_id: str, store: OrderStore
    ) -> None:
        body = PublishEventRequest(event_type=EventType.SHIPPED, payload={"carrier": "DHL"})

        with pytest.raises(HTTPException) as exc_info:
            publish_event(registered_order_id, body, fake_request)

        assert exc_info.value.status_code == 422
        mock_producer.publish_and_wait.assert_not_called()
        assert store.get(registered_order_id).last_sequence == 1

    def test_illegal_transition_without_force_is_409_and_does_not_publish(
        self, fake_request: FakeRequest, mock_producer: MagicMock, registered_order_id: str
    ) -> None:
        body = PublishEventRequest(
            event_type=EventType.SHIPPED,
            payload={"carrier": "DHL", "tracking_number": "1Z1"},
        )

        with pytest.raises(HTTPException) as exc_info:
            publish_event(registered_order_id, body, fake_request)

        assert exc_info.value.status_code == 409
        mock_producer.publish_and_wait.assert_not_called()

    def test_forced_illegal_transition_publishes_but_leaves_state_unchanged(
        self, fake_request: FakeRequest, mock_producer: MagicMock, registered_order_id: str
    ) -> None:
        mock_producer.publish_and_wait.return_value = DeliveryResult(partition=0, offset=2)
        body = PublishEventRequest(
            event_type=EventType.SHIPPED,
            payload={"carrier": "DHL", "tracking_number": "1Z1"},
            force=True,
        )

        response = publish_event(registered_order_id, body, fake_request)

        assert response.forced is True
        assert response.state == "CREATED"
        mock_producer.publish_and_wait.assert_called_once()

    def test_legal_transition_publishes_and_advances_state(
        self, fake_request: FakeRequest, mock_producer: MagicMock, registered_order_id: str
    ) -> None:
        mock_producer.publish_and_wait.return_value = DeliveryResult(partition=0, offset=2)
        body = PublishEventRequest(event_type=EventType.PACKED)

        response = publish_event(registered_order_id, body, fake_request)

        assert response.state == "PACKED"
        assert response.sequence == 2


class TestDeleteOrder:
    def test_unknown_order_is_404_and_no_tombstone_is_published(
        self, fake_request: FakeRequest, mock_producer: MagicMock
    ) -> None:
        with pytest.raises(HTTPException) as exc_info:
            delete_order("no-such-order", fake_request)

        assert exc_info.value.status_code == 404
        mock_producer.publish_tombstone.assert_not_called()

    def test_success_publishes_tombstone_and_removes_the_order(
        self,
        fake_request: FakeRequest,
        mock_producer: MagicMock,
        registered_order_id: str,
        store: OrderStore,
    ) -> None:
        mock_producer.publish_tombstone.return_value = DeliveryResult(partition=0, offset=3)

        response = delete_order(registered_order_id, fake_request)

        assert response.status_code == 204
        assert store.get(registered_order_id) is None

    def test_delivery_failure_leaves_the_order_intact(
        self,
        fake_request: FakeRequest,
        mock_producer: MagicMock,
        registered_order_id: str,
        store: OrderStore,
    ) -> None:
        mock_producer.publish_tombstone.side_effect = DeliveryFailed("rejected")

        with pytest.raises(HTTPException) as exc_info:
            delete_order(registered_order_id, fake_request)

        assert exc_info.value.status_code == 502
        assert store.get(registered_order_id) is not None

    def test_order_removed_by_a_concurrent_delete_after_the_tombstone_is_still_204(
        self,
        fake_request: FakeRequest,
        mock_producer: MagicMock,
        registered_order_id: str,
        store: OrderStore,
    ) -> None:
        mock_producer.publish_tombstone.return_value = DeliveryResult(partition=0, offset=3)

        def _remove_raced_away(order_id: str) -> None:
            raise UnknownOrder(order_id)

        # store.get() still finds the order; store.remove() races with another delete
        # that already won, exactly as it would if two requests overlapped.
        store.remove = _remove_raced_away

        response = delete_order(registered_order_id, fake_request)

        assert response.status_code == 204
