"""Fixtures shared across order_service unit tests."""

import pytest

from order_service.events import OrderItem, PaymentInfo, PaymentMethod


@pytest.fixture
def make_item():
    """Return a factory for an :class:`OrderItem` with sane defaults."""

    def _make(sku: str = "sku-1", qty: int = 1, unit_price: int = 100) -> OrderItem:
        return OrderItem(sku=sku, qty=qty, unit_price=unit_price)

    return _make


@pytest.fixture
def make_payment():
    """Return a factory for a :class:`PaymentInfo` matching a given amount."""

    def _make(
        amount: int,
        method: PaymentMethod = PaymentMethod.CARD,
        reference: str = "ref-1",
    ) -> PaymentInfo:
        return PaymentInfo(method=method, reference=reference, amount=amount)

    return _make
