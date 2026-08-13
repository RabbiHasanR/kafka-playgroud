"""The event contract shared by the order service and all three consumer services.

The only module every side imports, so the schema and the transition table cannot
drift apart. Money is carried as integer minor units (paisa/cents), never floats.
"""

from datetime import datetime, timezone
from enum import StrEnum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field, model_validator


class EventType(StrEnum):
    """The four order lifecycle event types (R1.2)."""

    ORDER_CREATED = "ORDER_CREATED"
    PACKED = "PACKED"
    SHIPPED = "SHIPPED"
    DELIVERED = "DELIVERED"


class OrderState(StrEnum):
    """The lifecycle state an order is in after applying an event."""

    CREATED = "CREATED"
    PACKED = "PACKED"
    SHIPPED = "SHIPPED"
    DELIVERED = "DELIVERED"


class PaymentMethod(StrEnum):
    """How a prepaid order was paid for. Every member settles before the order."""

    CARD = "CARD"
    BKASH = "BKASH"
    NAGAD = "NAGAD"


#: The one legal predecessor state per event type (R1.5). ``None`` means "the order
#: does not exist yet", so ORDER_CREATED is the only event legal for an unknown order.
LEGAL_PREDECESSOR: dict[EventType, OrderState | None] = {
    EventType.ORDER_CREATED: None,
    EventType.PACKED: OrderState.CREATED,
    EventType.SHIPPED: OrderState.PACKED,
    EventType.DELIVERED: OrderState.SHIPPED,
}

#: The state an order is in after an event of each type is applied.
RESULTING_STATE: dict[EventType, OrderState] = {
    EventType.ORDER_CREATED: OrderState.CREATED,
    EventType.PACKED: OrderState.PACKED,
    EventType.SHIPPED: OrderState.SHIPPED,
    EventType.DELIVERED: OrderState.DELIVERED,
}

#: Inverse of LEGAL_PREDECESSOR — which event a given state expects next. Derived, so
#: the two cannot disagree. DELIVERED is absent: the chain ends there.
EXPECTED_NEXT_EVENT: dict[OrderState | None, EventType] = {
    predecessor: event_type for event_type, predecessor in LEGAL_PREDECESSOR.items()
}


class OrderItem(BaseModel):
    """One line item of an order (R1.6)."""

    sku: str = Field(min_length=1)
    qty: int = Field(gt=0)
    unit_price: int = Field(gt=0)

    @property
    def line_total(self) -> int:
        """Return ``qty * unit_price`` in integer minor units."""
        return self.qty * self.unit_price


class PaymentInfo(BaseModel):
    """A settled payment carried on an ``ORDER_CREATED`` event (R1.6)."""

    method: PaymentMethod
    reference: str = Field(min_length=1)
    amount: int = Field(ge=0)


class OrderCreatedPayload(BaseModel):
    """Payload of an ``ORDER_CREATED`` event (R1.6)."""

    customer_id: str = Field(min_length=1)
    items: list[OrderItem] = Field(min_length=1)
    total_amount: int
    payment: PaymentInfo

    @model_validator(mode="after")
    def _validate_totals(self) -> "OrderCreatedPayload":
        """Check the total against the items and the payment (R1.14).

        Checking here means a disagreeing total is a rejected request rather than a
        published event every consumer has to notice separately.

        Raises:
            ValueError: If the total disagrees with the item sum or the payment.
        """
        item_sum = sum(item.line_total for item in self.items)
        if self.total_amount != item_sum:
            raise ValueError(
                f"total_amount {self.total_amount} does not equal the item sum "
                f"{item_sum}"
            )
        if self.payment.amount != self.total_amount:
            raise ValueError(
                f"payment.amount {self.payment.amount} does not equal total_amount "
                f"{self.total_amount}"
            )
        return self


class ShippedPayload(BaseModel):
    """Payload of a ``SHIPPED`` event (R1.7)."""

    carrier: str = Field(min_length=1)
    tracking_number: str = Field(min_length=1)


#: Event types whose payload has a defined shape. Absent types carry an empty payload.
PAYLOAD_MODELS: dict[EventType, type[BaseModel]] = {
    EventType.ORDER_CREATED: OrderCreatedPayload,
    EventType.SHIPPED: ShippedPayload,
}


def utc_now() -> datetime:
    """Return the current time as a timezone-aware UTC timestamp."""
    return datetime.now(timezone.utc)


def new_event_id() -> str:
    """Return a fresh UUID4 event identifier (R1.4)."""
    return str(uuid4())


def validate_payload(event_type: EventType, payload: dict[str, Any]) -> BaseModel | None:
    """Validate a payload against the model registered for its event type.

    Exposed separately from :class:`LifecycleEvent` so a route can reject a malformed
    payload *before* it reserves a sequence number.

    Args:
        event_type: The event type the payload belongs to.
        payload: The raw payload.

    Returns:
        The parsed payload model, or ``None`` for event types that carry no data.

    Raises:
        ValueError: If the payload does not match its event type's schema.
    """
    model = PAYLOAD_MODELS.get(event_type)
    if model is None:
        return None
    return model.model_validate(payload)


class LifecycleEvent(BaseModel):
    """A single order lifecycle event (R1.1).

    Attributes:
        order_id: Also the Kafka message key, which is what routes all of an order's
            events to one partition (R1.10).
        sequence: Position within its order, starting at 1 and increasing by 1 (R1.3).
    """

    event_id: str = Field(default_factory=new_event_id)
    order_id: str
    sequence: int = Field(ge=1)
    event_type: EventType
    occurred_at: datetime = Field(default_factory=utc_now)
    payload: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_payload_shape(self) -> "LifecycleEvent":
        """Validate the payload against the model registered for this event type.

        Raises:
            ValueError: If the payload does not match its event type's schema.
        """
        validate_payload(self.event_type, self.payload)
        return self

    def as_order_created(self) -> OrderCreatedPayload:
        """Return the payload typed as an ``ORDER_CREATED`` payload.

        Raises:
            ValueError: If this event is not an ``ORDER_CREATED`` event.
        """
        if self.event_type is not EventType.ORDER_CREATED:
            raise ValueError(f"{self.event_type} is not ORDER_CREATED")
        return OrderCreatedPayload.model_validate(self.payload)

    def as_shipped(self) -> ShippedPayload:
        """Return the payload typed as a ``SHIPPED`` payload.

        Raises:
            ValueError: If this event is not a ``SHIPPED`` event.
        """
        if self.event_type is not EventType.SHIPPED:
            raise ValueError(f"{self.event_type} is not SHIPPED")
        return ShippedPayload.model_validate(self.payload)


def is_legal_transition(event_type: EventType, current: OrderState | None) -> bool:
    """Report whether an event may legally follow the given lifecycle state."""
    return LEGAL_PREDECESSOR[event_type] == current
