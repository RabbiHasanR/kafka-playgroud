"""The event contract shared by the order service and all three consumer services.

This is the only module every side imports. Keeping the schema and the lifecycle
transition table in one place means what is produced and what is validated cannot
drift apart across the four processes that share it.

Two properties of this contract are deliberate and worth reading twice.

**There is no ``PAID`` event (D3).** This is a *prepaid* flow: payment settles before
the order exists, so it is a field on ``ORDER_CREATED`` rather than a step in the
chain. The lifecycle is ``CREATED → PACKED → SHIPPED → DELIVERED``.

**A wrong total cannot be represented.** :class:`OrderCreatedPayload` rejects a
``total_amount`` that disagrees with the items or with the payment, so the check
happens at the API boundary and the bad event never exists. The alternative — publish
it and let each consumer fold the items and compare — fixes nothing once, and has to be
got right in every consumer separately. There are three of them here.

Money is carried as **integer minor units** (paisa/cents), never floats, so the
equality checks above have exactly one possible cause of failure.
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
    """How a prepaid order was paid for.

    Every member is a *prepaid* method by definition — cash on delivery has no place
    in this contract, because an order that has not been paid for cannot carry a
    settled payment on its ``ORDER_CREATED`` event.
    """

    CARD = "CARD"
    BKASH = "BKASH"
    NAGAD = "NAGAD"


#: The one legal predecessor state per event type (R1.5).
#:
#: ``None`` means "the order does not exist yet", so ``ORDER_CREATED`` is the only
#: event that may legally arrive for an unknown order. A single value rather than a
#: set, because the prepaid chain is strictly linear with no repeatable step.
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

#: The inverse of :data:`LEGAL_PREDECESSOR`: which event a given state expects next.
#:
#: Derived rather than written out, so the two can never disagree. ``DELIVERED`` is
#: absent because the chain ends there — an order in that state expects nothing.
EXPECTED_NEXT_EVENT: dict[OrderState | None, EventType] = {
    predecessor: event_type for event_type, predecessor in LEGAL_PREDECESSOR.items()
}


class OrderItem(BaseModel):
    """One line item of an order (R1.6).

    Attributes:
        sku: Stock-keeping unit of the line item.
        qty: Quantity ordered; must be positive.
        unit_price: Price per unit in integer minor units.
    """

    sku: str = Field(min_length=1)
    qty: int = Field(gt=0)
    unit_price: int = Field(gt=0)

    @property
    def line_total(self) -> int:
        """Return the line's contribution to the order total.

        Returns:
            ``qty * unit_price`` in integer minor units — exact, never rounded.
        """
        return self.qty * self.unit_price


class PaymentInfo(BaseModel):
    """A settled payment carried on an ``ORDER_CREATED`` event (R1.6).

    Attributes:
        method: How the customer paid.
        reference: The payment processor's identifier for the transaction.
        amount: Amount paid in integer minor units.
    """

    method: PaymentMethod
    reference: str = Field(min_length=1)
    amount: int = Field(ge=0)


class OrderCreatedPayload(BaseModel):
    """Payload of an ``ORDER_CREATED`` event (R1.6).

    Attributes:
        customer_id: Who placed the order.
        items: The order's line items; at least one is required.
        total_amount: Order total in integer minor units.
        payment: The settled payment.
    """

    customer_id: str = Field(min_length=1)
    items: list[OrderItem] = Field(min_length=1)
    total_amount: int
    payment: PaymentInfo

    @model_validator(mode="after")
    def _validate_totals(self) -> "OrderCreatedPayload":
        """Check the total against the items and against the payment (R1.14).

        Doing it here means a disagreeing total is a rejected request rather than a
        published event that every downstream service has to notice independently.

        Returns:
            The validated payload.

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
    """Payload of a ``SHIPPED`` event (R1.7).

    Attributes:
        carrier: Who is carrying the parcel.
        tracking_number: The carrier's tracking identifier.
    """

    carrier: str = Field(min_length=1)
    tracking_number: str = Field(min_length=1)


#: Event types whose payload has a defined shape. Types absent from this map carry an
#: empty payload — ``PACKED`` and ``DELIVERED`` are facts, not data.
PAYLOAD_MODELS: dict[EventType, type[BaseModel]] = {
    EventType.ORDER_CREATED: OrderCreatedPayload,
    EventType.SHIPPED: ShippedPayload,
}


def utc_now() -> datetime:
    """Return the current time as a timezone-aware UTC timestamp.

    Returns:
        The current UTC time.
    """
    return datetime.now(timezone.utc)


def new_event_id() -> str:
    """Return a fresh globally unique event identifier (R1.4).

    Returns:
        A UUID4 string.
    """
    return str(uuid4())


def validate_payload(event_type: EventType, payload: dict[str, Any]) -> BaseModel | None:
    """Validate a payload against the model registered for its event type.

    Exposed separately from :class:`LifecycleEvent` so a route can reject a malformed
    payload *before* it reserves a sequence number, rather than burning one on a
    request that was never going to succeed.

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
        event_id: Globally unique identity of this event. Nothing reads it yet; it is
            the natural deduplication key once 003 and 008 make duplicates matter, and
            a stable handle in logs before then (D11).
        order_id: Identity of the order. Also the Kafka message key, which is what
            routes all of an order's events to one partition (R1.10).
        sequence: Position of this event within its order, starting at 1 and
            increasing by exactly 1 (R1.3).
        event_type: Which lifecycle event this is.
        occurred_at: When the order service emitted the event.
        payload: Event-type-specific data, validated against :data:`PAYLOAD_MODELS`.
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

        Returns:
            The validated event.

        Raises:
            ValueError: If the payload does not match its event type's schema.
        """
        validate_payload(self.event_type, self.payload)
        return self

    def as_order_created(self) -> OrderCreatedPayload:
        """Return the payload typed as an ``ORDER_CREATED`` payload.

        Returns:
            The parsed :class:`OrderCreatedPayload`.

        Raises:
            ValueError: If this event is not an ``ORDER_CREATED`` event.
        """
        if self.event_type is not EventType.ORDER_CREATED:
            raise ValueError(f"{self.event_type} is not ORDER_CREATED")
        return OrderCreatedPayload.model_validate(self.payload)

    def as_shipped(self) -> ShippedPayload:
        """Return the payload typed as a ``SHIPPED`` payload.

        Returns:
            The parsed :class:`ShippedPayload`.

        Raises:
            ValueError: If this event is not a ``SHIPPED`` event.
        """
        if self.event_type is not EventType.SHIPPED:
            raise ValueError(f"{self.event_type} is not SHIPPED")
        return ShippedPayload.model_validate(self.payload)


def is_legal_transition(event_type: EventType, current: OrderState | None) -> bool:
    """Report whether an event may legally follow the given lifecycle state.

    Args:
        event_type: The incoming event's type.
        current: The order's current state, or ``None`` if the order is unknown.

    Returns:
        ``True`` if the transition is permitted by :data:`LEGAL_PREDECESSOR`.
    """
    return LEGAL_PREDECESSOR[event_type] == current
