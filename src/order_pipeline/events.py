"""The event contract shared by the producer and the consumer.

This is the only module both sides import. Keeping the schema and the lifecycle
transition table in one place means what is produced and what is validated cannot
drift apart (D10).

Money is carried as **integer minor units** (paisa/cents), never floats (D4). A
float total would give a total-mismatch violation two possible causes — broken
ordering, or floating-point drift — and a signal with two causes proves nothing.
"""

from datetime import datetime, timezone
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field, model_validator


class EventType(StrEnum):
    """The six order lifecycle event types (R1.3)."""

    ORDER_CREATED = "ORDER_CREATED"
    ITEM_ADDED = "ITEM_ADDED"
    PAID = "PAID"
    PACKED = "PACKED"
    SHIPPED = "SHIPPED"
    DELIVERED = "DELIVERED"


class OrderState(StrEnum):
    """The lifecycle state an order is in after applying an event."""

    CREATED = "CREATED"
    PAID = "PAID"
    PACKED = "PACKED"
    SHIPPED = "SHIPPED"
    DELIVERED = "DELIVERED"


#: Legal predecessor states per event type (R1.5, D10).
#:
#: ``None`` means "the order does not exist yet", so ``ORDER_CREATED`` is the only
#: event that may legally arrive for an unknown order. ``ITEM_ADDED`` is repeatable
#: and leaves the order in ``CREATED``.
LEGAL_PREDECESSORS: dict[EventType, frozenset[OrderState | None]] = {
    EventType.ORDER_CREATED: frozenset({None}),
    EventType.ITEM_ADDED: frozenset({OrderState.CREATED}),
    EventType.PAID: frozenset({OrderState.CREATED}),
    EventType.PACKED: frozenset({OrderState.PAID}),
    EventType.SHIPPED: frozenset({OrderState.PACKED}),
    EventType.DELIVERED: frozenset({OrderState.SHIPPED}),
}

#: The state an order is in after an event of each type is applied.
#:
#: ``ITEM_ADDED`` maps back to ``CREATED`` because adding items does not advance the
#: lifecycle — that is what makes it repeatable.
RESULTING_STATE: dict[EventType, OrderState] = {
    EventType.ORDER_CREATED: OrderState.CREATED,
    EventType.ITEM_ADDED: OrderState.CREATED,
    EventType.PAID: OrderState.PAID,
    EventType.PACKED: OrderState.PACKED,
    EventType.SHIPPED: OrderState.SHIPPED,
    EventType.DELIVERED: OrderState.DELIVERED,
}

#: The happy-path lifecycle, excluding the repeatable ``ITEM_ADDED``.
LIFECYCLE_CHAIN: tuple[EventType, ...] = (
    EventType.ORDER_CREATED,
    EventType.PAID,
    EventType.PACKED,
    EventType.SHIPPED,
    EventType.DELIVERED,
)


class ItemAddedPayload(BaseModel):
    """Payload of an ``ITEM_ADDED`` event (R1.4).

    Attributes:
        sku: Stock-keeping unit of the line item.
        qty: Quantity ordered; must be positive.
        unit_price: Price per unit in integer minor units (D4).
    """

    sku: str
    qty: int = Field(gt=0)
    unit_price: int = Field(gt=0)

    @property
    def line_total(self) -> int:
        """Return the line's contribution to the order total.

        Returns:
            ``qty * unit_price`` in integer minor units — exact, never rounded.
        """
        return self.qty * self.unit_price


class PaidPayload(BaseModel):
    """Payload of a ``PAID`` event (R1.4).

    Attributes:
        amount: Amount paid in integer minor units. The consumer compares this
            against its folded running total (R1.21).
    """

    amount: int = Field(ge=0)


#: Event types whose payload has a defined shape. Types absent from this map carry
#: an empty payload.
PAYLOAD_MODELS: dict[EventType, type[BaseModel]] = {
    EventType.ITEM_ADDED: ItemAddedPayload,
    EventType.PAID: PaidPayload,
}


def utc_now() -> datetime:
    """Return the current time as a timezone-aware UTC timestamp.

    Returns:
        The current UTC time.
    """
    return datetime.now(timezone.utc)


class OrderEvent(BaseModel):
    """A single order lifecycle event (R1.1).

    Attributes:
        order_id: Identity of the order. Also used as the Kafka message key, which
            is what routes all of an order's events to one partition (R1.7).
        sequence: Position of this event within its order, starting at 1 and
            increasing by exactly 1 (R1.2).
        event_type: Which lifecycle event this is.
        occurred_at: When the producer emitted the event.
        payload: Event-type-specific data, validated against :data:`PAYLOAD_MODELS`.
    """

    order_id: str
    sequence: int = Field(ge=1)
    event_type: EventType
    occurred_at: datetime = Field(default_factory=utc_now)
    payload: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_payload_shape(self) -> "OrderEvent":
        """Validate the payload against the model registered for this event type.

        Returns:
            The validated event.

        Raises:
            ValueError: If the payload does not match its event type's schema.
        """
        model = PAYLOAD_MODELS.get(self.event_type)
        if model is not None:
            model.model_validate(self.payload)
        return self

    def as_item_added(self) -> ItemAddedPayload:
        """Return the payload typed as an ``ITEM_ADDED`` payload.

        Returns:
            The parsed :class:`ItemAddedPayload`.

        Raises:
            ValueError: If this event is not an ``ITEM_ADDED`` event.
        """
        if self.event_type is not EventType.ITEM_ADDED:
            raise ValueError(f"{self.event_type} is not ITEM_ADDED")
        return ItemAddedPayload.model_validate(self.payload)

    def as_paid(self) -> PaidPayload:
        """Return the payload typed as a ``PAID`` payload.

        Returns:
            The parsed :class:`PaidPayload`.

        Raises:
            ValueError: If this event is not a ``PAID`` event.
        """
        if self.event_type is not EventType.PAID:
            raise ValueError(f"{self.event_type} is not PAID")
        return PaidPayload.model_validate(self.payload)


def is_legal_transition(event_type: EventType, current: OrderState | None) -> bool:
    """Report whether an event may legally follow the given lifecycle state.

    Args:
        event_type: The incoming event's type.
        current: The order's current state, or ``None`` if the order is unknown.

    Returns:
        ``True`` if the transition is permitted by :data:`LEGAL_PREDECESSORS`.
    """
    return current in LEGAL_PREDECESSORS[event_type]
