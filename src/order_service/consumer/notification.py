"""The notification service.

Reacts to all four event types (R1.34).
"""

import logging

from order_service.events import EventType, LifecycleEvent
from order_service.consumer.runtime import Handler, ServiceSpec

logger = logging.getLogger(__name__)

SERVICE_NAME = "notification"

MESSAGES: dict[EventType, str] = {
    EventType.ORDER_CREATED: "Order confirmed — payment received, we are preparing it",
    EventType.PACKED: "Your order is packed and waiting for pickup",
    EventType.SHIPPED: "On its way",
    EventType.DELIVERED: "Delivered — thanks for shopping with us",
}


def _message_for(event: LifecycleEvent) -> str:
    """Compose the customer-facing message, with payload details where they exist."""
    message = MESSAGES[event.event_type]
    if event.event_type is EventType.SHIPPED:
        shipped = event.as_shipped()
        return f"{message} via {shipped.carrier}, tracking {shipped.tracking_number}"
    if event.event_type is EventType.ORDER_CREATED:
        created = event.as_order_created()
        return f"{message} (total {created.total_amount})"
    return message


def _notify(event: LifecycleEvent) -> None:
    """Send the customer the message for this event."""
    logger.info(
        "[%s] → customer of %s: %s",
        SERVICE_NAME,
        event.order_id,
        _message_for(event),
    )


def build_service() -> ServiceSpec:
    """Build the notification service spec."""
    handlers: dict[EventType, Handler] = {
        event_type: _notify for event_type in EventType
    }
    return ServiceSpec(name=SERVICE_NAME, handlers=handlers)
