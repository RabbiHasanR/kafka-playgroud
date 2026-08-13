"""The notification service.

Reacts to all four event types (R2.34) with a different customer-facing message each
time. One handler serves every type, with the wording chosen from a table — four
near-identical handler functions would be four places to fix a typo.
"""

import logging

from order_service.events import EventType, LifecycleEvent
from order_service.consumer.runtime import Handler, ServiceSpec

logger = logging.getLogger(__name__)

SERVICE_NAME = "notification"

#: What the customer is told for each lifecycle event.
MESSAGES: dict[EventType, str] = {
    EventType.ORDER_CREATED: "Order confirmed — payment received, we are preparing it",
    EventType.PACKED: "Your order is packed and waiting for pickup",
    EventType.SHIPPED: "On its way",
    EventType.DELIVERED: "Delivered — thanks for shopping with us",
}


def _message_for(event: LifecycleEvent) -> str:
    """Compose the customer-facing message for one event.

    Args:
        event: The event to describe.

    Returns:
        The message body, with shipping details appended where they exist.
    """
    message = MESSAGES[event.event_type]
    if event.event_type is EventType.SHIPPED:
        shipped = event.as_shipped()
        return f"{message} via {shipped.carrier}, tracking {shipped.tracking_number}"
    if event.event_type is EventType.ORDER_CREATED:
        created = event.as_order_created()
        return f"{message} (total {created.total_amount})"
    return message


def _notify(event: LifecycleEvent) -> None:
    """Send the customer the message for this event.

    Args:
        event: The event that triggered the notification.
    """
    logger.info(
        "[%s] → customer of %s: %s",
        SERVICE_NAME,
        event.order_id,
        _message_for(event),
    )


def build_service() -> ServiceSpec:
    """Build the notification service specification.

    Returns:
        A spec handling all four event types.
    """
    handlers: dict[EventType, Handler] = {
        event_type: _notify for event_type in EventType
    }
    return ServiceSpec(name=SERVICE_NAME, handlers=handlers)
