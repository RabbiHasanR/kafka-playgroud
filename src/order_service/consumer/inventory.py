"""The inventory service.

Reacts to two of the four event types and ignores the rest (R2.33) — which is the
point worth noticing. It is subscribed to the same topic as the other two services and
receives every message; caring about only some of them is a property of the *handler
map*, not of the subscription. Adding a service that cares about different events costs
a container, not a producer change.

The work is a log line. Reserving real stock is not what this feature is about.
"""

import logging

from order_service.events import EventType, LifecycleEvent
from order_service.consumer.runtime import Handler, ServiceSpec

logger = logging.getLogger(__name__)

SERVICE_NAME = "inventory"


def _reserve_stock(event: LifecycleEvent) -> None:
    """Reserve stock for every line item of a new order.

    Args:
        event: The ``ORDER_CREATED`` event.
    """
    payload = event.as_order_created()
    for item in payload.items:
        logger.info(
            "[%s] reserving %d × %s for %s",
            SERVICE_NAME,
            item.qty,
            item.sku,
            event.order_id,
        )


def _release_reservation(event: LifecycleEvent) -> None:
    """Turn a reservation into a committed stock movement once the parcel ships.

    Args:
        event: The ``SHIPPED`` event.
    """
    payload = event.as_shipped()
    logger.info(
        "[%s] reservation for %s committed — handed to %s (%s)",
        SERVICE_NAME,
        event.order_id,
        payload.carrier,
        payload.tracking_number,
    )


def build_service() -> ServiceSpec:
    """Build the inventory service specification.

    Returns:
        A spec handling ``ORDER_CREATED`` and ``SHIPPED`` only.
    """
    handlers: dict[EventType, Handler] = {
        EventType.ORDER_CREATED: _reserve_stock,
        EventType.SHIPPED: _release_reservation,
    }
    return ServiceSpec(name=SERVICE_NAME, handlers=handlers)
