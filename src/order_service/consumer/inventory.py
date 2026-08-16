"""The inventory service.

Receives every message like the other two, but reacts to only ``ORDER_CREATED`` and
``SHIPPED`` (R1.33) — a property of the handler map, not of the subscription. The work
is a log line; reserving real stock is out of scope.
"""

import logging

from order_service.events import EventType, LifecycleEvent
from order_service.consumer.runtime import Handler, ServiceSpec

logger = logging.getLogger(__name__)

SERVICE_NAME = "inventory"


def _reserve_stock(event: LifecycleEvent) -> None:
    """Reserve stock for every line item of a new order."""
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
    """Turn a reservation into a committed stock movement once the parcel ships."""
    payload = event.as_shipped()
    logger.info(
        "[%s] reservation for %s committed — handed to %s (%s)",
        SERVICE_NAME,
        event.order_id,
        payload.carrier,
        payload.tracking_number,
    )


def build_service() -> ServiceSpec:
    """Build the inventory service spec."""
    handlers: dict[EventType, Handler] = {
        EventType.ORDER_CREATED: _reserve_stock,
        EventType.SHIPPED: _release_reservation,
    }
    return ServiceSpec(name=SERVICE_NAME, handlers=handlers)
