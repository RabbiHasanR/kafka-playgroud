"""The analytics service.

Reacts to all four event types (R2.35) by counting them. The counter is a closure
created per process rather than a module-level dict, so building two services in one
interpreter — which the tests do — gives two independent counts.

Like every count in this repository, it lives in memory and dies with the process. That
is 001's X3 lesson repeated: the committed offset comes back on restart, the tally does
not.
"""

import logging
from collections import Counter

from order_service.events import EventType, LifecycleEvent
from order_service.consumer.runtime import Handler, ServiceSpec

logger = logging.getLogger(__name__)

SERVICE_NAME = "analytics"


def build_service() -> ServiceSpec:
    """Build the analytics service specification.

    Returns:
        A spec handling all four event types, sharing one counter between them.
    """
    counts: Counter[EventType] = Counter()

    def _count(event: LifecycleEvent) -> None:
        """Count one event and log the running tally.

        Args:
            event: The event to count.
        """
        counts[event.event_type] += 1
        tally = " ".join(
            f"{event_type}={counts[event_type]}"
            for event_type in EventType
            if counts[event_type]
        )
        logger.info(
            "[%s] counted %s for %s — %s",
            SERVICE_NAME,
            event.event_type,
            event.order_id,
            tally,
        )

    handlers: dict[EventType, Handler] = {
        event_type: _count for event_type in EventType
    }
    return ServiceSpec(name=SERVICE_NAME, handlers=handlers)
