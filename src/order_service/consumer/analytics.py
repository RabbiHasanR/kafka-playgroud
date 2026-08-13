"""The analytics service.

Counts all four event types (R1.35). The counter is a closure rather than a
module-level dict, so two services built in one interpreter get independent counts.
It lives in memory: the committed offset comes back on restart, the tally does not.
"""

import logging
from collections import Counter

from order_service.events import EventType, LifecycleEvent
from order_service.consumer.runtime import Handler, ServiceSpec

logger = logging.getLogger(__name__)

SERVICE_NAME = "analytics"


def build_service() -> ServiceSpec:
    """Build the analytics service spec, all four types sharing one counter."""
    counts: Counter[EventType] = Counter()

    def _count(event: LifecycleEvent) -> None:
        """Count one event and log the running tally."""
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
