"""Entry point for all three consumer services.

``SERVICE_NAME`` selects one entry from :data:`SERVICE_REGISTRY` (R2.37, D12). Three
containers run this same module with three different values and therefore three
different consumer groups, which is what makes the fan-out visible:

    docker compose logs -f inventory-consumer notification-consumer analytics-consumer

One event, three reactions, three independent offsets.
"""

import logging
import signal
import sys
from collections.abc import Callable
from types import FrameType

from confluent_kafka import KafkaException

from order_service.config import get_settings
from order_service.consumer import analytics, inventory, notification
from order_service.consumer.runtime import ServiceConsumer, ServiceSpec

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
)

logger = logging.getLogger("order_service.consumer")

#: Every service this image can run, by ``SERVICE_NAME``.
#:
#: Factories rather than instances, so per-service state — analytics' counter — is
#: created fresh per process instead of shared at import time.
SERVICE_REGISTRY: dict[str, Callable[[], ServiceSpec]] = {
    inventory.SERVICE_NAME: inventory.build_service,
    notification.SERVICE_NAME: notification.build_service,
    analytics.SERVICE_NAME: analytics.build_service,
}


def build_spec(service_name: str) -> ServiceSpec:
    """Look up and build one service by name.

    Args:
        service_name: The ``SERVICE_NAME`` to resolve.

    Returns:
        The built :class:`~order_service.consumer.runtime.ServiceSpec`.

    Raises:
        KeyError: If no service with that name is registered.
    """
    try:
        factory = SERVICE_REGISTRY[service_name]
    except KeyError as exc:
        known = ", ".join(sorted(SERVICE_REGISTRY))
        raise KeyError(
            f"unknown SERVICE_NAME '{service_name}' — expected one of: {known}"
        ) from exc
    return factory()


def main() -> None:
    """Run the service named by ``SERVICE_NAME`` until interrupted."""
    settings = get_settings()
    try:
        spec = build_spec(settings.service_name)
    except KeyError as exc:
        logger.error("%s", exc)
        sys.exit(2)

    consumer = ServiceConsumer(spec, settings)

    def shutdown(signum: int, _frame: FrameType | None) -> None:
        logger.info("[%s] signal %d received, shutting down", spec.name, signum)
        consumer.stop()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        consumer.run()
    except KafkaException as exc:
        logger.error("[%s] fatal kafka error: %s", spec.name, exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
