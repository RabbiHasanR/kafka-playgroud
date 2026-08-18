"""Entry point for all three consumer services.

``SERVICE_NAME`` selects one entry from :data:`SERVICE_REGISTRY` (R1.37, D12). Three
containers run this module with three different values, and so three different
consumer groups:

    docker compose logs -f inventory-consumer notification-consumer analytics-consumer
"""

import logging
import signal
import sys
from collections.abc import Callable
from types import FrameType

from confluent_kafka import KafkaException

from order_service.config import Settings, StateBackend, get_settings
from order_service.consumer import analytics, inventory, notification
from order_service.consumer.dlq import FailureRouter
from order_service.consumer.runtime import (
    ConsumerConfigError,
    ServiceConsumer,
    ServiceSpec,
)
from order_service.consumer.state import (
    MemoryStateStore,
    PostgresStateStore,
    StateStore,
    StateStoreUnavailable,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
)

logger = logging.getLogger("order_service.consumer")

#: Every service this image can run, by ``SERVICE_NAME``; factories, not instances.
SERVICE_REGISTRY: dict[str, Callable[[], ServiceSpec]] = {
    inventory.SERVICE_NAME: inventory.build_service,
    notification.SERVICE_NAME: notification.build_service,
    analytics.SERVICE_NAME: analytics.build_service,
}


def build_spec(service_name: str) -> ServiceSpec:
    """Look up and build one service by name.

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


def build_store(settings: Settings, group_id: str) -> StateStore:
    """Build the state store this process will fold into (003 D8).

    Args:
        settings: Resolved environment settings.
        group_id: The consumer group whose memory this store holds — part of the durable
            primary key, so the three services stay independent (R3.2).

    Returns:
        The store selected by ``STATE_BACKEND``.

    Raises:
        StateStoreUnavailable: If the durable backend is selected and the database
            cannot be reached, or ``STATE_DB_DSN`` is unset.
    """
    if settings.state_backend is StateBackend.MEMORY:
        return MemoryStateStore()

    if settings.state_db_dsn is None:
        raise StateStoreUnavailable(
            "STATE_BACKEND=postgres requires STATE_DB_DSN — see .env.example"
        )
    return PostgresStateStore(settings.state_db_dsn, group_id=group_id)


def main() -> None:
    """Run the service named by ``SERVICE_NAME`` until interrupted."""
    settings = get_settings()
    try:
        spec = build_spec(settings.service_name)
    except KeyError as exc:
        logger.error("%s", exc)
        sys.exit(2)

    # R3.21 — before the Consumer exists, so a doomed process never joins the group.
    try:
        store = build_store(settings, group_id=settings.group_id_for(spec.name))
    except StateStoreUnavailable as exc:
        logger.error("[%s] %s", spec.name, exc)
        sys.exit(2)

    # 005 D3 — where a failed message goes. Built here alongside the store, for the same
    # reason: one owner, constructed before the group is joined, closed in one place.
    router = FailureRouter(settings)

    # R2.21 — a bad protocol/setting pair exits immediately; restarting will not help.
    try:
        consumer = ServiceConsumer(spec, settings, store, router)
    except ConsumerConfigError as exc:
        logger.error("[%s] %s", spec.name, exc)
        router.close()
        store.close()
        sys.exit(2)

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
    except StateStoreUnavailable:
        # Already logged with its marker in the consume loop (R3.22).
        sys.exit(1)
    finally:
        router.close()
        store.close()


if __name__ == "__main__":
    main()
