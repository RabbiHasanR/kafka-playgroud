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

from confluent_kafka import KafkaException, Producer
from pydantic import ValidationError

from order_service.config import Settings, StateBackend, get_settings
from order_service.consumer import analytics, inventory, notification
from order_service.consumer.dlq import FailureRouter
from order_service.consumer.runtime import (
    ConsumerConfigError,
    ServiceConsumer,
    ServiceSpec,
)
from order_service.consumer.state import (
    LocalStateStore,
    MemoryStateStore,
    StateStore,
    StateStoreUnavailable,
)
from order_service.consumer.transactions import ProducerFenced, build_producer

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


def build_store(settings: Settings, group_id: str, producer: Producer) -> StateStore:
    """Build the state store this process will fold into (003 D8, 007 D2).

    Args:
        settings: Resolved environment settings.
        group_id: The consumer group whose memory this store holds. It names both the
            store's directory and its changelog topic, so the three services stay
            independent (R3.2).
        producer: The process's one producer, which the local backend writes its
            changelog through (R8.4). Ignored by the in-memory backend, which has no
            changelog and is 002's control.

    Returns:
        The store selected by ``STATE_BACKEND``.

    Raises:
        StateStoreUnavailable: If the local backend is selected and its state directory
            cannot be used.
    """
    if settings.state_backend is StateBackend.MEMORY:
        return MemoryStateStore()
    return LocalStateStore(settings, group_id=group_id, producer=producer)


def main() -> None:
    """Run the service named by ``SERVICE_NAME`` until interrupted."""
    # R8.12 — a refused setting pair fails here, before anything is built. The one that
    # matters is exactly_once with a checkpoint rebuild, which would have a repair skip
    # the records that repair it.
    try:
        settings = get_settings()
    except ValidationError as exc:
        for error in exc.errors():
            logger.error("%s", error["msg"])
        sys.exit(2)

    try:
        spec = build_spec(settings.service_name)
    except KeyError as exc:
        logger.error("%s", exc)
        sys.exit(2)

    group_id = settings.group_id_for(spec.name)

    # 008 D1 — ONE producer for this process, before the store and the router, because
    # both write through it and a transaction cannot span two. Its transactional identity
    # is claimed at init, which fences whatever held it before.
    producer = build_producer(settings, group_id=group_id, instance=settings.instance_label)

    # R3.21 — before the Consumer exists, so a doomed process never joins the group.
    try:
        store = build_store(settings, group_id=group_id, producer=producer)
    except StateStoreUnavailable as exc:
        logger.error("[%s] %s", spec.name, exc)
        sys.exit(2)

    # 005 D3 — where a failed message goes. Built here alongside the store, for the same
    # reason: one owner, constructed before the group is joined, closed in one place.
    router = FailureRouter(settings, producer)

    # R2.21 — a bad protocol/setting pair exits immediately; restarting will not help.
    # R8.5 — so does a transactional identity somebody else already holds.
    try:
        consumer = ServiceConsumer(spec, settings, store, router, producer)
    except ConsumerConfigError as exc:
        logger.error("[%s] %s", spec.name, exc)
        router.close()
        store.close()
        sys.exit(2)
    except ProducerFenced as exc:
        logger.error(
            "[%s] PRODUCER_FENCED at startup — is CONSUMER_INSTANCE_ID unique? %s",
            spec.name,
            exc,
        )
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
    except ProducerFenced:
        # R8.5 — already logged with its marker. Not retried: a fenced producer's epoch
        # is behind for good, so restarting the process is the only recovery.
        sys.exit(1)
    finally:
        router.close()
        store.close()
        # Last, and after the store's own flush: whatever is still queued here belongs to
        # writes the store just made durable. Under a transaction anything uncommitted was
        # already aborted by the loop's shutdown, so this only drains what committed.
        remaining = producer.flush(10.0)
        if remaining:
            logger.warning("[%s] producer flush left %d message(s) unsent", spec.name, remaining)


if __name__ == "__main__":
    main()
