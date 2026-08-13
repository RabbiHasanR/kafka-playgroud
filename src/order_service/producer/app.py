"""FastAPI application hosting the order service.

The lifespan owns the Kafka producer, its poll thread, and the order store (D6).
Delivery callbacks only fire while somebody calls ``poll()``, so without that thread a
request waiting on a delivery report would hang until its timeout — a failure that
looks like a broker problem and is not one.
"""

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from order_service.config import get_settings
from order_service.producer.kafka_producer import LifecycleEventProducer
from order_service.producer.orders import OrderStore
from order_service.producer.routes import router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Start and stop the Kafka producer alongside the application.

    Args:
        app: The application whose state holds the producer and the order store.

    Yields:
        Control to the running application.
    """
    settings = get_settings()
    producer = LifecycleEventProducer(settings)
    producer.start()

    app.state.settings = settings
    app.state.producer = producer
    app.state.orders = OrderStore()

    logger.info(
        "order service ready: brokers=%s topic=%s",
        settings.kafka_bootstrap_servers,
        settings.order_lifecycle_topic,
    )
    try:
        yield
    finally:
        # Flush before exit so buffered events are not lost on shutdown.
        producer.stop()


def create_app() -> FastAPI:
    """Build the order service application.

    Returns:
        The configured FastAPI application.
    """
    app = FastAPI(
        title="Prepaid Order Service",
        description=(
            "Creates prepaid orders and publishes their lifecycle events, keyed by "
            "order_id. See specs/001-prepaid-order-service/."
        ),
        version="0.1.0",
        lifespan=lifespan,
    )
    app.include_router(router)
    return app


app = create_app()


def main() -> None:
    """Run the order service with uvicorn using the configured host and port."""
    import uvicorn

    settings = get_settings()
    uvicorn.run(
        "order_service.producer.app:app",
        host=settings.order_service_host,
        port=settings.order_service_port,
        log_level="info",
    )


if __name__ == "__main__":
    main()
