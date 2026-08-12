"""FastAPI application hosting the producer.

The lifespan owns the Kafka producer and its poll thread (D7). Delivery callbacks
only fire while somebody calls ``poll()``, so without that thread a request waiting
on a delivery report would hang until its timeout — the failure looks like a broker
problem and is not one.
"""

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from order_pipeline.config import get_settings
from order_pipeline.producer.kafka_producer import OrderEventProducer
from order_pipeline.producer.routes import router
from order_pipeline.producer.simulator import JobRegistry

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Start and stop the Kafka producer alongside the application.

    Args:
        app: The application whose state holds the producer and job registry.

    Yields:
        Control to the running application.
    """
    settings = get_settings()
    producer = OrderEventProducer(settings)
    producer.start()

    app.state.settings = settings
    app.state.producer = producer
    app.state.jobs = JobRegistry()

    logger.info(
        "producer ready: brokers=%s topic=%s",
        settings.kafka_bootstrap_servers,
        settings.order_events_topic,
    )
    try:
        yield
    finally:
        # Flush before exit so buffered events are not lost on shutdown (R1.14).
        producer.stop()


def create_app() -> FastAPI:
    """Build the producer application.

    Returns:
        The configured FastAPI application.
    """
    app = FastAPI(
        title="Order Event Producer",
        description=(
            "Publishes order lifecycle events keyed by order_id. "
            "See specs/001-order-event-pipeline/."
        ),
        version="0.1.0",
        lifespan=lifespan,
    )
    app.include_router(router)
    return app


app = create_app()


def main() -> None:
    """Run the producer with uvicorn using the configured host and port."""
    import uvicorn

    settings = get_settings()
    uvicorn.run(
        "order_pipeline.producer.app:app",
        host=settings.producer_host,
        port=settings.producer_port,
        log_level="info",
    )


if __name__ == "__main__":
    main()
