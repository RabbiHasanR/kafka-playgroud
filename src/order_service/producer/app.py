"""FastAPI application hosting the order service.

The lifespan owns the Kafka producer, its poll thread, and the order store (D6).
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
    """Start and stop the Kafka producer alongside the application."""
    settings = get_settings()
    producer = LifecycleEventProducer(settings)
    producer.start()

    app.state.settings = settings
    app.state.producer = producer
    app.state.orders = OrderStore()

    # 004 D6/R4.8: acks is a PRODUCER setting and meaningless on a consumer, so it goes
    # in this banner rather than the consumer banner R3.23 established. Without it, a run
    # at acks=0 is indistinguishable in the logs from a run at acks=all — and the whole
    # point of the lever is comparing two runs after the fact.
    logger.info(
        "order service ready: brokers=%s topic=%s acks=%s",
        settings.kafka_bootstrap_servers,
        settings.order_lifecycle_topic,
        settings.producer_acks.value,
    )
    # 005 R5.21 — the producer's retry budget, on the same banner as acks for the same
    # reason: with min.insync.replicas set, `acks=all` can now be REFUSED rather than
    # merely slow, and how long the producer fought before giving up is the difference
    # between reading a 503 as "the cluster is degraded" and as "the client gave up early".
    logger.info(
        "producer retry budget: retries=%d backoff_ms=%d message_timeout_ms=%d",
        settings.producer_retries,
        settings.producer_retry_backoff_ms,
        settings.producer_message_timeout_ms,
    )
    try:
        yield
    finally:
        # Flush before exit so buffered events are not lost on shutdown.
        producer.stop()


def create_app() -> FastAPI:
    """Build the order service application."""
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
