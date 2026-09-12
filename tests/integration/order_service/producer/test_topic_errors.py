"""A missing topic fails explicitly against a real broker (spec 001, R1.11).

`KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` is a broker-level setting — no mock can stand
in for "the broker refuses to conjure a topic that was never created."
"""

from collections.abc import Callable

import pytest

from order_service.config import Settings
from order_service.events import EventType, LifecycleEvent
from order_service.producer.kafka_producer import DeliveryFailed
from tests.integration.order_service.conftest import running_producer

pytestmark = pytest.mark.integration


def test_publishing_to_a_nonexistent_topic_fails_explicitly(
    unique_id: str, settings_factory: Callable[..., Settings]
) -> None:
    # No scratch_topics() call — the whole point is a topic nobody created.
    settings = settings_factory(
        order_lifecycle_topic=f"it-nonexistent-{unique_id}",
        # Shortened from the 30s production default so this expected failure doesn't
        # cost ~11s of `producer_delivery_wait_seconds` for no benefit.
        producer_message_timeout_ms=3000,
    )

    with running_producer(settings) as producer:
        event = LifecycleEvent(
            order_id="it-missing-topic-order", sequence=1, event_type=EventType.ORDER_CREATED
        )
        with pytest.raises(DeliveryFailed, match="does not exist"):
            producer.publish_and_wait(event)
