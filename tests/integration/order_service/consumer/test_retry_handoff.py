"""Retry hand-off and commit ordering against a real broker (spec 005, R5.5–R5.7).

The property under test — the source offset commits only after the retry-topic
publish is acknowledged, and the partition keeps flowing for other keys while one
message is in retry — is a broker commit-ordering guarantee, not logic a mock can
stand in for.

Uses the real `handler_failure_mode=transient` lever (005 D11) to force exactly one
deterministic failure on one named order, rather than faking an infra failure.
"""

from collections.abc import Callable

import pytest
from confluent_kafka import Consumer, TopicPartition

from order_service.config import HandlerFailureMode, Settings
from order_service.consumer.dlq import HDR_ORIGINAL_TOPIC, HDR_SERVICE, decode_headers
from order_service.events import EventType, LifecycleEvent
from tests.integration.order_service.conftest import (
    build_service_consumer,
    committed_offset,
    running_consumer,
    running_producer,
    topic_high_watermark,
    wait_until,
)

pytestmark = pytest.mark.integration


def _first_message(bootstrap_servers: str, topic: str):  # noqa: ANN201 — confluent_kafka.Message
    consumer = Consumer({"bootstrap.servers": bootstrap_servers, "group.id": "it-retry-peek"})
    try:
        consumer.assign([TopicPartition(topic, 0, 0)])
        for _ in range(20):
            message = consumer.poll(1.0)
            if message is not None and message.error() is None:
                return message
        raise TimeoutError(f"no message found on {topic}-0")
    finally:
        consumer.close()


def test_failed_message_moves_to_retry_topic_without_blocking_the_partition(
    bootstrap_servers: str,
    scratch_topics: Callable[..., dict[str, str]],
    settings_factory: Callable[..., Settings],
    unique_group_id_factory: Callable[[str], str],
) -> None:
    topics = scratch_topics(lifecycle=True, snapshot=True, retry=True, partitions=1)
    group_id = unique_group_id_factory("inventory")
    settings = settings_factory(
        order_lifecycle_topic=topics["lifecycle"],
        order_snapshot_topic=topics["snapshot"],
        retry_topic=topics["retry"],
        service_name="inventory",
        consumer_group_id=group_id,
        handler_failure_mode=HandlerFailureMode.TRANSIENT,
        handler_failure_orders="it-retry-handoff-order",
        handler_failure_attempts=1,
        retry_max_attempts=3,
    )
    consumer, producer = build_service_consumer("inventory", settings)

    with running_consumer(consumer):
        wait_until(
            lambda: len(consumer._consumer.assignment()) > 0,  # noqa: SLF001
            description="inventory consumer assigned partitions",
        )

        producer_settings = settings_factory(order_lifecycle_topic=topics["lifecycle"])
        with running_producer(producer_settings) as source_producer:
            failing = source_producer.publish_and_wait(
                LifecycleEvent(
                    order_id="it-retry-handoff-order",
                    sequence=1,
                    event_type=EventType.ORDER_CREATED,
                )
            )
            healthy = source_producer.publish_and_wait(
                LifecycleEvent(
                    order_id="it-retry-handoff-healthy-order",
                    sequence=1,
                    event_type=EventType.ORDER_CREATED,
                )
            )

        wait_until(
            lambda: topic_high_watermark(bootstrap_servers, topics["retry"]) >= 1,
            description="one message reaches the retry topic",
        )

        # Both the failing and the healthy message's offsets are past — proving the
        # partition kept flowing rather than stalling behind the failed one (R5.7).
        wait_until(
            lambda: committed_offset(
                bootstrap_servers, group_id, topics["lifecycle"], failing.partition
            )
            > failing.offset
            and committed_offset(
                bootstrap_servers, group_id, topics["lifecycle"], healthy.partition
            )
            > healthy.offset,
            description="source partition committed past both the failing and healthy events",
        )

    retried = _first_message(bootstrap_servers, topics["retry"])
    headers = decode_headers(retried)
    assert headers[HDR_SERVICE] == "inventory"
    assert headers[HDR_ORIGINAL_TOPIC] == topics["lifecycle"]

    producer.flush(10.0)
