"""Partition affinity against a real broker (specs 001, R1.9, R1.10).

A mocked producer can't prove anything about murmur2 hashing or the broker's actual
partition count — this is the one property that only the real partitioner can show.
"""

from collections.abc import Callable

import pytest
from confluent_kafka.admin import AdminClient

from order_service.config import Settings
from order_service.events import EventType, LifecycleEvent
from tests.integration.order_service.conftest import running_producer

pytestmark = pytest.mark.integration


def test_same_order_id_always_lands_on_one_partition(
    kafka_admin_client: AdminClient,
    scratch_topics: Callable[..., dict[str, str]],
    settings_factory: Callable[..., Settings],
) -> None:
    topics = scratch_topics(lifecycle=True, partitions=3)
    settings = settings_factory(order_lifecycle_topic=topics["lifecycle"])

    with running_producer(settings) as producer:
        # One order, three events in its lifecycle — all keyed by the same order_id.
        order_id = "it-partition-affinity-order"
        partitions_seen = set()
        for sequence, event_type in enumerate(
            [EventType.ORDER_CREATED, EventType.PACKED, EventType.SHIPPED], start=1
        ):
            event = LifecycleEvent(
                order_id=order_id, sequence=sequence, event_type=event_type
            )
            result = producer.publish_and_wait(event)
            partitions_seen.add(result.partition)

        assert len(partitions_seen) == 1, (
            f"one order_id landed on {len(partitions_seen)} partitions: {partitions_seen}"
        )

        # Many distinct orders should spread across more than one partition — a weak
        # sanity check that the topic's 3 partitions are actually in play (R1.9), not a
        # claim about uniform distribution.
        distinct_partitions = set()
        for i in range(15):
            event = LifecycleEvent(
                order_id=f"it-spread-order-{i}",
                sequence=1,
                event_type=EventType.ORDER_CREATED,
            )
            result = producer.publish_and_wait(event)
            distinct_partitions.add(result.partition)

        assert len(distinct_partitions) > 1, (
            f"15 distinct order_ids all landed on partition(s) {distinct_partitions} — "
            "expected a spread across the topic's 3 partitions"
        )
