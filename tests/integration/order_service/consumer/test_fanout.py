"""Fan-out across three real consumer groups (spec 001, R1.28–R1.31).

Kafka's per-group offset tracking and rebalance/resume behavior are exactly what a
mocked Kafka client can't reproduce — this exercises the real `ServiceConsumer`
against a real broker, using the real inventory/notification/analytics handlers.
"""

import threading
from collections.abc import Callable

import pytest

from order_service.config import Settings
from order_service.events import EventType, LifecycleEvent
from tests.integration.order_service.conftest import (
    build_service_consumer,
    committed_offset,
    running_producer,
    wait_until,
)

pytestmark = pytest.mark.integration

_SERVICES = ["inventory", "notification", "analytics"]


def _start(consumer) -> threading.Thread:  # noqa: ANN001 — ServiceConsumer, avoid import cycle noise
    thread = threading.Thread(target=consumer.run, daemon=True)
    thread.start()
    return thread


def test_one_event_reaches_all_three_groups_independently(
    bootstrap_servers: str,
    scratch_topics: Callable[..., dict[str, str]],
    settings_factory: Callable[..., Settings],
    unique_group_id_factory: Callable[[str], str],
) -> None:
    topics = scratch_topics(lifecycle=True, snapshot=True, partitions=3)
    group_ids = {name: unique_group_id_factory(name) for name in _SERVICES}

    built = {
        name: build_service_consumer(
            name,
            settings_factory(
                order_lifecycle_topic=topics["lifecycle"],
                order_snapshot_topic=topics["snapshot"],
                service_name=name,
                consumer_group_id=group_ids[name],
            ),
        )
        for name in _SERVICES
    }
    consumers = {name: consumer for name, (consumer, _producer) in built.items()}
    threads = {name: _start(consumer) for name, consumer in consumers.items()}

    try:
        for name, consumer in consumers.items():
            wait_until(
                lambda c=consumer: len(c._consumer.assignment()) > 0,  # noqa: SLF001
                description=f"{name} consumer assigned partitions",
            )

        producer_settings = settings_factory(order_lifecycle_topic=topics["lifecycle"])
        with running_producer(producer_settings) as producer:
            event = LifecycleEvent(
                order_id="it-fanout-order-1", sequence=1, event_type=EventType.ORDER_CREATED
            )
            result = producer.publish_and_wait(event)

        for name in _SERVICES:
            wait_until(
                lambda name=name: committed_offset(
                    bootstrap_servers, group_ids[name], topics["lifecycle"], result.partition
                )
                > result.offset,
                description=f"{name} group's committed offset past {result.offset}",
            )
    finally:
        for consumer in consumers.values():
            consumer.stop()
        for thread in threads.values():
            thread.join(timeout=10.0)
        for _consumer, producer in built.values():
            producer.flush(10.0)


def test_stopping_one_group_does_not_affect_the_others_and_it_resumes_from_its_own_offset(
    bootstrap_servers: str,
    scratch_topics: Callable[..., dict[str, str]],
    settings_factory: Callable[..., Settings],
    unique_group_id_factory: Callable[[str], str],
) -> None:
    topics = scratch_topics(lifecycle=True, snapshot=True, partitions=3)
    group_ids = {name: unique_group_id_factory(name) for name in _SERVICES}

    def settings_for(name: str) -> Settings:
        return settings_factory(
            order_lifecycle_topic=topics["lifecycle"],
            order_snapshot_topic=topics["snapshot"],
            service_name=name,
            consumer_group_id=group_ids[name],
        )

    built = {name: build_service_consumer(name, settings_for(name)) for name in _SERVICES}
    consumers = {name: consumer for name, (consumer, _producer) in built.items()}
    threads = {name: _start(consumer) for name, consumer in consumers.items()}
    producer_settings = settings_factory(order_lifecycle_topic=topics["lifecycle"])

    try:
        for name, consumer in consumers.items():
            wait_until(
                lambda c=consumer: len(c._consumer.assignment()) > 0,  # noqa: SLF001
                description=f"{name} consumer assigned partitions",
            )

        with running_producer(producer_settings) as producer:
            warm_up = producer.publish_and_wait(
                LifecycleEvent(
                    order_id="it-fanout-restart-order",
                    sequence=1,
                    event_type=EventType.ORDER_CREATED,
                )
            )
        partition = warm_up.partition
        for name in _SERVICES:
            wait_until(
                lambda name=name: committed_offset(
                    bootstrap_servers, group_ids[name], topics["lifecycle"], partition
                )
                > warm_up.offset,
                description=f"{name} group caught up to the warm-up event",
            )

        # Stop notification; the other two must keep going without it.
        consumers["notification"].stop()
        threads["notification"].join(timeout=10.0)
        notification_offset_at_stop = committed_offset(
            bootstrap_servers, group_ids["notification"], topics["lifecycle"], partition
        )

        with running_producer(producer_settings) as producer:
            latest = producer.publish_and_wait(
                LifecycleEvent(
                    order_id="it-fanout-restart-order",
                    sequence=2,
                    event_type=EventType.PACKED,
                )
            )

        for name in ["inventory", "analytics"]:
            wait_until(
                lambda name=name: committed_offset(
                    bootstrap_servers, group_ids[name], topics["lifecycle"], partition
                )
                > latest.offset,
                description=f"{name} advanced past the event published while notification was down",
            )
        assert (
            committed_offset(
                bootstrap_servers, group_ids["notification"], topics["lifecycle"], partition
            )
            == notification_offset_at_stop
        ), "notification's committed offset moved while its consumer was stopped"

        # Restart notification under the SAME group id — a fresh process, same identity.
        built["notification"][1].flush(10.0)
        restarted, restarted_producer = build_service_consumer("notification", settings_for("notification"))
        consumers["notification"] = restarted
        threads["notification"] = _start(restarted)
        built["notification"] = (restarted, restarted_producer)

        wait_until(
            lambda: committed_offset(
                bootstrap_servers, group_ids["notification"], topics["lifecycle"], partition
            )
            > latest.offset,
            description="restarted notification group caught up from its own committed offset",
        )
    finally:
        for consumer in consumers.values():
            consumer.stop()
        for thread in threads.values():
            thread.join(timeout=10.0)
        for _consumer, producer in built.values():
            producer.flush(10.0)
