"""Unit tests for the Kafka producer wrapper (specs/001 R1.18, specs/006 D3).

No broker involved: ``confluent_kafka.Producer`` is replaced by
:class:`FakeKafkaProducer` (see ``conftest.py``), so every delivery outcome is
driven directly instead of waited for.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from order_service.events import EventType, LifecycleEvent, utc_now
from order_service.producer.kafka_producer import (
    DeliveryFailed,
    DeliveryResult,
    DeliveryTimeout,
    LifecycleEventProducer,
    _describe_delivery_error,
)

if TYPE_CHECKING:
    from tests.unit.order_service.producer.conftest import FakeKafkaProducer


class _Msg:
    """The bare minimum a delivery-report message needs for these tests."""

    def __init__(self, topic: str, partition: int) -> None:
        self._topic = topic
        self._partition = partition

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition


class TestDescribeDeliveryError:
    """R4.9 — the partition a failure applied to must survive into the message."""

    def test_includes_the_topic_partition_when_known(self) -> None:
        msg = _Msg("order-lifecycle", partition=1)
        result = _describe_delivery_error("Broker: Not enough in-sync replicas", msg)
        assert result == "Broker: Not enough in-sync replicas [order-lifecycle-1]"

    def test_notes_when_no_partition_was_assigned(self) -> None:
        msg = _Msg("order-lifecycle", partition=-1)
        result = _describe_delivery_error("some error", msg)
        assert result == "some error (partition not yet assigned)"

    def test_falls_back_to_the_bare_error_when_the_message_has_none(self) -> None:
        result = _describe_delivery_error("some error", None)
        assert result == "some error"


def _event() -> LifecycleEvent:
    return LifecycleEvent(
        order_id="ord-1", sequence=1, event_type=EventType.PACKED, occurred_at=utc_now()
    )


class TestPublishAndWait:
    def test_successful_delivery_returns_the_broker_assigned_position(
        self, event_producer: LifecycleEventProducer, fake_kafka_producer: FakeKafkaProducer
    ) -> None:
        result = event_producer.publish_and_wait(_event())
        assert result == DeliveryResult(partition=0, offset=42)

    def test_broker_error_raises_delivery_failed(
        self, event_producer: LifecycleEventProducer, fake_kafka_producer: FakeKafkaProducer
    ) -> None:
        fake_kafka_producer.deliver_error = "Broker: Not enough in-sync replicas"
        with pytest.raises(DeliveryFailed):
            event_producer.publish_and_wait(_event())

    def test_missing_topic_raises_delivery_failed_not_timeout(
        self, event_producer: LifecycleEventProducer, fake_kafka_producer: FakeKafkaProducer
    ) -> None:
        fake_kafka_producer.deliver_immediately = False
        fake_kafka_producer.topics_on_broker = set()  # topic does not exist
        with pytest.raises(DeliveryFailed, match="does not exist"):
            event_producer._publish_blocking(
                topic="order-lifecycle", key="ord-1", value=b"{}", what="test", timeout=0.05
            )

    def test_no_report_for_an_existing_topic_raises_delivery_timeout(
        self, event_producer: LifecycleEventProducer, fake_kafka_producer: FakeKafkaProducer
    ) -> None:
        fake_kafka_producer.deliver_immediately = False
        fake_kafka_producer.topics_on_broker = {"order-lifecycle"}
        with pytest.raises(DeliveryTimeout):
            event_producer._publish_blocking(
                topic="order-lifecycle", key="ord-1", value=b"{}", what="test", timeout=0.05
            )


class TestPublishTombstone:
    """006 D3 — a tombstone is a keyed message with ``value=None``."""

    def test_sends_a_null_value(
        self, event_producer: LifecycleEventProducer, fake_kafka_producer: FakeKafkaProducer
    ) -> None:
        result = event_producer.publish_tombstone("ord-1")
        assert result == DeliveryResult(partition=0, offset=42)
        assert fake_kafka_producer.produced[0]["value"] is None


class TestPublishSnapshot:
    """R6.5 — a lost snapshot must never fail the caller."""

    def test_never_raises_when_the_produce_call_fails(
        self, event_producer: LifecycleEventProducer, fake_kafka_producer: FakeKafkaProducer
    ) -> None:
        fake_kafka_producer.raise_on_produce = BufferError("queue full")
        event_producer.publish_snapshot("ord-1", {"order_id": "ord-1"})  # must not raise
