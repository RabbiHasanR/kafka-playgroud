"""Unit tests for the dead-letter replay tool (specs/005, R5.17, R5.18, R5.25)."""

import argparse

import pytest
from confluent_kafka import KafkaException, TopicPartition

from order_service.config import Settings
from order_service.consumer.errors import NonRetryableError
from order_service.tools.dlq_replay import Snapshot, describe, replay, snapshot_topic
from tests.unit.order_service.tools.conftest import (
    FakeDlqMessage,
    FakeReplayConsumer,
    FakeReplayProducer,
    encode_headers,
)


def make_args(
    *,
    publish: bool = False,
    service: str | None = None,
    include_poison: bool = False,
    limit: int = 0,
) -> argparse.Namespace:
    return argparse.Namespace(
        publish=publish, service=service, include_poison=include_poison, limit=limit
    )


class TestSnapshotMessageCount:
    def test_sums_across_multiple_partitions(self) -> None:
        snapshot = Snapshot(
            assignments=[
                TopicPartition("dlq", 0, 5),
                TopicPartition("dlq", 1, 0),
            ],
            end_offsets={0: 10, 1: 3},
        )
        assert snapshot.message_count == 8

    def test_partition_with_no_backlog_contributes_zero(self) -> None:
        snapshot = Snapshot(
            assignments=[TopicPartition("dlq", 0, 10)],
            end_offsets={0: 10},
        )
        assert snapshot.message_count == 0


class TestDescribe:
    def test_renders_fully_populated_headers(self) -> None:
        message = FakeDlqMessage(key=b"ord-1")
        headers = {
            "x-service": "inventory",
            "x-consumer-group": "inventory-service",
            "x-attempts-made": "3",
            "x-original-topic": "order-lifecycle",
            "x-original-partition": "0",
            "x-original-offset": "42",
            "x-error-class": "RetryableError",
            "x-error-message": "boom",
        }
        rendered = describe(message, headers)
        assert "ord-1" in rendered
        assert "service=inventory" in rendered
        assert "origin=order-lifecycle-0@42" in rendered
        assert "RetryableError: boom" in rendered

    def test_falls_back_for_missing_headers(self) -> None:
        message = FakeDlqMessage(key=None)
        rendered = describe(message, {})
        assert "<null>" in rendered
        assert "<no message>" in rendered


class TestSnapshotTopic:
    def test_builds_assignments_for_non_empty_partitions(self) -> None:
        consumer = FakeReplayConsumer(
            watermarks={0: (0, 5), 1: (2, 2)}, topic_error=None
        )
        snapshot = snapshot_topic(consumer, "dlq")
        assert snapshot.assignments == [TopicPartition("dlq", 0, 0)]
        assert snapshot.end_offsets == {0: 5}

    def test_empty_partition_is_excluded(self) -> None:
        consumer = FakeReplayConsumer(watermarks={0: (3, 3)}, topic_error=None)
        snapshot = snapshot_topic(consumer, "dlq")
        assert snapshot.assignments == []
        assert snapshot.end_offsets == {}

    def test_missing_topic_metadata_raises(self) -> None:
        consumer = FakeReplayConsumer(watermarks={}, topic_error="missing")
        with pytest.raises(KafkaException):
            snapshot_topic(consumer, "dlq")

    def test_errored_topic_metadata_raises(self) -> None:
        consumer = FakeReplayConsumer(watermarks={0: (0, 1)}, topic_error="boom")
        with pytest.raises(KafkaException):
            snapshot_topic(consumer, "dlq")


class TestReplay:
    def _consumer(self, queue: list[FakeDlqMessage]) -> FakeReplayConsumer:
        return FakeReplayConsumer(
            watermarks={0: (0, len(queue))}, queue=queue, topic_error=None
        )

    def test_dry_run_never_publishes(
        self, settings: Settings, patch_replay_clients, fake_replay_producer
    ) -> None:
        message = FakeDlqMessage(
            offset=0,
            headers=encode_headers(
                {"x-service": "inventory", "x-original-topic": "order-lifecycle"}
            ),
        )
        consumer = self._consumer([message])
        patch_replay_clients(consumer, fake_replay_producer)

        exit_code = replay(settings, make_args(publish=False))

        assert exit_code == 0
        assert fake_replay_producer.produced == []

    def test_publish_republishes_to_original_topic_with_no_headers(
        self, settings: Settings, patch_replay_clients, fake_replay_producer
    ) -> None:
        message = FakeDlqMessage(
            offset=0,
            key=b"ord-1",
            value=b'{"foo": "bar"}',
            headers=encode_headers(
                {"x-service": "inventory", "x-original-topic": "order-lifecycle"}
            ),
        )
        consumer = self._consumer([message])
        patch_replay_clients(consumer, fake_replay_producer)

        replay(settings, make_args(publish=True))

        assert fake_replay_producer.produced == [
            {"topic": "order-lifecycle", "key": b"ord-1", "value": b'{"foo": "bar"}'}
        ]

    def test_non_retryable_excluded_by_default(
        self, settings: Settings, patch_replay_clients, fake_replay_producer
    ) -> None:
        message = FakeDlqMessage(
            offset=0,
            headers=encode_headers(
                {
                    "x-service": "inventory",
                    "x-original-topic": "order-lifecycle",
                    "x-error-class": NonRetryableError.__name__,
                }
            ),
        )
        consumer = self._consumer([message])
        patch_replay_clients(consumer, fake_replay_producer)

        replay(settings, make_args(publish=True, include_poison=False))

        assert fake_replay_producer.produced == []

    def test_non_retryable_included_with_include_poison(
        self, settings: Settings, patch_replay_clients, fake_replay_producer
    ) -> None:
        message = FakeDlqMessage(
            offset=0,
            headers=encode_headers(
                {
                    "x-service": "inventory",
                    "x-original-topic": "order-lifecycle",
                    "x-error-class": NonRetryableError.__name__,
                }
            ),
        )
        consumer = self._consumer([message])
        patch_replay_clients(consumer, fake_replay_producer)

        replay(settings, make_args(publish=True, include_poison=True))

        assert len(fake_replay_producer.produced) == 1

    def test_service_filter_only_matches_named_service(
        self, settings: Settings, patch_replay_clients, fake_replay_producer
    ) -> None:
        inventory_msg = FakeDlqMessage(
            offset=0,
            headers=encode_headers(
                {"x-service": "inventory", "x-original-topic": "order-lifecycle"}
            ),
        )
        notification_msg = FakeDlqMessage(
            offset=1,
            headers=encode_headers(
                {"x-service": "notification", "x-original-topic": "order-lifecycle"}
            ),
        )
        consumer = self._consumer([inventory_msg, notification_msg])
        patch_replay_clients(consumer, fake_replay_producer)

        replay(settings, make_args(publish=True, service="inventory"))

        assert len(fake_replay_producer.produced) == 1

    def test_limit_stops_after_n_matches(
        self, settings: Settings, patch_replay_clients, fake_replay_producer
    ) -> None:
        messages = [
            FakeDlqMessage(
                offset=i,
                headers=encode_headers(
                    {"x-service": "inventory", "x-original-topic": "order-lifecycle"}
                ),
            )
            for i in range(3)
        ]
        consumer = FakeReplayConsumer(
            watermarks={0: (0, 3)}, queue=messages, topic_error=None
        )
        patch_replay_clients(consumer, fake_replay_producer)

        replay(settings, make_args(publish=True, limit=1))

        assert len(fake_replay_producer.produced) == 1

    def test_missing_original_topic_header_is_skipped(
        self, settings: Settings, patch_replay_clients, fake_replay_producer
    ) -> None:
        message = FakeDlqMessage(
            offset=0, headers=encode_headers({"x-service": "inventory"})
        )
        consumer = self._consumer([message])
        patch_replay_clients(consumer, fake_replay_producer)

        exit_code = replay(settings, make_args(publish=True))

        assert exit_code == 0
        assert fake_replay_producer.produced == []

    def test_stall_ends_run_and_returns_cleanly(
        self, settings: Settings, patch_replay_clients, fake_replay_producer
    ) -> None:
        # Watermark promises 2 messages but only 1 ever arrives — poll() then stalls.
        consumer = FakeReplayConsumer(
            watermarks={0: (0, 2)},
            queue=[
                FakeDlqMessage(
                    offset=0,
                    headers=encode_headers(
                        {"x-service": "inventory", "x-original-topic": "order-lifecycle"}
                    ),
                )
            ],
            topic_error=None,
        )
        patch_replay_clients(consumer, fake_replay_producer)

        exit_code = replay(settings, make_args(publish=True))

        assert exit_code == 0
        assert len(fake_replay_producer.produced) == 1
