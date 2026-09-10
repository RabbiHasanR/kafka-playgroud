"""Unit tests for :mod:`order_service.consumer.dlq` against a faked producer.

No broker: :class:`~tests.unit.order_service.consumer.conftest.FakeProducer` stands in
for ``confluent_kafka.Producer``, the same pattern the producer suite uses for
``LifecycleEventProducer``.
"""

from datetime import timedelta

import pytest

from order_service.config import ProcessingGuarantee, Settings
from order_service.consumer.dlq import (
    HDR_ATTEMPT,
    HDR_ATTEMPTS_MADE,
    HDR_ERROR_CLASS,
    HDR_ORIGINAL_TOPIC,
    HDR_RETRY_AT,
    HDR_RETRY_TARGET,
    FailurePublishFailed,
    FailureRouter,
    Origin,
)
from order_service.events import utc_now

from tests.unit.order_service.consumer.conftest import FakeMessage, FakeProducer


@pytest.fixture
def origin() -> Origin:
    return Origin(topic="order-lifecycle", partition=0, offset=7, timestamp="unknown")


@pytest.fixture
def router(settings: Settings, fake_producer: FakeProducer) -> FailureRouter:
    return FailureRouter(settings, fake_producer)


def test_to_retry_schedules_due_time_from_backoff_and_carries_error_headers(
    router: FailureRouter, fake_producer: FakeProducer, origin: Origin, settings: Settings
) -> None:
    before = utc_now()
    due_at = router.to_retry(
        FakeMessage(),
        origin=origin,
        service="inventory",
        group_id="inventory-service",
        attempt=2,
        error=ValueError("boom"),
    )
    expected_backoff = settings.backoff_for_attempt(2)
    assert due_at >= before + timedelta(seconds=expected_backoff)

    [published] = fake_producer.produced
    assert published["topic"] == settings.retry_topic
    assert published["headers"][HDR_ATTEMPT] == "2"
    assert published["headers"][HDR_ERROR_CLASS] == "ValueError"
    assert published["headers"][HDR_RETRY_AT] == due_at.isoformat()


def test_to_dead_letter_carries_attempts_made_and_error_info(
    router: FailureRouter, fake_producer: FakeProducer, origin: Origin, settings: Settings
) -> None:
    router.to_dead_letter(
        FakeMessage(),
        origin=origin,
        service="inventory",
        group_id="inventory-service",
        attempts_made=3,
        error=RuntimeError("still broken"),
    )
    [published] = fake_producer.produced
    assert published["topic"] == settings.dlq_topic
    assert published["headers"][HDR_ATTEMPTS_MADE] == "3"
    assert published["headers"][HDR_ERROR_CLASS] == "RuntimeError"


def test_to_source_republishes_to_origin_topic_with_retry_target_and_attempt(
    router: FailureRouter, fake_producer: FakeProducer, origin: Origin
) -> None:
    topic = router.to_source(
        FakeMessage(),
        origin=origin,
        service="inventory",
        group_id="inventory-service",
        attempt=2,
    )
    assert topic == origin.topic
    [published] = fake_producer.produced
    assert published["topic"] == origin.topic
    assert published["headers"][HDR_RETRY_TARGET] == "inventory"
    assert published["headers"][HDR_ATTEMPT] == "2"


def test_publish_raises_when_delivery_reports_an_error(
    router: FailureRouter, fake_producer: FakeProducer, origin: Origin
) -> None:
    fake_producer.deliver_error = "broker rejected it"
    with pytest.raises(FailurePublishFailed):
        router.to_dead_letter(
            FakeMessage(),
            origin=origin,
            service="inventory",
            group_id="inventory-service",
            attempts_made=1,
            error=ValueError("x"),
        )


def test_publish_raises_when_flush_leaves_messages_unacknowledged(
    router: FailureRouter, fake_producer: FakeProducer, origin: Origin
) -> None:
    fake_producer.deliver_immediately = False
    fake_producer.flush_remaining = 1
    with pytest.raises(FailurePublishFailed):
        router.to_dead_letter(
            FakeMessage(),
            origin=origin,
            service="inventory",
            group_id="inventory-service",
            attempts_made=1,
            error=ValueError("x"),
        )


def test_publish_under_exactly_once_does_not_block_on_flush(
    fake_producer: FakeProducer, origin: Origin
) -> None:
    settings = Settings(
        _env_file=None, processing_guarantee=ProcessingGuarantee.EXACTLY_ONCE
    )
    router = FailureRouter(settings, fake_producer)
    # Nothing acknowledged and flush would report messages left, but exactly_once skips
    # the flush wait entirely — no FailurePublishFailed should be raised.
    fake_producer.deliver_immediately = False
    fake_producer.flush_remaining = 5
    router.to_dead_letter(
        FakeMessage(),
        origin=origin,
        service="inventory",
        group_id="inventory-service",
        attempts_made=1,
        error=ValueError("x"),
    )
    assert len(fake_producer.produced) == 1


def test_origin_from_headers_uses_provenance_headers_when_present() -> None:
    headers = {
        HDR_ORIGINAL_TOPIC: "order-lifecycle",
        "x-original-partition": "2",
        "x-original-offset": "99",
        "x-original-timestamp": "2026-01-01T00:00:00+00:00",
    }
    resolved = Origin.from_headers(headers, FakeMessage())
    assert resolved == Origin(
        topic="order-lifecycle",
        partition=2,
        offset=99,
        timestamp="2026-01-01T00:00:00+00:00",
    )


def test_origin_from_headers_falls_back_to_message_when_absent() -> None:
    message = FakeMessage(topic="order-lifecycle.retry", partition=1, offset=5)
    resolved = Origin.from_headers({}, message)
    assert resolved.topic == "order-lifecycle.retry"
    assert resolved.partition == 1
    assert resolved.offset == 5
