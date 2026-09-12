"""The retry worker's due-time pause/resume against a real broker (spec 005, R5.8).

`pause()`/`seek()`/resume on a live partition, without tripping `max.poll.interval.ms`,
is consumer-group mechanics a mocked client can't reproduce — this drives the real
`RetryWorker` against a real retry topic.

The 30s/120s production backoff schedule is overridden to ~2s so this test runs in
single-digit seconds rather than half a minute.
"""

import time
from collections.abc import Callable
from datetime import timedelta

import pytest

from order_service.config import Settings
from order_service.consumer.dlq import (
    HDR_ATTEMPT,
    HDR_ORIGINAL_OFFSET,
    HDR_ORIGINAL_PARTITION,
    HDR_ORIGINAL_TIMESTAMP,
    HDR_ORIGINAL_TOPIC,
    HDR_RETRY_AT,
    HDR_SERVICE,
    FailureRouter,
)
from order_service.consumer.main import SERVICE_REGISTRY
from order_service.consumer.retry_worker import RetryWorker
from order_service.consumer.transactions import build_producer
from order_service.events import EventType, LifecycleEvent, utc_now
from tests.integration.order_service.conftest import (
    running_consumer,
    topic_high_watermark,
    wait_until,
)

pytestmark = pytest.mark.integration

_DUE_SECONDS = 2.0


def test_worker_releases_a_message_only_after_its_due_time(
    bootstrap_servers: str,
    scratch_topics: Callable[..., dict[str, str]],
    settings_factory: Callable[..., Settings],
    unique_group_id_factory: Callable[[str], str],
) -> None:
    topics = scratch_topics(retry=True, lifecycle=True, partitions=1)
    settings = settings_factory(
        retry_topic=topics["retry"],
        retry_backoff_seconds=str(_DUE_SECONDS),
        consumer_group_id=unique_group_id_factory("retry-worker"),
    )
    specs = {name: factory() for name, factory in SERVICE_REGISTRY.items()}
    worker_producer = build_producer(
        settings, group_id=settings.consumer_group_id, instance=settings.instance_label
    )
    router = FailureRouter(settings, worker_producer)
    worker = RetryWorker(settings, specs, router)

    hand_producer = build_producer(settings, group_id="it-retry-due-time-source", instance="test")
    due_at = utc_now() + timedelta(seconds=_DUE_SECONDS)
    event = LifecycleEvent(
        order_id="it-due-time-order", sequence=1, event_type=EventType.ORDER_CREATED
    )
    headers = [
        (HDR_SERVICE, b"inventory"),
        (HDR_ORIGINAL_TOPIC, topics["lifecycle"].encode("utf-8")),
        (HDR_ORIGINAL_PARTITION, b"0"),
        (HDR_ORIGINAL_OFFSET, b"0"),
        (HDR_ORIGINAL_TIMESTAMP, utc_now().isoformat().encode("utf-8")),
        (HDR_ATTEMPT, b"2"),
        (HDR_RETRY_AT, due_at.isoformat().encode("utf-8")),
    ]

    try:
        with running_consumer(worker):
            wait_until(
                lambda: len(worker._consumer.assignment()) > 0,  # noqa: SLF001
                description="retry worker assigned the retry topic's partitions",
            )

            produced_at = time.monotonic()
            hand_producer.produce(
                topic=topics["retry"],
                key=event.order_id.encode("utf-8"),
                value=event.model_dump_json().encode("utf-8"),
                headers=headers,
            )
            hand_producer.flush(10.0)

            # Not due yet — the worker must have paused the partition rather than
            # releasing the message early.
            time.sleep(_DUE_SECONDS * 0.4)
            assert topic_high_watermark(bootstrap_servers, topics["lifecycle"]) == 0, (
                "message was released before its due time"
            )

            wait_until(
                lambda: topic_high_watermark(bootstrap_servers, topics["lifecycle"]) >= 1,
                timeout=15.0,
                description="worker republishes the due message to its origin topic",
            )
            elapsed = time.monotonic() - produced_at
            assert elapsed >= _DUE_SECONDS * 0.8, (
                f"message released after only {elapsed:.1f}s, before its {_DUE_SECONDS}s due time"
            )
    finally:
        worker_producer.flush(10.0)
        hand_producer.flush(10.0)
