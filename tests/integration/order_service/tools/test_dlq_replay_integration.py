"""`dlq_replay.py`'s watermark-bounded read against a real broker (spec 005, R5.17).

The property under test is the bound itself: a message produced *after* the run's
snapshot — exactly what a replay's own republish provokes in production — must not
be read by that same run. That's an offset-arithmetic guarantee against real broker
watermarks, not something a mocked consumer's canned message queue can prove.
"""

import argparse
import logging
import re
from collections.abc import Callable

import pytest
from confluent_kafka import Producer

from order_service.config import Settings
from order_service.consumer.dlq import (
    HDR_ATTEMPTS_MADE,
    HDR_ERROR_CLASS,
    HDR_ERROR_MESSAGE,
    HDR_FAILED_AT,
    HDR_ORIGINAL_OFFSET,
    HDR_ORIGINAL_PARTITION,
    HDR_ORIGINAL_TIMESTAMP,
    HDR_ORIGINAL_TOPIC,
    HDR_SERVICE,
)
from order_service.events import utc_now
from order_service.tools.dlq_replay import build_consumer, replay, snapshot_topic
from tests.integration.order_service.conftest import topic_high_watermark

pytestmark = pytest.mark.integration


def _dead_letter_headers(*, original_topic: str, order_id: str) -> list[tuple[str, bytes]]:
    fields = {
        HDR_SERVICE: "inventory",
        "x-consumer-group": "it-inventory-service",
        HDR_ORIGINAL_TOPIC: original_topic,
        HDR_ORIGINAL_PARTITION: "0",
        HDR_ORIGINAL_OFFSET: "0",
        HDR_ORIGINAL_TIMESTAMP: utc_now().isoformat(),
        HDR_ATTEMPTS_MADE: "3",
        HDR_ERROR_CLASS: "RetryableError",
        HDR_ERROR_MESSAGE: f"downstream failure for {order_id}",
        HDR_FAILED_AT: utc_now().isoformat(),
    }
    return [(name, value.encode("utf-8")) for name, value in fields.items()]


def make_args(*, publish: bool, limit: int = 0) -> argparse.Namespace:
    return argparse.Namespace(publish=publish, service=None, include_poison=False, limit=limit)


def test_run_excludes_a_message_produced_after_its_snapshot(
    bootstrap_servers: str,
    scratch_topics: Callable[..., dict[str, str]],
    settings_factory: Callable[..., Settings],
    caplog: pytest.LogCaptureFixture,
) -> None:
    topics = scratch_topics(dlq=True, lifecycle=True, partitions=1)
    settings = settings_factory(dlq_topic=topics["dlq"])

    producer = Producer({"bootstrap.servers": bootstrap_servers})
    n_before_snapshot = 3
    for i in range(n_before_snapshot):
        order_id = f"it-dlq-replay-order-{i}"
        producer.produce(
            topic=topics["dlq"],
            key=order_id.encode("utf-8"),
            value=b"{}",
            headers=_dead_letter_headers(original_topic=topics["lifecycle"], order_id=order_id),
        )
    producer.flush(10.0)

    inspect_consumer = build_consumer(settings)
    try:
        snapshot = snapshot_topic(inspect_consumer, settings.dlq_topic)
        assert snapshot.message_count == n_before_snapshot
    finally:
        inspect_consumer.close()

    # Produced AFTER the snapshot — exactly what a live replay's own republish would
    # cause in production. This run must not see it.
    producer.produce(
        topic=topics["dlq"],
        key=b"it-dlq-replay-post-snapshot",
        value=b"{}",
        headers=_dead_letter_headers(
            original_topic=topics["lifecycle"], order_id="it-dlq-replay-post-snapshot"
        ),
    )
    producer.flush(10.0)

    caplog.set_level(logging.INFO, logger="dlq_replay")
    exit_code = replay(settings, make_args(publish=True))
    assert exit_code == 0

    summary = next(
        message
        for message in (record.getMessage() for record in caplog.records)
        if "dead letter(s) read" in message
    )
    seen, matched = (int(n) for n in re.findall(r"\d+", summary))
    assert (seen, matched) == (n_before_snapshot, n_before_snapshot), (
        f"expected exactly {n_before_snapshot} messages from the pre-snapshot run, got {summary!r}"
    )

    assert topic_high_watermark(bootstrap_servers, topics["lifecycle"]) == n_before_snapshot
