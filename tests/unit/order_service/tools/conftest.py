"""Fixtures shared across the operator tools' unit tests.

Two boundaries get faked, never the real thing:

- The Kafka client: :class:`FakeReplayConsumer` and :class:`FakeReplayProducer` stand
  in for ``confluent_kafka.Consumer``/``Producer`` restricted to what ``dlq_replay.py``
  actually calls, so ``snapshot_topic`` and ``replay`` can be tested with no broker.
- Nothing else: ``Snapshot.message_count`` and ``describe`` are pure functions and need
  no fixture at all.
"""

from collections.abc import Callable

import pytest
from confluent_kafka import TopicPartition

from order_service.config import Settings


def encode_headers(headers: dict[str, str]) -> list[tuple[str, bytes]]:
    """Build the ``(name, bytes)`` pairs :meth:`FakeDlqMessage.headers` returns."""
    return [(name, value.encode("utf-8")) for name, value in headers.items()]


class FakeDlqMessage:
    """Stand-in for ``confluent_kafka.Message`` as read off the dead-letter topic."""

    def __init__(
        self,
        *,
        partition: int = 0,
        offset: int = 0,
        key: bytes | None = b"ord-1",
        value: bytes | None = b"{}",
        headers: list[tuple[str, bytes | None]] | None = None,
        error: object | None = None,
    ) -> None:
        self._partition = partition
        self._offset = offset
        self._key = key
        self._value = value
        self._headers = headers
        self._error = error

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def key(self) -> bytes | None:
        return self._key

    def value(self) -> bytes | None:
        return self._value

    def headers(self) -> list[tuple[str, bytes | None]] | None:
        return self._headers

    def error(self) -> object | None:
        return self._error


class _FakeTopicMetadata:
    def __init__(self, partitions: list[int], error: object | None = None) -> None:
        self.partitions = {p: object() for p in partitions}
        self.error = error


class _FakeClusterMetadata:
    def __init__(self, topics: dict[str, _FakeTopicMetadata]) -> None:
        self.topics = topics


class FakeReplayConsumer:
    """Stand-in for ``confluent_kafka.Consumer``, scoped to what ``dlq_replay`` calls.

    ``watermarks`` maps partition -> ``(low, high)``. ``queue`` is the ordered sequence
    of messages ``poll()`` yields; once exhausted, ``poll()`` returns ``None`` to
    simulate a stall, matching what a real consumer does when nothing new arrives.
    """

    def __init__(
        self,
        *,
        watermarks: dict[int, tuple[int, int]],
        queue: list[FakeDlqMessage] | None = None,
        topic_error: object | None = "missing",
    ) -> None:
        self.watermarks = watermarks
        self.queue = list(queue or [])
        self.topic_error = topic_error
        self.assigned: list[TopicPartition] = []
        self.closed = False

    def list_topics(self, topic: str, timeout: float) -> _FakeClusterMetadata:
        del timeout
        if self.topic_error == "missing":
            return _FakeClusterMetadata(topics={})
        metadata = _FakeTopicMetadata(
            partitions=list(self.watermarks), error=self.topic_error
        )
        return _FakeClusterMetadata(topics={topic: metadata})

    def get_watermark_offsets(
        self, tp: TopicPartition, timeout: float, cached: bool
    ) -> tuple[int, int]:
        del timeout, cached
        return self.watermarks[tp.partition]

    def assign(self, partitions: list[TopicPartition]) -> None:
        self.assigned = partitions

    def poll(self, timeout: float) -> FakeDlqMessage | None:
        del timeout
        if not self.queue:
            return None
        return self.queue.pop(0)

    def close(self) -> None:
        self.closed = True


class FakeReplayProducer:
    """Stand-in for ``confluent_kafka.Producer``, recording every publish."""

    def __init__(self) -> None:
        self.produced: list[dict[str, object]] = []
        self.flush_remaining = 0

    def produce(self, *, topic: str, key: bytes | None, value: bytes | None) -> None:
        self.produced.append({"topic": topic, "key": key, "value": value})

    def flush(self, timeout: float) -> int:
        del timeout
        return self.flush_remaining


@pytest.fixture
def settings() -> Settings:
    """Default settings, isolated from the environment (no ``.env`` picked up)."""
    return Settings(_env_file=None)


@pytest.fixture
def fake_replay_producer() -> FakeReplayProducer:
    return FakeReplayProducer()


@pytest.fixture
def patch_replay_clients(
    monkeypatch: pytest.MonkeyPatch,
) -> Callable[[FakeReplayConsumer, FakeReplayProducer], None]:
    """Patch ``dlq_replay.build_consumer`` and its module-level ``Producer``."""

    def _patch(consumer: FakeReplayConsumer, producer: FakeReplayProducer) -> None:
        monkeypatch.setattr(
            "order_service.tools.dlq_replay.build_consumer", lambda settings: consumer
        )
        monkeypatch.setattr(
            "order_service.tools.dlq_replay.Producer", lambda config: producer
        )

    return _patch
