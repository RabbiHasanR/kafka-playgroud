"""Fixtures shared across the consumer's unit tests.

Two boundaries get faked here, never the real thing:

- The Kafka client: :class:`FakeMessage` stands in for ``confluent_kafka.Message``,
  :class:`FakeProducer` for ``confluent_kafka.Producer`` (with header support, unlike
  the producer side's fake, since ``dlq.py`` publishes headers on every hop), and
  :class:`FakeConsumer` for ``confluent_kafka.Consumer`` (pause/resume/seek/commit
  recorded, never sent anywhere).
- Nothing else: ``apply_event``, ``classify``, and the header-parsing helpers are pure
  functions and need no fixture at all.
"""

from collections.abc import Callable

import pytest

from order_service.config import Settings


class FakeMessage:
    """Stand-in for ``confluent_kafka.Message``.

    Only the accessors the consumer code actually calls are implemented.
    """

    def __init__(
        self,
        *,
        topic: str = "order-lifecycle",
        partition: int = 0,
        offset: int = 0,
        key: bytes | None = b"ord-1",
        value: bytes | None = b"{}",
        headers: list[tuple[str, bytes | None]] | None = None,
        timestamp_ms: int | None = None,
        error: object | None = None,
    ) -> None:
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._key = key
        self._value = value
        self._headers = headers
        self._timestamp_ms = timestamp_ms
        self._error = error

    def topic(self) -> str:
        return self._topic

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

    def timestamp(self) -> tuple[int, int]:
        if self._timestamp_ms is None:
            return (0, -1)
        return (1, self._timestamp_ms)

    def error(self) -> object | None:
        return self._error


def encode_headers(headers: dict[str, str]) -> list[tuple[str, bytes]]:
    """Build the ``(name, bytes)`` pairs :meth:`FakeMessage.headers` returns."""
    return [(name, value.encode("utf-8")) for name, value in headers.items()]


class FakeProducer:
    """Stand-in for ``confluent_kafka.Producer``, with header support.

    By default every ``produce()`` call fires its delivery callback immediately with a
    successful result. Set ``deliver_error`` or ``deliver_immediately = False`` to
    exercise ``FailureRouter``'s error paths.
    """

    def __init__(self) -> None:
        self.produced: list[dict[str, object]] = []
        self.deliver_immediately = True
        self.deliver_error: object | None = None
        self.raise_on_produce: Exception | None = None
        self.flush_remaining = 0

    def produce(
        self,
        *,
        topic: str,
        key: bytes | None,
        value: bytes | None,
        headers: list[tuple[str, bytes]] | None = None,
        on_delivery: Callable[[object, object], None],
    ) -> None:
        if self.raise_on_produce is not None:
            raise self.raise_on_produce
        self.produced.append(
            {"topic": topic, "key": key, "value": value, "headers": dict(
                (name, value.decode("utf-8")) for name, value in (headers or [])
            )}
        )
        if self.deliver_immediately:
            on_delivery(self.deliver_error, FakeMessage(topic=topic))

    def flush(self, timeout: float) -> int:
        return self.flush_remaining


class FakeConsumer:
    """Stand-in for ``confluent_kafka.Consumer``, recording calls rather than sending any."""

    def __init__(self) -> None:
        self.paused: list[tuple[str, int]] = []
        self.resumed: list[tuple[str, int]] = []
        self.sought: list[tuple[str, int, int]] = []
        self.committed: list[object] = []

    def subscribe(self, topics: list[str], **kwargs: object) -> None:
        del topics, kwargs

    def pause(self, partitions: list[object]) -> None:
        self.paused.extend((tp.topic, tp.partition) for tp in partitions)

    def resume(self, partitions: list[object]) -> None:
        self.resumed.extend((tp.topic, tp.partition) for tp in partitions)

    def seek(self, tp: object) -> None:
        self.sought.append((tp.topic, tp.partition, tp.offset))

    def commit(self, message: object, asynchronous: bool = False) -> None:
        del asynchronous
        self.committed.append(message)

    def close(self) -> None:
        pass


@pytest.fixture
def fake_producer() -> FakeProducer:
    return FakeProducer()


@pytest.fixture
def settings() -> Settings:
    """Default settings, isolated from the environment (no ``.env`` picked up)."""
    return Settings(_env_file=None)
