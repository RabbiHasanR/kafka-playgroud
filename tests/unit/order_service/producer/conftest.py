"""Fixtures shared across the producer's unit tests.

Two boundaries get faked here, never the real thing:

- The HTTP layer: routes only ever read ``request.app.state.producer`` and
  ``request.app.state.orders``, so :class:`FakeRequest` stands in for
  ``fastapi.Request`` without needing an ASGI scope.
- The Kafka client: :class:`FakeKafkaProducer` stands in for
  ``confluent_kafka.Producer`` so :class:`LifecycleEventProducer` can be tested
  with no broker running.
"""

from collections.abc import Callable
from unittest.mock import MagicMock

import pytest

from order_service.config import Settings
from order_service.events import OrderCreatedPayload
from order_service.producer.kafka_producer import LifecycleEventProducer
from order_service.producer.orders import OrderStore


class _FakeMessage:
    """Stand-in for the ``Message`` a delivery callback receives."""

    def __init__(self, topic: str, partition: int, offset: int) -> None:
        self._topic = topic
        self._partition = partition
        self._offset = offset

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset


class FakeKafkaProducer:
    """Stand-in for ``confluent_kafka.Producer``.

    By default every ``produce()`` call fires its delivery callback immediately
    with a successful result, so most tests need no setup. Set
    ``raise_on_produce``, ``deliver_error``, or ``deliver_immediately = False``
    to exercise the other paths.
    """

    def __init__(self, config: dict[str, object]) -> None:
        self.config = config
        self.produced: list[dict[str, object]] = []
        self.deliver_immediately = True
        self.deliver_error: object | None = None
        self.raise_on_produce: Exception | None = None
        self.topics_on_broker: set[str] = set()

    def produce(
        self,
        topic: str,
        key: bytes,
        value: bytes | None,
        on_delivery: Callable[[object, object], None],
    ) -> None:
        if self.raise_on_produce is not None:
            raise self.raise_on_produce
        self.produced.append({"topic": topic, "key": key, "value": value})
        if self.deliver_immediately:
            msg = _FakeMessage(topic, partition=0, offset=42)
            on_delivery(self.deliver_error, msg)

    def poll(self, timeout: float) -> None:
        pass

    def flush(self, timeout: float) -> int:
        return 0

    def list_topics(self, timeout: float) -> object:
        topics = {name: object() for name in self.topics_on_broker}
        for name in topics:
            topics[name] = _FakeTopicMetadata()
        return _FakeClusterMetadata(topics)


class _FakeTopicMetadata:
    error = None


class _FakeClusterMetadata:
    def __init__(self, topics: dict[str, object]) -> None:
        self.topics = topics


@pytest.fixture
def fake_kafka_producer(monkeypatch: pytest.MonkeyPatch) -> FakeKafkaProducer:
    """Patch ``confluent_kafka.Producer`` and return the fake instance it creates."""
    fake = FakeKafkaProducer(config={})
    monkeypatch.setattr(
        "order_service.producer.kafka_producer.Producer", lambda config: fake
    )
    return fake


@pytest.fixture
def event_producer(fake_kafka_producer: FakeKafkaProducer) -> LifecycleEventProducer:
    """A :class:`LifecycleEventProducer` wired to the fake Kafka client."""
    settings = Settings(_env_file=None)
    return LifecycleEventProducer(settings)


@pytest.fixture
def store() -> OrderStore:
    return OrderStore()


@pytest.fixture
def mock_producer() -> MagicMock:
    """A producer double for route tests, spec'd so a typo raises immediately."""
    return MagicMock(spec=LifecycleEventProducer)


class _State:
    def __init__(self, producer: object, orders: OrderStore) -> None:
        self.producer = producer
        self.orders = orders


class _App:
    def __init__(self, state: _State) -> None:
        self.state = state


class FakeRequest:
    """Minimal stand-in for ``fastapi.Request``: only ``app.state`` is read."""

    def __init__(self, producer: object, orders: OrderStore) -> None:
        self.app = _App(_State(producer, orders))


@pytest.fixture
def fake_request(mock_producer: MagicMock, store: OrderStore) -> FakeRequest:
    return FakeRequest(mock_producer, store)


@pytest.fixture
def registered_order_id(store: OrderStore, make_item, make_payment) -> str:
    """Register one CREATED order in ``store`` and return its id."""
    payload = OrderCreatedPayload(
        customer_id="cust-1",
        items=[make_item()],
        total_amount=100,
        payment=make_payment(amount=100),
    )
    order = store.register("ord-1", payload)
    return order.order_id
