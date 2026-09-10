"""Unit tests for :class:`~order_service.consumer.retry_worker.RetryWorker`.

Exercises the routing decision in ``_handle_message`` — defer / release / dead-letter —
against faked ``Consumer`` and ``Producer`` clients. The handler itself is never run
here: from 007 the worker is purely a scheduler (see the module docstring), so what
matters is which of the three destinations a message reaches, not any handler logic.
"""

from datetime import timedelta

import pytest

from order_service.config import Settings
from order_service.consumer.dlq import HDR_ATTEMPT, HDR_RETRY_AT, HDR_SERVICE, FailureRouter
from order_service.consumer.retry_worker import RetryWorker, _int_header, _time_header
from order_service.consumer.runtime import ServiceSpec
from order_service.events import utc_now

from tests.unit.order_service.consumer.conftest import FakeConsumer, FakeMessage, FakeProducer, encode_headers


@pytest.fixture
def fake_consumer(monkeypatch: pytest.MonkeyPatch) -> FakeConsumer:
    fake = FakeConsumer()
    monkeypatch.setattr(
        "order_service.consumer.retry_worker.Consumer", lambda config: fake
    )
    return fake


@pytest.fixture
def fake_producer() -> FakeProducer:
    return FakeProducer()


@pytest.fixture
def worker(
    settings: Settings, fake_consumer: FakeConsumer, fake_producer: FakeProducer
) -> RetryWorker:
    specs = {"inventory": ServiceSpec(name="inventory", handlers={})}
    router = FailureRouter(settings, fake_producer)
    return RetryWorker(settings, specs, router)


def _message(headers: dict[str, str], value: bytes = b'{"order_id": "ord-1"}') -> FakeMessage:
    return FakeMessage(headers=encode_headers(headers), value=value)


def _valid_event_json() -> bytes:
    return (
        b'{"order_id": "ord-1", "sequence": 2, "event_type": "ORDER_CREATED", '
        b'"payload": {"customer_id": "c", "items": [{"sku": "s", "qty": 1, '
        b'"unit_price": 100}], "total_amount": 100, '
        b'"payment": {"method": "CARD", "reference": "r", "amount": 100}}}'
    )


def test_handle_message_defers_when_not_yet_due(
    worker: RetryWorker, fake_consumer: FakeConsumer, fake_producer: FakeProducer
) -> None:
    due_at = utc_now() + timedelta(hours=1)
    message = _message({HDR_SERVICE: "inventory", HDR_RETRY_AT: due_at.isoformat()})

    worker._handle_message(message)

    assert fake_consumer.paused == [(message.topic(), message.partition())]
    assert fake_consumer.sought
    assert fake_producer.produced == []
    assert not fake_consumer.committed


def test_handle_message_unknown_service_goes_to_dead_letter(
    worker: RetryWorker, fake_consumer: FakeConsumer, fake_producer: FakeProducer, settings: Settings
) -> None:
    message = _message({HDR_SERVICE: "does-not-exist"}, value=_valid_event_json())

    worker._handle_message(message)

    [published] = fake_producer.produced
    assert published["topic"] == settings.dlq_topic
    assert fake_consumer.committed == [message]


def test_handle_message_undecodable_value_goes_to_dead_letter(
    worker: RetryWorker, fake_consumer: FakeConsumer, fake_producer: FakeProducer, settings: Settings
) -> None:
    message = _message({HDR_SERVICE: "inventory"}, value=b"not json")

    worker._handle_message(message)

    [published] = fake_producer.produced
    assert published["topic"] == settings.dlq_topic
    assert fake_consumer.committed == [message]


def test_handle_message_due_and_known_service_releases_to_source(
    worker: RetryWorker, fake_consumer: FakeConsumer, fake_producer: FakeProducer
) -> None:
    message = _message(
        {HDR_SERVICE: "inventory", HDR_ATTEMPT: "2"}, value=_valid_event_json()
    )

    worker._handle_message(message)

    [published] = fake_producer.produced
    assert published["topic"] == message.topic()
    assert fake_consumer.committed == [message]


def test_int_header_falls_back_to_default_on_malformed_value() -> None:
    assert _int_header({"x-attempt": "nope"}, "x-attempt", default=2) == 2
    assert _int_header({}, "x-attempt", default=2) == 2
    assert _int_header({"x-attempt": "5"}, "x-attempt", default=2) == 5


def test_time_header_falls_back_to_none_on_malformed_value() -> None:
    assert _time_header({"x-retry-at": "not-a-time"}, "x-retry-at") is None
    assert _time_header({}, "x-retry-at") is None
