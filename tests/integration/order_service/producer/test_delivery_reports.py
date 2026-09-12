"""Real delivery reports through the HTTP surface (spec 001, R1.17, R1.18).

Confirms the partition/offset the API returns are the broker's real numbers, not
something the producer wrapper echoed back — which only a real broker can prove.
"""

from collections.abc import Callable, Iterator

import pytest
from confluent_kafka import Consumer, TopicPartition
from fastapi.testclient import TestClient

from order_service.config import Settings

pytestmark = pytest.mark.integration


def _watermark(bootstrap_servers: str, topic: str, partition: int) -> int:
    observer = Consumer({"bootstrap.servers": bootstrap_servers, "group.id": "it-watermark-probe"})
    try:
        _low, high = observer.get_watermark_offsets(
            TopicPartition(topic, partition), timeout=10.0, cached=False
        )
        return high
    finally:
        observer.close()


@pytest.fixture
def app_client(
    monkeypatch: pytest.MonkeyPatch,
    scratch_topics: Callable[..., dict[str, str]],
    settings_factory: Callable[..., Settings],
) -> Iterator[tuple[TestClient, dict[str, str]]]:
    """Boot the real FastAPI app against scratch topics, tearing its producer down after."""
    topics = scratch_topics(lifecycle=True, snapshot=True)
    settings = settings_factory(
        order_lifecycle_topic=topics["lifecycle"], order_snapshot_topic=topics["snapshot"]
    )

    import order_service.producer.app as app_module

    monkeypatch.setattr(app_module, "get_settings", lambda: settings)

    with TestClient(app_module.app) as client:
        yield client, topics


def test_order_creation_returns_real_partition_and_offset(
    app_client: tuple[TestClient, dict[str, str]], bootstrap_servers: str
) -> None:
    client, topics = app_client
    body = {
        "customer_id": "cust-1",
        "items": [{"sku": "SKU-1", "qty": 2, "unit_price": 500}],
        "payment": {"method": "CARD", "reference": "ref-1", "amount": 1000},
    }

    response = client.post("/orders", json=body)
    assert response.status_code == 201
    created = response.json()

    assert created["partition"] >= 0
    assert created["offset"] >= 0
    high_after = _watermark(bootstrap_servers, topics["lifecycle"], created["partition"])
    assert created["offset"] < high_after

    advance = client.post(
        f"/orders/{created['order_id']}/events",
        json={"event_type": "PACKED", "payload": {}},
    )
    assert advance.status_code == 200
    advanced = advance.json()

    # Same order_id, so the same partition (R1.10) — incidental here, the focus is the
    # partition/offset being real broker-assigned values.
    assert advanced["partition"] == created["partition"]
    assert advanced["offset"] > created["offset"]
