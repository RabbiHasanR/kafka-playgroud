"""Fixtures shared by every spec's integration suite (not `order_service`-specific).

These tests talk to the real 3-broker compose cluster on
``localhost:9092,9094,9095`` — no mocked Kafka client anywhere under
``tests/integration/``, which is the whole reason this tree is separate from
``tests/unit/``. The developer is expected to have run ``docker compose up -d``
first; nothing here starts or waits for the stack to come up.
"""

import uuid

import pytest
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient

#: The host-exposed 3-broker cluster (`docker-compose.yml`), matching
#: ``Settings.kafka_bootstrap_servers``'s default exactly.
BOOTSTRAP_SERVERS = "localhost:9092,localhost:9094,localhost:9095"


@pytest.fixture(scope="session")
def bootstrap_servers() -> str:
    """Return the bootstrap string every fixture and test should use."""
    return BOOTSTRAP_SERVERS


@pytest.fixture(scope="session")
def kafka_admin_client(bootstrap_servers: str) -> AdminClient:
    """Return one shared `AdminClient`, or skip the whole run if no broker answers.

    A plain reachability probe, not a wait-for-broker loop — the project's own
    experiment culture is "run `docker compose up -d` first", not "the test harness
    boots infrastructure for you".
    """
    client = AdminClient({"bootstrap.servers": bootstrap_servers})
    try:
        client.list_topics(timeout=3)
    except KafkaException as exc:
        pytest.skip(
            f"Kafka broker not reachable at {bootstrap_servers} — "
            f"run `docker compose up -d` first ({exc})"
        )
    return client


@pytest.fixture
def unique_id() -> str:
    """Return a short id tying one test's scratch topics and group ids together."""
    return uuid.uuid4().hex[:8]
