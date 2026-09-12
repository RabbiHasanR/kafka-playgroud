"""Scratch-topic creation for `order_service`'s integration tests.

Talks to `AdminClient` directly rather than shelling out to
`scripts/create_topics.sh`: one broker round-trip per topic (`NewTopic(config=...)`
sets `min.insync.replicas` at creation, with no separate `kafka-configs.sh --alter`
pass needed), and each test asks for exactly the scratch topics it needs instead of
sharing the script's singular `ORDER_LIFECYCLE_TOPIC`/`RETRY_TOPIC`/`DLQ_TOPIC`
overrides.
"""

import logging
from dataclasses import dataclass, field

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

logger = logging.getLogger(__name__)

#: How long to wait for a create/delete future to settle.
_ADMIN_TIMEOUT_SECONDS = 15.0


@dataclass(frozen=True)
class TopicSpec:
    """One topic to create for a test."""

    name: str
    partitions: int = 3
    replication_factor: int = 3
    config: dict[str, str] = field(default_factory=dict)


def create_scratch_topics(admin_client: AdminClient, specs: list[TopicSpec]) -> None:
    """Create every topic in `specs`, waiting for each to actually exist.

    Raises:
        KafkaException: If a topic could not be created for any reason other than
            already existing (scratch names are UUID-suffixed, so a collision is
            practically impossible, but tolerating it is cheap).
    """
    new_topics = [
        NewTopic(
            spec.name,
            num_partitions=spec.partitions,
            replication_factor=spec.replication_factor,
            config=spec.config,
        )
        for spec in specs
    ]
    futures = admin_client.create_topics(new_topics)
    for name, future in futures.items():
        try:
            future.result(timeout=_ADMIN_TIMEOUT_SECONDS)
        except KafkaException as exc:
            if "already exists" in str(exc).lower():
                continue
            raise


def delete_scratch_topics(admin_client: AdminClient, names: list[str]) -> None:
    """Best-effort delete of scratch topics — never raises.

    A leaked `it-*` topic is harmless clutter, never a correctness risk, so teardown
    logs a warning rather than failing the test that is already tearing down.
    """
    if not names:
        return
    futures = admin_client.delete_topics(names)
    for name, future in futures.items():
        try:
            future.result(timeout=_ADMIN_TIMEOUT_SECONDS)
        except KafkaException as exc:
            logger.warning("could not delete scratch topic %s: %s", name, exc)
