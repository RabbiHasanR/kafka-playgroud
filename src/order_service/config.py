"""Environment-driven configuration shared by the order service and its consumers."""

import socket
from enum import StrEnum
from functools import lru_cache

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class GroupProtocol(StrEnum):
    """Which consumer group protocol a consumer joins with (002 D4, X9).

    Under ``CLASSIC`` an elected client computes the partition assignment and uploads
    it for the group. Under ``CONSUMER`` (KIP-848, GA in Kafka 4.0) the broker computes
    it and pushes each member its own share. The two accept partly disjoint client
    settings, which is why the choice is a type rather than a free string.
    """

    CLASSIC = "classic"
    CONSUMER = "consumer"


class Settings(BaseSettings):
    """Runtime settings resolved from the environment.

    Every setting introduced by spec 002 defaults to the behaviour spec 001 recorded
    (R2.34), so a consumer started with none of them set behaves exactly as it did.

    Attributes:
        kafka_bootstrap_servers: ``localhost:9092`` from the host, ``kafka:19092``
            from inside the compose network.
        consumer_group_id: When unset it is derived from ``service_name``; set it to
            an unused value to replay the topic from earliest.
        consumer_instance_id: Per-process identity used only in log lines, so three
            members of one group can be told apart. Falls back to the hostname.
        consumer_assignment_strategy: Classic protocol only. Unset leaves librdkafka's
            default (``range``).
        consumer_remote_assignor: KIP-848 only. Unset leaves the broker's default
            (``uniform``).
        consumer_session_timeout_ms: Classic protocol only — under KIP-848 the session
            timeout is broker-side and sending it raises.
        handler_delay_seconds: The eviction lever (002 D9). Sleeping past
            ``consumer_max_poll_interval_ms`` gets the member thrown out of its group
            while its process stays alive.
        consumer_instance_id_static: Sets ``group.instance.id`` for static membership.
            An empty value means unset, because Compose interpolation yields ``""``
            rather than removing the variable.
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    kafka_bootstrap_servers: str = "localhost:9092"
    order_lifecycle_topic: str = "order-lifecycle"

    service_name: str = "inventory"
    consumer_group_id: str | None = None

    # -- spec 002: membership, protocol, and the levers -------------------------
    consumer_instance_id: str | None = None
    consumer_group_protocol: GroupProtocol = GroupProtocol.CLASSIC
    consumer_assignment_strategy: str | None = None
    consumer_remote_assignor: str | None = None
    consumer_session_timeout_ms: int | None = None
    consumer_max_poll_interval_ms: int | None = None
    handler_delay_seconds: float = 0.0
    consumer_instance_id_static: str | None = None

    delivery_timeout_seconds: float = 10.0

    order_service_host: str = "0.0.0.0"
    order_service_port: int = 8010

    @field_validator(
        "consumer_instance_id",
        "consumer_assignment_strategy",
        "consumer_remote_assignor",
        "consumer_instance_id_static",
        mode="before",
    )
    @classmethod
    def _blank_is_unset(cls, value: object) -> object:
        """Treat an empty or whitespace-only environment value as unset.

        Compose's ``${VAR:+replacement}`` form yields ``""`` when the variable is off
        rather than removing it, and an empty ``group.instance.id`` would be a real,
        shared, static identity — worse than either intended state (002 D10).
        """
        if isinstance(value, str) and not value.strip():
            return None
        return value

    def group_id_for(self, service_name: str) -> str:
        """Return the configured group id, or ``<service_name>-service``."""
        return self.consumer_group_id or f"{service_name}-service"

    @property
    def instance_label(self) -> str:
        """Return this process's log identity, defaulting to the hostname (R2.7)."""
        return self.consumer_instance_id or socket.gethostname()


@lru_cache
def get_settings() -> Settings:
    """Return the process-wide settings, resolved once."""
    return Settings()
