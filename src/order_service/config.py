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


class StateBackend(StrEnum):
    """Where a consumer keeps its folded per-order state (003 D8).

    ``MEMORY`` is the default so that a consumer started with none of spec 003's
    settings behaves exactly as 002 recorded, keeping 001's and 002's experiments
    reproducible without a database running (R3.20, R3.27). Compose sets ``POSTGRES``
    explicitly, so the assembled system runs the durable path.
    """

    MEMORY = "memory"
    POSTGRES = "postgres"


class StateWriteOrder(StrEnum):
    """Which of the two writes goes first (003 D4).

    ``STATE_FIRST`` is not a preference. The reverse order does not merely risk a
    duplicate — a crash in its window loses the event from the fold permanently, and
    logs nothing while doing it. ``OFFSET_FIRST`` exists so that failure can be watched
    rather than asserted (R3.18).
    """

    STATE_FIRST = "state_first"
    OFFSET_FIRST = "offset_first"


class StateCrashPoint(StrEnum):
    """Where to kill the process on purpose (003 D5).

    The window between the state write and the offset commit is microseconds wide and
    cannot be hit by hand, so without a deliberate crash point R3.17 and R3.18 are
    unreachable and the dual-write problem stays a claim. This is 003's counterpart to
    001's ``force`` flag and 002's ``handler_delay_seconds``.
    """

    NONE = "none"
    STATE_WRITE = "state_write"
    OFFSET_COMMIT = "offset_commit"


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
        state_backend: Where folded state lives (003 D8). Defaults to ``memory``, which
            is 002's behaviour; compose sets ``postgres``.
        state_db_dsn: libpq connection string, required when the backend is
            ``postgres``. Empty means unset, for the same Compose reason as above.
        state_write_order: Which of the state write and the offset commit goes first
            (003 D4). The default is the correct one; the other is a lever.
        state_crash_after: Where to kill the process on purpose, to open the dual-write
            window (003 D5). Defaults to not crashing.
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

    # -- spec 003: durable consumer state, and the two levers -------------------
    state_backend: StateBackend = StateBackend.MEMORY
    state_db_dsn: str | None = None
    state_write_order: StateWriteOrder = StateWriteOrder.STATE_FIRST
    state_crash_after: StateCrashPoint = StateCrashPoint.NONE

    delivery_timeout_seconds: float = 10.0

    order_service_host: str = "0.0.0.0"
    order_service_port: int = 8010

    @field_validator(
        "consumer_instance_id",
        "consumer_assignment_strategy",
        "consumer_remote_assignor",
        "consumer_instance_id_static",
        "state_db_dsn",
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
