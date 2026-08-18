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


class ProducerAcks(StrEnum):
    """How many replicas must acknowledge a write before the producer calls it done
    (004 D5).

    ``NONE`` returns before the broker has confirmed anything, so a write lost in
    flight is never reported. ``LEADER`` waits for the partition leader alone, which a
    leader crash before replication can still lose. ``ALL`` waits for every replica
    currently in the in-sync set.

    ``ALL`` is the default and is what 001 through 003 ran with, hardcoded. Note what it
    does *not* promise: with no ``min.insync.replicas`` set on the topic, an ISR that has
    shrunk to one member still satisfies ``all`` (004 D8) — that gap is named in
    docs/replication.md and closed at 005.

    A ``StrEnum`` rather than a free string for the same reason ``GroupProtocol`` is one:
    an unrecognised value must fail at startup, not select a silent fallback (R4.7).
    """

    NONE = "0"
    LEADER = "1"
    ALL = "all"


class HandlerFailureMode(StrEnum):
    """How the failure lever makes handlers fail (005 D11).

    ``TRANSIENT`` raises a retryable error until ``handler_failure_attempts`` has been
    spent, then succeeds — which is what makes "attempt 2 recovered" observable rather
    than asserted. ``POISON`` raises a non-retryable error every time, so the message
    reaches the dead-letter topic having made exactly one attempt.

    ``NONE`` is the default, so a consumer started without this lever behaves exactly as
    004 recorded. This is 005's counterpart to 002's ``handler_delay_seconds`` and 003's
    ``state_crash_after``.

    A ``StrEnum`` for the same reason ``ProducerAcks`` is one: an unrecognised value must
    fail at startup rather than quietly selecting "do not fail" and producing a run that
    proves nothing.
    """

    NONE = "none"
    TRANSIENT = "transient"
    POISON = "poison"


class Settings(BaseSettings):
    """Runtime settings resolved from the environment.

    Every setting introduced by spec 002 defaults to the behaviour spec 001 recorded
    (R2.34), so a consumer started with none of them set behaves exactly as it did.

    Attributes:
        kafka_bootstrap_servers: All three brokers, so a client can still start while
            any one node is down (004 D3, R4.11). ``localhost:9092,9094,9095`` from the
            host; compose passes the ``kafka*:19092`` internal addresses.
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
        producer_acks: How many replicas must acknowledge a write (004 D5). Defaults to
            ``all``, which is what the producer hardcoded before this setting existed.
        retry_topic: Where a retryable failure waits out its backoff (005 D1). Consumed
            by the retry worker alone — no service consumer subscribes to it.
        dlq_topic: Where a message nothing could process ends up (005 D1). Consumed by
            **nothing**; that is what makes it terminal (005 D10).
        retry_max_attempts: Total attempts including the first, which the main consumer
            spends inline (005 D8). 3 means one inline attempt and two in the worker.
        retry_backoff_seconds: Comma-separated backoffs for the attempts *after* the
            first. Parsed by :attr:`retry_backoff_schedule`.
        producer_retries: How many times librdkafka retries a failed produce. Bounded in
            practice by ``producer_message_timeout_ms``, which caps the total.
        producer_message_timeout_ms: Total time a message may spend being retried before
            the delivery report reports failure — librdkafka's spelling of the Java
            client's ``delivery.timeout.ms`` (005 D12).
        handler_failure_mode: The failure lever (005 D11). Defaults to not failing.
        handler_failure_orders: Which order ids the lever applies to. Unset means every
            order, which is rarely what you want — name the orders.
        handler_failure_attempts: How many attempts ``transient`` fails before succeeding.
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    kafka_bootstrap_servers: str = "localhost:9092,localhost:9094,localhost:9095"
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

    # -- spec 004: the producer's half of the durability contract ---------------
    producer_acks: ProducerAcks = ProducerAcks.ALL

    # -- spec 005: retries, the dead-letter topic, and the failure lever --------
    retry_topic: str = "order-lifecycle.retry"
    dlq_topic: str = "order-lifecycle.dlq"
    retry_max_attempts: int = 3
    retry_backoff_seconds: str = "30,120"

    producer_retries: int = 3
    producer_retry_backoff_ms: int = 100
    producer_message_timeout_ms: int = 30_000

    handler_failure_mode: HandlerFailureMode = HandlerFailureMode.NONE
    handler_failure_orders: str | None = None
    handler_failure_attempts: int = 2

    delivery_timeout_seconds: float = 10.0

    order_service_host: str = "0.0.0.0"
    order_service_port: int = 8010

    @field_validator(
        "consumer_instance_id",
        "consumer_assignment_strategy",
        "consumer_remote_assignor",
        "consumer_instance_id_static",
        "state_db_dsn",
        "handler_failure_orders",
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

    @property
    def retry_backoff_schedule(self) -> list[float]:
        """Return the backoff, in seconds, for each attempt after the first (R5.10).

        Indexed by ``attempt - 2``: attempt 2 waits the first entry, attempt 3 the
        second. A list shorter than ``retry_max_attempts`` reuses its last entry rather
        than failing, so shortening the schedule cannot make the worker crash on an
        attempt it has no number for.
        """
        parsed = [
            float(part.strip())
            for part in self.retry_backoff_seconds.split(",")
            if part.strip()
        ]
        return parsed or [30.0]

    def backoff_for_attempt(self, attempt: int) -> float:
        """Return how long attempt ``attempt`` waits before it runs (R5.10).

        Args:
            attempt: The 1-based attempt being scheduled. Attempt 1 is spent inline by
                the main consumer and never waits, so anything below 2 waits nothing.
        """
        if attempt < 2:
            return 0.0
        schedule = self.retry_backoff_schedule
        return schedule[min(attempt - 2, len(schedule) - 1)]

    @property
    def failing_orders(self) -> frozenset[str]:
        """Return the order ids the failure lever applies to (R5.19).

        Empty means every order — which is why ``handler_failure_mode`` rather than this
        is what turns the lever on.
        """
        if self.handler_failure_orders is None:
            return frozenset()
        return frozenset(
            part.strip() for part in self.handler_failure_orders.split(",") if part.strip()
        )


@lru_cache
def get_settings() -> Settings:
    """Return the process-wide settings, resolved once."""
    return Settings()
