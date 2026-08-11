"""Environment-driven configuration shared by the producer and the consumer.

Nothing here carries a hardcoded connection string. The only difference between
running on the host and running inside the compose network is the value of
``KAFKA_BOOTSTRAP_SERVERS`` (R1.33).
"""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime settings resolved from the environment.

    Attributes:
        kafka_bootstrap_servers: Broker address. ``localhost:9092`` from the host,
            ``kafka:19092`` from inside the compose network.
        order_events_topic: Topic carrying order lifecycle events.
        consumer_group_id: Consumer group identity. Changing it to an unused value
            makes the consumer replay the topic from its earliest retained offset.
        delivery_timeout_seconds: How long a single-event publish waits for the
            broker's delivery report before giving up.
        producer_host: Bind address for the producer HTTP API.
        producer_port: Bind port for the producer HTTP API.
        consumer_state_host: Bind address for the consumer's state-dump server.
        consumer_state_port: Bind port for the consumer's state-dump server.
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    kafka_bootstrap_servers: str = "localhost:9092"
    order_events_topic: str = "order-events"
    consumer_group_id: str = "order-processor"

    delivery_timeout_seconds: float = 10.0

    producer_host: str = "0.0.0.0"
    producer_port: int = 8000
    consumer_state_host: str = "0.0.0.0"
    consumer_state_port: int = 8001


@lru_cache
def get_settings() -> Settings:
    """Return the process-wide settings, resolved once.

    Returns:
        The cached :class:`Settings` instance.
    """
    return Settings()
