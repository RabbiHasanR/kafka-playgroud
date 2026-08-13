"""Environment-driven configuration shared by the order service and its consumers."""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime settings resolved from the environment.

    Attributes:
        kafka_bootstrap_servers: ``localhost:9092`` from the host, ``kafka:19092``
            from inside the compose network.
        consumer_group_id: When unset it is derived from ``service_name``; set it to
            an unused value to replay the topic from earliest.
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    kafka_bootstrap_servers: str = "localhost:9092"
    order_lifecycle_topic: str = "order-lifecycle"

    service_name: str = "inventory"
    consumer_group_id: str | None = None

    delivery_timeout_seconds: float = 10.0

    order_service_host: str = "0.0.0.0"
    order_service_port: int = 8010

    def group_id_for(self, service_name: str) -> str:
        """Return the configured group id, or ``<service_name>-service``."""
        return self.consumer_group_id or f"{service_name}-service"


@lru_cache
def get_settings() -> Settings:
    """Return the process-wide settings, resolved once."""
    return Settings()
