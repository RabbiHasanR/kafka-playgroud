"""Environment-driven configuration shared by the order service and its consumers.

Nothing here carries a hardcoded connection string. The only difference between
running on the host and running inside the compose network is the value of
``KAFKA_BOOTSTRAP_SERVERS`` (R1.44).

``SERVICE_NAME`` is what makes one image and one entry point serve three consumers
(R1.37, D12): it selects an entry from the registry in
:mod:`order_service.consumer.main`. ``CONSUMER_GROUP_ID`` is left unset by default and
derived from the service name, so pointing a service at a fresh group — and thereby
replaying the topic from the beginning — is a one-variable change.
"""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime settings resolved from the environment.

    Attributes:
        kafka_bootstrap_servers: Broker address. ``localhost:9092`` from the host,
            ``kafka:19092`` from inside the compose network.
        order_lifecycle_topic: Topic carrying the order lifecycle events (R1.9).
        service_name: Which consumer service this process runs as.
        consumer_group_id: Consumer group identity. When unset it is derived from
            ``service_name``; set it to an unused value to replay from earliest.
        delivery_timeout_seconds: How long a publish waits for the broker's delivery
            report before giving up.
        order_service_host: Bind address for the order service HTTP API.
        order_service_port: Bind port for the order service HTTP API.
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
        """Return the consumer group a service should join.

        Args:
            service_name: The service whose group is wanted.

        Returns:
            The configured ``consumer_group_id`` when one is set, otherwise
            ``<service_name>-service``.
        """
        return self.consumer_group_id or f"{service_name}-service"


@lru_cache
def get_settings() -> Settings:
    """Return the process-wide settings, resolved once.

    Returns:
        The cached :class:`Settings` instance.
    """
    return Settings()
