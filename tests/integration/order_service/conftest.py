"""Fixtures shared by `order_service`'s integration tests.

Every test here builds real `confluent_kafka` clients and the real domain objects
(`LifecycleEventProducer`, `ServiceConsumer`, `RetryWorker`) — no fakes. Scratch
topics are named `it-order-lifecycle-<unique_id>` (plus `.retry`/`.dlq` suffixes and
an `it-order-snapshot-<unique_id>` companion) so they can never collide with the
compose stack's real `order-lifecycle`/`order-lifecycle.retry`/`order-lifecycle.dlq`
topics or with each other.
"""

import contextlib
import threading
import time
from collections.abc import Callable, Iterator
from typing import Protocol

import pytest
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient

from order_service.config import Settings
from order_service.consumer.dlq import FailureRouter
from order_service.consumer.main import SERVICE_REGISTRY
from order_service.consumer.runtime import ServiceConsumer, ServiceSpec
from order_service.consumer.state import MemoryStateStore
from order_service.consumer.transactions import build_producer
from order_service.producer.kafka_producer import LifecycleEventProducer
from tests.integration.order_service._topics import (
    TopicSpec,
    create_scratch_topics,
    delete_scratch_topics,
)


class _Runnable(Protocol):
    def run(self) -> None: ...
    def stop(self) -> None: ...


@pytest.fixture
def settings_factory() -> Callable[..., Settings]:
    """Return a factory building `Settings` isolated from `.env`, defaults intact."""

    def make(**overrides: object) -> Settings:
        return Settings(_env_file=None, **overrides)  # type: ignore[arg-type]

    return make


@pytest.fixture
def unique_group_id_factory(unique_id: str) -> Callable[[str], str]:
    """Return a factory for consumer group ids unique to this test."""

    def make(label: str) -> str:
        return f"it-{label}-{unique_id}"

    return make


@pytest.fixture
def scratch_topics(
    kafka_admin_client: AdminClient, unique_id: str
) -> Iterator[Callable[..., dict[str, str]]]:
    """Return a factory that creates and tears down scratch topics for one test.

    ``make(lifecycle=True, snapshot=False, retry=False, dlq=False, partitions=3)``
    returns a ``{role: topic_name}`` dict for whatever roles were requested. Every
    topic ever created by any call is deleted (best-effort) when the test ends.
    """
    created: list[str] = []

    def make(
        *,
        lifecycle: bool = True,
        snapshot: bool = False,
        retry: bool = False,
        dlq: bool = False,
        partitions: int = 3,
    ) -> dict[str, str]:
        base = f"it-order-lifecycle-{unique_id}"
        names: dict[str, str] = {}
        specs: list[TopicSpec] = []
        if lifecycle:
            names["lifecycle"] = base
            specs.append(
                TopicSpec(base, partitions=partitions, config={"min.insync.replicas": "2"})
            )
        if snapshot:
            names["snapshot"] = f"it-order-snapshot-{unique_id}"
            specs.append(TopicSpec(names["snapshot"], partitions=partitions))
        if retry:
            names["retry"] = f"{base}.retry"
            specs.append(TopicSpec(names["retry"], partitions=partitions))
        if dlq:
            names["dlq"] = f"{base}.dlq"
            specs.append(TopicSpec(names["dlq"], partitions=partitions))

        create_scratch_topics(kafka_admin_client, specs)
        created.extend(names.values())
        return names

    yield make

    delete_scratch_topics(kafka_admin_client, created)


@contextlib.contextmanager
def running_producer(settings: Settings) -> Iterator[LifecycleEventProducer]:
    """Start a `LifecycleEventProducer` for the duration of a `with` block."""
    producer = LifecycleEventProducer(settings)
    producer.start()
    try:
        yield producer
    finally:
        producer.stop()


@contextlib.contextmanager
def running_consumer(runnable: _Runnable) -> Iterator[threading.Thread]:
    """Run a `ServiceConsumer`/`RetryWorker`'s `run()` loop on a background thread.

    The caller calls ``runnable.stop()`` before (or at) the `with` block's exit; this
    context manager joins the thread with a bounded timeout either way, so a test
    failure can't leave a background consumer polling a topic past the test's life.
    """
    thread = threading.Thread(target=runnable.run, daemon=True)
    thread.start()
    try:
        yield thread
    finally:
        runnable.stop()
        thread.join(timeout=10.0)


def wait_until(
    predicate: Callable[[], bool],
    *,
    timeout: float = 10.0,
    interval: float = 0.2,
    description: str = "condition",
) -> None:
    """Poll `predicate` until it's true, or raise `TimeoutError` after `timeout`s.

    Used instead of a fixed `sleep(N)` everywhere in this suite: rebalance timing,
    handler dispatch latency, and commit timing all vary run to run.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(interval)
    raise TimeoutError(f"timed out after {timeout}s waiting for: {description}")


def build_service_consumer(
    service_name: str, settings: Settings
) -> tuple[ServiceConsumer, Producer]:
    """Build one real `ServiceConsumer`, wired the way `consumer/main.py` wires it.

    Uses the real `SERVICE_REGISTRY` (real inventory/notification/analytics handlers)
    and `MemoryStateStore` (the default backend; its `restore()` is a deliberate
    no-op, so it needs no changelog topic) rather than hand-rolled fakes — exercising
    the real assignment/dispatch path is the entire point of these tests.

    Returns:
        The consumer, and the producer it (and its `FailureRouter`) share — the
        caller must `.flush()`/close this producer once the consumer has stopped.
    """
    spec: ServiceSpec = SERVICE_REGISTRY[service_name]()
    group_id = settings.group_id_for(spec.name)
    producer = build_producer(settings, group_id=group_id, instance=settings.instance_label)
    store = MemoryStateStore()
    router = FailureRouter(settings, producer)
    consumer = ServiceConsumer(spec, settings, store, router, producer)
    return consumer, producer


def committed_offset(bootstrap_servers: str, group_id: str, topic: str, partition: int) -> int:
    """Return one group's committed offset for one partition, or -1 if none is set.

    A throwaway low-level `Consumer` used purely for observation — it never joins
    `group_id` (no `subscribe()`/`poll()` call), so asking for its committed offset
    cannot itself perturb the group being observed.
    """
    from confluent_kafka import TopicPartition

    observer = Consumer(
        {"bootstrap.servers": bootstrap_servers, "group.id": group_id, "enable.auto.commit": False}
    )
    try:
        (result,) = observer.committed([TopicPartition(topic, partition)], timeout=10.0)
        return result.offset
    finally:
        observer.close()


def topic_high_watermark(bootstrap_servers: str, topic: str) -> int:
    """Return the sum of a topic's high watermarks across all its partitions."""
    from confluent_kafka import TopicPartition

    observer = Consumer(
        {"bootstrap.servers": bootstrap_servers, "group.id": "it-watermark-observer"}
    )
    try:
        metadata = observer.list_topics(topic=topic, timeout=10.0)
        described = metadata.topics[topic]
        total = 0
        for partition in described.partitions:
            _low, high = observer.get_watermark_offsets(
                TopicPartition(topic, partition), timeout=10.0, cached=False
            )
            total += high
        return total
    finally:
        observer.close()
