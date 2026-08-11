"""Order lifecycle generation, rate pacing, and fault injection.

The simulator exists so ordering can be broken *on demand* (R1.15–R1.17). A clean
run and a broken run are one HTTP call apart, with no restart in between — which is
what makes the difference between them legible.

The two fault modes break ordering in genuinely different ways, and the distinction
is the point:

``unkeyed``
    Publishes with a null key, so the partitioner scatters one order's events across
    partitions. The events are emitted in the right order; Kafka simply has no way
    to keep them together. This is what the message key is *for*.

``shuffle``
    Publishes an order's events in a permuted order while still keying them
    correctly. Every event lands on the right partition, in the order the producer
    sent them — and that order is wrong. Partition ordering is faithful, not
    corrective: Kafka preserves the order you gave it, including a bad one.
"""

import logging
import random
import threading
import time
import uuid
from dataclasses import dataclass, field

from order_pipeline.events import (
    LIFECYCLE_CHAIN,
    EventType,
    OrderEvent,
    utc_now,
)
from order_pipeline.producer.kafka_producer import DeliveryFailed, OrderEventProducer

logger = logging.getLogger(__name__)

SKUS: tuple[str, ...] = ("WIDGET-A", "WIDGET-B", "GADGET-C", "GIZMO-D", "DOODAD-E")


@dataclass
class SimulationJob:
    """Progress and outcome of one simulation run.

    Attributes:
        job_id: Identifier callers use to poll this job.
        order_count: Number of orders requested.
        rate_per_second: Requested publishing rate in events per second.
        unkeyed: Whether the null-key fault mode is active.
        shuffle: Whether the permuted-order fault mode is active.
        events_planned: Total events this run will attempt.
        events_published: Events the broker acknowledged.
        events_failed: Events the broker rejected or that could not be enqueued.
        finished: Whether the run has completed.
        error: Fatal error that ended the run early, if any.
    """

    job_id: str
    order_count: int
    rate_per_second: float
    unkeyed: bool
    shuffle: bool
    events_planned: int = 0
    events_published: int = 0
    events_failed: int = 0
    finished: bool = False
    error: str | None = None
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def record_delivery(self, err: object) -> None:
        """Tally one delivery report.

        Args:
            err: The librdkafka error, or ``None`` if the broker acknowledged.
        """
        with self._lock:
            if err is None:
                self.events_published += 1
            else:
                self.events_failed += 1

    def summary(self) -> dict[str, object]:
        """Return a JSON-serialisable snapshot of this job.

        Returns:
            The job's parameters and current counts.
        """
        with self._lock:
            return {
                "job_id": self.job_id,
                "order_count": self.order_count,
                "rate_per_second": self.rate_per_second,
                "unkeyed": self.unkeyed,
                "shuffle": self.shuffle,
                "events_planned": self.events_planned,
                "events_published": self.events_published,
                "events_failed": self.events_failed,
                "finished": self.finished,
                "error": self.error,
            }


class JobRegistry:
    """In-memory registry of simulation jobs."""

    def __init__(self) -> None:
        """Initialise an empty registry."""
        self._jobs: dict[str, SimulationJob] = {}
        self._lock = threading.Lock()

    def create(
        self, order_count: int, rate_per_second: float, unkeyed: bool, shuffle: bool
    ) -> SimulationJob:
        """Register a new job.

        Args:
            order_count: Number of orders to generate.
            rate_per_second: Publishing rate in events per second.
            unkeyed: Whether to publish without a message key.
            shuffle: Whether to permute each order's emission order.

        Returns:
            The newly registered job.
        """
        job = SimulationJob(
            job_id=uuid.uuid4().hex[:12],
            order_count=order_count,
            rate_per_second=rate_per_second,
            unkeyed=unkeyed,
            shuffle=shuffle,
        )
        with self._lock:
            self._jobs[job.job_id] = job
        return job

    def get(self, job_id: str) -> SimulationJob | None:
        """Look up a job by id.

        Args:
            job_id: The job identifier.

        Returns:
            The job, or ``None`` if no such job exists.
        """
        with self._lock:
            return self._jobs.get(job_id)

    def all_summaries(self) -> list[dict[str, object]]:
        """Return summaries of every registered job.

        Returns:
            One summary per job, newest last.
        """
        with self._lock:
            return [job.summary() for job in self._jobs.values()]


def build_lifecycle(
    producer: OrderEventProducer,
    order_id: str,
    item_count: int,
    rng: random.Random,
) -> list[OrderEvent]:
    """Build one complete, internally consistent order lifecycle.

    Sequence numbers are assigned in lifecycle order, so a later shuffle permutes
    only the *emission* order — the sequences themselves stay correct, which is what
    lets the consumer detect the disorder.

    The ``PAID`` amount is the exact sum of the line totals, so a clean run produces
    no total-mismatch violation (R1.21) and any mismatch observed later is real.

    Args:
        producer: Producer used to assign sequence numbers.
        order_id: Identity of the order.
        item_count: How many ``ITEM_ADDED`` events to include.
        rng: Random source, seedable for reproducible runs.

    Returns:
        The order's events in lifecycle order.
    """
    events: list[OrderEvent] = []

    def emit(event_type: EventType, payload: dict[str, object] | None = None) -> None:
        events.append(
            OrderEvent(
                order_id=order_id,
                sequence=producer.next_sequence(order_id),
                event_type=event_type,
                occurred_at=utc_now(),
                payload=payload or {},
            )
        )

    emit(EventType.ORDER_CREATED)

    total = 0
    for _ in range(item_count):
        qty = rng.randint(1, 5)
        unit_price = rng.randrange(5_00, 500_00, 25)
        total += qty * unit_price
        emit(
            EventType.ITEM_ADDED,
            {"sku": rng.choice(SKUS), "qty": qty, "unit_price": unit_price},
        )

    for event_type in LIFECYCLE_CHAIN[1:]:
        emit(event_type, {"amount": total} if event_type is EventType.PAID else None)

    return events


def run_simulation(
    producer: OrderEventProducer,
    job: SimulationJob,
    *,
    items_per_order: int = 3,
    seed: int | None = None,
    order_prefix: str | None = None,
) -> None:
    """Generate and publish orders, pacing to the job's requested rate.

    Args:
        producer: Producer to publish through.
        job: Job to record progress against.
        items_per_order: Number of ``ITEM_ADDED`` events per order.
        seed: Random seed, for reproducible runs.
        order_prefix: Prefix for generated order ids. Defaults to the job id, which
            keeps separate runs distinguishable in the consumer's state dump.
    """
    rng = random.Random(seed)
    prefix = order_prefix or f"ord-{job.job_id}"
    # ORDER_CREATED + items + (PAID, PACKED, SHIPPED, DELIVERED)
    per_order = 1 + items_per_order + len(LIFECYCLE_CHAIN) - 1
    job.events_planned = per_order * job.order_count

    interval = 1.0 / job.rate_per_second if job.rate_per_second > 0 else 0.0

    def on_delivery(err: object, _msg: object) -> None:
        job.record_delivery(err)

    logger.info(
        "simulation %s starting: %d orders, %d events, %.1f eps, unkeyed=%s shuffle=%s",
        job.job_id,
        job.order_count,
        job.events_planned,
        job.rate_per_second,
        job.unkeyed,
        job.shuffle,
    )

    next_at = time.monotonic()
    try:
        for index in range(job.order_count):
            order_id = f"{prefix}-{index:04d}"
            events = build_lifecycle(producer, order_id, items_per_order, rng)

            if job.shuffle:
                rng.shuffle(events)

            for event in events:
                if interval:
                    next_at += interval
                    delay = next_at - time.monotonic()
                    if delay > 0:
                        time.sleep(delay)
                try:
                    producer.publish(
                        event, keyed=not job.unkeyed, on_delivery=on_delivery
                    )
                except DeliveryFailed as exc:
                    job.record_delivery(exc)
                    logger.error("enqueue failed for %s: %s", order_id, exc)
    except Exception as exc:  # noqa: BLE001 - recorded on the job, not swallowed
        job.error = str(exc)
        logger.exception("simulation %s failed", job.job_id)
    finally:
        # Drain so the delivery tallies are final before the job is marked finished.
        producer.flush(timeout=30.0)
        job.finished = True
        logger.info(
            "simulation %s finished: %d published, %d failed",
            job.job_id,
            job.events_published,
            job.events_failed,
        )
