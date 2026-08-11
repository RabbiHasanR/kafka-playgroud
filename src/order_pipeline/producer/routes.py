"""HTTP surface of the producer.

Note that :func:`publish_event` is a **synchronous** ``def``, not ``async def``.
That is deliberate (D5): it waits on the broker's delivery report, and FastAPI runs
synchronous handlers in a worker thread, so the wait cannot stall the event loop.
Making it ``async def`` would block every other request for the duration.
"""

import logging
from typing import Any

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from pydantic import BaseModel, Field

from order_pipeline.events import EventType, OrderEvent, utc_now
from order_pipeline.producer.kafka_producer import (
    DeliveryFailed,
    DeliveryTimeout,
    OrderEventProducer,
)
from order_pipeline.producer.simulator import JobRegistry, run_simulation

logger = logging.getLogger(__name__)

router = APIRouter()


class PublishEventRequest(BaseModel):
    """Body of a single-event publish.

    Attributes:
        event_type: Which lifecycle event to publish.
        payload: Event-type-specific data, validated against the event contract.
        keyed: When ``False``, publish with a null key so the partitioner scatters
            this event instead of routing it by ``order_id`` (R1.15).
    """

    event_type: EventType
    payload: dict[str, Any] = Field(default_factory=dict)
    keyed: bool = True


class PublishEventResponse(BaseModel):
    """Where the broker put a published event (R1.12).

    Attributes:
        order_id: The order the event belongs to.
        sequence: Sequence assigned to the event.
        event_type: The event's type.
        partition: Partition the broker chose.
        offset: Offset within that partition.
        keyed: Whether the event carried a message key.
    """

    order_id: str
    sequence: int
    event_type: EventType
    partition: int
    offset: int
    keyed: bool


class SimulateRequest(BaseModel):
    """Body of a simulation run.

    Attributes:
        order_count: How many complete order lifecycles to generate.
        rate_per_second: Publishing rate in events per second. Raise this above the
            consumer's throughput to grow consumer lag (T33).
        items_per_order: ``ITEM_ADDED`` events per order.
        unkeyed: Publish with a null key, scattering each order across partitions
            (R1.15).
        shuffle: Publish each order's events in a permuted order while still keying
            them correctly (R1.16).
        seed: Random seed, for reproducible runs.
    """

    order_count: int = Field(default=10, ge=1, le=100_000)
    rate_per_second: float = Field(default=50.0, gt=0, le=100_000)
    items_per_order: int = Field(default=3, ge=0, le=50)
    unkeyed: bool = False
    shuffle: bool = False
    seed: int | None = None


def _producer(request: Request) -> OrderEventProducer:
    """Return the producer held on the application state.

    Args:
        request: The incoming request.

    Returns:
        The process-wide :class:`OrderEventProducer`.
    """
    return request.app.state.producer


def _jobs(request: Request) -> JobRegistry:
    """Return the job registry held on the application state.

    Args:
        request: The incoming request.

    Returns:
        The process-wide :class:`JobRegistry`.
    """
    return request.app.state.jobs


@router.get("/health")
def health() -> dict[str, str]:
    """Report that the producer process is up.

    Returns:
        A static readiness marker.
    """
    return {"status": "ok"}


@router.post("/orders/{order_id}/events", response_model=PublishEventResponse)
def publish_event(
    order_id: str,
    body: PublishEventRequest,
    request: Request,
) -> PublishEventResponse:
    """Publish one event and report where the broker actually put it.

    Blocks on the delivery report so the response carries the true partition and
    offset rather than a claim (R1.10, R1.12). A broker that does not acknowledge
    becomes an error response, never a silent drop (R1.13).

    Args:
        order_id: Order the event belongs to; also the message key.
        body: The event type, payload, and keying choice.
        request: The incoming request, carrying application state.

    Returns:
        The published event's assigned partition and offset.

    Raises:
        HTTPException: ``422`` if the payload does not match the event type,
            ``502`` if the broker rejected the event, ``504`` if no delivery report
            arrived in time.
    """
    producer = _producer(request)
    sequence = producer.next_sequence(order_id)
    try:
        event = OrderEvent(
            order_id=order_id,
            sequence=sequence,
            event_type=body.event_type,
            occurred_at=utc_now(),
            payload=body.payload,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    try:
        result = producer.publish_and_wait(event, keyed=body.keyed)
    except DeliveryTimeout as exc:
        logger.error("delivery timeout for %s seq %d: %s", order_id, sequence, exc)
        raise HTTPException(status_code=504, detail=str(exc)) from exc
    except DeliveryFailed as exc:
        logger.error("delivery failed for %s seq %d: %s", order_id, sequence, exc)
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    return PublishEventResponse(
        order_id=order_id,
        sequence=sequence,
        event_type=body.event_type,
        partition=result.partition,
        offset=result.offset,
        keyed=body.keyed,
    )


@router.post("/simulate", status_code=202)
def simulate(
    body: SimulateRequest,
    request: Request,
    background: BackgroundTasks,
) -> dict[str, object]:
    """Start a simulation run in the background (R1.11).

    Unlike the single-event endpoint this does not block per event — one broker
    round-trip per message would cap throughput and defeat the lag experiment (D6).
    Poll ``GET /simulate/{job_id}`` for published and failed counts.

    Args:
        body: Run parameters, including the fault-injection flags.
        request: The incoming request, carrying application state.
        background: FastAPI's background task queue.

    Returns:
        The job's initial summary, including the id to poll.
    """
    job = _jobs(request).create(
        order_count=body.order_count,
        rate_per_second=body.rate_per_second,
        unkeyed=body.unkeyed,
        shuffle=body.shuffle,
    )
    background.add_task(
        run_simulation,
        _producer(request),
        job,
        items_per_order=body.items_per_order,
        seed=body.seed,
    )
    return job.summary()


@router.get("/simulate/{job_id}")
def simulation_status(job_id: str, request: Request) -> dict[str, object]:
    """Report a simulation run's progress and delivery counts.

    Args:
        job_id: Identifier returned by ``POST /simulate``.
        request: The incoming request, carrying application state.

    Returns:
        The job's current summary.

    Raises:
        HTTPException: ``404`` if no such job exists.
    """
    job = _jobs(request).get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"no job {job_id}")
    return job.summary()


@router.get("/simulate")
def simulation_list(request: Request) -> list[dict[str, object]]:
    """List every simulation run this process has started.

    Args:
        request: The incoming request, carrying application state.

    Returns:
        One summary per job.
    """
    return _jobs(request).all_summaries()
