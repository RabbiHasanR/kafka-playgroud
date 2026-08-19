"""HTTP surface of the order service.

Every publishing handler is a synchronous ``def``, not ``async def`` (D6): each waits
on the broker's delivery report, and FastAPI runs synchronous handlers in a worker
thread, so the wait cannot stall the event loop.

The order of operations in :func:`publish_event` is load-bearing:

1. does the order exist?           → ``404``, and nothing is spent
2. is the payload well-formed?     → ``422``, and no sequence is burned
3. is the transition legal?        → ``409``, unless ``force``
4. publish, waiting for the broker → ``502`` / ``504``
5. advance the recorded state

:func:`delete_order` follows the same shape for the same reason (006 D4):

1. does the order exist?           → ``404``, and nothing is published
2. publish the tombstone, waiting  → ``502`` / ``504``, and the order is left intact
3. forget the order                → ``204``

Both publishing handlers also write the order's current snapshot to the compacted topic.
That write is fire-and-forget and cannot fail the request: the event log is the source of
truth and the snapshot is derived from it (006 D3, R6.5).
"""

import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, Field

from order_service.events import (
    EventType,
    LifecycleEvent,
    OrderCreatedPayload,
    OrderItem,
    PaymentInfo,
    utc_now,
    validate_payload,
)
from order_service.producer.kafka_producer import (
    DeliveryFailed,
    DeliveryTimeout,
    LifecycleEventProducer,
)
from order_service.producer.orders import (
    IllegalTransition,
    Order,
    OrderStore,
    UnknownOrder,
    new_order_id,
)

logger = logging.getLogger(__name__)

router = APIRouter()


class CreateOrderRequest(BaseModel):
    """Body of a prepaid order creation (R1.12).

    Attributes:
        payment: The payment that has already settled. Its amount must equal
            ``Σ(qty × unit_price)`` or the request is rejected (R1.14).
    """

    customer_id: str = Field(min_length=1)
    items: list[OrderItem] = Field(min_length=1)
    payment: PaymentInfo


class CreateOrderResponse(BaseModel):
    """What the caller gets back from a successful creation (R1.17)."""

    order_id: str
    state: str
    total_amount: int
    sequence: int
    partition: int
    offset: int


class PublishEventRequest(BaseModel):
    """Body of a lifecycle advance (R1.19).

    Attributes:
        force: Bypass the transition guard and publish anyway (R1.24). The lab lever
            that makes the consumers' detection reachable; a real caller never sets it.
    """

    event_type: EventType
    payload: dict[str, Any] = Field(default_factory=dict)
    force: bool = False


class PublishEventResponse(BaseModel):
    """Where the broker put a published event (R1.23).

    Attributes:
        state: The order's recorded state after the publish. Unchanged when ``force``
            bypassed an illegal transition.
    """

    order_id: str
    sequence: int
    event_type: EventType
    state: str
    partition: int
    offset: int
    forced: bool


def _producer(request: Request) -> LifecycleEventProducer:
    """Return the producer held on the application state."""
    return request.app.state.producer


def _orders(request: Request) -> OrderStore:
    """Return the order store held on the application state."""
    return request.app.state.orders


def _publish(producer: LifecycleEventProducer, event: LifecycleEvent) -> tuple[int, int]:
    """Publish one event, translating delivery failures into HTTP errors.

    Args:
        producer: The producer to publish through.
        event: The event to publish.

    Returns:
        The partition and offset the broker assigned.

    Raises:
        HTTPException: ``502`` if the broker rejected the event, ``504`` if no
            delivery report arrived in time.
    """
    try:
        result = producer.publish_and_wait(event)
    except DeliveryTimeout as exc:
        logger.error(
            "delivery timeout for %s seq %d: %s", event.order_id, event.sequence, exc
        )
        raise HTTPException(status_code=504, detail=str(exc)) from exc
    except DeliveryFailed as exc:
        logger.error(
            "delivery failed for %s seq %d: %s", event.order_id, event.sequence, exc
        )
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return result.partition, result.offset


def _publish_snapshot(request: Request, order: Order) -> None:
    """Mirror an order's current state onto the compacted topic (R6.4).

    Deliberately returns nothing and raises nothing. A lost snapshot is repaired by the
    next event for that order; a lost lifecycle event is not repairable at all, so the
    derived write must never be able to fail the authoritative one (006 D3).
    """
    _producer(request).publish_snapshot(order.order_id, order.as_snapshot())


@router.get("/health")
def health() -> dict[str, str]:
    """Report that the order service is up."""
    return {"status": "ok"}


@router.post("/orders", response_model=CreateOrderResponse, status_code=201)
def create_order(body: CreateOrderRequest, request: Request) -> CreateOrderResponse:
    """Create a prepaid order and publish its ``ORDER_CREATED`` event.

    The order is recorded only *after* the broker acknowledges, so a delivery failure
    leaves no order behind.

    Args:
        body: The customer, the items, and the settled payment.
        request: The incoming request, carrying application state.

    Returns:
        The new order's id and where its creation event landed.

    Raises:
        HTTPException: ``422`` if the payment does not equal the item sum (R1.14),
            ``502`` if the broker rejected the event, ``504`` on delivery timeout.
    """
    order_id = new_order_id()
    try:
        payload = OrderCreatedPayload(
            customer_id=body.customer_id,
            items=body.items,
            total_amount=sum(item.line_total for item in body.items),
            payment=body.payment,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    event = LifecycleEvent(
        order_id=order_id,
        sequence=1,
        event_type=EventType.ORDER_CREATED,
        occurred_at=utc_now(),
        payload=payload.model_dump(mode="json"),
    )
    partition, offset = _publish(_producer(request), event)
    order = _orders(request).register(order_id, payload)
    _publish_snapshot(request, order)

    logger.info(
        "order created order_id=%s total=%d items=%d partition=%d offset=%d",
        order_id,
        order.total_amount,
        len(order.items),
        partition,
        offset,
    )
    return CreateOrderResponse(
        order_id=order_id,
        state=str(order.state),
        total_amount=order.total_amount,
        sequence=1,
        partition=partition,
        offset=offset,
    )


@router.post("/orders/{order_id}/events", response_model=PublishEventResponse)
def publish_event(
    order_id: str,
    body: PublishEventRequest,
    request: Request,
) -> PublishEventResponse:
    """Publish the next lifecycle event for an existing order.

    The service refuses an event the order's state cannot legally reach (R1.21).
    ``force`` bypasses that guard so an out-of-order event reaches the topic and the
    consumers' detection can be observed (R1.24).

    Args:
        order_id: Order the event belongs to; also the message key.
        body: The event type, payload, and force flag.
        request: The incoming request, carrying application state.

    Returns:
        The published event's sequence, partition, and offset.

    Raises:
        HTTPException: ``404`` if the order is unknown, ``422`` if the payload does not
            match the event type, ``409`` if the transition is illegal and ``force`` is
            not set, ``502`` if the broker rejected the event, ``504`` on timeout.
    """
    store = _orders(request)
    if store.get(order_id) is None:
        raise HTTPException(status_code=404, detail=f"no order {order_id}")

    # Before reserving, so a bad request cannot burn a sequence number.
    try:
        validate_payload(body.event_type, body.payload)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    try:
        sequence = store.reserve(order_id, body.event_type, force=body.force)
    except UnknownOrder as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except IllegalTransition as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    event = LifecycleEvent(
        order_id=order_id,
        sequence=sequence,
        event_type=body.event_type,
        occurred_at=utc_now(),
        payload=body.payload,
    )
    partition, offset = _publish(_producer(request), event)
    order = store.commit(order_id, body.event_type, force=body.force)
    _publish_snapshot(request, order)

    if body.force:
        logger.warning(
            "FORCED publish order_id=%s seq=%d type=%s state=%s",
            order_id,
            sequence,
            body.event_type,
            order.state,
        )
    logger.info(
        "event published order_id=%s seq=%d type=%s partition=%d offset=%d",
        order_id,
        sequence,
        body.event_type,
        partition,
        offset,
    )
    return PublishEventResponse(
        order_id=order_id,
        sequence=sequence,
        event_type=body.event_type,
        state=str(order.state),
        partition=partition,
        offset=offset,
        forced=body.force,
    )


@router.delete("/orders/{order_id}", status_code=204)
def delete_order(order_id: str, request: Request) -> Response:
    """Delete one order by publishing a tombstone for it (R6.6, R6.8).

    The tombstone — the order's key with a null value on the compacted topic — is what
    erases the order from the table and tells every consumer group to drop its fold. It
    is published *before* the order leaves this service's store, and the order is kept if
    the broker does not acknowledge (R6.9): a delete that is half-applied, gone locally
    but alive in three consumers' folds, has nothing left to re-drive it from.

    What this does **not** reach: the order's events stay in ``order-lifecycle``, its
    pending messages in the retry topic, its dead letters in the DLQ. Kafka has no
    cross-topic delete. See 006 D11 for the three paths that can therefore resurrect it.

    Args:
        order_id: The order to delete; also the tombstone's key.
        request: The incoming request, carrying application state.

    Returns:
        An empty ``204`` response.

    Raises:
        HTTPException: ``404`` if the order is unknown, ``502`` if the broker rejected
            the tombstone, ``504`` if no delivery report arrived in time.
    """
    store = _orders(request)
    # R6.7 — checked before anything is published, so an unknown id costs no message.
    if store.get(order_id) is None:
        raise HTTPException(status_code=404, detail=f"no order {order_id}")

    producer = _producer(request)
    try:
        result = producer.publish_tombstone(order_id)
    except DeliveryTimeout as exc:
        logger.error("tombstone delivery timeout for %s: %s", order_id, exc)
        raise HTTPException(status_code=504, detail=str(exc)) from exc
    except DeliveryFailed as exc:
        logger.error("tombstone delivery failed for %s: %s", order_id, exc)
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    try:
        order = store.remove(order_id)
    except UnknownOrder as exc:
        # Raced with another delete. The tombstone is already on the topic and is
        # idempotent, so this is a success, not a 404.
        logger.info("order %s was already removed: %s", order_id, exc)
        return Response(status_code=204)

    logger.warning(
        "TOMBSTONE published order_id=%s state=%s partition=%d offset=%d",
        order_id,
        order.state,
        result.partition,
        result.offset,
    )
    return Response(status_code=204)


@router.get("/orders/{order_id}")
def get_order(order_id: str, request: Request) -> dict[str, object]:
    """Report the service's own record of one order (R1.27).

    Worth comparing against what the consumers derived: this is the aggregate's view,
    theirs is folded from the log.

    Args:
        order_id: The order to look up.
        request: The incoming request, carrying application state.

    Returns:
        The order's state, last sequence, items, and total.

    Raises:
        HTTPException: ``404`` if no such order exists.
    """
    order = _orders(request).get(order_id)
    if order is None:
        raise HTTPException(status_code=404, detail=f"no order {order_id}")
    return order.as_dict()
