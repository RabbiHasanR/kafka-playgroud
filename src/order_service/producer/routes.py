"""HTTP surface of the order service.

Every publishing handler is a **synchronous** ``def``, not ``async def``. That is
deliberate (D6): each one waits on the broker's delivery report, and FastAPI runs
synchronous handlers in a worker thread, so the wait cannot stall the event loop.
Making them ``async def`` would block every other request for the duration.

The order of operations in :func:`publish_event` is load-bearing:

1. does the order exist?           → ``404``, and nothing is spent
2. is the payload well-formed?     → ``422``, and no sequence is burned
3. is the transition legal?        → ``409``, unless ``force``
4. publish, waiting for the broker → ``502`` / ``504``
5. advance the recorded state

Validating before reserving is what stops a malformed request from consuming a
sequence number that the consumers would later report as a gap.
"""

import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Request
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
    OrderStore,
    UnknownOrder,
    new_order_id,
)

logger = logging.getLogger(__name__)

router = APIRouter()


class CreateOrderRequest(BaseModel):
    """Body of a prepaid order creation (R1.12).

    Attributes:
        customer_id: Who is placing the order.
        items: The line items; at least one is required.
        payment: The payment that has already settled. Its amount must equal
            ``Σ(qty × unit_price)`` — a disagreement is rejected before any event is
            published (R1.14).
    """

    customer_id: str = Field(min_length=1)
    items: list[OrderItem] = Field(min_length=1)
    payment: PaymentInfo


class CreateOrderResponse(BaseModel):
    """What the caller gets back from a successful creation (R1.17).

    Attributes:
        order_id: Identity assigned to the new order.
        state: The order's lifecycle state, always ``CREATED`` here.
        total_amount: Order total in integer minor units.
        sequence: Sequence of the ``ORDER_CREATED`` event, always 1.
        partition: Partition the broker chose for it.
        offset: Offset within that partition.
    """

    order_id: str
    state: str
    total_amount: int
    sequence: int
    partition: int
    offset: int


class PublishEventRequest(BaseModel):
    """Body of a lifecycle advance (R1.19).

    Attributes:
        event_type: Which lifecycle event to publish.
        payload: Event-type-specific data, validated against the event contract.
        force: When ``True``, bypass the transition guard and publish anyway (R1.24).
            This is the lab lever that makes the consumers' detection reachable; a
            real caller never sets it.
    """

    event_type: EventType
    payload: dict[str, Any] = Field(default_factory=dict)
    force: bool = False


class PublishEventResponse(BaseModel):
    """Where the broker put a published event (R1.23).

    Attributes:
        order_id: The order the event belongs to.
        sequence: Sequence assigned to the event.
        event_type: The event's type.
        state: The order's recorded state after the publish. Unchanged when ``force``
            bypassed an illegal transition.
        partition: Partition the broker chose.
        offset: Offset within that partition.
        forced: Whether the transition guard was bypassed.
    """

    order_id: str
    sequence: int
    event_type: EventType
    state: str
    partition: int
    offset: int
    forced: bool


def _producer(request: Request) -> LifecycleEventProducer:
    """Return the producer held on the application state.

    Args:
        request: The incoming request.

    Returns:
        The process-wide :class:`LifecycleEventProducer`.
    """
    return request.app.state.producer


def _orders(request: Request) -> OrderStore:
    """Return the order store held on the application state.

    Args:
        request: The incoming request.

    Returns:
        The process-wide :class:`OrderStore`.
    """
    return request.app.state.orders


def _publish(producer: LifecycleEventProducer, event: LifecycleEvent) -> tuple[int, int]:
    """Publish one event, translating delivery failures into HTTP errors.

    Shared by both publishing endpoints so the broker-failure contract is defined
    once.

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


@router.get("/health")
def health() -> dict[str, str]:
    """Report that the order service is up.

    Returns:
        A static readiness marker.
    """
    return {"status": "ok"}


@router.post("/orders", response_model=CreateOrderResponse, status_code=201)
def create_order(body: CreateOrderRequest, request: Request) -> CreateOrderResponse:
    """Create a prepaid order and publish its ``ORDER_CREATED`` event.

    This is the synchronous half of the feature: the caller is blocked waiting for an
    ``order_id``, so it is an HTTP request and a Kafka publish it waits on, not an
    event it fires and forgets. Everything downstream of the event — reserving stock,
    notifying the customer, counting — is not the caller's business and happens off the
    log.

    The order is recorded only *after* the broker acknowledges, so a delivery failure
    leaves no order behind rather than one nobody downstream has heard of.

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

    The service refuses an event that the order's state cannot legally reach (R1.21) —
    a real service owns its aggregate rather than publishing whatever it is handed.
    ``force`` bypasses that guard so an out-of-order event can be put on the topic and
    the consumers' detection observed (R1.24); a forced event advances the sequence but
    not the recorded state.

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

    # Before reserving, so a malformed request cannot burn a sequence number that the
    # consumers would then report as a gap.
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
