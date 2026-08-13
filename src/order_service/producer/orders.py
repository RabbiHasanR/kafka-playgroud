"""The order aggregate: identity, sequence allocation, and the transition guard.

This module is the sharpest difference between 001 and 002. 001's producer publishes
whatever event type the caller names, because it has no notion of an order. This one
owns the order, so it can refuse: publishing ``SHIPPED`` for an order that was never
packed is a ``409``, not a message on a topic (R2.21, D4).

**The escape hatch is deliberate.** That guard would make the consumers' detection
unreachable — a service that never emits an illegal transition gives them nothing to
detect. So :meth:`OrderStore.reserve` takes ``force``, defaulted off, which bypasses
the successor check. Default behaviour is what production does; the flag is the lab
lever, exactly as 001 does with ``unkeyed`` and ``shuffle``.

**A forced event does not advance the recorded state.** The guard rejected the
transition precisely because the aggregate cannot make it; recording it anyway would
put a lie in the store. The event goes on the topic, the sequence advances, the order's
state does not.

**Sequences are spent, not reserved.** :meth:`reserve` increments under the lock and
does not roll back if the publish then fails. That is honest — the number was spent
whether or not the broker took the message — and it is the same behaviour as 001's D8.
"""

import threading
from dataclasses import dataclass, replace
from uuid import uuid4

from order_service.events import (
    EXPECTED_NEXT_EVENT,
    RESULTING_STATE,
    EventType,
    OrderCreatedPayload,
    OrderItem,
    OrderState,
    PaymentInfo,
    is_legal_transition,
)


class UnknownOrder(Exception):
    """No order with the given id exists (R2.20)."""


class IllegalTransition(Exception):
    """The requested event is not the legal successor of the order's state (R2.21).

    Attributes:
        order_id: The order the request was for.
        current: The order's current lifecycle state.
        requested: The event type the caller asked to publish.
        expected: The event type the order's state actually expects next, or ``None``
            when the order has reached the end of its lifecycle.
    """

    def __init__(
        self,
        order_id: str,
        current: OrderState,
        requested: EventType,
        expected: EventType | None,
    ) -> None:
        """Initialise the error with everything a `409` body needs.

        Args:
            order_id: The order the request was for.
            current: The order's current lifecycle state.
            requested: The event type the caller asked to publish.
            expected: The event type the order expects next, if any.
        """
        self.order_id = order_id
        self.current = current
        self.requested = requested
        self.expected = expected
        detail = (
            f"order {order_id} is {current}; "
            f"expected {expected} but got {requested}"
            if expected is not None
            else f"order {order_id} is {current} and its lifecycle is complete; "
            f"{requested} cannot follow"
        )
        super().__init__(detail)


@dataclass(frozen=True)
class Order:
    """The order service's own record of one order (R2.13).

    Attributes:
        order_id: Identity of the order, and the Kafka message key for its events.
        customer_id: Who placed it.
        items: The line items.
        total_amount: Order total in integer minor units.
        payment: The settled payment.
        state: Current lifecycle state.
        last_sequence: The highest sequence number handed out for this order.
    """

    order_id: str
    customer_id: str
    items: tuple[OrderItem, ...]
    total_amount: int
    payment: PaymentInfo
    state: OrderState
    last_sequence: int

    def as_dict(self) -> dict[str, object]:
        """Return a JSON-serialisable view of this order (R2.27).

        Returns:
            The order's fields, with enums rendered as plain strings.
        """
        return {
            "order_id": self.order_id,
            "customer_id": self.customer_id,
            "state": str(self.state),
            "last_sequence": self.last_sequence,
            "total_amount": self.total_amount,
            "payment": {
                "method": str(self.payment.method),
                "reference": self.payment.reference,
                "amount": self.payment.amount,
            },
            "items": [item.model_dump() for item in self.items],
            "expected_next_event": (
                str(EXPECTED_NEXT_EVENT[self.state])
                if self.state in EXPECTED_NEXT_EVENT
                else None
            ),
        }


def new_order_id() -> str:
    """Return a fresh order identifier.

    Returns:
        A short, readable, unique id — readable because it ends up in log lines,
        message keys, and curl commands.
    """
    return f"ord-{uuid4().hex[:12]}"


class OrderStore:
    """In-memory store of every order this process has created.

    **This deliberately has no persistence.** Restarting the service forgets every
    order, so advancing a pre-restart order returns `404`. That is the same class of
    limitation as 001's D8 and is not to be "fixed" here: a real service holds orders
    in a database and publishes through a transactional outbox, so that the row and
    the event cannot diverge. Neither is in scope for this feature.
    """

    def __init__(self) -> None:
        """Initialise an empty store."""
        self._orders: dict[str, Order] = {}
        # Route handlers run in FastAPI's worker threads, so every mutation and the
        # transition check that precedes it happen under one lock.
        self._lock = threading.Lock()

    def register(self, order_id: str, payload: OrderCreatedPayload) -> Order:
        """Record a newly created order at sequence 1 (R2.13).

        Called *after* the ``ORDER_CREATED`` event has been acknowledged, so a broker
        failure leaves no order behind rather than an order nobody downstream knows
        about.

        Args:
            order_id: The identity assigned to the order.
            payload: The validated creation payload.

        Returns:
            The recorded order.
        """
        order = Order(
            order_id=order_id,
            customer_id=payload.customer_id,
            items=tuple(payload.items),
            total_amount=payload.total_amount,
            payment=payload.payment,
            state=RESULTING_STATE[EventType.ORDER_CREATED],
            last_sequence=1,
        )
        with self._lock:
            self._orders[order_id] = order
        return order

    def get(self, order_id: str) -> Order | None:
        """Return one order, or ``None`` if it is unknown.

        Args:
            order_id: The order to look up.

        Returns:
            The order, or ``None``.
        """
        with self._lock:
            return self._orders.get(order_id)

    def reserve(
        self, order_id: str, event_type: EventType, *, force: bool = False
    ) -> int:
        """Check the transition and allocate the next sequence (R2.3, R2.21, R2.26).

        The check and the allocation happen under one lock, so two concurrent requests
        cannot both be told they hold sequence *n*.

        A forced reservation skips the successor check but still takes the next
        contiguous sequence. That keeps the two consumer-side signals independent: the
        resulting event raises an illegal-transition violation with no accompanying
        sequence gap, so each detector can be observed on its own.

        Args:
            order_id: The order to advance.
            event_type: The event type the caller wants to publish.
            force: When ``True``, bypass the successor check (R2.24).

        Returns:
            The sequence number allocated to the event.

        Raises:
            UnknownOrder: If no such order exists.
            IllegalTransition: If the event is not the legal successor and ``force``
                is not set.
        """
        with self._lock:
            order = self._orders.get(order_id)
            if order is None:
                raise UnknownOrder(f"no order {order_id}")
            if not force and not is_legal_transition(event_type, order.state):
                raise IllegalTransition(
                    order_id=order_id,
                    current=order.state,
                    requested=event_type,
                    expected=EXPECTED_NEXT_EVENT.get(order.state),
                )
            sequence = order.last_sequence + 1
            self._orders[order_id] = replace(order, last_sequence=sequence)
            return sequence

    def commit(
        self, order_id: str, event_type: EventType, *, force: bool = False
    ) -> Order:
        """Advance the order's recorded state after a successful publish (R2.23).

        A forced publish leaves the state untouched — see the module docstring.

        Args:
            order_id: The order to advance.
            event_type: The event type that was published.
            force: Whether the publish bypassed the transition guard.

        Returns:
            The order as recorded after the publish.

        Raises:
            UnknownOrder: If no such order exists.
        """
        with self._lock:
            order = self._orders.get(order_id)
            if order is None:
                raise UnknownOrder(f"no order {order_id}")
            if force and not is_legal_transition(event_type, order.state):
                return order
            advanced = replace(order, state=RESULTING_STATE[event_type])
            self._orders[order_id] = advanced
            return advanced
