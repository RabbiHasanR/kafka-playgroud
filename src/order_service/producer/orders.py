"""The order aggregate: identity, sequence allocation, and the transition guard.

Owning the order is what lets the service refuse: publishing ``SHIPPED`` for an order
that was never packed is a ``409``, not a message on a topic (R1.21, D4). ``force``
bypasses the guard so the consumers' detection is reachable; a forced event advances
the sequence but not the recorded state. Sequences are spent, not reserved — a failed
publish does not give the number back.
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
    """No order with the given id exists (R1.20)."""


class IllegalTransition(Exception):
    """The requested event is not the legal successor of the order's state (R1.21)."""

    def __init__(
        self,
        order_id: str,
        current: OrderState,
        requested: EventType,
        expected: EventType | None,
    ) -> None:
        """Initialise the error with everything a ``409`` body needs.

        Args:
            order_id: The order the request was for.
            current: The order's current lifecycle state.
            requested: The event type the caller asked to publish.
            expected: The event type the order expects next, or ``None`` when its
                lifecycle is complete.
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
    """The order service's own record of one order (R1.13)."""

    order_id: str
    customer_id: str
    items: tuple[OrderItem, ...]
    total_amount: int
    payment: PaymentInfo
    state: OrderState
    last_sequence: int

    def as_snapshot(self) -> dict[str, object]:
        """Return this order as one self-contained value for the compacted topic (R6.4).

        Built on :meth:`as_dict` so the HTTP view and the snapshot cannot drift apart.

        **Self-containment is a requirement here, not a convenience.** Compaction retains
        only the newest value per key and discards every earlier one, so a consumer may
        legitimately see this message and nothing else about the order — no creation, no
        intervening events. Everything needed to know the order must therefore be in this
        single dict. Trimming it to ``{state}`` would halve the bytes and destroy the
        property the compacted topic exists to demonstrate (006 D2).

        Returns:
            The order's complete current state, JSON-serialisable.
        """
        return self.as_dict()

    def as_dict(self) -> dict[str, object]:
        """Return a JSON-serialisable view of this order, enums as strings (R1.27)."""
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
    """Return a fresh order identifier, short enough to read in logs and curl."""
    return f"ord-{uuid4().hex[:12]}"


class OrderStore:
    """In-memory store of every order this process has created.

    No persistence by design: restarting forgets every order, so advancing a
    pre-restart order returns ``404``. A database and a transactional outbox are out
    of scope for this feature.
    """

    def __init__(self) -> None:
        self._orders: dict[str, Order] = {}
        # Handlers run in worker threads, so mutation and its check share one lock.
        self._lock = threading.Lock()

    def register(self, order_id: str, payload: OrderCreatedPayload) -> Order:
        """Record a newly created order at sequence 1 (R1.13).

        Called *after* the ``ORDER_CREATED`` event has been acknowledged, so a broker
        failure leaves no order behind.

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
        """Return one order, or ``None`` if it is unknown."""
        with self._lock:
            return self._orders.get(order_id)

    def remove(self, order_id: str) -> Order:
        """Forget one order entirely (R6.8).

        Called only *after* the tombstone has been acknowledged by the broker, mirroring
        :meth:`register`, which records an order only after its creation event lands. The
        ordering is what keeps a failed delete retryable: an order dropped here while the
        tombstone never reached the topic would be gone locally and still present in every
        consumer's fold, with nothing left to re-drive the delete from (006 D4).

        Args:
            order_id: The order to forget.

        Returns:
            The order as it was immediately before removal.

        Raises:
            UnknownOrder: If no such order exists.
        """
        with self._lock:
            order = self._orders.pop(order_id, None)
            if order is None:
                raise UnknownOrder(f"no order {order_id}")
            return order

    def reserve(
        self, order_id: str, event_type: EventType, *, force: bool = False
    ) -> int:
        """Check the transition and allocate the next sequence (R1.3, R1.21, R1.26).

        Check and allocation happen under one lock, so two concurrent requests cannot
        both be told they hold sequence *n*. A forced reservation skips the successor
        check but still takes the next contiguous sequence, keeping the two
        consumer-side signals independent: an illegal transition with no sequence gap.

        Args:
            order_id: The order to advance.
            event_type: The event type the caller wants to publish.
            force: When ``True``, bypass the successor check (R1.24).

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
        """Advance the order's recorded state after a successful publish (R1.23).

        A forced publish leaves the state untouched — the aggregate could not legally
        make that transition, and recording it would put a lie in the store.

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
