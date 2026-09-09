"""Unit tests for the order aggregate (specs/001 R1.21/R1.24/R1.26, specs/006 R6.8)."""

import pytest

from order_service.events import EventType, OrderState
from order_service.producer.orders import (
    IllegalTransition,
    OrderStore,
    UnknownOrder,
)


class TestReserve:
    """R1.21, R1.24, R1.26 — the transition guard and sequence allocation."""

    def test_legal_transition_allocates_the_next_sequence(
        self, store: OrderStore, registered_order_id: str
    ) -> None:
        sequence = store.reserve(registered_order_id, EventType.PACKED)
        assert sequence == 2

    def test_illegal_transition_without_force_is_rejected(
        self, store: OrderStore, registered_order_id: str
    ) -> None:
        with pytest.raises(IllegalTransition):
            store.reserve(registered_order_id, EventType.SHIPPED)

    def test_illegal_transition_does_not_burn_a_sequence(
        self, store: OrderStore, registered_order_id: str
    ) -> None:
        with pytest.raises(IllegalTransition):
            store.reserve(registered_order_id, EventType.SHIPPED)
        sequence = store.reserve(registered_order_id, EventType.PACKED)
        assert sequence == 2

    def test_unknown_order_is_rejected(self, store: OrderStore) -> None:
        with pytest.raises(UnknownOrder):
            store.reserve("no-such-order", EventType.PACKED)

    def test_force_bypasses_the_guard_but_still_allocates_a_contiguous_sequence(
        self, store: OrderStore, registered_order_id: str
    ) -> None:
        sequence = store.reserve(
            registered_order_id, EventType.SHIPPED, force=True
        )
        assert sequence == 2


class TestCommit:
    """R1.23 — a forced publish that was illegal must not lie about the state."""

    def test_legal_commit_advances_the_state(
        self, store: OrderStore, registered_order_id: str
    ) -> None:
        store.reserve(registered_order_id, EventType.PACKED)
        order = store.commit(registered_order_id, EventType.PACKED)
        assert order.state == OrderState.PACKED

    def test_forced_illegal_commit_leaves_state_unchanged(
        self, store: OrderStore, registered_order_id: str
    ) -> None:
        store.reserve(registered_order_id, EventType.SHIPPED, force=True)
        order = store.commit(registered_order_id, EventType.SHIPPED, force=True)
        assert order.state == OrderState.CREATED

    def test_unknown_order_is_rejected(self, store: OrderStore) -> None:
        with pytest.raises(UnknownOrder):
            store.commit("no-such-order", EventType.PACKED)


class TestRemove:
    """R6.8 — a tombstoned order is forgotten, and only once."""

    def test_removes_an_existing_order(
        self, store: OrderStore, registered_order_id: str
    ) -> None:
        order = store.remove(registered_order_id)
        assert order.order_id == registered_order_id
        assert store.get(registered_order_id) is None

    def test_removing_twice_raises_on_the_second_call(
        self, store: OrderStore, registered_order_id: str
    ) -> None:
        store.remove(registered_order_id)
        with pytest.raises(UnknownOrder):
            store.remove(registered_order_id)
