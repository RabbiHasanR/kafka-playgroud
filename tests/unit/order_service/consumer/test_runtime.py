"""Unit tests for the pure logic in :mod:`order_service.consumer.runtime`.

Covers ``apply_event`` (the fold), config assembly per group protocol, and the small
header/key parsing helpers. Excludes ``ServiceConsumer`` itself — its consume loop is
an integration seam (real Kafka assignment, transactions, state store), not unit-test
territory.
"""

import pytest

from order_service.config import GroupProtocol, Settings
from order_service.consumer.runtime import (
    ConsumerConfigError,
    ViolationType,
    _attempt_of,
    apply_event,
    build_consumer_config,
    key_of,
)
from order_service.events import EventType, LifecycleEvent, OrderState

from tests.unit.order_service.consumer.conftest import FakeMessage


def _event(order_id: str = "ord-1", sequence: int = 1, event_type: EventType = EventType.ORDER_CREATED) -> LifecycleEvent:
    payload: dict = {}
    if event_type is EventType.ORDER_CREATED:
        payload = {
            "customer_id": "cust-1",
            "items": [{"sku": "sku-1", "qty": 1, "unit_price": 100}],
            "total_amount": 100,
            "payment": {"method": "CARD", "reference": "ref-1", "amount": 100},
        }
    elif event_type is EventType.SHIPPED:
        payload = {"carrier": "DHL", "tracking_number": "trk-1"}
    return LifecycleEvent(
        order_id=order_id, sequence=sequence, event_type=event_type, payload=payload
    )


# -- apply_event (R1.38, R1.39, R1.40) ------------------------------------------------


def test_apply_event_first_event_no_violations() -> None:
    fold, violations = apply_event(None, _event(sequence=1))
    assert violations == []
    assert fold.last_sequence == 1
    assert fold.state is OrderState.CREATED


def test_apply_event_sequence_gap_still_updates_fold() -> None:
    fold, violations = apply_event(None, _event(sequence=2))
    assert len(violations) == 1
    assert violations[0].type is ViolationType.SEQUENCE_GAP
    # R1.40 — the violation does not block the fold.
    assert fold.last_sequence == 2


def test_apply_event_illegal_transition_still_updates_fold() -> None:
    current, _ = apply_event(None, _event(sequence=1, event_type=EventType.ORDER_CREATED))
    fold, violations = apply_event(
        current, _event(sequence=2, event_type=EventType.DELIVERED)
    )
    assert len(violations) == 1
    assert violations[0].type is ViolationType.ILLEGAL_TRANSITION
    assert fold.state is OrderState.DELIVERED


def test_apply_event_gap_and_illegal_transition_both_reported() -> None:
    current, _ = apply_event(None, _event(sequence=1, event_type=EventType.ORDER_CREATED))
    # Sequence should be 2 next; skip to 5 AND send an event illegal after CREATED.
    fold, violations = apply_event(
        current, _event(sequence=5, event_type=EventType.DELIVERED)
    )
    types = {v.type for v in violations}
    assert types == {ViolationType.SEQUENCE_GAP, ViolationType.ILLEGAL_TRANSITION}
    assert fold.last_sequence == 5


# -- build_consumer_config / validate_protocol_settings (R2.21) ----------------------


def test_build_consumer_config_rejects_classic_only_setting_under_kip848() -> None:
    settings = Settings(
        _env_file=None,
        consumer_group_protocol=GroupProtocol.CONSUMER,
        consumer_assignment_strategy="range",
    )
    with pytest.raises(ConsumerConfigError):
        build_consumer_config(settings, group_id="g", client_id="c")


def test_build_consumer_config_classic_includes_strategy_and_omits_remote_assignor() -> None:
    settings = Settings(
        _env_file=None,
        consumer_group_protocol=GroupProtocol.CLASSIC,
        consumer_assignment_strategy="range",
    )
    config = build_consumer_config(settings, group_id="g", client_id="c")
    assert config["partition.assignment.strategy"] == "range"
    assert "group.remote.assignor" not in config


def test_build_consumer_config_kip848_includes_remote_assignor_and_omits_classic_keys() -> None:
    settings = Settings(
        _env_file=None,
        consumer_group_protocol=GroupProtocol.CONSUMER,
        consumer_remote_assignor="uniform",
    )
    config = build_consumer_config(settings, group_id="g", client_id="c")
    assert config["group.remote.assignor"] == "uniform"
    assert "partition.assignment.strategy" not in config
    assert "session.timeout.ms" not in config


# -- small header/key helpers ----------------------------------------------------------


def test_attempt_of_defaults_to_one_when_header_missing_or_malformed() -> None:
    assert _attempt_of({}) == 1
    assert _attempt_of({"x-attempt": "not-a-number"}) == 1


def test_attempt_of_parses_valid_header() -> None:
    assert _attempt_of({"x-attempt": "3"}) == 3


def test_key_of_null_key() -> None:
    assert key_of(FakeMessage(key=None)) == "<null>"


def test_key_of_undecodable_key() -> None:
    assert key_of(FakeMessage(key=b"\xff\xfe")) == "<undecodable-key>"


def test_key_of_normal_key() -> None:
    assert key_of(FakeMessage(key=b"ord-42")) == "ord-42"
