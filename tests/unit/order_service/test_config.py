"""Unit tests for environment-driven settings (config.py)."""

import pytest

from order_service.config import (
    ProcessingGuarantee,
    Settings,
    StateRebuild,
)


def make_settings(**overrides: object) -> Settings:
    """Return a ``Settings`` isolated from the real environment/.env file."""
    return Settings(_env_file=None, **overrides)


class TestBlankIsUnset:
    """A blank env value on the affected fields is treated as unset."""

    def test_blank_string_becomes_none(self) -> None:
        settings = make_settings(consumer_instance_id="   ")
        assert settings.consumer_instance_id is None

    def test_empty_string_becomes_none(self) -> None:
        settings = make_settings(handler_failure_orders="")
        assert settings.handler_failure_orders is None

    def test_non_blank_value_passes_through(self) -> None:
        settings = make_settings(consumer_instance_id="worker-1")
        assert settings.consumer_instance_id == "worker-1"


class TestRefuseCheckpointUnderExactlyOnce:
    """R8.12 — this one combination is rejected outright."""

    def test_exactly_once_with_checkpoint_raises(self) -> None:
        with pytest.raises(ValueError, match="STATE_REBUILD=checkpoint"):
            make_settings(
                processing_guarantee=ProcessingGuarantee.EXACTLY_ONCE,
                state_rebuild=StateRebuild.CHECKPOINT,
            )

    def test_exactly_once_with_full_is_accepted(self) -> None:
        settings = make_settings(
            processing_guarantee=ProcessingGuarantee.EXACTLY_ONCE,
            state_rebuild=StateRebuild.FULL,
        )
        assert settings.state_rebuild is StateRebuild.FULL

    def test_at_least_once_with_checkpoint_is_accepted(self) -> None:
        settings = make_settings(
            processing_guarantee=ProcessingGuarantee.AT_LEAST_ONCE,
            state_rebuild=StateRebuild.CHECKPOINT,
        )
        assert settings.state_rebuild is StateRebuild.CHECKPOINT


class TestExactlyOnceProperty:
    def test_true_under_exactly_once(self) -> None:
        settings = make_settings(processing_guarantee=ProcessingGuarantee.EXACTLY_ONCE)
        assert settings.exactly_once is True

    def test_false_under_at_least_once(self) -> None:
        settings = make_settings(processing_guarantee=ProcessingGuarantee.AT_LEAST_ONCE)
        assert settings.exactly_once is False


class TestGroupIdFor:
    def test_explicit_group_id_wins(self) -> None:
        settings = make_settings(consumer_group_id="custom-group")
        assert settings.group_id_for("inventory") == "custom-group"

    def test_unset_falls_back_to_service_name(self) -> None:
        settings = make_settings()
        assert settings.group_id_for("inventory") == "inventory-service"


class TestInstanceLabel:
    def test_explicit_instance_id_wins(self) -> None:
        settings = make_settings(consumer_instance_id="worker-1")
        assert settings.instance_label == "worker-1"

    def test_falls_back_to_hostname(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("socket.gethostname", lambda: "host-abc")
        settings = make_settings()
        assert settings.instance_label == "host-abc"


class TestTransactionalIdFor:
    def test_combines_group_id_and_instance_label(self) -> None:
        settings = make_settings(consumer_instance_id="worker-1")
        assert settings.transactional_id_for("inventory-service") == (
            "inventory-service-worker-1"
        )


class TestChangelogTopicFor:
    def test_combines_prefix_and_group_id(self) -> None:
        settings = make_settings(state_changelog_prefix="order-fold")
        assert settings.changelog_topic_for("inventory-service") == (
            "order-fold.inventory-service"
        )


class TestRetryBackoffSchedule:
    def test_parses_comma_separated_values(self) -> None:
        settings = make_settings(retry_backoff_seconds="30,120")
        assert settings.retry_backoff_schedule == [30.0, 120.0]

    def test_empty_string_falls_back_to_default(self) -> None:
        settings = make_settings(retry_backoff_seconds="")
        assert settings.retry_backoff_schedule == [30.0]

    def test_tolerates_whitespace_and_trailing_comma(self) -> None:
        settings = make_settings(retry_backoff_seconds=" 30 , 120 ,")
        assert settings.retry_backoff_schedule == [30.0, 120.0]


class TestBackoffForAttempt:
    def test_attempt_below_two_waits_nothing(self) -> None:
        settings = make_settings(retry_backoff_seconds="30,120")
        assert settings.backoff_for_attempt(1) == 0.0

    def test_attempt_two_uses_first_entry(self) -> None:
        settings = make_settings(retry_backoff_seconds="30,120")
        assert settings.backoff_for_attempt(2) == 30.0

    def test_attempt_three_uses_second_entry(self) -> None:
        settings = make_settings(retry_backoff_seconds="30,120")
        assert settings.backoff_for_attempt(3) == 120.0

    def test_attempt_beyond_schedule_clamps_to_last_entry(self) -> None:
        settings = make_settings(retry_backoff_seconds="30,120")
        assert settings.backoff_for_attempt(10) == 120.0


class TestFailingOrders:
    def test_unset_is_empty(self) -> None:
        settings = make_settings()
        assert settings.failing_orders == frozenset()

    def test_parses_comma_separated_ids_with_whitespace(self) -> None:
        settings = make_settings(handler_failure_orders=" ord-1, ord-2 ,")
        assert settings.failing_orders == frozenset({"ord-1", "ord-2"})


class TestProducerDeliveryWaitSeconds:
    def test_derived_from_message_timeout(self) -> None:
        settings = make_settings(producer_message_timeout_ms=30_000)
        assert settings.producer_delivery_wait_seconds == 31.0
