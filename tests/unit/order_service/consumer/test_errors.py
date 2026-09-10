"""Unit tests for :func:`order_service.consumer.errors.classify` (R5.1, R5.3)."""

from order_service.consumer.errors import NonRetryableError, RetryableError, classify


def test_classify_non_retryable_stays_non_retryable() -> None:
    assert classify(NonRetryableError("bad payload")) is NonRetryableError


def test_classify_retryable_stays_retryable() -> None:
    assert classify(RetryableError("db down")) is RetryableError


def test_classify_unknown_exception_defaults_to_retryable() -> None:
    """An undeclared exception is the cheaper mistake to misfile (see module docstring)."""
    assert classify(ValueError("something else")) is RetryableError
