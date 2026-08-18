"""Making handlers fail on purpose (005 D11).

Every rung of this ladder has a lever, because a failure path nobody can trigger is a
failure path nobody has seen: 001 had ``force``, 002 ``handler_delay_seconds``, 003
``state_crash_after``. This is 005's.

It covers the two failures that go through a *handler*. It deliberately does not simulate
a **decode** failure — a handler raising "pretend this was malformed JSON" would exercise
the routing while proving nothing about the decoder. ``scripts/produce_poison.sh`` writes
genuinely malformed bytes for that.
"""

import logging

from order_service.config import HandlerFailureMode, Settings
from order_service.consumer.errors import NonRetryableError, RetryableError
from order_service.events import LifecycleEvent

logger = logging.getLogger(__name__)


def maybe_fail(settings: Settings, event: LifecycleEvent, attempt: int) -> None:
    """Raise if the failure lever is aimed at this event (R5.19).

    Args:
        settings: Resolved environment settings.
        event: The event about to be handled.
        attempt: The 1-based attempt about to run. Attempt 1 is the main consumer's;
            later attempts are the retry worker's.

    Raises:
        RetryableError: Under ``transient``, until ``handler_failure_attempts`` has been
            spent — so the attempt after that one succeeds, and recovery is observable.
        NonRetryableError: Under ``poison``, on every attempt.
    """
    mode = settings.handler_failure_mode
    if mode is HandlerFailureMode.NONE:
        return

    # An empty set means every order. Naming orders is the usual case, because a lever
    # aimed at everything makes the good path unobservable at the same time.
    targeted = settings.failing_orders
    if targeted and event.order_id not in targeted:
        return

    if mode is HandlerFailureMode.POISON:
        raise NonRetryableError(
            f"HANDLER_FAILURE_MODE=poison: {event.order_id} seq {event.sequence} "
            "can never be processed"
        )

    if attempt <= settings.handler_failure_attempts:
        raise RetryableError(
            f"HANDLER_FAILURE_MODE=transient: {event.order_id} seq {event.sequence} "
            f"failed attempt {attempt} of {settings.handler_failure_attempts}"
        )

    logger.info(
        "FAILURE_LEVER_RECOVERED order_id=%s seq=%d attempt=%d",
        event.order_id,
        event.sequence,
        attempt,
    )
