"""What kind of failure this was, which is what decides where the message goes (005 D2).

Two failures that look identical in a stack trace need opposite treatment. A dropped
database connection, a downstream ``503``, a lock timeout — retrying works. Malformed
JSON, a schema violation, an ``order_id`` that will never exist — retrying produces the
identical exception every time, and the attempts are spent proving it.

So classification comes first, and routing follows from it:

============================  ==========================================================
:class:`RetryableError`       the retry topic, until the attempt budget is spent
:class:`NonRetryableError`    the dead-letter topic, immediately, one attempt made
============================  ==========================================================
"""

from typing import TypeAlias

#: What :func:`classify` returns — the *kind* of failure, not an instance of one.
FailureKind: TypeAlias = "type[RetryableError] | type[NonRetryableError]"


class RetryableError(Exception):
    """The environment failed; the same message may well succeed on another attempt."""


class NonRetryableError(Exception):
    """The message itself is wrong; no number of attempts will change that (R5.2)."""


def classify(exc: BaseException) -> FailureKind:
    """Return the kind of failure ``exc`` represents (R5.1, R5.3).

    Anything not declared as one of the two is treated as **retryable**. The two mistakes
    are not symmetric, and this is the cheaper one: a handler bug misfiled as retryable
    costs the attempt budget and reaches the dead-letter topic anyway, while a genuine
    outage misfiled as poison is discarded on its first attempt with no second chance.

    Args:
        exc: The exception a handler or the decoder raised.

    Returns:
        :class:`NonRetryableError` if the message can never be processed as it stands,
        :class:`RetryableError` otherwise.
    """
    if isinstance(exc, NonRetryableError):
        return NonRetryableError
    return RetryableError
