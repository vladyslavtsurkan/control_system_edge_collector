"""Reusable exponential-backoff retry helper."""

import asyncio
import random
from collections.abc import Awaitable, Callable
from typing import TypeVar

import structlog

__all__ = ["retry_with_backoff"]

log = structlog.get_logger()

T = TypeVar("T")


async def retry_with_backoff(
    coro_factory: Callable[[], Awaitable[T]],
    *,
    max_retries: int = 0,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    operation_name: str = "operation",
    fatal_exceptions: tuple[type[BaseException], ...] = (),
) -> T:
    """Execute *coro_factory()* with exponential back-off + jitter.

    Parameters
    ----------
    coro_factory:
        A zero-arg callable returning a new awaitable on each invocation.
    max_retries:
        Maximum number of retries.  ``0`` means *infinite*.
    base_delay:
        Initial delay in seconds.
    max_delay:
        Upper-bound delay cap in seconds.
    operation_name:
        Human-readable label for log messages.
    fatal_exceptions:
        Exception types that must **not** be retried and should
        propagate immediately.
    """
    attempt = 0
    while True:
        try:
            return await coro_factory()
        except fatal_exceptions:
            raise  # non-retryable – propagate immediately
        except Exception as exc:
            attempt += 1
            if max_retries and attempt >= max_retries:
                log.error(
                    "max retries exceeded",
                    operation=operation_name,
                    attempts=attempt,
                    error=str(exc),
                )
                raise
            delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
            jitter = random.uniform(0, delay * 0.25)
            wait = delay + jitter
            log.warning(
                "retrying after error",
                operation=operation_name,
                attempt=attempt,
                wait_s=round(wait, 2),
                error=str(exc),
            )
            await asyncio.sleep(wait)
