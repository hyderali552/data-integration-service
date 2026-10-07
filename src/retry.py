"""Retry policy shared by the API fetcher and the downstream client.

The policy is a small immutable value object. It owns *how long to wait*
(exponential backoff, optional jitter, `Retry-After` handling with a cap) and
*how to wait* (an injectable ``sleep``), so tests can run retry logic with zero
real delay and still assert on the exact delays that would have been used.
"""
from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Optional

SleepFn = Callable[[float], Awaitable[None]]


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3          # total tries, including the first one
    base_delay: float = 0.5        # delay after attempt 1; doubles each attempt
    max_delay: float = 8.0         # cap for exponential backoff
    max_retry_after: float = 30.0  # never obey a server asking us to wait longer
    jitter: float = 0.0            # +/- fraction; 0 keeps runs deterministic
    sleep: SleepFn = field(default_factory=lambda: asyncio.sleep)

    def delay_for(self, attempt: int, retry_after: Optional[float] = None) -> float:
        """Delay to wait *after* failed attempt number ``attempt`` (1-based)."""
        if retry_after is not None and retry_after >= 0:
            delay = min(retry_after, self.max_retry_after)
        else:
            delay = min(self.base_delay * (2 ** (attempt - 1)), self.max_delay)
        if self.jitter:
            delay *= 1 + random.uniform(-self.jitter, self.jitter)
        return max(delay, 0.0)


def parse_retry_after(value: Optional[str]) -> Optional[float]:
    """Parse a ``Retry-After`` header given in seconds. HTTP-dates are ignored
    (we fall back to normal backoff) - simple and safe."""
    if value is None:
        return None
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return None
    return seconds if seconds >= 0 else None
