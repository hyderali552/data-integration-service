"""Runtime configuration.

Defaults are tuned so the demo finishes in a few seconds. Everything can be
overridden through ``DIS_*`` environment variables (see README) so no code
changes are needed to point the service at different files or timeouts.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field, replace
from datetime import timedelta

from src.retry import RetryPolicy


@dataclass(frozen=True)
class Settings:
    db_path: str = "data/records.db"
    csv_path: str = "data/sample_input.csv"
    rejected_path: str = "data/rejected_records.jsonl"

    request_timeout: float = 1.0        # per HTTP request (seconds)
    source_deadline: float = 60.0       # wall-clock budget per source across all pages/retries
    max_concurrent_requests: int = 10   # global cap on in-flight source requests
    max_pages: int = 10_000             # hard ceiling on pages per source (pagination guard)

    downstream_concurrency: int = 10
    downstream_timeout: float = 1.0

    db_batch_size: int = 500            # rows per transaction
    db_busy_timeout: float = 5.0        # seconds SQLite waits on a locked DB

    max_future_skew: timedelta = timedelta(days=1)  # updated_at further ahead than this is invalid

    fetch_retry: RetryPolicy = field(default_factory=lambda: RetryPolicy(max_attempts=3, base_delay=0.2))
    downstream_retry: RetryPolicy = field(default_factory=lambda: RetryPolicy(max_attempts=3, base_delay=0.2))

    @classmethod
    def from_env(cls, env=None) -> "Settings":
        env = os.environ if env is None else env
        base = cls()
        attempts = int(env.get("DIS_MAX_ATTEMPTS", base.fetch_retry.max_attempts))
        backoff = float(env.get("DIS_BASE_BACKOFF", base.fetch_retry.base_delay))
        retry = RetryPolicy(max_attempts=attempts, base_delay=backoff)
        timeout = float(env.get("DIS_REQUEST_TIMEOUT", base.request_timeout))
        return replace(
            base,
            db_path=env.get("DIS_DB_PATH", base.db_path),
            csv_path=env.get("DIS_CSV_PATH", base.csv_path),
            rejected_path=env.get("DIS_REJECTED_PATH", base.rejected_path),
            request_timeout=timeout,
            downstream_timeout=timeout,
            db_batch_size=int(env.get("DIS_DB_BATCH_SIZE", base.db_batch_size)),
            max_concurrent_requests=int(env.get("DIS_MAX_CONCURRENCY", base.max_concurrent_requests)),
            fetch_retry=retry,
            downstream_retry=retry,
        )


def configure_logging(verbose: bool = False) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.WARNING,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
