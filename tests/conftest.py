"""Shared fixtures/helpers.

Mocking strategy (details in README):
  * Unit tests of the fetcher/downstream client use `httpx.MockTransport` with
    tiny handler functions -> total control of status codes, headers, bodies,
    timeouts and connection errors, no app code involved.
  * Integration/e2e tests use the real mock FastAPI app through
    `InProcessTransport` (which enforces timeouts) - realistic HTTP semantics, no sockets.
  * Time is mocked by injecting `RetryPolicy.sleep`: tests never really wait,
    and can assert on the exact backoff delays requested.
"""
from datetime import datetime, timezone

import httpx
import pytest

from src.config import Settings
from src.models import RawRecord
from src.retry import RetryPolicy

NOW = datetime(2026, 6, 1, tzinfo=timezone.utc)


class SleepRecorder:
    """Drop-in for asyncio.sleep that records requested delays instead of waiting."""
    def __init__(self):
        self.delays = []

    async def __call__(self, seconds):
        self.delays.append(seconds)


@pytest.fixture
def sleeper():
    return SleepRecorder()


@pytest.fixture
def policy(sleeper):
    return RetryPolicy(max_attempts=3, base_delay=1.0, max_delay=8.0, max_retry_after=5.0, sleep=sleeper)


def client_for(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="http://test")


def raw(payload, source="A", origin=""):
    return RawRecord(source=source, payload=payload, origin=origin)


def good(id_="r1", name="Name", status="active", ts="2026-01-01T00:00:00Z", **extra):
    return {"id": id_, "name": name, "status": status, "updated_at": ts, **extra}


@pytest.fixture
def settings(tmp_path, sleeper):
    retry = RetryPolicy(max_attempts=3, base_delay=0.01, sleep=sleeper)
    return Settings(
        db_path=str(tmp_path / "records.db"),
        csv_path="data/sample_input.csv",
        rejected_path=str(tmp_path / "rejected.jsonl"),
        request_timeout=0.2, downstream_timeout=0.2,
        fetch_retry=retry, downstream_retry=retry,
    )
