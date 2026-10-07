"""Resilient, paginated, concurrent fetching from REST sources.

Behaviour summary (see README "Technical decisions" for the reasoning):

* Transient failures (connection errors, timeouts, HTTP 408/429/5xx) are
  retried with exponential backoff, honouring a capped ``Retry-After``.
* Permanent failures (other 4xx, malformed body) are NOT retried.
* A page that cannot be fetched ends pagination for *that source only*; pages
  already collected are kept (partial success).
* Pagination is guarded three ways: a seen-pages set (the "page 2 -> next_page
  2" loop), a max-pages ceiling, and a per-source wall-clock deadline.
* `fetch_source` never raises, so one source can't take down the others.
* Pages are produced by an async generator (`iter_source_pages`) - the
  building block for a streaming pipeline at larger scale.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import AsyncIterator, Optional

import httpx

from src.models import RawRecord, SourceFetchResult
from src.retry import RetryPolicy, parse_retry_after

log = logging.getLogger(__name__)

TRANSIENT_STATUSES = {408, 429}


@dataclass(frozen=True)
class SourceSpec:
    name: str   # "A", "B", "C" - also the priority key
    path: str   # e.g. "/source/a"


def _is_transient_status(status: int) -> bool:
    return status in TRANSIENT_STATUSES or 500 <= status < 600


async def _request_json(client, spec, page, policy: RetryPolicy, timeout, semaphore, result):
    """GET one page with retries. Returns parsed JSON, or None (error recorded)."""
    where = f"{spec.name} page {page}"
    for attempt in range(1, policy.max_attempts + 1):
        retry_after = None
        try:
            async with semaphore:  # held only while the request is in flight, not while backing off
                resp = await client.get(spec.path, params={"page": page}, timeout=timeout)
            result.requests_made += 1
        except httpx.TransportError as exc:  # connect errors, timeouts, resets, protocol errors
            reason = f"{type(exc).__name__}"
        else:
            status = resp.status_code
            if status == 200:
                try:
                    return resp.json()
                except ValueError:
                    result.errors.append(f"{where}: malformed JSON body")
                    return None
            if not _is_transient_status(status):
                result.errors.append(f"{where}: permanent failure HTTP {status}")
                return None
            reason = f"HTTP {status}"
            retry_after = parse_retry_after(resp.headers.get("Retry-After"))

        if attempt == policy.max_attempts:
            result.errors.append(f"{where}: gave up after {attempt} attempts ({reason})")
            return None
        delay = policy.delay_for(attempt, retry_after)
        log.info("%s: %s, retry %d/%d in %.2fs", where, reason, attempt, policy.max_attempts - 1, delay)
        await policy.sleep(delay)
    return None  # pragma: no cover  (loop always returns)


async def iter_source_pages(
    client: httpx.AsyncClient,
    spec: SourceSpec,
    result: SourceFetchResult,
    *,
    policy: RetryPolicy,
    timeout: float,
    semaphore: asyncio.Semaphore,
    max_pages: int,
) -> AsyncIterator[list]:
    """Yield the ``records`` list of each page, following ``next_page``."""
    page: Optional[int] = 1
    seen: set = set()
    while page is not None:
        if page in seen:
            result.errors.append(f"{spec.name}: pagination loop detected (next_page={page} already fetched); stopping")
            return
        if len(seen) >= max_pages:
            result.errors.append(f"{spec.name}: exceeded max_pages={max_pages}; stopping")
            return
        seen.add(page)

        data = await _request_json(client, spec, page, policy, timeout, semaphore, result)
        if data is None:
            return
        records = data.get("records") if isinstance(data, dict) else None
        if not isinstance(records, list):
            result.errors.append(f"{spec.name} page {page}: malformed response (no 'records' list)")
            return
        result.pages_fetched += 1
        yield records

        nxt = data.get("next_page")
        if nxt is not None and (isinstance(nxt, bool) or not isinstance(nxt, int) or nxt < 1):
            result.errors.append(f"{spec.name} page {page}: invalid next_page={nxt!r}; stopping")
            return
        page = nxt


async def fetch_source(
    client: httpx.AsyncClient,
    spec: SourceSpec,
    *,
    policy: Optional[RetryPolicy] = None,
    timeout: float = 2.0,
    semaphore: Optional[asyncio.Semaphore] = None,
    deadline: Optional[float] = None,
    max_pages: int = 10_000,
) -> SourceFetchResult:
    """Fetch every page of one source. Never raises; failures land in ``result.errors``."""
    policy = policy or RetryPolicy()
    semaphore = semaphore or asyncio.Semaphore(10)
    result = SourceFetchResult(source=spec.name)
    try:
        async with asyncio.timeout(deadline):
            async for page_no, records in _numbered(
                iter_source_pages(client, spec, result, policy=policy, timeout=timeout,
                                  semaphore=semaphore, max_pages=max_pages)
            ):
                result.records.extend(
                    RawRecord(spec.name, rec, origin=f"page {page_no}#{i}") for i, rec in enumerate(records)
                )
    except TimeoutError:
        result.errors.append(f"{spec.name}: source deadline of {deadline}s exceeded; keeping {len(result.records)} records")
    except Exception as exc:  # defensive: isolation is a hard requirement
        log.exception("unexpected error fetching %s", spec.name)
        result.errors.append(f"{spec.name}: unexpected {type(exc).__name__}: {exc}")
    return result


async def _numbered(agen):
    n = 0
    async for item in agen:
        n += 1
        yield n, item


async def fetch_all_sources(
    client: httpx.AsyncClient,
    specs: list,
    *,
    policy: Optional[RetryPolicy] = None,
    timeout: float = 2.0,
    deadline: Optional[float] = None,
    max_concurrent_requests: int = 10,
    max_pages: int = 10_000,
) -> list:
    """Fetch all sources concurrently. Results are returned in ``specs`` order."""
    semaphore = asyncio.Semaphore(max_concurrent_requests)
    outcomes = await asyncio.gather(
        *(fetch_source(client, s, policy=policy, timeout=timeout, semaphore=semaphore,
                       deadline=deadline, max_pages=max_pages) for s in specs),
        return_exceptions=True,
    )
    results = []
    for spec, outcome in zip(specs, outcomes):
        if isinstance(outcome, BaseException):  # should be unreachable; belt and braces
            outcome = SourceFetchResult(spec.name, errors=[f"{spec.name}: unexpected {outcome!r}"])
        results.append(outcome)
    return results
