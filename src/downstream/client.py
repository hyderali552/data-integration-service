"""Idempotent, retrying client for the downstream ``POST /processed``.

THE problem: a POST times out - did the downstream process it or not? We cannot
know. Naive options are both wrong: *don't retry* can lose the update; *retry
blindly* can double-process it. Solution used here:

1. Every request carries a deterministic ``Idempotency-Key`` derived from the
   record's id + updated_at + content (`models.idempotency_key_for`). A retry
   (in this run, or in a later run after a crash) of the same version sends the
   SAME key, so the receiver can recognise it and answer "already processed".
2. Retry only what is transient: timeouts, connection errors, 408/429/5xx
   (honouring Retry-After) - with exponential backoff and a hard attempt cap.
3. Permanent 4xx (other than 408/429) are NOT retried: `REJECTED`.
4. When retries are exhausted we do not pretend to know the outcome: the
   record is `UNCONFIRMED`, persisted in the ``delivery`` table, and re-sent
   (same key, therefore safe) on the next run. Nothing is lost, nothing is
   silently duplicated, and the run keeps going for the other records.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Optional

import httpx

from src.models import ValidRecord, idempotency_key_for, to_db_timestamp
from src.retry import RetryPolicy, parse_retry_after

log = logging.getLogger(__name__)


class Outcome(str, Enum):
    CONFIRMED = "confirmed"      # 2xx received
    UNCONFIRMED = "unconfirmed"  # retries exhausted; downstream MAY have processed it
    REJECTED = "rejected"        # permanent 4xx; retrying will not help


@dataclass
class DeliveryOutcome:
    record_id: str
    idempotency_key: str
    outcome: Outcome
    attempts: int
    error: Optional[str] = None


def build_payload(record: ValidRecord) -> dict:
    return {"id": record.id, "name": record.name, "status": record.status,
            "updated_at": to_db_timestamp(record.updated_at), "source": record.source}


async def send_record(client: httpx.AsyncClient, record: ValidRecord, *,
                      policy: Optional[RetryPolicy] = None, timeout: float = 2.0) -> DeliveryOutcome:
    policy = policy or RetryPolicy()
    key = idempotency_key_for(record)
    payload = build_payload(record)
    last_error = "unknown"

    for attempt in range(1, policy.max_attempts + 1):
        retry_after = None
        try:
            resp = await client.post("/processed", json=payload,
                                     headers={"Idempotency-Key": key}, timeout=timeout)
        except httpx.TransportError as exc:
            last_error = type(exc).__name__  # includes timeouts: outcome ambiguous
        else:
            if 200 <= resp.status_code < 300:
                return DeliveryOutcome(record.id, key, Outcome.CONFIRMED, attempt)
            if not (resp.status_code in (408, 429) or 500 <= resp.status_code < 600):
                return DeliveryOutcome(record.id, key, Outcome.REJECTED, attempt, f"HTTP {resp.status_code}")
            last_error = f"HTTP {resp.status_code}"
            retry_after = parse_retry_after(resp.headers.get("Retry-After"))

        if attempt < policy.max_attempts:
            await policy.sleep(policy.delay_for(attempt, retry_after))

    return DeliveryOutcome(record.id, key, Outcome.UNCONFIRMED, policy.max_attempts, last_error)


async def send_all(client: httpx.AsyncClient, records: list, *, policy: Optional[RetryPolicy] = None,
                   timeout: float = 2.0, concurrency: int = 10) -> list:
    """Send many records with bounded concurrency (backpressure). Order preserved.
    A failure for one record never affects the others."""
    semaphore = asyncio.Semaphore(concurrency)

    async def one(rec: ValidRecord) -> DeliveryOutcome:
        async with semaphore:
            try:
                return await send_record(client, rec, policy=policy, timeout=timeout)
            except Exception as exc:  # defensive isolation
                log.exception("unexpected error sending %s", rec.id)
                return DeliveryOutcome(rec.id, idempotency_key_for(rec), Outcome.UNCONFIRMED, 0,
                                       f"unexpected {type(exc).__name__}: {exc}")

    return list(await asyncio.gather(*(one(r) for r in records)))
