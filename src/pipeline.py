"""End-to-end orchestration:

    fetch (concurrent) -> validate -> consolidate -> persist -> deliver downstream

Design rules:
* Partial source failure degrades the run (exit code 2) but never discards data
  from healthy sources - they are still validated, persisted and delivered.
* Downstream delivery happens only AFTER the database commit succeeded, and only
  for versions not yet confirmed (state lives in the DB), so a crash at any
  point is recovered by simply running again.
* A database failure is fatal for the run (exit code 1): nothing is delivered
  that was not persisted.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Optional

import httpx

from src.config import Settings
from src.db.database import Database, DatabaseError, UpsertStats
from src.downstream.client import Outcome, send_all
from src.fetchers.api_fetcher import SourceSpec, fetch_all_sources
from src.fetchers.csv_fetcher import fetch_csv
from src.processing.consolidator import consolidate, validate_all

log = logging.getLogger(__name__)

SOURCES = [SourceSpec("A", "/source/a"), SourceSpec("B", "/source/b"), SourceSpec("C", "/source/c")]

EXIT_OK, EXIT_FATAL, EXIT_DEGRADED = 0, 1, 2


@dataclass
class SourceReport:
    source: str
    status: str
    records: int
    pages: int
    errors: list


@dataclass
class RunReport:
    sources: list = field(default_factory=list)
    valid: int = 0
    invalid: int = 0
    invalid_by_reason: dict = field(default_factory=dict)
    consolidated: int = 0
    db: Optional[UpsertStats] = None
    delivery_attempted: int = 0
    confirmed: int = 0
    unconfirmed: int = 0
    rejected: int = 0
    fatal_error: Optional[str] = None
    duration_seconds: float = 0.0

    @property
    def degraded(self) -> bool:
        return any(s.status != "ok" for s in self.sources) or self.unconfirmed > 0 or self.rejected > 0

    @property
    def exit_code(self) -> int:
        if self.fatal_error:
            return EXIT_FATAL
        return EXIT_DEGRADED if self.degraded else EXIT_OK

    def to_dict(self) -> dict:
        d = asdict(self)
        d["exit_code"] = self.exit_code
        return d

    def format(self) -> str:
        lines = ["=" * 64, "RUN REPORT", "=" * 64, "Sources:"]
        for s in self.sources:
            lines.append(f"  {s.source:<5} {s.status.upper():<8} {s.records:>6} records, {s.pages} page(s)")
            lines += [f"        ! {e}" for e in s.errors]
        lines.append(f"Validation : {self.valid} valid, {self.invalid} invalid {dict(self.invalid_by_reason) or ''}")
        lines.append(f"Consolidate: {self.consolidated} unique records")
        if self.db:
            lines.append(f"Database   : inserted={self.db.inserted} updated={self.db.updated} unchanged={self.db.unchanged}")
        lines.append(f"Downstream : attempted={self.delivery_attempted} confirmed={self.confirmed} "
                     f"unconfirmed={self.unconfirmed} rejected={self.rejected}")
        if self.fatal_error:
            lines.append(f"FATAL      : {self.fatal_error}")
        lines.append(f"Duration   : {self.duration_seconds:.2f}s   Exit code: {self.exit_code} "
                     f"({'ok' if self.exit_code == 0 else 'fatal' if self.exit_code == 1 else 'degraded'})")
        return "\n".join(lines)


def write_rejected(path: str, invalid: list) -> None:
    """Dead-letter file: every rejected record with source, location and reasons.
    Overwritten each run so it always reflects the latest input."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for inv in invalid:
            fh.write(json.dumps({"source": inv.source, "origin": inv.origin, "reasons": list(inv.reasons),
                                 "payload": inv.payload}, default=str, ensure_ascii=False) + "\n")


async def run_pipeline(settings: Settings, api_client: httpx.AsyncClient, downstream_client: httpx.AsyncClient,
                       *, csv_path: Optional[str] = None, now: Optional[datetime] = None) -> RunReport:
    started = asyncio.get_running_loop().time()
    report = RunReport()

    # 1. FETCH - all REST sources and the CSV concurrently
    api_task = fetch_all_sources(
        api_client, SOURCES, policy=settings.fetch_retry, timeout=settings.request_timeout,
        deadline=settings.source_deadline, max_concurrent_requests=settings.max_concurrent_requests,
        max_pages=settings.max_pages)
    csv_task = asyncio.to_thread(fetch_csv, csv_path or settings.csv_path)
    api_results, csv_result = await asyncio.gather(api_task, csv_task)

    raw = []
    for res in (*api_results, csv_result):
        report.sources.append(SourceReport(res.source, res.status, len(res.records), res.pages_fetched, res.errors))
        raw.extend(res.records)

    # 2. VALIDATE (shared pipeline) and 3. CONSOLIDATE
    valid, invalid = validate_all(raw, now=now, max_future_skew=settings.max_future_skew)
    report.valid, report.invalid = len(valid), len(invalid)
    report.invalid_by_reason = dict(Counter(r for inv in invalid for r in inv.reasons))
    write_rejected(settings.rejected_path, invalid)
    consolidated = consolidate(valid)
    report.consolidated = len(consolidated)

    # 4. PERSIST - fatal on DB failure; nothing is delivered that isn't committed
    db = Database(settings.db_path, batch_size=settings.db_batch_size, busy_timeout=settings.db_busy_timeout)
    try:
        db.init()
        report.db = db.upsert_records(consolidated)
        pending = db.pending_deliveries()
    except DatabaseError as exc:
        report.fatal_error = f"database unavailable/failed: {exc}"
        log.error(report.fatal_error)
        report.duration_seconds = asyncio.get_running_loop().time() - started
        return report

    # 5. DELIVER only versions not yet confirmed (new/changed + previously unconfirmed)
    report.delivery_attempted = len(pending)
    outcomes = await send_all(downstream_client, pending, policy=settings.downstream_retry,
                              timeout=settings.downstream_timeout, concurrency=settings.downstream_concurrency)
    counts = Counter(o.outcome for o in outcomes)
    report.confirmed, report.unconfirmed, report.rejected = (
        counts[Outcome.CONFIRMED], counts[Outcome.UNCONFIRMED], counts[Outcome.REJECTED])
    try:
        db.record_deliveries(outcomes)
    except DatabaseError as exc:
        # Deliveries happened but could not be recorded: the next run re-sends them with
        # the SAME idempotency keys, which the downstream de-duplicates. Safe by design.
        report.fatal_error = f"could not record delivery state (will be re-sent idempotently): {exc}"
        log.error(report.fatal_error)

    report.duration_seconds = asyncio.get_running_loop().time() - started
    return report

