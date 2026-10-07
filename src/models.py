"""Shared record models + the single validation pipeline.

EVERY record - from any REST source or from the CSV - becomes a `RawRecord`
and passes through `validate_record()`. There is exactly one place that
decides what "valid" means.

`validate_record` never raises: any problem (wrong type, garbage, missing
field) becomes an `InvalidRecord` carrying machine-readable reasons, so one bad
row can never abort a run.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Union

# Tie-break when id AND updated_at are equal: A > B > C > FILE.
SOURCE_PRIORITY = {"A": 4, "B": 3, "C": 2, "FILE": 1}

# Timestamps before this are almost always "null" sentinels (0001-01-01, epoch 0).
MIN_VALID_TIMESTAMP = datetime(1970, 1, 2, tzinfo=timezone.utc)
DEFAULT_MAX_FUTURE_SKEW = timedelta(days=1)


@dataclass
class RawRecord:
    """A record exactly as received, before validation. `payload` may be anything."""
    source: str
    payload: Any
    origin: str = ""  # e.g. "page 2" / "row 7" - for diagnostics only


@dataclass(frozen=True)
class ValidRecord:
    id: str
    name: str
    status: str
    updated_at: datetime  # always timezone-aware UTC
    source: str
    origin: str = ""

    @property
    def priority(self) -> int:
        return SOURCE_PRIORITY.get(self.source, 0)

    @property
    def version_key(self) -> tuple:
        """Total ordering used everywhere a "better" record must be chosen.

        (updated_at, source priority, name, status): latest timestamp first,
        then source priority, then content as a last-resort tie-break so the
        result never depends on arrival order (e.g. conflicting duplicates
        inside one source with the same timestamp).
        """
        return (self.updated_at, self.priority, self.name, self.status)


@dataclass(frozen=True)
class InvalidRecord:
    source: str
    reasons: tuple
    payload: Any = None
    origin: str = ""

    @property
    def reason(self) -> str:
        return ",".join(self.reasons)


@dataclass
class SourceFetchResult:
    """Outcome of reading one source (REST API or CSV). Never raises."""
    source: str
    records: list = field(default_factory=list)
    errors: list = field(default_factory=list)
    pages_fetched: int = 0
    requests_made: int = 0

    @property
    def status(self) -> str:
        if not self.errors:
            return "ok"
        return "partial" if self.records else "failed"


def parse_timestamp(value: Any) -> Optional[datetime]:
    """Parse an ISO-8601 string to an aware UTC datetime, else None.

    Naive timestamps are assumed to be UTC. Numbers (epoch seconds), other
    types and unparseable strings are rejected rather than guessed at.
    """
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        dt = datetime.fromisoformat(value.strip())
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    try:
        return dt.astimezone(timezone.utc)
    except (OverflowError, ValueError):
        return None


def _clean_text(value: Any, field_name: str, reasons: list, *, allow_int: bool = False) -> Optional[str]:
    if value is None or (isinstance(value, str) and not value.strip()):
        reasons.append(f"missing_{field_name}")
        return None
    if allow_int and isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    if not isinstance(value, str):
        reasons.append(f"invalid_{field_name}")
        return None
    try:
        value.encode("utf-8")  # e.g. a lone surrogate ("\ud800") is legal JSON but unstorable/unhashable
    except UnicodeEncodeError:
        reasons.append(f"invalid_{field_name}")
        return None
    return value.strip()


def validate_record(
    raw: RawRecord,
    now: Optional[datetime] = None,
    max_future_skew: timedelta = DEFAULT_MAX_FUTURE_SKEW,
) -> Union[ValidRecord, InvalidRecord]:
    """Validate one raw record. See README "Data rules" for the full table."""
    payload = raw.payload
    if not isinstance(payload, dict):
        return InvalidRecord(raw.source, ("payload_not_an_object",), payload, raw.origin)

    reasons: list = []
    rec_id = _clean_text(payload.get("id"), "id", reasons, allow_int=True)
    name = _clean_text(payload.get("name"), "name", reasons)
    status = _clean_text(payload.get("status"), "status", reasons)

    updated_at = None
    ts_raw = payload.get("updated_at")
    if ts_raw is None or (isinstance(ts_raw, str) and not ts_raw.strip()):
        reasons.append("missing_updated_at")
    else:
        updated_at = parse_timestamp(ts_raw)
        if updated_at is None:
            reasons.append("invalid_updated_at")
        elif updated_at < MIN_VALID_TIMESTAMP:
            reasons.append("invalid_updated_at")
            updated_at = None
        elif updated_at > (now or datetime.now(timezone.utc)) + max_future_skew:
            reasons.append("future_updated_at")
            updated_at = None

    if reasons:
        return InvalidRecord(raw.source, tuple(reasons), payload, raw.origin)
    return ValidRecord(rec_id, name, status, updated_at, raw.source, raw.origin)


def to_db_timestamp(dt: datetime) -> str:
    """Fixed-width UTC string. Lexicographic order == chronological order,
    which is what makes SQL comparisons on the column correct."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def idempotency_key_for(record: ValidRecord) -> str:
    """Deterministic key for one *version* of a record.

    Same content -> same key on every retry and every re-run; a new version of
    the record (different updated_at/content) -> a new key.
    """
    raw = "\x1f".join([record.id, to_db_timestamp(record.updated_at), record.name, record.status])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]
