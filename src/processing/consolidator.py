"""Validation fan-out + consolidation (one winning record per id).

Rule: the record with the latest valid `updated_at` wins; ties are broken by
source priority A > B > C > FILE; any remaining tie (same id, timestamp AND
source, e.g. conflicting duplicates) is broken on content so the winner never
depends on arrival order. Both functions are pure -> trivially testable and
deterministic.
"""
from __future__ import annotations

from datetime import datetime
from typing import Iterable, Optional

from src.models import (DEFAULT_MAX_FUTURE_SKEW, InvalidRecord, RawRecord, ValidRecord,
                        validate_record)


def validate_all(raw_records: Iterable[RawRecord], now: Optional[datetime] = None,
                 max_future_skew=DEFAULT_MAX_FUTURE_SKEW) -> tuple:
    """Run every raw record through the shared validator -> (valid, invalid)."""
    valid, invalid = [], []
    for raw in raw_records:
        outcome = validate_record(raw, now=now, max_future_skew=max_future_skew)
        (invalid if isinstance(outcome, InvalidRecord) else valid).append(outcome)
    return valid, invalid


def consolidate(records: Iterable[ValidRecord]) -> list:
    """Return one winning record per id, sorted by id."""
    best: dict = {}
    for rec in records:
        current = best.get(rec.id)
        if current is None or rec.version_key > current.version_key:
            best[rec.id] = rec
    return [best[k] for k in sorted(best)]
