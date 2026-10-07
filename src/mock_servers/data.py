"""Mock datasets served by the mock sources.

`curated_dataset()` is a small hand-built set where EVERY record exists to
exercise one rule (the comments say which). Together with data/sample_input.csv
it yields exactly 15 consolidated records - the e2e tests assert that.

`generate_bulk(n)` builds a large deterministic dataset (default 10,000 unique
ids) with overlap, duplicates, conflicts and invalid rows, for the
performance/scale demonstration.
"""
from __future__ import annotations

import csv
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

PAGE_SIZE_BULK = 500


@dataclass
class Dataset:
    pages: dict = field(default_factory=dict)      # {"a": {1: {...page...}}, "b": ..., "c": ...}
    csv_rows: list = field(default_factory=list)   # only used by bulk; curated uses data/sample_input.csv
    expected_unique_ids: int = 0


def _rec(id_, name, status, ts, **extra):
    return {"id": id_, "name": name, "status": status, "updated_at": ts, **extra}


def curated_dataset() -> Dataset:
    a = {
        1: {"records": [
            _rec("rec-001", "Alpha Corp", "active", "2026-01-10T10:00:00Z"),
            _rec("rec-002", "Beta LLC (A)", "active", "2026-01-11T09:30:00Z"),      # older than B's copy
            _rec("rec-003", "", "active", "2026-01-09T08:00:00Z"),                  # missing name -> invalid (C has a valid copy)
            _rec("rec-004", "Delta Inc", "inactive", "2026-01-09T08:00:00Z"),
        ], "next_page": 2},
        2: {"records": [
            _rec("rec-005", "Epsilon Co", "active", "not-a-timestamp"),             # invalid timestamp
            _rec("rec-001", "Alpha Corp", "active", "2026-01-10T10:00:00Z"),        # exact duplicate within source
            _rec("rec-006", "Zeta Partners (A)", "active", "2026-01-13T15:00:00Z"), # 3-way timestamp tie: A wins
            _rec("rec-007", "Eta Ltd", "active", "2026-01-14T10:00:00Z", tags=["x"], region="EU"),  # unexpected fields
            "this-is-not-an-object",                                                # malformed record mid-page
            None,
        ], "next_page": 3},
        3: {"records": [], "next_page": None},                                      # trailing empty page
    }
    b = {
        1: {"records": [
            _rec("rec-002", "Beta LLC (B)", "active", "2026-01-12T12:00:00Z"),      # newer version than A's -> B wins
            _rec("rec-008", "Theta Group (old)", "active", "2026-01-07T00:00:00Z"),
            _rec("rec-008", "Theta Group", "active", "2026-01-08T00:00:00Z"),       # different versions in one source
        ], "next_page": 2},
        2: {"records": [
            _rec("rec-006", "Zeta Partners (B)", "active", "2026-01-13T15:00:00Z"), # tie, loses to A
            {"id": "rec-009", "name": "Iota Systems", "updated_at": "2026-01-15T11:00:00Z"},  # missing status field
            _rec(" rec-010 ", "Kappa Inc", "active", "2026-01-12T00:00:00Z"),      # id with whitespace -> "rec-010"
        ], "next_page": None},
    }
    c = {
        1: {"records": [
            _rec("rec-006", "Zeta Partners (C)", "active", "2026-01-13T15:00:00Z"), # tie, loses to A and B
            _rec("rec-011", "Lambda GmbH", "active", "2026-01-15T11:00:00+05:30"),  # non-UTC offset
            _rec(12, "Mu Numeric Id", "active", "2026-01-16T00:00:00Z"),            # integer id -> "12"
        ], "next_page": 2},
        2: {"records": [
            _rec("rec-013", "Nu Future", "active", "2099-01-01T00:00:00Z"),        # far-future timestamp -> invalid
            {"id": "rec-014", "name": "Xi Missing", "updated_at": "2026-01-01T00:00:00Z"},  # status absent
            _rec("rec-003", "Gamma Ltd", "active", "2026-01-09T08:00:00Z"),         # valid copy of the entity A had invalid
        ], "next_page": None},
    }
    return Dataset(pages={"a": a, "b": b, "c": c}, expected_unique_ids=15)


def generate_bulk(n_unique: int = 10_000, page_size: int = PAGE_SIZE_BULK) -> Dataset:
    """Deterministic large dataset. Roughly: every id appears in >=1 source,
    ~1/2 also in B, ~1/3 in C, ~1/4 in FILE (with different timestamps/names,
    so conflicts resolve both by recency and by priority), plus an exact
    duplicate and an invalid row every so often."""
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    rows = {"a": [], "b": [], "c": [], "file": []}
    for i in range(n_unique):
        rid = f"bulk-{i:06d}"
        ts = base + timedelta(minutes=i % 5000)
        iso = ts.strftime("%Y-%m-%dT%H:%M:%SZ")
        newer = (ts + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        if i % 7 == 0:
            rows["a"].append(_rec(rid, f"A-{i}", "active", iso))
            rows["a"].append(_rec(rid, f"A-{i}", "active", iso))                   # duplicate within source
        else:
            rows["a"].append(_rec(rid, f"A-{i}", "active", iso))
        if i % 2 == 0:
            rows["b"].append(_rec(rid, f"B-{i}", "active", iso if i % 4 else newer))  # tie (A wins) or newer (B wins)
        if i % 3 == 0:
            rows["c"].append(_rec(rid, f"C-{i}", "inactive", iso))
        if i % 4 == 0:
            rows["file"].append({"id": rid, "name": f"F-{i}", "status": "active", "updated_at": iso})
        if i % 97 == 0:
            rows["a"].append(_rec(f"bad-{i}", "", "active", iso))                  # invalid: missing name
            rows["b"].append(_rec(f"bad-{i}", "Bad", "active", "garbage"))         # invalid: timestamp
    pages = {}
    for key in ("a", "b", "c"):
        chunks = [rows[key][i:i + page_size] for i in range(0, len(rows[key]), page_size)] or [[]]
        pages[key] = {n + 1: {"records": ch, "next_page": n + 2 if n + 1 < len(chunks) else None}
                      for n, ch in enumerate(chunks)}
    return Dataset(pages=pages, csv_rows=rows["file"], expected_unique_ids=n_unique)


def write_csv(path: str, rows: list) -> None:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=["id", "name", "status", "updated_at"])
        w.writeheader()
        w.writerows(rows)
