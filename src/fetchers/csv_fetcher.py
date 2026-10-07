"""CSV input -> the same `RawRecord` shape the API fetcher produces.

Parsing is deliberately forgiving (the validator, not the reader, decides what
is acceptable): BOM, header case/whitespace, blank lines, short rows, long
rows, quoted commas and unicode are all handled. File-level problems (missing
file, undecodable bytes) are reported as errors on the result - never raised -
and rows read before the problem are kept.
"""
from __future__ import annotations

import csv
import logging
from typing import Iterator

from src.models import RawRecord, SourceFetchResult

log = logging.getLogger(__name__)
SOURCE_NAME = "FILE"
REQUIRED_COLUMNS = ("id", "name", "status", "updated_at")


def iter_csv_records(path: str) -> Iterator[RawRecord]:
    """Stream rows one at a time (constant memory). Raises on file-level errors."""
    with open(path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.reader(fh)
        header = next(reader, None)
        if header is None:
            return
        columns = [h.strip().lower() for h in header]
        for row in reader:
            if not any(cell.strip() for cell in row):
                continue  # blank line / only separators
            payload = {col: (row[i] if i < len(row) else None) for i, col in enumerate(columns)}
            if len(row) > len(columns):
                payload["_extra_columns"] = row[len(columns):]
            yield RawRecord(SOURCE_NAME, payload, origin=f"line {reader.line_num}")


def fetch_csv(path: str) -> SourceFetchResult:
    result = SourceFetchResult(source=SOURCE_NAME)
    try:
        for rec in iter_csv_records(path):
            result.records.append(rec)
    except FileNotFoundError:
        result.errors.append(f"FILE: input file not found: {path}")
    except (OSError, UnicodeDecodeError, csv.Error) as exc:
        result.errors.append(f"FILE: could not fully read {path}: {type(exc).__name__}: {exc}")
    else:
        if result.records:
            missing = [c for c in REQUIRED_COLUMNS if c not in result.records[0].payload]
            if missing:
                result.errors.append(f"FILE: header is missing required column(s): {', '.join(missing)}")
    return result
