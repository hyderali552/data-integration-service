"""SQLite persistence.

Tables
------
records   One row per logical entity (PRIMARY KEY id) = the consolidated winner.
delivery  One row per *version* of a record that we attempted to send
          downstream (PRIMARY KEY idempotency_key) with its outcome. This is
          what makes "send downstream" safe to resume after a crash.

Key properties
--------------
* Duplicate prevention: PRIMARY KEY + ``INSERT .. ON CONFLICT DO UPDATE``.
* Safe reprocessing: the conflict clause only overwrites a row when the
  incoming record is strictly "better" by the SAME ordering the consolidator
  uses (updated_at, source priority, name, status). Replaying old or identical
  data is a no-op; a stale/partial run can never regress newer data.
  Because of this, the database enforces the consolidation rule by itself -
  the property that lets the pipeline stream at scale (see README).
* Transactions: each batch is one ``BEGIN IMMEDIATE`` ... ``COMMIT``; any
  error rolls the whole batch back. Earlier committed batches stay valid
  because every write is idempotent.
* Resources: every operation opens a short-lived connection that is always
  closed. SQLite errors are wrapped in `DatabaseError` so callers handle one type.
"""
from __future__ import annotations

import logging
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Iterator

from src.models import SOURCE_PRIORITY, ValidRecord, idempotency_key_for, parse_timestamp, to_db_timestamp

log = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS records (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    status          TEXT NOT NULL,
    updated_at      TEXT NOT NULL,            -- fixed-width UTC, sorts chronologically
    source          TEXT NOT NULL,
    source_priority INTEGER NOT NULL,
    version_key     TEXT NOT NULL,            -- idempotency key of this exact version
    first_seen_at   TEXT NOT NULL,
    last_synced_at  TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS delivery (
    idempotency_key TEXT PRIMARY KEY,
    record_id       TEXT NOT NULL REFERENCES records(id),
    state           TEXT NOT NULL CHECK (state IN ('confirmed', 'unconfirmed', 'rejected')),
    attempts        INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT,
    updated_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_records_version_key ON records(version_key);
"""

_UPSERT = """
INSERT INTO records (id, name, status, updated_at, source, source_priority, version_key, first_seen_at, last_synced_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
    name = excluded.name, status = excluded.status, updated_at = excluded.updated_at,
    source = excluded.source, source_priority = excluded.source_priority,
    version_key = excluded.version_key, last_synced_at = excluded.last_synced_at
WHERE (excluded.updated_at, excluded.source_priority, excluded.name, excluded.status)
    > (records.updated_at, records.source_priority, records.name, records.status)
"""

_DELIVERY_UPSERT = """
INSERT INTO delivery (idempotency_key, record_id, state, attempts, last_error, updated_at)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(idempotency_key) DO UPDATE SET
    state = excluded.state, attempts = delivery.attempts + excluded.attempts,
    last_error = excluded.last_error, updated_at = excluded.updated_at
WHERE delivery.state != 'confirmed'
"""


class DatabaseError(RuntimeError):
    """Any database failure (unavailable, locked, constraint, bad binding...)."""


@dataclass
class UpsertStats:
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0

    def __iadd__(self, other: "UpsertStats") -> "UpsertStats":
        self.inserted += other.inserted
        self.updated += other.updated
        self.unchanged += other.unchanged
        return self


def _now() -> str:
    return to_db_timestamp(datetime.now(timezone.utc))


class Database:
    def __init__(self, path: str, *, batch_size: int = 500, busy_timeout: float = 5.0):
        self.path = path
        self.batch_size = max(1, batch_size)
        self.busy_timeout = busy_timeout

    # -- connection / transaction management -------------------------------
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=self.busy_timeout, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        conn = None
        try:
            conn = self._connect()
            yield conn
        except sqlite3.Error as exc:
            raise DatabaseError(f"{type(exc).__name__}: {exc}") from exc
        finally:
            if conn is not None:
                conn.close()

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        """BEGIN IMMEDIATE (take the write lock up front) ... COMMIT / ROLLBACK."""
        with self._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
                conn.execute("COMMIT")
            except BaseException:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass  # connection is already unusable; closing it discards the txn
                raise

    def init(self) -> None:
        with self._connection() as conn:
            try:
                conn.execute("PRAGMA journal_mode = WAL")
            except sqlite3.Error:
                pass  # e.g. in-memory / read-only FS: WAL is an optimisation, not a requirement
            conn.executescript(SCHEMA)

    # -- records -----------------------------------------------------------
    def upsert_records(self, records: Iterable[ValidRecord]) -> UpsertStats:
        """Insert new / improve existing rows. Idempotent. One transaction per batch."""
        total = UpsertStats()
        batch: list = []
        for rec in records:
            batch.append(rec)
            if len(batch) >= self.batch_size:
                total += self._upsert_batch(batch)
                batch = []
        if batch:
            total += self._upsert_batch(batch)
        return total

    def _upsert_batch(self, batch: list) -> UpsertStats:
        stats = UpsertStats()
        now = _now()
        with self._transaction() as conn:
            marks = ",".join("?" * len(batch))
            existing = {r["id"] for r in conn.execute(
                f"SELECT id FROM records WHERE id IN ({marks})", [r.id for r in batch])}
            for rec in batch:
                cur = conn.execute(_UPSERT, (
                    rec.id, rec.name, rec.status, to_db_timestamp(rec.updated_at), rec.source,
                    SOURCE_PRIORITY.get(rec.source, 0), idempotency_key_for(rec), now, now))
                if rec.id not in existing:
                    stats.inserted += 1
                elif cur.rowcount > 0:
                    stats.updated += 1
                else:
                    stats.unchanged += 1
        return stats

    def get_all_records(self) -> list:
        with self._connection() as conn:
            return [dict(r) for r in conn.execute("SELECT * FROM records ORDER BY id")]

    def count_records(self) -> int:
        with self._connection() as conn:
            return conn.execute("SELECT COUNT(*) FROM records").fetchone()[0]

    # -- downstream delivery state ----------------------------------------
    def pending_deliveries(self) -> list:
        """Current record versions that are NOT yet confirmed/rejected downstream.

        = never attempted (new or changed version) + previously 'unconfirmed'.
        """
        sql = """
            SELECT r.* FROM records r
            LEFT JOIN delivery d ON d.idempotency_key = r.version_key
            WHERE d.idempotency_key IS NULL OR d.state = 'unconfirmed'
            ORDER BY r.id
        """
        with self._connection() as conn:
            return [
                ValidRecord(r["id"], r["name"], r["status"], parse_timestamp(r["updated_at"]), r["source"])
                for r in conn.execute(sql)
            ]

    def record_deliveries(self, outcomes: Iterable) -> None:
        """Persist delivery outcomes (objects with record_id/idempotency_key/outcome/attempts/error)."""
        rows = [(o.idempotency_key, o.record_id, str(o.outcome.value), o.attempts, o.error, _now()) for o in outcomes]
        if not rows:
            return
        with self._transaction() as conn:
            conn.executemany(_DELIVERY_UPSERT, rows)

    def delivery_states(self) -> dict:
        with self._connection() as conn:
            return {r["record_id"]: r["state"] for r in conn.execute("SELECT record_id, state FROM delivery")}
