"""Database: insert, update, duplicate prevention, reprocessing, transactions, failures, delivery state."""
import random
import sqlite3
from datetime import datetime, timezone

import pytest

from src.db.database import Database, DatabaseError
from src.downstream.client import DeliveryOutcome, Outcome
from src.models import ValidRecord, idempotency_key_for
from src.processing.consolidator import consolidate


def vr(id_="1", ts="2026-01-01T00:00:00+00:00", source="A", name="n", status="active"):
    return ValidRecord(id_, name, status, datetime.fromisoformat(ts), source)


@pytest.fixture
def db(tmp_path):
    d = Database(str(tmp_path / "t.db"), batch_size=3)
    d.init()
    return d


def test_insert(db):
    stats = db.upsert_records([vr("1"), vr("2")])
    assert (stats.inserted, stats.updated, stats.unchanged) == (2, 0, 0)
    rows = db.get_all_records()
    assert [r["id"] for r in rows] == ["1", "2"] and rows[0]["updated_at"] == "2026-01-01T00:00:00.000000Z"


def test_update_with_newer_version(db):
    db.upsert_records([vr("1", name="old")])
    stats = db.upsert_records([vr("1", ts="2026-02-01T00:00:00+00:00", name="new", source="B")])
    assert stats.updated == 1
    row = db.get_all_records()[0]
    assert row["name"] == "new" and row["source"] == "B" and db.count_records() == 1


def test_older_version_never_overwrites_newer(db):
    db.upsert_records([vr("1", ts="2026-02-01T00:00:00+00:00", name="new")])
    stats = db.upsert_records([vr("1", ts="2026-01-01T00:00:00+00:00", name="stale")])
    assert stats.unchanged == 1 and db.get_all_records()[0]["name"] == "new"


def test_same_timestamp_higher_priority_source_wins_lower_does_not(db):
    db.upsert_records([vr("1", source="C", name="from C")])
    assert db.upsert_records([vr("1", source="B", name="from B")]).updated == 1
    assert db.upsert_records([vr("1", source="FILE", name="from FILE")]).unchanged == 1
    assert db.get_all_records()[0]["name"] == "from B"


def test_timezone_offsets_compare_chronologically_not_textually(db):
    db.upsert_records([vr("1", ts="2026-01-01T10:00:00+00:00", name="10:00Z")])
    # 11:00+05:30 == 05:30Z, which is EARLIER than 10:00Z even though "11" > "10" as text
    db.upsert_records([vr("1", ts="2026-01-01T11:00:00+05:30", name="05:30Z")])
    assert db.get_all_records()[0]["name"] == "10:00Z"


def test_duplicate_prevention_within_a_single_call(db):
    db.upsert_records([vr("1"), vr("1"), vr("1")])
    assert db.count_records() == 1


def test_reprocessing_same_input_is_a_noop(db):
    batch = [vr(str(i)) for i in range(10)]
    first = db.upsert_records(batch)
    before = db.get_all_records()
    second = db.upsert_records(batch)
    assert first.inserted == 10 and (second.inserted, second.updated, second.unchanged) == (0, 0, 10)
    assert db.get_all_records() == before          # byte-for-byte identical, incl. last_synced_at


def test_batches_are_split_by_batch_size(db):
    assert db.upsert_records([vr(str(i)) for i in range(10)]).inserted == 10   # batch_size=3 -> 4 transactions


def test_db_side_consolidation_equals_in_memory_consolidation(tmp_path):
    """Upserting UNconsolidated, shuffled records yields the same rows as consolidate()->upsert.
    This is the property that lets the pipeline stream at scale."""
    rnd = random.Random(42)
    recs = [vr(str(rnd.randint(1, 20)), ts=f"2026-01-{rnd.randint(1, 5):02d}T00:00:00+00:00",
               source=rnd.choice("ABC") if rnd.random() < .8 else "FILE", name=rnd.choice("xyz"))
            for _ in range(300)]
    a, b = Database(str(tmp_path / "a.db"), batch_size=7), Database(str(tmp_path / "b.db"), batch_size=50)
    a.init(); b.init()
    a.upsert_records(consolidate(recs))
    shuffled = recs[:]
    rnd.shuffle(shuffled)
    b.upsert_records(shuffled)
    strip = lambda rows: [{k: v for k, v in r.items() if "_at" not in k or k == "updated_at"} for r in rows]
    assert strip(a.get_all_records()) == strip(b.get_all_records())


# ---- transactions & failure handling -------------------------------------
def poisoned():
    """source=None violates NOT NULL -> a genuine sqlite IntegrityError in the middle of a batch."""
    return ValidRecord("p", "n", "active", datetime(2026, 1, 1, tzinfo=timezone.utc), None)


def test_failed_batch_is_rolled_back_atomically(db):
    with pytest.raises(DatabaseError):
        db.upsert_records([vr("1"), vr("2"), poisoned()])           # all three share one batch (size 3)
    assert db.count_records() == 0                                   # nothing from the batch committed


def test_earlier_committed_batches_survive_and_rerun_completes(db):
    batch = [vr(str(i)) for i in range(3)] + [poisoned()] + [vr("9")]
    with pytest.raises(DatabaseError):
        db.upsert_records(batch)
    assert db.count_records() == 3                                   # batch 1 committed, batch 2 rolled back
    db.upsert_records([vr(str(i)) for i in range(3)] + [vr("9")])    # "restart": idempotent re-run
    assert db.count_records() == 4


def test_database_unavailable_raises_database_error(tmp_path):
    bad = Database(str(tmp_path / "missing-dir" / "x.db"))
    with pytest.raises(DatabaseError):
        bad.init()
    with pytest.raises(DatabaseError):
        bad.upsert_records([vr()])


def test_database_locked_by_another_writer_raises_database_error(tmp_path):
    path = str(tmp_path / "l.db")
    d = Database(path, busy_timeout=0.05)
    d.init()
    blocker = sqlite3.connect(path, isolation_level=None)
    blocker.execute("BEGIN IMMEDIATE")
    try:
        with pytest.raises(DatabaseError, match="locked"):
            d.upsert_records([vr()])
    finally:
        blocker.execute("ROLLBACK"); blocker.close()
    assert d.upsert_records([vr()]).inserted == 1                    # recovers once the lock is gone


def test_foreign_key_enforced_on_delivery(db):
    bad = DeliveryOutcome("ghost", "k", Outcome.CONFIRMED, 1)
    with pytest.raises(DatabaseError):
        db.record_deliveries([bad])


# ---- delivery state ------------------------------------------------------
def outcome(rec, state, attempts=1, error=None):
    return DeliveryOutcome(rec.id, idempotency_key_for(rec), state, attempts, error)


def test_new_records_are_pending_confirmed_are_not(db):
    r1, r2 = vr("1"), vr("2")
    db.upsert_records([r1, r2])
    assert [r.id for r in db.pending_deliveries()] == ["1", "2"]
    db.record_deliveries([outcome(r1, Outcome.CONFIRMED)])
    assert [r.id for r in db.pending_deliveries()] == ["2"]


def test_unconfirmed_stays_pending_rejected_does_not(db):
    r1, r2 = vr("1"), vr("2")
    db.upsert_records([r1, r2])
    db.record_deliveries([outcome(r1, Outcome.UNCONFIRMED, 3, "ReadTimeout"), outcome(r2, Outcome.REJECTED, 1, "HTTP 400")])
    assert [r.id for r in db.pending_deliveries()] == ["1"]


def test_new_version_of_a_confirmed_record_becomes_pending_again(db):
    r = vr("1")
    db.upsert_records([r])
    db.record_deliveries([outcome(r, Outcome.CONFIRMED)])
    assert db.pending_deliveries() == []
    db.upsert_records([vr("1", ts="2026-03-01T00:00:00+00:00", name="v2")])
    assert [p.name for p in db.pending_deliveries()] == ["v2"]


def test_confirmed_is_never_downgraded_and_attempts_accumulate(db):
    r = vr("1")
    db.upsert_records([r])
    db.record_deliveries([outcome(r, Outcome.UNCONFIRMED, 3)])
    db.record_deliveries([outcome(r, Outcome.CONFIRMED, 1)])
    db.record_deliveries([outcome(r, Outcome.UNCONFIRMED, 3)])      # late/duplicate report must not regress
    assert db.delivery_states() == {"1": "confirmed"}
    with sqlite3.connect(db.path) as c:
        assert c.execute("SELECT attempts FROM delivery").fetchone()[0] == 4


def test_record_deliveries_with_nothing_is_fine(db):
    db.record_deliveries([])
