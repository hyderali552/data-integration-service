"""Data processing: validation rules (shared by API and CSV records)."""
from datetime import datetime, timedelta, timezone

import pytest

from src.models import InvalidRecord, ValidRecord, parse_timestamp, validate_record
from tests.conftest import NOW, good, raw


def check(payload, **kw):
    return validate_record(raw(payload), now=NOW, **kw)


def test_valid_record():
    r = check(good("rec-1", "Alpha", "active", "2026-01-10T10:00:00Z"))
    assert isinstance(r, ValidRecord)
    assert (r.id, r.name, r.status, r.source) == ("rec-1", "Alpha", "active", "A")
    assert r.updated_at == datetime(2026, 1, 10, 10, tzinfo=timezone.utc)


@pytest.mark.parametrize("field,reason", [("id", "missing_id"), ("name", "missing_name"),
                                          ("status", "missing_status"), ("updated_at", "missing_updated_at")])
@pytest.mark.parametrize("how", ["absent", "none", "empty", "blank"])
def test_missing_mandatory_fields(field, reason, how):
    p = good()
    if how == "absent":
        del p[field]
    else:
        p[field] = {"none": None, "empty": "", "blank": "   "}[how]
    r = check(p)
    assert isinstance(r, InvalidRecord) and reason in r.reasons


@pytest.mark.parametrize("ts", ["not-a-date", "2026-13-45T00:00:00Z", "2026-02-30T00:00:00Z", "yesterday",
                                "01/02/2026", "1736503200", 1736503200, 12.5, ["2026-01-01"], {"a": 1}, True])
def test_invalid_timestamps(ts):
    r = check(good(ts=ts))
    assert isinstance(r, InvalidRecord) and "invalid_updated_at" in r.reasons


@pytest.mark.parametrize("ts", ["0001-01-01T00:00:00Z", "1970-01-01T00:00:00Z"])
def test_sentinel_timestamps_are_invalid(ts):
    assert "invalid_updated_at" in check(good(ts=ts)).reasons


def test_far_future_timestamp_is_invalid_but_small_skew_is_tolerated():
    assert "future_updated_at" in check(good(ts="2099-01-01T00:00:00Z")).reasons
    ok = (NOW + timedelta(hours=2)).isoformat()
    assert isinstance(check(good(ts=ok)), ValidRecord)


def test_timestamps_are_normalised_to_utc():
    r = check(good(ts="2026-01-15T11:00:00+05:30"))
    assert r.updated_at == datetime(2026, 1, 15, 5, 30, tzinfo=timezone.utc)
    assert r.updated_at.utcoffset() == timedelta(0)


def test_naive_timestamp_is_assumed_utc():
    assert parse_timestamp("2026-01-01T00:00:00") == datetime(2026, 1, 1, tzinfo=timezone.utc)


def test_id_whitespace_is_stripped_and_int_ids_coerced():
    assert check(good(id_="  rec-9 ")).id == "rec-9"
    assert check(good(id_=12)).id == "12"


@pytest.mark.parametrize("bad_id", [True, 1.5, ["x"], {"a": 1}])
def test_wrong_typed_id_is_invalid(bad_id):
    assert "invalid_id" in check(good(id_=bad_id)).reasons


@pytest.mark.parametrize("payload", [None, "string", 42, ["list"], 3.14])
def test_non_object_payload_is_isolated_not_raised(payload):
    r = check(payload)
    assert isinstance(r, InvalidRecord) and r.reasons == ("payload_not_an_object",)


def test_unexpected_fields_are_ignored():
    r = check(good(tags=["x"], nested={"a": 1}, extra=None))
    assert isinstance(r, ValidRecord)


def test_unexpected_status_value_is_accepted():
    assert isinstance(check(good(status="totally-new-status")), ValidRecord)


def test_all_problems_are_reported_together():
    r = check({"id": "", "name": None, "status": 5, "updated_at": "bad"})
    assert set(r.reasons) == {"missing_id", "missing_name", "invalid_status", "invalid_updated_at"}


def test_origin_and_payload_are_kept_on_invalid_records_for_diagnostics():
    r = validate_record(raw({"id": ""}, origin="page 2#3"), now=NOW)
    assert r.origin == "page 2#3" and r.payload == {"id": ""}


def test_text_that_cannot_be_utf8_encoded_is_invalid():
    # a lone surrogate is legal in JSON ("\\ud800") but would crash hashing/storage later
    assert "invalid_name" in check(good(name="bad\ud800name")).reasons
    assert "invalid_id" in check(good(id_="x\udfff")).reasons
