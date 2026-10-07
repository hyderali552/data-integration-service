"""Data processing: duplicates, latest selection, source-priority tie-break, determinism, isolation."""
import itertools
import random
from datetime import datetime, timezone

import pytest

from src.models import RawRecord, ValidRecord
from src.processing.consolidator import consolidate, validate_all
from tests.conftest import NOW, good


def vr(id_="1", ts="2026-01-01", source="A", name="n", status="active"):
    return ValidRecord(id_, name, status, datetime.fromisoformat(ts).replace(tzinfo=timezone.utc), source)


def test_latest_updated_at_wins_regardless_of_source():
    out = consolidate([vr(ts="2026-01-02", source="FILE", name="newest"), vr(ts="2026-01-01", source="A", name="old")])
    assert [(r.name, r.source) for r in out] == [("newest", "FILE")]


@pytest.mark.parametrize("higher,lower", [("A", "B"), ("A", "C"), ("A", "FILE"), ("B", "C"), ("B", "FILE"), ("C", "FILE")])
def test_same_timestamp_uses_source_priority(higher, lower):
    for order in ([higher, lower], [lower, higher]):
        out = consolidate([vr(source=s, name=s) for s in order])
        assert out[0].source == higher


def test_exact_duplicates_collapse_to_one():
    assert len(consolidate([vr(), vr(), vr()])) == 1


def test_duplicate_ids_within_one_source_different_versions():
    out = consolidate([vr(ts="2026-01-01", name="v1"), vr(ts="2026-01-03", name="v3"), vr(ts="2026-01-02", name="v2")])
    assert out[0].name == "v3"


def test_output_is_one_per_id_sorted_by_id():
    out = consolidate([vr("b"), vr("c"), vr("a"), vr("b", ts="2026-02-01")])
    assert [r.id for r in out] == ["a", "b", "c"]


def test_empty_input():
    assert consolidate([]) == []


def test_same_id_timestamp_and_source_but_conflicting_content_is_deterministic():
    a, b = vr(name="alpha"), vr(name="beta")
    assert consolidate([a, b]) == consolidate([b, a])


def test_result_is_independent_of_input_order():
    records = [vr("1", "2026-01-01", "A", "x"), vr("1", "2026-01-02", "C", "y"), vr("1", "2026-01-02", "B", "z"),
               vr("2", "2026-01-01", "FILE", "q"), vr("2", "2026-01-01", "C", "r"), vr("2", "2026-01-01", "C", "s")]
    expected = consolidate(records)
    for perm in itertools.permutations(records):
        assert consolidate(perm) == expected
    rnd = random.Random(7)
    for _ in range(50):
        shuffled = records[:]
        rnd.shuffle(shuffled)
        assert consolidate(shuffled) == expected


def test_invalid_records_are_isolated_and_do_not_affect_valid_ones():
    raws = [RawRecord("A", good("1")), RawRecord("A", "garbage"), RawRecord("A", good("2", name="")),
            RawRecord("B", good("3", ts="nope")), RawRecord("B", None), RawRecord("C", good("4"))]
    valid, invalid = validate_all(raws, now=NOW)
    assert [v.id for v in valid] == ["1", "4"]
    assert len(invalid) == 4
    assert [r.id for r in consolidate(valid)] == ["1", "4"]


def test_invalid_copy_does_not_hide_valid_copy_from_another_source():
    raws = [RawRecord("A", good("1", name="")), RawRecord("C", good("1", name="from C"))]
    valid, _ = validate_all(raws, now=NOW)
    assert consolidate(valid)[0].name == "from C"


def test_api_and_csv_records_go_through_the_same_validator():
    api = RawRecord("A", good("1", ts="bad"))
    csv_ = RawRecord("FILE", good("1", ts="bad"))
    _, inv = validate_all([api, csv_], now=NOW)
    assert [i.reasons for i in inv] == [("invalid_updated_at",)] * 2
