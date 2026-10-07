"""CSV input: parsing edge cases and shared validation."""
from src.fetchers.csv_fetcher import fetch_csv
from src.processing.consolidator import validate_all
from tests.conftest import NOW


def write(tmp_path, text, encoding="utf-8"):
    p = tmp_path / "in.csv"
    p.write_bytes(text.encode(encoding))
    return str(p)


def test_reads_rows_as_file_source(tmp_path):
    res = fetch_csv(write(tmp_path, "id,name,status,updated_at\n1,A,active,2026-01-01T00:00:00Z\n"))
    assert res.status == "ok" and len(res.records) == 1
    assert res.records[0].source == "FILE" and res.records[0].payload["name"] == "A"


def test_bom_header_case_and_blank_lines(tmp_path):
    text = "\ufeff ID , Name,STATUS,Updated_At\n\n1,A,active,2026-01-01T00:00:00Z\n,,,\n\n"
    res = fetch_csv(write(tmp_path, text))
    assert len(res.records) == 1 and res.errors == []
    valid, invalid = validate_all(res.records, now=NOW)
    assert len(valid) == 1 and not invalid


def test_short_rows_become_invalid_not_crashes(tmp_path):
    res = fetch_csv(write(tmp_path, "id,name,status,updated_at\n1,A,active\n"))
    valid, invalid = validate_all(res.records, now=NOW)
    assert not valid and "missing_updated_at" in invalid[0].reasons


def test_long_rows_extra_columns_are_ignored(tmp_path):
    res = fetch_csv(write(tmp_path, "id,name,status,updated_at\n1,A,active,2026-01-01T00:00:00Z,surprise\n"))
    valid, _ = validate_all(res.records, now=NOW)
    assert len(valid) == 1


def test_quoted_commas_and_unicode(tmp_path):
    res = fetch_csv(write(tmp_path, 'id,name,status,updated_at\n1,"Acme, Inc.",active,2026-01-01T00:00:00Z\n2,Ünï 株式会社,active,2026-01-01T00:00:00Z\n'))
    assert [r.payload["name"] for r in res.records] == ["Acme, Inc.", "Ünï 株式会社"]


def test_missing_file_is_reported_not_raised(tmp_path):
    res = fetch_csv(str(tmp_path / "nope.csv"))
    assert res.status == "failed" and "not found" in res.errors[0]


def test_empty_file(tmp_path):
    res = fetch_csv(write(tmp_path, ""))
    assert res.records == [] and res.errors == []


def test_missing_required_column_is_reported(tmp_path):
    res = fetch_csv(write(tmp_path, "id,name\n1,A\n"))
    assert "missing required column" in res.errors[0] and "status" in res.errors[0]


def test_undecodable_bytes_keep_earlier_rows(tmp_path):
    p = tmp_path / "in.csv"
    p.write_bytes(b"id,name,status,updated_at\n1,A,active,2026-01-01T00:00:00Z\n" + (b"x" * 5000) + b"\n2,\xff\xfe,active,2026-01-01T00:00:00Z\n")
    res = fetch_csv(str(p))
    assert res.errors and "UnicodeDecodeError" in res.errors[0]


def test_bundled_sample_file_has_expected_valid_and_invalid_rows():
    res = fetch_csv("data/sample_input.csv")
    valid, invalid = validate_all(res.records, now=NOW)
    assert len(res.records) == 13 and len(valid) == 9 and len(invalid) == 4
    assert {i.reasons[0] for i in invalid} == {"missing_name", "invalid_updated_at", "missing_id", "missing_updated_at"}
