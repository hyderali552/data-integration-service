"""CLI entry point, configuration and retry-policy maths."""
import json
import os

import pytest

from src.config import Settings
from src.main import main, parse_args
from src.retry import RetryPolicy, parse_retry_after


def test_backoff_is_exponential_and_capped():
    p = RetryPolicy(base_delay=1, max_delay=5)
    assert [p.delay_for(n) for n in (1, 2, 3, 4)] == [1, 2, 4, 5]


def test_retry_after_overrides_backoff_but_is_capped():
    p = RetryPolicy(base_delay=1, max_retry_after=10)
    assert p.delay_for(1, retry_after=3) == 3 and p.delay_for(1, retry_after=500) == 10


def test_jitter_stays_within_bounds():
    p = RetryPolicy(base_delay=10, max_delay=100, jitter=0.2)
    assert all(8 <= p.delay_for(1) <= 12 for _ in range(200))


@pytest.mark.parametrize("value,expected", [("2", 2.0), ("0.5", 0.5), (None, None), ("abc", None),
                                             ("-3", None), ("Wed, 21 Oct 2026 07:28:00 GMT", None)])
def test_parse_retry_after(value, expected):
    assert parse_retry_after(value) == expected


def test_settings_from_env_overrides_defaults():
    s = Settings.from_env({"DIS_DB_PATH": "x.db", "DIS_REQUEST_TIMEOUT": "7", "DIS_MAX_ATTEMPTS": "5",
                           "DIS_BASE_BACKOFF": "0.1", "DIS_DB_BATCH_SIZE": "42", "DIS_MAX_CONCURRENCY": "3"})
    assert (s.db_path, s.request_timeout, s.db_batch_size, s.max_concurrent_requests) == ("x.db", 7.0, 42, 3)
    assert s.fetch_retry.max_attempts == 5 and s.downstream_retry.base_delay == 0.1


def test_settings_defaults_without_env():
    assert Settings.from_env({}).db_path == "data/records.db"


def run_cli(tmp_path, *argv, env=None):
    db = str(tmp_path / "cli.db")
    out = str(tmp_path / "report.json")
    with pytest.raises(SystemExit) as exc:
        main([*argv, "--db", db, "--json", out])
    return exc.value.code, json.load(open(out)), db


def test_cli_happy_run_exit_0_and_json_report(tmp_path, capsys, monkeypatch):
    monkeypatch.setenv("DIS_REJECTED_PATH", str(tmp_path / "rej.jsonl"))
    code, report, db = run_cli(tmp_path, "--scenario", "happy")
    assert code == 0 and report["consolidated"] == 15 and report["confirmed"] == 15
    assert "RUN REPORT" in capsys.readouterr().out and os.path.exists(db)


def test_cli_degraded_run_exit_2(tmp_path, monkeypatch):
    monkeypatch.setenv("DIS_REJECTED_PATH", str(tmp_path / "rej.jsonl"))
    monkeypatch.setenv("DIS_BASE_BACKOFF", "0.001")
    monkeypatch.setenv("DIS_REQUEST_TIMEOUT", "0.05")
    code, report, _ = run_cli(tmp_path, "--scenario", "demo")
    assert code == 2 and report["exit_code"] == 2
    assert {s["source"]: s["status"] for s in report["sources"]} == {"A": "ok", "B": "partial", "C": "failed", "FILE": "ok"}


def test_cli_fatal_exit_1_when_db_unusable(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("DIS_REJECTED_PATH", str(tmp_path / "rej.jsonl"))
    with pytest.raises(SystemExit) as exc:
        main(["--scenario", "happy", "--db", str(tmp_path)])      # a directory is not a usable database file
    assert exc.value.code == 1
    assert "FATAL" in capsys.readouterr().out


def test_cli_reset_db_and_rerun_idempotency(tmp_path, monkeypatch):
    monkeypatch.setenv("DIS_REJECTED_PATH", str(tmp_path / "rej.jsonl"))
    _, first, db = run_cli(tmp_path, "--scenario", "happy")
    _, second, _ = run_cli(tmp_path, "--scenario", "happy")
    assert first["db"]["inserted"] == 15 and second["db"]["inserted"] == 0 and second["delivery_attempted"] == 0
    _, third, _ = run_cli(tmp_path, "--scenario", "happy", "--reset-db")
    assert third["db"]["inserted"] == 15


def test_cli_bulk_mode(tmp_path, monkeypatch):
    monkeypatch.setenv("DIS_REJECTED_PATH", str(tmp_path / "rej.jsonl"))
    code, report, _ = run_cli(tmp_path, "--bulk", "300")
    assert code == 0 and report["consolidated"] == 300


def test_parse_args_defaults():
    a = parse_args([])
    assert a.scenario == "demo" and a.bulk == 0 and a.base_url is None
