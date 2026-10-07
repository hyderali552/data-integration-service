"""End-to-end behaviour, including every failure scenario listed in Part 6 of the brief."""
import json

import pytest

import src.pipeline as pipeline
from src.config import Settings
from src.db.database import Database, DatabaseError
from src.mock_servers.data import generate_bulk, write_csv
from src.mock_servers.scenarios import PRESETS, Scenario
from src.mock_servers.transport import make_client
from src.mock_servers.world import create_app
from src.pipeline import EXIT_DEGRADED, EXIT_FATAL, EXIT_OK, run_pipeline
from tests.conftest import NOW

EXPECTED_HAPPY = {
    "rec-001": ("FILE", "Alpha Corp (FILE newer)"),   # newest timestamp beats source priority
    "rec-002": ("B", "Beta LLC (B)"),                  # B newer than A and FILE
    "rec-003": ("C", "Gamma Ltd"),                     # A's copy invalid; C's valid copy used
    "rec-004": ("A", "Delta Inc"),
    "rec-006": ("A", "Zeta Partners (A)"),             # A/B/C/FILE tie -> priority A
    "rec-007": ("A", "Eta Ltd"),                       # extra fields ignored
    "rec-008": ("B", "Theta Group"),                   # latest of two versions in one source
    "rec-010": ("B", "Kappa Inc"),                     # whitespace-padded id normalised
    "rec-011": ("C", "Lambda GmbH"),
    "12":      ("C", "Mu Numeric Id"),                 # integer id coerced
    "rec-015": ("FILE", "Omicron Works"),
    "rec-018": ("FILE", "Rho, Inc."),
    "rec-019": ("FILE", "Sigma Systems"),              # unexpected status value accepted
    "rec-021": ("FILE", "Upsilon Extra"),
    "rec-022": ("FILE", "Ünïcödé 株式会社"),
}


def make(settings, scenario=None, dataset=None):
    scenario = scenario or PRESETS["happy"]
    app = create_app(scenario, dataset)
    return app, make_client(app, scenario)


async def run(settings, scenario=None, dataset=None, app_client=None, **kw):
    app, client = app_client or make(settings, scenario, dataset)
    async with client:
        report = await run_pipeline(settings, client, client, now=NOW, **kw)
    return app, report


def table(settings):
    return {r["id"]: (r["source"], r["name"]) for r in Database(settings.db_path).get_all_records()}


async def test_happy_path_produces_exactly_the_expected_consolidated_records(settings):
    app, report = await run(settings)
    assert table(settings) == EXPECTED_HAPPY
    assert (report.consolidated, report.confirmed, report.exit_code) == (15, 15, EXIT_OK)
    assert len(app.state.world.effects) == 15


async def test_invalid_records_are_written_to_the_dead_letter_file(settings):
    _, report = await run(settings)
    rows = [json.loads(l) for l in open(settings.rejected_path, encoding="utf-8")]
    assert len(rows) == report.invalid == 11
    assert {r["source"] for r in rows} == {"A", "B", "C", "FILE"}
    assert all(r["reasons"] and r["origin"] for r in rows)
    assert any(r["payload"] == "this-is-not-an-object" for r in rows)


async def test_rerunning_the_same_input_changes_nothing(settings):
    app, first = await run(settings)
    snapshot = Database(settings.db_path).get_all_records()
    _, second = await run(settings)                                  # fresh mock, same input, same DB
    assert (second.db.inserted, second.db.updated, second.db.unchanged) == (0, 0, 15)
    assert second.delivery_attempted == 0                           # nothing re-sent downstream
    assert Database(settings.db_path).get_all_records() == snapshot
    assert Database(settings.db_path).count_records() == 15


async def test_same_downstream_reused_across_runs_sees_no_duplicates(settings):
    ac = make(settings)
    await run(settings, app_client=ac)
    ac2 = (ac[0], make_client(ac[0], PRESETS["happy"]))              # same server state, new client
    await run(settings, app_client=ac2)
    assert ac[0].state.world.stats()["downstream_requests"] == 15


# ---- Part 6 failure scenarios -----------------------------------------------
async def test_source_a_completely_unavailable(settings):
    _, report = await run(settings, PRESETS["source-a-down"])
    a = next(s for s in report.sources if s.source == "A")
    assert a.status == "failed" and a.records == 0
    t = table(settings)
    assert "rec-004" not in t and "rec-002" in t and "rec-015" in t   # B, C, FILE still consolidated and persisted
    assert report.exit_code == EXIT_DEGRADED and report.confirmed == report.consolidated > 0


async def test_source_b_fails_after_returning_a_page(settings):
    _, report = await run(settings, Scenario(sources={"b": "fail_after_page_1"}))
    b = next(s for s in report.sources if s.source == "B")
    assert b.status == "partial" and b.records == 3 and b.pages == 1
    assert table(settings)["rec-002"][0] == "B"                      # page-1 data was kept and used
    assert "rec-010" not in table(settings)                          # page-2 data (lost) is absent
    assert report.exit_code == EXIT_DEGRADED


async def test_source_c_continuously_429(settings, sleeper):
    _, report = await run(settings, Scenario(sources={"c": "always_429"}))
    c = next(s for s in report.sources if s.source == "C")
    assert c.status == "failed" and "gave up after 3 attempts (HTTP 429)" in c.errors[0]
    assert len(sleeper.delays) >= 2                                  # it did back off, bounded
    assert table(settings)["rec-001"][0] == "FILE" and report.exit_code == EXIT_DEGRADED


async def test_pagination_loop_does_not_run_forever(settings):
    app, report = await run(settings, PRESETS["pagination-loop"])
    a = next(s for s in report.sources if s.source == "A")
    assert any("pagination loop" in e for e in a.errors) and a.pages == 2
    hits = app.state.world.source_hits
    assert hits[("a", 1)] == 1 and hits[("a", 2)] == 1               # page 2 was never requested twice


async def test_database_unavailable_is_fatal_and_nothing_is_delivered(settings, tmp_path):
    bad = Settings(**{**settings.__dict__, "db_path": str(tmp_path / "no-such-dir" / "x.db")})
    app, report = await run(bad)
    assert report.exit_code == EXIT_FATAL and "database" in report.fatal_error
    assert app.state.world.stats()["downstream_requests"] == 0       # no delivery without persistence


async def test_database_failure_mid_write_rolls_back_then_rerun_recovers(settings, monkeypatch):
    real = Database._upsert_batch
    calls = {"n": 0}

    def flaky(self, batch):
        calls["n"] += 1
        if calls["n"] == 2:
            raise DatabaseError("disk I/O error (injected)")
        return real(self, batch)

    small = Settings(**{**settings.__dict__, "db_batch_size": 5})
    monkeypatch.setattr(Database, "_upsert_batch", flaky)
    app, report = await run(small)
    assert report.exit_code == EXIT_FATAL and Database(small.db_path).count_records() == 5   # batch 1 committed only
    assert app.state.world.stats()["downstream_requests"] == 0

    monkeypatch.undo()
    _, report2 = await run(small)                                   # "restart"
    assert report2.exit_code == EXIT_OK and table(small) == EXPECTED_HAPPY


async def test_downstream_times_out_after_accepting_and_nothing_is_duplicated(settings):
    app, report = await run(settings, Scenario(downstream="timeout_once", downstream_delay=5))
    w = app.state.world
    assert report.confirmed == 15 and report.exit_code == EXIT_OK
    assert w.stats()["downstream_requests"] == 30                    # every record sent twice (timeout, then retry)
    assert w.stats()["downstream_unique_effects"] == 15              # ...but processed exactly once


async def test_downstream_down_leaves_records_unconfirmed_and_next_run_delivers_them(settings):
    app1, r1 = await run(settings, PRESETS["downstream-down"])
    assert r1.unconfirmed == 15 and r1.exit_code == EXIT_DEGRADED
    assert Database(settings.db_path).count_records() == 15          # data safely persisted regardless
    assert len(Database(settings.db_path).pending_deliveries()) == 15

    app2, r2 = await run(settings)                                  # downstream healthy again
    assert r2.db.unchanged == 15 and r2.delivery_attempted == 15 and r2.confirmed == 15
    assert len(app2.state.world.effects) == 15 and Database(settings.db_path).pending_deliveries() == []


async def test_downstream_permanent_rejection_is_recorded_and_not_retried_forever(settings):
    _, r1 = await run(settings, PRESETS["downstream-reject"])
    assert r1.rejected == 15 and r1.exit_code == EXIT_DEGRADED
    _, r2 = await run(settings)
    assert r2.delivery_attempted == 0                                # rejected versions are not hammered every run


async def test_crash_after_persist_before_delivery_then_restart(settings, monkeypatch):
    async def crash(*a, **k):
        raise RuntimeError("process killed")

    monkeypatch.setattr(pipeline, "send_all", crash)
    with pytest.raises(RuntimeError):
        await run(settings)
    assert Database(settings.db_path).count_records() == 15          # persisted before the "crash"
    monkeypatch.undo()
    app, report = await run(settings)
    assert report.db.inserted == 0 and report.confirmed == 15 and len(app.state.world.effects) == 15


async def test_crash_after_delivery_before_state_recorded_is_deduplicated_downstream(settings, monkeypatch):
    app, client = make(settings)
    monkeypatch.setattr(Database, "record_deliveries", lambda self, o: (_ for _ in ()).throw(DatabaseError("lost")))
    async with client:
        r1 = await run_pipeline(settings, client, client, now=NOW)
    assert "will be re-sent idempotently" in r1.fatal_error
    monkeypatch.undo()
    _, r2 = await run(settings, app_client=(app, make_client(app, PRESETS["happy"])))
    assert r2.delivery_attempted == 15 and r2.confirmed == 15        # re-sent...
    assert len(app.state.world.effects) == 15                        # ...with no duplicate side effects


async def test_newer_version_arriving_later_updates_row_and_redelivers_only_that_record(settings):
    await run(settings)
    ds = create_app(PRESETS["happy"]).state.world.dataset
    ds.pages["c"][2]["records"].append({"id": "rec-004", "name": "Delta Inc v2", "status": "active",
                                         "updated_at": "2026-03-01T00:00:00Z"})
    app, report = await run(settings, dataset=ds)
    assert (report.db.updated, report.delivery_attempted, report.confirmed) == (1, 1, 1)
    assert table(settings)["rec-004"] == ("C", "Delta Inc v2")


async def test_a_stale_partial_run_cannot_regress_stored_data(settings):
    await run(settings)
    _, report = await run(settings, PRESETS["source-a-down"])        # run with the newest sources missing
    assert report.db.updated == 0 and table(settings) == EXPECTED_HAPPY


async def test_all_sources_failing_still_completes_cleanly_as_degraded(settings, tmp_path):
    s = Scenario(sources={"a": "down", "b": "down", "c": "down"})
    cfg = Settings(**{**settings.__dict__, "csv_path": str(tmp_path / "missing.csv")})
    _, report = await run(cfg, s)
    assert report.consolidated == 0 and report.exit_code == EXIT_DEGRADED and report.fatal_error is None


async def test_conflicting_versions_across_sources_resolve_deterministically(settings):
    await run(settings)
    first = Database(settings.db_path).get_all_records()
    settings2 = Settings(**{**settings.__dict__, "db_path": settings.db_path + ".2"})
    await run(settings2)
    strip = lambda rows: [{k: v for k, v in r.items() if k not in ("first_seen_at", "last_synced_at")} for r in rows]
    assert strip(first) == strip(Database(settings2.db_path).get_all_records())


@pytest.mark.slow
async def test_ten_thousand_records_end_to_end(settings, tmp_path):
    ds = generate_bulk(10_000)
    csv_path = str(tmp_path / "bulk.csv")
    write_csv(csv_path, ds.csv_rows)
    _, report = await run(settings, PRESETS["happy"], ds, csv_path=csv_path)
    assert report.exit_code == EXIT_OK
    assert report.consolidated == 10_000 == Database(settings.db_path).count_records()
    assert report.confirmed == 10_000 and report.invalid > 0
    # B and the FILE hold newer/tied copies for some ids: spot-check both resolution paths
    rows = {r["id"]: r for r in Database(settings.db_path).get_all_records()}
    assert rows["bulk-000001"]["source"] == "A"                      # only A has it
    assert rows["bulk-000004"]["source"] == "B"                      # B newer than A (i % 4 == 0 -> +1h)
    assert rows["bulk-000002"]["source"] == "A"                      # A/B tie -> A
