"""The mocks themselves must behave as documented, or the e2e tests prove nothing."""
import httpx
import pytest

from src.mock_servers.data import curated_dataset, generate_bulk
from src.mock_servers.scenarios import PRESETS, Scenario
from src.mock_servers.transport import make_client
from src.mock_servers.world import create_app


async def get(scenario, path, timeout=1.0):
    app = create_app(scenario)
    async with make_client(app, scenario) as c:
        return await c.get(path, timeout=timeout)


async def test_normal_source_serves_pages_and_empty_beyond_the_end():
    r = await get(Scenario(), "/source/a?page=1")
    assert r.status_code == 200 and r.json()["next_page"] == 2 and len(r.json()["records"]) == 4
    assert (await get(Scenario(), "/source/a?page=99")).json() == {"records": [], "next_page": None}


async def test_unknown_source_and_bad_page():
    assert (await get(Scenario(), "/source/zzz")).status_code == 404
    assert (await get(Scenario(), "/source/a?page=0")).status_code == 422


@pytest.mark.parametrize("mode,status", [("down", 503), ("always_429", 429)])
async def test_failure_modes(mode, status):
    r = await get(Scenario(sources={"a": mode}), "/source/a")
    assert r.status_code == status
    if status == 429:
        assert "Retry-After" in r.headers


async def test_fail_after_page_1():
    s = Scenario(sources={"b": "fail_after_page_1"})
    assert (await get(s, "/source/b?page=1")).status_code == 200
    assert (await get(s, "/source/b?page=2")).status_code == 500


async def test_flaky_recovers_after_n_failures():
    s = Scenario(sources={"a": "flaky"}, flaky_failures=2)
    app = create_app(s)
    async with make_client(app, s) as c:
        codes = [(await c.get("/source/a")).status_code for _ in range(4)]
    assert codes[:2] == [500, 503] and codes[2:] == [200, 200]


async def test_malformed_and_wrong_shape_and_loop_and_empty():
    r = await get(Scenario(sources={"a": "malformed_json"}), "/source/a")
    with pytest.raises(ValueError):
        r.json()
    assert "records" not in (await get(Scenario(sources={"a": "bad_shape"}), "/source/a")).json()
    assert (await get(Scenario(sources={"a": "empty"}), "/source/a")).json()["records"] == []
    loop = Scenario(sources={"a": "pagination_loop"})
    assert (await get(loop, "/source/a?page=1")).json()["next_page"] == 2
    assert (await get(loop, "/source/a?page=2")).json()["next_page"] == 2


async def test_slow_source_really_times_out_in_process():
    with pytest.raises(httpx.ReadTimeout):
        await get(Scenario(sources={"a": "slow"}, slow_seconds=5), "/source/a", timeout=0.05)


async def test_unreachable_source_raises_connect_error_only_for_that_source():
    s = Scenario(unreachable_sources=("a",))
    with pytest.raises(httpx.ConnectError):
        await get(s, "/source/a")
    assert (await get(s, "/source/b")).status_code == 200


async def test_downstream_modes():
    for mode, code in [("down", 503), ("reject_400", 400), ("ok", 200)]:
        s = Scenario(downstream=mode)
        async with make_client(create_app(s), s) as c:
            assert (await c.post("/processed", json={"id": "1"}, headers={"Idempotency-Key": "k"})).status_code == code
    s = Scenario(downstream="ok")
    async with make_client(create_app(s), s) as c:
        assert (await c.post("/processed", content=b"nope", headers={"Idempotency-Key": "k"})).status_code == 400
        assert (await c.post("/processed", json=[1], headers={"Idempotency-Key": "k"})).status_code == 400


async def test_downstream_flaky_then_ok_and_debug_stats():
    s = Scenario(downstream="flaky_503", flaky_failures=1)
    app = create_app(s)
    async with make_client(app, s) as c:
        h = {"Idempotency-Key": "k"}
        assert (await c.post("/processed", json={"id": "1"}, headers=h)).status_code == 503
        assert (await c.post("/processed", json={"id": "1"}, headers=h)).json()["status"] == "accepted"
        assert (await c.post("/processed", json={"id": "1"}, headers=h)).json()["status"] == "already_processed"
        assert (await c.get("/_debug/stats")).json()["downstream_unique_effects"] == 1


def test_presets_are_valid_and_datasets_have_the_documented_shape():
    assert {"happy", "demo", "source-a-down", "downstream-timeout"} <= set(PRESETS)
    ds = curated_dataset()
    assert set(ds.pages) == {"a", "b", "c"} and len(ds.pages["a"]) == 3          # multiple pages
    assert ds.pages["a"][3]["records"] == []                                      # empty page present
    bulk = generate_bulk(1000)
    assert bulk.expected_unique_ids == 1000 and bulk.csv_rows and len(bulk.pages["a"]) > 1
    assert generate_bulk(1000).pages["a"][1] == bulk.pages["a"][1]                # deterministic
