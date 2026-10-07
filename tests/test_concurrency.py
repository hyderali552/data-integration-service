"""Concurrency: sources run concurrently and independently."""
import asyncio

import httpx

from src.fetchers.api_fetcher import SourceSpec, fetch_all_sources
from tests.conftest import client_for, good

SPECS = [SourceSpec("A", "/source/a"), SourceSpec("B", "/source/b"), SourceSpec("C", "/source/c")]


def ok_body(rid):
    return {"records": [good(rid)], "next_page": None}


async def test_sources_are_fetched_concurrently_not_sequentially(policy):
    """Deterministic proof (no timing): every source's handler waits at a barrier that
    only opens once ALL THREE requests are in flight. A sequential implementation would
    deadlock (and fail via the timeout); a concurrent one passes."""
    barrier = asyncio.Barrier(3)

    async def handler(request):
        await barrier.wait()
        return httpx.Response(200, json=ok_body(request.url.path[-1]))

    async with client_for(handler) as c:
        results = await asyncio.wait_for(fetch_all_sources(c, SPECS, policy=policy), timeout=2)
    assert [r.source for r in results] == ["A", "B", "C"]
    assert all(r.status == "ok" and len(r.records) == 1 for r in results)


async def test_results_come_back_in_spec_order_regardless_of_completion_order(policy):
    async def handler(request):
        await asyncio.sleep({"a": 0.03, "b": 0.01, "c": 0.0}[request.url.path[-1]])
        return httpx.Response(200, json=ok_body("x"))

    async with client_for(handler) as c:
        results = await fetch_all_sources(c, SPECS, policy=policy)
    assert [r.source for r in results] == ["A", "B", "C"]


async def test_failure_of_one_source_does_not_stop_the_others(policy):
    def handler(request):
        letter = request.url.path[-1]
        if letter == "a":
            raise httpx.ConnectError("A is down", request=request)
        if letter == "b":
            return httpx.Response(200, json=ok_body("b1")) if request.url.params["page"] == "1" else httpx.Response(500)
        return httpx.Response(200, json=ok_body("c1"))

    async with client_for(handler) as c:
        a, b, cc = await fetch_all_sources(c, SPECS, policy=policy)
    assert (a.status, len(a.records)) == ("failed", 0)
    assert (cc.status, len(cc.records)) == ("ok", 1)
    assert b.status == "ok" or b.status == "partial"
    assert len(b.records) == 1


async def test_a_source_that_raises_unexpectedly_is_contained(policy):
    def handler(request):
        if request.url.path.endswith("a"):
            raise ValueError("bug in A path")
        return httpx.Response(200, json=ok_body("z"))

    async with client_for(handler) as c:
        a, b, cc = await fetch_all_sources(c, SPECS, policy=policy)
    assert a.status == "failed" and b.status == cc.status == "ok"


async def test_slow_source_is_cut_off_by_deadline_while_others_finish_immediately(policy):
    async def handler(request):
        if request.url.path.endswith("a"):
            await asyncio.sleep(5)
        return httpx.Response(200, json=ok_body("ok"))

    async with client_for(handler) as c:
        a, b, cc = await asyncio.wait_for(fetch_all_sources(c, SPECS, policy=policy, deadline=0.1), timeout=2)
    assert a.status == "failed" and "deadline" in a.errors[0]
    assert len(b.records) == len(cc.records) == 1


async def test_global_concurrency_limit_is_respected(policy):
    state = {"now": 0, "peak": 0}

    async def handler(request):
        state["now"] += 1
        state["peak"] = max(state["peak"], state["now"])
        await asyncio.sleep(0.01)
        state["now"] -= 1
        return httpx.Response(200, json=ok_body("x"))

    specs = [SourceSpec(str(i), f"/source/{i}") for i in range(8)]
    async with client_for(handler) as c:
        await fetch_all_sources(c, specs, policy=policy, max_concurrent_requests=3)
    assert state["peak"] == 3


async def test_backoff_sleep_does_not_hold_a_concurrency_slot(policy, sleeper):
    """With a limit of 1, a source that is backing off must not block the other source."""
    order = []

    def handler(request):
        letter = request.url.path[-1]
        order.append(letter)
        return httpx.Response(429) if letter == "a" and order.count("a") == 1 else httpx.Response(200, json=ok_body(letter))

    async with client_for(handler) as c:
        res = await fetch_all_sources(c, SPECS[:2], policy=policy, max_concurrent_requests=1)
    assert all(r.status == "ok" for r in res)
