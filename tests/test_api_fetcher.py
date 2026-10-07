"""API integration: retrieval, pagination, empty, 429, 5xx, timeout, connection failure, malformed."""
import asyncio

import httpx
import pytest

from src.fetchers.api_fetcher import SourceSpec, fetch_source
from tests.conftest import client_for, good

SPEC = SourceSpec("A", "/source/a")


def paged(pages):
    """Handler serving {page: body}; counts requests per page."""
    hits = []

    def handler(request):
        page = int(request.url.params["page"])
        hits.append(page)
        return httpx.Response(200, json=pages[page])
    handler.hits = hits
    return handler


async def run(handler, policy, **kw):
    async with client_for(handler) as client:
        return await fetch_source(client, SPEC, policy=policy, timeout=0.5, **kw)


async def test_successful_single_page(policy):
    res = await run(paged({1: {"records": [good("1"), good("2")], "next_page": None}}), policy)
    assert [r.payload["id"] for r in res.records] == ["1", "2"]
    assert res.status == "ok" and res.pages_fetched == 1 and res.errors == []
    assert {r.source for r in res.records} == {"A"}


async def test_multiple_pages_are_followed_in_order(policy):
    h = paged({1: {"records": [good("1")], "next_page": 2},
               2: {"records": [good("2")], "next_page": 3},
               3: {"records": [good("3")], "next_page": None}})
    res = await run(h, policy)
    assert [r.payload["id"] for r in res.records] == ["1", "2", "3"]
    assert h.hits == [1, 2, 3] and res.pages_fetched == 3


async def test_empty_response_is_ok_not_an_error(policy):
    res = await run(paged({1: {"records": [], "next_page": None}}), policy)
    assert res.records == [] and res.errors == [] and res.status == "ok"


async def test_empty_page_in_the_middle_does_not_stop_pagination(policy):
    h = paged({1: {"records": [good("1")], "next_page": 2}, 2: {"records": [], "next_page": 3},
               3: {"records": [good("3")], "next_page": None}})
    res = await run(h, policy)
    assert [r.payload["id"] for r in res.records] == ["1", "3"]


async def test_http_429_retries_and_honours_retry_after(policy, sleeper):
    calls = []

    def handler(request):
        calls.append(1)
        if len(calls) < 3:
            return httpx.Response(429, headers={"Retry-After": "2"})
        return httpx.Response(200, json={"records": [good("1")], "next_page": None})

    res = await run(handler, policy)
    assert len(res.records) == 1 and res.errors == []
    assert sleeper.delays == [2.0, 2.0]          # server-provided delay used


async def test_retry_after_is_capped(policy, sleeper):
    calls = []

    def handler(request):
        calls.append(1)
        return httpx.Response(429, headers={"Retry-After": "9999"}) if len(calls) == 1 else \
            httpx.Response(200, json={"records": [], "next_page": None})

    await run(handler, policy)
    assert sleeper.delays == [5.0]               # policy.max_retry_after


async def test_http_429_forever_gives_up_after_max_attempts(policy, sleeper):
    calls = []

    def handler(request):
        calls.append(1)
        return httpx.Response(429)

    res = await run(handler, policy)
    assert len(calls) == 3 and res.status == "failed"
    assert "gave up after 3 attempts (HTTP 429)" in res.errors[0]
    assert sleeper.delays == [1.0, 2.0]          # exponential backoff, no sleep after the last try


@pytest.mark.parametrize("status", [500, 502, 503, 504])
async def test_http_5xx_is_retried_then_succeeds(policy, status):
    calls = []

    def handler(request):
        calls.append(1)
        return httpx.Response(status) if len(calls) == 1 else \
            httpx.Response(200, json={"records": [good("1")], "next_page": None})

    res = await run(handler, policy)
    assert len(res.records) == 1 and res.errors == [] and len(calls) == 2


async def test_http_5xx_forever_is_a_failed_source_not_an_exception(policy):
    res = await run(lambda r: httpx.Response(503), policy)
    assert res.status == "failed" and res.records == []


async def test_5xx_on_later_page_keeps_earlier_pages(policy):
    def handler(request):
        if request.url.params["page"] == "1":
            return httpx.Response(200, json={"records": [good("1"), good("2")], "next_page": 2})
        return httpx.Response(500)

    res = await run(handler, policy)
    assert [r.payload["id"] for r in res.records] == ["1", "2"]
    assert res.status == "partial" and res.pages_fetched == 1


async def test_timeout_is_retried_then_succeeds(policy):
    calls = []

    def handler(request):
        calls.append(1)
        if len(calls) == 1:
            raise httpx.ReadTimeout("slow", request=request)
        return httpx.Response(200, json={"records": [good("1")], "next_page": None})

    res = await run(handler, policy)
    assert len(res.records) == 1 and len(calls) == 2


async def test_timeout_forever_is_reported(policy):
    def handler(request):
        raise httpx.ReadTimeout("slow", request=request)

    res = await run(handler, policy)
    assert res.status == "failed" and "ReadTimeout" in res.errors[0]


async def test_connection_failure_is_retried_and_reported(policy):
    calls = []

    def handler(request):
        calls.append(1)
        raise httpx.ConnectError("refused", request=request)

    res = await run(handler, policy)
    assert len(calls) == 3 and res.status == "failed" and "ConnectError" in res.errors[0]


async def test_malformed_json_body_is_not_retried(policy):
    calls = []

    def handler(request):
        calls.append(1)
        return httpx.Response(200, content=b"{not json")

    res = await run(handler, policy)
    assert len(calls) == 1 and "malformed JSON" in res.errors[0]


@pytest.mark.parametrize("body", [{"unexpected": "shape"}, {"records": "nope"}, [1, 2, 3], "text"])
async def test_wrong_shape_is_reported(policy, body):
    res = await run(lambda r: httpx.Response(200, json=body), policy)
    assert res.status == "failed" and "malformed response" in res.errors[0]


async def test_permanent_4xx_is_not_retried(policy, sleeper):
    calls = []

    def handler(request):
        calls.append(1)
        return httpx.Response(404)

    res = await run(handler, policy)
    assert len(calls) == 1 and sleeper.delays == [] and "permanent failure HTTP 404" in res.errors[0]


async def test_pagination_loop_terminates(policy):
    """page 1 -> next 2, page 2 -> next 2 (the scenario from the brief)."""
    h = paged({1: {"records": [good("1")], "next_page": 2}, 2: {"records": [good("2")], "next_page": 2}})
    res = await run(h, policy)
    assert h.hits == [1, 2]                      # page 2 fetched once, never again
    assert any("pagination loop" in e for e in res.errors)
    assert [r.payload["id"] for r in res.records] == ["1", "2"]


async def test_max_pages_ceiling(policy):
    def handler(request):
        p = int(request.url.params["page"])
        return httpx.Response(200, json={"records": [], "next_page": p + 1})   # endless, never repeats

    res = await run(handler, policy, max_pages=5)
    assert res.pages_fetched == 5 and "max_pages" in res.errors[-1]


@pytest.mark.parametrize("bad", [0, -1, "2", True, 1.5])
async def test_invalid_next_page_value_stops_pagination(policy, bad):
    res = await run(paged({1: {"records": [good("1")], "next_page": bad}}), policy)
    assert len(res.records) == 1 and "invalid next_page" in res.errors[0]


async def test_non_object_records_are_passed_through_for_validation_to_reject(policy):
    res = await run(paged({1: {"records": [good("1"), "junk", None, 42], "next_page": None}}), policy)
    assert len(res.records) == 4 and res.records[1].payload == "junk"


async def test_source_deadline_keeps_partial_data(policy):
    async def handler(request):
        if request.url.params["page"] == "1":
            return httpx.Response(200, json={"records": [good("1")], "next_page": 2})
        await asyncio.sleep(1)
        return httpx.Response(200, json={"records": [], "next_page": None})

    res = await run(handler, policy, deadline=0.05)
    assert [r.payload["id"] for r in res.records] == ["1"]
    assert "deadline" in res.errors[0]


async def test_unexpected_exception_is_contained(policy):
    def handler(request):
        raise RuntimeError("boom")

    res = await run(handler, policy)
    assert res.status == "failed" and "unexpected RuntimeError" in res.errors[0]
