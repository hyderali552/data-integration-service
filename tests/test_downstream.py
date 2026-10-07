"""Downstream integration: success, timeout, retry, permanent failure, idempotency, duplicates."""
import asyncio
from datetime import datetime

import httpx
import pytest

from src.downstream.client import Outcome, build_payload, send_all, send_record
from src.mock_servers.scenarios import Scenario
from src.mock_servers.transport import make_client
from src.mock_servers.world import create_app
from src.models import ValidRecord, idempotency_key_for
from tests.conftest import client_for


def vr(id_="1", ts="2026-01-01T00:00:00+00:00", name="n"):
    return ValidRecord(id_, name, "active", datetime.fromisoformat(ts), "A")


async def test_successful_request_sends_payload_and_idempotency_key(policy):
    seen = {}

    def handler(request):
        seen["key"] = request.headers.get("Idempotency-Key")
        seen["path"] = request.url.path
        seen["json"] = request.read().decode()
        return httpx.Response(200, json={"status": "accepted"})

    async with client_for(handler) as c:
        out = await send_record(c, vr(), policy=policy)
    assert out.outcome == Outcome.CONFIRMED and out.attempts == 1
    assert seen["path"] == "/processed" and seen["key"] == idempotency_key_for(vr())
    assert '"id":"1"' in seen["json"].replace(" ", "")


def test_idempotency_key_is_deterministic_and_version_sensitive():
    assert idempotency_key_for(vr()) == idempotency_key_for(vr())
    assert idempotency_key_for(vr()) != idempotency_key_for(vr(ts="2026-01-02T00:00:00+00:00"))
    assert idempotency_key_for(vr()) != idempotency_key_for(vr(name="other"))
    assert idempotency_key_for(vr()) != idempotency_key_for(vr(id_="2"))
    assert build_payload(vr())["updated_at"] == "2026-01-01T00:00:00.000000Z"


async def test_timeout_is_retried_with_the_same_idempotency_key(policy, sleeper):
    keys = []

    def handler(request):
        keys.append(request.headers["Idempotency-Key"])
        if len(keys) == 1:
            raise httpx.ReadTimeout("slow", request=request)
        return httpx.Response(200, json={"status": "already_processed"})

    async with client_for(handler) as c:
        out = await send_record(c, vr(), policy=policy)
    assert out.outcome == Outcome.CONFIRMED and out.attempts == 2
    assert len(keys) == 2 and keys[0] == keys[1]
    assert sleeper.delays == [1.0]


async def test_retries_exhausted_is_unconfirmed_not_failed_or_raised(policy, sleeper):
    calls = []

    def handler(request):
        calls.append(1)
        raise httpx.ReadTimeout("slow", request=request)

    async with client_for(handler) as c:
        out = await send_record(c, vr(), policy=policy)
    assert out.outcome == Outcome.UNCONFIRMED and out.attempts == 3 and len(calls) == 3
    assert out.error == "ReadTimeout" and sleeper.delays == [1.0, 2.0]


@pytest.mark.parametrize("status", [429, 500, 502, 503, 408])
async def test_transient_http_errors_are_retried(policy, status):
    calls = []

    def handler(request):
        calls.append(1)
        return httpx.Response(status, headers={"Retry-After": "1"}) if len(calls) < 3 else httpx.Response(201)

    async with client_for(handler) as c:
        out = await send_record(c, vr(), policy=policy)
    assert out.outcome == Outcome.CONFIRMED and len(calls) == 3


async def test_retry_after_is_honoured_for_downstream_429(policy, sleeper):
    calls = []

    def handler(request):
        calls.append(1)
        return httpx.Response(429, headers={"Retry-After": "3"}) if len(calls) == 1 else httpx.Response(200)

    async with client_for(handler) as c:
        await send_record(c, vr(), policy=policy)
    assert sleeper.delays == [3.0]


@pytest.mark.parametrize("status", [400, 401, 403, 404, 409, 422])
async def test_permanent_failures_are_not_retried(policy, status, sleeper):
    calls = []

    def handler(request):
        calls.append(1)
        return httpx.Response(status)

    async with client_for(handler) as c:
        out = await send_record(c, vr(), policy=policy)
    assert out.outcome == Outcome.REJECTED and len(calls) == 1 and sleeper.delays == []
    assert out.error == f"HTTP {status}"


async def test_connection_error_is_retried(policy):
    calls = []

    def handler(request):
        calls.append(1)
        raise httpx.ConnectError("refused", request=request)

    async with client_for(handler) as c:
        out = await send_record(c, vr(), policy=policy)
    assert out.outcome == Outcome.UNCONFIRMED and len(calls) == 3 and out.error == "ConnectError"


async def test_one_failing_record_does_not_affect_the_others(policy):
    def handler(request):
        import json
        rid = json.loads(request.read())["id"]
        return httpx.Response(400) if rid == "bad" else httpx.Response(200)

    async with client_for(handler) as c:
        outs = await send_all(c, [vr("a"), vr("bad"), vr("b")], policy=policy)
    assert [o.outcome for o in outs] == [Outcome.CONFIRMED, Outcome.REJECTED, Outcome.CONFIRMED]
    assert [o.record_id for o in outs] == ["a", "bad", "b"]       # order preserved


async def test_send_all_respects_concurrency_limit(policy):
    state = {"now": 0, "peak": 0}

    async def handler(request):
        state["now"] += 1
        state["peak"] = max(state["peak"], state["now"])
        await asyncio.sleep(0.005)
        state["now"] -= 1
        return httpx.Response(200)

    async with client_for(handler) as c:
        outs = await send_all(c, [vr(str(i)) for i in range(30)], policy=policy, concurrency=4)
    assert all(o.outcome == Outcome.CONFIRMED for o in outs)
    assert state["peak"] <= 4 and state["peak"] > 1


async def test_unexpected_exception_is_contained_per_record(policy):
    def handler(request):
        raise RuntimeError("bug")

    async with client_for(handler) as c:
        outs = await send_all(c, [vr("a"), vr("b")], policy=policy)
    assert [o.outcome for o in outs] == [Outcome.UNCONFIRMED] * 2 and "RuntimeError" in outs[0].error


# ---- against the real mock downstream (timeout-but-processed scenario) ------
async def test_timeout_after_server_accepted_does_not_double_process(policy):
    """First attempt: server applies the effect then answers too slowly (client times out).
    Retry with the same key: server recognises it. Exactly one side effect."""
    app = create_app(Scenario(downstream="timeout_once", downstream_delay=5))
    async with make_client(app) as c:
        out = await send_record(c, vr(), policy=policy, timeout=0.05)
    world = app.state.world
    assert out.outcome == Outcome.CONFIRMED and out.attempts == 2
    assert world.stats()["downstream_requests"] == 2
    assert len(world.effects) == 1
    assert world.stats()["duplicate_requests_absorbed"] == 1


async def test_sending_the_same_record_twice_is_idempotent(policy):
    app = create_app(Scenario())
    async with make_client(app) as c:
        o1 = await send_record(c, vr(), policy=policy)
        o2 = await send_record(c, vr(), policy=policy)
    assert o1.outcome == o2.outcome == Outcome.CONFIRMED
    assert len(app.state.world.effects) == 1 and app.state.world.stats()["downstream_requests"] == 2


async def test_a_new_version_is_a_new_effect(policy):
    app = create_app(Scenario())
    async with make_client(app) as c:
        await send_record(c, vr(), policy=policy)
        await send_record(c, vr(ts="2026-02-01T00:00:00+00:00"), policy=policy)
    assert len(app.state.world.effects) == 2


async def test_mock_downstream_without_key_is_rejected(policy):
    app = create_app(Scenario())
    async with make_client(app) as c:
        r = await c.post("/processed", json={"id": "1"})
    assert r.status_code == 400
