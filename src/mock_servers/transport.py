"""In-process transport for the mock app.

httpx's stock `ASGITransport` ignores request timeouts and cannot simulate a
dropped connection, so a naive in-process mock would silently skip the most
important failure paths. This wrapper:

* enforces the request's read timeout (raising `httpx.ReadTimeout`), exactly as
  a real network client would, and
* raises `httpx.ConnectError` for sources listed as unreachable.

Result: retry/timeout code runs the identical path whether it talks to this
transport or to a real socket.
"""
from __future__ import annotations

import asyncio

import httpx

from src.mock_servers.scenarios import Scenario


class InProcessTransport(httpx.AsyncBaseTransport):
    def __init__(self, app, scenario: Scenario | None = None):
        self._inner = httpx.ASGITransport(app=app)
        self._unreachable = tuple(f"/source/{s}" for s in (scenario.unreachable_sources if scenario else ()))

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if any(request.url.path == p for p in self._unreachable):
            raise httpx.ConnectError("simulated connection failure", request=request)
        timeout = (request.extensions.get("timeout") or {}).get("read")
        try:
            return await asyncio.wait_for(self._inner.handle_async_request(request), timeout)
        except asyncio.TimeoutError:
            raise httpx.ReadTimeout("simulated read timeout", request=request) from None

    async def aclose(self) -> None:
        await self._inner.aclose()


def make_client(app, scenario: Scenario | None = None) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=InProcessTransport(app, scenario), base_url="http://mock")
