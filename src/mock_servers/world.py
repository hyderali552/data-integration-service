"""The mock external world: three paginated sources + the downstream API.

    GET  /source/{a|b|c}?page=N      -> {"records": [...], "next_page": N+1 | null}
    POST /processed                  (requires Idempotency-Key header)
    GET  /_debug/stats               -> request / effect counters (for demos & tests)

One FastAPI app serves everything, so the same code runs in-process (tests,
default CLI) or under uvicorn (`python -m src.mock_servers.server`). All state
lives on `app.state.world`, never in module globals, so tests are isolated.
"""
from __future__ import annotations

import asyncio
from collections import Counter

from fastapi import FastAPI, Header, Query, Request
from fastapi.responses import JSONResponse, Response

from src.mock_servers.data import Dataset, curated_dataset
from src.mock_servers.scenarios import Scenario


class World:
    def __init__(self, scenario: Scenario, dataset: Dataset):
        self.scenario = scenario
        self.dataset = dataset
        self.source_hits: Counter = Counter()        # (source, page) -> requests received
        self.downstream_hits: Counter = Counter()    # idempotency key -> requests received
        self.effects: dict = {}                      # idempotency key -> payload (the REAL side effect, applied once)

    def stats(self) -> dict:
        return {
            "source_hits": {f"{s}:{p}": n for (s, p), n in sorted(self.source_hits.items())},
            "downstream_requests": sum(self.downstream_hits.values()),
            "downstream_unique_effects": len(self.effects),
            "duplicate_requests_absorbed": sum(n - 1 for n in self.downstream_hits.values() if n > 1),
        }


def create_app(scenario: Scenario | None = None, dataset: Dataset | None = None) -> FastAPI:
    world = World(scenario or Scenario(), dataset or curated_dataset())
    app = FastAPI(title="Mock external world")
    app.state.world = world

    @app.get("/source/{name}")
    async def source(name: str, page: int = Query(1, ge=1)):
        sc = world.scenario
        if name not in world.dataset.pages:
            return JSONResponse({"error": "unknown_source"}, status_code=404)
        world.source_hits[(name, page)] += 1
        hits = world.source_hits[(name, page)]
        mode = sc.sources.get(name, "normal")

        if mode == "down":
            return JSONResponse({"error": "service_unavailable"}, status_code=503)
        if mode == "always_429":
            return JSONResponse({"error": "rate_limited"}, status_code=429,
                                headers={"Retry-After": str(sc.retry_after_seconds)})
        if mode == "fail_after_page_1" and page >= 2:
            return JSONResponse({"error": "internal_server_error"}, status_code=500)
        if mode == "flaky" and hits <= sc.flaky_failures:
            return JSONResponse({"error": "temporary"}, status_code=500 if hits % 2 else 503)
        if mode == "slow":
            await asyncio.sleep(sc.slow_seconds)
        if mode == "malformed_json":
            return Response("{this is not json", media_type="application/json")
        if mode == "bad_shape":
            return JSONResponse({"unexpected": "shape"})
        if mode == "empty":
            return {"records": [], "next_page": None}
        if mode == "pagination_loop":
            if page == 1:
                return world.dataset.pages[name].get(1, {"records": [], "next_page": None}) | {"next_page": 2}
            return {"records": [], "next_page": 2}  # page 2 points at itself forever
        return world.dataset.pages[name].get(page, {"records": [], "next_page": None})

    @app.post("/processed")
    async def processed(request: Request, idempotency_key: str | None = Header(default=None)):
        sc = world.scenario
        if not idempotency_key:
            return JSONResponse({"error": "missing_idempotency_key"}, status_code=400)
        try:
            payload = await request.json()
        except ValueError:
            return JSONResponse({"error": "invalid_json"}, status_code=400)
        if not isinstance(payload, dict) or "id" not in payload:
            return JSONResponse({"error": "invalid_payload"}, status_code=400)

        world.downstream_hits[idempotency_key] += 1
        seen = world.downstream_hits[idempotency_key]
        mode = sc.downstream

        if mode == "down":
            return JSONResponse({"error": "unavailable"}, status_code=503)
        if mode == "reject_400":
            return JSONResponse({"error": "rejected"}, status_code=400)
        if mode == "flaky_503" and seen <= sc.flaky_failures:
            return JSONResponse({"error": "temporary"}, status_code=503, headers={"Retry-After": "0"})

        # Idempotent processing: the side effect happens at most once per key.
        duplicate = idempotency_key in world.effects
        if not duplicate:
            world.effects[idempotency_key] = payload

        # "Processed, but the response is too slow": the effect above is already
        # applied when the client gives up - the ambiguous-timeout scenario.
        if mode == "always_timeout" or (mode == "timeout_once" and seen == 1):
            await asyncio.sleep(sc.downstream_delay)
        return {"status": "already_processed" if duplicate else "accepted", "idempotency_key": idempotency_key}

    @app.get("/_debug/stats")
    async def debug_stats():
        return world.stats()

    return app
