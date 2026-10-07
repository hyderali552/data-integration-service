# Reflection

## Approximate time spent
**Approximately 3 days: ~1 day for design and mock-source setup, ~1 day for implementation and resiliency features, and ~1 day for testing, debugging, and documentation.**

## What was completed
All eight parts of the brief:
- **Mocks (Part 1):** one FastAPI app serving sources A/B/C and the downstream, with 10 named failure scenarios (429, 5xx, slow/timeout, TCP connection failure, malformed body, pagination loop, ...), curated edge-case data, a CSV with valid/duplicate/invalid/edge rows, and a deterministic 10,000-record generator. Runs in-process or as a real uvicorn server.
- **Integration (Part 2):** paginated, retried, concurrent fetching with partial-failure isolation; one shared validator for API and CSV; deterministic consolidation (latest `updated_at` ▸ A>B>C>FILE ▸ content).
- **Database (Part 3):** transactional, batched, idempotent SQLite upsert whose `WHERE` clause enforces the same ordering as the consolidator; delivery-state table; PostgreSQL adaptation documented.
- **Downstream (Part 4):** deterministic idempotency keys, bounded retries, transient/permanent classification, `unconfirmed` state persisted and retried next run.
- **Concurrency & scale (Part 5):** asyncio with global/per-run limits and deadlines; generator-based page iterators; documented path to 10-20 M records/source.
- **Failure scenarios (Part 6):** each listed scenario has an executable test.
- **Tests (Part 7):** 205 tests, 99 % coverage, fully offline, no real sleeping.
- **Docs (Part 8):** README with diagram, setup, decisions (9), assumptions/limitations, scaling, mocking strategy.

## Assumptions made
See README §13. Main ones: `id` is the only identity (case-sensitive, trimmed); naive timestamps are UTC; "valid timestamp" means ISO-8601, ≥ 1970 and ≤ now + 1 day; any non-empty `status` is accepted; the downstream honours `Idempotency-Key`; "selected records" = every record version not yet confirmed downstream.

## What was intentionally simplified
- All records of a run are held in memory (fine for ~10 k; the streaming path is designed but not built).
- No checkpoint/resume inside a source; no background reconciler for `unconfirmed` deliveries; no circuit breaker; no metrics/structured logging; per-record POSTs; SQLite instead of PostgreSQL; no auth/TLS.

## What I would improve for production
1. PostgreSQL + connection pool + migrations; `COPY`→staging→set-based upsert.
2. Streaming pipeline with bounded queues between stages and a per-source checkpoint table (resumable at-least-once).
3. Cursor/keyset pagination and partitioned parallel fetching for very large sources.
4. Outbox-style delivery workers (`FOR UPDATE SKIP LOCKED`), batch endpoint, rate limiting, reconciliation via key lookup, alerting on aged `unconfirmed` rows.
5. Circuit breakers, retry budgets and jitter on by default; adaptive concurrency on 429.
6. Metrics, tracing and structured logs with a run id; CI with coverage gate plus a contract test against the real downstream; load test at 10 M+ scale.
7. Secrets management, TLS/mTLS, least-privilege DB role.

## Notes on this submission
- While building, the test suite exposed (and I fixed) real robustness gaps: a non-dict record crashing validation, JSON strings with lone surrogates crashing hashing, and DB timestamp comparison being text-based across timezone offsets.
- httpx's stock `ASGITransport` ignores timeouts, so I wrote `InProcessTransport` to enforce them; otherwise in-process timeout tests would silently pass without testing anything.
