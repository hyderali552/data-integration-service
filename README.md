# Resilient Multi-Source Data Integration Service

## Introduction

This project is a Python service that collects records from three REST APIs and a CSV file. It validates the records, combines duplicate data, saves the final records in SQLite, and sends them to a downstream API.

The service is designed to keep working when an API fails or when some records are invalid. The APIs and downstream service are mocked locally, so the project runs fully offline.

## How it works

1. Fetch records from sources A, B, C, and a CSV file.
2. Validate every record using shared validation rules.
3. Save invalid records in a rejected-records file.
4. Choose one final version for each record ID.
5. Save the result safely in SQLite.
6. Send new or changed records to the downstream API.

```text
API A ──┐
API B ──┼──> Fetch ──> Validate ──> Combine ──> SQLite ──> Downstream API
API C ──┤                    │
CSV   ──┘                    └──> rejected_records.jsonl
```

## Main features

- Reads from three paginated REST APIs and a CSV file.
- Uses `asyncio` to fetch the API sources at the same time.
- Retries temporary problems such as timeouts, connection errors, `429`, and `5xx` responses.
- Uses one shared validation function for API and CSV data.
- Logs invalid records instead of stopping the whole program.
- Uses clear rules to handle duplicate and conflicting records.
- Uses SQLite transactions and upserts, making repeated runs safe.
- Uses idempotency keys to avoid duplicate downstream effects after timeouts.
- Includes local mock services and automated tests.

## Setup

Use Python 3.11 or newer.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

On Windows, activate the environment with:

```bash
.venv\Scripts\activate
```

The database is created automatically at `data/records.db`.

## Running the project

Run the normal demonstration:

```bash
python -m src.main
```

Run a successful scenario:

```bash
python -m src.main --scenario happy
```

Run with 10,000 generated records:

```bash
python -m src.main --bulk 10000
```

See all available options:

```bash
python -m src.main --help
```

## Testing

Run every test:

```bash
pytest
```

Run the faster test set without the large-record test:

```bash
pytest -m "not slow"
```

The project has 205 automated tests. Saved test and coverage results are in `docs/test-results.txt` and `docs/coverage.txt`.

## Validation and data rules

A valid record must contain an `id`, `name`, `status`, and `updated_at`.

- Missing or blank required fields make a record invalid.
- Timestamps must be valid ISO-8601 dates.
- Dates before 1970 and dates more than one day in the future are rejected.
- Extra fields are ignored.
- A bad record does not stop good records from being processed.

Invalid records are written to `data/rejected_records.jsonl`. The file records the source, location, original data, and validation reason.

If more than one source has the same ID, these rules choose the final record:

1. The newest `updated_at` timestamp wins.
2. If timestamps match, source A wins over B, B wins over C, and C wins over the CSV file.
3. If there is still a tie, record content is used as a final consistent tie-breaker.

This makes the final result independent of the order in which records arrive.

## Error handling

| Problem | What happens |
|---|---|
| One API is unavailable | The other APIs and the CSV file still run. |
| An API fails after some pages | Pages already received are kept. |
| Rate limit (`429`) | The request is retried with a limited delay. |
| Timeout or connection issue | The request is retried a fixed number of times. |
| Bad JSON or response shape | That source fails, but other sources continue. |
| Bad record | It is logged and other records continue. |
| Repeated page number | A pagination guard stops an infinite loop. |
| Database issue | The run stops before anything is sent downstream. |

Exit code `0` means success. Exit code `2` means a degraded run, where some data was still processed. Exit code `1` means a database failure.

## Database and downstream delivery

SQLite stores:

- `records`: the newest valid version of each record.
- `delivery`: the downstream delivery status for each record version.

The database uses transactions and upserts. Duplicate data does not create duplicate rows, and old data cannot overwrite newer data. Running the same input again does not change already-correct records.

After saving records, the program sends them to `POST /processed`. Each record version has an idempotency key. If a timeout happens after the downstream API may have received a record, the program retries with the same key. This helps prevent duplicate effects.

A delivery can be:

- `confirmed`: downstream accepted it.
- `unconfirmed`: retries ended but the final result is unknown; it is retried on the next run.
- `rejected`: downstream sent a permanent client error, so that version is not retried.

## Concurrency and mock services

The project uses `asyncio` and `httpx.AsyncClient` because network work is mostly waiting for responses. A semaphore limits the number of requests running at one time.

The included FastAPI mock server can simulate slow responses, failed pages, rate limits, bad data, connection failures, and pagination loops.

To run it over real HTTP:

```bash
# Terminal 1
python -m src.mock_servers.server --scenario demo --port 8000

# Terminal 2
python -m src.main --base-url http://127.0.0.1:8000 --reset-db
```

Available scenarios include `happy`, `demo`, `source-a-down`, `source-a-503`, `slow-source`, `flaky`, `downstream-timeout`, `downstream-down`, `downstream-reject`, and `pagination-loop`.

## Scaling idea

This version is designed for about 10,000 records. For 10–20 million records per source, I would process data one page at a time, use queues between stages, use cursor-based pagination, save source checkpoints, move to PostgreSQL with bulk loading, use separate delivery workers, and add monitoring and alerts.

## Project structure

```text
src/
  fetchers/       API and CSV readers
  processing/     Validation and consolidation logic
  db/             SQLite database code
  downstream/     Downstream API client
  mock_servers/   Mock APIs and failure scenarios
  main.py         Command-line entry point
  pipeline.py     Main pipeline flow
tests/            Automated tests
data/             CSV input and local database files
docs/             Test and coverage results
```

## Assumptions and limitations

- Record IDs are case-sensitive.
- A timestamp without a timezone is treated as UTC.
- The downstream API supports idempotency keys.
- This version uses SQLite and sends one downstream request per record.
- Authentication and TLS are not included because all services are mocked locally.
- Failed downstream deliveries are retried during the next application run, not by a background worker.

## Reflection

See [REFLECTION.md](REFLECTION.md) for the project reflection and possible improvements.
