"""Command-line entry point.

    python -m src.main                         # in-process mocks, 'demo' failure scenario
    python -m src.main --scenario happy
    python -m src.main --bulk 10000            # 10k-record performance run
    python -m src.main --base-url http://127.0.0.1:8000   # against `python -m src.mock_servers.server`

Exit codes: 0 = clean, 1 = fatal (e.g. database), 2 = degraded (a source failed
or downstream deliveries are unconfirmed/rejected; healthy data was still processed).
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import tempfile

import httpx

from src.config import Settings, configure_logging
from src.mock_servers.data import generate_bulk, write_csv
from src.mock_servers.scenarios import PRESETS
from src.mock_servers.transport import make_client
from src.mock_servers.world import create_app
from src.pipeline import run_pipeline


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Resilient multi-source data integration service",
                                formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__)
    p.add_argument("--scenario", choices=sorted(PRESETS), default="demo", help="in-process mock failure scenario (default: demo)")
    p.add_argument("--base-url", help="use real HTTP against this base URL instead of in-process mocks")
    p.add_argument("--csv", help="CSV input path (default: data/sample_input.csv)")
    p.add_argument("--db", help="SQLite path (default: data/records.db)")
    p.add_argument("--bulk", type=int, default=0, help="generate and process N unique records (scale demo)")
    p.add_argument("--reset-db", action="store_true", help="delete the database first (fresh start)")
    p.add_argument("--json", metavar="FILE", help="also write the run report as JSON")
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args(argv)


async def amain(args: argparse.Namespace) -> int:
    settings = Settings.from_env()
    if args.db:
        settings = settings.__class__(**{**settings.__dict__, "db_path": args.db})
    csv_path = args.csv or settings.csv_path
    if args.reset_db and os.path.exists(settings.db_path):
        for suffix in ("", "-wal", "-shm"):
            if os.path.exists(settings.db_path + suffix):
                os.remove(settings.db_path + suffix)
    os.makedirs(os.path.dirname(settings.db_path) or ".", exist_ok=True)

    dataset = None
    if args.bulk:
        dataset = generate_bulk(args.bulk)
        csv_path = os.path.join(tempfile.mkdtemp(prefix="dis-bulk-"), "bulk_input.csv")
        write_csv(csv_path, dataset.csv_rows)
        print(f"Generated bulk dataset: {args.bulk} unique ids")

    if args.base_url:
        client = httpx.AsyncClient(base_url=args.base_url)
    else:
        scenario = PRESETS["happy" if args.bulk and args.scenario == "demo" else args.scenario]
        client = make_client(create_app(scenario, dataset), scenario)
        print(f"In-process mock world, scenario: {args.scenario if not (args.bulk and args.scenario == 'demo') else 'happy'}")

    async with client:
        report = await run_pipeline(settings, client, client, csv_path=csv_path)

    print(report.format())
    if args.json:
        with open(args.json, "w", encoding="utf-8") as fh:
            json.dump(report.to_dict(), fh, indent=2, default=str)
    return report.exit_code


def main(argv=None) -> None:
    args = parse_args(argv)
    configure_logging(args.verbose)
    sys.exit(asyncio.run(amain(args)))


if __name__ == "__main__":
    main()
