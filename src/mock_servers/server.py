"""Run the mock world as a real HTTP server.

    python -m src.mock_servers.server --scenario demo --port 8000
    python -m src.main --base-url http://127.0.0.1:8000

Try it by hand:  curl "http://127.0.0.1:8000/source/a?page=1"
"""
from __future__ import annotations

import argparse

import uvicorn

from src.mock_servers.data import generate_bulk
from src.mock_servers.scenarios import PRESETS
from src.mock_servers.world import create_app


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--scenario", choices=sorted(PRESETS), default="happy")
    p.add_argument("--bulk", type=int, default=0, help="serve N unique generated records instead of the curated set")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8000)
    args = p.parse_args()
    app = create_app(PRESETS[args.scenario], generate_bulk(args.bulk) if args.bulk else None)
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()
