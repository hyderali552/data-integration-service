.PHONY: install test test-fast cov run run-happy mocks bulk lint clean

install:
	python -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt

test:
	pytest

test-fast:
	pytest -m "not slow"

cov:
	pytest --cov --cov-report=term-missing

run:            ## demo scenario: B dies after page 1, C always 429, downstream accepts-then-times-out
	python -m src.main --reset-db

run-happy:
	python -m src.main --scenario happy --reset-db

mocks:          ## run the mocks as a real HTTP server on :8000
	python -m src.mock_servers.server --scenario demo --port 8000

bulk:           ## 10,000-record scale demo
	python -m src.main --bulk 10000 --reset-db --db data/bulk.db

lint:
	python -m pyflakes src tests

clean:
	rm -rf .pytest_cache .coverage htmlcov data/*.db data/*.db-* data/rejected_records.jsonl
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
