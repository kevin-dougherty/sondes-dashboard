.PHONY: bootstrap ingest fmt lint

bootstrap:
python -m src.bootstrap_por

ingest:
python -m src.ingest_y2d

fmt:
black src

lint:
ruff check src
