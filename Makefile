.PHONY: help venv install env db-reset load-fixtures run dev test check planner export-plan clean

SHELL := /bin/bash

VENV ?= .venv
PYTHON ?= python3
PY := $(VENV)/bin/python
PIP := $(PY) -m pip

HOST ?= 127.0.0.1
PORT ?= 8000

PLAN_ID ?= 1
EXPORT_DIR ?= data/exports

help:
	@echo "Targets:"
	@echo "  make venv           Create local venv in $(VENV)"
	@echo "  make install        Install Python deps into venv"
	@echo "  make env            Create .env from .env.example if missing"
	@echo "  make db-reset       Reset SQLite DB at DATABASE_URL (default ./data/investor.db)"
	@echo "  make load-fixtures  Load sample CSVs from fixtures/"
	@echo "  make dev            Run FastAPI UI (reload) on http://$(HOST):$(PORT)"
	@echo "  make run            Run FastAPI UI (no reload) on http://$(HOST):$(PORT)"
	@echo "  make planner        Run planner (rebalance BOTH) and save as DRAFT"
	@echo "  make export-plan    Export plan $(PLAN_ID) to $(EXPORT_DIR)"
	@echo "  make test           Run unit tests"
	@echo "  make check          Compile + tests"
	@echo "  make clean          Remove venv + caches (keeps DB)"
	@echo ""
	@echo "Notes:"
	@echo "  - Override PYTHON if needed (recommended: python3.11 or python3.12):"
	@echo "      make venv PYTHON=python3.12"

$(PY):
	$(PYTHON) -m venv $(VENV)

venv: $(PY)

install: venv
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt

env:
	@test -f .env || cp .env.example .env

db-reset: install env
	$(PY) scripts/reset_db.py

load-fixtures: db-reset
	$(PY) -m src.cli import-csv --kind securities --path fixtures/securities.csv
	$(PY) -m src.cli import-csv --kind lots --path fixtures/lots.csv
	$(PY) -m src.cli import-csv --kind cash_balances --path fixtures/cash_balances.csv
	$(PY) -m src.cli import-csv --kind income_events --path fixtures/income_events.csv
	$(PY) -m src.cli import-csv --kind transactions --path fixtures/transactions.csv

dev: install env
	$(PY) -m uvicorn src.app.main:app --reload --host $(HOST) --port $(PORT)

run: install env
	$(PY) -m uvicorn src.app.main:app --host $(HOST) --port $(PORT)

planner: install env
	$(PY) -m src.cli run-planner --goal rebalance --scope BOTH --save

export-plan: install env
	$(PY) -m src.cli export-plan --plan-id $(PLAN_ID) --out $(EXPORT_DIR)

test: install
	$(PY) -m pytest -q

check: install
	$(PY) -m compileall -q src tests scripts
	$(PY) -m pytest -q

clean:
	rm -rf $(VENV) .pytest_cache .ruff_cache .mypy_cache __pycache__

