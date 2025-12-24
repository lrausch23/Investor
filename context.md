# Investor — Project Context (rolling)

Investor is a local-first, audit-focused MVP for bucketized portfolio + tax planning.

## What exists today

- Stack: FastAPI + Jinja/HTMX + SQLite + SQLAlchemy; pytest.
- Goal: correct, explainable holdings + cash + cashflow metrics across accounts/taxpayers, with idempotent imports and minimal user confusion.
- Connectors / ingestion:
  - `IB_FLEX_OFFLINE`: local Flex exports (no network).
  - `IB_FLEX_WEB`: on-demand manual refresh via IB Flex Web Service (network-gated).
  - `CHASE_OFFLINE`: Chase IRA offline CSV/TSV exports (no network).
- UI: read-only Holdings page + cashflow summaries + broker-derived reporting (e.g., IB closed lots / wash rows).

## Current state

- Holdings attribution aggregates by `(account_id, symbol)` (combined scope shows separate rows per account).
- Chase offline ingest:
  - Positions: robust delimiter sniffing; supports TSV exports with commas in numeric values.
  - Cash: excludes cash-like tickers from holdings rows and sums them into `CashBalance` (sweep vehicles like `QCERQ`, “US DOLLAR”); includes `CASH:USD` in holdings snapshot payload as a fallback.
  - Transactions classification: internal sweep mechanics are excluded from external flows; “Reinvest” is treated conservatively to avoid double counting.
- Tests exist and should be run with the venv interpreter: `.venv/bin/pytest` (system Python 3.13 can break SQLAlchemy due to typing changes).

## In progress

- Reconcile app cashflow metrics with Chase UI “Performance” definitions for the user’s selected timeframe (likely definition/timeframe/export mismatches).

## Open questions / risks

- Chase “Performance” contributions/withdrawals definitions may exclude certain rows (timeframe boundaries, pending settlement, banklink/ACH semantics).
- Without a dedicated Chase “Transfers/Contributions/Distributions” export, external transfers can be ambiguous in generic activity exports.

## Immediate next steps

- Ask for the Chase UI timeframe + the exact export(s) used to produce the “Performance” screen values.
- Add a read-only “cashflow diagnostics” section on the connection detail page (counts/sums by `Transaction.type`, top descriptions for transfers).
- If mismatch persists, refine transfer classification to prefer deterministic fields (`Tran Code` / descriptions) while preserving sweep exclusions.

