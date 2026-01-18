# Investor — Bucketized Portfolio Manager (MVP)

Local-first Python app (FastAPI + SQLite) to manage a bucketized portfolio across multiple accounts and taxpayers, generate **tax-aware** transition/rebalancing plans, and maintain strong **audit trails**.

This is an MVP: manual entry + CSV upload, plus adapter-based ingestion for offline exports (and optional on-demand refresh for select connectors when networking is enabled).

## Project memory

- `context.md`: rolling project state, open questions, next steps
- `decisions.md`: stable “do not re-litigate” decisions + rationale
- `assumptions.md`: explicit product/data/environment assumptions
- `Architecture_Guide.md`: architecture guidance (“source of truth”)

## Feature docs

- `docs/benchmarks.md` — Benchmark price cache (cache-first; Stooq default)
- `docs/cash_bills.md` — Cash & Bills dashboard (credit card bills + monthly recurring outflows)
- `docs/expenses.md` — Expense analysis (CSV imports, categorization, recurring reports)
- `docs/plaid_chase.md` — Plaid connections (Chase/AMEX sync, liabilities snapshots)
- `docs/rj_qfx.md` — Raymond James QFX/OFX (“Quicken Downloads”) imports

## Quickstart

Recommended Python: **3.11 or 3.12** (some dependencies may lag on 3.13).

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
uvicorn src.app.main:app --reload
```

App DB is created at `data/investor.db`.

## Monthly Performance Report (standalone)

A standalone, local reporting pipeline lives in `portfolio_report/README.md` and runs via:

```bash
python -m portfolio_report --help
```

### Market data (Yahoo Finance cache)

The monthly report can optionally fetch and cache missing price history using `yfinance`:

- Cache directory: `data/prices/yfinance/` (one file per ticker)
- Optional deps: `pandas`, `yfinance` (and `pyarrow` for Parquet)

If these deps aren't installed, the report still runs using any existing local CSVs in `data/prices/`.

### Makefile shortcuts

```bash
make install
make load-fixtures
make dev
```

## Security / Access

- MVP supports optional HTTP Basic auth via `APP_PASSWORD` in `.env`.
- If `APP_PASSWORD` is **unset**, the UI runs without auth and shows a warning banner. Run locally only.

## Defaults (chosen for MVP)

- **Taxpayers**:
  - `Trust` (`type=TRUST`) for taxable accounts (IB + RJ)
  - `Personal` (`type=PERSONAL`) for IRA (Chase IRA)
- **Wash sale scope**: within a single `TaxpayerEntity` only.
  - Trust wash checks only Trust accounts.
  - IRA is treated as *Personal* and does **not** create Trust wash sales.
- **“Substantially identical” (MVP)**: same `ticker` OR same `substitute_group_id`.
- **Prices**:
  - MVP uses `Security.metadata_json.last_price` for market value + trade estimates.
  - If missing, planner marks warnings and uses `last_price=1.0` as a placeholder.
- **Multi-account / multi-taxpayer planning**:
  - MVP does **not** model transfers or cross-taxpayer funding (e.g. selling in Trust to buy in IRA).
  - When scope is `BOTH`, the planner runs **independently per taxpayer entity** and aggregates results.
- **Qualified dividends**: assumed **0%** unless user edits assumptions.

## Dashboard Scope

The Dashboard supports a scope selector (query param + dropdown):
- `?scope=household` (default)
- `?scope=trust`
- `?scope=personal`

Scope affects: bucket drift, tax, fees, allocation breakdown, ST exposure, and wash-risk watchlist.

### Partial dataset warning
If the selected-scope total value is below `PARTIAL_DATASET_THRESHOLD` (default `$100,000`), the dashboard shows:
“This appears to be a partial dataset; metrics reflect imported subset only.”

Set in `.env`:
```bash
PARTIAL_DATASET_THRESHOLD=100000
```

## Core Concepts

### Buckets
- `B1` Liquidity (cash/MMF, 18–24 months)
- `B2` Defensive / Income
- `B3` Growth
- `B4` Alpha / Opportunistic

Bucket allocation is computed using `BucketAssignment` (ticker → bucket code) for the active policy. Cash is treated as `B1`.

If a ticker has no explicit `BucketAssignment`, the MVP uses a **fallback inference** from `Security.asset_class` (and records a warning). Set explicit assignments for auditability and to remove ambiguity.

### Policy Governance
- Policies are versioned (`BucketPolicy` with `effective_date`).
- Each bucket has `min/target/max` percent and allowed asset classes.
- Planner **will not** recommend policy-violating trades unless user explicitly overrides and provides a reason; violations are recorded in `Plan.outputs_json.warnings` and `AuditLog`.

### Lots & Holding Period
- Taxable positions are tracked at **lot** level (`PositionLot`).
- Holding period: **LT** if `sale_date - acquisition_date >= 365 days`, else **ST**.

### Audit Trail
Every UI/CLI mutation writes an `AuditLog` row:
- timestamp, actor, action, entity, entity_id
- `old_json`, `new_json`
- optional note/reason

Every generated trade plan is saved as an **immutable** `Plan` record:
- inputs (goal, scope, assumptions, snapshot totals)
- outputs (proposed trades, lot picks, tax estimates, drift, warnings)

## CSV Import (MVP)

Use the UI “Imports” page or the CLI.

Sample files live in `fixtures/`.

Supported imports:
- `lots.csv`: lot-level holdings (taxable)
- `cash_balances.csv`
- `income_events.csv`
- `transactions.csv` (optional; improves wash-risk and YTD realized reporting)

### Transactions convention (important)
- `Transaction.amount` is a **signed cashflow**:
  - `BUY`: usually negative (cash out)
  - `SELL`: usually positive (cash in / proceeds)
- For `SELL`, realized gains reporting and wash-loss watchlist are most accurate when you provide:
  - `lot_basis_total` (basis allocated to the sold shares)
  - optional `lot_acquisition_date` and/or `term` (`ST`/`LT`)

### How to enter lots
- Enter one row per tax lot with:
  - `account_name`, `ticker`, `acquisition_date`, `qty`, `basis_total`
  - optional `adjusted_basis_total`

## Wash Sale Detection (MVP)

When a plan includes a **loss sale**, wash checks look for:
- any **executed BUY** transactions within ±30 days (same taxpayer scope), and
- any **proposed BUY** trades in the same plan within ±30 days.

Substantially identical = same ticker OR same substitute group.

The engine flags:
- `definite`: a matching buy exists
- `possible`: missing mappings/unknown group

Recommended mitigations:
- delay sale beyond the window
- reduce sale size
- swap into a different substitute group (if configured)

## Trade Planner (MVP)

Goals supported:
- Rebalance to policy targets
- Raise cash for B1 (enter amount)
- Reduce Alpha risk (cap B4 at max)
- Harvest losses (target loss $X or “maximize safe losses”)

### Lot selection (default: tax-minimizing Specific-ID)
For a SELL quantity `Q`, choose lots in order:
1. Loss lots (most negative unrealized first), only if wash-safe (or flagged with mitigation)
2. LT gain lots (lower rate)
3. LT flat lots
4. ST lots (avoided; planner flags and requires override to *finalize* if ST gains are realized)

Alternative methods: FIFO/LIFO are available in the UI.

## Tax Engine (MVP)

User-editable assumptions (per run):
- `ordinary_rate`, `ltcg_rate`, `state_rate`
- NIIT toggle + rate (`niit_enabled`, `niit_rate`)
- `qualified_dividend_pct` (global)

Estimated tax per taxpayer:
```
tax = ordinary_rate*(ST_gains + interest + nonqualified_divs)
    + ltcg_rate*(LT_gains + qualified_divs)
    + state_rate*(ST_gains + LT_gains + dividends + interest)
    + NIIT_proxy (if enabled)
```

Limitations:
- No true QDI detection per security (global % only).
- No AMT, phaseouts, capital loss carryforwards, or bracket modeling.
- Dashboard “Estimated tax (YTD)” is federal-only (ordinary + LTCG + NIIT proxy) and is planning-grade.

### Tax assumptions
Edit rates at `/tax/assumptions` (stored in SQLite, audited).

## Sync (IB via Yodlee) — FULL vs INCREMENTAL (MVP)

This iteration adds selectable sync modes and a provider-agnostic sync runner. The shipped adapter is **fixtures-only** (`YodleeIBFixtureAdapter`) so you can exercise the workflow without network credentials.

The Sync UI shows:
- the latest run status + counts (`fetched`, `new`, `dupes`, `parse_fails`, `pages`)
- a read-only preview of the **latest holdings snapshot** and **recent imported transactions** per connection (helps confirm “data downloaded” even when a run is idempotent and inserts 0 new rows).

DB note: the app runs lightweight SQLite schema upgrades on startup (adds missing columns when possible). If you hit a schema mismatch, you can still hard-reset with `python scripts/reset_db.py`.

## Sync (IB Flex Offline files) — “Real data” without network

If your environment is network-restricted (or you want to validate the sync runner on real brokerage exports), the codebase supports an **IB Flex (Offline files)** connector that reads local `.csv`/`.xml` exports.

## Expense Analysis (NEW)

Local-first expense ingestion and analysis (credit card + bank statements) lives under `src/investor/expenses/`.

- Docs: `docs/expenses.md`
- Web UI: `/expenses`

Note: to avoid double-counting from multiple feeds, the default UI only allows creating `IB_FLEX_WEB`, `CHASE_OFFLINE`, and `RJ_OFFLINE` connections. Existing/legacy connections can still be viewed/disabled and continue to work.

Notes / limitations:
- This connector does not call IB/Yodlee APIs; it only reads local files.
- For offline file ingest, the sync runner uses date ranges only for display/coverage; transactions are ingested from selected files regardless of transaction date.
- CSV parsing is “best-effort” with flexible column mappings; if your export format differs, normalize columns to: `account,date,type,symbol,qty,amount,description,provider_transaction_id`.
- Holdings snapshots come from the newest `*positions*` / `*holdings*` file in the directory when present.
- Cash balances: when a positions export includes a cash section, the sync runner imports **USD** cash into `CashBalance` (non-USD is ignored with a warning; MVP does not model FX cash).

## Sync (Chase IRA Offline CSV) — local files (no network)

Use this for a tax-deferred IRA account (positions + cash + activity), imported from local Chase/JPM CSV exports:

- Create connection: `Sync → Connections` → Connector = `Chase (Offline CSV)`
- Upload your Chase CSV(s) on the connection detail page, or drop them into the data directory and run sync.
- `FULL`: imports all rows in the selected files; `INCREMENTAL`: ingests only new transaction files (tracked by file hash).

Recommended exports (to avoid misclassified cashflows):
- **Positions / Holdings snapshot** (required for the Holdings page): include `Ticker` (or `Symbol`), `Quantity`, `Value`, and `As of`; include `Cost` / `Orig Cost (Base)` when available to populate “Initial cost”.
- **Account Activity / Transactions**: include `Trade Date` (or `Date`), `Type`, `Description`, `Amount USD`, and ideally `Ticker`; `Tran Code` improves classification.
- Chase IRA exports commonly include **internal sweep activity** (cash ↔ money-market sweep, often `DBS`/`WDL` and “DEPOSIT SWEEP”). The app treats these as internal mechanics (`OTHER`), not external contributions/withdrawals.
- If your Chase UI provides a dedicated **Transfers / Contributions / Distributions** export, include it; it is typically the most reliable source for external cash movements.

IRA behavior:
- Included in holdings / bucket allocation / portfolio value.
- Excluded from reconstructed wash-sale logic and taxable gain calculations (but withholding is still shown if present).

## Sync (Raymond James Offline CSV) — local files (no network)

Use this for a taxable RJ account imported from local CSV/TSV exports:

- Create connection: `Sync → Connections` → Connector = `Raymond James (Offline CSV)`
- Upload your RJ export(s) on the connection detail page, or drop them into the data directory and run sync.

Recommended export columns (the parser is best-effort):
- **Holdings / Positions snapshot**: `Symbol`/`Ticker`, `Quantity`/`Shares`, `Market Value`/`Value` (+ `Cost Basis` if available)
- **Activity / Transactions**: `Date`, `Description`, `Amount`/`Net Amount`, `Symbol`/`Ticker` (+ `Quantity` when applicable)
- **Realized P&L / Closed lots**: `Opening Date`, `Opening Amount`, `Closing Date`, `Closing Amount`, `Realized Gain/(Loss)$` (symbol is extracted from `Description (Symbol/CUSIP)` like `... (AMD)`)

## Sync (IB Flex Web Service) — live manual refresh (network)

This connector lets you run `Sync → Connections → Run` without uploading local files by calling the **IB Flex Web Service** endpoints.

Important:
- Flex Web Service is **IP restricted**. Ensure the machine running this app is allowlisted in IB Portal.
- Secrets are stored **encrypted at rest** in SQLite using `APP_SECRET_KEY`.
- Secrets are **never logged**; outbound requests are restricted by an allowlist.

### Enable networking (safe-by-default)

By default, live connectors are disabled. To enable:

```bash
export NETWORK_ENABLED=1
```

Outbound hosts are allowlisted. Default:
- `ndcdyn.interactivebrokers.com`
- `gdcdyn.interactivebrokers.com` (some environments)
- `www.interactivebrokers.com`

Override (comma-separated) if needed:

```bash
export ALLOWED_OUTBOUND_HOSTS=ndcdyn.interactivebrokers.com,www.interactivebrokers.com
```

Entries can be hostnames (recommended) or full `https://...` URLs; only the hostname is used.

Disable outbound host allowlist (unsafe; not recommended):

```bash
export DISABLE_OUTBOUND_HOST_ALLOWLIST=1
```

Optional (advanced) base URL override:

```bash
export IB_FLEX_WEB_BASE_URL=https://ndcdyn.interactivebrokers.com/Universal/servlet/
```

If you see `HTTP error status=404` during sync, your IB environment may host the Flex servlet on a different domain.
You can provide multiple candidates (tried in order):

```bash
export IB_FLEX_WEB_BASE_URLS=https://ndcdyn.interactivebrokers.com/Universal/servlet/,https://gdcdyn.interactivebrokers.com/Universal/servlet/,https://www.interactivebrokers.com/Universal/servlet/
```

### Create a connection

- Go to `/sync/connections`
- Connector: `IB Flex (Web Service)`
- Enter:
  - Token (Flex Web Service token)
  - Flex Query (use the **Flex Query name** from IB Portal Reports → Flex Queries; numeric query ids are optional if available)
  - Optional: Extra Flex Queries (comma-separated) if you have separate Flex queries for trades/positions/cash.

The adapter resolves query names to query ids by calling `FlexStatementService.GetUserInfo` for your token.

Click **Test** to verify a report can be requested and downloaded.

### FULL vs INCREMENTAL

- `FULL`: uses your selected start/end dates.
- `INCREMENTAL`: uses `last_successful_txn_end - overlap_days` (default overlap=7) through today.

Idempotency:
- Each downloaded report is hashed; if the same payload is returned again, the run skips importing it (no duplicate holdings snapshots / cash rows).

## Reconstructed Tax Lots (from IB Flex transactions)

IB does not provide bulk tax-lot exports via the Client Portal UI. This MVP can **reconstruct planning-grade lots** deterministically from the full imported transaction history:

- Rebuild action: `Sync → Connection → Rebuild Lots`
- Views:
  - Open lots: `/taxlots`
  - Realized gains (ST/LT): `/taxlots/gains`
  - Wash sale adjustments: `/taxlots/wash-sales`

Defaults / limitations (important):
- Source is always `RECONSTRUCTED` (planning). Not authoritative tax reporting.
- Disposal method: FIFO.

## Broker-Based Realized Gains (IB Trades “CLOSED_LOT” / “WASH_SALE”)

If your IB Flex Trades export includes a `LevelOfDetail` column (common values: `EXECUTION`, `CLOSED_LOT`, `WASH_SALE`, `SYMBOL_SUMMARY`), the offline connector will ingest:
- `EXECUTION` rows → `Transaction` (used for positions, planner, wash checks, etc.)
- `CLOSED_LOT` rows → broker-reported realized gain components (preferred for realized gains reporting)
- `WASH_SALE` rows → broker-reported wash-sale events (informational; preserves broker fields)

Pages:
- Realized gains: `/taxlots/gains?source=auto` (defaults to broker `CLOSED_LOT` when present), or force with `source=broker`.
- Broker wash rows: `/taxlots/wash-sales-broker`

Notes:
- `proceeds` is derived as `CostBasis + FifoPnlRealized` when IB omits proceeds on `CLOSED_LOT` / `WASH_SALE` rows.
- These rows are still “planning-grade” inside this MVP, but typically closer to broker reporting than reconstructed FIFO from executions alone.
- Basis comes from transaction cashflow (commissions assumed included). Transfers with unknown basis are flagged.
- Wash sales are computed from executed BUYs within ±30 days. Replacement buys in IRA are flagged (loss treatment not fully modeled).
- Corporate actions: you can record splits via `CorporateActionEvent` (manual for now); if details are missing, lots may need review.

### Troubleshooting: “Why do I see duplicate/OTHER transactions?”

The main `Transactions` page (`/holdings/transactions`) shows **manual** + **imported** transactions from **all** connections by default.

If you accidentally created multiple sync connections pointing at the **same export directory**, you will see duplicates (often including older “OTHER” rows imported before the classifier improvements).

Fix:
- Use the filters at `/holdings/transactions` to select a single `Connection`, or show `Imported only`.
- On the extra connection page, use **Danger Zone → Purge imported data** to delete that connection’s imported transactions/snapshots/runs (Accounts/Securities/Lots are not deleted).
- Re-run `FULL` with **Reprocess files** enabled to upgrade legacy imports in-place.

### Entering IB auth (Token + Flex Query)
Go to:
- `/sync/connections` → “Edit Auth”, or
- `/sync/connections/{id}/auth`

To store credentials in SQLite you must set `APP_SECRET_KEY` in `.env`:
```bash
APP_SECRET_KEY=change-this-to-a-long-random-string
```

Credentials are encrypted at rest using Fernet with a key derived from:
- `urlsafe_b64encode(sha256(APP_SECRET_KEY))`

UI always masks values (shows only the last 4).

### Local plaintext config (optional)
If present, the UI will show config-sourced connections and allow manual import:
- `./connectors.yaml` (repo root), or
- `~/.bucketmgr/connectors.yaml`

This file is plaintext and should be treated as sensitive.

Example `connectors.yaml`:
```yaml
connections:
  - name: "IB (Yodlee)"
    provider: "YODLEE"
    broker: "IB"
    taxpayer: "Trust"
    fixture_dir: "fixtures/yodlee_ib"
    token: "PASTE_TOKEN"
    query_id: "PASTE_QUERY_ID"
```

### Modes
- **INCREMENTAL** (default):
  - If `last_successful_sync_at` exists: start = `date(last_successful_sync_at) - overlap_days`
  - Else: start = `today - 90 days`
  - End = `today`
- **FULL**:
  - User-provided start/end (defaults: 10 years ago → today)
  - If provider rejects large ranges, the runner shrinks to: 10y → 5y → 3y → 2y → 1y → 180d → 90d
  - The “effective” range is stored on the `SyncRun`
  - Raw payload snapshots default **ON** for FULL (for audit/debug)

### Overlap days (why it exists)
INCREMENTAL uses an overlap window (default 7 days) to safely re-fetch recent history so late-posting/corrected transactions are not missed. Idempotency prevents duplicates.
Raw payload snapshots default **OFF** for INCREMENTAL.

### Idempotency / duplicates
Transactions are deduplicated via `ExternalTransactionMap(connection_id, provider_txn_id)`:
- Prefer provider stable transaction id
- Fallback: deterministic hash key `HASH:sha256(date|amount|type|symbol|description|account_id|qty)`

### Completeness gates
Runs are marked:
- `ERROR` if 0 accounts fetched
- `PARTIAL` if pagination did not exhaust or if parse failures occurred
- `SUCCESS` otherwise

### UI
- Connections: `/sync/connections`
- Connection detail + coverage: `/sync/connections/{id}`

## CLI

```bash
python -m src.cli import-csv --kind lots --path fixtures/lots.csv
python -m src.cli run-planner --goal rebalance --scope BOTH
python -m src.cli export-plan --plan-id 1 --out data/exports
```

## Tests

```bash
pytest -q
```

## Reset DB

```bash
python scripts/reset_db.py
```

## Repo Layout

- `src/app/` FastAPI UI (routes/templates)
- `src/core/` engines (policy/tax/wash/planner/fees)
- `src/db/` SQLAlchemy models + sessions + audit helpers
- `src/importers/` CSV import + Phase 2 adapter stubs
- `tests/` unit tests
- `fixtures/` sample CSVs + sample policy JSON
