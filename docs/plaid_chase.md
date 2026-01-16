# Plaid (Chase) — Investor Integration (Sync Connections)

Investor supports a first-class **Plaid (Chase)** connection in **Sync → Connections**.

This integration (MVP):
- Links via **Plaid Link** (OAuth-capable institutions like Chase)
- Stores the Plaid `access_token` encrypted in `data/investor.db` via `external_credentials`
- Syncs **bank/credit transactions into the Expenses system** (`expense_*` tables) to avoid polluting investment performance math
- Persists Plaid `/transactions/sync` cursor in `external_connections.metadata_json` for incremental runs
- Optional: can import **investment holdings snapshots** (requires enabling Investments in the connection settings + re-link)
- Optional: can ingest **investment transactions** (buys/sells/dividends/fees/transfers) into Investor’s `transactions` table for Performance (requires Investments enabled + re-link)

## Dependencies

Install:
- `plaid-python`
- `python-dotenv` (already used elsewhere)

## Environment variables

Create `.env` entries (or a separate file and `source` it):

- `PLAID_CLIENT_ID`
- `PLAID_SECRET`
- `PLAID_ENV` (`sandbox` default; use `production` for real Chase)
- `PLAID_REDIRECT_URI` (required for Chase OAuth in production; must match what you configured in Plaid Dashboard “OAuth redirect URIs”; recommended: an HTTPS tunnel URL like `https://<your-tunnel>/sync/plaid/oauth-return`)
 - `APP_SECRET_KEY` (required to store credentials encrypted in the Investor DB)
 - `NETWORK_ENABLED=1` (required for live network calls)

Optional:
 - `ALLOWED_OUTBOUND_HOSTS` (if set, must include the Plaid host for your env, e.g. `sandbox.plaid.com` or `production.plaid.com`)

## UI workflow

1) Go to `Sync → Connections`
2) Create a connection: `Chase (Plaid · Automated)`
3) Open the connection and click `Credentials`
4) Click `Connect / Re-link via Plaid` and complete Plaid Link
5) Run `Sync now`

### Enabling Chase Investments holdings

By default, the Plaid connection requests `transactions` only.
To sync Chase **investment holdings** into the Holdings page:

1) Open the connection → `Settings`
2) Enable `Investment holdings sync`
3) Go to `Credentials` and re-link via Plaid (this grants the `investments` product)
4) Run `Sync now`

## Replacing legacy Chase CSV/Yodlee data

If you previously imported Chase data via CSV files or the legacy Yodlee connector, and you want Plaid to be the only Chase source:

1) Open the `Chase (Plaid · Automated)` connection
2) Expand `Danger zone` → click `Purge legacy Chase (CSV/Yodlee)`
3) Re-run `Sync now` on the Plaid connection

This disables the legacy Chase connections and deletes their imported sync artifacts (transactions, holdings snapshots, runs).

### Chase re-auth (`ITEM_LOGIN_REQUIRED`)

If Plaid returns `ITEM_LOGIN_REQUIRED` (often every ~90 days), the sync run will fail cleanly with a message.
Re-link the connection from `Credentials` (Plaid Link) and run sync again.

## Legacy standalone scripts (kept for reference)

This repo still includes two standalone scripts (`setup_auth.py`, `daily_sync.py`) that sync to `investments.db`.
They are no longer the recommended path when using the Investor web app.
