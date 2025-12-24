# Investor — Assumptions (explicit)

## Product

- The app is **local-first**; planning-grade outputs are acceptable as long as limitations are clear and audit trails are strong.
- Holdings + cash + cashflow summaries must be explainable, conservative, and avoid “surprising” classification (prefer under-classifying to misclassifying).

## Data sources / connector behavior

- Imports must be **idempotent**; re-running sync should not double-import the same file/rows.
- Chase offline exports are noisy:
  - Many “cash movement” rows are internal sweep mechanics, not external contributions/withdrawals.
  - Some exports omit helpful columns (e.g., `Tran Code`); heuristics must tolerate missing fields.
- Chase positions exports may represent cash in multiple lines (MMF sweep vehicle + “US DOLLAR”, sometimes negative during settlement).

## Security / operations

- No secrets are logged; DB-stored secrets are encrypted at rest.
- Network access may be restricted; live connectors must be gated (e.g., by `NETWORK_ENABLED` and outbound allowlists).

## Development

- Run tests with the venv interpreter (`.venv/bin/pytest`), not system Python 3.13.

