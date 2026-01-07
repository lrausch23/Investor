## Expense Analysis (MVP)

Local-first expense ingestion + categorization + recurring detection for credit card and bank statements.

### Web UI

Run the app and open:

- `http://127.0.0.1:8000/expenses`

### Quickstart (Web)

1) Start the app: `make dev`
2) Open `http://127.0.0.1:8000/expenses`
3) Create starter rules at `/expenses/rules` (if missing)
4) Import CSV statement(s) from the Expenses page
5) Run “Categorize”, then view `/expenses/reports`, `/expenses/merchants`, and category drill-downs

### Canonical schema (SQLite)

Expense transactions are stored in `expense_transactions` with:

- `txn_id`: stable SHA-256 hash (used to avoid double imports)
- `expense_account_id`: links to `expense_accounts`
- `posted_date` / `transaction_date`
- `description_raw` + normalized fields (`description_norm`, `merchant_norm`)
- `amount` (canonical sign): **debit/charges negative**, **credits/payments positive**
- `category_system` (from rules) and `category_user` (manual override)

### Supported statement formats (initial)

Formats are auto-detected when possible (by header signature) and can be forced with `--format`.

- `chase_card_csv` (Chase credit card export-style headers)
- `amex_csv` (simple AMEX export-style headers)
- `apple_card_csv` (Apple Card export-style headers, includes “Purchased By”)
- `generic_bank_csv` (`Date,Description,Amount`)

To add a new provider format:

1) Add a new importer under `src/investor/expenses/importers/`.
2) Implement `detect(headers)` and `parse_rows(rows, default_currency)`.
3) Add it to `src/investor/expenses/importers/__init__.py`.
4) Add a synthetic fixture + tests.

### Categorization rules YAML

Default path: `expenses_rules.yaml` (repo root), configurable via `expenses.yaml`.

Rules are deterministic and applied in priority order (higher first).

Example:

```yaml
version: 1
rules:
  - name: Amazon shopping
    priority: 100
    category: Shopping
    match:
      merchant_exact: Amazon
  - name: Grocery heuristic
    priority: 10
    category: Groceries
    match:
      description_regex: "\\b(trader joe|kroger|safeway)\\b"
```

### Privacy

- Do not pass full account numbers to the CLI (`--account` should be a friendly label).
- `expense_accounts.last4_masked` stores last-4 only (optional).
- Raw row JSON is **not stored** by default; if enabled via `--store-original-rows`, it is redacted using regex patterns from config.

### Configuration (`expenses.yaml`)

Optional file locations:

- `./expenses.yaml`
- `~/.bucketmgr/expenses.yaml`

Example:

```yaml
expenses:
  default_currency: USD
  provider_formats: ["chase_card_csv", "amex_csv"]
  categorization:
    rules_path: expenses_rules.yaml
    budgets_monthly:
      Groceries: 800
      Dining: 300
  redaction:
    enabled: true
```

### Example workflow

- Import statements (monthly or annual) → categorize → review Unknowns in category detail → override categories → rerun reports.

### Recurring settings (per merchant)

In the merchant detail view (`/expenses/merchant?...`), you can mark a merchant as recurring and set a frequency (weekly/monthly/quarterly/semiannual/annual). These settings are stored in SQLite and used by the recurring report (`/expenses/recurring`) even if there are fewer than `min_months` occurrences (useful for annual payments).
