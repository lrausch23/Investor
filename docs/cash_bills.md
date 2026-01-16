# Cash & Bills

## User guide

**Cash & Bills** is a high-level dashboard for short‑term liquidity planning. It combines:
- credit card statements (liabilities)
- checking balances (cash available)
- recurring monthly bills (checking)
- recurring monthly card charges (subscriptions)

### Key sections

**KPI row**
- **Cash available**: total across checking accounts in the selected scope.
- **Card balances due**: statement balances due within the selected range.
- **Net after bills**: cash minus bills due (credit cards + monthly bills).
- **Next bill due**: nearest due date and amount.

**Card bills**
- Shows statement balances, due dates, and last payments.
- Statuses are merged into: **Overdue**, **Due soon**, **Paid**, **Unknown**.

**Monthly card charges**
- Recurring charges detected on credit cards.
- Use **Manage charges** to review suggested recurring charges or edit active ones.

**Monthly bills (checking)**
- Recurring ACH/debit outflows detected from checking.
- Use **Manage bills** to review suggested bills or edit active ones.

**Checking accounts**
- Available/current balances per checking account.

**Bill coverage**
- Shows due totals for the selected range and net cash after bills.
- Includes a **Projected recurring outflows** block so paid items still show up if the next cycle falls inside the range.

**Finance charges (monthly)**
- Aggregates plan/finance fees on credit cards.
- If a month has multiple charges, select **View** to see the transactions.

### Range and status filters
- Range chips (7/14/30/60) apply to card bills, monthly bills, and coverage totals.
- Status filter applies to both card bills and monthly bills.

### Manage bills / charges
Each modal has three tabs:
- **Suggested**: auto‑detected candidates (add or ignore).
- **Active**: confirmed recurring items (edit or deactivate).
- **Recent (30d)**: recent transactions to discover new recurring items.

## Technical notes

### Data sources
- **Card bills**: Plaid liabilities (statement balances, due dates, last payment).
- **Checking balances**: Plaid accounts.
- **Checking transactions**: Plaid transactions (used for recurring bill detection).
- **Card transactions**: Plaid transactions (used for recurring card charges).

### Tables
- `recurring_bill`, `recurring_bill_rule`, `recurring_bill_ignore`
- `recurring_card_charge`, `recurring_card_charge_rule`, `recurring_card_charge_ignore`

### Detection heuristics (MVP)
Recurring candidates are detected from 6–12 months of checking or card activity:
- Debits only (outflows)
- Grouped by `plaid_merchant_id` when available, otherwise normalized merchant name
- Monthly cadence: occurrences spaced ~28–33 days
- Amount classification:
  - **FIXED**: low variance
  - **RANGE**: moderate variance
  - **VARIABLE**: higher variance

### Status logic
For a given cycle:
- **Paid**: payment exists in the cycle month
- **Overdue**: due date passed without payment
- **Due soon**: due within 7 days
- **Unknown**: due day not set and cannot be inferred

### Projected recurring outflows
Projected totals are computed for the selected range even if the current cycle is paid:
- If a due day exists, the next occurrence within the range is included.
- If due day is unknown, the item is excluded from projection totals.
- Amounts use expected values:
  - **FIXED**: `amount_expected`
  - **RANGE**: `amount_max` (conservative)
  - **VARIABLE**: last payment/charge amount

### Endpoints
These endpoints back the dashboard and modals:
- `GET /cash-bills`
- `GET /api/cash-bills/recurring/summary`
- `GET /api/cash-bills/recurring/suggestions`
- `POST /api/cash-bills/recurring/activate`
- `POST /api/cash-bills/recurring/ignore`
- `PATCH /api/cash-bills/recurring/{bill_id}`
- `GET /api/cash-bills/recurring/recent`
- `GET /api/cash-bills/card-recurring/summary`
- `GET /api/cash-bills/card-recurring/suggestions`
- `POST /api/cash-bills/card-recurring/activate`
- `POST /api/cash-bills/card-recurring/ignore`
- `PATCH /api/cash-bills/card-recurring/{charge_id}`
- `GET /api/cash-bills/card-recurring/recent`
- `GET /api/cash-bills/card-finance`
- `GET /api/cash-bills/card-finance/transactions`

### Notes
- Unknown due dates appear in tables but are excluded from **due** and **projected** totals.
- Range chips are the single source of truth for table filtering and coverage totals.
