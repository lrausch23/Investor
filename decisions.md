# Investor — Decisions (stable)

This file captures “do not re-litigate” decisions and the rationale behind them.

## Holdings

### Aggregate holdings by `(account_id, symbol)`
- **Decision**: combined holdings must show separate rows for the same symbol across different brokerage accounts.
- **Why**: symbol-only aggregation misattributes positions, can double-count basis, and breaks trust/auditability when users want “which account owns what”.
- **Where**: `src/core/external_holdings.py` (`build_holdings_view`).

## Chase offline connector

### Exclude internal sweep mechanics from external cashflows
- **Decision**: rows like `DEPOSIT SWEEP` / `INTRA-DAY DEPOSIT/WITHDRWAL`, sweep tickers (e.g., `QCERQ`), and typical sweep codes (`DBS`/`WDL`) are treated as internal mechanics (`OTHER`) and are **not** counted as contributions/withdrawals.
- **Why**: Chase exports include internal cash↔MMF sweep activity that inflates flows and breaks reconciliation/user trust.
- **Constraint**: keep this exclusion even when the `Tran Code` column is missing (use conservative heuristics on description/ticker/type).
- **Where**: `src/adapters/chase_offline/adapter.py` (`_classify_txn`).

### Model Chase cash as liquidity, not a holdings row
- **Decision**: cash-like lines in positions exports are summed into `CashBalance`; holdings rows should represent investable securities.
- **Why**: keeping sweep vehicles as holdings clutters the table and breaks “available liquidity” semantics; Chase positions can include multiple cash lines (including negative “US DOLLAR” during settlement).
- **Where**: `src/adapters/chase_offline/adapter.py` (`fetch_holdings`).

### Delimiter detection must be robust to commas in numeric values
- **Decision**: if `csv.Sniffer` fails or mis-detects, select delimiter by frequency (e.g., favor tab for TSV).
- **Why**: Chase TSV exports often contain commas inside numeric fields; mis-detection leads to all-zero parsing.
- **Where**: `src/adapters/chase_offline/adapter.py` (`_sniff_delimiter`).

### Classify “Reinvest” conservatively
- **Decision**: “Reinvest” is treated as `OTHER`, not `INT`/`DIV`/`TRANSFER`.
- **Why**: avoids double counting where interest/dividend rows appear alongside reinvestment mechanics; safer to under-classify than distort income/flows.
- **Where**: `src/adapters/chase_offline/adapter.py` (`_classify_txn`).

## Environment

### Tests run under `.venv`
- **Decision**: use `.venv/bin/pytest` for local testing.
- **Why**: system Python 3.13 may break SQLAlchemy imports due to typing changes; venv pins compatible deps.

