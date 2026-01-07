# Monthly Portfolio Performance Report

A local, deterministic pipeline that reads:

- `./transactions.csv`
- `./2025-Monthly-performance.csv` (or any similar monthly NAV + flow export)
- optional `./holdings.csv` (end-of-period snapshot)
- optional price cache `./data/prices/*.csv`

and produces:

- `./out/report_YYYY-MM.html`
- `./out/report_full_year_YYYY.html`
- `./out/analytics_daily.parquet` (if `pyarrow`/`pandas` available; otherwise `.csv`)
- `./out/analytics_monthly.parquet` (if available; otherwise `.csv`)
- `./out/position_guidance_YYYY-MM.csv`

This project intentionally favors simple, explainable methods and emits explicit warnings when data is insufficient.

## Install

This repo already includes `typer` and `jinja2`. For richer analytics outputs:

```bash
python -m pip install -r requirements.txt
# Optional (recommended for Parquet + faster ops):
python -m pip install pandas pyarrow
# Optional (only if you want automatic price downloads and have internet access):
python -m pip install yfinance
```

Price downloads are **off by default**; you can run fully offline using `./data/prices/*.csv`.

## Run

Full calendar year 2025 (default) and an as-of month-end report:

```bash
python -m portfolio_report \
  --transactions ./transactions.csv \
  --monthly ./2025-Monthly-performance.csv \
  --holdings ./holdings.csv \
  --out ./out \
  --start 2025-01-01 \
  --end 2025-12-31 \
  --asof 2025-12-31
```

Offline prices:

- Put one CSV per symbol under `./data/prices/`.
- Each file should have columns like `Date` and `Adj Close` (preferred) or `Close`.
  - Example: `./data/prices/SPY.csv`, `./data/prices/AAPL.csv`.

Optional price download (requires `yfinance` and internet access):

```bash
python -m portfolio_report ... --download-prices
```

## Methods (high level)

- **Monthly returns (TWR proxy)**: Modified Dietz per month using monthly NAV + net external flows, then chain-linked.
- **Money-weighted return (MWR)**: XIRR using dated external cashflows + begin/end NAV (flow timing from transactions when available; otherwise mid-month approximation).
- **Benchmark**: SPY total return proxy via `Adj Close` (or `Close` if needed).
- **Attribution**:
  - Position contribution ≈ average weight × return (best-effort, warns about residual “unexplained” return when holdings are incomplete).
  - Realized P&L via FIFO matching; flags “carry-in basis unknown” when sells have no matched buys in-period and no basis is provided (those fills are reported as proceeds-only with P&L omitted).
- **Guidance**: deterministic rules engine using contribution, concentration, volatility/drawdown, and correlation to benchmark.

## Data gaps & warnings

The report always lists data sufficiency warnings, e.g.:

- missing prices for some symbols (excludes them from attribution/risk stats),
- inferred flow timing (IRR differences vs platform),
- carry-in basis unknown for realized P&L,
- incomplete holdings reconstruction.
