# Momentum Screener (YTD + Trend Confirmation)

The Momentum Screener helps you identify sector and stock leadership, focusing on **Year‑to‑Date (YTD)** performance while filtering for **uptrend confirmation**.

This is designed for *directional* insight and idea generation — it is not investment advice.

## What It Shows

### Momentum Dashboard (`/momentum`)
- **Sector leaderboard** (ranked by YTD return)
  - Sector return is **equal‑weight average** of member stock returns (MVP).
  - Breadth is **% of stocks above SMA200** (MVP).
  - “Top leaders” lists the top 3 stocks by YTD return within the sector.
- **Stock leaderboard** (ranked by YTD return)
  - Includes trend confirmation columns and liquidity filter.
  - Click a sector name to drill into a **Sector Detail** page.

### Sector Detail (`/momentum/sector/<Sector>`)
- Shows a **relative performance chart** (“growth of $1”) for:
  - equal‑weight sector index (MVP)
  - benchmark (default `SPY`)
- Shows the sector’s member stocks table and allows adding the top N to Watchlist.

## Methodology (Deterministic)

### Prices
- Uses **adjusted close** when available; otherwise close.
- Daily prices are cached in the app DB (`price_daily`) so that reports work offline once warmed.

**Price providers**
- Default: **Stooq** (daily OHLCV).
- Optional: **Finnhub** (daily candles; requires `FINNHUB_API_KEY` + network enabled).
- You can switch providers from the Momentum UI (“Price source”).

### Returns
- **YTD return**
  - `YTD = last_close / close(last trading day of prior year) - 1`
- **1M / 3M returns**
  - Uses trading-day lookbacks:
    - 1M ≈ 21 observations
    - 3M ≈ 63 observations

### Trend Confirmation
Computed from daily prices:
- `SMA200`: 200‑day simple moving average
- `SMA50`: 50‑day simple moving average
- `SMA50_slope_20d`: change in SMA50 over the last ~20 trading observations

**Uptrend (MVP definition)**
- `close > SMA200`
- `SMA50 > SMA200`
- `SMA50_slope_20d > 0`

### Liquidity Filter (MVP)
When enabled:
- Requires `Avg$Vol20d ≥ $10,000,000`
- `Avg$Vol20d` is computed as `mean(close * volume)` over the last ~20 trading observations.

## Data Setup (Sectors + Universes)

### Market data (prices)
Daily prices are fetched from **Stooq** (network) and stored in the app DB (`price_daily`). Once warmed, Momentum works offline.

### Sector mapping
Sector leadership requires `ticker → sector` classification. The Momentum page includes a CSV importer:
- Upload a CSV with columns:
  - `ticker` (required)
  - `sector` (recommended)
  - `industry` (optional)

### Universes (SP500 / Nasdaq100)
To use SP500 or Nasdaq100 universes, you have two options:

**Option A (no CSV): fetch constituents from Stooq**
- In Momentum → “Data (universes & sectors)”, use “Load constituents from Stooq”.
- This loads tickers only; sector/industry may remain **Unknown** unless you also import a mapping CSV.

**Option B: upload a universe CSV (recommended for sector views)**
Upload a universe CSV in the Momentum “Data” panel:
- Choose universe (`SP500` or `NASDAQ100`)
- Upload CSV with `ticker` + optional `sector`/`industry`

This populates:
- `universe_membership`
- `ticker_classification` (when sector/industry provided)

## Interpretation Tips
- **Momentum (return)** and **trend (uptrend confirmation)** complement each other:
  - High YTD with *no* uptrend confirmation may indicate a fading move.
  - Strong breadth within a sector often signals “healthy” leadership.
- Always validate with your own risk constraints, diversification, and tax planning.
