# Benchmarks (Daily Candle Cache)

Investor reports (e.g., `Reports → Performance`) can compare portfolio performance to a benchmark like `VOO`, `SPY`, or `QQQ`.

To make reports reliable (and usable offline), Investor uses a **cache-first** benchmark candle pipeline:

1. **SQLite cache** (authoritative, offline)
2. **Stooq** (preferred network source for daily candles)
3. **Yahoo Finance** (optional fallback; disabled by default due to HTTP 429s)

## Data Contract

The benchmark pipeline normalizes candles into a canonical schema:

- Index: timezone-naive daily `datetime` (date at midnight)
- Columns (float):
  - `open`, `high`, `low`, `close`, `adj_close` (optional), `volume` (optional)
- At minimum `close` is required.

## Symbol Normalization

- `^GSPC` is mapped to a proxy (default `SPY`) so you can request “S&P 500” without an index feed.
- The cache key is the **canonical** symbol after mapping.

Configure the proxy in `benchmarks.yaml`:

```yaml
benchmarks:
  benchmark_proxy: SPY
```

## Configuration (`benchmarks.yaml`)

Create `benchmarks.yaml` in the repo root (or `~/.bucketmgr/benchmarks.yaml`):

```yaml
benchmarks:
  provider_order: [cache, stooq]
  cache:
    type: sqlite
    path: data/benchmarks/benchmarks.sqlite
  stooq:
    enabled: true
  yahoo:
    enabled: false
    max_rps: 1
    max_retries: 6
    backoff_base_seconds: 2
```

Notes:
- `provider_order` is cache-first; cache is always authoritative.
- Yahoo is intentionally slow and conservative to avoid rate limits.

## CLI

Warm the cache so reports can run offline:

```bash
python -m src.cli benchmarks warm --symbols VOO,SPY,QQQ --start 2000-01-01
```

Inspect cache coverage:

```bash
python -m src.cli benchmarks status --symbols VOO,SPY,QQQ
```

## Network Safety Controls

Investor may block outbound hosts unless allowlisted.

To warm via Stooq/Yahoo, you may need to allowlist:
- `stooq.com`
- `query1.finance.yahoo.com` (fallback only)

Set:

```bash
export NETWORK_ENABLED=1
export ALLOWED_OUTBOUND_HOSTS="stooq.com,query1.finance.yahoo.com"
```

Or (unsafe) disable the allowlist:

```bash
export DISABLE_OUTBOUND_HOST_ALLOWLIST=1
```
