# Basket Construction Study (BCS) — Specification

Status: Draft v1 — research-only. Depends on the Sharadar ingestion layer (survivorship-free PIT data).
Not investment advice. No production defaults.

## Objective

Test whether a **concentrated, factor-tilted, point-in-time-reconstituted basket** beats the current
static survivorship-biased basket and a passive index — **after tax, out-of-sample, on survivorship-free
data** — and identify which selection rule (if any) is robustly best. The study interrogates the *engine*
(name selection) rather than an overlay.

Two questions it must answer with evidence:
1. How much of the current basket's apparent edge over SPY is **survivorship/selection bias** (quantified
   by running the same liquidity screen point-in-time vs static)?
2. Does any selection rule beat both the static basket **and** a passive index after tax, OOS — or is the
   honest answer "just hold the index"?

## Data & universe (the load-bearing part)

- Source: **Sharadar SF1 (fundamentals) + SEP (prices) + DAILY (market cap) + TICKERS/ACTIONS**, via the
  ingestion layer. Survivorship-free; **keyed on `permaticker`**, never ticker.
- Eligibility computed **as-of each reconstitution date** (no look-ahead): US common stock, dollar-ADV >=
  $10M, market cap above a floor, minimum listing history.
- **Delisted names are selectable at the time they qualified**, and their full return path — including the
  delisting outcome from `ACTIONS` — flows through. A name that later failed must be eligible then.

## Selection rules (arms)

```yaml
study: basket_construction
version: 1
basket_sizes: [10, 12, 15]          # sensitivity
reconstitution: annual               # see mechanic below
weighting: equal_weight_at_entry     # let winners run; sensitivity = equal_weight_rebalanced
momentum_formation: 12_1             # 12-month return skipping most recent month; sensitivity = 6_1

arms:
  C0_static_basket:                  # CONTROL: current 30-name static liquidity screen (survivorship-biased)
    selection: existing_static
  C0b_static_pit:                    # same liquidity screen but POINT-IN-TIME -> isolates the bias delta
    selection: liquidity_topN_pit
  A1_pure_momentum:
    selection: rank(momentum_12_1) -> top N
  A2_quality_momentum:
    selection: rank( z(momentum_12_1) + z(quality) ) -> top N
    quality: [roic, fcf_margin, gross_profitability, low_leverage]   # from SF1 PIT
  A3_momentum_valuation_cap:
    selection: rank(momentum_12_1) over names EXCLUDING the most expensive valuation decile
    valuation: [ev_ebitda, p_fcf, p_e]                               # from SF1/DAILY PIT; exclude top decile
  A4_quality_momentum_valuation:     # all three
    selection: rank( z(momentum_12_1)+z(quality) ) over non-expensive-decile -> top N
  benchmarks: [static_basket_equal_weight, SPY_buy_hold, QQQ_buy_hold, L1_vol_target]
```

## Reconstitution mechanic (encodes the proposed rule)

Once per year, on a fixed calendar date, using only as-of data:
1. Re-rank the eligible universe by the arm's selection score.
2. **Drop the bottom third** of current holdings by that score (the genuine laggards).
3. **Replace them with the top-ranked names not already held**, back to the target size.
4. **Let winners run:** a current holding that is *not* in the bottom third is retained and **not trimmed**
   to an equal-weight target — its weight is allowed to drift up.
5. Dropped names are sold tax-aware: realize harvested losses first; names dropped at a significant gain
   are flagged (the turnover/tax cost is part of what the study measures).

Sensitivity variant: `full_reselect` (rebuild the whole basket from the top N each year) vs the
drop-bottom-third mechanic above — to see whether partial rotation (lower turnover) beats full rotation.

## Tax treatment

Reuse the existing FIFO tax-lot engine (32% ST / 20% LT, wash-sale aware, after-tax terminal wealth).
Annual reconstitution drops laggards (often losses -> harvested) and holds winners (gains deferred);
significant gainers are held to long-term. After-tax terminal wealth is the headline metric — pre-tax is
reported but does not decide.

## Validation discipline (must pass before any verdict)

- **Point-in-time:** selection at each date uses only `datekey`/`date <= as_of`. Momentum from prices up to
  as-of (skip most recent month). Fundamentals = SF1 as-reported, `datekey <= as_of`. No look-ahead.
- **Survivorship-free:** universe includes delisted names; their returns (and delisting outcomes) flow
  through. Selection from a frozen, hindsight-built list is forbidden.
- **Out-of-sample:** walk-forward or a held-out final window; rules pre-registered; no peeking.
- **After tax, after costs/slippage.** Stamp `data_snapshot_hash` and the readiness label on every run.
- **Fail-closed:** quality/valuation signals return `UNAVAILABLE` (run cannot certify; readiness
  downgrades) if SF1 PIT data is missing for a name/date — never fall back to current fundamentals.

## Headline outputs

- Per arm: after-tax terminal wealth, CAGR, max DD, Ulcer, Calmar, Sharpe, annual turnover, per-name
  return distribution (skew / win-rate), reconstitution churn.
- **Survivorship-bias delta:** `C0_static_basket` minus `C0b_static_pit` — quantifies how much the old
  results were inflated by hindsight. (Expected: large.)
- Each arm minus the static basket and minus the index, after tax.

## Pre-registered verdict bar

- A selection rule **wins** only if it beats **both** the static basket **and** the passive index
  (QQQ/SPY) on after-tax terminal wealth **and** risk-adjusted return (Calmar/Ulcer), out-of-sample, with
  drawdown inside tolerance.
- **Kill-switch:** if no rule beats the index after-tax OOS, basket construction adds no edge -> the honest
  recommendation is the passive index (or the simple static hold), and we stop.
- A rule beating the static basket but not the index is **inconclusive**, not a win.

## Ablations (to attribute any edge)

Basket size (10/12/15); drop-bottom-third vs full-reselect; let-winners-run vs equal-weight-rebalanced;
6-1 vs 12-1 formation; each factor's marginal contribution (A1 -> A2 -> A3 -> A4).

## Known risks encoded as checks

- **Momentum buys late/high:** A1 will tend to buy names *after* they run (the NVDA-late-2022 problem) —
  the valuation cap (A3) and quality (A2) screens exist to test whether tempering that helps.
- **Momentum crashes:** expect deep drawdowns in sharp reversals; the Ulcer/Calmar bar guards against it.
- **Turnover/tax drag:** annual rotation realizes gains/losses; the after-tax metric is the judge.
- **Concentration:** 10-15 names raises single-name risk; per-name loss is bounded only by sizing, not by
  the rule — report worst-name contribution.
- **Look-ahead is the #1 trap:** strictly as-of selection; reasoning-from-known-winners (NVDA/MU) is
  exactly what the blind point-in-time test removes.

## Dependency

Requires the Sharadar ingestion layer (bulk -> local PIT store, `permaticker`/`datekey` keyed) and the
readiness/fail-closed hooks. Do not run on the yfinance proxy — the whole point is survivorship-free PIT.
