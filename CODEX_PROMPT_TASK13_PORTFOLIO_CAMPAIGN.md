# Codex Prompt — Task 13: Modular Portfolio Strategy Framework, Portfolio-Level Backtester, Campaign 2 (Layer Ablation)

You are working in the `Investor/` Python project. Campaign 1 (Task 11) returned a clean negative: the per-name regime strategy earned 1.82% OOS vs 60.39% SPY because its exit ladder structurally caps winners and its gate stack structurally under-participates. The redesign inverts the burden of proof: **own the market by default; every deviation must pay rent.** This task builds the machinery to test that design and runs Campaign 2 under the same pre-registration discipline as Campaign 1 (`CODEX_PROMPT_TASK10_ALPHA_CAMPAIGN.md` rules apply: no default flips, evidence floors, full disclosure of configurations evaluated).

Three deliverables, in order: (A) modular strategy framework, (B) portfolio-level backtester, (C) Campaign 2 execution + report.

## Shared constraints

Same as all prior tasks: offline deterministic tests, no new runtime deps (numpy/pandas/sklearn already present), no pickle, never touch `.env`, additive only, full suite + `scripts/typecheck.sh` green (route file and sprint57 isolated per project notes; suite-wide `pytest-timeout` is configured). **Do not modify** `pipeline_backtest.py`, the live trading path, `alpha_campaign.py` phases 0–3, or anything the agents currently execute — this is parallel research machinery. Reuse, do not duplicate: price cache, universe screen, stress windows, `compute_equity_metrics`, basket loader, campaign artifact/resume patterns.

---

## A — Modular strategy framework (`src/regime/strategy/`)

The explicit goal (user requirement): strategies must be **swappable and extensible** so that new market conditions can be answered with new layer implementations, not rewrites.

New package `src/regime/strategy/` with:

1. **Layer protocols** (`interfaces.py`) — small, typed, documented `Protocol` classes. The engine depends ONLY on these:
   - `SignalProvider`: `prepare(ticker, frame) -> None` (precompute), `signals(ticker, date) -> dict[str, float|str]` — wraps regime labels/posteriors, realized vol, momentum scores. Implementations must be leak-free: signals for date T may use data through T only (the walk-forward HMM provider from `pipeline_backtest` is the reference pattern; reuse its capped-window approach).
   - `ExposurePolicy`: `target_exposure(date, portfolio_state, signal_map) -> float` in [0, 1] — portfolio-level equity fraction (Layer 1 lives here).
   - `OverridePolicy`: `override(date, portfolio_state, signal_map) -> ExposureOverride | None` — crash brake; may force per-name exclusions and/or a portfolio exposure cap, with a machine-readable reason (Layer 2).
   - `AllocationPolicy`: `weights(date, eligible_names, signal_map) -> dict[str, float]` summing to 1 — within-sleeve weights (Layer 0 equal-weight; Layer 3 momentum tilt).
   - `RebalancePolicy`: `should_rebalance(date, drift_state) -> bool` — cadence + drift bands; overrides from `OverridePolicy` always execute regardless.
2. **`StrategySpec`** (`spec.py`) — frozen dataclass naming each layer implementation by registry key plus its params; `to_dict`/`from_dict` (JSON, no pickle); a content hash. Every backtest result embeds the spec dict + hash — same attribution rule as everything else in this project.
3. **Registry** (`registry.py`) — `register_layer(kind, key)(cls)` decorator + `build(kind, key, params)`. New layers plug in without touching the engine or the campaign runner. Unknown key → clear error listing available keys.
4. **Reference implementations** (`layers.py`) — exactly these for Campaign 2:
   - `equal_weight` (AllocationPolicy): 1/N over eligible names.
   - `vol_target` (ExposurePolicy): exposure = clip(target_vol / forecast_vol, min_exposure, 1.0); forecast_vol = annualized EWMA (lambda=0.94, 20d min history) of daily portfolio returns computed from current weights. Params: `target_vol=0.15`, `min_exposure=0.25`. Long-only, never >1.0 (no leverage).
   - `regime_brake` (OverridePolicy): per-name exclusion while HMM label is Bear; portfolio cap `breadth_cap` (default 0.5) when Bear-share of names ≥ `breadth_trigger` (default 0.5); auxiliary grinding-bear trigger: portfolio drawdown from peak ≥ `aux_dd_trigger` (default 8%) AND median name label ≠ Bull → cap exposure at `aux_cap` (default 0.5). **Re-entry is explicit and tested**: a name re-enters when label is Bull, or Neutral with `p_bull_day5 > p_bear_day5`, sustained `reentry_days` (default 3) consecutive sessions. All triggers/reasons recorded per day for the report.
   - `momentum_tilt` (AllocationPolicy): 12-1 cross-sectional momentum (252d return skipping most recent 21d), hold top `top_fraction` (default 0.5) of eligible names equal-weighted. Recompute at rebalance only.
   - `monthly_bands` (RebalancePolicy): first trading day of month, plus intra-month trigger if any weight drifts > `band` (default 25% relative).

## B — Portfolio-level backtester (`src/regime/portfolio_backtest.py`)

Daily event loop over a basket — the missing infrastructure (the pipeline backtest is per-ticker; Layers 1–3 are portfolio constructs).

- **Inputs:** dict of normalized market frames (reuse the loader + price cache), `StrategySpec`, `PortfolioBacktestConfig` (starting_cash=100_000, entry/exit cost bps =5 each, oos_start, integer_shares=True).
- **Loop semantics (mirror the verified pipeline-backtest conventions):** signals computed from history through day T; rebalance decisions at T execute at T+1 open with costs; overrides (brake) also fill at T+1 open — never same-close (no look-ahead anywhere; this is the #1 review focus). Integer shares with cash residual; mark-to-market daily equity curve.
- **Accounting invariants enforced in code** (assert, not hope): cash + Σ(position value) == equity every day; Σ costs == turnover × cost rate; cash never negative; exposure == position value / equity within tolerance.
- **Outputs:** `PortfolioBacktestResult`: equity curve, daily exposure series, trades, turnover (annualized), costs paid, metrics via the existing `compute_equity_metrics` (plus Calmar = CAGR/|maxDD| and annualized turnover), IS/OOS segments, stress-window table including `days_to_derisk` (days from window start until exposure first drops below 0.7) and exposure-during-window, per-day brake-reason log, embedded spec + config + git SHA.
- **Controls implemented as specs, not special cases:** `spy_buy_hold` (single-name SPY frame through the same engine), `spy_200dma` (exposure 1.0/0.0 on SPY price vs 200-day MA, 5-day confirmation), `spy_vol_target` (vol_target layer on SPY). If the engine can't express a control cleanly, that's an interface bug — fix the interface.

## C — Campaign 2: pre-registered layer ablation

Runner: extend the campaign CLI (`alpha-campaign run --campaign 2 --phase N` or a new `portfolio-campaign` subcommand — keep artifact/resume/run-log conventions identical to Campaign 1). Same basket (`data/campaign/basket.json`, the re-pinned 30 names), same OOS boundary 2024-01-01, 10y window, same frozen-cache rule.

**Pre-registered arms (this is the registration — do not add arms after seeing results):**

| Arm | Spec |
| --- | --- |
| L0 | equal_weight + monthly_bands, always 100% exposed |
| L1 | L0 + vol_target |
| L2 | L1 + regime_brake |
| L3 | L2 + momentum_tilt |
| C1–C3 | spy_buy_hold, spy_200dma, spy_vol_target |

Small pre-registered sensitivity grids ONLY (disclose total count): vol_target ∈ {0.12, 0.15, 0.18}; brake `aux_dd_trigger` ∈ {6%, 8%, 10%} × `reentry_days` ∈ {3, 5}; momentum `top_fraction` ∈ {0.33, 0.5}. Headline comparisons use defaults; grids inform robustness commentary only.

**Pre-registered promotion rules (encode in the runner verdict logic, mirroring `robustness_verdict`):**
1. Layer N is *supported* iff it beats Layer N−1 OOS on Sharpe AND Calmar, and OOS return is not degraded by more than 15% relative.
2. The best supported stack must beat `spy_200dma` (the embarrassing control) on OOS Sharpe — otherwise the verdict line reads "the HMM brake does not yet pay for its complexity," verbatim.
3. Stress preservation: the final stack must retain ≥ 50% of L2's stress-window drawdown advantage vs L0 in COVID and 2022 windows.
4. Cost fragility: rerun the winning stack at 2× cost bps; if support flips, report "cost-fragile" and do not recommend.

**Report:** `ALPHA_CAMPAIGN_2_REPORT.md` (committed), structure mirroring Campaign 1: executive answers (does each layer pay rent? does the stack beat the dumb controls? full-cycle vs OOS-bull behavior?), per-arm table (CAGR, vol, Sharpe, Calmar, maxDD, turnover, costs, exposure mean), stress-window table with `days_to_derisk` per arm, sensitivity grids, configurations-evaluated count, recommended-next-steps awaiting human sign-off. Honesty clause carries over: if L0 wins outright, the first paragraph says so.

## Tests (`tests/regime/test_portfolio_backtest.py`, `test_strategy_layers.py`, `test_campaign2.py`)

All offline/synthetic. Required coverage:

- **Engine accounting:** invariants hold on a random-walk basket; costs reconcile against turnover by hand on a 2-name fixture; integer-share residual cash tracked; a forced rebalance produces T+1-open fills (assert no same-day fill anywhere).
- **No look-ahead:** a signal provider stub that poisons future data (returns NaN for dates > T unless leaked) — engine results must be NaN-free.
- **vol_target math:** hand-computed EWMA case; exposure clipped to [min_exposure, 1].
- **regime_brake:** Bear name excluded at T+1; breadth cap engages at exactly the trigger; aux drawdown trigger fires on a crafted grind; re-entry requires the full consecutive-day run (test an interrupted run does NOT re-enter); every override day carries a reason.
- **momentum_tilt:** 12-1 skip-month ranks on a fixture (assert the skip month actually skipped); top-fraction selection; recompute only at rebalance.
- **monthly_bands:** first-trading-day fires; drift band fires intra-month; override executes despite no rebalance.
- **Controls:** spy_200dma on a crafted series crosses where expected with confirmation.
- **Spec/registry:** round-trip spec serialization + hash stability; unknown layer key errors helpfully; a dummy registered layer is buildable (extensibility proof).
- **Verdict logic:** promotion rules 1–4 on hand-built result fixtures, including the cost-fragility flip.
- **Determinism:** same inputs → identical result hash.

## Execution & definition of done

1. A+B merged with tests green before C runs (separate commits: framework → engine → controls → campaign runner → campaign artifacts/report).
2. Campaign 2 executed locally against real data (network OK): warm cache (reuse manifest pattern), run all arms + grids + controls, `run_log.md` updated, wall-clock recorded. Expect this to be much cheaper than Campaign 1 (one HMM pass per name, shared across arms — cache regime signal series per name once and reuse across arms; build that reuse in, 30 names × 7 arms must not mean 210 HMM fits).
3. `ALPHA_CAMPAIGN_2_REPORT.md` committed with all four promotion rules evaluated verbatim and the configurations-evaluated count.
4. No production defaults or agent behavior changed. Mapping winning specs onto the four agents is explicitly a future task pending human review of the report.
5. PR description: layer-ablation table, the controls verdict sentence, cost-fragility result, total configurations, wall-clock, and any spec/interface decisions that deviated from this prompt (with rationale).
