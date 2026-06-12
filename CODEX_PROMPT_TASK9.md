# Codex Prompt — Task 9: Universe Eligibility Screen + Black-Swan-Horizon Backtesting

You are working in the `Investor/` Python project. Tasks 1–8 are merged (event-driven pipeline backtest, managed-label meta-labeler with skill gate, threshold sweep, HMM robustness, module split). This task has two parts; deliver as **two commits/PR sections in this order** (9A then 9B). 9B's acceptance runs depend on 9A's universe.

**Decisions already made (do not revisit):** trading stays within regular market hours — no extended-hours capability of any kind. Theme-forcing for agents is being retired in favor of an eligibility screen. No live-default changes beyond what is explicitly stated here.

## Shared constraints

Same as prior tasks: tests offline/deterministic (no network in CI; synthetic frames), `tmp_path` + `HMM_DATA_DIR` patterns, no new dependencies, no pickle, never touch `.env`, behavior-named test files, clean under `-W error::DeprecationWarning`. Run the full suite (`python -m pytest -q`) plus `scripts/typecheck.sh` before declaring done. Do not modify the exit-ladder semantics in `pipeline_backtest._manage_position` / `triple_barrier._managed_label_single_bar`.

---

## Task 9A — Universe eligibility screen

### Problem
Agent candidate flow is constrained to human-curated themes, which concentrates all four agents in correlated names. The replacement is a mechanical eligibility screen for the whole US-listed universe, within which agents differentiate. Penny stocks and short-history names must be excluded because the HMM needs ~2 years to fit, and thin names make the slippage/hurdle models understate true costs.

### Deliverables

1. **`src/regime/universe.py`** — new module:
   - `@dataclass(frozen=True) UniverseEligibility` with the screen result: `eligible: bool`, `reasons: list[str]`, plus the measured values (price, history_days, dollar_adv, asset_class).
   - `check_universe_eligibility(ticker, *, market_frame=None) -> UniverseEligibility`. Criteria, each individually settings-overridable via `get_setting` with these defaults:
     - `universe_min_price` = **5.0** (last close)
     - `universe_min_history_days` = **756** trading days (~3 years)
     - `universe_min_dollar_adv` = **10_000_000** (20-day average of close × volume)
     - existing `EXCLUDED_TICKER_PATTERNS` and `HMM_ELIGIBLE_ASSET_CLASSES` from `config.py` still apply (keep the leveraged/inverse/FX/bond-fund exclusions; extend the pattern set with common leveraged-ETF suffixes if not already covered)
   - `universe_screen_enabled` setting, default **true**.
   - Accept an injected `market_frame` for testability; only fetch when not provided. Cache results per ticker per day (in-memory + settings-backed daily stamp is fine) so discovery scans don't refetch.
2. **Wire the screen into every entry path** (the same belt-and-suspenders pattern as the other gates):
   - discovery candidate intake (`discovery.py` / `agent_candidate_intake.py`) — ineligible tickers never reach the watchlist; record the reason on the candidate record;
   - `generate_buy_plans` — gate with logging and a `gate_counts`-style audit event, like anti-churn/hurdle (covers candidates grandfathered onto the watchlist before the screen existed);
   - the pipeline backtest — `PipelineBacktestConfig.enforce_universe_screen: bool = True` evaluated once per run from the provided frame (history length, price, ADV computable offline from the frame itself — no network).
   - Exits are NEVER screened — you can always sell what you hold.
3. **Retire theme-forcing for agent portfolios**: behind setting `agent_theme_budgets_enabled` (default **false** — this is the one deliberate live-behavior change, flag it in the PR), agent buy planning sizes from portfolio-level budget (existing role-budget math against total budget) instead of requiring theme membership. Themes remain for human organization and the non-agent flows; nothing about theme CRUD changes. When the setting is true, behavior is bit-identical to today (regression-pin it).

### Tests (`tests/regime/test_universe_screen.py`)
- Each criterion individually: $4.99 close fails with reason `min_price`; 700-day history fails `min_history`; low ADV fails `min_dollar_adv`; excluded pattern fails; all-pass case eligible with empty reasons.
- Settings overrides honored; `universe_screen_enabled=false` → everything eligible.
- `generate_buy_plans` blocks an ineligible watchlist ticker with an audit/log entry (follow the anti-churn test pattern in `test_beta_target_deployment.py`).
- Backtest: a synthetic frame failing the screen produces zero entries and a populated `gate_counts["universe"]`; `enforce_universe_screen=False` restores entries.
- Theme retirement: with `agent_theme_budgets_enabled=false`, a themeless watchlist candidate produces a buy plan; with `true`, today's theme-required behavior is reproduced exactly.

---

## Task 9B — Black-swan-horizon backtesting

### Problem
The pipeline backtest is exercised at `5y`, which misses the regimes that matter most for a regime-detection strategy. The window must extend far enough to cover recent black-swan events, and results must be reported per stress event — how the strategy and the regime model behaved *through* each crisis, not just on average.

### Deliverables

1. **Long-horizon support**:
   - `run_pipeline_backtest_for_ticker` and the CLI accept `--period` up to `10y`/`max` and an explicit `--start/--end` date pair (passed through to the data layer). Verify `download_market_frame` supports these; extend it if needed.
   - **Performance budget:** profile a 10-year single-ticker run. The known hot spot: `_ProductionSignalProvider` calls `fit_regime_model` on refit days, and `fit_regime_model` internally walk-forwards over its whole input — making long runs superlinear. Fix within the existing semantics, e.g. cap the frame passed to `fit_regime_model` at `training_window + lookback margin` trailing bars (the model only needs the training window; document that decoded-history-dependent fields like `regime_days` still compute correctly from the capped window or are carried by the provider). A 10-year run for one ticker must complete in **< 5 minutes** on a typical dev machine; print timing in CLI output. The capped-window optimization must be regression-pinned: identical signals vs. the uncapped path on a fixture frame.
   - **On-disk price cache** (`HMM_DATA_DIR/price_cache/{ticker}_{interval}.parquet` or CSV — no new deps; pandas parquet needs pyarrow, so use CSV if pyarrow is absent): `download_market_frame` gains an opt-in `cache=True` used by backtest/sweep CLI paths, refreshing only missing trailing dates. Basket sweeps must not re-download ten years per combo.
2. **Named stress windows** (`src/regime/stress_windows.py`):
   - `DEFAULT_STRESS_WINDOWS: tuple[StressWindow, ...]` — frozen dataclass `StressWindow(key, label, start, end)` — seeded with: `covid_crash` (2020-02-19 → 2020-04-30), `bear_2022` (2022-01-03 → 2022-10-14), `vol_shock_aug_2024` (2024-07-31 → 2024-08-19), `tariff_shock_2025` (2025-03-01 → 2025-05-31). Dates are defaults; user-overridable via a `stress_windows` JSON setting. (Verify the 2024/2025 window dates against the actual drawdowns in the data when you run acceptance; adjust defaults if the data says otherwise and note it in the PR.)
   - `PipelineBacktestResult` gains `stress_windows: list[dict]` — for each window overlapping the run: the standard segment metrics (reuse `_segment_metrics`), benchmark comparison for the same window, plus regime-model diagnostics: `days_to_bear_flag` (bars from window start until the model first labels Bear), `max_drawdown_strategy` vs `max_drawdown_benchmark`, `exposure_pct` during the window, and exit-type counts inside the window. These diagnostics answer the only question that matters in a crash: did the model get you out, and how fast.
   - CLI: `pipeline-backtest ... --stress-report` prints a per-window table; threshold-sweep output gains optional per-window aggregate columns (flag-gated to keep default output unchanged).
3. **Defaults for the alpha campaign**: bump the CLI default `--period` for `pipeline-backtest` and `threshold-sweep` from `5y` to `10y`. (Default change is contained to offline tooling — allowed.)

### Tests (`tests/regime/test_stress_windows.py`)
- Synthetic 10-year frame with an engineered crash (e.g. −35% over 25 bars with elevated synthetic VIX) at a known date: stress-window segmentation returns exact hand-computed segment metrics; `days_to_bear_flag` computed correctly against a stub signal provider that flips to Bear N bars in; windows outside the run are omitted.
- Window overrides via the JSON setting parse and replace defaults; malformed JSON falls back to defaults with a logged warning (and a `record_fallback` call).
- Capped-window provider parity: identical `PipelineSignal` sequences capped vs. uncapped on a fixture frame.
- Price cache: second load hits cache (no fetch — assert via a counting stub), trailing refresh appends only new dates.
- CLI `--stress-report` smoke test with a stub provider (follow the existing CLI test pattern in `test_pipeline_backtest.py`).

---

## Definition of done

1. Full suite + typecheck green; new tests clean under `-W error::DeprecationWarning`.
2. Regression pins hold: `agent_theme_budgets_enabled=true` and `enforce_universe_screen=False` and the uncapped provider all reproduce current behavior bit-identically.
3. Acceptance evidence in the PR (local, network OK): (a) screen statistics for a ~30-name candidate list — how many pass, top exclusion reasons; (b) one real 10-year `pipeline-backtest --stress-report` run (e.g. MSFT or SPY-adjacent large cap) showing all four stress windows with `days_to_bear_flag` populated, plus the run's wall-clock time under the 5-minute budget.
4. PR description: the two deliberate default changes (`agent_theme_budgets_enabled=false`, tooling period 10y), the stress-window date verification notes, and the capped-window design note.
