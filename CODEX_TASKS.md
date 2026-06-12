# Codex Task Brief — Regime Agent: Pipeline Backtest + Meta-Labeler Alignment

Implements items 1.4 and 1.2 from `REGIME_REVIEW.md`. Do Task 1 first; Task 2's validation depends on it.

## Context you need before writing code

The Regime module (`src/regime/`) is an automated trading agent: a walk-forward 3-state Gaussian HMM (`hmm_engine.fit_regime_model`, production defaults `training_window=504`, `refit_step=21`) produces regime labels; `signals.signal_from_forward_curve` + `signals.build_composite_signal` produce Buy/Sell/Hold; `paper_trading.generate_buy_plans` applies gates and sizing; `paper_trading.generate_exit_plans` manages exits.

Recent changes you MUST account for (already merged):

- Buy-plan geometry (stop/target/R:R) is recomputed from the actual routed fill price via `paper_trading._actual_fill_trade_geometry`, stored on plans (`stop_price`, `target_price`, `risk_reward_ratio`, `timeframe_days`, `trade_geometry_source` columns in `persistence._PAPER_TRADE_PLAN_COLUMNS`).
- Exit management now includes, in priority order: profit-target exit (`current >= target_price`), ATR trailing-stop ratchet (`paper_trading._ratchet_trailing_stop`: activates only when `current > entry + 1×ATR`, ratchets stop to `current − 2×ATR`, never down), time stop (`_holding_days >= timeframe_days`, default `DEFAULT_EXIT_TIME_STOP_DAYS = 21`), regime-flip exits (cached Bear / composite Sell), and a Neutral partial reduce (`_neutral_reduce_reason`, sells `DEFAULT_NEUTRAL_REDUCE_FRACTION = 0.5` of the position).
- Missing-ATR sizing is conservative: `_risk_adjusted_quantity` returns half the capital-based shares when ATR is unavailable.
- The scheduler passes full cached payload rows (dicts with `regime`, `probability`, `composite_signal`, `previous_regime`, `p_bull_day5`, `p_bear_day5`, …) — not tuples.

Existing reusable pieces: gates in `signal_quality.evaluate_signal_quality`, `hurdle_rate.check_hurdle_rate` / `check_duration_gate`, `anti_churn.check_anti_churn`; execution cost in `slippage.estimate_execution_cost`; routing in `order_routing.decide_routing`; labeling in `triple_barrier.py`; ML in `meta_labeler.py`. Note `calibration.py` is *guardrail* calibration — unrelated to probability calibration; do not extend it.

## Environment and conventions

- Run tests with `cd Investor && .venv/bin/pytest tests/regime/... -q`. Full suite: `make test`.
- Tests must not hit the network (yfinance is blocked in CI). All new code paths must accept injected price data; never call `download_market_frame` inside something a test exercises without a seam to replace it.
- Do not modify `.env` (it contains absolute `/Volumes/...` paths) and do not hard-code absolute paths anywhere, including tests. Use `HMM_DATA_DIR` env var for data dirs (see `meta_labeler._default_models_dir`).
- Schema changes: additive only, via the `_PAPER_TRADE_PLAN_COLUMNS`-style dicts in `persistence.py` (auto-migration pattern already in place).
- Pinned deps: `scikit-learn==1.8.0`, `xgboost==2.1.4`. No new dependencies without strong justification.
- Follow existing style: `from __future__ import annotations`, dataclasses for configs/results, module-level `logger`, type hints everywhere. New tests go in behavior-named files (`tests/regime/test_pipeline_backtest.py`, `tests/regime/test_meta_labeler_alignment.py`), not sprint-numbered files.
- Do not refactor unrelated code. `paper_trading.py` and `persistence.py` are oversized; resist the urge — extraction is a separate task.

---

## Task 1 — Event-driven backtest of the production pipeline (review item 1.4)

### Problem
`src/regime/backtest.py` does not test the system that trades: it caps `training_window` at 120 (production: 504), trades the raw composite signal only at 21-day refit boundaries, applies no gates, no stops/targets/trailing/time exits, no sizing, no costs, and reports `mean/std×sqrt(n_trades)` as "Sharpe" (a t-stat) and `alpha/|benchmark|` as "information ratio" (not an IR).

### Deliverable
New module `src/regime/pipeline_backtest.py`. Leave `backtest.py` untouched (UI depends on it) except: rename the misleading metric keys there is NOT in scope — do not touch it at all.

### Requirements

1. **`PipelineBacktestConfig` dataclass** mirroring production defaults: `training_window=504`, `refit_step=21`, HMM `lookback_window=20`, sizing (`DEFAULT_SIZING_ATR_MULTIPLIER`, `DEFAULT_SIZING_BASE_RISK_FRACTION`, sizing method), gate toggles (hurdle, duration, anti-churn, signal quality), exit params (`profit_target`, `trailing_atr_multiplier=2.0`, `trailing_activation_atr=1.0`, `time_stop_days=21`, `neutral_reduce_fraction=0.5`), cost model toggle, `starting_cash`. Every result must embed the exact config used (`dataclasses.asdict`) — this is the parameter-attribution requirement from the review.

2. **Daily event loop** (not refit-interval trading). For each trading day after warm-up:
   - Update regime state. Refit the HMM only every `refit_step` days (reuse `fit_regime_model` on the data available up to that day — no future rows may enter the window). Between refits, reuse the prior model's decode for the new day, mirroring `hmm_engine`'s walk-forward semantics. Cache aggressively: one `fit_regime_model` call per refit boundary, not per day.
   - Generate the entry signal exactly as production does: `forward_regime_curve` → `signal_from_forward_curve` → `intra_regime_signal` → `build_composite_signal`.
   - Apply gates in production order with production semantics: signal-quality (staleness is moot in backtest — pass a same-day timestamp), hurdle rate (`check_hurdle_rate` with `estimate_execution_cost`; if those functions require DB/settings access, replicate their *formulas* in pure functions and add a unit test asserting parity against the originals on fixed inputs), duration gate, anti-churn (track round trips within the backtest itself).
   - Size with `_risk_adjusted_quantity` logic (import it; it is pure).
   - Enter at next-day open if available, else same-day close, minus/plus a configurable cost in bps (default from `estimate_execution_cost`'s typical output; make it a config field `entry_cost_bps`, `exit_cost_bps`).
   - Compute trade geometry from the actual modeled fill via the same rules as `_actual_fill_trade_geometry` (import if importable without DB side effects; otherwise replicate + parity test).
   - **Manage open positions daily with the production exit ladder**: profit target (intraday touch: use day high for longs), trailing ratchet (same activation and distance rules as `_ratchet_trailing_stop`), stop (day low touch), time stop, regime-flip full exit, Neutral partial reduce at 50%. Conservative tie-break: if both stop and target are touched the same day, assume the stop filled.

3. **Correct metrics**, computed from the *daily* mark-to-market equity curve:
   - `annualized_return`, `annualized_volatility`, `sharpe_ratio = (mean_daily_excess × 252) / (std_daily × sqrt(252))` (rf=0 acceptable, document it), `max_drawdown` (peak-to-trough on daily equity), `win_rate`, `profit_factor`, `avg_holding_days`, `exposure_pct` (fraction of days with an open position), per-exit-type counts (target/stop/trailing/time/regime/reduce), total costs paid.
   - Benchmark comparison: true excess return vs buy-and-hold, plus information ratio = mean(daily active return)×252 / (std(daily active return)×sqrt(252)).
   - Out-of-sample split: accept `oos_start` date; report all metrics for IS and OOS segments separately.

4. **Determinism + injectability**: accept a prepared `market_frame` (the same shape `download_market_frame(...).frame` returns: `price, high, low, volume, vix, yield_10y` indexed by date). Provide a thin convenience wrapper that downloads, but the core function must be pure given a frame. Fixed seeds throughout.

5. **CLI hook**: add a subcommand to `src/regime/cli.py` (`pipeline-backtest TICKER --period 5y --oos-start 2025-01-01 --json out.json`) following the existing command pattern there.

### Tests (`tests/regime/test_pipeline_backtest.py`)
Use synthetic OHLCV frames (e.g. geometric random walk with a seeded generator, plus hand-built segments). Must cover at minimum:
- A deterministic uptrend → profit-target exit fires; exit price respects target; costs deducted.
- A spike-then-decay path → trailing ratchet locks gains; stop never moves down.
- A flat path → time stop fires at exactly `time_stop_days`.
- Stop+target same-day touch → stop wins (conservative).
- Neutral reduce sells exactly half (floor for share counts, matching `_reduced_exit_quantity` semantics).
- Sharpe/max-drawdown asserted against hand-computed values on a tiny fixed equity curve.
- Config is embedded in the result and round-trips through JSON.
- Parity tests for any replicated production formula (hurdle, geometry) against the original functions on fixed inputs.

### Acceptance criteria
All new tests pass; `make test` stays green; no network calls in tests; running the CLI on a real ticker locally produces a JSON report whose exit-type distribution is non-degenerate (more than one exit type appears in a 5y run).

---

## Task 2 — Meta-labeler alignment + probability calibration (review item 1.2)

### Problem
The XGBoost meta-labeler trains on triple-barrier outcomes (2×ATR target, 2×ATR static stop, 21-day vertical, timeout = loss) but live trades are managed with a trailing ratchet, profit target, time stop, and regime exits — so its "probability of success" describes a game that is no longer played. Probabilities are also uncalibrated, evaluated on a single 80/20 split with overlapping (serially correlated) samples, and missing features silently default to 0.0.

### Deliverables
Modify `src/regime/triple_barrier.py` (additive) and `src/regime/meta_labeler.py`; new module `src/regime/probability_calibration.py`.

### Requirements

1. **Management-aware labeling** (`triple_barrier.py`): add `ManagedExitConfig` (profit_target_atr_mult=2.0, trailing_atr_mult=2.0, trailing_activation_atr=1.0, time_stop_days=21, stop_atr_mult=2.0) and `apply_managed_exit_labels(price_frame, ...)` that simulates the *live* exit ladder forward from each Bull-regime bar (long-only is correct here — production never opens shorts; Bear bars get no label): static stop until activation, then trailing ratchet; target touch = win; stop/trailing touch = loss; timeout = label by sign of net return at timeout (win if > 0 after round-trip costs, configurable `cost_bps` default ~20). Return the same column contract as `apply_triple_barrier_labels` plus `barrier_type` values `{"target","stop","trailing","time_win","time_loss"}`. Keep the old function untouched for backward compatibility.
   - **Bar-by-bar semantics — match `pipeline_backtest._manage_position` exactly** (it is the reference implementation, already verified):
     (a) each day's low is tested against the stop *as of the prior close* — the trailing ratchet is computed from the day's **close** (not high) and takes effect the next bar;
     (b) stop touch beats target touch on the same bar (conservative);
     (c) `time_stop_days` counts **calendar days** between bar dates, not trading bars (matches production `_holding_days`).
     Add a test asserting label outcomes match `_manage_position` exit types on a shared synthetic path.
   - **Single source of truth**: the trailing/activation rules must be expressed once. Extract the ratchet math into a small pure function (e.g. `triple_barrier.trailing_stop_level(entry, current, atr, config)`) and refactor `paper_trading._ratchet_trailing_stop` to call it. Add a parity test.

2. **Overlap handling**: in the labeled frame, add `label_end_idx` (bar index where the outcome resolved). Provide `sample_uniqueness_weights(labeled_frame)` computing average-uniqueness weights (López de Prado ch.4: weight_i = mean over the label's lifespan of 1/concurrent_label_count). Pass as `sample_weight` to XGBoost fit.

3. **Purged walk-forward CV** in `meta_labeler.py`: replace the single 80/20 split with K chronological folds (default 5); for each fold, train on data strictly before the fold minus an embargo of `max_holding_days` bars (purge any training sample whose `label_end_idx` reaches into the test fold). Report per-fold and aggregate accuracy/precision/recall/F1/AUC and Brier score. Keep `MetaLabelerConfig` defaults; add `n_folds`, `embargo_bars` fields.

4. **Calibration** (`probability_calibration.py`): fit isotonic regression (sklearn `IsotonicRegression`, `out_of_bounds="clip"`) on out-of-fold predictions from the CV in (3) — never on training-fold predictions. Persist alongside the model version (extend `meta_labeler.save_model`/`load_model` to write/read a sibling `meta_labeler_v{N}_calibrator.json` — serialize the isotonic thresholds yourself as JSON; do not pickle). Report pre/post-calibration Brier score; warn if calibration makes Brier worse (small samples).

5. **Wire calibrated probability into decisions**: `MetaLabelerEngine.analyze` must return the *calibrated* probability when a calibrator is loaded (raw probability still in `details["raw_probability"]`). The 0.50 veto / 0.65 confirm thresholds and `compute_position_size(meta_labeler_probability=...)` then receive calibrated values with no further changes.

6. **Fail loudly on degraded features**: in `extract_meta_features` and `analyze`, if more than 2 of the 8 `META_FEATURES` are missing/None on the input, return the not-trained passthrough result with `details["status"]="degraded_features"` and log a warning — never score a near-zero vector silently.

7. **Validation against Task 1**: add a CLI or script entry (`pipeline-backtest ... --meta-labeler v{N}`) that runs the Task 1 backtest twice — meta-labeler veto on vs off — and reports the delta (return, Sharpe, trade count). This is the acceptance evidence that the realigned labeler helps rather than hurts.

### Tests (`tests/regime/test_meta_labeler_alignment.py`)
- Managed-exit labeling on hand-built price paths: target win, trailing-locked win, static-stop loss, timeout-positive → `time_win`, timeout-negative → `time_loss`; costs flip a marginal timeout from win to loss.
- Parity: `trailing_stop_level` vs `_ratchet_trailing_stop` on fixed inputs.
- Uniqueness weights: non-overlapping labels → all weights 1.0; fully overlapping → 1/n.
- Purged CV: assert no training sample's `label_end_idx` falls inside any test fold; embargo respected.
- Calibrator round-trip: fit on synthetic miscalibrated probabilities, save, load, predictions match; JSON file, not pickle.
- Degraded-features passthrough triggers at 3+ missing features.
- `analyze` returns calibrated probability when calibrator present, raw in details.

### Acceptance criteria
All new tests pass; existing `tests/regime/test_sprint*` suites untouched and green (`make test`); a real-data run (local, with network) shows the CV Brier score post-calibration ≤ pre-calibration, and the Task 1 A/B harness runs end-to-end.

---

## Out of scope (do not do)

Sizing rework (review 2.1), empirical regime durations (1.3), HMM seed ensembles (3.1), module splits of `paper_trading.py`/`persistence.py`, UI changes, anything in `backtest.py`, live/IBKR execution paths.

## Definition of done (both tasks)

1. `make test` green locally.
2. New tests are deterministic and offline.
3. No new warnings from `pytest -W error::DeprecationWarning` on the new test files.
4. Every replicated production formula has a parity test pinning it to the original.
5. Short summary in the PR description: per-task what changed, metric definitions used, and the A/B backtest delta table.
