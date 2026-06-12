# Codex Prompt — Tasks 4–7: Remaining Items from REGIME_REVIEW.md

You are working in the `Investor/` Python project. Tasks 1–3 are complete and merged: the event-driven pipeline backtest (`src/regime/pipeline_backtest.py`) replays the production decision path with correct metrics; the meta-labeler trains on managed-exit labels with date-aware purged CV and JSON isotonic calibration; production retraining defaults to managed labels (`meta_labeler_label_mode` setting, active model `meta_labeler_v3`).

**Execute as separate PRs, in order (4 → 4b → 5 → 6 → 7). Do not start a task until the previous one is merged or explicitly approved.** Each task below is self-contained.

## Shared constraints (all tasks)

- Tests offline and deterministic: no network (yfinance is blocked in CI), no hard-coded `/Volumes/...` paths, `tmp_path` + `HMM_DATA_DIR` monkeypatch (see `tests/regime/test_meta_labeler_alignment.py` for patterns). Never touch `.env`.
- Pinned deps: `scikit-learn==1.8.0`, `xgboost==2.1.4`, `hmmlearn` as pinned. No new dependencies. No pickle.
- New tests in behavior-named files. Clean under `-W error::DeprecationWarning`.
- Run `tests/test_regime_route.py` in isolation (known order-dependent background-job flake). The sprint sweep has a known Torch segfault when `test_sprint57.py` runs after `test_sprint51-56` in one process — run it isolated; do not silently skip.
- The exit-ladder semantics in `pipeline_backtest._manage_position` and `triple_barrier._managed_label_single_bar` are verified and frozen — do not modify them.
- Style: `from __future__ import annotations`, dataclasses, type hints, module logger.
- **Tuning discipline:** any change to a live decision default must be justified by basket-level OOS results from the pipeline backtest, not a single ticker's in-sample run. Where evidence is weak, ship the capability behind a setting and keep the current default.

---

## Task 4 — Risk-first sizing + veto redesign (review 2.1 + v3 A/B finding)

### Problem
`signals.compute_position_size` anchors on `base_pct = regime_probability × 100` — a 0.70 posterior implies a 70% position before caps. Regime posterior is model certainty, not edge; HMM posteriors routinely exceed 0.9, so the caps (half-Kelly, 2×ATR/`max_risk_pct`) always bind and the anchor is meaningless noise. Hold still sizes at 25% of base. Kelly consumes regime probability as win-prob — a category error.

Separately, the v3 A/B showed the binary meta-labeler veto (calibrated prob < 0.50 in `MetaLabelerEngine.analyze`) suppresses ~90% of entries: isotonic calibration centers probabilities on the label base rate, so when the base win rate is below 0.50 the cutoff vetoes mechanically, regardless of discrimination. Trades collapsed 9→1 / 8→1 / 1→0 (NVDA/AVGO/MU).

### Deliverables

1. **Rework `compute_position_size`** (`src/regime/signals.py`) — keep the signature and `PositionSize` return shape (additive fields only):
   - Primary sizing = the existing risk-budget block: position such that a 2×ATR stop loses at most `max_risk_pct` of portfolio. This becomes the anchor, not a cap.
   - Scale the anchor by `0.5 + 0.5 × calibrated_ml_probability` when `meta_labeler_probability` is provided (range 0.5–1.0×); when absent, scale 1.0. Remove the `regime_probability × 100` anchor and the `ml_ratio` (0.25–1.5) construct.
   - `composite_action == "Hold"` → `suggested_pct = 0.0` (no new size on Hold). Sell paths unchanged (0.0).
   - Kelly: compute it with the **calibrated ML probability** as win-prob (never regime probability) and report it in the rationale as an advisory diagnostic; apply it as a cap only when `meta_labeler_probability` is present. If absent, no Kelly term.
   - Keep portfolio-concentration adjustments (regime exposure, sector, correlation) exactly as they are, applied after the anchor.
   - `sizing_rationale` must name the anchor ("risk-budget: …") and each modifier applied.
2. **Veto redesign** (`src/regime/meta_labeler.py`):
   - Replace the fixed 0.50 cutoff in `analyze` with a base-rate-relative rule: veto when `calibrated_prob < positive_rate_train − veto_margin`, confirm when `calibrated_prob ≥ positive_rate_train + confirm_margin`. New `MetaLabelerConfig` fields `veto_margin: float = 0.10`, `confirm_margin: float = 0.15`. `positive_rate_train` comes from `_training_metrics`; if unavailable (loaded model without metrics), fall back to the current 0.50/0.65 absolute thresholds and set `details["threshold_mode"]="absolute_fallback"`, else `"base_rate_relative"`. Persist `positive_rate_train` into the model sidecar metadata at `save_model` time so loaded models don't lose it.
   - Add setting `meta_labeler_veto_mode` (read where the veto is consumed in agent flows): `"gate"` (current behavior — veto blocks entry) or `"size_only"` (veto never blocks; the calibrated probability acts only through `compute_position_size`). Default `"gate"` for now.
3. **A/B evidence**: extend the `pipeline-backtest --meta-labeler-ab` CLI to accept `--veto-mode {gate,size_only}` and `--tickers` (multi-ticker loop with a summary table). Rerun against `meta_labeler_v3` for at least 5 tickers, 5y, with the OOS split, in all three configurations (no-veto / gate / size_only).

### Tests (`tests/regime/test_sizing_rework.py`)
- Risk-budget anchor: portfolio 100k, `max_risk_pct=2`, ATR and price fixed → hand-computed `suggested_dollars`; ML probability 0.5 → 0.75× anchor; 1.0 → 1.0×; absent → 1.0×.
- Hold → 0.0; Sell → 0.0.
- Kelly only present with ML probability; uses it, not regime probability (assert via rationale/fields on a case where they'd differ).
- Concentration adjustments still applied (reuse an existing case from `tests/regime/test_signals.py` if present).
- Veto thresholds: model with `positive_rate_train=0.42`, margins default → veto below 0.32, confirm at ≥0.57; loaded model without metrics → absolute fallback flagged.
- `size_only` mode: entry not blocked, probability passed through (unit-level, monkeypatched flow).

### Acceptance
All tests green; basket A/B table in the PR (return/Sharpe/max-DD/trade-count per ticker per mode, IS and OOS); explicit recommendation backed by the OOS rows on whether `meta_labeler_veto_mode` default should become `size_only`. Do not change the default yourself.

---

## Task 4b — Meta-labeler skill gate + feature enrichment (must land before Task 5)

### Problem — grounded in v4 evidence
The retrained `meta_labeler_v4` (1,756 samples, managed labels, non-degenerate weights) has **no predictive skill**: aggregate OOF ROC-AUC **0.468** (per-fold 0.43/0.48/0.53/0.48/0.64), accuracy 0.486 vs base rate 0.525. Calibration "improved" Brier to 0.2482 — but a constant prediction of the base rate scores 0.2494, so the calibrator learned to predict the base rate for everything. Consequences observed in the corrected basket A/B: `gate` vetoes nothing (no calibrated probability falls below `0.525 − 0.10`), and `size_only` is a uniform ~0.76× exposure haircut (drawdown −, return −, zero discrimination). Two failures to fix: (a) nothing prevents a skill-less model from influencing decisions; (b) the 8 `META_FEATURES` describe the market environment, not the trade — the signal-specific context the system already computes never reaches the model.

### Deliverables

1. **Skill gate** (`src/regime/meta_labeler.py`):
   - Persist aggregate OOF `roc_auc` into the metadata sidecar at `save_model` (it is already in `_training_metrics`; make sure loaded models recover it).
   - In `analyze()`: when `meta_labeler_skill_gate_enabled` (setting, default **true**) and the loaded model's aggregate OOF `roc_auc` is known and `< meta_labeler_min_oof_auc` (setting, default **0.55**), return the passthrough/neutral result with `details["status"]="insufficient_model_skill"` and `details["oof_roc_auc"]` populated. The model must not influence veto or sizing in either mode.
   - **Missing metrics keep current behavior** (absolute-fallback thresholds, no skill gate) — older saved models without AUC are not retroactively blocked; flag them `details["skill_gate"]="unknown_skill"`.
   - Setting off → behavior identical to today.
2. **Feature enrichment** — the real model task:
   - Extend the feature set with per-trade signal context: `composite_strength`, `transition_risk`, `regime_days`, `p_bull_day5` (and `p_bear_day5`), `risk_reward_ratio`, ATR-normalized distance to stop and to target, technical state (e.g. RSI bucket / MACD-histogram sign from `compute_technicals`), and signal-quality score where available.
   - **Leakage discipline is the hard requirement:** every feature must be computable identically per historical bar at labeling time and per live signal at decision time. For per-bar forward-curve probabilities, extend the walk-forward loop in `hmm_engine.fit_regime_model` (behind an optional flag) to record `p_bull_day5`/`p_bear_day5` per bar from the *then-current* transition matrix and posterior — never from the final model. Any feature that cannot be reconstructed per-bar without future information is excluded.
   - **Feature-set versioning:** persist the feature list + a `feature_set_version` in the metadata sidecar; `analyze()` must build the vector from the loaded model's feature list (not the module constant) so old and new models coexist. Degraded-feature passthrough threshold scales with the feature count (keep "more than 25% missing" semantics).
3. **A/B evidence upgrade** (`cli.py`): the `--meta-labeler-ab` output (table and JSON) must additionally report, per run: aggregate OOF `roc_auc`, base-rate benchmark Brier (`p̂(1−p̂)`), calibration lift vs that constant benchmark (`benchmark_brier − calibrated_brier`), and calibrated-probability dispersion (std and IQR over the run's analyzed signals). A reviewer must be able to see at a glance whether the model has skill and whether its probabilities actually vary.

### Tests (`tests/regime/test_meta_labeler_skill_gate.py`)
- v4-like metrics (`roc_auc=0.468`) self-disqualify: `analyze` passthrough with `insufficient_model_skill`, in both veto modes (probability must not reach sizing).
- `roc_auc=0.60` → normal behavior; gate setting off → normal behavior even at 0.468; missing metrics → current fallback path, flagged `unknown_skill`.
- Metadata round-trip: AUC and feature list survive save → load.
- Feature-set versioning: a model saved with the old 8-feature list still scores correctly after the module constant grows; mismatched vector lengths never reach XGBoost.
- Per-bar forward probabilities: walk-forward recording uses the refit-time matrix (construct a case where the final matrix differs and assert the recorded value matches the earlier one).
- Train/inference feature parity: for a fixed bar, the training-frame row and `extract_meta_features` produce identical vectors for the enriched set.
- A/B output contains the four new evidence fields; dispersion ≈ 0 flags a degenerate model on a synthetic constant-probability engine.

### Acceptance
Tests green; defaults: skill gate **on** at 0.55 (this is a deliberate live-behavior change — it disables influence of the current skill-less v4, which is the point; note it prominently in the PR). Retrain v5 with enriched features on the same basket; report OOF AUC per fold, base-rate Brier benchmark, calibration lift, and probability dispersion. Only if v5 clears the skill bar, rerun the 5-ticker A/B in both modes with the upgraded evidence output. No change to the `gate` default either way.

---

## Task 5 — Empirical thresholds: durations, transition-risk gates, Neutral tilt, composite adjustments (review 1.3 + 2.3 + 3.2)

### Problem
`signals.signal_from_forward_curve` gates Strong Buy/Buy on `transition_risk` (= 1 − p_stay) thresholds 0.05/0.15 — fitted HMM self-transitions cluster near 1.0, so these are near-degenerate, and `expected_duration = 1/(1−p_stay)` is hyper-sensitive exactly there. The Neutral→Buy tilt fires at `p_bull_day5 > 0.40` (Bull need not be modal). `build_composite_signal`'s ±0.15/−0.20 technical adjustments and override rules are untested degrees of freedom. All thresholds live in `config.SignalThresholds`.

### Deliverables

1. **Empirical regime durations** (`src/regime/hmm_engine.py`, additive): from the walk-forward decode (`RegimeResult.price_frame["regime"]`), compute the distribution of completed regime-spell lengths per label; expose `empirical_duration_quantiles: dict[str, dict[str, float]]` (p25/p50/p75 per label) on `RegimeResult`. Add `signal_from_forward_curve` support (new optional parameter) to use the median empirical duration for the current label as `expected_holding_days` instead of the matrix-implied value, behind `SignalThresholds.use_empirical_durations: bool = False`.
2. **Forward-curve gates**: add optional threshold fields gating Strong Buy/Buy on `p_bull_day5` (already computed) instead of raw transition risk — e.g. `strong_buy_min_p_bull_day5`, `buy_min_p_bull_day5` — used when `use_forward_curve_gates: bool = False` is on. Modal-Bull requirement for the Neutral tilt: `neutral_tilt_requires_modal: bool = False` → tilt fires only if `p_bull_day5 > max(p_neutral_day5, p_bear_day5)` as well as the threshold.
3. **Sweep harness** (`src/regime/threshold_sweep.py` + CLI subcommand `threshold-sweep`): given tickers, period, OOS date, and a parameter grid (JSON file or inline defaults), run the pipeline backtest per combination per ticker via `signal_provider` injection or config wiring (the backtest already replays `signal_from_forward_curve` — add the thresholds object to `PipelineBacktestConfig` and thread it through `_ProductionSignalProvider`). Output a tidy CSV/JSON: one row per (ticker, combo) with IS and OOS return/Sharpe/max-DD/trade-count, plus an aggregate row per combo. Include an ablation flag isolating the composite technical adjustments (`composite_adjustments_enabled: bool` threaded into `build_composite_signal` — when off, skip the ±0.15/−0.20 and override-to-Hold rules) and one isolating Neutral-tilt entries (count and P&L of trades whose entry signal came from the tilt path — tag the signal source).
4. **No default changes in code.** The PR ships capabilities, settings/flags, and the sweep report. Propose new defaults in the PR description only, supported by aggregate OOS rows across ≥5 tickers.

### Tests (`tests/regime/test_threshold_sweep.py`)
- Empirical durations: hand-built decode sequence (e.g. Bull×5, Bear×3, Bull×7) → exact quantiles; incomplete trailing spell excluded.
- Forward-curve gates and modal-tilt flags change actions on constructed forward curves exactly as specified; flags off → behavior identical to today (regression-pin one current case from `tests/regime/test_signals.py`).
- Composite ablation off/on produces the documented differences on a fixed input.
- Sweep harness on synthetic frames with a stub signal provider: grid of 2×2 produces 4 combo rows + aggregates; JSON/CSV round-trip; deterministic.

### Acceptance
Tests green; defaults bit-identical behavior (regression pins); sweep report for ≥5 real tickers attached to the PR with a written recommendation per flag.

---

## Task 6 — HMM robustness (review 3.1)

### Problem
Single fixed-seed (`random_state=7`) GaussianHMM with diagonal covariance; EM is initialization-sensitive. The regime posterior driving decisions is uncalibrated (`compute_unified_confidence` has a calibrator hook, but nothing in the decision path uses calibrated values). `macro_weighting` multiplies two standardized columns by 1.5 ad hoc.

### Deliverables

1. **Multi-seed stability** (`hmm_engine.py`): `n_seeds: int = 1` parameter on `fit_regime_model`; when >1, fit with `n_seeds` seeds (`random_state + k`), keep the best-log-likelihood model, and compute `seed_agreement`: fraction of the last `refit_step` bars where all seeds' decoded canonical labels agree. Expose on `RegimeResult`. Below `seed_agreement_min` (new setting, default 0.8), set a `regime_ambiguous` flag — and in `paper_trading.generate_buy_plans`, skip entries for ambiguous tickers (log + count like other gates). Keep `n_seeds=1` as default; enable via setting `hmm_n_seeds`.
2. **Regime-probability calibration**: reuse `probability_calibration.ProbabilityCalibrator`. Build a small fitter (`src/regime/regime_calibration.py`) that, from a walk-forward decode, pairs each bar's regime posterior with a binary outcome ("label persisted N bars forward" or "5d return sign consistent with label" — pick ONE, document it) and fits per-label isotonic calibrators; persist as JSON under `HMM_DATA_DIR/models/regime_calibrator_{label}.json`. Wire into `compute_unified_confidence` (it already accepts a calibrator) AND — behind setting `regime_probability_calibrated` (default off) — into the `regime_probability` used by `signal_from_forward_curve` strength and `compute_position_size` callers in the routes.
3. **Covariance experiment, evidence only**: add `covariance_type` parameter to `fit_regime_model` (default `"diag"`, unchanged). Extend the threshold-sweep CLI (Task 5) to accept `--hmm-covariance` so full-vs-diag can be A/B'd through the backtest. Report results in the PR; do not change the default.
4. **Macro weighting**: replace the bare `×1.5` literals with a `macro_weight: float = 1.5` parameter (default preserves behavior); include in sweep grid support.

### Tests (`tests/regime/test_hmm_robustness.py`)
- Multi-seed: synthetic data with well-separated states → agreement ≈ 1.0 and identical labels regardless of seed count; pathological near-degenerate data (two states with same mean/var) → agreement below threshold and `regime_ambiguous` set. Seeds deterministic.
- Ambiguity gate: `generate_buy_plans` skips a ticker whose snapshot row carries `regime_ambiguous=True` (follow the existing gate-test patterns in `tests/regime/test_beta_target_deployment.py`).
- Regime calibrator: synthetic miscalibrated posteriors → Brier improves; JSON round-trip; per-label files.
- `covariance_type` and `macro_weight` parameters: defaults produce bit-identical `RegimeResult` on a fixed frame (regression pin).

### Acceptance
Tests green; defaults unchanged and pinned; PR includes a 3-ticker backtest comparison for `n_seeds=3` vs 1 and full-vs-diag covariance, with a recommendation.

---

## Task 7 — Engineering hygiene + LLM attribution (review 4.x + 3.3)

Do these as one PR of mostly mechanical changes. **No behavior changes except where stated.**

1. **Split `persistence.py` (~3,900 lines)** into a package `src/regime/persistence/` with modules by domain (`plans.py`, `positions.py`, `portfolios.py`, `snapshots.py`, `settings.py`, `audit.py`, `signals_cache.py`, `core.py` for connection/migration helpers). `src/regime/persistence/__init__.py` re-exports **everything** currently importable from `src.regime.persistence` so no import site changes. Pure mechanical move — `git diff` should show moves, not edits. Same treatment for `paper_trading.py` (~2,400 lines) → package with `planning.py` (buy/holdings/exit plan generation), `execution.py` (approve/execute/cancel/kill-switch), `performance.py` (snapshots, benchmarks, metrics), `sizing.py`, `core.py`; `__init__.py` re-exports all current names. Run the full regression set after each split before proceeding.
2. **Silent-failure telemetry**: add `src/regime/decision_health.py` with `record_fallback(component: str, detail: str)` writing a counter (per component, per day) via the settings/audit persistence. Instrument the decision-path swallows found in review: `agent_policy.setting_float`, `paper_trading._lookup_atr`/`_lookup_beta` (exception → None), earnings lookup in `agent_policy.earnings_blackout_status`, calibrator application in `signals.compute_unified_confidence`. Threshold alert: if any component records > N (setting, default 10) fallbacks in a day, `save_alert(severity="warning")` once per day per component (dedupe like `thesis_monitor` does).
3. **Stale-signal window**: change buy-side snapshot freshness from `max_age_days=7` to a setting `entry_signal_max_age_days` (default **3**) in `generate_buy_plans`. This is the one intentional behavior change; note it prominently in the PR.
4. **LLM verdict-outcome attribution** (review 3.3): when a trade plan is created with an LLM verdict attached (`llm_used`, `agent_trace`/frontier panel fields on plans), persist `llm_verdict` and `llm_confidence` columns (additive, `_PAPER_TRADE_PLAN_COLUMNS` pattern). In `record_trade_outcome`, join the closed position back to its entry plan and write an `llm_attribution` audit event with verdict, confidence, and realized net P&L. Add a small read API `get_llm_attribution_summary(days)` returning per-verdict trade count, win rate, and avg net P&L — and surface it on the existing agent dashboard payload (`agent_dashboard.py`) as a new section.
5. **Config consolidation, minimal version**: create `src/regime/decision_constants.py` housing the scattered literals that govern decisions and aren't already in `SignalThresholds`/configs (composite ±0.15/−0.20, Neutral-tilt 0.40 if not moved by Task 5, veto margins if not in config, `DEFAULT_EXIT_TIME_STOP_DAYS`, `DEFAULT_NEUTRAL_REDUCE_FRACTION`, ratchet activation 1.0×ATR). Import them at use sites; values unchanged. Stamp `decision_constants_version` (a hash of the module's values) onto created trade plans (additive column).

### Tests
- Split: no new tests needed beyond the full existing suite passing; add one import-compatibility test asserting every public name from a pinned list is importable from both old paths.
- `decision_health`: counters increment, daily alert fires once, dedupes.
- Stale window: snapshot 4 days old rejected at default, accepted with setting=7.
- LLM attribution: plan→outcome→audit event round-trip with hand-built rows; summary math.
- Constants: plans carry `decision_constants_version`; hash changes when a constant changes (test with monkeypatch).

### Acceptance
Full regression set green (route file isolated, sprint57 isolated, as documented); `git diff --stat` shows the splits are moves; PR notes the stale-window default change and shows a screenshot or JSON sample of the LLM attribution dashboard section.

---

## Definition of done (overall)

Each task: its own PR, tests green under the documented run order, evidence tables in the description, and **no live-default changes without basket OOS evidence** (Tasks 4–6 propose; the human decides). After Task 7, every open item from `REGIME_REVIEW.md` is either closed or explicitly settings-gated with evidence attached.
