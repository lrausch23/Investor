# Codex Prompt — Task 2: Meta-Labeler Realignment + Probability Calibration

You are working in the `Investor/` Python project (FastAPI + pandas + hmmlearn + xgboost). Implement the changes below exactly. Task 1 (the event-driven pipeline backtest) is already complete and merged — you will build on it, not modify it.

## Problem

The XGBoost meta-labeler (`src/regime/meta_labeler.py`) predicts P(trade success), gates entries (veto < 0.50, confirm ≥ 0.65), and scales position size. It is trained on triple-barrier labels from `src/regime/triple_barrier.py`: static 2×ATR target, static 2×ATR stop, 21-trading-bar vertical barrier, timeout = loss.

Live trades are no longer managed that way. Production (`src/regime/paper_trading.py`) uses: profit target, ATR trailing-stop ratchet, calendar-day time stop, regime-flip exits, and Neutral partial reduces. So the labeler's probabilities describe a game that is no longer played. They are also uncalibrated (raw XGBoost output compared to fixed thresholds and fed to Kelly sizing), validated on a single 80/20 split over heavily overlapping samples, and `extract_meta_features` silently zero-fills missing features.

## Reference implementation — read these first

- `src/regime/pipeline_backtest.py` → `_manage_position` is the **canonical exit ladder**. Your labeling simulation must reproduce its semantics exactly:
  1. Each bar's **low is tested against the stop as of the prior close**. The trailing ratchet is computed from the day's **close** (never the high) via `paper_trading.trailing_stop_level` and takes effect the **next** bar.
  2. Stop touch beats target touch on the same bar (conservative).
  3. Time stop counts **calendar days** between bar dates (matches `paper_trading._holding_days`), not trading bars.
  4. Exit priority: stop/trailing → target → time → regime.
- `src/regime/paper_trading.py` → `trailing_stop_level(entry_price, current_price, atr_14, existing_stop, atr_multiplier, activation_atr)` is the single source of truth for ratchet math (activation at entry + 1×ATR, distance 2×ATR, never moves down). **Import and call it — do not reimplement.**
- `src/regime/triple_barrier.py` → existing `apply_triple_barrier_labels` (keep untouched, backward compatible), `compute_atr`.
- `src/regime/meta_labeler.py` → `MetaLabelerEngine`, `META_FEATURES`, model save/load versioning (`meta_labeler_v{N}.json` under `HMM_DATA_DIR/models`).

## Deliverables

### 1. Management-aware labeling — `src/regime/triple_barrier.py` (additive)

Add:

```python
@dataclass(frozen=True)
class ManagedExitConfig:
    profit_target_atr_mult: float = 2.0
    stop_atr_mult: float = 2.0
    trailing_atr_mult: float = 2.0
    trailing_activation_atr: float = 1.0
    time_stop_days: int = 21          # CALENDAR days
    cost_bps: float = 20.0            # round-trip, used at timeout labeling
    min_atr: float = 0.01

def apply_managed_exit_labels(price_frame, regime_col="regime", close_col="price",
                              high_col="high", low_col="low",
                              config: ManagedExitConfig = ...) -> pd.DataFrame
```

Behavior: for each bar whose regime is `Bull` (long-only — production never opens shorts; Neutral/Bear bars get NaN labels), simulate entry at that bar's close and walk forward applying the exit ladder with the four semantics from the reference section. Outcomes:

- target touch → `barrier_outcome=1.0`, `barrier_type="target"`
- static stop touch (stop ≤ entry) → `0.0`, `"stop"`
- trailing stop touch (stop > entry) → `0.0` if exit below entry net of costs else `1.0`, `"trailing"` (a trailing exit above entry+costs is a win — it locked a gain)
- calendar timeout → label by sign of net return after `cost_bps`: `1.0`/`"time_win"` or `0.0`/`"time_loss"`

Output columns: same contract as `apply_triple_barrier_labels` (`barrier_outcome`, `barrier_type`, `barrier_days` — calendar days, `barrier_entry`, `barrier_target`, `barrier_stop`) **plus** `label_end_idx` (integer positional index of the resolving bar; for unresolved tail bars set NaN outcome and exclude from training).

### 2. Overlap handling — same module

```python
def sample_uniqueness_weights(labeled_frame: pd.DataFrame) -> pd.Series
```

Average-uniqueness weights (López de Prado, AFML ch. 4): for each labeled bar, over its lifespan `[idx, label_end_idx]`, uniqueness_t = 1 / (number of concurrent labels alive at t); weight = mean of uniqueness over the lifespan. Non-overlapping labels → weight 1.0. Pass these as `sample_weight` to XGBoost `fit`.

### 3. Purged walk-forward CV — `src/regime/meta_labeler.py`

Replace the single 80/20 split in `MetaLabelerEngine.train` with K chronological folds (add `n_folds: int = 5`, `embargo_bars: int = 21` to `MetaLabelerConfig`; keep all existing defaults). For each fold: train on samples strictly before the fold, **purging** any training sample whose `label_end_idx` falls inside the test fold, plus an embargo of `embargo_bars` before the fold start. Train requires the labeled frame to carry `label_end_idx` and optional sample weights. Report per-fold and aggregate: accuracy, precision, recall, F1, ROC-AUC, Brier score; store in `_training_metrics`. Final model = trained on all data (with weights) after CV metrics are computed.

### 4. Probability calibration — new module `src/regime/probability_calibration.py`

- Fit `sklearn.isotonic.IsotonicRegression(out_of_bounds="clip")` on **out-of-fold** predictions pooled from the CV in (3) — never on in-fold training predictions.
- Serialize as **JSON, not pickle**: store the fitted `X_thresholds_`/`y_thresholds_` arrays; reconstruct with `np.interp` on load. Provide `save_calibrator(path)`, `load_calibrator(path)`, `calibrate(probabilities) -> np.ndarray`.
- Persist next to the model version: `meta_labeler_v{N}_calibrator.json` (extend `save_model`/`load_model` in `meta_labeler.py` to write/read the sibling file when a calibrator is attached).
- Report Brier score pre- vs post-calibration on the pooled OOF set; log a warning (do not fail) if calibration worsens Brier.

### 5. Wire calibrated probability into decisions — `meta_labeler.py`

`MetaLabelerEngine.analyze` returns the **calibrated** probability as `confidence` when a calibrator is loaded; raw model output goes in `details["raw_probability"]`, plus `details["calibrated"]: bool`. The existing 0.50 veto / 0.65 confirm thresholds and `signals.compute_position_size(meta_labeler_probability=...)` need **no changes** — they receive calibrated values transparently.

### 6. Fail loudly on degraded features — `meta_labeler.py`

In `analyze`: if more than 2 of the 8 `META_FEATURES` are missing/None in the input dict (count before zero-filling), return the not-trained passthrough result with `details["status"]="degraded_features"` and `logger.warning`. Never score a silently zero-filled vector.

### 7. A/B validation hook — `src/regime/cli.py`

Add `--meta-labeler-ab` flag to the existing `pipeline-backtest` subcommand: when set, run the backtest twice — once with a `signal_provider` wrapper that applies the meta-labeler veto (calibrated prob < 0.50 → suppress Buy), once without — and print a compact delta table (total_return, sharpe_ratio, max_drawdown, trade_count, exit_type_counts) for both runs plus the diff. Reuse `run_pipeline_backtest`; do not duplicate its logic.

## Tests — new file `tests/regime/test_meta_labeler_alignment.py`

All offline, deterministic, no network, no hard-coded absolute paths (use `tmp_path` + `HMM_DATA_DIR` monkeypatch — see `tests/regime/test_pipeline_backtest.py` and `conftest.py` for the established patterns). Required cases:

1. Managed-exit labels on hand-built OHLC paths: target win; trailing exit above entry+costs → win, below → loss; static-stop loss; timeout positive → `time_win`; timeout negative → `time_loss`; `cost_bps` flips a marginal timeout from win to loss.
2. **Parity with the reference ladder**: on a shared synthetic path, `apply_managed_exit_labels` outcomes/types match `pipeline_backtest._manage_position` exit types (build a tiny harness that feeds the same bars through both).
3. Calendar-day semantics: a Friday entry with `time_stop_days=3` resolves on Monday (3 calendar days, 1 trading bar).
4. Same-bar conservatism: a wide bar whose high would ratchet the stop above its own low does NOT exit that bar.
5. Uniqueness weights: disjoint labels → all 1.0; n fully-overlapping labels → 1/n.
6. Purged CV: assert no training sample's `label_end_idx` lands inside its test fold; embargo respected; folds chronological.
7. Calibrator: fit on synthetic miscalibrated probs, save → JSON file exists and contains no pickle; load → identical predictions; Brier improves on a deliberately overconfident synthetic set.
8. `analyze` returns calibrated confidence with calibrator attached; raw in `details`; degraded-features passthrough triggers at 3+ missing features and NOT at 2.

## Environment & constraints

- Run: `cd Investor && .venv/bin/pytest tests/regime/test_meta_labeler_alignment.py -q`; then the regression set: `tests/regime/test_pipeline_backtest.py tests/regime/test_sprint*.py tests/regime/test_beta_target_deployment.py`; finally `make test`.
- Pinned deps: `scikit-learn==1.8.0`, `xgboost==2.1.4`. No new dependencies. No pickle for any persisted artifact.
- Do not modify `.env`. Do not hard-code `/Volumes/...` paths. Schema/DB untouched (this task has no persistence changes).
- Keep `apply_triple_barrier_labels` and all existing public signatures backward compatible; `MetaLabelerConfig` gains fields with defaults only.
- Style: `from __future__ import annotations`, dataclasses, type hints, module logger — match the surrounding code.
- Out of scope: sizing rework, HMM changes, `paper_trading.py` refactors, `backtest.py` (legacy), UI, live/IBKR paths. Do not touch `pipeline_backtest.py` except the CLI flag in `cli.py`.

## Definition of done

1. All new tests pass; full `make test` green; new tests clean under `-W error::DeprecationWarning`.
2. Parity test (#2) proves labeling matches the verified backtest exit ladder.
3. `python -m src.regime.cli pipeline-backtest NVDA --period 5y --oos-start 2025-01-01 --meta-labeler-ab` runs end-to-end locally (network OK locally) and prints the A/B delta table.
4. PR description: what changed per deliverable, CV fold metrics, pre/post-calibration Brier, and the A/B delta table for at least one real ticker.
