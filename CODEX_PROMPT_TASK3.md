# Codex Prompt — Task 3: Wire Managed-Exit Labels into Production Retraining

You are working in the `Investor/` Python project. Tasks 1 (event-driven pipeline backtest) and 2 (managed-exit labeling, uniqueness weights, purged CV, isotonic calibration) are complete and merged. **Task 2's machinery exists but production never uses it** — this task closes that gap. Do not modify `pipeline_backtest.py` or the verified labeling semantics in `triple_barrier.py`'s `_managed_label_single_bar`.

## Problem

The production retraining path (`src/app/routes/regime.py` lines ~4006, ~4127, and ~4295; runtime wiring at ~342 and ~506–507) still builds training data via `build_multi_ticker_labeled_frame` → old `build_labeled_frame` → static triple-barrier labels (`apply_triple_barrier_labels`). Consequences, verified:

1. The meta-labeler retrains on the **old, misaligned** labels — the Task 2 realignment is dead code in production.
2. Old labels carry no `label_end_idx`, so `MetaLabelerEngine._prepare_training_data` falls back to one-bar lifespans: uniqueness weights silently become all-1.0 and CV purging degenerates to no purge.
3. Even if the multi-ticker builder were naively switched to managed labels, the coordinates would be wrong: `label_end_idx` is **ticker-local bar position**, but the builder drops NaN-outcome rows and `reset_index(drop=True)`s the concat, while `_prepare_training_data` assigns `_label_start_idx = arange(len(frame))` over the filtered concatenated frame. Start and end would live in different coordinate systems, and `_purged_walk_forward_splits` folds over concatenated per-ticker blocks are not chronological — they would mix eras across tickers.

## Design decision (follow this, do not improvise)

**Move purge/uniqueness coordinates from positions to dates.** Dates are ticker-independent and survive filtering, concatenation, and reindexing. Positional `label_end_idx` stays for backward compatibility and for the single-frame fast path, but the multi-ticker production path runs on dates.

## Deliverables

### 1. Date-stamped managed labels — `src/regime/triple_barrier.py` (additive)

- `apply_managed_exit_labels` already knows the resolving bar `j`; additionally emit:
  - `label_entry_date` — the entry bar's timestamp (`pd.Timestamp(index[idx])`)
  - `label_end_date` — the resolving bar's timestamp (NaT for unresolved/non-Bull bars)
- Add `build_managed_labeled_frame(ticker, regime_result, config: ManagedExitConfig)` — managed analogue of `build_labeled_frame`: takes `regime_result.price_frame`, calls `apply_managed_exit_labels` with `regime_col="regime"`, `close_col="price"`.
- Add `build_multi_ticker_managed_frame(ticker_regime_pairs, config: ManagedExitConfig | None = None)`:
  - per ticker: `build_managed_labeled_frame`, stamp `ticker` column;
  - concat with `ignore_index=True`, drop NaN `barrier_outcome` rows — this is safe now because uniqueness/purge will use the date columns, not positions;
  - guarantee output columns: everything `build_multi_ticker_labeled_frame` emits **plus** `label_end_idx`, `label_entry_date`, `label_end_date`.
- Leave `build_multi_ticker_labeled_frame` and `apply_triple_barrier_labels` untouched.

### 2. Date-aware uniqueness weights — `src/regime/triple_barrier.py`

Extend `sample_uniqueness_weights`: when `label_entry_date`/`label_end_date` columns are present and non-null, compute concurrency on a per-ticker daily timeline between entry and end dates (inclusive); weight = mean over the label's lifespan of 1/concurrent-count, computed **within the same ticker** (labels on different tickers are not redundant copies of each other — do not count cross-ticker concurrency). Fall back to the existing positional logic when date columns are absent. Existing positional tests must keep passing.

### 3. Date-aware purged walk-forward CV — `src/regime/meta_labeler.py`

In `_prepare_training_data`: if `label_entry_date`/`label_end_date` are present, parse them (`pd.to_datetime`, coerce) and keep them on the prepared frame; sort the prepared frame **chronologically by `label_entry_date`** (stable sort; ties across tickers keep input order) before splitting — this is what makes multi-ticker folds chronological.

In `_purged_walk_forward_splits`: when date columns are present, use date semantics:
- folds: K contiguous chronological blocks of the date-sorted samples (same initial-train carve-out as now);
- `test_start_date` = min `label_entry_date` in the fold;
- train mask: `label_entry_date < test_start_date − embargo_days` AND `label_end_date < test_start_date`;
- `embargo_days: int = 30` new config field (calendar days; the existing `embargo_bars` stays for the positional path);
- fold metadata gains `test_start_date`/`test_end_date` (ISO strings).
Positional path stays as-is for frames without dates.

### 4. Rewire production training — `src/app/routes/regime.py`

- Register `build_managed_labeled_frame` and `build_multi_ticker_managed_frame` in the route runtime (the dict around lines 506–507; import near line 342).
- At each training call site (~4006, ~4127, ~4295): select the builder via a setting `meta_labeler_label_mode` (`get_setting`, default **"managed"**; `"legacy"` selects the old builders). Follow the existing settings pattern used elsewhere in the routes.
- Stamp the result: training response payloads and `_training_metrics` must include `label_mode` and, for managed mode, the `ManagedExitConfig` as a dict — parameter attribution, same rule as the backtest config stamping.
- Trained-model versioning is unchanged (next version + calibrator sidecar already handled by Task 2's `save_model`).

### 5. Degeneracy guard — `src/regime/meta_labeler.py`

In `train`: after computing sample weights, if `label_end_idx`/`label_end_date` were absent or fell back to one-bar lifespans for **more than half** the samples, log a warning and set `_training_metrics["weights_degenerate"] = True`. This makes the silent-degradation failure mode we found impossible to miss.

## Tests — extend `tests/regime/test_meta_labeler_alignment.py` (or a new `test_meta_labeler_production_wiring.py`)

All offline, deterministic, `tmp_path` + `HMM_DATA_DIR` monkeypatch patterns as in the existing alignment tests. Required:

1. `build_multi_ticker_managed_frame` on two synthetic tickers: output has `ticker`, `label_entry_date`, `label_end_date`; dates match the resolving bars of each ticker's own frame (hand-check one label per ticker).
2. Date-aware uniqueness: two overlapping labels on ticker A and one disjoint label on ticker B → A's weights < 1.0, B's weight == 1.0 (cross-ticker concurrency NOT counted).
3. Date-aware purged CV on a two-ticker frame with interleaved dates: folds are chronological by `label_entry_date` across tickers; no training sample has `label_end_date >= test_start_date`; embargo respected in calendar days.
4. Positional fallback: frames without date columns reproduce the existing behavior (reuse/extend the current purge test).
5. Route wiring: monkeypatch the runtime dict, call the training endpoint (see `tests/test_regime_route.py` for the TestClient pattern), assert the managed builder is invoked when `meta_labeler_label_mode` is unset/`"managed"` and the legacy builder when `"legacy"`; assert the response payload carries `label_mode` and the managed config dict.
6. Degeneracy guard: training on a frame without `label_end_idx` sets `weights_degenerate=True` and logs a warning; training on a managed frame does not.

## Environment & constraints

- Same as Tasks 1–2: pinned `scikit-learn==1.8.0` / `xgboost==2.1.4`, no new deps, no pickle, no network in tests, never touch `.env` or hard-code `/Volumes/...` paths, additive signatures only (`MetaLabelerConfig` gains `embargo_days` with a default; nothing else changes shape).
- Run order: new tests → `tests/regime/test_meta_labeler_alignment.py tests/regime/test_pipeline_backtest.py` → `tests/test_regime_route.py` (run this file **in isolation**; it has a known order-dependent flake when run after other suites) → `make test`.
- Known issue: the full `tests/regime/test_sprint*.py` sweep can hang around 62% on a network-touching test. If it hangs, run the sprint files in halves to bisect and report which file hangs — do not "fix" it by skipping silently.
- Out of scope: `pipeline_backtest.py`, exit-ladder semantics, sizing, HMM, UI templates, live/IBKR paths, and any change to the legacy label functions.

## Definition of done

1. All new + existing alignment/pipeline/route tests pass; `make test` green (modulo the documented hang, reported per above); new tests clean under `-W error::DeprecationWarning`.
2. A local retrain through the route (real data, network OK locally) produces a new model version whose training metrics show: `label_mode="managed"`, `weights_degenerate` absent/False, non-trivial sample weights (mean < 1.0), per-fold dates in metadata, and pre/post-calibration Brier.
3. Rerun `python -m src.regime.cli pipeline-backtest NVDA --period 5y --oos-start 2025-01-01 --meta-labeler-ab` against the **newly trained** model and include the before/after A/B delta table in the PR description (prior baseline for comparison: old-label model gave return −0.40pp, Sharpe −0.0043, trades +5 vs no-veto).
4. PR description lists: files changed per deliverable, the label-mode setting name and default, and the CV fold date ranges from the real retrain.
