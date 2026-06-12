# Regime Trading Agent — Objective Review

Scope: `src/regime/` decision pipeline — HMM engine → forward-curve signals → composite → quality/risk gates → sizing → plan generation → execution. Reviewed June 2026.

**Overall assessment:** The architecture is unusually mature for a personal project — walk-forward HMM fitting, a meta-labeler trained on triple-barrier outcomes, layered gates (signal quality, hurdle rate, anti-churn, earnings blackout, VIX freeze, drawdown pause), tax-aware overrides, and audit trails. The weaknesses are not in plumbing; they are in *statistical coherence*: several components are trained or validated under assumptions that don't match how trades are actually managed, and the exit side is much weaker than the entry side.

---

## Priority 1 — Issues that directly distort buy/sell/hold decisions

### 1.1 Exits are stop-or-regime-flip only; entries are over-engineered by comparison
`generate_exit_plans` (paper_trading.py:940) triggers a sell on exactly three conditions: stop price hit, cached regime = Bear / composite Sell, or fallback screen flips to Bear. There is **no profit-target exit, no trailing stop, no time-based exit** — confirmed by grep, nothing updates `stop_price` after entry and nothing compares price to `exit_price`, even though exit targets are computed (`compute_price_targets`) and stored on plans.

Consequences: winners are held until the regime flips (often well after the move is over, since HMM labels lag), a Bull→Neutral transition leaves a position with only its original static stop, and your realized trade distribution will not match anything you've modeled.

**Fix:** add to the daily exit pass: (a) profit-target exit (or partial scale-out) when `current_price >= exit_price`; (b) ATR trailing stop ratchet (e.g. `stop = max(stop, price − 2×ATR)` recomputed daily); (c) time-stop at the expected holding period / 21 days, matching the triple-barrier vertical barrier; (d) treat Bull→Neutral with declining confidence trajectory as a "reduce" signal, not a non-event.

### 1.2 Meta-labeler is trained on a different game than the one it referees
The XGBoost meta-labeler learns P(success) under triple-barrier rules: 2×ATR target, 2×ATR stop, 21-day vertical barrier (triple_barrier.py). But live trades have no profit target and no time exit (see 1.1), so the probability it emits does not describe live trade outcomes. Its veto/confirm thresholds (0.50/0.65) and its use as a Kelly input in `compute_position_size` inherit this mismatch.

**Fix:** make live exit management mirror the barrier config (cleanest), or relabel training data with rules matching live management. Either way, also:

- Calibrate the probabilities (isotonic/Platt on a held-out fold). Raw XGBoost probabilities are not calibrated, and you compare them against fixed thresholds and feed them to Kelly as if they were.
- The vertical barrier labels a timeout as a loss (`barrier_outcome 0.0`) even if the trade ended profitable. Consider a third class or sign-of-return labeling at timeout.
- Overlapping labels: every bar gets a label whose outcome window overlaps its neighbors' — samples are heavily serially correlated, which inflates effective training size and test scores. Use sample uniqueness weights or purged splits (López de Prado, *Advances in Financial Machine Learning*, ch. 4–7). The current single 80/20 split with a 5-day gap is a start but one split is high-variance; use purged walk-forward CV.
- `extract_meta_features` silently defaults missing features to 0.0 — a degraded feature vector produces a confident-looking score. Fail or flag instead.

### 1.3 Transition-risk gating is nearly degenerate
`signal_from_forward_curve` gates Strong Buy on `transition_risk < 0.05`, i.e. p_stay > 0.95, and Buy on p_stay > 0.85. Fitted HMM self-transition probabilities cluster near 1.0 almost by construction (regimes persist for weeks), so these gates are likely either almost-always-on or almost-never-on per ticker, and `expected_duration = 1/(1−p_stay)` is hyper-sensitive in exactly that region (p_stay 0.95→0.99 moves duration 20d→100d). Your Strong Buy / Buy distinction may be mostly noise.

**Fix:** replace matrix-implied duration with *empirical* regime durations from the decoded history (you already compute `regime_days`; aggregate the distribution of past regime spell lengths per ticker). Gate on calibrated quantities — e.g. P(still Bull at day 5) from the forward curve, which you already compute, rather than raw p_stay thresholds. Then sweep the thresholds in backtests instead of hard-coding 0.05/0.15.

### 1.4 The backtest doesn't test the system you run
Three separate divergences in `backtest.py`:

- `training_window` is capped at 120 days vs production default 504 (hmm_engine.py:164). You're validating a different model.
- It trades only the raw composite signal at `refit_step` (21-day) intervals — no stops, no meta-labeler, no hurdle/duration/anti-churn gates, no costs, no slippage, no position sizing. The live pipeline is gate-dominated; the backtest measures something else.
- Metrics: `_returns_summary` computes `mean/std × sqrt(n_trades)` and calls it Sharpe — that's a t-statistic, not an annualized Sharpe. `compare_to_benchmark`'s "information ratio" (`alpha/|benchmark_return|`) isn't an IR. These will mislead any threshold tuning you do against them.

**Fix:** build an event-driven backtest that replays the actual `generate_daily_plans` → gates → sizing → exit path on historical data with the slippage model you already have (`slippage.py`). Until then, treat backtest numbers as signal-direction sanity checks only. Fix the Sharpe computation (annualize from time-based returns, not per-trade counts).

### 1.5 Planned trade geometry ≠ executed trade geometry
`compute_price_targets` anchors entry at the lower Bollinger band, stop at `entry − 2×ATR`, and the risk/reward ratio at that hypothetical entry. But `generate_buy_plans` buys at *current price*. If price is mid-band, the actual stop distance and R:R differ materially from what the hurdle gate and Kelly sizing were computed on.

**Fix:** recompute stop/target/R:R from the actual proposed fill price at plan creation; either that, or make buys limit-at-entry-price so the plan geometry is honored.

---

## Priority 2 — Sizing and risk logic

### 2.1 Position sizing starts from a category error
`compute_position_size` (signals.py:423) sets `base_pct = regime_probability × 100` — a 0.70 posterior implies a 70% position before caps. Regime posterior is "how sure the model is about the state," not edge or win-rate, and HMM posteriors are routinely >0.9. In practice the Kelly/ATR caps rescue you, which means the base number is doing no useful work and the binding constraint is opaque. Also: Hold still gets 25% of base — sizing a position on a Hold signal is questionable.

Similarly, Kelly is computed with regime probability (or uncalibrated ML probability) as win-prob and planned R:R as odds — both inputs biased (see 1.2, 1.5). Half-Kelly dampens but doesn't fix garbage-in.

**Fix:** size from risk, not confidence: target a fixed portfolio risk per trade (you already do this with the 2×ATR / `max_risk_pct` block — make *that* primary), then scale 0.5–1.0× by calibrated meta-labeler probability. Drop probability×100 as the anchor. Hold → 0 new size.

### 2.2 Missing data increases position size
`_risk_adjusted_quantity` (paper_trading.py:585): if ATR is unavailable it returns `max_shares_by_capital` — the *largest* possible size. Same pattern in `generate_holdings_plans`, which never risk-sizes at all (`floor(role_budget/price)`). Unknown risk should shrink size, not remove the cap.

**Fix:** on missing ATR, fall back to a conservative proxy (e.g. rolling σ×√14 from prices, or half the capital-based size) and log it.

### 2.3 Asymmetric responses to regime uncertainty
Neutral→Buy tilt triggers at `p_bull_day5 > 0.40` — Bull doesn't even need to be the most likely state. Meanwhile Bear-emerging triggers a Sell at the same 0.40. The thresholds are symmetric on paper but the costs aren't: a marginal speculative buy in a Neutral regime carries spread+tax+churn costs your hurdle gate only partially captures.

**Fix:** require Bull to be the modal state for the Neutral tilt buy, or raise the tilt threshold above 0.5; backtest the marginal contribution of Neutral-tilt entries specifically — I'd bet they're net negative after costs.

---

## Priority 3 — Model-layer improvements

### 3.1 HMM engine
The walk-forward design (refit every 21 steps, label by training-window stats only, canonical state mapping) is honest — better than most. Improvements, in order of value:

- **Probability calibration on the regime posterior.** You have `calibration.py` and a calibrator hook in `compute_unified_confidence`, but the calibrated probability isn't what drives `signal_from_forward_curve` or sizing. Wire calibrated probabilities into the actual decision path, and track Brier score of "Bull today → positive 5d return" as a standing health metric.
- **Seed sensitivity.** Single fixed `random_state=7` GaussianHMM. EM is initialization-sensitive; refit with 3–5 seeds and use the best-likelihood fit (or check label agreement across seeds as a stability score; disagreement = regime is ambiguous = stand down).
- **Diagonal covariance** with these features (return, vol, trend, vol-z, ΔVIX, Δ10y) ignores strong cross-correlations (return↔ΔVIX). Try `covariance_type="full"` with regularization; cheap experiment.
- The `macro_weighting` ×1.5 multiply on standardized columns is an ad-hoc prior; if macro matters, let a model selection criterion (BIC across feature sets) say so.
- Mixed feature semantics: VIX/10y are market-wide, the rest ticker-level — every ticker's HMM partly re-learns the same macro state. Consider one shared market-regime model (SPY/VIX) plus ticker-relative features, which also cuts compute.

### 3.2 Composite signal
The hand-tuned ±0.15/−0.20 technical-agreement adjustments and the override table in `build_composite_signal` are reasonable heuristics, but they're untested degrees of freedom. Either backtest each rule's marginal contribution or simplify. Note "Take partial profits" currently maps to Hold — with no partial-exit mechanism it's dead advice (see 1.1: implement scale-outs and this becomes real).

### 3.3 LLM layer
Keep the LLM strictly in the veto/context lane (qualitative catalyst assessment, thesis checks) with the deterministic fallbacks you already have. Don't let it originate or upsize positions; non-reproducible decisions will poison your attribution data. Log every LLM verdict alongside the eventual trade outcome so you can measure whether it adds alpha at all — after a few hundred decisions you'll know whether the API cost is justified.

---

## Priority 4 — Engineering

- **God modules:** `persistence.py` (3,811 lines) and `paper_trading.py` (2,294 lines, ~30 responsibilities) are where the next subtle bug will hide. Split persistence by domain (plans, positions, snapshots, settings, audit) and pull plan-generation vs execution vs performance out of paper_trading.
- **Silent exception swallowing:** the `try/except Exception: return default` pattern (e.g. `setting_float`, `_as_float`, earnings lookups, calibrator application) means data failures degrade into wrong-but-plausible trading behavior with only debug-level traces. For anything in the decision path, count these in monitoring and alert past a threshold — you have the alert plumbing already.
- **Sprint-named tests** (`test_sprint10.py` … `test_sprint34a.py`): coverage exists but is organized by when code was written, not what it protects. When you refactor (first bullet), regroup by behavior — `test_exit_logic.py`, `test_sizing.py` — or regressions will be hard to localize.
- **Stale signals:** buys accept snapshots up to 7 days old (`get_latest_signal_snapshot(max_age_days=7)`). The signal-quality gate checks staleness, but 7 days is generous for a regime model with daily refits; consider 2–3 days for entries.
- **Config sprawl:** thresholds live in frozen dataclasses (config.py), DB settings (`get_setting`), and hard-coded literals (0.40 tilt, 0.65 confirm, ±0.15 adjustments). Consolidate decision-relevant constants into one versioned config object that gets stamped onto every trade plan — without that you can't attribute performance changes to parameter changes.

---

## What's already good (keep)

Walk-forward HMM with leakage-conscious labeling; the gate stack (hurdle rate net of execution cost and taxes is genuinely uncommon); VIX freeze with hysteresis; drawdown pause + guardrail cooldown; LTCG override and wash-sale awareness; idempotent plan keys; audit trail; deterministic LLM fallbacks; kill switch.

## Suggested order of attack

1. Exit logic parity (1.1) — biggest expected impact, no research required.
2. Recompute plan geometry from actual entry (1.5) — small change, fixes hurdle gate and Kelly inputs.
3. Event-driven backtest of the real pipeline (1.4) — prerequisite for tuning anything else honestly.
4. Meta-labeler retraining to match live management + calibration (1.2).
5. Sizing rework: risk-first, calibrated-probability scaling (2.1, 2.2).
6. Empirical durations / threshold sweep (1.3, 3.1).
7. Refactors (Priority 4) opportunistically alongside the above.

---

*This is a code and methodology review, not financial advice. Validate every change in paper mode against the improved backtest before enabling live execution.*
