# Regime Platform — Executive Overview

*June 2026. Reflects the system after the review-driven improvement program (Tasks 1–7 complete, Task 8 structural cleanup in progress).*

## What Regime is

Regime is an autonomous trading-agent platform built inside the Investor monorepo. Its founding goal is to field multiple specialized agents that compete against each other in paper portfolios — and eventually live capital — to generate above-benchmark returns. It is best understood as three layers: a **signal engine** that decides what the market is doing, a **decision pipeline** that turns signals into guarded trades, and an **agent competition layer** that runs four differently-mandated agents against each other and keeps score.

The signal engine fits a three-state Gaussian Hidden Markov Model per ticker (Bull / Neutral / Bear) over price, volatility, trend, volume, and macro features (VIX, 10-year yield), walk-forward with scheduled refits so it never trains on the future. From the fitted model it projects a forward probability curve, overlays technical indicators (RSI, MACD, Bollinger), and emits a composite Buy / Hold / Sell signal with confidence, expected duration, and price targets. An XGBoost "meta-labeler" — trained on labels that replay the platform's own exit rules — estimates the probability that each signal will succeed, with isotonic-calibrated probabilities and a skill gate that disqualifies the model from influencing decisions unless it demonstrates out-of-fold predictive power.

The decision pipeline is deliberately gate-heavy. A candidate entry must pass signal-quality, hurdle-rate (net of execution cost and short-term tax), regime-duration, anti-churn, earnings-blackout, VIX-freeze, and regime-ambiguity checks before it becomes a plan. Position size is anchored on risk — how much a 2×ATR stop can lose, capped at a fixed fraction of the portfolio — and scaled by the meta-labeler's calibrated probability. Exits are managed daily through a full ladder: profit targets, ATR trailing stops, calendar time stops, regime-flip exits, and partial reduction when a Bull regime deteriorates to Neutral. Tax logic (long-term-gains override, wash-sale awareness) can suppress or trim exits. Execution flows through guarded broker adapters (IBKR paper today) with order routing, price collars, daily loss limits, drawdown pauses, and a kill switch. Every decision is stamped with the configuration that produced it and written to an audit trail.

The agent layer fields four competitors, each with its own paper portfolio, mandate, and configurable frontier-LLM advisor: a **Quant** agent (regime/ML signals), a **Fundamental** agent (quality, moat, catalysts), a **Portfolio/Tax** agent (sizing, churn, tax efficiency), and an **Execution** agent (liquidity and fill quality). An orchestrator coordinates candidate intake; mandate policies and cross-agent overlap checks keep them differentiated; a dashboard ranks them on a leaderboard with after-tax performance, benchmark comparisons, and LLM verdict-outcome attribution.

What distinguishes the platform from most hobbyist trading systems is its validation machinery. An event-driven backtest replays the *actual* production pipeline — same signals, same gates, same sizing, same exit ladder, with costs — rather than an idealized strategy, and reports honest metrics (annualized Sharpe from daily equity, true information ratio, in-sample/out-of-sample splits). A threshold-sweep harness can grid-search any decision parameter through that backtest across a ticker basket. Model training uses purged walk-forward cross-validation with overlap-aware sample weights. The discipline that no live default changes without basket-level out-of-sample evidence is encoded in the development process itself.

## Where it stands

The infrastructure is ahead of the alpha. That is the single most important sentence in this document.

Everything around the edge — leak-free training, calibrated probabilities, realistic backtesting, risk-first sizing, layered guardrails, audit trails, observability — is in place and tested (~1,500 tests, green in one run, clean typecheck). But the platform's measured predictive edge is, today, approximately zero: the latest meta-labeler (v4, trained on 1,756 properly-labeled samples) scored an out-of-fold ROC-AUC of 0.468 — no better than chance — and the skill gate now correctly bars it from influencing trades. The HMM regime signals themselves have not yet been shown to beat buy-and-hold after costs across a basket out-of-sample; the corrected backtest exists precisely to answer that question, and it has not yet been run as a systematic campaign.

This is not a failure; it is the honest baseline most quantitative efforts never establish. The platform can now *detect* that it has no edge — which is the prerequisite for building one.

## Principal weaknesses

**No demonstrated alpha.** The competition currently ranks agents on small paper samples over a period dominated by noise. With single-digit trades per agent per quarter, leaderboard rank measures luck. Until a strategy clears the pipeline backtest across a basket out-of-sample, the agents are competing at executing a strategy with no proven edge.

**Thin and narrow training data.** The meta-labeler trains on a handful of tech tickers (NVDA, AVGO, MU and peers) — correlated names, one market era, ~1,800 samples for 8 coarse features. Its features describe the market environment, not the specific trade; the enrichment work (signal strength, forward probabilities, risk/reward geometry, technical state) is specified but unproven. Single-name, single-sector concentration also means all four "competing" agents largely share one factor exposure: the semiconductor cycle.

**Agent differentiation is shallow.** The four agents differ in mandates and gates but draw from the same signal engine, the same watchlist, and the same discovery process. True competition requires genuinely independent hypotheses — different data, different horizons, different alpha sources — otherwise the leaderboard ranks risk appetite, not skill.

**Regime-signal weaknesses identified in review remain evidence-gated, not resolved.** Transition-risk gates are near-degenerate; expected durations from the transition matrix are unstable; the Neutral→Buy tilt can fire when Bull isn't even the most likely state; the composite technical adjustments are untested heuristics. The capabilities to fix all of these (empirical durations, forward-curve gates, modal-tilt requirement, ablation flags) shipped default-off and await sweep evidence.

**LLM layer is unproven spend.** Frontier-model advisors attach qualitative verdicts to decisions, but until the new verdict-outcome attribution accumulates a few hundred resolved trades, there is no evidence the LLM adds alpha rather than narrative. Costs and non-reproducibility argue for keeping it strictly advisory.

**Operational and structural debt.** The two core modules remain monoliths behind a package facade (Task 8 in progress); known test flakes (a background-job race, a Torch ordering segfault) are documented but unfixed; sandboxed/CI environments can't reach market data, so full validation requires a local machine with the IBKR gateway up.

## Recommended path forward

The sequencing principle: **prove edge before scaling competition; scale competition before scaling capital.**

1. **Run the alpha campaign (highest priority).** Use the pipeline backtest and threshold sweep across a 20–30 ticker basket spanning sectors, 5+ years, fixed out-of-sample window. Answer three questions with evidence: does the regime strategy beat buy-and-hold after costs at all; which gate configuration (empirical durations, forward-curve gates, modal tilt, composite ablation) is robustly best; and does any configuration produce enough trades for statistical significance. Flip defaults only on aggregate OOS results.
2. **Make the meta-labeler earn its seat.** Retrain with enriched trade-specific features on the wider basket; require OOF AUC ≥ 0.55 to pass the skill gate; prefer probability-scaled sizing over binary vetoes once skill is demonstrated. If it cannot clear the bar after feature enrichment, freeze it and redirect effort to the regime signal itself.
3. **Deepen agent differentiation.** Give each agent a genuinely distinct alpha hypothesis and data diet — e.g., cross-sectional momentum for Quant, fundamental-event-driven for Fundamental, tax-loss/rebalancing alpha for Portfolio/Tax, execution-cost capture for Execution — with separate watchlists and uncorrelated universes. Score the competition on risk-adjusted, benchmark-relative OOS performance with minimum trade counts, not raw P&L.
4. **Extend the evidence loop to the agents.** The leaderboard should display each agent's pipeline-backtest expectation alongside realized paper results, so divergence (live underperforming its own backtest) is a first-class alarm. Wire the decision-health fallback telemetry and LLM attribution into weekly review.
5. **Then, and only then, graduate capital.** Define promotion criteria now: e.g., an agent earns a small live allocation after N months and M trades of paper performance above benchmark with drawdown inside budget, and automatic demotion on breach. The guardrail stack (collars, loss limits, kill switch) is already live-grade; the criteria are the missing piece.
6. **Finish the hygiene.** Complete the module split (Task 8), fix the two known test flakes, and add the full suite + typecheck as a pre-commit/CI gate so the "one command, all green" state achieved this week becomes permanent.

## Bottom line

Regime has completed the unglamorous half of building an alpha platform: it can now run, measure, audit, and falsify trading ideas without fooling itself — a bar most retail and many professional systems never clear. The glamorous half is unstarted in the only sense that matters: no strategy in the system has yet demonstrated out-of-sample edge. The next quarter should be spent not on more infrastructure but on running the evidence campaign the infrastructure was built for — and letting the results, not the architecture, decide which agents deserve capital.
