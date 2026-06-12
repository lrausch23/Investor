# Codex Prompt — Task 10: The Alpha Campaign

You are working in the `Investor/` Python project. Execution order for this engagement:

0. **Precondition — commit hygiene.** The worktree contains verified but uncommitted work from Tasks 1–8. Before writing any new code, commit the existing tree in coarse per-task chunks (the user will provide guidance if asked; otherwise group by the task boundaries documented in `CODEX_PROMPT_*.md` files). Do not mix Task 9/10 work into those commits.
1. **Task 9 first** — implement `CODEX_PROMPT_TASK9.md` (universe eligibility screen + black-swan-horizon backtesting) exactly as written there. Task 10 depends on its screen, price cache, capped-window performance fix, and stress-window reporting.
2. **Task 10 — this document.** The campaign that everything since the original review has been building toward.

## Purpose — read this carefully

Every prior task built measurement machinery. This task **uses** it to answer, with out-of-sample evidence, the questions on which the platform's future depends:

- **Q1.** Does the regime strategy, as configured today, beat buy-and-hold after costs across a diversified basket, out of sample?
- **Q2.** Which of the flag-gated capabilities (empirical durations, forward-curve gates, modal Neutral tilt, composite-adjustment ablation, multi-seed HMM, covariance/macro variants) robustly improve OOS results — and which are noise?
- **Q3.** Can the meta-labeler, retrained on the wide basket with enriched features, clear the 0.55 OOF-AUC skill bar — and if so, does gate or size-only mode add value?
- **Q4.** How does the strategy behave through crises (the four stress windows), relative to benchmark?

This task produces **evidence and a report, not default changes**. The standing rule is absolute here: you flip nothing. Every recommendation lands in the report for human sign-off.

## Campaign design — pre-registered, no improvisation

### Universe and basket
- Candidate pool: tickers passing the Task 9A screen with ≥ 10 years of history.
- Basket: **30 names**, selected by documented mechanical procedure — the 3 largest-dollar-ADV screen-passing names in each of 10 GICS-style sectors (use the sector cache / `fetch_financial_statements` sector data; document any substitutions where sector data is missing). No discretionary picks, no swaps after results are seen. Pin the final list in the campaign config.
- Benchmarks: per-ticker buy-and-hold, plus SPY as the common benchmark.

### Windows — fixed before any run
- Full window: 10 years ending at run date.
- **OOS boundary: 2024-01-01.** Rationale: IS contains the COVID crash and 2022 bear (the model may learn from them); OOS contains the Aug-2024 vol shock and 2025 tariff shock as genuinely unseen crises. All decisions are made on OOS metrics only. IS is reported for sanity/overfit diagnosis (large IS≫OOS gaps are themselves findings).
- Stress windows: the four defaults from Task 9B, reported in every phase.

### Statistical discipline — encode these in the runner, not just the report
- **Minimum evidence floor:** a configuration is only rankable if it produces ≥ 100 aggregate OOS trades across the basket and trades in ≥ 20 of 30 names. Below the floor, report "insufficient sample" — no ranking.
- **Robustness over magnitude:** a capability is "recommended" only if it improves OOS Sharpe AND OOS return at the basket-aggregate level, improves ≥ 60% of individual names, and does not worsen aggregate max drawdown by more than 20% relative. Otherwise it is "not supported" or "mixed".
- **Multiple-comparisons honesty:** the report must state the total number of configurations evaluated, and rank findings by robustness criteria above — never by best single number. Sweep on a fixed 10-name subset (first name per sector by the same mechanical rule), then validate only the top ≤ 3 configurations on the full 30-name basket. Subset-vs-full-basket agreement is itself reported.
- **Determinism:** every run stamped with config (already supported), git SHA, and data-cache date. One campaign = one frozen price-cache snapshot; do not refresh data mid-campaign.

### Phases — run in order, each writes its own artifact

**Phase 0 — Baseline.** Current production defaults, 30 names, 10y, OOS 2024-01-01, stress reports on. Artifact: per-ticker and aggregate IS/OOS metrics, stress-window tables, exit-type and gate-count distributions. This is the Q1 answer and the reference for everything after.

**Phase 1 — Capability sweep (Q2).** On the 10-name subset, sweep: `use_empirical_durations` on/off; forward-curve gates on/off (and a coarse grid over `strong_buy_min_p_bull_day5`/`buy_min_p_bull_day5`, e.g. {0.45, 0.55, 0.65} / {0.40, 0.50}); `neutral_tilt_requires_modal` on/off; `composite_adjustments_enabled` on/off; Neutral-tilt entries tagged and reported separately (the review predicted they are net negative after costs — confirm or refute). Validate winners on the full basket.

**Phase 2 — HMM robustness (Q2, continued).** On the subset: `n_seeds=3` vs 1 (with ambiguity-gate entry counts), `covariance_type` full vs diag, `macro_weight` {1.0, 1.5}. Same validation rule.

**Phase 3 — Meta-labeler v5 (Q3).** Retrain on the full 30-name basket: managed labels, enriched features, date-purged CV, calibration. Report per-fold and aggregate OOF AUC, Brier vs base-rate benchmark, probability dispersion. **If AUC < 0.55: stop — report it, skip the A/B, and say plainly that the labeler stays disqualified.** If ≥ 0.55: run the three-way A/B (no-veto / gate / size_only) on the full basket OOS, using the best configuration from Phases 1–2.

**Phase 4 — Synthesis.** `ALPHA_CAMPAIGN_REPORT.md` (committed to the repo) answering Q1–Q4 in plain language, with: the headline aggregate OOS table (baseline vs best-validated config vs buy-and-hold/SPY), stress-window behavior including `days_to_bear_flag` distributions, per-capability verdicts (recommended / not supported / mixed, with the evidence row), meta-labeler verdict, the configurations-evaluated count, and a short "recommended default changes" list — each entry citing its evidence table and explicitly awaiting human approval.

## Engineering deliverables

1. **`src/regime/alpha_campaign.py`** + CLI subcommand `alpha-campaign`:
   - `alpha-campaign select-basket` — runs the mechanical selection, writes `data/campaign/basket.json` (pinned list + selection metadata + screen stats).
   - `alpha-campaign run --phase {0,1,2,3} [--resume]` — orchestrates the runs with the Task 9B price cache; **resumable** (per-ticker/per-config result files under `data/campaign/phase{N}/`; skip completed units on resume — these are multi-hour runs and must survive interruption).
   - `alpha-campaign report` — aggregates artifacts into the Phase 4 report skeleton with all tables computed; prose conclusions are completed by you (Codex) at acceptance time, clearly marked.
   - Reuse `run_pipeline_backtest` / `threshold_sweep` / meta-labeler training; do not duplicate their logic. The campaign module is orchestration + aggregation + report rendering only.
2. **Tests (`tests/regime/test_alpha_campaign.py`)** — offline, stub providers, tiny synthetic frames: basket selection is mechanical and deterministic given a stubbed screen/sector source; resume skips completed units (assert via counting stub); evidence-floor and robustness-verdict logic on hand-built result fixtures (a config below the trade floor is "insufficient sample"; a config winning aggregate but only 12/30 names is "mixed"); report renderer produces all required tables from fixtures.
3. The usual: full suite + typecheck green, deprecation-clean new tests, no behavior changes to the trading path (this task only **reads** through existing interfaces), no network in tests.

## Definition of done

1. Code + tests merged; full suite green in one command.
2. Phase 0–4 executed locally against real data (network OK), artifacts under `data/campaign/` (gitignored except `basket.json` and the final report), `ALPHA_CAMPAIGN_REPORT.md` committed.
3. The report answers Q1–Q4 with the discipline above — including, if the honest answer to Q1 is "no edge over buy-and-hold," saying exactly that. A negative result with clean evidence is a successful campaign outcome; a flattering result built on subset-mining is a failed one.
4. No defaults changed. The "recommended changes" list is the handoff to the human.
