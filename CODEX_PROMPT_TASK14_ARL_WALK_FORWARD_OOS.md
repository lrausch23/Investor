# CODEX TASK: Fix ARL OOS evaluation — walk-forward across full DEV span with multiple crash regimes

## Context
The Stage-2 go-live smoke test is in place and correctly paused:

- Snapshot: `d2ccfd9ea42e4db663003dcfacfa6a3ce69e4e91ea5c059de82b356f3a17f527`
- Ledger: `data/agent_research/arl_trials.jsonl`
- First hypothesis: `H001_A1_momentum_risk_overlay`
- Current verdict: `inconclusive`
- Holdout: `2024-01-01 -> 2025-12-31`, not accessed
- Research-only; `production_defaults_changed = False`

The current OOS evaluation is too weak. `run_first_momentum_risk_overlay_iteration(...)` uses a single DEV
OOS cut (`2021-01-01 -> 2023-12-31`). That is not enough for the Agent Research Loop because it can make a
strategy look acceptable or unacceptable based on one regime path. The loop needs a repeatable
walk-forward OOS evaluator across the full non-holdout history, with explicit crash/stress windows.

## Goal
Replace the single-slice Stage-2 OOS read with a robust DEV-only walk-forward evaluation that covers the
full available non-holdout span and multiple crash regimes. Do not touch the locked holdout.

## Non-negotiable guardrails
- Research-only. Do not change live trading or production defaults.
- Do not access, read, evaluate, summarize, chart, or compare the locked holdout (`2024-01-01 -> 2025-12-31`).
- Do not reset, rewrite, or branch the hash-chained ledger. Append only.
- Do not self-declare `certified`, `winner`, or `certifiable` from the ARL path.
- Do not relax reality guards: survivorship-free PIT, after-tax 32%/20% with wash-sale handling where applicable, costs/slippage, mechanical rules only, and liquidity/ADV constraints where strategy selection uses them.
- Do not tune the strategy to pass the new evaluator. This task fixes the evaluator and reruns the already pre-registered first hypothesis once.

## Implementation scope

### 1. Add a reusable DEV walk-forward evaluator
Add a reusable evaluator in `src/regime/agent_research_loop.py` or a small companion module:

```python
run_dev_walk_forward_evaluation(...)
```

It should:

- Use only DEV data ending no later than `2023-12-31`.
- Prefer the widest available PIT DEV span: `1998-01-01 -> 2023-12-31`.
- If a strategy cannot form signals at the beginning of the period, start each arm only when its required lookback/history exists; report effective start dates.
- Run rolling walk-forward OOS folds rather than one static OOS split.
- Use fixed pre-registered folds, not dynamically selected folds.
- Return machine-readable fold metrics, aggregate metrics, stress-window metrics, and pass/fail flags.

Suggested default folds:

| Fold | Train / formation available through | OOS window | Stress covered |
| --- | --- | --- | --- |
| `wf_2000_2002_dotcom` | `1998-12-31` | `2000-01-01 -> 2002-12-31` | dot-com / Nasdaq crash |
| `wf_2008_2009_gfc` | `2007-12-31` | `2008-01-01 -> 2009-12-31` | GFC |
| `wf_2011_2012_macro` | `2010-12-31` | `2011-01-01 -> 2012-12-31` | Euro/debt ceiling risk |
| `wf_2015_2016_growth_scare` | `2014-12-31` | `2015-01-01 -> 2016-12-31` | China/oil/growth scare |
| `wf_2018_q4` | `2017-12-31` | `2018-01-01 -> 2018-12-31` | Q4 2018 drawdown |
| `wf_2020_covid` | `2019-12-31` | `2020-01-01 -> 2020-12-31` | COVID crash/rebound |
| `wf_2022_inflation_bear` | `2021-12-31` | `2022-01-01 -> 2022-12-31` | inflation/rates bear market |
| `wf_2023_recovery` | `2022-12-31` | `2023-01-01 -> 2023-12-31` | post-bear recovery |

If early folds lack enough data for the basket arm, do not silently skip. Emit a fold row with
`status = insufficient_history`, the reason, and the effective start date. The aggregate should disclose
which folds were included.

### 2. Evaluate H001 through the new evaluator
Rerun the already pre-registered first hypothesis:

`A1_pure_momentum + L1-style volatility-target / drawdown-brake overlay`

Compare it against:

- Synthesized S&P 500 benchmark
- Bare `A1_pure_momentum`

The comparison must be OOS-only by fold. Do not let a fold use post-fold information to configure the
strategy. For this first implementation, the overlay parameters stay fixed exactly as recorded in trial 6:

- `target_vol = 0.15`
- `vol_window_days = 63`
- `drawdown_brake_1 = -0.10`, exposure `0.50`
- `drawdown_brake_2 = -0.20`, exposure `0.25`
- `overlay_cost_bps = 2.0`

### 3. Replace the verdict basis for Stage-2 iterations
For ARL iterations, verdicts should no longer be based on one OOS slice. Add an explicit
`oos_evaluation_mode = "walk_forward_stress_folds"` field to iteration artifacts.

Pre-registered success criterion for H001 after this fix:

- On aggregate included DEV folds, beat synthesized S&P 500 on after-tax total return, Calmar, and Ulcer.
- In at least 60% of included folds, beat synthesized S&P 500 on at least 2 of the 3 metrics.
- During named crash folds (`dotcom`, `gfc`, `covid`, `inflation_bear` where available), max drawdown and
  Ulcer must both improve versus bare A1. This validates the overlay is actually reducing pain rather than
  simply changing return timing.

Verdict mapping:

- `promising`: all aggregate criteria pass and crash-fold pain reduction passes.
- `inconclusive`: aggregate criteria partially pass or crash-fold evidence is mixed.
- `killed`: aggregate criteria fail on two or more metrics, or crash folds show no risk benefit.

Do not use `certified` or `winner`.

### 4. Ledger handling
Do not edit or remove trial 6. Append a new corrective evaluation entry, for example:

`H001R_A1_momentum_risk_overlay_walk_forward`

The entry must:

- Reference original trial `H001_A1_momentum_risk_overlay`.
- State that the prior trial used a single OOS slice and is superseded for verdict purposes.
- Include the full fold table and aggregate criteria.
- Increment the trial count.
- Preserve `holdout_accessed = False`.
- Preserve `production_defaults_changed = False`.

### 5. Outputs
Write artifacts under `data/agent_research/`:

- `iteration_001_walk_forward_result.json`
- `iteration_001_walk_forward_folds.csv`
- `iteration_001_walk_forward_summary.md`
- updated `go_live_summary.json` or a new `stage2_walk_forward_summary.json`

The markdown summary should be management-readable and should start with the answer:

- `Gate`
- `Holdout accessed`
- `Verdict`
- `Why the verdict changed or did not change`
- Fold table
- Stress-window observations
- Ledger status

### 6. Tests
Add focused tests in `tests/regime/test_agent_research_loop.py`.

Required tests:

1. `test_walk_forward_evaluator_never_uses_holdout`
   - Build a fixture with data extending into a fake holdout.
   - Assert no fold end date exceeds the configured DEV end.
   - Assert output stamps `holdout_accessed = False`.

2. `test_walk_forward_requires_multiple_folds_for_verdict`
   - A one-fold result must not produce `promising`.
   - It should return `inconclusive` or `killed` with a clear `insufficient_fold_count` reason.

3. `test_walk_forward_crash_folds_are_reported`
   - Fixture includes at least two named stress folds.
   - Assert fold rows include stress labels and crash/risk metrics.

4. `test_h001_corrective_entry_appends_not_rewrites`
   - Seed a ledger with trial 6.
   - Run the corrective append.
   - Assert trial count increments, previous hash links correctly, and trial 6 remains unchanged.

5. Existing tests remain green.

## Acceptance criteria
- Stage-1 gate is still confirmed as `HARNESS READY` for snapshot `d2ccfd9e...`.
- H001 is rerun through the new walk-forward stress-fold evaluator.
- The locked holdout remains untouched.
- Ledger has a new appended corrective entry; chain verifies.
- Artifacts include fold-level OOS metrics, aggregate metrics, stress/crash-window diagnostics, and a management-readable summary.
- Verdict is one of `killed`, `inconclusive`, or `promising` only.
- Full suite and typecheck pass.
- `production_defaults_changed = False`.

## Notes for reviewers
This is an evaluator fix, not a new strategy. The first Stage-2 smoke test gave useful process evidence
but not enough strategy evidence. A single 2021-2023 OOS slice is not robust enough for a research loop
that will otherwise multiply-test ideas. The corrected evaluator should make it hard for a strategy to
look good unless it survives multiple independent market regimes and crash periods.
