# Alpha Campaign Report

Generated: 2026-06-12T21:03:19.364127+00:00
Git SHA: `f465b04`
OOS boundary: `2024-01-01`

## Executive Answer

Q1: The baseline regime strategy does not show enough OOS edge to deserve more capital yet; it averaged 1.82% OOS return versus 60.39% for the SPY buy-and-hold benchmark and beat the benchmark on 0 / 30 names, although it used much less drawdown.

Q2: The Phase 1 threshold sweep found a return/Sharpe-improving candidate family, but it also increased drawdown and trade count; Phase 2 HMM robustness knobs produced no aggregate change.

Q3: The v5 meta-labeler cleared the skill bar statistically with 0.749 OOF ROC-AUC and non-degenerate probability dispersion, but both gate and size_only A/B variants underperformed no-veto on return and Sharpe, so neither should become the default.

Q4: The strategy is useful as a defensive regime filter in sharp stress windows, but it is too late in the 2022 bear window and gives up too much upside in non-crash OOS periods.

## Basket

Selection rule: top 3 dollar-ADV screen-passers per 10 GICS-style sectors, one listing per issuer (dual-class dedupe)
Basket size: 30

| Sector | Selected |
| --- | --- |
| Communication Services | GOOGL, META, NFLX |
| Consumer Discretionary | TSLA, AMZN, HD |
| Consumer Staples | WMT, COST, KO |
| Energy | XOM, CVX, COP |
| Financials | JPM, V, BRK-B |
| Health Care | LLY, UNH, JNJ |
| Industrials | CAT, BA, GE |
| Information Technology | NVDA, AMD, AAPL |
| Materials | LIN, FCX, NEM |
| Utilities | NEE, AEP, D |

## Q1 Baseline Versus Buy-And-Hold

The baseline is not competitive versus SPY buy-and-hold over the OOS period beginning 2024-01-01. It traded all 30 names and generated positive return on 16 / 30 names, but average excess return versus the benchmark was -58.56 percentage points and no ticker beat the benchmark. The useful property is risk containment: average max drawdown was -8.05% versus materially worse drawdowns in the stress-window benchmark comparisons.

| Metric | Value |
| --- | --- |
| Ticker count | 30 |
| Traded tickers | 30 |
| OOS trades | 904 |
| OOS return | 1.82% |
| SPY buy-and-hold benchmark return | 60.39% |
| Average excess return | -58.56% |
| Tickers beating benchmark | 0 / 30 |
| Tickers with positive strategy return | 16 / 30 |
| OOS Sharpe | 0.161 |
| OOS max drawdown | -8.05% |

## Q2 Capability Sweep And HMM Robustness

Phase 1: completed; configurations evaluated `96`.
Phase 2: completed; configurations evaluated `5`.

Phase 1 promoted the same family three times: forward-curve gates on, composite adjustments on, empirical durations off, neutral modal tilt off, buy_min_p_bull_day5 0.5, with strong_buy_min_p_bull_day5 at 0.45 / 0.55 / 0.65. The promoted family improved average OOS return and Sharpe versus baseline, but with higher drawdown and far more trades. That is not clean enough for a default change without a per-name improvement check and drawdown review.

| Candidate | OOS Return | OOS Sharpe | Trades | Max DD |
| --- | --- | --- | --- | --- |
| Baseline | 1.82% | 0.161 | 904 | -8.05% |
| Phase 1 promoted family avg | 7.89% | 0.205 | 2749 | -12.63% |

Phase 2 promoted `hmm_baseline`, `macro_weight=1.0`, and `macro_weight=1.5`. All three produced identical full-basket metrics, so this campaign does not support changing HMM seed/covariance/macro-weight defaults.

| Phase 2 Config | OOS Return | OOS Sharpe | Trades | Max DD |
| --- | --- | --- | --- | --- |
| hmm_baseline | 1.82% | 0.161 | 904 | -8.05% |
| macro_weight=1.0 | 1.82% | 0.161 | 904 | -8.05% |
| macro_weight=1.5 | 1.82% | 0.161 | 904 | -8.05% |

## Q3 Meta-Labeler Verdict

| Metric | Value |
| --- | --- |
| Status | qualified |
| OOF ROC-AUC | 0.749 |
| Brier | 0.193 |
| Positive rate | 50.95% |
| Labeled samples | 27910 |
| Skill bar | 0.55 |

The model is statistically useful but not yet economically useful as a live veto. Calibration improved Brier versus base rate by 0.0571, probability standard deviation was 0.173, and probability IQR was 0.189, so this is not the old flat-probability failure mode. The A/B result is still negative: both model-driven modes reduced drawdown, but both also reduced return and Sharpe.

| Mode | OOS Return | OOS Sharpe | Trades | Max DD | Decision |
| --- | --- | --- | --- | --- | --- |
| no_veto | 1.82% | 0.161 | 904 | -8.05% | Keep as default |
| gate | 1.29% | 0.145 | 779 | -6.56% | Do not adopt |
| size_only | 1.10% | 0.140 | 778 | -5.50% | Do not adopt |

## Q4 Stress Windows

Stress behavior is the main positive result. In COVID and 2022 the strategy sharply reduced drawdown versus the benchmark. The weakness is timing and opportunity cost: the average bear flag took 76.5 days in the 2022 bear window, and in 2024/2025 shock windows the strategy preserved capital but lagged benchmark returns.

| Window | Strategy Return | Benchmark Return | Strategy DD | Benchmark DD | Days To Bear | Trades |
| --- | --- | --- | --- | --- | --- | --- |
| bear_2022 | -1.92% | -24.27% | -4.10% | -24.50% | 76.500 | 293 |
| covid_crash | -0.30% | -13.64% | -0.40% | -33.72% | 5.833 | 14 |
| tariff_shock_2025 | -0.04% | 1.27% | -0.74% | -14.70% | 8.833 | 52 |
| vol_shock_aug_2024 | 0.08% | 1.60% | -0.20% | -6.07% | 3.739 | 8 |

## Configurations Evaluated

| Phase | Configurations |
| --- | --- |
| Phase 1 | 96 |
| Phase 2 | 5 |
| Phase 3 | 3 |

## Recommended Default Changes

No defaults are changed by this campaign runner. Any recommendation below requires human approval after reviewing the evidence tables.

- Do not allocate additional live/paper capital based on the current baseline; it did not beat benchmark buy-and-hold OOS.
- Do not enable meta-labeler gate or size_only by default despite the improved AUC; both failed the economic A/B test.
- Do not change HMM macro-weight/seed/covariance defaults from Phase 2; the tested robustness knobs were behaviorally identical in aggregate.
- Treat the Phase 1 forward-curve/composite candidate as a research candidate only; it improved return and Sharpe but worsened drawdown and turnover.
- Next research target: reduce 2022 bear detection latency and improve OOS upside capture without giving up the stress-window drawdown advantage.
