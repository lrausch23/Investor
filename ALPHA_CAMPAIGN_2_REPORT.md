# Alpha Campaign 2 Report

L0 wins raw OOS return; `L1` is the best supported arm under the pre-registered promotion rules.

Generated: 2026-06-13T04:54:52.459168+00:00
Git SHA: `3fa406e`
OOS boundary: `2024-01-01`
Configurations evaluated: `19`
Wall clock seconds: `584.962152`

## Executive Answers

- Raw OOS return winner: `L0`.
- Best supported arm: `L1`.
- Dumb-control verdict: best supported stack beats spy_200dma.
- Cost fragility: `not_cost_fragile`.
- No production defaults or agent behavior were changed.

## Per-Arm Results

| Arm | CAGR | Vol | Sharpe | Calmar | Max DD | OOS Return | OOS Sharpe | Turnover | Costs | Exposure |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| L0 | 24.81% | 18.15% | 1.313 | 0.735 | -33.75% | 87.79% | 1.898 | 0.924 | 1553.118 | 99.21% |
| L1 | 23.04% | 14.20% | 1.532 | 1.064 | -21.65% | 79.22% | 1.987 | 1.554 | 2404.873 | 92.65% |
| L2 | 20.90% | 13.14% | 1.511 | 1.300 | -16.08% | 58.73% | 1.712 | 25.435 | 43293.367 | 90.08% |
| L3 | 18.16% | 14.95% | 1.192 | 0.823 | -22.06% | 56.23% | 1.334 | 41.242 | 57822.710 | 87.10% |
| C1_spy_buy_hold | 15.41% | 17.95% | 0.889 | 0.458 | -33.67% | 60.33% | 1.298 | 0.100 | 50.039 | 99.84% |
| C2_spy_200dma | 12.17% | 12.21% | 1.002 | 0.637 | -19.12% | 40.40% | 1.183 | 2.102 | 1947.038 | 83.51% |
| C3_spy_vol_target | 13.68% | 14.25% | 0.971 | 0.624 | -21.93% | 47.84% | 1.236 | 0.990 | 915.237 | 92.99% |
| S_vol_0.12 | 21.30% | 13.17% | 1.532 | 1.119 | -19.02% | 72.95% | 1.958 | 1.942 | 2739.872 | 88.41% |
| S_vol_0.15 | 23.04% | 14.20% | 1.532 | 1.064 | -21.65% | 79.22% | 1.987 | 1.554 | 2404.873 | 92.65% |
| S_vol_0.18 | 23.46% | 14.99% | 1.482 | 1.016 | -23.08% | 79.83% | 1.945 | 1.378 | 2149.476 | 95.06% |
| S_brake_dd_6_reentry_3 | 20.58% | 13.02% | 1.503 | 1.183 | -17.40% | 58.60% | 1.709 | 25.865 | 42908.537 | 89.13% |
| S_brake_dd_6_reentry_5 | 17.41% | 13.22% | 1.280 | 0.861 | -20.23% | 46.13% | 1.397 | 25.011 | 35260.633 | 88.75% |
| S_brake_dd_8_reentry_3 | 20.90% | 13.14% | 1.511 | 1.300 | -16.08% | 58.73% | 1.712 | 25.435 | 43293.367 | 90.08% |
| S_brake_dd_8_reentry_5 | 18.50% | 13.35% | 1.339 | 0.984 | -18.79% | 46.24% | 1.399 | 24.025 | 36843.883 | 89.71% |
| S_brake_dd_10_reentry_3 | 21.36% | 13.26% | 1.527 | 1.329 | -16.07% | 60.42% | 1.749 | 25.096 | 43282.527 | 90.79% |
| S_brake_dd_10_reentry_5 | 18.21% | 13.43% | 1.313 | 0.970 | -18.77% | 46.22% | 1.399 | 24.019 | 35923.176 | 90.18% |
| S_momentum_top_0.33 | 18.32% | 16.62% | 1.096 | 0.753 | -24.32% | 44.15% | 0.967 | 45.586 | 67133.238 | 83.84% |
| S_momentum_top_0.50 | 18.16% | 14.95% | 1.192 | 0.823 | -22.06% | 56.23% | 1.334 | 41.242 | 57822.710 | 87.10% |

## Stress Windows

| Arm | Window | Return | Max DD | Days To Derisk | Exposure |
| --- | --- | --- | --- | --- | --- |
| L0 | covid_crash | -11.21% | -33.75% |  | 99.22% |
| L0 | bear_2022 | -19.11% | -21.40% |  | 99.34% |
| L0 | vol_shock_aug_2024 | 2.46% | -5.03% |  | 99.24% |
| L0 | tariff_shock_2025 | 2.38% | -13.41% |  | 99.37% |
| L1 | covid_crash | -11.29% | -21.65% | 9.000 | 52.62% |
| L1 | bear_2022 | -16.56% | -17.84% | 119.000 | 80.13% |
| L1 | vol_shock_aug_2024 | 2.24% | -5.03% |  | 97.38% |
| L1 | tariff_shock_2025 | -2.25% | -11.62% | 34.000 | 75.76% |
| L2 | covid_crash | -9.87% | -15.92% | 6.000 | 45.38% |
| L2 | bear_2022 | -11.32% | -15.80% | 119.000 | 76.02% |
| L2 | vol_shock_aug_2024 | 3.01% | -3.76% | 8.000 | 96.21% |
| L2 | tariff_shock_2025 | -3.17% | -10.52% | 6.000 | 58.08% |
| L3 | covid_crash | -16.54% | -22.06% | 6.000 | 44.01% |
| L3 | bear_2022 | -7.96% | -16.45% | 32.000 | 73.38% |
| L3 | vol_shock_aug_2024 | 3.17% | -3.73% | 6.000 | 83.45% |
| L3 | tariff_shock_2025 | 1.40% | -11.96% | 6.000 | 55.43% |
| C1_spy_buy_hold | covid_crash | -13.62% | -33.67% |  | 99.84% |
| C1_spy_buy_hold | bear_2022 | -24.25% | -24.47% |  | 99.90% |
| C1_spy_buy_hold | vol_shock_aug_2024 | 1.60% | -6.06% |  | 99.92% |
| C1_spy_buy_hold | tariff_shock_2025 | 1.27% | -14.69% |  | 99.93% |
| C2_spy_200dma | covid_crash | -8.55% | -12.41% | 13.000 | 17.61% |
| C2_spy_200dma | bear_2022 | -15.24% | -15.81% | 30.000 | 32.30% |
| C2_spy_200dma | vol_shock_aug_2024 | 1.60% | -6.06% |  | 99.92% |
| C2_spy_200dma | tariff_shock_2025 | -4.52% | -6.18% | 32.000 | 49.16% |
| C3_spy_vol_target | covid_crash | -11.96% | -21.42% | 13.000 | 54.36% |
| C3_spy_vol_target | bear_2022 | -21.42% | -21.93% | 119.000 | 80.94% |
| C3_spy_vol_target | vol_shock_aug_2024 | 0.02% | -6.07% |  | 85.07% |
| C3_spy_vol_target | tariff_shock_2025 | -3.27% | -12.40% | 34.000 | 69.34% |
| S_vol_0.12 | covid_crash | -9.47% | -19.02% | 6.000 | 47.23% |
| S_vol_0.12 | bear_2022 | -15.07% | -16.21% | 50.000 | 71.58% |
| S_vol_0.12 | vol_shock_aug_2024 | 0.62% | -4.95% |  | 80.77% |
| S_vol_0.12 | tariff_shock_2025 | -1.83% | -10.32% | 34.000 | 67.58% |
| S_vol_0.15 | covid_crash | -11.29% | -21.65% | 9.000 | 52.62% |
| S_vol_0.15 | bear_2022 | -16.56% | -17.84% | 119.000 | 80.13% |
| S_vol_0.15 | vol_shock_aug_2024 | 2.24% | -5.03% |  | 97.38% |
| S_vol_0.15 | tariff_shock_2025 | -2.25% | -11.62% | 34.000 | 75.76% |
| S_vol_0.18 | covid_crash | -11.86% | -23.08% | 9.000 | 57.23% |
| S_vol_0.18 | bear_2022 | -16.42% | -18.14% |  | 86.66% |
| S_vol_0.18 | vol_shock_aug_2024 | 2.48% | -5.04% |  | 99.43% |
| S_vol_0.18 | tariff_shock_2025 | -2.15% | -12.74% | 37.000 | 82.68% |
| S_brake_dd_6_reentry_3 | covid_crash | -9.86% | -15.92% | 6.000 | 45.37% |
| S_brake_dd_6_reentry_3 | bear_2022 | -13.04% | -17.40% | 21.000 | 75.09% |
| S_brake_dd_6_reentry_3 | vol_shock_aug_2024 | 3.03% | -3.74% | 8.000 | 96.23% |
| S_brake_dd_6_reentry_3 | tariff_shock_2025 | -3.17% | -10.52% | 6.000 | 58.07% |
| S_brake_dd_6_reentry_5 | covid_crash | -12.44% | -17.34% | 6.000 | 43.01% |
| S_brake_dd_6_reentry_5 | bear_2022 | -16.18% | -20.23% | 21.000 | 75.58% |
| S_brake_dd_6_reentry_5 | vol_shock_aug_2024 | 2.99% | -4.06% | 8.000 | 95.92% |
| S_brake_dd_6_reentry_5 | tariff_shock_2025 | -6.14% | -12.02% | 6.000 | 56.61% |
| S_brake_dd_8_reentry_3 | covid_crash | -9.87% | -15.92% | 6.000 | 45.38% |
| S_brake_dd_8_reentry_3 | bear_2022 | -11.32% | -15.80% | 119.000 | 76.02% |
| S_brake_dd_8_reentry_3 | vol_shock_aug_2024 | 3.01% | -3.76% | 8.000 | 96.21% |
| S_brake_dd_8_reentry_3 | tariff_shock_2025 | -3.17% | -10.52% | 6.000 | 58.08% |
| S_brake_dd_8_reentry_5 | covid_crash | -12.44% | -17.35% | 6.000 | 43.01% |
| S_brake_dd_8_reentry_5 | bear_2022 | -14.60% | -18.79% | 119.000 | 76.36% |
| S_brake_dd_8_reentry_5 | vol_shock_aug_2024 | 3.01% | -4.05% | 8.000 | 95.94% |
| S_brake_dd_8_reentry_5 | tariff_shock_2025 | -6.13% | -12.02% | 6.000 | 56.62% |
| S_brake_dd_10_reentry_3 | covid_crash | -9.88% | -15.93% | 6.000 | 45.38% |
| S_brake_dd_10_reentry_3 | bear_2022 | -9.98% | -14.52% | 119.000 | 77.08% |
| S_brake_dd_10_reentry_3 | vol_shock_aug_2024 | 3.02% | -3.76% | 8.000 | 96.24% |
| S_brake_dd_10_reentry_3 | tariff_shock_2025 | -2.14% | -10.51% | 6.000 | 59.65% |
| S_brake_dd_10_reentry_5 | covid_crash | -12.43% | -17.34% | 6.000 | 43.01% |
| S_brake_dd_10_reentry_5 | bear_2022 | -14.58% | -18.77% | 119.000 | 76.34% |
| S_brake_dd_10_reentry_5 | vol_shock_aug_2024 | 3.00% | -4.05% | 8.000 | 95.93% |
| S_brake_dd_10_reentry_5 | tariff_shock_2025 | -6.12% | -12.01% | 6.000 | 56.62% |
| S_momentum_top_0.33 | covid_crash | -18.04% | -21.97% | 6.000 | 43.03% |
| S_momentum_top_0.33 | bear_2022 | -2.47% | -12.52% | 122.000 | 73.08% |
| S_momentum_top_0.33 | vol_shock_aug_2024 | 3.66% | -4.27% | 5.000 | 68.32% |
| S_momentum_top_0.33 | tariff_shock_2025 | 0.14% | -14.27% | 2.000 | 52.28% |
| S_momentum_top_0.50 | covid_crash | -16.54% | -22.06% | 6.000 | 44.01% |
| S_momentum_top_0.50 | bear_2022 | -7.96% | -16.45% | 32.000 | 73.38% |
| S_momentum_top_0.50 | vol_shock_aug_2024 | 3.17% | -3.73% | 6.000 | 83.45% |
| S_momentum_top_0.50 | tariff_shock_2025 | 1.40% | -11.96% | 6.000 | 55.43% |

## Promotion Rules

| Rule | Result |
| --- | --- |
| Layer support | {"L1": {"calmar_ok": true, "return_ok": true, "sharpe_ok": true, "supported": true}, "L2": {"calmar_ok": false, "return_ok": false, "sharpe_ok": false, "supported": false}, "L3": {"calmar_ok": false, "return_ok": true, "sharpe_ok": false, "supported": false}} |
| Control hurdle | best supported stack beats spy_200dma |
| Stress preservation | {"bear_2022": {"final_advantage": 0.03557856150897509, "l2_advantage": 0.0559841572952251, "passed": true}, "covid_crash": {"final_advantage": 0.12104532572751359, "l2_advantage": 0.17831659977150605, "passed": true}, "passed": true} |
| Cost fragility | not_cost_fragile |

## Sensitivity Grids

Sensitivity arms evaluated: `11`. Headline comparisons use defaults; grids are for robustness commentary only.

## Recommended Next Steps

No default changes are recommended without human sign-off. Mapping winning specs onto the four agents remains a future task.
