
## 2026-06-12T16:51:46.651118+00:00 - Preflight cache warm

- IBKR requirement: not required for backtest campaign execution; market data uses cached/yfinance daily bars.
- Basket: 30 names; duplicate issuer scan complete before freeze.
- Warmed entries: 31 / 31; failures: 0.
- Timing check: GOOGL 10y pipeline backtest completed in 16.30 seconds, below the 5 minute stop threshold.

## 2026-06-12T17:02:42.077986+00:00 - Phase 0 complete

- Baseline artifacts: 30 / 30.
- OOS trades: 904; traded names: 30; average OOS return: 0.0182; average OOS Sharpe: 0.1607; average max drawdown: -0.0805.

## 2026-06-12T19:52:10+00:00 - Phase 1 complete

- Threshold subset artifacts: 96 / 96; full-basket validation artifacts: 3 / 3.
- Promoted configs: forward-curve gates on, composite adjustments on, empirical durations off, neutral modal tilt off, buy_min_p_bull_day5 0.5, strong_buy_min_p_bull_day5 0.45 / 0.55 / 0.65.
- Full validation rows: 93; average OOS return across promoted rows: 0.0789; average OOS Sharpe: 0.2050; aggregate trade count across rows: 2749.

## 2026-06-12T20:28:10+00:00 - Phase 2 complete

- HMM robustness artifacts: 140 / 140 plus summary.
- Promoted configs: hmm_baseline, macro_weight=1.0, macro_weight=1.5.
- Full-basket metrics were identical across promoted configs: OOS trades 904; OOS return 0.0182; OOS Sharpe 0.1607; max drawdown -0.0805.

## 2026-06-12T21:01:10+00:00 - Phase 3 complete

- Meta-labeler status: qualified; OOF ROC-AUC 0.7489 versus 0.55 skill gate; labeled samples 27910.
- A/B no-veto baseline: OOS return 0.0182; Sharpe 0.1607; trades 904; max drawdown -0.0805.
- A/B gate: OOS return 0.0129; Sharpe 0.1453; trades 779; max drawdown -0.0656.
- A/B size_only: OOS return 0.0110; Sharpe 0.1404; trades 778; max drawdown -0.0550.
- Interpretation for report: meta-labeler has statistical signal, but neither gate nor size_only earned default adoption in this OOS basket because both reduced return and Sharpe.

## 2026-06-12T22:25:00+00:00 - Suite hang fix verified

- Reproduced the order-dependent stall with `python -m pytest tests/regime --timeout=120 --timeout-method=thread -q`; the timeout stack stopped in `tests/regime/test_sprint57.py::test_lstm_train_save_load_and_analyze` inside `torch.nn.LSTM.forward`.
- Isolated predecessor-pair checks with the IB thread tests and Sprint 56 did not reproduce; the direct LSTM test passed alone in about 2.3 seconds, so the failure is a cumulative PyTorch runtime interaction rather than a deterministic model-code failure.
- Added `pytest-timeout` and a default 300 second thread timeout, then moved the Torch-heavy LSTM train/save/load assertion into a bounded subprocess with single-thread Torch/OpenMP environment settings.
- Verification: `python -m pytest tests/regime/test_sprint57.py -q` passed 6 tests in 4.08 seconds; `python -m pytest tests/regime -q` passed 1176 tests in 158.38 seconds; `python -m pytest -q` passed 1504 tests with 1 existing skip in 168.32 seconds; `scripts/typecheck.sh` passed.
