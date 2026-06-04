# Regime Agent Beta Target

## Intended target

The Regime agent beta is intended to test whether each paper-trading agent can produce a 2% or greater average monthly pre-tax gain over a rolling 6-12 month paper-trading period.

The 2% monthly target is a stretch feasibility objective, not an expectation that every agent must be profitable every month. A 2% monthly return compounds to about 26.8% annually, so results must be evaluated against benchmark and risk.

Initial testing is paper trading only, likely starting with four agents. The project should treat a single month above 2% as evidence of possible viability, not proof.

## Success framing

The beta should pass on risk-adjusted evidence, not on a single profitable month. Each agent should be evaluated on:

- Monthly pre-tax return and rolling 6-12 month average.
- Alpha versus appropriate benchmarks such as SPY, QQQ, and/or SOXX.
- Max drawdown and recovery time.
- Turnover, trade count, and slippage.
- Realized versus unrealized P&L.
- Concentration and correlated exposure.
- Guardrail and veto history.

## Operating assumption

The primary target is pre-tax performance during paper trading. After-tax impact can be tracked separately once the workflow has enough realized trades and holding-period history to make tax analysis meaningful.

Persistent 2%+ average monthly performance with controlled drawdown, reasonable turnover, and benchmark outperformance is the intended success signal.

## Deployment

The repeatable local deployment command is:

```bash
.venv/bin/python scripts/deploy_regime_beta.py --budget 25000
```

The deployment creates or reuses `Regime Agent Beta - IBKR Paper`, sets `broker_type=ibkr`, enables `IBKR_PAPER_BACKEND=true`, keeps `IBKR_LIVE_BACKEND=false`, enables the four-agent topology, sets autonomous paper mode, and records the beta target/benchmark settings.

## Daily market-hours schedule

The preferred unattended run window is regular US market hours. The configured runner is:

```bash
.venv/bin/python scripts/run_regime_beta_market_session.py
```

The runner uses America/New_York time, skips weekends and configured US market holidays, and only runs inside the 10:05-15:30 ET preferred window unless explicitly forced. It records completion/skip state in Regime settings and will not run the same trade date twice.

The launchd job definition is `scripts/com.investor.regime-beta.plist`. It is scheduled for Monday-Friday at 10:05 AM local time, with the Python runner enforcing the ET market-hours guard.
