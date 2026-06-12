from __future__ import annotations

import pandas as pd

from src.regime import universe
from src.regime.pipeline_backtest import PipelineBacktestConfig, PipelineSignal, run_pipeline_backtest


def _frame(days: int = 800, *, price: float = 20.0, volume: float = 1_000_000.0) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "price": [price] * days,
            "open": [price] * days,
            "high": [price * 1.01] * days,
            "low": [price * 0.99] * days,
            "volume": [volume] * days,
            "vix": [20.0] * days,
            "yield_10y": [4.0] * days,
        },
        index=pd.bdate_range("2021-01-04", periods=days),
    )


def test_universe_screen_accepts_liquid_common_stock(monkeypatch) -> None:
    monkeypatch.setattr(universe, "get_setting", lambda _key: None)
    result = universe.check_universe_eligibility("NVDA", market_frame=_frame(), asset_class="stock", use_cache=False)
    assert result.eligible is True
    assert result.reasons == []
    assert result.measured_history_days == 800
    assert result.measured_dollar_adv == 20_000_000.0


def test_universe_screen_rejects_excluded_or_illiquid_symbols(monkeypatch) -> None:
    monkeypatch.setattr(universe, "get_setting", lambda _key: None)
    leveraged = universe.check_universe_eligibility("TQQQ", market_frame=_frame(), asset_class="ETF", use_cache=False)
    illiquid = universe.check_universe_eligibility("TEST", market_frame=_frame(days=50, price=3.0, volume=10_000.0), asset_class="stock", use_cache=False)
    assert "excluded_ticker_pattern" in leveraged.reasons
    assert {"price_below_min", "insufficient_history", "dollar_adv_below_min"} <= set(illiquid.reasons)


def test_pipeline_default_universe_gate_blocks_ineligible_short_history(monkeypatch) -> None:
    monkeypatch.setattr(universe, "get_setting", lambda _key: None)
    frame = _frame(days=5, price=100.0, volume=2_000_000.0)

    def provider(ticker, date, history, config, previous_regime):
        del ticker, history, config, previous_regime
        return PipelineSignal(
            date=date.date().isoformat(),
            regime="Bull",
            probability=0.8,
            composite_action="Buy",
            composite_strength=0.9,
            expected_duration=21.0,
            transition_risk=0.1,
            regime_days=5,
            price_targets={"entry_price": 100.0, "target_price": 120.0, "stop_price": 90.0, "timeframe_days": 21},
        )

    config = PipelineBacktestConfig(
        training_window=0,
        enable_hurdle_gate=False,
        enable_duration_gate=False,
        enable_anti_churn_gate=False,
        enable_signal_quality_gate=False,
    )
    result = run_pipeline_backtest("TEST", frame, config=config, signal_provider=provider)
    assert result.trades == []
    assert result.gate_counts["universe"] >= 1
