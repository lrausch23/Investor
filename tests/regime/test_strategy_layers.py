from __future__ import annotations

import math

import pandas as pd
import pytest

from src.regime.strategy import StrategySpec
from src.regime.strategy.layers import MomentumTiltAllocation, MonthlyBandsRebalance, PriceHistorySignalProvider, RegimeBrakeOverride, VolTargetExposure
from src.regime.strategy.registry import build, register_layer


def _frame(prices: list[float]) -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-01", periods=len(prices))
    return pd.DataFrame({"open": prices, "price": prices, "volume": 1_000_000}, index=dates)


def test_strategy_spec_roundtrip_hash_and_registry_error() -> None:
    spec = StrategySpec(
        name="demo",
        exposure_policy="vol_target",
        exposure_params={"target_vol": 0.15},
        override_policy="regime_brake",
    )
    restored = StrategySpec.from_dict(spec.to_dict())
    assert restored == spec
    assert restored.hash == spec.hash
    with pytest.raises(KeyError, match="Available"):
        build("allocation", "missing_layer", {})


def test_registry_accepts_new_layer_without_engine_changes() -> None:
    @register_layer("allocation", "unit_test_dummy")
    class DummyAllocation:
        def weights(self, date, eligible_names, signal_map):
            return {eligible_names[0]: 1.0}

    layer = build("allocation", "unit_test_dummy", {})
    assert layer.weights(pd.Timestamp("2024-01-01"), ["A"], {"A": {}}) == {"A": 1.0}


def test_vol_target_ewma_exposure_is_clipped() -> None:
    policy = VolTargetExposure(target_vol=0.10, min_exposure=0.25)
    calm_returns = [0.001] * 20
    volatile_returns = [0.04, -0.04] * 10
    assert policy.target_exposure(pd.Timestamp("2024-01-01"), {"portfolio_returns": calm_returns}, {}) == 1.0
    assert 0.25 <= policy.target_exposure(pd.Timestamp("2024-01-01"), {"portfolio_returns": volatile_returns}, {}) < 1.0


def test_regime_brake_excludes_bears_caps_breadth_and_requires_full_reentry() -> None:
    brake = RegimeBrakeOverride(breadth_trigger=0.5, breadth_cap=0.5, reentry_days=3)
    signals = {
        "AAA": {"regime": "Bear", "p_bull_day5": 0.1, "p_bear_day5": 0.8},
        "BBB": {"regime": "Bull", "p_bull_day5": 0.8, "p_bear_day5": 0.1},
    }
    override = brake.override(pd.Timestamp("2024-01-01"), {"drawdown": 0.0}, signals)
    assert override is not None
    assert override.exposure_cap == 0.5
    assert override.exclude_tickers == ("AAA",)

    neutral = {"AAA": {"regime": "Neutral", "p_bull_day5": 0.6, "p_bear_day5": 0.4}, "BBB": signals["BBB"]}
    assert "AAA" in (brake.override(pd.Timestamp("2024-01-02"), {"drawdown": 0.0}, neutral).exclude_tickers)
    interrupted = {"AAA": {"regime": "Neutral", "p_bull_day5": 0.3, "p_bear_day5": 0.4}, "BBB": signals["BBB"]}
    assert "AAA" in (brake.override(pd.Timestamp("2024-01-03"), {"drawdown": 0.0}, interrupted).exclude_tickers)
    assert "AAA" in (brake.override(pd.Timestamp("2024-01-04"), {"drawdown": 0.0}, neutral).exclude_tickers)
    assert "AAA" in (brake.override(pd.Timestamp("2024-01-05"), {"drawdown": 0.0}, neutral).exclude_tickers)
    assert brake.override(pd.Timestamp("2024-01-06"), {"drawdown": 0.0}, neutral) is None


def test_regime_brake_aux_grinding_drawdown_trigger_records_reason() -> None:
    brake = RegimeBrakeOverride(aux_dd_trigger=0.08, aux_cap=0.5)
    signals = {
        "AAA": {"regime": "Neutral", "p_bull_day5": 0.4, "p_bear_day5": 0.3},
        "BBB": {"regime": "Neutral", "p_bull_day5": 0.4, "p_bear_day5": 0.3},
    }
    override = brake.override(pd.Timestamp("2024-02-01"), {"drawdown": -0.09}, signals)
    assert override is not None
    assert override.exposure_cap == 0.5
    assert "aux_grinding_bear_drawdown" in override.reason


def test_momentum_tilt_uses_12_1_skip_month_score() -> None:
    provider = PriceHistorySignalProvider()
    prices = [100.0] * 252 + [150.0] * 21 + [50.0]
    provider.prepare("AAA", _frame(prices))
    last = pd.bdate_range("2024-01-01", periods=len(prices))[-1]
    signal = provider.signals("AAA", last)
    assert math.isclose(float(signal["momentum_12_1"]), 0.5, rel_tol=1e-9)

    allocation = MomentumTiltAllocation(top_fraction=0.5)
    weights = allocation.weights(
        last,
        ["AAA", "BBB", "CCC"],
        {"AAA": {"momentum_12_1": 0.2}, "BBB": {"momentum_12_1": 0.5}, "CCC": {"momentum_12_1": -0.1}},
    )
    assert weights == {"BBB": 0.5, "AAA": 0.5}


def test_monthly_bands_first_trading_day_and_drift() -> None:
    policy = MonthlyBandsRebalance(band=0.25)
    assert policy.should_rebalance(pd.Timestamp("2024-02-01"), {"is_first_trading_day_month": True, "relative_drifts": {}})
    assert policy.should_rebalance(pd.Timestamp("2024-02-15"), {"is_first_trading_day_month": False, "relative_drifts": {"AAA": 0.30}})
    assert not policy.should_rebalance(pd.Timestamp("2024-02-16"), {"is_first_trading_day_month": False, "relative_drifts": {"AAA": 0.10}})
