from __future__ import annotations

import math
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from src.regime import hmm_engine, paper_trading
from src.regime.regime_calibration import (
    build_persistence_calibration_samples,
    fit_regime_calibrators,
    load_regime_calibrator,
)


def _market_frame(periods: int = 180) -> pd.DataFrame:
    index = pd.bdate_range("2025-01-02", periods=periods)
    trend = np.concatenate(
        [
            np.linspace(100, 120, periods // 3),
            np.linspace(120, 118, periods // 3),
            np.linspace(118, 90, periods - 2 * (periods // 3)),
        ]
    )
    price = trend + np.sin(np.arange(periods) / 3.0)
    return pd.DataFrame(
        {
            "price": price,
            "high": price + 1.0,
            "low": price - 1.0,
            "volume": np.linspace(1_000_000, 1_200_000, periods),
            "vix": np.linspace(16.0, 28.0, periods),
            "yield_10y": np.linspace(4.0, 4.4, periods),
        },
        index=index,
    )


def test_seed_agreement_exact_for_matching_and_diverging_labels() -> None:
    index = pd.bdate_range("2025-01-02", periods=5)
    window = pd.DataFrame(index=index)
    base = pd.Series(["Bull", "Bull", "Bear", "Bear", "Neutral"], index=index)
    assert hmm_engine._seed_agreement([base, base.copy()], window, refit_step=5) == pytest.approx(1.0)
    diverged = pd.Series(["Bull", "Bear", "Bear", "Bull", "Neutral"], index=index)
    assert hmm_engine._seed_agreement([base, diverged], window, refit_step=5) == pytest.approx(3 / 5)


def test_fit_regime_model_defaults_preserve_covariance_and_macro_noop() -> None:
    frame = _market_frame()
    baseline = hmm_engine.fit_regime_model("TEST", frame, training_window=120, refit_step=21, iterations=50)
    same_default = hmm_engine.fit_regime_model(
        "TEST",
        frame,
        training_window=120,
        refit_step=21,
        iterations=50,
        covariance_type="diag",
        macro_weight=9.0,
    )
    assert same_default.latest_label == baseline.latest_label
    assert same_default.latest_probability == pytest.approx(baseline.latest_probability)
    assert same_default.seed_agreement == pytest.approx(1.0)
    assert same_default.regime_ambiguous is False


def test_regime_calibrator_json_round_trip_and_improves_brier(tmp_path: Path) -> None:
    frame = pd.DataFrame(
        {
            "regime": ["Bull", "Bear", "Bull", "Bear", "Bull", "Bull", "Bull"],
            "state_probability": [0.20, 0.70, 0.30, 0.80, 0.80, 0.90, 0.95],
        }
    )
    samples = build_persistence_calibration_samples(frame, horizon_days=1)
    assert samples["Bull"]["probabilities"]

    result = fit_regime_calibrators(frame, horizon_days=1, models_dir=tmp_path)
    assert result["Bull"]["saved"] is True
    calibrator = load_regime_calibrator("Bull", models_dir=tmp_path)
    assert calibrator is not None
    probs = np.asarray(samples["Bull"]["probabilities"], dtype=float)
    outcomes = np.asarray(samples["Bull"]["outcomes"], dtype=float)
    raw_brier = float(np.mean((probs - outcomes) ** 2))
    calibrated = calibrator.calibrate(probs)
    calibrated_brier = float(np.mean((calibrated - outcomes) ** 2))
    assert calibrated_brier <= raw_brier


def test_generate_buy_plans_skips_regime_ambiguous_snapshot(monkeypatch) -> None:
    monkeypatch.setattr(paper_trading, "get_paper_portfolio", lambda _portfolio_id: {"id": 1, "current_cash": 25_000})
    import src.regime.vix_freeze as vix_freeze

    monkeypatch.setattr(vix_freeze, "is_vix_frozen", lambda: False)
    monkeypatch.setattr(
        paper_trading,
        "allocate_budget",
        lambda _portfolio_id, config=None: {"themes": [{"theme_id": 10, "by_role": {"Critical-Path": 5_000}}]},
    )
    monkeypatch.setattr(paper_trading, "get_sizing_settings", lambda: {"sizing_method": "equal_dollar"})
    monkeypatch.setattr(paper_trading, "get_hurdle_settings", lambda: {"hurdle_enabled": False, "duration_gate_enabled": False})
    monkeypatch.setattr(paper_trading, "_pending_plan_index", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(paper_trading, "_open_position_index", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(
        paper_trading,
        "get_watchlist",
        lambda status=None: [
            {
                "ticker": "TEST",
                "theme_id": 10,
                "suggested_role": "Critical-Path",
                "suggested_entry_price": 100.0,
                "suggested_exit_price": 120.0,
                "suggested_stop_price": 90.0,
            }
        ],
    )
    monkeypatch.setattr(paper_trading, "_batch_current_prices", lambda _tickers: {"TEST": 100.0})
    monkeypatch.setattr(paper_trading, "agent_candidate_policy", lambda *_args, **_kwargs: {"allowed": True})
    monkeypatch.setattr(paper_trading, "get_latest_signal_snapshot", lambda *_args, **_kwargs: {"regime_ambiguous": True, "current_price": 100.0})
    monkeypatch.setattr(paper_trading, "create_trade_plan", lambda *_args, **_kwargs: pytest.fail("ambiguous regime should not create a plan"))
    import src.regime.anti_churn as anti_churn

    monkeypatch.setattr(anti_churn, "get_anti_churn_settings", lambda: {"anti_churn_enabled": False})
    monkeypatch.setattr(anti_churn, "check_anti_churn", lambda *_args, **_kwargs: SimpleNamespace(passed=True))
    assert paper_trading.generate_buy_plans(1) == []
