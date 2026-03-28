from __future__ import annotations

import importlib

import numpy as np
import pandas as pd
import pytest
from fastapi.testclient import TestClient

from src.app.main import create_app
from src.app.routes import regime as regime_route
from src.regime import ensemble as ensemble_module
from src.regime import persistence as persistence_module
from src.regime import triple_barrier as triple_barrier_module


@pytest.fixture()
def temp_modules(tmp_path, monkeypatch):
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    store = importlib.reload(persistence_module)
    monkeypatch.setattr(store, "DB_PATH", tmp_path / "regime_watch.db")
    ensemble = importlib.reload(ensemble_module)
    triple_barrier = importlib.reload(triple_barrier_module)
    return store, ensemble, triple_barrier


def _frame(prices, regimes, atr=1.0):
    prices = list(prices)
    index = pd.date_range("2026-01-01", periods=len(prices), freq="D")
    return pd.DataFrame(
        {
            "price": prices,
            "high": [float(price + atr) for price in prices],
            "low": [float(price - atr) for price in prices],
            "regime": list(regimes),
        },
        index=index,
    )


def test_barrier_bull_upper_hit(temp_modules) -> None:
    _store, _ensemble, tb = temp_modules
    frame = _frame([100, 102, 104, 105], ["Bull", "Bull", "Bull", "Bull"])
    labeled = tb.apply_triple_barrier_labels(
        frame,
        close_col="price",
        high_col="high",
        low_col="low",
        config=tb.BarrierConfig(atr_period=1, profit_target_atr_mult=2.0, stop_loss_atr_mult=2.0, max_holding_days=3),
    )
    assert labeled["barrier_outcome"].iloc[0] == 1.0
    assert labeled["barrier_type"].iloc[0] == "upper"


def test_barrier_bull_lower_hit(temp_modules) -> None:
    _store, _ensemble, tb = temp_modules
    frame = _frame([100, 98, 97, 96], ["Bull", "Bull", "Bull", "Bull"])
    labeled = tb.apply_triple_barrier_labels(
        frame,
        close_col="price",
        high_col="high",
        low_col="low",
        config=tb.BarrierConfig(atr_period=1, profit_target_atr_mult=2.0, stop_loss_atr_mult=2.0, max_holding_days=3),
    )
    assert labeled["barrier_outcome"].iloc[0] == 0.0
    assert labeled["barrier_type"].iloc[0] == "lower"


def test_barrier_bull_vertical_timeout(temp_modules) -> None:
    _store, _ensemble, tb = temp_modules
    frame = pd.DataFrame(
        {
            "price": [100] * 25,
            "high": [101] * 25,
            "low": [99] * 25,
            "regime": ["Bull"] * 25,
        },
        index=pd.date_range("2026-01-01", periods=25, freq="D"),
    )
    labeled = tb.apply_triple_barrier_labels(
        frame,
        close_col="price",
        high_col="high",
        low_col="low",
        config=tb.BarrierConfig(atr_period=1, profit_target_atr_mult=3.0, stop_loss_atr_mult=3.0, max_holding_days=21),
    )
    assert labeled["barrier_outcome"].iloc[0] == 0.0
    assert labeled["barrier_type"].iloc[0] == "vertical"
    assert labeled["barrier_days"].iloc[0] == 21


def test_barrier_bear_success(temp_modules) -> None:
    _store, _ensemble, tb = temp_modules
    frame = _frame([100, 98, 96, 95], ["Bear", "Bear", "Bear", "Bear"])
    labeled = tb.apply_triple_barrier_labels(
        frame,
        close_col="price",
        high_col="high",
        low_col="low",
        config=tb.BarrierConfig(atr_period=1, profit_target_atr_mult=2.0, stop_loss_atr_mult=2.0, max_holding_days=3),
    )
    assert labeled["barrier_outcome"].iloc[0] == 1.0
    assert labeled["barrier_type"].iloc[0] == "upper"


def test_barrier_bear_stopped_out(temp_modules) -> None:
    _store, _ensemble, tb = temp_modules
    frame = _frame([100, 102, 103, 104], ["Bear", "Bear", "Bear", "Bear"])
    labeled = tb.apply_triple_barrier_labels(
        frame,
        close_col="price",
        high_col="high",
        low_col="low",
        config=tb.BarrierConfig(atr_period=1, profit_target_atr_mult=2.0, stop_loss_atr_mult=2.0, max_holding_days=3),
    )
    assert labeled["barrier_outcome"].iloc[0] == 0.0
    assert labeled["barrier_type"].iloc[0] == "lower"


def test_barrier_neutral_skipped(temp_modules) -> None:
    _store, _ensemble, tb = temp_modules
    frame = _frame([100, 101, 102], ["Neutral", "Neutral", "Neutral"])
    labeled = tb.apply_triple_barrier_labels(
        frame,
        close_col="price",
        high_col="high",
        low_col="low",
        config=tb.BarrierConfig(atr_period=1),
    )
    assert np.isnan(labeled["barrier_outcome"].iloc[0])


def test_barrier_custom_config(temp_modules) -> None:
    _store, _ensemble, tb = temp_modules
    frame = _frame([100, 101, 102], ["Bull", "Bull", "Bull"], atr=1.0)
    labeled = tb.apply_triple_barrier_labels(
        frame,
        close_col="price",
        high_col="high",
        low_col="low",
        config=tb.BarrierConfig(atr_period=1, profit_target_atr_mult=3.0, stop_loss_atr_mult=1.5, max_holding_days=10),
    )
    assert labeled["barrier_target"].iloc[0] == pytest.approx(106.0)
    assert labeled["barrier_stop"].iloc[0] == pytest.approx(97.0)


def test_barrier_insufficient_forward_data(temp_modules) -> None:
    _store, _ensemble, tb = temp_modules
    frame = pd.DataFrame(
        {
            "price": [100, 100, 100],
            "high": [101, 101, 101],
            "low": [99, 99, 99],
            "regime": ["Bull", "Bull", "Bull"],
        },
        index=pd.date_range("2026-01-01", periods=3, freq="D"),
    )
    labeled = tb.apply_triple_barrier_labels(
        frame,
        close_col="price",
        high_col="high",
        low_col="low",
        config=tb.BarrierConfig(atr_period=1, profit_target_atr_mult=3.0, stop_loss_atr_mult=3.0, max_holding_days=21),
    )
    assert labeled["barrier_type"].iloc[1] == "vertical"
    assert labeled["barrier_days"].iloc[1] == 1


def test_compute_atr_matches_expected(temp_modules) -> None:
    _store, _ensemble, tb = temp_modules
    high = pd.Series([10.0, 11.0, 12.0, 13.0])
    low = pd.Series([9.0, 10.0, 11.0, 12.0])
    close = pd.Series([9.5, 10.5, 11.5, 12.5])
    atr = tb.compute_atr(high, low, close, period=2)
    assert atr.iloc[1] == pytest.approx(1.25)
    assert atr.iloc[2] == pytest.approx(1.375)
    assert atr.iloc[3] == pytest.approx(1.4375)


def test_passthrough_analyst_confirms(temp_modules) -> None:
    _store, ensemble, _tb = temp_modules
    analyst = ensemble.PassthroughAnalyst()
    result = analyst.analyze("NVDA", {}, None)
    assert result.confidence == 1.0
    assert result.signal == "confirm"


def test_registry_register_and_list(temp_modules) -> None:
    _store, ensemble, _tb = temp_modules

    class ReadyAnalyst(ensemble.AnalystBase):
        @property
        def name(self) -> str:
            return "ready"

        def is_ready(self) -> bool:
            return True

        def analyze(self, ticker, features, regime_result):
            del ticker, features, regime_result
            return ensemble.AnalystResult(self.name, 0.8, "confirm")

        def train(self, labeled_frame, **kwargs):
            del labeled_frame, kwargs
            return {}

    class NotReadyAnalyst(ReadyAnalyst):
        @property
        def name(self) -> str:
            return "not_ready"

        def is_ready(self) -> bool:
            return False

    registry = ensemble.AnalystRegistry()
    registry.register(ReadyAnalyst())
    registry.register(NotReadyAnalyst())
    assert set(registry.list_analysts()) == {"ready", "not_ready"}
    assert [analyst.name for analyst in registry.ready_analysts()] == ["ready"]


def test_aggregate_all_confirm(temp_modules) -> None:
    _store, ensemble, _tb = temp_modules
    verdict = ensemble.aggregate_analysts(
        [
            ensemble.AnalystResult("one", 0.8, "confirm"),
            ensemble.AnalystResult("two", 0.8, "confirm"),
        ]
    )
    assert verdict.composite_confidence == pytest.approx(0.8)
    assert verdict.signal == "confirm"
    assert verdict.sizing_multiplier == 1.0


def test_aggregate_veto_below_threshold(temp_modules) -> None:
    _store, ensemble, _tb = temp_modules
    verdict = ensemble.aggregate_analysts([ensemble.AnalystResult("one", 0.3, "neutral")])
    assert verdict.signal == "veto"
    assert verdict.sizing_multiplier == 0.0


def test_aggregate_neutral_between_thresholds(temp_modules) -> None:
    _store, ensemble, _tb = temp_modules
    verdict = ensemble.aggregate_analysts([ensemble.AnalystResult("one", 0.55, "neutral")])
    assert verdict.signal == "neutral"
    assert 0.25 < verdict.sizing_multiplier < 1.0


def test_analyst_veto_overrides_high_confidence(temp_modules) -> None:
    _store, ensemble, _tb = temp_modules
    verdict = ensemble.aggregate_analysts(
        [
            ensemble.AnalystResult("one", 0.9, "confirm"),
            ensemble.AnalystResult("two", 0.95, "veto"),
        ]
    )
    assert verdict.signal == "veto"
    assert "two" in str(verdict.veto_reason)


def test_analyst_base_requires_methods(temp_modules) -> None:
    _store, ensemble, _tb = temp_modules

    class BrokenAnalyst(ensemble.AnalystBase):
        pass

    with pytest.raises(TypeError):
        BrokenAnalyst()


def _settings_runtime(store, ensemble):
    return {
        "get_setting": store.get_setting,
        "set_setting": store.set_setting,
        "get_all_settings": store.get_all_settings,
        "delete_setting": store.delete_setting,
        "get_registry": ensemble.get_registry,
    }


def test_ensemble_settings_get_put_roundtrip(temp_modules, monkeypatch) -> None:
    store, ensemble, _tb = temp_modules
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (_settings_runtime(store, ensemble), None))
    client = TestClient(create_app())
    put_response = client.put(
        "/regime/ensemble/settings",
        json={
            "ensemble_enabled": "true",
            "ensemble_veto_threshold": "0.45",
            "barrier_max_holding_days": "15",
            "meta_compute_backend": "local",
        },
    )
    assert put_response.status_code == 200
    get_response = client.get("/regime/ensemble/settings")
    assert get_response.status_code == 200
    payload = get_response.json()
    assert payload["ensemble_enabled"] == "true"
    assert payload["ensemble_veto_threshold"] == "0.45"
    assert payload["barrier_max_holding_days"] == "15"
    assert payload["meta_compute_backend"] == "local"


def test_ensemble_settings_rejects_unknown_prefix(temp_modules, monkeypatch) -> None:
    store, ensemble, _tb = temp_modules
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (_settings_runtime(store, ensemble), None))
    client = TestClient(create_app())
    response = client.put(
        "/regime/ensemble/settings",
        json={"dangerous_key": "boom", "ensemble_enabled": "true"},
    )
    assert response.status_code == 200
    assert store.get_setting("dangerous_key") is None
    assert store.get_setting("ensemble_enabled") == "true"


def test_ensemble_analysts_list_route(temp_modules, monkeypatch) -> None:
    store, ensemble, _tb = temp_modules

    class RouteAnalyst(ensemble.AnalystBase):
        @property
        def name(self) -> str:
            return "route_test"

        def is_ready(self) -> bool:
            return True

        def analyze(self, ticker, features, regime_result):
            del ticker, features, regime_result
            return ensemble.AnalystResult(self.name, 0.9, "confirm")

        def train(self, labeled_frame, **kwargs):
            del labeled_frame, kwargs
            return {}

    registry = ensemble.AnalystRegistry()
    registry.register(RouteAnalyst())
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: ({**_settings_runtime(store, ensemble), "get_registry": lambda: registry}, None))
    client = TestClient(create_app())
    response = client.get("/regime/ensemble/analysts")
    assert response.status_code == 200
    payload = response.json()
    assert payload["analysts"] == [{"name": "route_test", "ready": True}]
