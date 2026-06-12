from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest

from src.app.routes import regime as regime_route
from src.regime import discovery as discovery_module
from src.regime import llm_layer as llm_layer_module
from src.regime import signals as signals_module


@pytest.fixture(autouse=True)
def _disable_universe_screen(monkeypatch):
    monkeypatch.setattr(discovery_module, "universe_screen_enabled", lambda: False)


def test_position_size_meta_labeler_scales_risk_budget_anchor() -> None:
    base = signals_module.compute_position_size(
        regime_probability=0.60,
        composite_action="Buy",
        risk_reward_ratio=None,
        atr_value=2.0,
        current_price=100.0,
        portfolio_value=100000.0,
    )
    boosted = signals_module.compute_position_size(
        regime_probability=0.60,
        composite_action="Buy",
        risk_reward_ratio=None,
        atr_value=2.0,
        current_price=100.0,
        portfolio_value=100000.0,
        meta_labeler_probability=0.90,
    )
    assert base.suggested_pct == 50.0
    assert boosted.ml_sizing_multiplier == 0.95
    assert boosted.suggested_pct == 47.5


def test_position_size_meta_labeler_low_probability_uses_half_anchor_floor() -> None:
    sized = signals_module.compute_position_size(
        regime_probability=0.60,
        composite_action="Buy",
        risk_reward_ratio=None,
        atr_value=2.0,
        current_price=100.0,
        portfolio_value=100000.0,
        meta_labeler_probability=0.01,
    )
    assert sized.ml_sizing_multiplier == 0.505
    assert sized.suggested_pct == 25.2


def test_position_size_kelly_uses_meta_labeler_probability() -> None:
    sized = signals_module.compute_position_size(
        regime_probability=0.80,
        composite_action="Buy",
        risk_reward_ratio=2.0,
        atr_value=None,
        current_price=100.0,
        meta_labeler_probability=0.55,
    )
    assert sized.kelly_fraction is not None
    assert round(sized.kelly_fraction, 4) == 0.1625


def test_position_size_rationale_includes_ml_confidence() -> None:
    sized = signals_module.compute_position_size(
        regime_probability=0.70,
        composite_action="Buy",
        risk_reward_ratio=None,
        atr_value=2.0,
        current_price=100.0,
        portfolio_value=100000.0,
        meta_labeler_probability=0.64,
    )
    assert "ML multiplier" in sized.sizing_rationale
    assert sized.meta_labeler_probability == 0.64


def test_check_entry_signals_veto_skips_candidate(monkeypatch) -> None:
    candidate = {"id": 1, "ticker": "LSCC", "regime_label": "Bull", "regime_probability": 0.70, "crowd_score": 25}
    theme = {"id": 1, "status": "Active", "conviction": 4}
    regime_result = SimpleNamespace(price_frame=pd.DataFrame([{"canonical_state": 0, "return": 0.01, "volatility": 0.2, "volume_zscore": 1.0, "vix": 20.0, "vix_change": -0.1, "yield_10y": 4.0, "yield_10y_change": 0.0}]))
    monkeypatch.setattr(discovery_module, "get_theme", lambda theme_id: theme)
    monkeypatch.setattr(discovery_module, "list_themes", lambda include_closed=False: [theme])
    monkeypatch.setattr(discovery_module, "get_watchlist", lambda theme_id=None, status=None: [candidate])
    monkeypatch.setattr(discovery_module, "get_setting", lambda key: "false" if key == "fundamental_gate_enabled" else None)
    monkeypatch.setattr(discovery_module, "update_watchlist_status", lambda *args, **kwargs: {"id": 1, "status": "Entry Signal"})

    class StubMeta:
        def is_ready(self):
            return True

        def analyze(self, **kwargs):
            return SimpleNamespace(signal="veto", confidence=0.42, details={})

    triggered = discovery_module.check_entry_signals(
        1,
        meta_labeler_engine=StubMeta(),
        regime_results={"LSCC": regime_result},
    )
    assert triggered == []


def test_check_entry_signals_promotes_when_meta_confirms(monkeypatch) -> None:
    candidate = {"id": 1, "ticker": "LSCC", "regime_label": "Bull", "regime_probability": 0.70, "crowd_score": 25}
    theme = {"id": 1, "status": "Active", "conviction": 4}
    regime_result = SimpleNamespace(price_frame=pd.DataFrame([{"canonical_state": 0, "return": 0.01, "volatility": 0.2, "volume_zscore": 1.0, "vix": 20.0, "vix_change": -0.1, "yield_10y": 4.0, "yield_10y_change": 0.0}]))
    monkeypatch.setattr(discovery_module, "get_theme", lambda theme_id: theme)
    monkeypatch.setattr(discovery_module, "list_themes", lambda include_closed=False: [theme])
    monkeypatch.setattr(discovery_module, "get_watchlist", lambda theme_id=None, status=None: [candidate])
    monkeypatch.setattr(discovery_module, "get_setting", lambda key: "false" if key == "fundamental_gate_enabled" else None)
    monkeypatch.setattr(discovery_module, "update_watchlist_status", lambda *args, **kwargs: {"id": 1, "status": "Entry Signal"})

    class StubMeta:
        def is_ready(self):
            return True

        def analyze(self, **kwargs):
            return SimpleNamespace(signal="confirm", confidence=0.82, details={})

    triggered = discovery_module.check_entry_signals(
        1,
        meta_labeler_engine=StubMeta(),
        regime_results={"LSCC": regime_result},
    )
    assert len(triggered) == 1


def test_check_entry_signals_error_degrades_gracefully(monkeypatch) -> None:
    candidate = {"id": 1, "ticker": "LSCC", "regime_label": "Bull", "regime_probability": 0.70, "crowd_score": 25}
    theme = {"id": 1, "status": "Active", "conviction": 4}
    monkeypatch.setattr(discovery_module, "get_theme", lambda theme_id: theme)
    monkeypatch.setattr(discovery_module, "list_themes", lambda include_closed=False: [theme])
    monkeypatch.setattr(discovery_module, "get_watchlist", lambda theme_id=None, status=None: [candidate])
    monkeypatch.setattr(discovery_module, "get_setting", lambda key: "false" if key == "fundamental_gate_enabled" else None)
    monkeypatch.setattr(discovery_module, "update_watchlist_status", lambda *args, **kwargs: {"id": 1, "status": "Entry Signal"})

    class StubMeta:
        def is_ready(self):
            return True

        def analyze(self, **kwargs):
            raise RuntimeError("boom")

    triggered = discovery_module.check_entry_signals(1, meta_labeler_engine=StubMeta(), regime_results={})
    assert len(triggered) == 1


def test_check_entry_signals_not_ready_ignores_meta_labeler(monkeypatch) -> None:
    candidate = {"id": 1, "ticker": "LSCC", "regime_label": "Bull", "regime_probability": 0.70, "crowd_score": 25}
    theme = {"id": 1, "status": "Active", "conviction": 4}
    monkeypatch.setattr(discovery_module, "get_theme", lambda theme_id: theme)
    monkeypatch.setattr(discovery_module, "list_themes", lambda include_closed=False: [theme])
    monkeypatch.setattr(discovery_module, "get_watchlist", lambda theme_id=None, status=None: [candidate])
    monkeypatch.setattr(discovery_module, "get_setting", lambda key: "false" if key == "fundamental_gate_enabled" else None)
    monkeypatch.setattr(discovery_module, "update_watchlist_status", lambda *args, **kwargs: {"id": 1, "status": "Entry Signal"})

    class StubMeta:
        def is_ready(self):
            return False

    triggered = discovery_module.check_entry_signals(1, meta_labeler_engine=StubMeta(), regime_results={})
    assert len(triggered) == 1


def test_build_decision_prompt_includes_high_meta_section() -> None:
    prompt = llm_layer_module.build_decision_prompt("NVDA", "Neutral", "Bull", 0.8, "Bull", [], meta_labeler_score=0.82)
    assert "XGBoost Meta-Labeler Assessment" in prompt
    assert "Assessment: HIGH" in prompt


def test_build_decision_prompt_includes_moderate_meta_section() -> None:
    prompt = llm_layer_module.build_decision_prompt("NVDA", "Neutral", "Bull", 0.8, "Bull", [], meta_labeler_score=0.58)
    assert "Assessment: MODERATE" in prompt


def test_build_decision_prompt_includes_low_meta_section() -> None:
    prompt = llm_layer_module.build_decision_prompt("NVDA", "Neutral", "Bear", 0.8, "Bear", [], meta_labeler_score=0.40)
    assert "Assessment: LOW" in prompt


def test_build_decision_prompt_omits_meta_section_when_none() -> None:
    prompt = llm_layer_module.build_decision_prompt("NVDA", "Neutral", "Bull", 0.8, "Bull", [])
    assert "XGBoost Meta-Labeler Assessment" not in prompt


def test_build_qualitative_assessment_passes_meta_labeler_score(monkeypatch) -> None:
    captured: dict[str, float | None] = {}
    monkeypatch.setattr(llm_layer_module, "analyze_catalysts", lambda *args, **kwargs: ([], 0, "Neutral"))
    monkeypatch.setattr(llm_layer_module, "request_frontier_decision", lambda *args, **kwargs: None)

    def fake_prompt(*args, **kwargs):
        captured["meta"] = kwargs.get("meta_labeler_score")
        return "prompt"

    monkeypatch.setattr(llm_layer_module, "build_decision_prompt", fake_prompt)
    llm_layer_module.build_qualitative_assessment(
        ticker="NVDA",
        regime_signal="Bull detected",
        state_name="Bull",
        latest_probability=0.8,
        meta_labeler_score=0.67,
    )
    assert captured["meta"] == 0.67


def test_load_qualitative_result_passes_meta_labeler_score(monkeypatch) -> None:
    captured: dict[str, float | None] = {}
    runtime = {
        "save_regime_event": lambda ticker, label, state_id: {"previous_label": "Neutral"},
        "build_qualitative_assessment": lambda **kwargs: captured.update(meta=kwargs.get("meta_labeler_score")) or {"ok": True},
    }
    monkeypatch.setattr(regime_route, "load_qualitative_cache", lambda *args, **kwargs: None)
    monkeypatch.setattr(regime_route, "save_qualitative_cache", lambda *args, **kwargs: None)
    result, fresh = regime_route._load_qualitative_result(
        runtime,
        ticker="NVDA",
        state_id=0,
        regime_signal="Bull detected",
        state_name="Bull",
        latest_probability=0.8,
        benchmark="SPY",
        benchmark_state="Bull",
        frontier_provider="auto",
        frontier_enabled=True,
        force_refresh=True,
        meta_labeler_score=0.73,
    )
    assert fresh is True
    assert result == {"ok": True}
    assert captured["meta"] == 0.73


def test_dashboard_payload_includes_meta_labeler_fields(monkeypatch) -> None:
    from tests.test_regime_route import _fake_runtime, FakeRegime

    runtime = _fake_runtime()
    price_frame = pd.DataFrame(
        {
            "state_probability": [0.80, 0.84, 0.88, 0.91],
            "canonical_state": [0, 0, 0, 0],
            "return": [0.01, 0.02, 0.015, 0.012],
            "volatility": [0.2, 0.19, 0.18, 0.17],
            "volume_zscore": [1.0, 1.1, 1.2, 1.1],
            "vix": [20.0, 19.0, 18.0, 18.5],
            "vix_change": [-0.1, -0.1, -0.05, 0.01],
            "yield_10y": [4.0, 4.0, 3.9, 3.95],
            "yield_10y_change": [0.0, -0.01, -0.01, 0.01],
        }
    )
    runtime["fit_regime_model"] = lambda ticker, market_frame, training_window=504, refit_step=21: FakeRegime(ticker, "Bull", price_frame=price_frame)
    runtime["fit_regime_model_weekly"] = lambda ticker, market_frame: FakeRegime(ticker, "Bull", price_frame=price_frame)
    runtime["compute_position_size"] = signals_module.compute_position_size
    runtime["extract_meta_features"] = lambda row: {
        "hmm_state": float(row.get("canonical_state", 0.0)),
        "log_ret": float(row.get("return", 0.0)),
        "volatility": float(row.get("volatility", 0.0)),
        "vol_z": float(row.get("volume_zscore", 0.0)),
        "vix_level": float(row.get("vix", 0.0)),
        "vix_change": float(row.get("vix_change", 0.0)),
        "yield_10y_level": float(row.get("yield_10y", 0.0)),
        "yield_10y_change": float(row.get("yield_10y_change", 0.0)),
    }

    class StubMeta:
        name = "xgboost_meta_labeler"

        def is_ready(self):
            return True

        def analyze(self, **kwargs):
            return SimpleNamespace(signal="confirm", confidence=0.74, details={"probability_of_success": 0.74})

    runtime["get_registry"] = lambda: SimpleNamespace(get=lambda name: StubMeta() if name == "xgboost_meta_labeler" else None)
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    monkeypatch.setattr(regime_route, "load_previous_payload", lambda: None)
    monkeypatch.setattr(regime_route, "_portfolio_tickers", lambda runtime, session, show_all, portfolio_scope, account_id: ("/tmp/investor.db", ["NVDA"]))
    payload = regime_route._build_regime_dashboard_payload(frontier_enabled=False, tickers=["NVDA"])
    row = payload["rows"][0]
    assert payload["ensemble_status"]["meta_labeler_active"] is True
    assert row["meta_labeler_probability"] == 0.74
    assert row["meta_labeler_signal"] == "confirm"
    assert row["signal_diagnostics"]["meta_labeler_probability"] == 0.74
    assert row["signal_diagnostics"]["meta_labeler_signal"] == "confirm"


def test_dashboard_payload_position_size_uses_meta_labeler_probability(monkeypatch) -> None:
    from tests.test_regime_route import _fake_runtime, FakeRegime

    runtime = _fake_runtime()
    price_frame = pd.DataFrame(
        {
            "state_probability": [0.80, 0.84, 0.88, 0.91],
            "canonical_state": [0, 0, 0, 0],
            "return": [0.01, 0.02, 0.015, 0.012],
            "volatility": [0.2, 0.19, 0.18, 0.17],
            "volume_zscore": [1.0, 1.1, 1.2, 1.1],
            "vix": [20.0, 19.0, 18.0, 18.5],
            "vix_change": [-0.1, -0.1, -0.05, 0.01],
            "yield_10y": [4.0, 4.0, 3.9, 3.95],
            "yield_10y_change": [0.0, -0.01, -0.01, 0.01],
        }
    )
    runtime["fit_regime_model"] = lambda ticker, market_frame, training_window=504, refit_step=21: FakeRegime(ticker, "Bull", latest_probability=0.90, price_frame=price_frame)
    runtime["fit_regime_model_weekly"] = lambda ticker, market_frame: FakeRegime(ticker, "Bull", latest_probability=0.90, price_frame=price_frame)
    runtime["compute_position_size"] = signals_module.compute_position_size
    runtime["extract_meta_features"] = lambda row: {
        "hmm_state": float(row.get("canonical_state", 0.0)),
        "log_ret": float(row.get("return", 0.0)),
        "volatility": float(row.get("volatility", 0.0)),
        "vol_z": float(row.get("volume_zscore", 0.0)),
        "vix_level": float(row.get("vix", 0.0)),
        "vix_change": float(row.get("vix_change", 0.0)),
        "yield_10y_level": float(row.get("yield_10y", 0.0)),
        "yield_10y_change": float(row.get("yield_10y_change", 0.0)),
    }

    class StubMeta:
        name = "xgboost_meta_labeler"

        def is_ready(self):
            return True

        def analyze(self, **kwargs):
            return SimpleNamespace(signal="neutral", confidence=0.45, details={"probability_of_success": 0.45})

    runtime["get_registry"] = lambda: SimpleNamespace(get=lambda name: StubMeta() if name == "xgboost_meta_labeler" else None)
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    monkeypatch.setattr(regime_route, "load_previous_payload", lambda: None)
    monkeypatch.setattr(regime_route, "_portfolio_tickers", lambda runtime, session, show_all, portfolio_scope, account_id: ("/tmp/investor.db", ["NVDA"]))
    payload = regime_route._build_regime_dashboard_payload(frontier_enabled=False, tickers=["NVDA"])
    row = payload["rows"][0]
    assert row["position_size"]["meta_labeler_probability"] == 0.45
    assert row["position_size"]["suggested_pct"] < 90.0


def test_dashboard_payload_frontier_uses_meta_score(monkeypatch) -> None:
    from tests.test_regime_route import _fake_runtime, FakeRegime

    runtime = _fake_runtime()
    price_frame = pd.DataFrame(
        {
            "state_probability": [0.80, 0.84, 0.88, 0.91],
            "canonical_state": [0, 0, 0, 0],
            "return": [0.01, 0.02, 0.015, 0.012],
            "volatility": [0.2, 0.19, 0.18, 0.17],
            "volume_zscore": [1.0, 1.1, 1.2, 1.1],
            "vix": [20.0, 19.0, 18.0, 18.5],
            "vix_change": [-0.1, -0.1, -0.05, 0.01],
            "yield_10y": [4.0, 4.0, 3.9, 3.95],
            "yield_10y_change": [0.0, -0.01, -0.01, 0.01],
        }
    )
    runtime["fit_regime_model"] = lambda ticker, market_frame, training_window=504, refit_step=21: FakeRegime(ticker, "Bull", price_frame=price_frame)
    runtime["fit_regime_model_weekly"] = lambda ticker, market_frame: FakeRegime(ticker, "Bull", price_frame=price_frame)
    captured: dict[str, float | None] = {}
    runtime["build_qualitative_assessment"] = lambda **kwargs: captured.update(meta=kwargs.get("meta_labeler_score")) or {"verdict": "Hold", "confidence_score": 7}
    runtime["extract_meta_features"] = lambda row: {
        "hmm_state": float(row.get("canonical_state", 0.0)),
        "log_ret": float(row.get("return", 0.0)),
        "volatility": float(row.get("volatility", 0.0)),
        "vol_z": float(row.get("volume_zscore", 0.0)),
        "vix_level": float(row.get("vix", 0.0)),
        "vix_change": float(row.get("vix_change", 0.0)),
        "yield_10y_level": float(row.get("yield_10y", 0.0)),
        "yield_10y_change": float(row.get("yield_10y_change", 0.0)),
    }

    class StubMeta:
        name = "xgboost_meta_labeler"

        def is_ready(self):
            return True

        def analyze(self, **kwargs):
            return SimpleNamespace(signal="confirm", confidence=0.66, details={"probability_of_success": 0.66})

    runtime["get_registry"] = lambda: SimpleNamespace(get=lambda name: StubMeta() if name == "xgboost_meta_labeler" else None)
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    monkeypatch.setattr(regime_route, "load_previous_payload", lambda: None)
    monkeypatch.setattr(regime_route, "_portfolio_tickers", lambda runtime, session, show_all, portfolio_scope, account_id: ("/tmp/investor.db", ["NVDA"]))
    monkeypatch.setattr(regime_route, "load_qualitative_cache", lambda *args, **kwargs: None)
    monkeypatch.setattr(regime_route, "save_qualitative_cache", lambda *args, **kwargs: None)
    regime_route._build_regime_dashboard_payload(frontier_enabled=True, force_refresh=True, tickers=["NVDA"])
    assert captured["meta"] == 0.66
