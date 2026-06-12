from __future__ import annotations

import importlib
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.app.routes import regime as regime_route
from src.regime.event_bus import get_event_bus, reset_event_bus
from src.regime.events import EnrichedSignalEvent, FundamentalAssessmentEvent


@pytest.fixture
def temp_modules(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    import src.regime.persistence as store
    import src.regime.fundamental_data as fundamental_data
    import src.regime.fundamental_gating as gating
    import src.regime.discovery as discovery
    import src.regime.agents.fundamental_agent as fundamental_agent_module
    import src.regime.llm_layer as llm

    store = importlib.reload(store)
    store.DB_PATH = tmp_path / "regime_watch.db"
    fundamental_data = importlib.reload(fundamental_data)
    gating = importlib.reload(gating)
    discovery = importlib.reload(discovery)
    monkeypatch.setattr(discovery, "universe_screen_enabled", lambda: False)
    fundamental_agent_module = importlib.reload(fundamental_agent_module)
    llm = importlib.reload(llm)
    fundamental_data.clear_cache()
    reset_event_bus()
    yield store, fundamental_data, gating, discovery, fundamental_agent_module, llm
    reset_event_bus()


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(regime_route.router)
    app.dependency_overrides[regime_route.require_actor] = lambda: "tester"
    app.dependency_overrides[regime_route.db_session] = lambda: iter([None])
    return TestClient(app)


def _frame(rows: dict[str, list[float]], columns: list[str] | None = None) -> pd.DataFrame:
    cols = columns or ["2025-12-31", "2024-12-31", "2023-12-31"]
    return pd.DataFrame(rows, index=cols).T


def _statements(fundamental_data, profile: str):
    cols = ["2025-12-31", "2024-12-31", "2023-12-31"]
    if profile == "safe":
        income = _frame(
            {
                "Net Income": [120, 100, 90],
                "Operating Income": [150, 130, 120],
                "Pretax Income": [140, 120, 110],
                "Tax Provision": [28, 24, 22],
                "Total Revenue": [1200, 1100, 1000],
                "Gross Profit": [700, 620, 560],
                "Interest Expense": [8, 10, 12],
            },
            cols,
        )
        balance = _frame(
            {
                "Total Assets": [1000, 950, 900],
                "Current Assets": [500, 450, 420],
                "Current Liabilities": [100, 110, 120],
                "Retained Earnings": [200, 160, 120],
                "Total Liabilities Net Minority Interest": [300, 320, 340],
                "Total Debt": [80, 90, 110],
                "Ordinary Shares Number": [100, 100, 101],
            },
            cols,
        )
        cashflow = _frame({"Operating Cash Flow": [180, 150, 130]}, cols)
        info = {"beta": 0.8, "marketCap": 1_000}
    elif profile == "distress":
        income = _frame(
            {
                "Net Income": [-20, -10, 5],
                "Operating Income": [-40, -20, 10],
                "Pretax Income": [-45, -25, 8],
                "Tax Provision": [0, 0, 1],
                "Total Revenue": [300, 320, 330],
                "Gross Profit": [120, 130, 135],
                "Interest Expense": [40, 35, 30],
            },
            cols,
        )
        balance = _frame(
            {
                "Total Assets": [1000, 980, 960],
                "Current Assets": [100, 110, 120],
                "Current Liabilities": [300, 290, 280],
                "Retained Earnings": [-150, -120, -100],
                "Total Liabilities Net Minority Interest": [900, 860, 820],
                "Total Debt": [500, 470, 430],
                "Ordinary Shares Number": [120, 115, 110],
            },
            cols,
        )
        cashflow = _frame({"Operating Cash Flow": [-10, -5, 12]}, cols)
        info = {"beta": 1.5, "marketCap": 50}
    elif profile == "grey":
        income = _frame(
            {
                "Net Income": [60, 50, 45],
                "Operating Income": [80, 70, 65],
                "Pretax Income": [75, 65, 60],
                "Tax Provision": [15, 13, 12],
                "Total Revenue": [800, 760, 740],
                "Gross Profit": [320, 300, 288],
                "Interest Expense": [18, 18, 18],
            },
            cols,
        )
        balance = _frame(
            {
                "Total Assets": [1000, 980, 950],
                "Current Assets": [250, 240, 230],
                "Current Liabilities": [100, 100, 100],
                "Retained Earnings": [100, 90, 80],
                "Total Liabilities Net Minority Interest": [400, 410, 420],
                "Total Debt": [150, 160, 170],
                "Ordinary Shares Number": [100, 100, 100],
            },
            cols,
        )
        cashflow = _frame({"Operating Cash Flow": [90, 80, 75]}, cols)
        info = {"beta": 1.0, "marketCap": 600}
    elif profile == "healthy_but_distressed":
        income = _frame(
            {
                "Net Income": [100, 80, 70],
                "Operating Income": [120, 100, 90],
                "Pretax Income": [115, 96, 85],
                "Tax Provision": [23, 19, 17],
                "Total Revenue": [100, 90, 85],
                "Gross Profit": [80, 70, 64],
                "Interest Expense": [4, 5, 6],
            },
            cols,
        )
        balance = _frame(
            {
                "Total Assets": [1000, 900, 850],
                "Current Assets": [100, 80, 70],
                "Current Liabilities": [300, 300, 300],
                "Retained Earnings": [50, 40, 30],
                "Total Liabilities Net Minority Interest": [900, 820, 780],
                "Total Debt": [100, 150, 180],
                "Ordinary Shares Number": [100, 100, 100],
            },
            cols,
        )
        cashflow = _frame({"Operating Cash Flow": [110, 95, 80]}, cols)
        info = {"beta": 0.9, "marketCap": 20}
    else:
        raise ValueError(profile)

    return fundamental_data.FinancialStatements(
        ticker="NVDA",
        income_statement=income,
        balance_sheet=balance,
        cashflow=cashflow,
        quarterly_income=pd.DataFrame(),
        quarterly_balance_sheet=pd.DataFrame(),
        quarterly_cashflow=pd.DataFrame(),
        info=info,
        fetched_at=0.0,
    )


def _agent_runtime(llm_response: dict[str, object], *, gate_enabled: str = "true") -> dict[str, object]:
    return {
        "get_setting": lambda key: gate_enabled if key == "fundamental_gate_enabled" else "auto" if key == "frontier_provider" else None,
        "build_qualitative_assessment": lambda **kwargs: SimpleNamespace(
            catalyst_sentiment="Positive",
            catalysts=[],
            llm_response=llm_response,
            source="llm",
        ),
    }


def test_fundamental_agent_uses_per_agent_frontier_model(temp_modules) -> None:
    store, _fundamental_data, _gating, _discovery, agent_module, _llm = temp_modules
    store.set_setting("fundamental_gate_enabled", "false")
    store.set_setting("agent_frontier_provider_quant", "ollama")
    store.set_setting("agent_frontier_model_quant", "deepseek-v4-pro:cloud")
    calls: list[dict[str, object]] = []
    runtime = {
        "get_setting": store.get_setting,
        "build_qualitative_assessment": lambda **kwargs: calls.append(kwargs) or SimpleNamespace(
            catalyst_sentiment="Positive",
            catalysts=[],
            llm_response={"institutional_report": {"verdict": "Buy", "confidence_score": 8, "moat_classification": "Network", "moat_justification": "Scale"}},
            source="llm",
            frontier_provider=kwargs["frontier_provider"],
            frontier_model=kwargs["frontier_model"],
            model_name="Ollama: deepseek-v4-pro:cloud",
            llm_used=True,
        ),
    }
    agent = agent_module.FundamentalAgent(SimpleNamespace(), runtime=runtime)
    event = EnrichedSignalEvent(
        ticker="NVDA",
        source="quant_agent",
        benchmark="SOXX",
        regime_label="Bull",
        composite_action="Buy",
        regime_probability=0.82,
        meta_labeler_score=0.72,
    )

    result = agent._evaluate(runtime, event, portfolio_id=12, agent_key="quant")

    assert calls[0]["frontier_provider"] == "ollama"
    assert calls[0]["frontier_model"] == "deepseek-v4-pro:cloud"
    assert result is not None
    assert result.agent_key == "quant"
    assert result.portfolio_id == 12
    assert result.llm_used is True
    assert result.llm_influenced is True
    assert result.llm_influence == "confirmed"
    assert result.llm_model_display == "Ollama: deepseek-v4-pro:cloud"


def test_altman_z_safe_zone(temp_modules) -> None:
    _store, fundamental_data, gating, _discovery, _agent, _llm = temp_modules
    result = gating.calculate_altman_z_score("NVDA", statements=_statements(fundamental_data, "safe"))
    assert result.z_score is not None and result.z_score > 2.99
    assert result.interpretation == "Safe"


def test_altman_z_distress_zone(temp_modules) -> None:
    _store, fundamental_data, gating, _discovery, _agent, _llm = temp_modules
    result = gating.calculate_altman_z_score("NVDA", statements=_statements(fundamental_data, "distress"))
    assert result.z_score is not None and result.z_score < 1.81
    assert result.interpretation == "Distress"


def test_altman_z_grey_zone(temp_modules) -> None:
    _store, fundamental_data, gating, _discovery, _agent, _llm = temp_modules
    result = gating.calculate_altman_z_score("NVDA", statements=_statements(fundamental_data, "grey"))
    assert result.z_score is not None and 1.81 <= result.z_score <= 2.99
    assert result.interpretation == "Grey Zone"


def test_altman_z_insufficient_data(temp_modules) -> None:
    _store, fundamental_data, gating, _discovery, _agent, _llm = temp_modules
    empty = fundamental_data.FinancialStatements("NVDA", pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}, 0.0)
    result = gating.calculate_altman_z_score("NVDA", statements=empty)
    assert result.data_quality == "insufficient"
    assert result.z_score is None


def test_altman_z_components_correct(temp_modules) -> None:
    _store, fundamental_data, gating, _discovery, _agent, _llm = temp_modules
    result = gating.calculate_altman_z_score("NVDA", statements=_statements(fundamental_data, "safe"))
    assert result.components["X1_working_capital_ta"] == pytest.approx(0.4, rel=1e-4)
    assert result.components["X2_retained_earnings_ta"] == pytest.approx(0.2, rel=1e-4)
    assert result.components["X3_ebit_ta"] == pytest.approx(0.15, rel=1e-4)
    assert result.components["X5_sales_ta"] == pytest.approx(1.2, rel=1e-4)


def test_gate_vetoes_distressed_company(temp_modules) -> None:
    _store, fundamental_data, gating, _discovery, _agent, _llm = temp_modules
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(gating, "fetch_financial_statements", lambda ticker: _statements(fundamental_data, "healthy_but_distressed"))
        result = gating.run_fundamental_gate("NVDA")
    assert result.passed is False
    assert result.altman_z is not None and result.altman_z.interpretation == "Distress"
    assert any("Altman Z-Score" in reason for reason in result.veto_reasons)


def test_gate_passes_safe_z_score(temp_modules) -> None:
    _store, fundamental_data, gating, _discovery, _agent, _llm = temp_modules
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(gating, "fetch_financial_statements", lambda ticker: _statements(fundamental_data, "safe"))
        result = gating.run_fundamental_gate("NVDA")
    assert result.passed is True
    assert result.altman_z is not None and result.altman_z.interpretation == "Safe"


def test_gate_z_score_disabled(temp_modules) -> None:
    _store, fundamental_data, gating, _discovery, _agent, _llm = temp_modules
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(gating, "fetch_financial_statements", lambda ticker: _statements(fundamental_data, "healthy_but_distressed"))
        result = gating.run_fundamental_gate("NVDA", altman_z_enabled=False)
    assert result.altman_z is not None and result.altman_z.interpretation == "Distress"
    assert not any("Altman Z-Score" in reason for reason in result.veto_reasons)


def test_prompt_includes_moat_section(temp_modules) -> None:
    _store, _fundamental_data, _gating, _discovery, _agent, llm = temp_modules
    prompt = llm.build_decision_prompt("NVDA", "Neutral", "Bull", 0.8, "Bull", [])
    assert "Competitive Moat Assessment" in prompt
    assert "Network Effect" in prompt
    assert "Cost Advantage" in prompt


def test_prompt_is_sector_agnostic(temp_modules) -> None:
    _store, _fundamental_data, _gating, _discovery, _agent, llm = temp_modules
    prompt = llm.build_decision_prompt("NVDA", "Neutral", "Bull", 0.8, "Bull", [])
    assert "Semiconductor" not in prompt
    assert "Physical AI" not in prompt
    assert "2026" not in prompt


def test_fallback_includes_moat_defaults(temp_modules) -> None:
    _store, _fundamental_data, _gating, _discovery, _agent, llm = temp_modules
    payload = llm._fallback_regime_validation("NVDA", "Neutral", "Bull", "Bull", 0.8, 0)
    assert payload["moat_classification"] == "none"
    assert "moat_justification" in payload


def test_deterministic_override_includes_moat_defaults(temp_modules) -> None:
    _store, _fundamental_data, _gating, _discovery, _agent, llm = temp_modules
    payload = llm._deterministic_defensive_response("NVDA", "Bear", 0.2, 0.3)
    report = payload["institutional_report"]
    assert report["moat_classification"] == "none"
    assert "moat_justification" in report


def test_moat_classification_extracted_from_llm(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    _store, _fundamental_data, gating, _discovery, agent_module, _llm = temp_modules
    monkeypatch.setattr(gating, "run_fundamental_gate", lambda *args, **kwargs: gating.FundamentalGateResult("NVDA", True, None, None, [], None))
    monkeypatch.setattr(gating, "get_fundamental_gate_settings", lambda: {"piotroski_min": 6, "require_roic_above_wacc": True, "roic_lookback_years": 3, "pass_on_insufficient_data": True, "gate_enabled": True, "altman_z_enabled": True, "altman_z_distress_threshold": 1.81})
    runtime = _agent_runtime({"institutional_report": {"verdict": "Buy", "confidence_score": 8, "moat_classification": "Intangibles", "moat_justification": "Durable IP portfolio."}})
    agent = agent_module.FundamentalAgent(SimpleNamespace(), runtime=runtime)
    result = agent._evaluate(runtime, SimpleNamespace(correlation_id="c1", ticker="NVDA", regime_label="Bull", benchmark="SOXX", regime_probability=0.8, composite_action="Buy", meta_labeler_score=0.6, source="quant_agent"))
    assert result is not None
    assert result.moat_classification == "Intangibles"
    assert result.moat_justification == "Durable IP portfolio."


def test_moat_veto_when_none(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    _store, _fundamental_data, gating, _discovery, agent_module, _llm = temp_modules
    monkeypatch.setattr(gating, "run_fundamental_gate", lambda *args, **kwargs: gating.FundamentalGateResult("NVDA", True, None, None, [], None))
    monkeypatch.setattr(gating, "get_fundamental_gate_settings", lambda: {"piotroski_min": 6, "require_roic_above_wacc": True, "roic_lookback_years": 3, "pass_on_insufficient_data": True, "gate_enabled": True, "altman_z_enabled": True, "altman_z_distress_threshold": 1.81})
    runtime = _agent_runtime({"institutional_report": {"verdict": "Hold", "confidence_score": 5, "moat_classification": "none", "moat_justification": "No durable moat visible."}})
    agent = agent_module.FundamentalAgent(SimpleNamespace(), runtime=runtime)
    result = agent._evaluate(runtime, SimpleNamespace(correlation_id="c1", ticker="NVDA", regime_label="Bull", benchmark="SOXX", regime_probability=0.8, composite_action="Buy", meta_labeler_score=0.6, source="quant_agent"))
    assert result is not None
    assert result.vetoed is True
    assert result.source == "moat_veto"


def test_event_has_moat_fields() -> None:
    event = FundamentalAssessmentEvent(ticker="NVDA", verdict="Buy", moat_classification="Intangibles", moat_justification="Brand and patents")
    payload = event.to_dict()
    assert payload["moat_classification"] == "Intangibles"
    assert payload["moat_justification"] == "Brand and patents"


def test_event_moat_fields_default_empty() -> None:
    event = FundamentalAssessmentEvent(ticker="NVDA", verdict="Buy")
    assert event.moat_classification == ""
    assert event.moat_justification == ""


def test_watchlist_z_score_columns_persisted(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    store, fundamental_data, gating, discovery, _agent, _llm = temp_modules
    theme = store.create_theme("AI", conviction=5)
    item = store.upsert_watchlist_candidate(theme["id"], "NVDA", regime_label="Bull", regime_probability=0.9, crowd_score=20)
    safe = gating.calculate_altman_z_score("NVDA", statements=_statements(fundamental_data, "safe"))
    gate_result = gating.FundamentalGateResult(
        "NVDA",
        True,
        gating.calculate_piotroski_f_score("NVDA", statements=_statements(fundamental_data, "safe")),
        gating.calculate_roic("NVDA", statements=_statements(fundamental_data, "safe")),
        [],
        safe,
    )
    monkeypatch.setattr(gating, "run_fundamental_gate", lambda *args, **kwargs: gate_result)
    discovery.check_entry_signals(theme["id"])
    refreshed = store.get_watchlist_entry(item["id"])
    assert refreshed["altman_z_score"] == safe.z_score
    assert refreshed["altman_z_interpretation"] == "Safe"


def test_check_entry_signals_blocks_distressed(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    store, _fundamental_data, gating, discovery, _agent, _llm = temp_modules
    theme = store.create_theme("AI", conviction=5)
    store.upsert_watchlist_candidate(theme["id"], "NVDA", regime_label="Bull", regime_probability=0.9, crowd_score=20)
    gate_result = gating.FundamentalGateResult("NVDA", False, None, None, ["Altman Z-Score 1.20 < 1.81 (Distress Zone)"], gating.AltmanZScoreResult("NVDA", 1.2, "Distress", {}, {}, "full"))
    monkeypatch.setattr(gating, "run_fundamental_gate", lambda *args, **kwargs: gate_result)
    assert discovery.check_entry_signals(theme["id"]) == []


def test_agent_vetoes_no_moat(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    _store, _fundamental_data, gating, _discovery, agent_module, _llm = temp_modules
    monkeypatch.setattr(gating, "run_fundamental_gate", lambda *args, **kwargs: gating.FundamentalGateResult("NVDA", True, None, None, [], None))
    monkeypatch.setattr(gating, "get_fundamental_gate_settings", lambda: {"piotroski_min": 6, "require_roic_above_wacc": True, "roic_lookback_years": 3, "pass_on_insufficient_data": True, "gate_enabled": True, "altman_z_enabled": True, "altman_z_distress_threshold": 1.81})
    runtime = _agent_runtime({"institutional_report": {"verdict": "Entry", "confidence_score": 7, "moat_classification": "none", "moat_justification": "Commoditized market."}})
    agent = agent_module.FundamentalAgent(SimpleNamespace(), runtime=runtime)
    result = agent._evaluate(runtime, SimpleNamespace(correlation_id="c2", ticker="NVDA", regime_label="Bull", benchmark="SOXX", regime_probability=0.8, composite_action="Buy", meta_labeler_score=0.6, source="quant_agent"))
    assert result is not None and result.vetoed is True


def test_agent_passes_with_moat(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    _store, _fundamental_data, gating, _discovery, agent_module, _llm = temp_modules
    monkeypatch.setattr(gating, "run_fundamental_gate", lambda *args, **kwargs: gating.FundamentalGateResult("NVDA", True, None, None, [], None))
    monkeypatch.setattr(gating, "get_fundamental_gate_settings", lambda: {"piotroski_min": 6, "require_roic_above_wacc": True, "roic_lookback_years": 3, "pass_on_insufficient_data": True, "gate_enabled": True, "altman_z_enabled": True, "altman_z_distress_threshold": 1.81})
    runtime = _agent_runtime({"institutional_report": {"verdict": "Entry", "confidence_score": 8, "moat_classification": "Cost Advantage", "moat_justification": "Scale and process efficiency."}})
    agent = agent_module.FundamentalAgent(SimpleNamespace(), runtime=runtime)
    result = agent._evaluate(runtime, SimpleNamespace(correlation_id="c3", ticker="NVDA", regime_label="Bull", benchmark="SOXX", regime_probability=0.8, composite_action="Buy", meta_labeler_score=0.6, source="quant_agent"))
    assert result is not None and result.vetoed is False
    assert result.moat_classification == "Cost Advantage"


def test_diagnostic_route_includes_z_score(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    _store, _fundamental_data, gating, _discovery, _agent, _llm = temp_modules
    gate_result = gating.FundamentalGateResult(
        "NVDA",
        True,
        gating.PiotroskiResult("NVDA", 7, {}, {}, "full", 3),
        gating.ROICResult("NVDA", 12.0, 8.0, True, {"2025": 12.0}, "full"),
        [],
        gating.AltmanZScoreResult("NVDA", 3.25, "Safe", {"X1_working_capital_ta": 0.4}, {}, "full"),
    )
    monkeypatch.setattr(gating, "run_fundamental_gate", lambda *args, **kwargs: gate_result)
    monkeypatch.setattr(gating, "get_fundamental_gate_settings", lambda: {"piotroski_min": 6, "require_roic_above_wacc": True, "roic_lookback_years": 3, "pass_on_insufficient_data": True, "gate_enabled": True, "altman_z_enabled": True, "altman_z_distress_threshold": 1.81})
    response = _client().get("/regime/fundamental-gate/NVDA")
    assert response.status_code == 200
    payload = response.json()
    assert payload["altman_z"]["z_score"] == 3.25
    assert payload["altman_z"]["interpretation"] == "Safe"


def test_settings_get_put_persists_altman_fields(temp_modules) -> None:
    store, _fundamental_data, _gating, _discovery, _agent, _llm = temp_modules
    client = _client()
    payload = client.get("/regime/fundamental-gate/settings").json()
    assert payload["altman_z_enabled"] is True
    assert payload["altman_z_distress_threshold"] == pytest.approx(1.81)
    response = client.put("/regime/fundamental-gate/settings", json={"altman_z_enabled": False, "altman_z_distress_threshold": 2.2})
    assert response.status_code == 200
    updated = response.json()
    assert updated["altman_z_enabled"] is False
    assert updated["altman_z_distress_threshold"] == pytest.approx(2.2)
    assert store.get_setting("fundamental_altman_z_enabled") == "false"
    assert store.get_setting("fundamental_altman_z_threshold") == "2.2"


def test_consensus_route_includes_moat(temp_modules) -> None:
    _store, _fundamental_data, _gating, _discovery, _agent, _llm = temp_modules
    bus = get_event_bus()
    bus.publish_sync(FundamentalAssessmentEvent(ticker="NVDA", verdict="Buy", moat_classification="Intangibles", moat_justification="Brand", source="llm"))
    response = _client().get("/regime/agents/consensus")
    assert response.status_code == 200
    payload = response.json()
    assert payload["consensus"]["NVDA"]["fundamental"]["moat"] == "Intangibles"
