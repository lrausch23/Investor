from __future__ import annotations

import importlib
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.app.routes import regime as regime_route


@pytest.fixture
def temp_modules(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    import src.regime.persistence as store
    import src.regime.fundamental_data as fundamental_data
    import src.regime.fundamental_gating as gating
    import src.regime.discovery as discovery
    import src.regime.agents.fundamental_agent as fundamental_agent_module

    store = importlib.reload(store)
    store.DB_PATH = tmp_path / "regime_watch.db"
    fundamental_data = importlib.reload(fundamental_data)
    gating = importlib.reload(gating)
    discovery = importlib.reload(discovery)
    fundamental_agent_module = importlib.reload(fundamental_agent_module)
    fundamental_data.clear_cache()
    return store, fundamental_data, gating, discovery, fundamental_agent_module


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(regime_route.router)
    app.dependency_overrides[regime_route.require_actor] = lambda: "tester"
    app.dependency_overrides[regime_route.db_session] = lambda: iter([None])
    return TestClient(app)


def _frame(rows: dict[str, list[float]], columns: list[str] | None = None) -> pd.DataFrame:
    cols = columns or ["2025-12-31", "2024-12-31", "2023-12-31"]
    return pd.DataFrame(rows, index=cols).T


def _statements(fundamental_data, *, healthy: bool = True, years: int = 3):
    cols = ["2025-12-31", "2024-12-31", "2023-12-31"][:years]
    if healthy:
        income = _frame(
            {
                "Net Income": [120, 80, 70][:years],
                "Operating Income": [300, 240, 200][:years],
                "Pretax Income": [250, 210, 190][:years],
                "Tax Provision": [50, 42, 38][:years],
                "Total Revenue": [800, 700, 640][:years],
                "Gross Profit": [400, 300, 250][:years],
                "Interest Expense": [10, 12, 12][:years],
            },
            cols,
        )
        balance = _frame(
            {
                "Total Assets": [1000, 950, 900][:years],
                "Total Debt": [100, 150, 180][:years],
                "Current Assets": [300, 240, 220][:years],
                "Current Liabilities": [100, 100, 100][:years],
                "Ordinary Shares Number": [100, 100, 101][:years],
            },
            cols,
        )
        cashflow = _frame(
            {
                "Operating Cash Flow": [150, 100, 90][:years],
            },
            cols,
        )
        info = {"beta": 0.8, "marketCap": 1_000_000_000}
    else:
        income = _frame(
            {
                "Net Income": [-50, 80, 90][:years],
                "Operating Income": [20, 40, 60][:years],
                "Pretax Income": [10, 50, 70][:years],
                "Tax Provision": [2, 10, 14][:years],
                "Total Revenue": [400, 500, 550][:years],
                "Gross Profit": [100, 170, 210][:years],
                "Interest Expense": [25, 20, 20][:years],
            },
            cols,
        )
        balance = _frame(
            {
                "Total Assets": [1000, 900, 850][:years],
                "Total Debt": [300, 200, 180][:years],
                "Current Assets": [100, 180, 200][:years],
                "Current Liabilities": [200, 150, 140][:years],
                "Ordinary Shares Number": [120, 100, 95][:years],
            },
            cols,
        )
        cashflow = _frame(
            {
                "Operating Cash Flow": [-10, 60, 70][:years],
            },
            cols,
        )
        info = {"beta": 1.3, "marketCap": 50_000_000}
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


def test_piotroski_perfect_score(temp_modules) -> None:
    _store, fundamental_data, gating, _discovery, _agent_module = temp_modules
    result = gating.calculate_piotroski_f_score("NVDA", statements=_statements(fundamental_data, healthy=True))
    assert result.score == 9
    assert all(value == 1 for value in result.components.values())


def test_piotroski_zero_score(temp_modules) -> None:
    _store, fundamental_data, gating, _discovery, _agent_module = temp_modules
    result = gating.calculate_piotroski_f_score("NVDA", statements=_statements(fundamental_data, healthy=False))
    assert result.score == 1
    assert result.components["ocf_exceeds_net_income"] == 1


def test_piotroski_partial_data(temp_modules) -> None:
    _store, fundamental_data, gating, _discovery, _agent_module = temp_modules
    result = gating.calculate_piotroski_f_score("NVDA", statements=_statements(fundamental_data, healthy=True, years=1))
    assert result.data_quality == "insufficient"
    assert result.years_used == 1


def test_piotroski_mixed_score(temp_modules) -> None:
    _store, fundamental_data, gating, _discovery, _agent_module = temp_modules
    statements = _statements(fundamental_data, healthy=True)
    statements.balance_sheet.loc["Ordinary Shares Number", "2025-12-31"] = 101
    statements.income_statement.loc["Gross Profit", "2025-12-31"] = 280
    statements.cashflow.loc["Operating Cash Flow", "2025-12-31"] = 60
    statements.balance_sheet.loc["Total Debt", "2025-12-31"] = 170
    result = gating.calculate_piotroski_f_score("NVDA", statements=statements)
    assert result.score == 5


def test_piotroski_empty_statements(temp_modules) -> None:
    _store, fundamental_data, gating, _discovery, _agent_module = temp_modules
    empty = fundamental_data.FinancialStatements("NVDA", pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}, 0.0)
    result = gating.calculate_piotroski_f_score("NVDA", statements=empty)
    assert result.data_quality == "insufficient"


def test_roic_exceeds_wacc(temp_modules) -> None:
    _store, fundamental_data, gating, _discovery, _agent_module = temp_modules
    result = gating.calculate_roic("NVDA", statements=_statements(fundamental_data, healthy=True))
    assert result.roic_exceeds_wacc is True
    assert result.roic_avg is not None and result.roic_avg > result.wacc_estimate


def test_roic_below_wacc(temp_modules) -> None:
    _store, fundamental_data, gating, _discovery, _agent_module = temp_modules
    result = gating.calculate_roic("NVDA", statements=_statements(fundamental_data, healthy=False))
    assert result.roic_exceeds_wacc is False


def test_roic_trailing_average(temp_modules) -> None:
    _store, fundamental_data, gating, _discovery, _agent_module = temp_modules
    result = gating.calculate_roic("NVDA", statements=_statements(fundamental_data, healthy=True), lookback_years=3)
    assert result.roic_avg == pytest.approx(sum(result.roic_by_year.values()) / 3, rel=1e-3)


def test_roic_insufficient_data(temp_modules) -> None:
    _store, fundamental_data, gating, _discovery, _agent_module = temp_modules
    empty = fundamental_data.FinancialStatements("NVDA", pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}, 0.0)
    result = gating.calculate_roic("NVDA", statements=empty)
    assert result.data_quality == "insufficient"


def test_gate_passes_healthy_company(temp_modules) -> None:
    _store, fundamental_data, gating, _discovery, _agent_module = temp_modules
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(gating, "fetch_financial_statements", lambda ticker: _statements(fundamental_data, healthy=True))
        result = gating.run_fundamental_gate("NVDA")
    assert result.passed is True
    assert result.veto_reasons == []


def test_gate_fails_low_piotroski(temp_modules) -> None:
    _store, fundamental_data, gating, _discovery, _agent_module = temp_modules
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(gating, "fetch_financial_statements", lambda ticker: _statements(fundamental_data, healthy=False))
        result = gating.run_fundamental_gate("NVDA")
    assert result.passed is False
    assert any("Piotroski" in reason for reason in result.veto_reasons)


def test_gate_fails_low_roic(temp_modules) -> None:
    _store, fundamental_data, gating, _discovery, _agent_module = temp_modules
    statements = _statements(fundamental_data, healthy=True)
    statements.income_statement.loc["Operating Income", "2025-12-31"] = 10
    statements.income_statement.loc["Operating Income", "2024-12-31"] = 12
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(gating, "fetch_financial_statements", lambda ticker: statements)
        result = gating.run_fundamental_gate("NVDA", piotroski_min=0)
    assert result.passed is False
    assert any("ROIC" in reason for reason in result.veto_reasons)


def test_gate_passes_on_insufficient_data(temp_modules) -> None:
    _store, fundamental_data, gating, _discovery, _agent_module = temp_modules
    empty = fundamental_data.FinancialStatements("NVDA", pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}, 0.0)
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(gating, "fetch_financial_statements", lambda ticker: empty)
        allowed = gating.run_fundamental_gate("NVDA", pass_on_insufficient_data=True)
        blocked = gating.run_fundamental_gate("NVDA", pass_on_insufficient_data=False)
    assert allowed.passed is True
    assert blocked.passed is False


def test_check_entry_signals_blocks_failing_gate(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    store, _fundamental_data, gating, discovery, _agent_module = temp_modules
    theme = store.create_theme("AI", conviction=5)
    item = store.upsert_watchlist_candidate(theme["id"], "NVDA", regime_label="Bull", regime_probability=0.9, crowd_score=20)
    monkeypatch.setattr(gating, "run_fundamental_gate", lambda *args, **kwargs: gating.FundamentalGateResult("NVDA", False, None, None, ["bad"]))
    signals = discovery.check_entry_signals(theme["id"])
    assert signals == []
    refreshed = store.get_watchlist_entry(item["id"])
    assert refreshed["status"] == "Watching"


def test_check_entry_signals_passes_good_gate(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    store, fundamental_data, gating, discovery, _agent_module = temp_modules
    theme = store.create_theme("AI", conviction=5)
    item = store.upsert_watchlist_candidate(theme["id"], "NVDA", regime_label="Bull", regime_probability=0.9, crowd_score=20)
    gate_result = gating.FundamentalGateResult("NVDA", True, gating.calculate_piotroski_f_score("NVDA", statements=_statements(fundamental_data, healthy=True)), gating.calculate_roic("NVDA", statements=_statements(fundamental_data, healthy=True)), [])
    monkeypatch.setattr(gating, "run_fundamental_gate", lambda *args, **kwargs: gate_result)
    signals = discovery.check_entry_signals(theme["id"])
    assert len(signals) == 1
    assert signals[0]["status"] == "Entry Signal"
    refreshed = store.get_watchlist_entry(item["id"])
    assert refreshed["status"] == "Entry Signal"


def test_watchlist_gate_columns_persisted(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    store, fundamental_data, gating, discovery, _agent_module = temp_modules
    theme = store.create_theme("AI", conviction=5)
    item = store.upsert_watchlist_candidate(theme["id"], "NVDA", regime_label="Bull", regime_probability=0.9, crowd_score=20)
    gate_result = gating.FundamentalGateResult("NVDA", True, gating.calculate_piotroski_f_score("NVDA", statements=_statements(fundamental_data, healthy=True)), gating.calculate_roic("NVDA", statements=_statements(fundamental_data, healthy=True)), [])
    monkeypatch.setattr(gating, "run_fundamental_gate", lambda *args, **kwargs: gate_result)
    discovery.check_entry_signals(theme["id"])
    refreshed = store.get_watchlist_entry(item["id"])
    assert refreshed["fundamental_gate_passed"] == 1
    assert refreshed["piotroski_score"] == gate_result.piotroski.score
    assert refreshed["roic_pct"] == gate_result.roic.roic_avg


def _agent_runtime(llm_counter: dict[str, int], *, gate_enabled: str = "true") -> dict[str, object]:
    def build_qualitative_assessment(**kwargs):
        llm_counter["calls"] += 1
        return SimpleNamespace(
            catalyst_sentiment="Positive",
            catalysts=[],
            llm_response={"institutional_report": {"verdict": "Buy", "confidence_score": 7}},
            source="llm",
        )

    return {
        "get_setting": lambda key: gate_enabled if key == "fundamental_gate_enabled" else "auto" if key == "frontier_provider" else None,
        "build_qualitative_assessment": build_qualitative_assessment,
    }


def test_fundamental_agent_vetoes_on_gate_failure(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    _store, _fundamental_data, gating, _discovery, fundamental_agent_module = temp_modules
    llm_counter = {"calls": 0}
    monkeypatch.setattr(gating, "run_fundamental_gate", lambda *args, **kwargs: gating.FundamentalGateResult("NVDA", False, None, None, ["bad fundamentals"]))
    monkeypatch.setattr(gating, "get_fundamental_gate_settings", lambda: {"piotroski_min": 6, "require_roic_above_wacc": True, "roic_lookback_years": 3, "pass_on_insufficient_data": True, "gate_enabled": True})
    agent = fundamental_agent_module.FundamentalAgent(SimpleNamespace(), runtime=_agent_runtime(llm_counter))
    result = agent._evaluate(_agent_runtime(llm_counter), SimpleNamespace(correlation_id="c1", ticker="NVDA", regime_label="Bull", benchmark="SOXX", regime_probability=0.8, composite_action="Buy", meta_labeler_score=0.6))
    assert result is not None and result.vetoed is True
    assert result.source == "fundamental_gating"
    assert llm_counter["calls"] == 0


def test_fundamental_agent_proceeds_on_gate_pass(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    _store, _fundamental_data, gating, _discovery, fundamental_agent_module = temp_modules
    llm_counter = {"calls": 0}
    monkeypatch.setattr(gating, "run_fundamental_gate", lambda *args, **kwargs: gating.FundamentalGateResult("NVDA", True, None, None, []))
    monkeypatch.setattr(gating, "get_fundamental_gate_settings", lambda: {"piotroski_min": 6, "require_roic_above_wacc": True, "roic_lookback_years": 3, "pass_on_insufficient_data": True, "gate_enabled": True})
    agent = fundamental_agent_module.FundamentalAgent(SimpleNamespace(), runtime=_agent_runtime(llm_counter))
    result = agent._evaluate(_agent_runtime(llm_counter), SimpleNamespace(correlation_id="c1", ticker="NVDA", regime_label="Bull", benchmark="SOXX", regime_probability=0.8, composite_action="Buy", meta_labeler_score=0.6))
    assert result is not None and result.vetoed is False
    assert llm_counter["calls"] == 1


def test_fundamental_agent_proceeds_on_gate_error(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    _store, _fundamental_data, gating, _discovery, fundamental_agent_module = temp_modules
    llm_counter = {"calls": 0}
    monkeypatch.setattr(gating, "run_fundamental_gate", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")))
    monkeypatch.setattr(gating, "get_fundamental_gate_settings", lambda: {"piotroski_min": 6, "require_roic_above_wacc": True, "roic_lookback_years": 3, "pass_on_insufficient_data": True, "gate_enabled": True})
    agent = fundamental_agent_module.FundamentalAgent(SimpleNamespace(), runtime=_agent_runtime(llm_counter))
    result = agent._evaluate(_agent_runtime(llm_counter), SimpleNamespace(correlation_id="c1", ticker="NVDA", regime_label="Bull", benchmark="SOXX", regime_probability=0.8, composite_action="Buy", meta_labeler_score=0.6))
    assert result is not None and result.vetoed is False
    assert llm_counter["calls"] == 1


def test_fundamental_gate_diagnostic_route(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    _store, _fundamental_data, gating, _discovery, _agent_module = temp_modules
    monkeypatch.setattr(gating, "run_fundamental_gate", lambda *args, **kwargs: gating.FundamentalGateResult("NVDA", True, gating.PiotroskiResult("NVDA", 7, {}, {}, "full", 2), gating.ROICResult("NVDA", 12.0, 8.0, True, {"2025": 12.0}, "full"), []))
    monkeypatch.setattr(gating, "get_fundamental_gate_settings", lambda: {"piotroski_min": 6, "require_roic_above_wacc": True, "roic_lookback_years": 3, "pass_on_insufficient_data": True, "gate_enabled": True})
    client = _client()
    response = client.get("/regime/fundamental-gate/NVDA")
    assert response.status_code == 200
    payload = response.json()
    assert payload["ticker"] == "NVDA"
    assert payload["piotroski"]["score"] == 7
    assert payload["roic"]["roic_avg"] == 12.0


def test_fundamental_gate_settings_get_put(temp_modules) -> None:
    store, _fundamental_data, _gating, _discovery, _agent_module = temp_modules
    client = _client()
    response = client.get("/regime/fundamental-gate/settings")
    assert response.status_code == 200
    assert response.json()["piotroski_min"] == 6
    update = client.put(
        "/regime/fundamental-gate/settings",
        json={
            "piotroski_min": 7,
            "require_roic_above_wacc": False,
            "roic_lookback_years": 2,
            "pass_on_insufficient_data": False,
            "gate_enabled": False,
        },
    )
    assert update.status_code == 200
    payload = update.json()
    assert payload["piotroski_min"] == 7
    assert payload["require_roic_above_wacc"] is False
    assert payload["roic_lookback_years"] == 2
    assert payload["pass_on_insufficient_data"] is False
    assert payload["gate_enabled"] is False
    assert store.get_setting("fundamental_gate_enabled") == "false"


def test_gate_disabled_skips_checks(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    store, _fundamental_data, gating, discovery, fundamental_agent_module = temp_modules
    store.set_setting("fundamental_gate_enabled", "false")
    theme = store.create_theme("AI", conviction=5)
    store.upsert_watchlist_candidate(theme["id"], "NVDA", regime_label="Bull", regime_probability=0.9, crowd_score=20)
    monkeypatch.setattr(gating, "run_fundamental_gate", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("gate should be skipped")))
    signals = discovery.check_entry_signals(theme["id"])
    assert len(signals) == 1
    llm_counter = {"calls": 0}
    agent = fundamental_agent_module.FundamentalAgent(SimpleNamespace(), runtime=_agent_runtime(llm_counter, gate_enabled="false"))
    result = agent._evaluate(_agent_runtime(llm_counter, gate_enabled="false"), SimpleNamespace(correlation_id="c1", ticker="NVDA", regime_label="Bull", benchmark="SOXX", regime_probability=0.8, composite_action="Buy", meta_labeler_score=0.6))
    assert result is not None and result.vetoed is False
    assert llm_counter["calls"] == 1
