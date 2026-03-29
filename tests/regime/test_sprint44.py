from __future__ import annotations

import importlib
import json

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.app.routes import regime as regime_route
from src.regime import attribution as attribution_module
from src.regime import paper_trading as paper_trading_module
from src.regime import persistence as persistence_module


@pytest.fixture()
def temp_modules(tmp_path, monkeypatch):
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    store = importlib.reload(persistence_module)
    monkeypatch.setattr(store, "DB_PATH", tmp_path / "regime_watch.db")
    paper = importlib.reload(paper_trading_module)
    attribution = importlib.reload(attribution_module)
    return store, paper, attribution


def _portfolio(store):
    return store.create_paper_portfolio("Sandbox", 100000.0)


def _route_client(monkeypatch, runtime: dict) -> TestClient:
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    app = FastAPI()
    app.include_router(regime_route.router)
    app.dependency_overrides[regime_route.require_actor] = lambda: "tester"
    app.dependency_overrides[regime_route.db_session] = lambda: iter([None])
    return TestClient(app)


def test_theme_attribution_groups_by_theme(temp_modules, monkeypatch) -> None:
    store, _paper, attribution = temp_modules
    portfolio = _portfolio(store)
    ai = store.create_theme("AI", conviction=4)
    infra = store.create_theme("Infra", conviction=3)
    store.open_paper_position(portfolio["id"], "NVDA", 10, 100.0, "2026-03-01", theme_id=ai["id"])
    store.open_paper_position(portfolio["id"], "AVGO", 5, 100.0, "2026-03-01", theme_id=infra["id"])
    store.open_paper_position(portfolio["id"], "TSM", 2, 100.0, "2026-03-01", theme_id=None)
    monkeypatch.setattr(attribution, "_batch_current_prices", lambda tickers: {"NVDA": 120.0, "AVGO": 80.0, "TSM": 105.0})
    payload = attribution.compute_theme_attribution(portfolio["id"])
    assert {row["theme_name"] for row in payload["themes"]} == {"AI", "Infra", "Unassigned"}
    assert payload["theme_count"] == 3


def test_theme_attribution_win_rate_calculation(temp_modules, monkeypatch) -> None:
    store, _paper, attribution = temp_modules
    portfolio = _portfolio(store)
    theme = store.create_theme("AI", conviction=4)
    pos1 = store.open_paper_position(portfolio["id"], "NVDA", 10, 100.0, "2026-03-01", theme_id=theme["id"])
    pos2 = store.open_paper_position(portfolio["id"], "AVGO", 10, 100.0, "2026-03-02", theme_id=theme["id"])
    store.close_paper_position(pos1["id"], 120.0, "2026-03-10")
    store.close_paper_position(pos2["id"], 90.0, "2026-03-10")
    monkeypatch.setattr(attribution, "_batch_current_prices", lambda tickers: {})
    row = attribution.compute_theme_attribution(portfolio["id"])["themes"][0]
    assert row["win_rate"] == 0.5


def test_theme_attribution_with_no_positions(temp_modules) -> None:
    store, _paper, attribution = temp_modules
    portfolio = _portfolio(store)
    payload = attribution.compute_theme_attribution(portfolio["id"])
    assert payload["themes"] == []
    assert payload["total_pnl"] == 0.0


def test_theme_attribution_best_worst_trade(temp_modules, monkeypatch) -> None:
    store, _paper, attribution = temp_modules
    portfolio = _portfolio(store)
    theme = store.create_theme("AI", conviction=4)
    pos1 = store.open_paper_position(portfolio["id"], "NVDA", 10, 100.0, "2026-03-01", theme_id=theme["id"])
    pos2 = store.open_paper_position(portfolio["id"], "AVGO", 10, 100.0, "2026-03-02", theme_id=theme["id"])
    store.close_paper_position(pos1["id"], 130.0, "2026-03-10")
    store.close_paper_position(pos2["id"], 80.0, "2026-03-10")
    monkeypatch.setattr(attribution, "_batch_current_prices", lambda tickers: {})
    row = attribution.compute_theme_attribution(portfolio["id"])["themes"][0]
    assert row["best_trade"]["ticker"] == "NVDA"
    assert row["worst_trade"]["ticker"] == "AVGO"


def test_source_attribution_groups_by_source(temp_modules, monkeypatch) -> None:
    store, _paper, attribution = temp_modules
    portfolio = _portfolio(store)
    store.create_trade_plan(portfolio["id"], "NVDA", "Buy", 10, "disc", source="discovery")
    store.update_trade_plan_status(1, "Executed", executed_at="2026-03-01T10:00:00+00:00", execution_price=100.0)
    store.open_paper_position(portfolio["id"], "NVDA", 10, 100.0, "2026-03-01T10:00:00+00:00")
    store.create_trade_plan(portfolio["id"], "AVGO", "Buy", 5, "hold", source="holdings")
    store.update_trade_plan_status(2, "Executed", executed_at="2026-03-02T10:00:00+00:00", execution_price=100.0)
    pos = store.open_paper_position(portfolio["id"], "AVGO", 5, 100.0, "2026-03-02T10:00:00+00:00")
    store.close_paper_position(pos["id"], 120.0, "2026-03-10")
    monkeypatch.setattr(attribution, "_batch_current_prices", lambda tickers: {"NVDA": 110.0})
    payload = attribution.compute_source_attribution(portfolio["id"])
    assert {row["source"] for row in payload["sources"]} == {"discovery", "holdings"}


def test_source_attribution_slippage_calculation(temp_modules, monkeypatch) -> None:
    store, _paper, attribution = temp_modules
    portfolio = _portfolio(store)
    plan = store.create_trade_plan(portfolio["id"], "NVDA", "Buy", 10, "disc", source="discovery", proposed_price=100.0)
    store.update_trade_plan_status(plan["id"], "Executed", executed_at="2026-03-01T10:00:00+00:00", execution_price=105.0)
    store.open_paper_position(portfolio["id"], "NVDA", 10, 105.0, "2026-03-01T10:00:00+00:00")
    monkeypatch.setattr(attribution, "_batch_current_prices", lambda tickers: {"NVDA": 105.0})
    row = attribution.compute_source_attribution(portfolio["id"])["sources"][0]
    assert round(row["avg_slippage_pct"], 2) == 5.0


def test_source_attribution_unmatched_positions(temp_modules, monkeypatch) -> None:
    store, _paper, attribution = temp_modules
    portfolio = _portfolio(store)
    store.open_paper_position(portfolio["id"], "NVDA", 10, 100.0, "2026-03-01")
    monkeypatch.setattr(attribution, "_batch_current_prices", lambda tickers: {"NVDA": 110.0})
    payload = attribution.compute_source_attribution(portfolio["id"])
    assert payload["unmatched_positions"] == 1


def test_regime_attribution_groups_by_entry_regime(temp_modules, monkeypatch) -> None:
    store, _paper, attribution = temp_modules
    portfolio = _portfolio(store)
    buy1 = store.create_trade_plan(portfolio["id"], "NVDA", "Buy", 10, "buy", source="discovery", regime_label="Bull")
    store.update_trade_plan_status(buy1["id"], "Executed", executed_at="2026-03-01T10:00:00+00:00", execution_price=100.0)
    pos1 = store.open_paper_position(portfolio["id"], "NVDA", 10, 100.0, "2026-03-01T10:00:00+00:00")
    store.close_paper_position(pos1["id"], 120.0, "2026-03-10")
    buy2 = store.create_trade_plan(portfolio["id"], "AVGO", "Buy", 10, "buy", source="discovery", regime_label="Bear")
    store.update_trade_plan_status(buy2["id"], "Executed", executed_at="2026-03-02T10:00:00+00:00", execution_price=100.0)
    pos2 = store.open_paper_position(portfolio["id"], "AVGO", 10, 100.0, "2026-03-02T10:00:00+00:00")
    store.close_paper_position(pos2["id"], 90.0, "2026-03-10")
    monkeypatch.setattr(attribution, "_batch_current_prices", lambda tickers: {})
    payload = attribution.compute_regime_attribution(portfolio["id"])
    assert {row["regime"] for row in payload["regimes"]} == {"Bull", "Bear"}


def test_regime_attribution_falls_back_to_history(temp_modules, monkeypatch) -> None:
    store, _paper, attribution = temp_modules
    portfolio = _portfolio(store)
    position = store.open_paper_position(portfolio["id"], "NVDA", 10, 100.0, "2026-03-05T10:00:00+00:00")
    store.close_paper_position(position["id"], 120.0, "2026-03-10T10:00:00+00:00")
    store.save_regime_change_with_price("NVDA", "Neutral", "Bull", 2, 100.0)
    monkeypatch.setattr(attribution, "_batch_current_prices", lambda tickers: {})
    payload = attribution.compute_regime_attribution(portfolio["id"])
    assert payload["regimes"][0]["regime"] in {"Bull", "Unknown"}


def test_regime_attribution_unknown_when_no_data(temp_modules, monkeypatch) -> None:
    store, _paper, attribution = temp_modules
    portfolio = _portfolio(store)
    store.open_paper_position(portfolio["id"], "NVDA", 10, 100.0, "2026-03-01")
    monkeypatch.setattr(attribution, "_batch_current_prices", lambda tickers: {"NVDA": 110.0})
    payload = attribution.compute_regime_attribution(portfolio["id"])
    assert payload["regimes"][0]["regime"] == "Unknown"


def test_ml_accuracy_calibration_bands(temp_modules) -> None:
    store, _paper, attribution = temp_modules
    portfolio = _portfolio(store)
    for idx, (ticker, confidence, exit_price) in enumerate([("NVDA", 95, 120.0), ("AVGO", 80, 80.0), ("AMD", 45, 110.0)], start=1):
        plan = store.create_trade_plan(portfolio["id"], ticker, "Buy", 10, f"ML confidence: {confidence}%", source="holdings")
        store.update_trade_plan_status(plan["id"], "Executed", executed_at=f"2026-03-0{idx}T10:00:00+00:00", execution_price=100.0)
        position = store.open_paper_position(portfolio["id"], ticker, 10, 100.0, f"2026-03-0{idx}T10:00:00+00:00")
        store.close_paper_position(position["id"], exit_price, f"2026-03-1{idx}T10:00:00+00:00")
    payload = attribution.compute_ml_accuracy(portfolio["id"])
    assert payload["total_trades_with_ml"] == 3
    assert {row["band"] for row in payload["calibration"]} == {"90-100%", "70-90%", "30-50%"}


def test_ml_accuracy_empty_when_no_ml_data(temp_modules) -> None:
    store, _paper, attribution = temp_modules
    portfolio = _portfolio(store)
    plan = store.create_trade_plan(portfolio["id"], "NVDA", "Buy", 10, "no ml", source="holdings")
    store.update_trade_plan_status(plan["id"], "Executed", executed_at="2026-03-01T10:00:00+00:00", execution_price=100.0)
    position = store.open_paper_position(portfolio["id"], "NVDA", 10, 100.0, "2026-03-01T10:00:00+00:00")
    store.close_paper_position(position["id"], 120.0, "2026-03-10T10:00:00+00:00")
    payload = attribution.compute_ml_accuracy(portfolio["id"])
    assert payload["calibration"] == []
    assert payload["total_trades_with_ml"] == 0


def test_ml_accuracy_model_history(temp_modules) -> None:
    store, _paper, attribution = temp_modules
    portfolio = _portfolio(store)
    store.log_training_run(version=2, ticker="NVDA", model_path="/tmp/model.json", metrics={"accuracy": 0.8, "f1": 0.76, "feature_importances": {}})
    payload = attribution.compute_ml_accuracy(portfolio["id"])
    assert payload["model_history"][0]["version"] == 2


def test_attribution_routes_return_200(temp_modules, monkeypatch) -> None:
    store, paper, attribution = temp_modules
    portfolio = _portfolio(store)
    runtime = {
        "get_paper_portfolio": store.get_paper_portfolio,
        "compute_theme_attribution": attribution.compute_theme_attribution,
        "compute_source_attribution": attribution.compute_source_attribution,
        "compute_regime_attribution": attribution.compute_regime_attribution,
        "compute_ml_accuracy": attribution.compute_ml_accuracy,
        "compute_paper_performance": paper.compute_paper_performance,
        "compute_attribution_summary": attribution.compute_attribution_summary,
    }
    client = _route_client(monkeypatch, runtime)
    for path in ("theme", "source", "regime", "ml", "summary"):
        response = client.get(f"/regime/paper-portfolio/{portfolio['id']}/attribution/{path}")
        assert response.status_code == 200


def test_summary_route_composite(temp_modules, monkeypatch) -> None:
    store, paper, attribution = temp_modules
    portfolio = _portfolio(store)
    runtime = {
        "get_paper_portfolio": store.get_paper_portfolio,
        "compute_theme_attribution": attribution.compute_theme_attribution,
        "compute_source_attribution": attribution.compute_source_attribution,
        "compute_regime_attribution": attribution.compute_regime_attribution,
        "compute_ml_accuracy": attribution.compute_ml_accuracy,
        "compute_paper_performance": paper.compute_paper_performance,
        "compute_attribution_summary": attribution.compute_attribution_summary,
    }
    client = _route_client(monkeypatch, runtime)
    payload = client.get(f"/regime/paper-portfolio/{portfolio['id']}/attribution/summary").json()
    assert {"portfolio", "performance", "theme_attribution", "source_attribution", "regime_attribution", "ml_accuracy", "generated_at"} <= set(payload.keys())


def test_attribution_route_404_for_missing_portfolio(temp_modules, monkeypatch) -> None:
    _store, paper, attribution = temp_modules
    runtime = {
        "get_paper_portfolio": lambda portfolio_id: None,
        "compute_theme_attribution": attribution.compute_theme_attribution,
        "compute_source_attribution": attribution.compute_source_attribution,
        "compute_regime_attribution": attribution.compute_regime_attribution,
        "compute_ml_accuracy": attribution.compute_ml_accuracy,
        "compute_paper_performance": paper.compute_paper_performance,
        "compute_attribution_summary": attribution.compute_attribution_summary,
    }
    client = _route_client(monkeypatch, runtime)
    response = client.get("/regime/paper-portfolio/999/attribution/summary")
    assert response.status_code == 404


def test_daily_snapshot_includes_drawdown(temp_modules, monkeypatch) -> None:
    store, paper, _attribution = temp_modules
    portfolio = _portfolio(store)
    store.save_daily_snapshot(portfolio["id"], "2026-03-20", equity=120000.0, cash=120000.0, market_value=0.0)
    monkeypatch.setattr(paper_trading_module, "load_payload", lambda: {"rows": []}, raising=False)
    snapshot = paper.compute_daily_snapshot(portfolio["id"])
    assert round(snapshot["drawdown_pct"], 2) == -16.67


def test_daily_snapshot_includes_regime_exposure(temp_modules, monkeypatch) -> None:
    store, paper, _attribution = temp_modules
    portfolio = _portfolio(store)
    store.open_paper_position(portfolio["id"], "NVDA", 10, 100.0, "2026-03-01")
    monkeypatch.setattr(paper, "get_paper_portfolio_summary", lambda portfolio_id: {
        "current_cash": 90000.0,
        "total_market_value": 10000.0,
        "realized_pnl": 0.0,
        "unrealized_pnl": 0.0,
        "positions": [{"ticker": "NVDA", "market_value": 10000.0}],
    })
    import src.app.routes.regime_cache as regime_cache
    monkeypatch.setattr(regime_cache, "load_payload", lambda: {"rows": [{"ticker": "NVDA", "regime": "Bull"}]})
    snapshot = paper.compute_daily_snapshot(portfolio["id"])
    exposure = json.loads(snapshot["regime_exposure_json"])
    assert exposure["Bull"] == 1.0


def test_performance_timeseries_returns_daily_cumulative_returns(temp_modules) -> None:
    store, _paper, _attribution = temp_modules
    portfolio = _portfolio(store)
    store.save_daily_snapshot(portfolio["id"], "2026-03-20", equity=100000.0, cash=100000.0, market_value=0.0)
    store.save_daily_snapshot(portfolio["id"], "2026-03-21", equity=105000.0, cash=105000.0, market_value=0.0)
    store.save_daily_snapshot(portfolio["id"], "2026-03-22", equity=102900.0, cash=102900.0, market_value=0.0)
    rows = store.get_performance_timeseries(portfolio["id"], days=365)
    assert rows[1]["daily_return_pct"] == 5.0
    assert round(rows[2]["daily_return_pct"], 2) == -2.0
    assert round(rows[2]["cumulative_return_pct"], 2) == 2.9
