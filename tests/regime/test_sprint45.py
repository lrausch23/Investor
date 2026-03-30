from __future__ import annotations

import importlib
import json
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from src.app.routes import regime as regime_route
from src.regime.exceptions import DataValidationError
from src.regime import paper_trading as paper_trading_module
from src.regime import persistence as persistence_module


@pytest.fixture()
def temp_modules(tmp_path, monkeypatch):
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    store = importlib.reload(persistence_module)
    monkeypatch.setattr(store, "DB_PATH", tmp_path / "regime_watch.db")
    paper = importlib.reload(paper_trading_module)
    return store, paper, tmp_path / "regime_watch.db"


def _route_client(monkeypatch, runtime: dict) -> TestClient:
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    app = FastAPI()
    app.include_router(regime_route.router)
    app.dependency_overrides[regime_route.require_actor] = lambda: "tester"
    app.dependency_overrides[regime_route.db_session] = lambda: iter([None])
    return TestClient(app)


def _allowed_result(allowed: bool = True) -> SimpleNamespace:
    return SimpleNamespace(allowed=allowed, checks=[{"name": "ok", "allowed": allowed}])


def _audit_counts(store, portfolio_id: int) -> dict[str, int]:
    return store.get_daily_audit_summary(portfolio_id).get("counts", {})


def _base_runtime(store, paper) -> dict:
    return {
        "get_paper_portfolio": store.get_paper_portfolio,
        "get_paper_portfolio_summary": store.get_paper_portfolio_summary,
        "get_daily_audit_summary": store.get_daily_audit_summary,
        "get_daily_capital_deployed": store.get_daily_capital_deployed,
        "get_operating_mode": store.get_operating_mode,
        "get_auto_approve_threshold": store.get_auto_approve_threshold,
        "get_daily_capital_ceiling_pct": store.get_daily_capital_ceiling_pct,
        "set_operating_mode": store.set_operating_mode,
        "set_auto_approve_threshold": store.set_auto_approve_threshold,
        "set_daily_capital_ceiling_pct": store.set_daily_capital_ceiling_pct,
        "OPERATING_MODES": store.OPERATING_MODES,
        "auto_approve_plans": paper.auto_approve_plans,
    }


def _make_plan(store, portfolio_id: int, ticker: str, *, action: str = "Buy", score: float | None = None, price: float = 100.0):
    return store.create_trade_plan(
        portfolio_id,
        ticker,
        action,
        10,
        f"{ticker} rationale",
        proposed_price=price,
        source="holdings",
        meta_labeler_score=score,
    )


def test_operating_mode_defaults_and_setters(temp_modules) -> None:
    store, _paper, _db_path = temp_modules
    assert store.get_operating_mode() == "manual"
    assert store.get_auto_approve_threshold() == 0.65
    assert store.get_daily_capital_ceiling_pct() == 0.25
    store.set_operating_mode("semi_auto")
    store.set_auto_approve_threshold(0.72)
    store.set_daily_capital_ceiling_pct(0.4)
    assert store.get_operating_mode() == "semi_auto"
    assert store.get_auto_approve_threshold() == 0.72
    assert store.get_daily_capital_ceiling_pct() == 0.4


def test_operating_mode_rejects_invalid_value(temp_modules) -> None:
    store, _paper, _db_path = temp_modules
    try:
        store.set_operating_mode("unsafe")
    except DataValidationError as exc:
        assert "Invalid mode" in str(exc)
    else:
        raise AssertionError("Expected DataValidationError")


def test_trade_plan_persists_meta_labeler_score(temp_modules) -> None:
    store, _paper, _db_path = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    plan = store.create_trade_plan(
        portfolio["id"],
        "NVDA",
        "Buy",
        10,
        "ml-backed",
        source="holdings",
        meta_labeler_score=0.81,
    )
    loaded = store.get_trade_plan(plan["id"])
    assert loaded is not None
    assert loaded["meta_labeler_score"] == 0.81


def test_get_daily_capital_deployed_counts_buy_fills_only(temp_modules) -> None:
    store, _paper, _db_path = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    store.log_audit_event(order_id="1", portfolio_id=portfolio["id"], event_type="filled", ticker="NVDA", action="Buy", quantity=10, price=100.0)
    store.log_audit_event(order_id="2", portfolio_id=portfolio["id"], event_type="partially_filled", ticker="AVGO", action="Buy", quantity=5, price=50.0)
    store.log_audit_event(order_id="3", portfolio_id=portfolio["id"], event_type="filled", ticker="AMD", action="Sell", quantity=7, price=90.0)
    assert store.get_daily_capital_deployed(portfolio["id"]) == 1250.0


def test_auto_approve_manual_mode_is_noop(temp_modules, monkeypatch) -> None:
    store, paper, _db_path = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    _make_plan(store, portfolio["id"], "NVDA", score=0.9)
    monkeypatch.setattr(paper, "validate_guardrails", lambda *args, **kwargs: _allowed_result(True))
    result = paper.auto_approve_plans(portfolio["id"])
    assert result["mode"] == "manual"
    assert result["approved"] == 0
    assert store.get_trade_plans(portfolio["id"], status="Pending")[0]["status"] == "Pending"


def test_auto_approve_semi_auto_uses_ml_threshold(temp_modules, monkeypatch) -> None:
    store, paper, _db_path = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    store.set_operating_mode("semi_auto")
    store.set_auto_approve_threshold(0.65)
    buy_good = _make_plan(store, portfolio["id"], "NVDA", score=0.8)
    buy_low = _make_plan(store, portfolio["id"], "AVGO", score=0.4)
    buy_none = _make_plan(store, portfolio["id"], "AMD", score=None)
    sell_plan = _make_plan(store, portfolio["id"], "TSLA", action="Sell", score=None)
    monkeypatch.setattr(paper, "validate_guardrails", lambda *args, **kwargs: _allowed_result(True))
    result = paper.auto_approve_plans(portfolio["id"])
    approved_ids = {item["plan_id"] for item in result["details"] if item["result"] == "approved"}
    assert approved_ids == {buy_good["id"], sell_plan["id"]}
    skipped = {item["result"] for item in result["details"] if item["plan_id"] in {buy_low["id"], buy_none["id"]}}
    assert skipped == {"skipped_ml", "skipped_no_score"}


def test_auto_approve_autonomous_ignores_ml_threshold_for_buys(temp_modules, monkeypatch) -> None:
    store, paper, _db_path = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    store.set_operating_mode("autonomous")
    low_score = _make_plan(store, portfolio["id"], "NVDA", score=0.1)
    monkeypatch.setattr(paper, "validate_guardrails", lambda *args, **kwargs: _allowed_result(True))
    result = paper.auto_approve_plans(portfolio["id"])
    assert result["approved"] == 1
    assert result["details"][0]["plan_id"] == low_score["id"]
    assert store.get_trade_plan(low_score["id"])["status"] == "Approved"


def test_auto_approve_blocks_by_daily_capital_ceiling(temp_modules, monkeypatch) -> None:
    store, paper, _db_path = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    store.set_operating_mode("semi_auto")
    store.set_daily_capital_ceiling_pct(0.1)
    _make_plan(store, portfolio["id"], "NVDA", score=0.9, price=2000.0)
    monkeypatch.setattr(paper, "validate_guardrails", lambda *args, **kwargs: _allowed_result(True))
    result = paper.auto_approve_plans(portfolio["id"])
    assert result["blocked"] == 1
    assert result["details"][0]["result"] == "blocked_ceiling"


def test_auto_approve_blocks_guardrail_failure(temp_modules, monkeypatch) -> None:
    store, paper, _db_path = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    store.set_operating_mode("semi_auto")
    plan = _make_plan(store, portfolio["id"], "NVDA", score=0.9)
    monkeypatch.setattr(paper, "validate_guardrails", lambda *args, **kwargs: _allowed_result(False))
    result = paper.auto_approve_plans(portfolio["id"])
    assert result["blocked"] == 1
    assert result["details"][0]["plan_id"] == plan["id"]
    assert result["details"][0]["result"] == "blocked_guardrail"


def test_auto_approve_logs_auto_approved_audit_event(temp_modules, monkeypatch) -> None:
    store, paper, _db_path = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    store.set_operating_mode("semi_auto")
    _make_plan(store, portfolio["id"], "NVDA", score=0.9)
    monkeypatch.setattr(paper, "validate_guardrails", lambda *args, **kwargs: _allowed_result(True))
    paper.auto_approve_plans(portfolio["id"])
    counts = _audit_counts(store, portfolio["id"])
    assert counts.get("auto_approved", 0) == 1


def test_autonomy_settings_routes_round_trip(temp_modules, monkeypatch) -> None:
    store, paper, _db_path = temp_modules
    client = _route_client(monkeypatch, _base_runtime(store, paper))
    get_payload = client.get("/regime/autonomy/settings").json()
    assert get_payload["operating_mode"] == "manual"
    response = client.put(
        "/regime/autonomy/settings",
        content=json.dumps({
            "operating_mode": "semi_auto",
            "auto_approve_threshold": 0.7,
            "daily_capital_ceiling_pct": 0.3,
        }),
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["operating_mode"] == "semi_auto"
    assert payload["auto_approve_threshold"] == 0.7
    assert payload["daily_capital_ceiling_pct"] == 0.3


def test_autonomy_settings_route_rejects_invalid_mode(temp_modules, monkeypatch) -> None:
    store, paper, _db_path = temp_modules
    client = _route_client(monkeypatch, _base_runtime(store, paper))
    response = client.put(
        "/regime/autonomy/settings",
        content=json.dumps({"operating_mode": "invalid"}),
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 422


def test_auto_approve_route_returns_runtime_result(temp_modules, monkeypatch) -> None:
    store, paper, _db_path = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    store.set_operating_mode("semi_auto")
    _make_plan(store, portfolio["id"], "NVDA", score=0.8)
    monkeypatch.setattr(paper, "validate_guardrails", lambda *args, **kwargs: _allowed_result(True))
    client = _route_client(monkeypatch, _base_runtime(store, paper))
    response = client.post(f"/regime/paper-portfolio/{portfolio['id']}/auto-approve")
    assert response.status_code == 200
    assert response.json()["approved"] == 1


def test_autonomy_status_route_reports_remaining_capacity(temp_modules, monkeypatch) -> None:
    store, paper, _db_path = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    store.set_operating_mode("semi_auto")
    store.set_daily_capital_ceiling_pct(0.25)
    store.log_audit_event(order_id="1", portfolio_id=portfolio["id"], event_type="filled", ticker="NVDA", action="Buy", quantity=100, price=100.0)
    client = _route_client(monkeypatch, _base_runtime(store, paper))
    response = client.get(f"/regime/paper-portfolio/{portfolio['id']}/autonomy/status")
    assert response.status_code == 200
    payload = response.json()
    assert payload["capital_deployed_today"] == 10000.0
    assert payload["max_daily_capital"] == 25000.0
    assert payload["capital_remaining"] == 15000.0
