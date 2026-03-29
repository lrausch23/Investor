from __future__ import annotations

import importlib

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from src.app.routes import regime as regime_route
from src.regime import broker_adapter as broker_adapter_module
from src.regime import persistence as persistence_module
from src.regime import tax_lot_router as tax_lot_router_module


@pytest.fixture()
def temp_modules(tmp_path, monkeypatch):
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    store = importlib.reload(persistence_module)
    monkeypatch.setattr(store, "DB_PATH", tmp_path / "regime_watch.db")
    store = importlib.reload(persistence_module)
    broker = importlib.reload(broker_adapter_module)
    router = importlib.reload(tax_lot_router_module)
    return store, broker, router


def _route_client(monkeypatch, runtime: dict) -> TestClient:
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    app = FastAPI()
    app.include_router(regime_route.router)
    app.dependency_overrides[regime_route.require_actor] = lambda: "tester"
    app.dependency_overrides[regime_route.db_session] = lambda: iter([None])
    return TestClient(app)


def test_create_tax_lot_persists_and_returns(temp_modules) -> None:
    store, _broker, _router = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    position = store.open_paper_position(portfolio["id"], "NVDA", 10, 100.0, "2025-01-01T00:00:00+00:00")
    lot = store.create_tax_lot(portfolio["id"], position["id"], "NVDA", 10, 100.0, "2025-01-01T00:00:00+00:00")
    assert lot["ticker"] == "NVDA"
    assert lot["cost_basis_total"] == pytest.approx(1000.0)


def test_get_tax_lots_computed_fields(temp_modules) -> None:
    store, _broker, _router = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    position = store.open_paper_position(portfolio["id"], "NVDA", 10, 100.0, "2025-01-01T00:00:00+00:00")
    store.create_tax_lot(portfolio["id"], position["id"], "NVDA", 10, 100.0, "2025-01-01T00:00:00+00:00")
    lot = store.get_tax_lots(portfolio["id"])[0]
    assert "days_held" in lot
    assert lot["term"] in {"ST", "LT"}
    assert "days_to_ltcg" in lot


def test_close_tax_lot_partial(temp_modules) -> None:
    store, _broker, _router = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    position = store.open_paper_position(portfolio["id"], "NVDA", 10, 100.0, "2025-01-01T00:00:00+00:00")
    lot = store.create_tax_lot(portfolio["id"], position["id"], "NVDA", 10, 100.0, "2025-01-01T00:00:00+00:00")
    closed = store.close_tax_lot(lot["id"], 4, 120.0, "2026-01-01T00:00:00+00:00")
    assert closed["status"] == "partial"
    refreshed = store.get_tax_lot(lot["id"])
    assert refreshed["remaining_quantity"] == pytest.approx(6.0)


def test_add_wash_sale_restriction_and_active_check(temp_modules) -> None:
    store, _broker, _router = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    store.add_wash_sale_restriction(portfolio["id"], "NVDA", "2026-03-01T00:00:00+00:00", -200.0)
    assert store.is_wash_sale_restricted(portfolio["id"], "NVDA") is True


def test_select_lots_hifo_ordering(temp_modules) -> None:
    store, _broker, router = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    position = store.open_paper_position(portfolio["id"], "NVDA", 30, 10.0, "2025-01-01T00:00:00+00:00")
    store.create_tax_lot(portfolio["id"], position["id"], "NVDA", 10, 10.0, "2025-01-01T00:00:00+00:00")
    store.create_tax_lot(portfolio["id"], position["id"], "NVDA", 10, 15.0, "2025-02-01T00:00:00+00:00")
    store.create_tax_lot(portfolio["id"], position["id"], "NVDA", 10, 20.0, "2025-03-01T00:00:00+00:00")
    selections = router.select_lots(portfolio["id"], "NVDA", 15, method="HIFO")
    assert selections[0]["cost_basis_per_share"] == pytest.approx(20.0)
    assert selections[1]["cost_basis_per_share"] == pytest.approx(15.0)


def test_guardrail_blocks_wash_sale_buy(temp_modules) -> None:
    store, broker, _router = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    store.add_wash_sale_restriction(portfolio["id"], "NVDA", "2026-03-01T00:00:00+00:00", -100.0)
    adapter = broker.PaperBrokerAdapter(portfolio["id"])
    order = broker.OrderRequest(portfolio_id=portfolio["id"], ticker="NVDA", action="Buy", quantity=10)
    result = broker.validate_guardrails(order, adapter)
    failed = {check.name for check in result.checks if not check.passed}
    assert "wash_sale_restricted" in failed


def test_buy_creates_tax_lot(temp_modules, monkeypatch) -> None:
    store, broker, _router = temp_modules
    monkeypatch.setattr("src.regime.paper_trading._batch_current_prices", lambda tickers: {"NVDA": 100.0})
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    adapter = broker.PaperBrokerAdapter(portfolio["id"])
    result = adapter.submit_order(broker.OrderRequest(portfolio_id=portfolio["id"], ticker="NVDA", action="Buy", quantity=10))
    assert result.status == "filled"
    lots = store.get_tax_lots(portfolio["id"], ticker="NVDA", status="all")
    assert len(lots) == 1


def test_sell_uses_lot_routing_and_creates_wash_sale_restriction(temp_modules, monkeypatch) -> None:
    store, broker, _router = temp_modules
    monkeypatch.setattr("src.regime.paper_trading._batch_current_prices", lambda tickers: {"NVDA": 12.0})
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    position = store.open_paper_position(portfolio["id"], "NVDA", 20, 15.0, "2025-01-01T00:00:00+00:00")
    store.create_tax_lot(portfolio["id"], position["id"], "NVDA", 10, 10.0, "2025-01-01T00:00:00+00:00")
    high_cost = store.create_tax_lot(portfolio["id"], position["id"], "NVDA", 10, 20.0, "2025-02-01T00:00:00+00:00")
    store.set_lot_selection_method("HIFO")
    adapter = broker.PaperBrokerAdapter(portfolio["id"])
    result = adapter.submit_order(broker.OrderRequest(portfolio_id=portfolio["id"], ticker="NVDA", action="Sell", quantity=10))
    assert result.status == "filled"
    closed_high = store.get_tax_lot(high_cost["id"])
    assert closed_high["status"] == "closed"
    assert store.is_wash_sale_restricted(portfolio["id"], "NVDA") is True


def test_estimate_tax_impact(temp_modules) -> None:
    _store, _broker, router = temp_modules
    impact = router.estimate_tax_impact(
        [
            {"quantity": 10, "cost_basis_per_share": 100.0, "term": "ST"},
            {"quantity": 5, "cost_basis_per_share": 90.0, "term": "LT"},
        ],
        110.0,
    )
    assert impact["short_term_gain"] == pytest.approx(100.0)
    assert impact["long_term_gain"] == pytest.approx(100.0)


def test_tax_routes(temp_modules, monkeypatch) -> None:
    store, _broker, router = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    position = store.open_paper_position(portfolio["id"], "NVDA", 10, 100.0, "2025-01-01T00:00:00+00:00")
    lot = store.create_tax_lot(portfolio["id"], position["id"], "NVDA", 10, 100.0, "2025-01-01T00:00:00+00:00")
    store.add_wash_sale_restriction(portfolio["id"], "NVDA", "2026-03-01T00:00:00+00:00", -200.0)
    runtime = {
        "get_tax_lots": store.get_tax_lots,
        "get_tax_lot": store.get_tax_lot,
        "get_wash_sale_restrictions": store.get_wash_sale_restrictions,
        "compute_wash_sale_opportunity_cost": router.compute_wash_sale_opportunity_cost,
        "set_lot_selection_method": store.set_lot_selection_method,
        "set_ltcg_defer_window_days": store.set_ltcg_defer_window_days,
        "get_lot_selection_method": store.get_lot_selection_method,
        "get_ltcg_defer_window_days": store.get_ltcg_defer_window_days,
        "select_lots": router.select_lots,
        "estimate_tax_impact": router.estimate_tax_impact,
    }
    monkeypatch.setattr("src.regime.paper_trading._batch_current_prices", lambda tickers: {"NVDA": 110.0})
    client = _route_client(monkeypatch, runtime)
    assert client.get(f"/regime/paper-portfolio/{portfolio['id']}/tax-lots").status_code == 200
    assert client.get(f"/regime/paper-portfolio/{portfolio['id']}/tax-lots/{lot['id']}").status_code == 200
    assert client.get(f"/regime/paper-portfolio/{portfolio['id']}/wash-sale").status_code == 200
    settings = client.put("/regime/tax-settings", json={"lot_selection_method": "FIFO", "ltcg_defer_window_days": 20})
    assert settings.status_code == 200
    estimate = client.post(f"/regime/paper-portfolio/{portfolio['id']}/tax-lots/estimate", json={"ticker": "NVDA", "quantity": 5, "exit_price": 110.0})
    assert estimate.status_code == 200
