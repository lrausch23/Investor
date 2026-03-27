from __future__ import annotations

from fastapi.testclient import TestClient

from src.app.main import create_app
from src.app.routes import regime as regime_route
from src.regime.broker_adapter import OrderRequest
from src.regime.config import IBKRConfig, validate_ibkr_readiness
from src.regime.ib_connection import MockIBBackend
from src.regime.ibkr_adapter import IBKRBrokerAdapter


def test_verdict_to_action_entry() -> None:
    assert regime_route._verdict_to_action("Entry") == "Buy"


def test_verdict_to_action_exit() -> None:
    assert regime_route._verdict_to_action("Exit") == "Sell"


def test_verdict_to_action_hold_returns_none() -> None:
    assert regime_route._verdict_to_action("Hold") is None


def test_verdict_to_action_suppressed_returns_none() -> None:
    assert regime_route._verdict_to_action("Hold — Regime too new (3d < 5d minimum)") is None


def test_verdict_to_action_none_returns_none() -> None:
    assert regime_route._verdict_to_action(None) is None


def test_ibkr_config_defaults(monkeypatch) -> None:
    monkeypatch.delenv("IBKR_LIVE_BACKEND", raising=False)
    monkeypatch.delenv("IBKR_PORT", raising=False)
    monkeypatch.delenv("IBKR_ACCOUNT_ID", raising=False)
    config = IBKRConfig()
    assert config.port == 7497
    assert config.account_id == "DUP579027"
    assert config.live_backend is False


def test_ibkr_config_env_override(monkeypatch) -> None:
    monkeypatch.setenv("IBKR_LIVE_BACKEND", "true")
    monkeypatch.setenv("IBKR_PORT", "4002")
    monkeypatch.setenv("IBKR_ACCOUNT_ID", "DUP000999")
    config = IBKRConfig()
    assert config.live_backend is True
    assert config.port == 4002
    assert config.account_id == "DUP000999"


def test_validate_ibkr_readiness_paper(monkeypatch) -> None:
    monkeypatch.setenv("IBKR_LIVE_BACKEND", "true")
    monkeypatch.setenv("IBKR_HOST", "127.0.0.1")
    monkeypatch.setenv("IBKR_PORT", "7497")
    monkeypatch.setenv("IBKR_ACCOUNT_ID", "DUP579027")
    checks = validate_ibkr_readiness()
    assert checks["live_backend_enabled"] is True
    assert checks["port_is_paper"] is True
    assert checks["host_is_local"] is True
    assert checks["account_configured"] is True
    assert checks["all_clear"] is True


def test_paper_account_guard_rejects_live_account() -> None:
    backend = MockIBBackend(account_id="U123456", starting_cash=100000.0)
    adapter = IBKRBrokerAdapter(backend, 1)
    request = OrderRequest(portfolio_id=1, ticker="SPY", action="Buy", quantity=1.0)
    result = adapter.submit_order(request)
    assert result.status == "rejected"
    assert "paper only" in (result.message or "").lower()


def test_paper_account_guard_allows_paper_account() -> None:
    from src.regime import ibkr_adapter as ibkr_adapter_module

    backend = MockIBBackend(account_id="DUP579027", starting_cash=100000.0)
    adapter = IBKRBrokerAdapter(backend, 1)
    original = ibkr_adapter_module.is_market_open
    ibkr_adapter_module.is_market_open = lambda now=None: True
    request = OrderRequest(portfolio_id=1, ticker="SPY", action="Buy", quantity=1.0)
    try:
        result = adapter.submit_order(request)
        assert result.status in {"filled", "submitted", "partially_filled"}
    finally:
        ibkr_adapter_module.is_market_open = original


def test_cancel_order_route(monkeypatch) -> None:
    from tests.test_regime_route import _ImmediateExecutor, _fake_runtime

    runtime = _fake_runtime()
    runtime["get_paper_portfolio"] = lambda portfolio_id: {"id": int(portfolio_id), "name": "Sandbox", "starting_budget": 100000.0, "current_cash": 95000.0, "broker_type": "ibkr", "status": "Active", "created_at": "2026-03-26T12:00:00+00:00"}
    runtime["get_ib_backend"] = lambda portfolio_id, live=False, account_id="DUP579027", starting_cash=100000.0: MockIBBackend(account_id=account_id, starting_cash=starting_cash)
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    monkeypatch.setattr(regime_route, "_EXECUTOR", _ImmediateExecutor())
    monkeypatch.setattr(regime_route, "get_current_tickers_by_scope", lambda session, scope, account_id=None: ["NVDA"])
    monkeypatch.setattr(regime_route, "get_available_portfolio_scopes", lambda session: [{"value": "household", "label": "All Portfolios", "ticker_count": 1, "accounts": []}])
    app = create_app()
    client = TestClient(app)

    response = client.post("/regime/paper-portfolio/1/orders/1/cancel")
    assert response.status_code == 200
    assert response.json()["cancelled"] is True
