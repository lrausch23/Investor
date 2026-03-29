from __future__ import annotations

import asyncio
import importlib
import json
import sys
import threading
import time
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.app.routes import regime as regime_route
from src.regime import ib_connection as ib_connection_module
from src.regime import ib_live_backend as ib_live_backend_module
from src.regime import ib_thread as ib_thread_module
from src.regime import ibkr_adapter as ibkr_adapter_module
from src.regime import persistence as persistence_module
from src.regime.broker_adapter import OrderRequest, OrderResult


@pytest.fixture()
def temp_store(tmp_path, monkeypatch):
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    store = importlib.reload(persistence_module)
    monkeypatch.setattr(store, "DB_PATH", tmp_path / "regime_watch.db")
    return store


def _route_client(monkeypatch, runtime: dict) -> TestClient:
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    app = FastAPI()
    app.include_router(regime_route.router)
    app.dependency_overrides[regime_route.require_actor] = lambda: "tester"
    app.dependency_overrides[regime_route.db_session] = lambda: iter([None])
    return TestClient(app)


def test_ib_thread_starts_and_runs_callable() -> None:
    thread = ib_thread_module.get_ib_thread()
    assert thread.is_alive
    assert thread.run(lambda: 42) == 42


def test_ib_thread_runs_on_dedicated_thread() -> None:
    caller = threading.current_thread()
    ib_thread = ib_thread_module.get_ib_thread()
    worker = ib_thread.run(threading.current_thread)
    assert worker is not caller
    assert worker.name == "ib-thread"


def test_ib_thread_timeout_raises() -> None:
    with pytest.raises(TimeoutError):
        ib_thread_module.get_ib_thread().run(time.sleep, 0.5, timeout=0.01)


def test_ib_thread_singleton() -> None:
    assert ib_thread_module.get_ib_thread() is ib_thread_module.get_ib_thread()


def test_live_backend_no_rlock_attribute() -> None:
    backend = ib_live_backend_module.LiveIBBackend(account_id="DUP579027")
    assert not hasattr(backend, "_lock")


def test_get_ib_backend_no_event_loop_creation(monkeypatch) -> None:
    import asyncio as std_asyncio

    ib_connection_module._LIVE_BACKENDS.clear()
    monkeypatch.setattr(std_asyncio, "new_event_loop", lambda: (_ for _ in ()).throw(AssertionError("should not create loop")))

    class FakeLiveBackend:
        def __init__(self, *, account_id: str):
            self._account_id = account_id
            self._connected = False

        def connect(self, host: str, port: int, client_id: int) -> bool:
            del host, port, client_id
            self._connected = True
            return True

        def is_connected(self) -> bool:
            return self._connected

    monkeypatch.setattr(ib_live_backend_module, "LiveIBBackend", FakeLiveBackend)
    backend = ib_connection_module.get_ib_backend(3, live=True, account_id="DUP579027")
    assert backend.is_connected() is True


def test_precheck_returns_error_on_broker_failure(monkeypatch) -> None:
    runtime = {
        "get_paper_portfolio": lambda portfolio_id: {"id": int(portfolio_id), "broker_type": "ibkr"},
        "get_trade_plans": lambda portfolio_id, status="Pending": [{"id": 1, "ticker": "NVDA", "action": "Buy", "quantity": 10, "proposed_price": 100.0}],
        "OrderRequest": OrderRequest,
        "validate_guardrails": lambda order, adapter, guardrails: (_ for _ in ()).throw(RuntimeError("ibkr down")),
        "DEFAULT_RISK_GUARDRAILS": object(),
    }

    async def fake_adapter(runtime_arg, portfolio_id_arg):
        del runtime_arg, portfolio_id_arg
        return object()

    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    monkeypatch.setattr(regime_route, "_get_broker_adapter_safe_async", fake_adapter)
    response = asyncio.run(regime_route.regime_paper_plan_precheck(2, session=None, actor="tester"))
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["plans"][0]["guardrail_passed"] is False
    assert "Broker connection error" in payload["plans"][0]["error"]


def test_execute_returns_503_on_broker_failure(monkeypatch) -> None:
    runtime = {
        "get_paper_portfolio": lambda portfolio_id: {"id": int(portfolio_id), "broker_type": "ibkr", "status": "Active"},
        "execute_approved_plans_via_adapter": lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("ibkr down")),
        "DEFAULT_RISK_GUARDRAILS": object(),
    }

    async def fake_adapter(runtime_arg, portfolio_id_arg):
        del runtime_arg, portfolio_id_arg
        return object()

    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    monkeypatch.setattr(regime_route, "_get_broker_adapter_safe_async", fake_adapter)
    response = asyncio.run(regime_route.regime_paper_execute(2, session=None, actor="tester"))
    payload = json.loads(response.body.decode("utf-8"))
    assert response.status_code == 503
    assert payload["broker_error"] is True


def test_live_trading_locked_by_default(temp_store) -> None:
    assert temp_store.is_live_trading_unlocked() is False


def test_du_prefix_always_allowed(temp_store, monkeypatch) -> None:
    backend = ib_connection_module.MockIBBackend(account_id="DUP579027", starting_cash=100000.0)
    adapter = ibkr_adapter_module.IBKRBrokerAdapter(backend, 1)
    monkeypatch.setattr(ibkr_adapter_module, "is_market_open", lambda now=None: True)
    temp_store.set_live_trading_unlocked(False)
    result = adapter.submit_order(OrderRequest(portfolio_id=1, ticker="SPY", action="Buy", quantity=1.0))
    assert result.status in {"filled", "submitted", "partially_filled"}


def test_non_du_blocked_when_locked(temp_store) -> None:
    backend = ib_connection_module.MockIBBackend(account_id="U123456", starting_cash=100000.0)
    adapter = ibkr_adapter_module.IBKRBrokerAdapter(backend, 1)
    temp_store.set_live_trading_unlocked(False)
    result = adapter.submit_order(OrderRequest(portfolio_id=1, ticker="SPY", action="Buy", quantity=1.0))
    assert result.status == "rejected"
    assert "paper only" in (result.message or "").lower()


def test_non_du_allowed_when_unlocked_manual(temp_store, monkeypatch) -> None:
    backend = ib_connection_module.MockIBBackend(account_id="U123456", starting_cash=100000.0)
    adapter = ibkr_adapter_module.IBKRBrokerAdapter(backend, 1)
    temp_store.set_live_trading_unlocked(True)
    temp_store.set_operating_mode("manual")
    monkeypatch.setattr(ibkr_adapter_module, "is_market_open", lambda now=None: True)
    result = adapter.submit_order(OrderRequest(portfolio_id=1, ticker="SPY", action="Buy", quantity=1.0))
    assert result.status in {"filled", "submitted", "partially_filled"}


def test_non_du_blocked_in_non_manual_mode(temp_store) -> None:
    backend = ib_connection_module.MockIBBackend(account_id="U123456", starting_cash=100000.0)
    adapter = ibkr_adapter_module.IBKRBrokerAdapter(backend, 1)
    temp_store.set_live_trading_unlocked(True)
    temp_store.set_operating_mode("semi_auto")
    result = adapter.submit_order(OrderRequest(portfolio_id=1, ticker="SPY", action="Buy", quantity=1.0))
    assert result.status == "rejected"
    assert "manual mode" in (result.message or "").lower()


def test_ibkr_status_route(temp_store, monkeypatch) -> None:
    monkeypatch.setattr(persistence_module, "DB_PATH", temp_store.DB_PATH)
    app = FastAPI()
    app.include_router(regime_route.router)
    app.dependency_overrides[regime_route.require_actor] = lambda: "tester"
    client = TestClient(app)
    response = client.get("/regime/ibkr/status")
    payload = response.json()
    assert response.status_code == 200
    assert {"ib_thread_alive", "readiness", "live_trading_unlocked", "config"} <= set(payload.keys())


def test_live_unlock_requires_confirmation(temp_store, monkeypatch) -> None:
    monkeypatch.setattr(persistence_module, "DB_PATH", temp_store.DB_PATH)
    app = FastAPI()
    app.include_router(regime_route.router)
    app.dependency_overrides[regime_route.require_actor] = lambda: "tester"
    client = TestClient(app)
    response = client.put("/regime/ibkr/live-unlock", json={"unlocked": True})
    assert response.status_code == 422


def test_paper_portfolio_still_uses_paper_adapter(monkeypatch) -> None:
    runtime = {
        "get_paper_portfolio": lambda portfolio_id: {"id": int(portfolio_id), "broker_type": "paper", "current_cash": 50000.0, "starting_budget": 50000.0},
        "DEFAULT_IBKR_CONFIG": SimpleNamespace(live_backend=False, live_account_id="", account_id="DUP579027", host="127.0.0.1", port=7497, client_id=1),
        "get_ib_backend": lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should not call ibkr backend")),
        "IBKRBrokerAdapter": object,
        "PaperBrokerAdapter": lambda portfolio_id: {"adapter": "paper", "portfolio_id": portfolio_id},
    }
    adapter = regime_route._get_broker_adapter(runtime, 7)
    assert adapter["adapter"] == "paper"


def test_scheduler_skips_auto_execute_for_live_unlocked_ibkr(monkeypatch) -> None:
    monkeypatch.setattr("src.regime.scheduled_runner.list_paper_portfolios", lambda include_closed=False: [{"id": 2, "name": "Live", "broker_type": "ibkr", "status": "Active", "current_cash": 100000.0, "starting_budget": 100000.0}])
    monkeypatch.setattr("src.regime.scheduled_runner.expire_stale_plans", lambda portfolio_id: 0)
    monkeypatch.setattr("src.regime.scheduled_runner.generate_daily_plans", lambda *args, **kwargs: {"buy_plans": [], "exit_plans": [], "holdings_plans": [], "created_count": 0})
    monkeypatch.setattr("src.regime.scheduled_runner.auto_approve_plans", lambda portfolio_id: {"approved": 2})
    monkeypatch.setattr("src.regime.scheduled_runner.get_operating_mode", lambda: "autonomous")
    monkeypatch.setattr("src.regime.scheduled_runner.is_live_trading_unlocked", lambda: True)
    monkeypatch.setattr("src.regime.scheduled_runner.get_ib_backend", lambda *args, **kwargs: ib_connection_module.MockIBBackend())
    monkeypatch.setattr("src.regime.scheduled_runner.poll_pending_orders", lambda *args, **kwargs: [])
    monkeypatch.setattr("src.regime.scheduled_runner.load_payload", lambda: {"rows": []})
    result = importlib.import_module("src.regime.scheduled_runner").run_scheduled_paper_plans()
    assert result["portfolios"][0]["auto_execution"]["skipped"] is True

