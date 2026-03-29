from __future__ import annotations

import asyncio
import json
from pathlib import Path

from src.app.routes import regime as regime_route
from src.regime import config as config_module


def _ibkr_runtime() -> dict:
    return {
        "get_paper_portfolio": lambda portfolio_id: {
            "id": int(portfolio_id),
            "name": "IBKR Sandbox",
            "starting_budget": 100000.0,
            "current_cash": 95000.0,
            "broker_type": "ibkr",
            "status": "Active",
        },
        "get_paper_portfolio_summary": lambda portfolio_id: {
            "id": int(portfolio_id),
            "current_value": 101000.0,
            "current_cash": 95000.0,
            "exposure_pct": 0.06,
            "unrealized_pnl": 600.0,
        },
        "get_paper_positions": lambda portfolio_id, status="Open": [{"ticker": "NVDA", "status": status}],
        "get_trade_plans": lambda portfolio_id, status="all": [],
        "count_todays_trades": lambda portfolio_id: 0,
        "DEFAULT_RISK_GUARDRAILS": config_module.DEFAULT_RISK_GUARDRAILS,
        "validate_ibkr_readiness": lambda: {"ready": True, "issues": []},
    }


def _response_json(response) -> dict:
    return json.loads(response.body.decode("utf-8"))


def test_paper_portfolio_payload_async_uses_to_thread_for_health(monkeypatch) -> None:
    runtime = _ibkr_runtime()
    calls: list[str] = []

    class FakeAdapter:
        def health(self):
            calls.append("health")
            return {"connected": True, "market_hours": "regular"}

    async def fake_adapter(runtime_arg, portfolio_id_arg):
        del runtime_arg, portfolio_id_arg
        return FakeAdapter()

    async def fake_to_thread(func, *args, **kwargs):
        calls.append("to_thread")
        return func(*args, **kwargs)

    monkeypatch.setattr(regime_route, "_get_broker_adapter_safe_async", fake_adapter)
    monkeypatch.setattr(regime_route.asyncio, "to_thread", fake_to_thread)
    payload = asyncio.run(regime_route._paper_portfolio_payload_async(runtime, 2))
    assert calls == ["to_thread", "health"]
    assert payload["broker_status"]["connection"] == "connected"


def test_paper_portfolio_payload_async_falls_back_on_health_error(monkeypatch) -> None:
    runtime = _ibkr_runtime()

    class FakeAdapter:
        def health(self):
            raise RuntimeError("ibkr down")

    async def fake_adapter(runtime_arg, portfolio_id_arg):
        del runtime_arg, portfolio_id_arg
        return FakeAdapter()

    async def fake_to_thread(func, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr(regime_route, "_get_broker_adapter_safe_async", fake_adapter)
    monkeypatch.setattr(regime_route.asyncio, "to_thread", fake_to_thread)
    payload = asyncio.run(regime_route._paper_portfolio_payload_async(runtime, 2))
    assert payload["broker_status"]["connection"] == "disconnected"


def test_monitoring_batches_adapter_calls_in_single_to_thread(monkeypatch) -> None:
    runtime = _ibkr_runtime()
    inside = {"value": False}
    to_thread_calls: list[str] = []

    class Backend:
        def is_connected(self):
            assert inside["value"] is True
            return True

    class Manager:
        backend = Backend()

    class FakeAdapter:
        _manager = Manager()

        def get_account_summary(self):
            assert inside["value"] is True
            return {"equity": 100500.0, "cash": 95000.0, "exposure_pct": 0.06, "daily_pnl": 150.0}

        def get_positions(self):
            assert inside["value"] is True
            return [{"ticker": "NVDA"}]

        def health(self):
            assert inside["value"] is True
            return {"connected": True, "market_hours": "regular"}

    async def fake_adapter(runtime_arg, portfolio_id_arg):
        del runtime_arg, portfolio_id_arg
        return FakeAdapter()

    async def fake_to_thread(func, *args, **kwargs):
        to_thread_calls.append(func.__name__)
        inside["value"] = True
        try:
            return func(*args, **kwargs)
        finally:
            inside["value"] = False

    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    monkeypatch.setattr(regime_route, "_get_broker_adapter_safe_async", fake_adapter)
    monkeypatch.setattr(regime_route.asyncio, "to_thread", fake_to_thread)
    response = asyncio.run(regime_route.regime_paper_monitoring(2, session=None, actor="tester"))
    payload = _response_json(response)
    assert to_thread_calls == ["_load_live_monitoring"]
    assert payload["connection"]["connected"] is True
    assert payload["account"]["equity"] == 100500.0


def test_monitoring_falls_back_when_batched_call_raises(monkeypatch) -> None:
    runtime = _ibkr_runtime()

    class Backend:
        def is_connected(self):
            raise RuntimeError("event loop issue")

    class Manager:
        backend = Backend()

    class FakeAdapter:
        _manager = Manager()

        def get_account_summary(self):
            raise AssertionError("should not be reached")

        def get_positions(self):
            raise AssertionError("should not be reached")

        def health(self):
            raise AssertionError("should not be reached")

    async def fake_adapter(runtime_arg, portfolio_id_arg):
        del runtime_arg, portfolio_id_arg
        return FakeAdapter()

    async def fake_to_thread(func, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    monkeypatch.setattr(regime_route, "_get_broker_adapter_safe_async", fake_adapter)
    monkeypatch.setattr(regime_route.asyncio, "to_thread", fake_to_thread)
    response = asyncio.run(regime_route.regime_paper_monitoring(2, session=None, actor="tester"))
    payload = _response_json(response)
    assert payload["connection"]["connected"] is False
    assert "cached data" in payload["connection"]["note"].lower()


def test_monitoring_falls_back_when_adapter_unavailable(monkeypatch) -> None:
    runtime = _ibkr_runtime()

    async def fake_adapter(runtime_arg, portfolio_id_arg):
        del runtime_arg, portfolio_id_arg
        return None

    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    monkeypatch.setattr(regime_route, "_get_broker_adapter_safe_async", fake_adapter)
    response = asyncio.run(regime_route.regime_paper_monitoring(2, session=None, actor="tester"))
    payload = _response_json(response)
    assert payload["connection"]["connected"] is False
    assert payload["account"]["cash"] == 95000.0


def test_monitoring_route_returns_response_payload_shape(monkeypatch) -> None:
    runtime = _ibkr_runtime()

    async def fake_adapter(runtime_arg, portfolio_id_arg):
        del runtime_arg, portfolio_id_arg
        return None

    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    monkeypatch.setattr(regime_route, "_get_broker_adapter_safe_async", fake_adapter)
    response = asyncio.run(regime_route.regime_paper_monitoring(2, session=None, actor="tester"))
    payload = _response_json(response)
    assert set(payload.keys()) == {"account", "positions", "pending_orders", "guardrails", "connection", "readiness"}


def test_gitignore_includes_ibgw() -> None:
    gitignore = Path("/Volumes/T9/Projects/Dev/Investor/.gitignore").read_text()
    assert "IBGW/" in gitignore.splitlines()
