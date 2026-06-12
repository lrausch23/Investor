from __future__ import annotations

import inspect
import sys
import time
import threading
from types import SimpleNamespace
from pathlib import Path
import pandas as pd

from fastapi.testclient import TestClient

from src.app.main import create_app, templates
from src.app.routes import regime as regime_route

HMM_ROOT = Path("/Volumes/T9/Projects/Dev/HMM")
if str(HMM_ROOT) not in sys.path:
    sys.path.insert(0, str(HMM_ROOT))
HMM_TESTS = HMM_ROOT / "tests"
if str(HMM_TESTS) not in sys.path:
    sys.path.insert(0, str(HMM_TESTS))

from _fixtures import FakeRegime


def _fake_runtime() -> dict:
    settings_store = {
        "frontier_provider": "auto",
        "frontier_model": "",
    }
    alert_store = [
        {"id": 1, "alert_type": "vix_freeze", "severity": "critical", "title": "VIX freeze activated", "message": "Buys frozen.", "acknowledged": 0, "created_at": "2026-03-29T12:00:00+00:00", "data": {}},
    ]

    class FakePaperBrokerAdapter:
        def __init__(self, portfolio_id, **kwargs):
            self.portfolio_id = int(portfolio_id)
            self.kwargs = kwargs

    class FakeIBKRBrokerAdapter(FakePaperBrokerAdapter):
        def __init__(self, backend, portfolio_id, **kwargs):
            super().__init__(portfolio_id, **kwargs)
            self.backend = backend

        def health(self):
            return {"connected": True, "market_hours": "regular"}

        def cancel_order(self, order_id):
            self.last_cancelled = str(order_id)
            return True

    class FakeOrderRequest:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    class FakeTaxSignal:
        def __init__(self):
            self.account_name = "Brokerage"
            self.account_type = "TAXABLE"
            self.adjusted_action = "Hold"
            self.original_action = "Hold"
            self.tax_note = "Wait for LTCG."
            self.ltcg_threshold_date = None
            self.estimated_tax_impact = 0.0
            self.wash_sale_warning = None

    class FakeDigest:
        def __init__(self):
            self.generated_at = "2026-03-22T12:00:00+00:00"
            self.benchmark_regime = "Bear"
            self.entries = [
                {
                    "ticker": "NVDA",
                    "current_regime": "Bull",
                    "regime_changed_this_week": False,
                    "composite_action": "Buy",
                    "sentiment_trend": "improving",
                    "tax_note": "Brokerage: Hold (Wait for LTCG.)",
                    "priority": "WATCH",
                }
            ]
            self.regime_changes = []
            self.sentiment_divergences = []
            self.tax_alerts = []
            self.action_items = ["NVDA: WATCH — Buy in Bull regime"]

    pd = __import__("pandas")
    return {
        "DEFAULT_TICKERS": ["NVDA", "AVGO"],
        "download_market_frame": lambda **kwargs: type("MarketSeries", (), {"frame": pd.DataFrame({"price": [100.0, 101.0], "volume": [1_000_000, 1_050_000], "high": [101.0, 102.0], "low": [99.0, 100.0]})})(),
        "generate_weekly_digest": lambda **kwargs: FakeDigest(),
        "fit_regime_model": lambda ticker, market_frame: FakeRegime(ticker, "Bear" if ticker == "SOXX" else "Bull"),
        "configured_frontier_model": lambda provider="auto": f"OpenAI: {provider}",
        "list_provider_models": lambda provider: [{"id": f"{provider}-model", "name": f"{provider.title()} Model"}],
        "get_setting": lambda key: settings_store.get(str(key)),
        "set_setting": lambda key, value: settings_store.__setitem__(str(key), str(value)),
        "get_all_settings": lambda prefix="": {k: v for k, v in settings_store.items() if not prefix or k.startswith(prefix)},
        "delete_setting": lambda key: settings_store.pop(str(key), None) is not None,
        "save_alert": lambda alert_type, title, **kwargs: {"id": 99, "alert_type": alert_type, "title": title, **kwargs},
        "get_alerts": lambda unacknowledged_only=False, alert_type=None, limit=50, since=None: [
            item for item in alert_store
            if (not unacknowledged_only or not item.get("acknowledged"))
            and (not alert_type or item.get("alert_type") == alert_type)
        ][:limit],
        "acknowledge_alert": lambda alert_id: next((item.update({"acknowledged": 1}) or True for item in alert_store if int(item["id"]) == int(alert_id)), False),
        "acknowledge_all_alerts": lambda: sum(1 for item in alert_store if not item.get("acknowledged") and not item.update({"acknowledged": 1})),
        "delete_thesis": lambda ticker: True,
        "format_alert_summary": lambda alerts: "summary",
        "get_investor_db_path": lambda: "/tmp/investor.db",
        "fit_regime_model_weekly": lambda ticker, market_frame: FakeRegime(ticker, "Bull"),
        "calibration_payload": lambda rows: {"calibration": {"bins": [{"bin": 1, "predicted": 0.7, "observed": 0.6, "count": 10}], "brier_score": 0.12}, "sharpness": {"histogram": [1, 2, 3], "count": 6}},
        "compare_to_benchmark": lambda result, benchmark_ticker="SPY", period="5y": {"alpha": 0.1},
        "get_transition_journal": lambda ticker=None, limit=50: [{"ticker": "NVDA", "previous_label": "Neutral", "current_label": "Bull", "changed_at": "2026-03-20T12:00:00+00:00", "price_at_change": 100.0, "return_5d": 0.05, "return_10d": 0.06, "return_21d": 0.08}],
        "get_transition_statistics": lambda: {"rows": [{"transition": "Neutral→Bull", "avg_return_5d": 0.05, "avg_return_10d": 0.06, "avg_return_21d": 0.08, "count": 1}]},
        "get_calibration_data": lambda lookback_days=365: [{"action": "Buy", "regime_probability": 0.8, "return_1m": 0.05}] * 30,
        "list_theses": lambda: [{"ticker": "NVDA", "thesis": "AI demand remains durable.", "updated_at": "2026-03-23T12:00:00+00:00"}],
        "list_themes": lambda include_closed=False: [{"id": 1, "name": "AI", "narrative": "Narrative", "conviction": 4, "status": "Active", "tickers": [{"ticker": "NVDA", "role": "Core", "time_horizon": "strategic"}]}],
        "get_theme": lambda theme_id: {"id": int(theme_id), "name": "AI", "narrative": "Narrative", "conviction": 4, "status": "Active", "tickers": [{"ticker": "NVDA", "role": "Core", "time_horizon": "strategic"}]},
        "get_supply_chain": lambda theme_id: [{"id": 1, "theme_id": int(theme_id), "layer": "GPU Silicon", "description": "Critical compute layer", "example_companies": "NVDA, AVGO", "generated_at": "2026-03-25T00:00:00+00:00"}],
        "generate_supply_chain": lambda theme_id, frontier_enabled=True, frontier_provider="auto": [{"id": 1, "theme_id": int(theme_id), "layer": "GPU Silicon", "description": "Critical compute layer", "example_companies": "NVDA, AVGO", "generated_at": "2026-03-25T00:00:00+00:00"}],
        "delete_supply_chain": lambda theme_id: 1,
        "get_watchlist": lambda theme_id=None, status=None, max_crowd_score=None: [{"id": 1, "theme_id": 1, "theme_name": "AI", "ticker": "WOLF", "company_name": "Wolfspeed", "supply_chain_layer": "Power", "discovery_rationale": "Critical substrate supplier", "suggested_role": "Critical-Path", "suggested_entry_price": 20.0, "suggested_stop_price": 16.0, "crowd_score": 32, "crowd_details": "{}", "regime_label": "Bull", "regime_probability": 0.61, "status": "Entry Signal"}],
        "get_watchlist_stats": lambda: {"total": 1, "by_status": {"Entry Signal": 1}},
        "get_watchlist_entry": lambda watchlist_id: {"id": int(watchlist_id), "theme_id": 1, "ticker": "WOLF", "status": "Watching"},
        "update_watchlist_status": lambda watchlist_id, status, **kwargs: {"id": int(watchlist_id), "theme_id": 1, "ticker": "WOLF", "status": status, **kwargs},
        "delete_watchlist_entry": lambda watchlist_id: True,
        "promote_candidate": lambda watchlist_id: {"theme_id": 1, "ticker": "WOLF", "role": "Critical-Path"},
        "check_entry_signals": lambda theme_id=None: [{"id": 1, "ticker": "WOLF", "status": "Entry Signal"}],
        "run_discovery_scan": lambda theme_id, frontier_enabled=True, frontier_provider="auto": [{"id": 1, "theme_id": int(theme_id), "ticker": "WOLF"}],
        "run_full_discovery": lambda frontier_enabled=True, frontier_provider="auto", theme_ids=None: {"themes_scanned": 1, "candidates_found": 1, "entry_signals": 1, "results": []},
        "expire_stale_candidates": lambda max_age_days=90: 0,
        "create_paper_portfolio": lambda name, starting_budget=100000.0, broker_type="paper": {"id": 1, "name": name, "starting_budget": float(starting_budget), "current_cash": float(starting_budget), "broker_type": broker_type, "status": "Active"},
        "get_paper_portfolio": lambda portfolio_id: {"id": int(portfolio_id), "name": "Sandbox", "starting_budget": 100000.0, "current_cash": 95000.0, "broker_type": "paper", "status": "Active", "created_at": "2026-03-26T12:00:00+00:00"},
        "list_paper_portfolios": lambda include_closed=False: [{"id": 1, "name": "Sandbox", "starting_budget": 100000.0, "current_cash": 95000.0, "broker_type": "paper", "status": "Active", "created_at": "2026-03-26T12:00:00+00:00"}],
        "update_paper_portfolio": lambda portfolio_id, **fields: {"id": int(portfolio_id), "name": fields.get("name", "Sandbox"), "starting_budget": float(fields.get("starting_budget", 100000.0)), "current_cash": 95000.0, "status": fields.get("status", "Active")},
        "delete_paper_portfolio": lambda portfolio_id: True,
        "PaperBrokerAdapter": FakePaperBrokerAdapter,
        "IBKRBrokerAdapter": FakeIBKRBrokerAdapter,
        "get_mock_ib_backend": lambda portfolio_id, starting_cash=100000.0: object(),
        "poll_pending_orders": lambda adapter, portfolio_id: [],
        "get_market_hours_status": lambda: SimpleNamespace(value="regular"),
        "DEFAULT_IBKR_CONFIG": SimpleNamespace(host="127.0.0.1", port=7497, client_id=1, account_id="DUP579027", live_backend=False),
        "validate_ibkr_readiness": lambda: {"all_clear": True, "port_is_paper": True, "host_is_local": True, "live_backend_enabled": False, "account_configured": True},
        "check_vix_freeze": lambda: {"vix": 22.5, "frozen": False, "freeze_threshold": 35.0, "resume_threshold": 30.0, "changed": False},
        "manual_override_vix_freeze": lambda unfreeze: {"vix": 22.5, "frozen": not bool(unfreeze), "freeze_threshold": 35.0, "resume_threshold": 30.0, "changed": True},
        "get_vix_freeze_threshold": lambda: 35.0,
        "get_vix_resume_threshold": lambda: 30.0,
        "is_vix_frozen": lambda: False,
        "dispatch_notification": lambda *args, **kwargs: {"in_app": True},
        "sweep_monitoring_alerts": lambda portfolio_id: [],
        "DEFAULT_RISK_GUARDRAILS": object(),
        "OrderRequest": FakeOrderRequest,
        "validate_guardrails": lambda order, adapter, guardrails: SimpleNamespace(
            allowed=str(getattr(order, "ticker", "")) != "BLOCK",
            checks=[SimpleNamespace(name="max_single_order_value", passed=str(getattr(order, "ticker", "")) != "BLOCK", message="too large" if str(getattr(order, "ticker", "")) == "BLOCK" else "", limit="10000", actual="15000" if str(getattr(order, "ticker", "")) == "BLOCK" else "1000")],
        ),
        "open_paper_position": lambda *args, **kwargs: {"id": 1, "ticker": args[1] if len(args) > 1 else kwargs.get("ticker", "NVDA")},
        "close_paper_position": lambda *args, **kwargs: {"id": int(args[0]) if args else 1, "status": "Closed"},
        "get_paper_position": lambda position_id: {"id": int(position_id), "ticker": "NVDA", "status": "Open"},
        "get_paper_positions": lambda portfolio_id, status="Open": [{"id": 1, "ticker": "NVDA", "quantity": 10.0, "entry_price": 100.0, "current_price": 110.0, "unrealized_pnl": 100.0, "stop_price": 95.0, "role": "Core", "entry_date": "2026-03-20T12:00:00+00:00", "status": status}],
        "get_trade_plan": lambda plan_id: {"id": int(plan_id), "portfolio_id": 1, "ticker": "NVDA", "action": "Buy", "quantity": 10.0, "status": "Submitted", "broker_order_id": "abc123"},
        "create_trade_plan": lambda *args, **kwargs: {"id": 1, "ticker": args[1] if len(args) > 1 else kwargs.get("ticker", "NVDA"), "status": "Pending"},
        "get_trade_plans": lambda portfolio_id, status="Pending": [{"id": 1, "portfolio_id": int(portfolio_id), "ticker": "NVDA", "action": "Buy", "quantity": 10.0, "proposed_price": 100.0, "rationale": "Entry Signal", "regime_label": "Bull", "regime_probability": 0.7, "crowd_score": 25, "source": "discovery", "status": "Approved" if str(status).lower() == "all" else status}],
        "update_trade_plan_status": lambda plan_id, status, **kwargs: {"id": int(plan_id), "ticker": "NVDA", "status": status, **kwargs},
        "get_audit_trail": lambda **kwargs: [{"id": 1, "order_id": "abc", "ticker": kwargs.get("ticker") or "NVDA", "event_type": kwargs.get("event_type") or "filled", "created_at": "2026-03-26T12:00:00+00:00"}],
        "log_audit_event": lambda **kwargs: {"id": 1, **kwargs},
        "get_daily_audit_summary": lambda portfolio_id: {"portfolio_id": int(portfolio_id), "counts": {"filled": 1}, "filled_count": 1, "blocked_count": 0, "rejected_count": 0},
        "count_todays_trades": lambda portfolio_id: 1,
        "get_paper_portfolio_summary": lambda portfolio_id: {"id": int(portfolio_id), "current_cash": 95000.0, "total_market_value": 1100.0, "realized_pnl": 50.0, "unrealized_pnl": 100.0, "total_return_pct": 1.15, "positions_open": 1, "positions_closed": 1},
        "allocate_budget": lambda portfolio_id: {"portfolio_id": int(portfolio_id), "cash_reserve": 10000.0, "unallocated": 5000.0, "themes": [{"theme_id": 1, "theme_name": "AI", "conviction": 4, "allocated": 25000.0, "by_role": {"Core": 12500.0, "Critical-Path": 8750.0, "Speculative": 3750.0}}]},
        "generate_buy_plans": lambda portfolio_id, config=None: [{"id": 1, "ticker": "WOLF", "action": "Buy"}],
        "generate_exit_plans": lambda portfolio_id, cached_regime=None: [{"id": 2, "ticker": "NVDA", "action": "Sell"}],
        "generate_daily_plans": lambda portfolio_id, cached_regime=None, cached_payload=None, config=None: {"buy_plans": [{"id": 1, "ticker": "WOLF"}], "holdings_plans": [], "exit_plans": [{"id": 2, "ticker": "NVDA"}], "created_count": 2, "generated_at": "2026-03-26T12:00:00+00:00"},
        "kill_switch": lambda portfolio_id, actor="user", reason="Manual kill switch activated": {"rejected_count": 2, "portfolio_status": "Paused", "reason": reason, "killed_at": "2026-03-26T12:00:00+00:00"} if int(portfolio_id) == 1 else None,
        "execute_approved_plans": lambda portfolio_id: {"executed": [{"plan_id": 1, "ticker": "NVDA"}], "skipped": [], "portfolio": {"id": int(portfolio_id), "current_cash": 94000.0}},
        "execute_approved_plans_via_adapter": lambda portfolio_id, adapter, guardrails, actor="user": {"executed": [{"plan_id": 1, "ticker": "NVDA", "adapter_portfolio_id": adapter.portfolio_id, "actor": actor}], "skipped": [], "portfolio": {"id": int(portfolio_id), "current_cash": 94000.0}},
        "expire_stale_trade_plans": lambda portfolio_id, max_age_days=2: 1,
        "compute_paper_performance": lambda portfolio_id: {"portfolio_id": int(portfolio_id), "total_return_pct": 1.2, "win_rate": 0.5, "realized_pnl": 50.0, "unrealized_pnl": 100.0, "total_market_value": 1100.0},
        "compute_benchmark_comparison": lambda portfolio_id, benchmark="SPY": {"benchmark": benchmark, "benchmark_return_pct": 0.8, "paper_return_pct": 1.2, "alpha_pct": 0.4},
        "compute_agent_monitor_funnel": lambda date="today": {
            "date": "2026-03-26" if date == "today" else date,
            "stages": [{"key": "candidates", "label": "Candidates", "count": 2}],
            "blockers": [{"reason": "signal stale", "count": 1}],
        },
        "compute_agent_monitor_feed": lambda limit=50, before=None: {
            "items": [
                {
                    "ts": "2026-03-26T12:00:00+00:00",
                    "agent_key": "quant",
                    "kind": "trade",
                    "text": f"Quant bought NVDA - limit {limit}",
                    "detail": {"before": before},
                }
            ],
            "has_more": False,
        },
        "portfolio_risk_summary_dict": lambda positions, results: {
            "regime_exposure": {"bull_pct": 0.75, "neutral_pct": 0.0, "bear_pct": 0.25},
            "sector_concentration": [{"sector": "Semiconductors", "value": 100000.0, "bull_pct": 0.75, "neutral_pct": 0.0, "bear_pct": 0.25, "flag": ""}],
            "correlation_risk": {"dominant_regime": "Bull", "dominant_pct": 0.75, "diversification_score": 0.58, "warning": "Moderate concentration"},
            "aggregate_transition_risk": 0.07,
            "portfolio_composite_signal": "Buy",
            "diversification_score": 0.58,
            "risk_flags": [],
            "total_value": 100000.0,
        },
        "get_portfolio_positions": lambda db_path, tickers=None, account_id=None: [],
        "get_portfolio_tickers": lambda db_path: ["NVDA", "AVGO", "TSM"],
        "save_regime_event": lambda ticker, label, state_id: {"previous_label": "Neutral", "days_in_regime": 1},
        "get_tax_assumptions": lambda db_path: {},
        "upsert_thesis": lambda ticker, thesis=None: "AI demand remains durable." if thesis is None else thesis,
        "get_wash_sale_risk": lambda db_path, ticker: "NONE",
        "run_backtest": lambda ticker, period="5y": SimpleNamespace(trades=[], total_return=0.2, annualized_return=0.1, max_drawdown=-0.05, sharpe_ratio=1.2, win_rate=0.6, avg_win=0.08, avg_loss=-0.03, profit_factor=2.0, buy_and_hold_return=0.15, equity_curve=[]),
        "positions_by_ticker_and_account": lambda positions: {
            "NVDA": [SimpleNamespace(market_value=75000.0)],
            "AVGO": [SimpleNamespace(market_value=25000.0)],
        },
        "build_composite_signal": lambda *args, **kwargs: type("Composite", (), {"composite_action": "Buy"})(),
        "compute_technicals": lambda *args, **kwargs: pd.DataFrame({"rsi_14": [45, 50], "bb_pct": [0.4, 0.5], "macd_histogram": [0.1, 0.2]}),
        "confidence_trajectory": lambda *args, **kwargs: type("Trajectory", (), {"trend": "rising"})(),
        "forward_regime_curve": lambda *args, **kwargs: pd.DataFrame({"day": [1, 2], "p_bull": [0.7, 0.72], "p_neutral": [0.2, 0.18], "p_bear": [0.1, 0.1]}),
        "intra_regime_signal": lambda *args, **kwargs: "Buy the dip",
        "multi_timeframe_signal": lambda daily, weekly: "Strong trend, high confidence",
        "sentiment_momentum": lambda *args, **kwargs: (
            type("Sentiment", (), {"trend": "improving"})(),
            pd.DataFrame({"recorded_at": ["2026-03-21", "2026-03-22"], "score": [1, 2]}),
        ),
        "signal_from_forward_curve": lambda *args, **kwargs: type("Signal", (), {"action": "Buy"})(),
        "tax_adjusted_signals": lambda *args, **kwargs: [FakeTaxSignal()],
        "compute_price_targets": lambda **kwargs: type("Targets", (), {
            "current_price": kwargs.get("current_price"),
            "price_position": "In target range",
            "entry_price": 100.0,
            "exit_price": 120.0,
            "stop_price": 95.0,
            "risk_reward_ratio": 2.0,
            "timeframe_days": 10,
            "atr_value": 2.0,
        })(),
    }


class _ImmediateExecutor:
    def submit(self, fn, *args, **kwargs):
        fn(*args, **kwargs)
        return type("DoneFuture", (), {"result": lambda self: None})()


def _client(monkeypatch) -> TestClient:
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (_fake_runtime(), None))
    monkeypatch.setattr(regime_route, "_EXECUTOR", _ImmediateExecutor())
    monkeypatch.setattr(
        regime_route,
        "get_current_tickers_by_scope",
        lambda session, scope, account_id=None: (
            ["NVDA", "AVGO"]
            if scope == "household" and account_id is None
            else ["NVDA"]
            if scope == "personal" or account_id == 101
            else ["AVGO"]
        ),
    )
    monkeypatch.setattr(
        regime_route,
        "get_available_portfolio_scopes",
        lambda session: [
            {"value": "household", "label": "All Portfolios", "ticker_count": 2, "accounts": [{"id": 101, "name": "RJ-Taxable", "ticker_count": 1, "has_holdings": True}, {"id": 202, "name": "Chase-2138", "ticker_count": 1, "has_holdings": True}]},
            {"value": "personal", "label": "Personal", "ticker_count": 1, "accounts": [{"id": 101, "name": "RJ-Taxable", "ticker_count": 1, "has_holdings": True}]},
            {"value": "trust", "label": "Trust", "ticker_count": 1, "accounts": [{"id": 202, "name": "Chase-2138", "ticker_count": 1, "has_holdings": True}]},
        ],
    )
    monkeypatch.setattr(
        regime_route,
        "_fetch_regime_change_history",
        lambda tickers, days=90: [{"ticker": "NVDA", "previous_label": "Neutral", "current_label": "Bull", "changed_at": "2026-03-20T12:00:00+00:00"}],
    )
    regime_route._JOBS.clear()
    app = create_app()
    return TestClient(app)


def test_regime_route_renders_cached_shell(monkeypatch) -> None:
    monkeypatch.setattr(
        regime_route,
        "load_payload",
        lambda: {"rows": [{"ticker": "NVDA", "regime": "Bull"}], "last_run_display": "2026-03-23 09:00:00 EDT", "warnings": []},
    )
    client = _client(monkeypatch)
    response = client.get("/regime")
    assert response.status_code == 200
    assert "Showing cached results from 2026-03-23 09:00:00 EDT." in response.text
    assert "All Portfolios" in response.text


def test_regime_run_and_poll(monkeypatch) -> None:
    saved: list[dict] = []
    monkeypatch.setattr(regime_route, "save_payload", lambda payload: saved.append(payload))
    client = _client(monkeypatch)
    run_response = client.post("/regime/run", data={"tickers": "NVDA,AVGO", "benchmark": "SOXX", "period": "3y"})
    assert run_response.status_code == 200
    job_id = run_response.json()["job_id"]

    status_response = client.get(f"/regime/status/{job_id}")
    assert status_response.status_code == 200
    payload = status_response.json()
    assert payload["status"] == "done"
    assert payload["payload"]["rows"]
    assert payload["payload"]["selected_tickers"] == ["NVDA", "AVGO"]
    assert payload["payload"]["regime_exposure"]["Bull"] == 1.0
    assert payload["payload"]["regime_history"][0]["ticker"] == "NVDA"
    assert saved


def test_holdings_endpoint(monkeypatch) -> None:
    client = _client(monkeypatch)
    response = client.get("/regime/holdings")
    assert response.status_code == 200
    assert response.json()["tickers"] == ["NVDA", "AVGO"]
    assert response.json()["groups"]["Current Holdings"] == ["NVDA", "AVGO"]


def test_ticker_limit_enforced(monkeypatch) -> None:
    client = _client(monkeypatch)
    response = client.post("/regime/run", data={"tickers": ",".join(f"T{i}" for i in range(1, 52))})
    assert response.status_code == 422


def test_portfolios_endpoint(monkeypatch) -> None:
    client = _client(monkeypatch)
    get_response = client.get("/regime/portfolios")
    assert get_response.status_code == 200
    assert get_response.json()["scopes"][0]["label"] == "All Portfolios"
    assert get_response.json()["scopes"][0]["accounts"][0]["name"] == "RJ-Taxable"


def test_paper_portfolio_routes(monkeypatch) -> None:
    client = _client(monkeypatch)
    created = client.post("/regime/paper-portfolio", data={"name": "Sandbox", "starting_budget": "125000", "broker_type": "ibkr"})
    assert created.status_code == 200
    assert created.json()["broker_type"] == "ibkr"
    listed = client.get("/regime/paper-portfolio")
    assert listed.status_code == 200
    detail = client.get("/regime/paper-portfolio/1")
    assert detail.status_code == 200
    updated = client.put("/regime/paper-portfolio/1", data={"status": "Paused"})
    assert updated.status_code == 200
    deleted = client.delete("/regime/paper-portfolio/1")
    assert deleted.status_code == 200


def test_paper_plan_and_performance_routes(monkeypatch) -> None:
    client = _client(monkeypatch)
    generated = client.post("/regime/paper-portfolio/1/plans/generate")
    assert generated.status_code == 200
    plans = client.get("/regime/paper-portfolio/1/plans?status=all")
    assert plans.status_code == 200
    approved = client.put("/regime/paper-portfolio/1/plans/1", data={"status": "Approved"})
    assert approved.status_code == 200
    executed = client.post("/regime/paper-portfolio/1/plans/execute")
    assert executed.status_code == 200
    budget = client.get("/regime/paper-portfolio/1/budget")
    assert budget.status_code == 200
    performance = client.get("/regime/paper-portfolio/1/performance")
    assert performance.status_code == 200
    audit = client.get("/regime/paper-portfolio/1/audit?ticker=NVDA")
    assert audit.status_code == 200
    assert audit.json()["audit"][0]["ticker"] == "NVDA"
    pending = client.get("/regime/paper-portfolio/1/orders/pending")
    assert pending.status_code == 200


def test_agent_monitor_funnel_route(monkeypatch) -> None:
    client = _client(monkeypatch)
    response = client.get("/regime/agents/funnel?date=2026-03-26")
    assert response.status_code == 200
    payload = response.json()
    assert payload["date"] == "2026-03-26"
    assert payload["stages"][0]["key"] == "candidates"
    assert payload["blockers"][0]["reason"] == "signal stale"


def test_agent_monitor_feed_route(monkeypatch) -> None:
    client = _client(monkeypatch)
    response = client.get("/regime/agents/feed?limit=7&before=2026-03-26T13:00:00%2B00:00")
    assert response.status_code == 200
    payload = response.json()
    assert payload["items"][0]["text"] == "Quant bought NVDA - limit 7"
    assert payload["items"][0]["detail"]["before"] == "2026-03-26T13:00:00+00:00"


def test_holdings_endpoint_filters_by_account(monkeypatch) -> None:
    client = _client(monkeypatch)
    response = client.get("/regime/holdings?portfolio_scope=personal&account_id=101")
    assert response.status_code == 200
    assert response.json()["account_id"] == 101
    assert response.json()["tickers"] == ["NVDA"]


def test_holdings_endpoint_show_all_still_respects_scope(monkeypatch) -> None:
    client = _client(monkeypatch)
    response = client.get("/regime/holdings?show_all=true&portfolio_scope=personal&account_id=101")
    assert response.status_code == 200
    assert response.json()["account_id"] == 101
    assert response.json()["tickers"] == ["NVDA"]


def test_holdings_badge_partial_renders() -> None:
    template = templates.get_template("partials/holdings/_table.html")
    view = SimpleNamespace(
        as_of="2026-03-23",
        total_value=100.0,
        total_market_value=100.0,
        total_initial_cost=50.0,
        total_pnl_amount=50.0,
        avg_pnl_pct=0.5,
        totals_missing_cost_count=0,
        positions=[
            SimpleNamespace(
                symbol="NVDA",
                account_id=None,
                account_name="Brokerage",
                taxpayer_type="PERSONAL",
                qty=1,
                latest_price=100.0,
                market_value=100.0,
                cost_basis_total=50.0,
                pnl_amount=50.0,
                pnl_pct=1.0,
                tax_status=None,
                entered_date=None,
                wash_safe_exit_date=None,
            )
        ],
    )
    rendered = template.render(view=view, account_id=None, today="2026-03-23", scope="household", regime_map={"NVDA": "Bull"})
    assert "Bull" in rendered


def test_alerts_endpoint(monkeypatch) -> None:
    client = _client(monkeypatch)
    response = client.get("/regime/alerts")
    assert response.status_code == 200
    assert response.json()["count"] == 1
    assert response.json()["alerts"][0]["alert_type"] == "vix_freeze"


def test_alert_acknowledge_routes(monkeypatch) -> None:
    client = _client(monkeypatch)
    response = client.post("/regime/alerts/1/acknowledge")
    assert response.status_code == 200
    assert response.json()["acknowledged"] is True
    all_response = client.post("/regime/alerts/acknowledge-all")
    assert all_response.status_code == 200
    assert "acknowledged_count" in all_response.json()


def test_vix_routes(monkeypatch) -> None:
    client = _client(monkeypatch)
    status_response = client.get("/regime/vix/status")
    assert status_response.status_code == 200
    assert status_response.json()["freeze_threshold"] == 35.0
    settings_response = client.put("/regime/vix/settings", json={"freeze_threshold": 40, "resume_threshold": 33})
    assert settings_response.status_code == 200
    assert settings_response.json()["freeze_threshold"] == 35.0
    override_response = client.post("/regime/vix/override", json={"unfreeze": False})
    assert override_response.status_code == 200
    assert override_response.json()["frozen"] is True


def test_journal_endpoints(monkeypatch) -> None:
    client = _client(monkeypatch)
    journal = client.get("/regime/journal?ticker=NVDA")
    stats = client.get("/regime/journal/stats")
    assert journal.status_code == 200
    assert journal.json()["rows"][0]["ticker"] == "NVDA"
    assert stats.status_code == 200
    assert stats.json()["rows"][0]["transition"] == "Neutral→Bull"


def test_backtest_endpoint(monkeypatch) -> None:
    monkeypatch.setattr(regime_route, "load_backtest_cache", lambda ticker, period: None)
    monkeypatch.setattr(regime_route, "save_backtest_cache", lambda ticker, period, data: None)
    client = _client(monkeypatch)
    response = client.get("/regime/backtest/NVDA?period=5y")
    assert response.status_code == 200
    assert response.json()["result"]["total_return"] == 0.2


def test_regime_run_surfaces_price_target_errors(monkeypatch) -> None:
    def fake_runtime():
        runtime = _fake_runtime()
        runtime["compute_price_targets"] = lambda **kwargs: (_ for _ in ()).throw(RuntimeError("missing OHLCV columns"))
        return runtime

    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (fake_runtime(), None))
    monkeypatch.setattr(regime_route, "_EXECUTOR", _ImmediateExecutor())
    monkeypatch.setattr(regime_route, "get_current_tickers_by_scope", lambda session, scope, account_id=None: ["NVDA"])
    monkeypatch.setattr(regime_route, "get_available_portfolio_scopes", lambda session: [{"value": "household", "label": "All Portfolios", "ticker_count": 1, "accounts": []}])
    monkeypatch.setattr(regime_route, "_fetch_regime_change_history", lambda tickers, days=90: [])
    monkeypatch.setattr(regime_route, "save_payload", lambda payload: None)
    regime_route._JOBS.clear()
    app = create_app()
    client = TestClient(app)

    run_response = client.post("/regime/run", data={"tickers": "NVDA", "benchmark": "SOXX", "period": "3y"})
    assert run_response.status_code == 200
    job_id = run_response.json()["job_id"]
    status_response = client.get(f"/regime/status/{job_id}")
    assert status_response.status_code == 200
    row = status_response.json()["payload"]["rows"][0]
    assert row["price_targets"] is None
    assert row["price_targets_error"] == "missing OHLCV columns"


def test_regime_run_does_not_fallback_to_unscoped_legacy_lots(monkeypatch) -> None:
    position = SimpleNamespace(
        account_name="Other Account",
        account_type="TAXABLE",
        ticker="NVDA",
        current_price=125.0,
        lots=[
            SimpleNamespace(
                acquisition_date="2026-01-01",
                qty=10.0,
                basis_total=1000.0,
                unrealized_gain=250.0,
                days_to_ltcg=300,
                term="ST",
            )
        ],
        market_value=1250.0,
    )

    def fake_runtime():
        runtime = _fake_runtime()
        runtime["get_portfolio_positions"] = lambda db_path, tickers=None, account_id=None: [position]
        runtime["positions_by_ticker_and_account"] = lambda positions: {"NVDA": [position]}
        return runtime

    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (fake_runtime(), None))
    monkeypatch.setattr(regime_route, "_EXECUTOR", _ImmediateExecutor())
    monkeypatch.setattr(regime_route, "get_current_tickers_by_scope", lambda session, scope, account_id=None: ["NVDA"])
    monkeypatch.setattr(regime_route, "get_available_portfolio_scopes", lambda session: [{"value": "household", "label": "All Portfolios", "ticker_count": 1, "accounts": []}])
    monkeypatch.setattr(regime_route, "get_lot_details_by_scope", lambda session, **kwargs: {"NVDA": []})
    monkeypatch.setattr(regime_route, "_fetch_regime_change_history", lambda tickers, days=90: [])
    monkeypatch.setattr(regime_route, "save_payload", lambda payload: None)
    regime_route._JOBS.clear()
    app = create_app()
    client = TestClient(app)

    run_response = client.post("/regime/run", data={"tickers": "NVDA", "benchmark": "SOXX", "period": "3y", "account_id": "101"})
    assert run_response.status_code == 200
    job_id = run_response.json()["job_id"]
    status_response = client.get(f"/regime/status/{job_id}")
    assert status_response.status_code == 200
    row = status_response.json()["payload"]["rows"][0]
    assert row["lot_details"] == []
    assert row["open_lot_count"] == 0


def test_normalize_selected_tickers_splits_whitespace_groups() -> None:
    assert regime_route._normalize_selected_tickers("PLAB SOLS LSCC MTRN") == ["PLAB", "SOLS", "LSCC", "MTRN"]
    assert regime_route._normalize_selected_tickers("META MSFT") == ["META", "MSFT"]
    assert regime_route._normalize_selected_tickers("BRK B") == ["BRK B"]


def test_fit_regime_with_adaptive_window_retries_short_history() -> None:
    calls = []

    def fake_fit_regime_model(*, ticker, market_frame, training_window=504, **kwargs):
        calls.append(training_window)
        if training_window == 504:
            raise RuntimeError("Insufficient history for walk-forward analysis. Need at least 504 feature rows.")
        return {"ticker": ticker, "training_window": training_window}

    frame = pd.DataFrame({"price": [1.0] * 252})
    payload = regime_route._fit_regime_with_adaptive_window({"fit_regime_model": fake_fit_regime_model}, ticker="SPY", market_frame=frame)
    assert calls[0] == 504
    assert payload["training_window"] < 504


def test_docs_route_renders_navigation_and_sections(monkeypatch) -> None:
    client = _client(monkeypatch)
    response = client.get("/docs")
    assert response.status_code == 200
    assert 'href="/static/docs.css' in response.text
    assert 'href="/docs"' in response.text
    assert 'href="#tech-overview"' in response.text
    assert 'id="tech-overview"' in response.text
    assert 'id="user-glossary"' in response.text


def test_docs_route_includes_search_and_script(monkeypatch) -> None:
    client = _client(monkeypatch)
    response = client.get("/docs")
    assert response.status_code == 200
    assert 'id="docs-search"' in response.text
    assert 'data-doc-section' in response.text
    assert 'src="/static/docs.js' in response.text


def test_ibkr_monitoring_falls_back_when_adapter_times_out(monkeypatch) -> None:
    def slow_adapter(runtime, portfolio_id):
        del runtime, portfolio_id
        time.sleep(0.05)
        return None

    runtime = _fake_runtime()
    runtime["get_paper_portfolio"] = lambda portfolio_id: {
        "id": int(portfolio_id),
        "name": "IBKR Sandbox",
        "starting_budget": 100000.0,
        "current_cash": 95000.0,
        "broker_type": "ibkr",
        "status": "Active",
    }
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    monkeypatch.setattr(regime_route, "_get_broker_adapter", slow_adapter)
    monkeypatch.setattr(regime_route, "_ADAPTER_TIMEOUT", 0.01)
    client = TestClient(create_app())
    response = client.get("/regime/paper-portfolio/2/monitoring")
    assert response.status_code == 200
    payload = response.json()
    assert payload["connection"]["connected"] is False
    assert "cached data" in payload["connection"]["note"]


def test_ibkr_precheck_returns_error_when_adapter_times_out(monkeypatch) -> None:
    def slow_adapter(runtime, portfolio_id):
        del runtime, portfolio_id
        time.sleep(0.05)
        return None

    runtime = _fake_runtime()
    runtime["get_paper_portfolio"] = lambda portfolio_id: {
        "id": int(portfolio_id),
        "name": "IBKR Sandbox",
        "starting_budget": 100000.0,
        "current_cash": 95000.0,
        "broker_type": "ibkr",
        "status": "Active",
    }
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    monkeypatch.setattr(regime_route, "_get_broker_adapter", slow_adapter)
    monkeypatch.setattr(regime_route, "_ADAPTER_TIMEOUT", 0.01)
    client = TestClient(create_app())
    response = client.post("/regime/paper-portfolio/2/plans/precheck")
    assert response.status_code == 200
    payload = response.json()
    assert payload["plans"] == []
    assert "connection unavailable" in payload["error"].lower()


def test_ibkr_precheck_returns_error_when_to_thread_raises(monkeypatch) -> None:
    runtime = _fake_runtime()
    runtime["get_paper_portfolio"] = lambda portfolio_id: {
        "id": int(portfolio_id),
        "name": "IBKR Sandbox",
        "starting_budget": 100000.0,
        "current_cash": 95000.0,
        "broker_type": "ibkr",
        "status": "Active",
    }

    async def fake_adapter(runtime_arg, portfolio_id_arg):
        del runtime_arg, portfolio_id_arg
        return object()

    async def broken_to_thread(*args, **kwargs):
        del args, kwargs
        raise RuntimeError("no current event loop")

    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    monkeypatch.setattr(regime_route, "_get_broker_adapter_safe_async", fake_adapter)
    monkeypatch.setattr(regime_route.asyncio, "to_thread", broken_to_thread)
    client = TestClient(create_app())
    response = client.post("/regime/paper-portfolio/2/plans/precheck")
    assert response.status_code == 200
    payload = response.json()
    assert payload["plans"] == []
    assert "event loop" in payload["error"].lower()


def test_ibkr_portfolio_get_returns_unavailable_status_when_adapter_times_out(monkeypatch) -> None:
    def slow_adapter(runtime, portfolio_id):
        del runtime, portfolio_id
        time.sleep(0.05)
        return None

    runtime = _fake_runtime()
    runtime["get_paper_portfolio"] = lambda portfolio_id: {
        "id": int(portfolio_id),
        "name": "IBKR Sandbox",
        "starting_budget": 100000.0,
        "current_cash": 95000.0,
        "broker_type": "ibkr",
        "status": "Active",
    }
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    monkeypatch.setattr(regime_route, "_get_broker_adapter", slow_adapter)
    monkeypatch.setattr(regime_route, "_ADAPTER_TIMEOUT", 0.01)
    client = TestClient(create_app())
    response = client.get("/regime/paper-portfolio/2")
    assert response.status_code == 200
    assert response.json()["broker_status"]["connection"] == "unavailable"


def test_ibkr_endpoints_are_async() -> None:
    assert inspect.iscoroutinefunction(regime_route.regime_paper_portfolio_get)
    assert inspect.iscoroutinefunction(regime_route.regime_paper_monitoring)
    assert inspect.iscoroutinefunction(regime_route.regime_paper_pending_orders)
    assert inspect.iscoroutinefunction(regime_route.regime_paper_cancel_order)
    assert inspect.iscoroutinefunction(regime_route.regime_paper_plan_precheck)
    assert inspect.iscoroutinefunction(regime_route.regime_paper_execute)


def test_generate_plans_not_blocked_by_ibkr_timeout(monkeypatch) -> None:
    def slow_adapter(runtime, portfolio_id):
        del runtime, portfolio_id
        time.sleep(0.05)
        return None

    runtime = _fake_runtime()
    runtime["get_paper_portfolio"] = lambda portfolio_id: {
        "id": int(portfolio_id),
        "name": "IBKR Sandbox",
        "starting_budget": 100000.0,
        "current_cash": 95000.0,
        "broker_type": "ibkr",
        "status": "Active",
    }
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    monkeypatch.setattr(regime_route, "_get_broker_adapter", slow_adapter)
    monkeypatch.setattr(regime_route, "_ADAPTER_TIMEOUT", 0.01)
    app = create_app()
    client = TestClient(app)

    results: list[int] = []

    def hit_monitoring() -> None:
        with TestClient(app) as thread_client:
            thread_client.get("/regime/paper-portfolio/2/monitoring")
            results.append(1)

    threads = [threading.Thread(target=hit_monitoring) for _ in range(3)]
    for thread in threads:
        thread.start()
    time.sleep(0.01)

    started = time.monotonic()
    response = client.post("/regime/paper-portfolio/2/plans/generate")
    elapsed = time.monotonic() - started

    for thread in threads:
        thread.join()

    assert response.status_code == 200
    assert elapsed < 1.0


def test_ibkr_precheck_continues_after_single_plan_failure(monkeypatch) -> None:
    runtime = _fake_runtime()
    runtime["get_paper_portfolio"] = lambda portfolio_id: {
        "id": int(portfolio_id),
        "name": "IBKR Sandbox",
        "starting_budget": 100000.0,
        "current_cash": 95000.0,
        "broker_type": "ibkr",
        "status": "Active",
    }
    runtime["get_trade_plans"] = lambda portfolio_id, status="Pending": [
        {"id": 1, "portfolio_id": int(portfolio_id), "ticker": "FAIL", "action": "Buy", "quantity": 10.0, "proposed_price": 100.0, "rationale": "Bad plan", "status": status},
        {"id": 2, "portfolio_id": int(portfolio_id), "ticker": "NVDA", "action": "Buy", "quantity": 5.0, "proposed_price": 100.0, "rationale": "Good plan", "status": status},
    ]

    def validate_guardrails(order, adapter, guardrails):
        del adapter, guardrails
        if getattr(order, "ticker", "") == "FAIL":
            raise RuntimeError("guardrail blew up")
        return SimpleNamespace(allowed=True, checks=[SimpleNamespace(name="ok", passed=True, message="", limit="1", actual="1")])

    async def fake_adapter(runtime_arg, portfolio_id_arg):
        del runtime_arg, portfolio_id_arg
        return object()

    runtime["validate_guardrails"] = validate_guardrails
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    monkeypatch.setattr(regime_route, "_get_broker_adapter_safe_async", fake_adapter)
    client = TestClient(create_app())
    response = client.post("/regime/paper-portfolio/2/plans/precheck")
    assert response.status_code == 200
    payload = response.json()
    assert len(payload["plans"]) == 2
    by_ticker = {row["ticker"]: row for row in payload["plans"]}
    assert by_ticker["FAIL"]["guardrail_passed"] is False
    assert "guardrail blew up" in by_ticker["FAIL"]["error"]
    assert by_ticker["NVDA"]["guardrail_passed"] is True
