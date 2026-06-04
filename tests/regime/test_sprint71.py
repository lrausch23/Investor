from __future__ import annotations

import importlib
from dataclasses import asdict
from pathlib import Path

import pandas as pd
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.app.routes import regime as regime_route


@pytest.fixture
def temp_modules(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    import src.regime.hurdle_rate as hurdle_rate
    import src.regime.paper_trading as paper_trading
    import src.regime.persistence as store
    import src.regime.slippage as slippage

    store = importlib.reload(store)
    store.DB_PATH = tmp_path / "regime_watch.db"
    hurdle_rate = importlib.reload(hurdle_rate)
    paper_trading = importlib.reload(paper_trading)
    slippage = importlib.reload(slippage)
    return store, hurdle_rate, paper_trading, slippage


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(regime_route.router)
    app.dependency_overrides[regime_route.require_actor] = lambda: "tester"
    return TestClient(app)


def test_slippage_metric_buy_positive(temp_modules) -> None:
    _store, _hurdle, _paper, slippage = temp_modules
    metric = slippage.compute_slippage_metric(
        {"id": 1, "ticker": "NVDA", "action": "Buy", "arrival_price": 100.0, "execution_price": 100.2}
    )
    assert metric.impl_shortfall_bps == pytest.approx(20.0)


def test_slippage_metric_sell_positive(temp_modules) -> None:
    _store, _hurdle, _paper, slippage = temp_modules
    metric = slippage.compute_slippage_metric(
        {"id": 1, "ticker": "NVDA", "action": "Sell", "arrival_price": 100.0, "execution_price": 99.8}
    )
    assert metric.impl_shortfall_bps == pytest.approx(20.0)


def test_vs_vwap_and_close_computation(temp_modules) -> None:
    _store, _hurdle, _paper, slippage = temp_modules
    metric = slippage.compute_slippage_metric(
        {
            "id": 1,
            "ticker": "NVDA",
            "action": "Buy",
            "arrival_price": 100.0,
            "execution_price": 100.1,
            "vwap_benchmark": 99.9,
            "close_price": 100.2,
            "proposed_price": 100.0,
        }
    )
    assert metric.vs_vwap_bps == pytest.approx(((100.1 - 99.9) / 99.9) * 10000.0)
    assert metric.vs_close_bps == pytest.approx(((100.1 - 100.2) / 100.2) * 10000.0)


def test_backfill_populates_vwap_and_close(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    store, _hurdle, _paper, slippage = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox")
    plan = store.create_trade_plan(portfolio["id"], "NVDA", "Buy", 10, "Entry", proposed_price=100.0, arrival_price=100.0)
    store.update_trade_plan_status(
        int(plan["id"]),
        "Executed",
        executed_at="2026-03-31T15:00:00+00:00",
        execution_price=100.2,
    )
    monkeypatch.setattr(
        slippage,
        "download_daily_bars",
        lambda ticker, period="3mo", auto_adjust=False: pd.DataFrame(
            {"High": [101.0], "Low": [99.0], "Close": [100.0]},
            index=pd.to_datetime(["2026-03-31"]),
        ),
    )
    result = slippage.backfill_execution_benchmarks(portfolio["id"], "2026-03-31")
    refreshed = store.get_trade_plan(int(plan["id"]))
    assert result["updated"] == 1
    assert refreshed["vwap_benchmark"] == pytest.approx(100.0)
    assert refreshed["close_price"] == pytest.approx(100.0)


def test_estimate_execution_cost_with_history(temp_modules) -> None:
    store, hurdle_rate, _paper, slippage = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox")
    store.create_trade_plan(portfolio["id"], "NVDA", "Buy", 10, "Entry", proposed_price=100.0, arrival_price=100.0, routing_strategy="VWAP", algo_strategy="VWAP")
    store.update_trade_plan_status(1, "Executed", executed_at="2026-03-31T15:00:00+00:00", execution_price=100.1)
    store.create_trade_plan(portfolio["id"], "NVDA", "Sell", 10, "Exit", proposed_price=100.0, arrival_price=100.0, routing_strategy="VWAP", algo_strategy="VWAP")
    store.update_trade_plan_status(2, "Executed", executed_at="2026-03-31T15:30:00+00:00", execution_price=99.9)
    store.create_trade_plan(portfolio["id"], "NVDA", "Buy", 10, "Entry 2", proposed_price=100.0, arrival_price=100.0, routing_strategy="VWAP", algo_strategy="VWAP")
    store.update_trade_plan_status(3, "Executed", executed_at="2026-03-31T16:00:00+00:00", execution_price=100.1)
    hurdle_rate.set_hurdle_settings({"slippage_feedback_enabled": True, "slippage_min_sample_size": 3, "slippage_lookback_days": 365})
    estimated = slippage.estimate_execution_cost("NVDA", "VWAP", "VWAP", portfolio["id"])
    assert estimated == pytest.approx(0.2)


def test_hurdle_rate_with_execution_cost(temp_modules) -> None:
    _store, hurdle_rate, _paper, _slippage = temp_modules
    result = hurdle_rate.check_hurdle_rate("NVDA", 100.0, 108.0, estimated_execution_cost_pct=0.3)
    assert result.estimated_execution_cost_pct == pytest.approx(0.3)
    assert result.net_return_pct == pytest.approx((8.0 - 0.3) * 0.68)


def test_create_plan_with_arrival_price_and_update_benchmarks(temp_modules) -> None:
    store, _hurdle, _paper, _slippage = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox")
    plan = store.create_trade_plan(portfolio["id"], "NVDA", "Buy", 10, "Entry", proposed_price=100.0, arrival_price=99.95)
    assert plan["arrival_price"] == pytest.approx(99.95)
    assert store.update_trade_plan_benchmarks(int(plan["id"]), vwap_benchmark=100.1, close_price=100.5)
    refreshed = store.get_trade_plan(int(plan["id"]))
    assert refreshed["vwap_benchmark"] == pytest.approx(100.1)
    assert refreshed["close_price"] == pytest.approx(100.5)


def test_save_load_execution_quality_snapshot(temp_modules) -> None:
    store, _hurdle, _paper, _slippage = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox")
    report = {
        "portfolio_id": portfolio["id"],
        "analysis_date": "2026-03-31",
        "total_trades": 3,
        "overall_avg_impl_shortfall_bps": 2.5,
        "overall_avg_vs_vwap_bps": 1.5,
        "by_strategy": [{"dimension": "strategy", "bucket": "VWAP", "sample_count": 3}],
        "by_algo": [],
        "by_time_of_day": [],
        "by_theme": [],
        "by_adv_bucket": [],
        "patterns": [{"pattern_type": "morning_bias"}],
        "best_strategy": "VWAP",
        "worst_strategy": "Passive",
    }
    store.save_execution_quality_snapshot(report)
    loaded = store.get_execution_quality_snapshot(portfolio["id"])
    history = store.get_execution_quality_history(portfolio["id"])
    assert loaded is not None
    assert loaded["total_trades"] == 3
    assert loaded["by_strategy"][0]["bucket"] == "VWAP"
    assert history[0]["analysis_date"] == "2026-03-31"


def test_compute_execution_quality_groups_by_strategy(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    store, _hurdle, _paper, slippage = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox")
    for idx, strategy in enumerate(("VWAP", "TWAP"), start=1):
        plan = store.create_trade_plan(
            portfolio["id"],
            f"SYM{idx}",
            "Buy",
            10,
            "Entry",
            proposed_price=100.0,
            arrival_price=100.0,
            routing_strategy=strategy,
            algo_strategy=strategy,
            theme_id=None,
        )
        store.update_trade_plan_status(int(plan["id"]), "Executed", executed_at=f"2026-03-31T1{idx}:00:00+00:00", execution_price=100.0 + idx / 10)
    monkeypatch.setattr(slippage, "compute_adv", lambda ticker, lookback_days=20: 2_000_000.0)
    report = slippage.compute_execution_quality(portfolio["id"], lookback_days=365)
    buckets = {row.bucket for row in report.by_strategy}
    assert buckets == {"TWAP", "VWAP"}


def test_buy_plan_has_arrival_price(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    store, hurdle_rate, paper_trading, _slippage = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox")
    theme = store.create_theme("AI", conviction=5)
    store.upsert_watchlist_candidate(theme["id"], "NVDA", suggested_entry_price=100.0, crowd_score=30, regime_label="Bull", regime_probability=0.8, status="Entry Signal")
    store.save_signal_snapshot(
        ticker="NVDA",
        snapshot_date="2026-03-31",
        action="Buy",
        regime_label="Bull",
        regime_probability=0.8,
        composite_strength=0.7,
        benchmark="SPY",
        current_price=100.0,
        entry_price=100.0,
        exit_price=110.0,
        stop_price=95.0,
        risk_reward_ratio=2.0,
        timeframe_days=10,
        expected_regime_duration=12.0,
    )
    hurdle_rate.set_hurdle_settings({"hurdle_enabled": True, "duration_gate_enabled": True, "slippage_feedback_enabled": False})
    monkeypatch.setattr(paper_trading, "allocate_budget", lambda portfolio_id, config=None: {"themes": [{"theme_id": theme["id"], "by_role": {"Critical-Path": 10000.0}}]})
    monkeypatch.setattr(paper_trading, "_lookup_atr", lambda ticker: None)
    monkeypatch.setattr(paper_trading, "_lookup_beta", lambda ticker: None)
    monkeypatch.setattr(paper_trading, "_batch_current_prices", lambda tickers: {"NVDA": 100.0})
    plans = paper_trading.generate_buy_plans(portfolio["id"])
    assert plans
    assert plans[0]["arrival_price"] == pytest.approx(100.0)


def test_route_execution_quality_report_and_trend(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _client()
    fake_runtime = {
        "list_paper_portfolios": lambda include_closed=False: [{"id": 1, "status": "Active"}],
        "get_paper_portfolio": lambda portfolio_id: {"id": int(portfolio_id), "name": "Sandbox"},
        "get_execution_quality_snapshot": lambda portfolio_id: None,
        "compute_execution_quality": lambda portfolio_id, lookback_days=90: type(
            "Report",
            (),
            {
                "portfolio_id": int(portfolio_id),
                "analysis_date": "2026-03-31",
                "total_trades": 2,
                "overall_avg_impl_shortfall_bps": 1.5,
                "overall_avg_vs_vwap_bps": 1.0,
                "by_strategy": [],
                "by_algo": [],
                "by_time_of_day": [],
                "by_theme": [],
                "by_adv_bucket": [],
                "patterns": [],
                "best_strategy": "VWAP",
                "worst_strategy": "TWAP",
            },
        )(),
        "save_execution_quality_snapshot": lambda report: 1,
        "get_execution_quality_history": lambda portfolio_id, limit=30: [{"analysis_date": "2026-03-31", "overall_avg_impl_shortfall_bps": 1.5, "overall_avg_vs_vwap_bps": 1.0, "total_trades": 2}],
        "get_execution_quality_trades": lambda portfolio_id, **kwargs: [{"id": 1, "ticker": "NVDA"}],
        "get_execution_quality_ticker_diagnostic": lambda ticker, portfolio_id, lookback_days=90: {"ticker": ticker, "sample_count": 1},
    }
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (fake_runtime, None))
    report = client.get("/regime/execution-quality/1")
    trend = client.get("/regime/execution-quality/1/trend")
    trades = client.get("/regime/execution-quality/1/trades")
    ticker = client.get("/regime/execution-quality/ticker/NVDA?portfolio_id=1")
    assert report.status_code == 200
    assert report.json()["best_strategy"] == "VWAP"
    assert trend.status_code == 200
    assert trend.json()[0]["trade_count"] == 2
    assert trades.status_code == 200
    assert trades.json()["count"] == 1
    assert ticker.status_code == 200
    assert ticker.json()["ticker"] == "NVDA"


def test_route_slippage_settings_get_put(temp_modules) -> None:
    _store, _hurdle, _paper, _slippage = temp_modules
    client = _client()
    response = client.get("/regime/slippage/settings")
    assert response.status_code == 200
    payload = response.json()
    assert payload["slippage_feedback_enabled"] is True
    update = client.put(
        "/regime/slippage/settings",
        json={
            "slippage_feedback_enabled": False,
            "slippage_min_sample_size": 12,
            "slippage_lookback_days": 120,
        },
    )
    assert update.status_code == 200
    updated = update.json()
    assert updated["slippage_feedback_enabled"] is False
    assert updated["slippage_min_sample_size"] == 12
    assert updated["slippage_lookback_days"] == 120
