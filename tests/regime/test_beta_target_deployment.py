from __future__ import annotations

import datetime as dt
import importlib
import math
from types import SimpleNamespace

import pandas as pd
import pytest

from src.regime import broker_adapter as broker_adapter_module
from src.regime import config as config_module
from src.regime import data as data_module
from src.regime import ibkr_market_data as ibkr_market_data_module
from src.regime import paper_trading as paper_trading_module
from src.regime import persistence as persistence_module
from src.regime import scheduled_runner as scheduled_runner_module
from scripts import run_regime_beta_market_session as beta_market_session_module


@pytest.fixture()
def temp_modules(tmp_path, monkeypatch):
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    store = importlib.reload(persistence_module)
    monkeypatch.setattr(store, "DB_PATH", tmp_path / "regime_watch.db")
    config = importlib.reload(config_module)
    broker = importlib.reload(broker_adapter_module)
    ibkr_market_data = importlib.reload(ibkr_market_data_module)
    data = importlib.reload(data_module)
    paper = importlib.reload(paper_trading_module)
    monkeypatch.setattr(paper, "universe_screen_enabled", lambda: False)
    return store, config, broker, ibkr_market_data, data, paper


def test_beta_target_progress_uses_two_percent_monthly_compounding(temp_modules) -> None:
    store, _config, _broker, _ibkr_market_data, _data, paper = temp_modules
    portfolio = store.create_paper_portfolio("Regime Agent Beta - Paper", 25_000.0)
    created_at = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=30.4375)).isoformat()
    with store._connect() as conn:
        conn.execute("UPDATE paper_portfolio SET created_at = ? WHERE id = ?", (created_at, int(portfolio["id"])))

    progress = paper.compute_beta_target_progress(int(portfolio["id"]))
    assert progress["target_return"] == pytest.approx(0.02, rel=1e-3)
    assert progress["status"] == "behind"

    store.update_paper_portfolio(int(portfolio["id"]), current_cash=25_600.0)
    progress = paper.compute_beta_target_progress(int(portfolio["id"]))
    assert progress["current_monthly_run_rate"] > progress["target_monthly_return"]
    assert progress["status"] == "on_track"
    assert progress["basis"] == "after_tax"


def test_agent_candidate_policy_is_strategy_flexible_by_default(temp_modules) -> None:
    store, _config, _broker, _ibkr_market_data, _data, _paper = temp_modules
    from src.regime import agent_policy as agent_policy_module

    agent_policy = importlib.reload(agent_policy_module)
    first = store.create_paper_portfolio("Regime Agent Beta - Agent 1 Quant", 25_000.0, broker_type="ibkr")
    second = store.create_paper_portfolio("Regime Agent Beta - Agent 2 Fundamental", 25_000.0, broker_type="ibkr")
    store.set_setting("regime_beta_portfolio_ids", f"{int(first['id'])},{int(second['id'])}")

    decision = agent_policy.agent_candidate_policy(
        int(second["id"]),
        "NVDA",
        source="discovery",
        candidate={"regime_probability": 0.05, "fundamental_gate_passed": False, "spread_pct": 0.02},
    )

    assert decision["allowed"] is True
    assert decision["reason"] == "strategy_flexible_policy"
    assert decision["policy"] == "strategy_flexible"


def test_daily_loss_guardrail_scales_down_for_beta_budget(temp_modules, monkeypatch) -> None:
    store, config, broker, _ibkr_market_data, _data, _paper = temp_modules
    portfolio = store.create_paper_portfolio("Regime Agent Beta - Paper", 25_000.0)
    store.open_paper_position(int(portfolio["id"]), "NVDA", 1, 100.0, "2026-06-08T14:00:00+00:00")
    adapter = broker.PaperBrokerAdapter(int(portfolio["id"]))
    monkeypatch.setattr(
        adapter,
        "get_account_summary",
        lambda: broker.AccountSummary(
            portfolio_id=int(portfolio["id"]),
            equity=25_000.0,
            cash=25_000.0,
            market_value=0.0,
            realized_pnl=0.0,
            unrealized_pnl=0.0,
            daily_pnl=-600.0,
            exposure_pct=0.0,
        ),
    )
    result = broker.validate_guardrails(
        broker.OrderRequest(
            portfolio_id=int(portfolio["id"]),
            ticker="NVDA",
            action="Buy",
            quantity=1,
            order_type="limit",
            limit_price=100.0,
        ),
        adapter,
        config.RiskGuardrails(daily_loss_limit=5_000.0, daily_loss_limit_pct=0.02),
    )
    daily_loss = next(check for check in result.checks if check.name == "daily_loss_limit")
    assert daily_loss.limit == pytest.approx(500.0)
    assert daily_loss.passed is False
    assert result.allowed is False

    exit_result = broker.validate_guardrails(
        broker.OrderRequest(
            portfolio_id=int(portfolio["id"]),
            ticker="NVDA",
            action="Sell",
            quantity=1,
            order_type="limit",
            limit_price=100.0,
        ),
        adapter,
        config.RiskGuardrails(daily_loss_limit=5_000.0, daily_loss_limit_pct=0.02),
    )
    exit_daily_loss = next(check for check in exit_result.checks if check.name == "daily_loss_limit")
    assert exit_daily_loss.limit == pytest.approx(500.0)
    assert exit_daily_loss.passed is True
    assert exit_result.allowed is True


def test_ibkr_paper_backend_readiness_uses_paper_flag(monkeypatch) -> None:
    monkeypatch.setenv("IBKR_PAPER_BACKEND", "true")
    monkeypatch.setenv("IBKR_LIVE_BACKEND", "false")
    monkeypatch.setenv("IBKR_HOST", "127.0.0.1")
    monkeypatch.setenv("IBKR_PORT", "7497")
    monkeypatch.setenv("IBKR_ACCOUNT_ID", "DUP579027")
    config = importlib.reload(config_module)

    checks = config.validate_ibkr_readiness()

    assert checks["live_backend_enabled"] is False
    assert checks["paper_backend_enabled"] is True
    assert checks["paper_backend_ready"] is True
    assert checks["execution_backend_enabled"] is True
    assert checks["all_clear"] is True


def test_beta_budget_guardrail_blocks_sell_without_local_position(temp_modules) -> None:
    store, config, broker, _ibkr_market_data, _data, _paper = temp_modules
    portfolio = store.create_paper_portfolio("Regime Agent Beta - IBKR Paper", 25_000.0, broker_type="ibkr")
    adapter = broker.MockBrokerAdapter(fill_price=100.0)
    adapter.set_account_summary(
        broker.AccountSummary(
            portfolio_id=int(portfolio["id"]),
            equity=1_000_000.0,
            cash=950_000.0,
            market_value=50_000.0,
            realized_pnl=0.0,
            unrealized_pnl=0.0,
            daily_pnl=0.0,
            exposure_pct=0.05,
        )
    )

    result = broker.validate_guardrails(
        broker.OrderRequest(
            portfolio_id=int(portfolio["id"]),
            ticker="NVDA",
            action="Sell",
            quantity=1,
            order_type="market",
        ),
        adapter,
        config.DEFAULT_RISK_GUARDRAILS,
    )

    sell_check = next(check for check in result.checks if check.name == "portfolio_sell_position_available")
    assert sell_check.passed is False
    assert result.allowed is False


def test_guardrails_block_autonomous_market_buy_orders(temp_modules) -> None:
    store, config, broker, _ibkr_market_data, _data, _paper = temp_modules
    portfolio = store.create_paper_portfolio("Regime Agent Beta - IBKR Paper", 25_000.0, broker_type="ibkr")
    adapter = broker.MockBrokerAdapter(fill_price=100.0)

    result = broker.validate_guardrails(
        broker.OrderRequest(
            portfolio_id=int(portfolio["id"]),
            ticker="SPY",
            action="Buy",
            quantity=1,
            order_type="market",
            source="agent",
        ),
        adapter,
        config.DEFAULT_RISK_GUARDRAILS,
    )

    market_buy = next(check for check in result.checks if check.name == "agent_market_buy_disabled")
    assert market_buy.passed is False
    assert result.allowed is False


def test_pending_broker_submission_audits_as_submitted(temp_modules, monkeypatch) -> None:
    store, config, broker, _ibkr_market_data, _data, _paper = temp_modules
    portfolio = store.create_paper_portfolio("Regime Agent Beta - IBKR Paper", 25_000.0, broker_type="ibkr")
    adapter = broker.MockBrokerAdapter(fill_price=100.0)
    adapter.set_account_summary(
        broker.AccountSummary(
            portfolio_id=int(portfolio["id"]),
            equity=25_000.0,
            cash=25_000.0,
            market_value=0.0,
            realized_pnl=0.0,
            unrealized_pnl=0.0,
            daily_pnl=0.0,
            exposure_pct=0.0,
        )
    )
    monkeypatch.setattr(
        adapter,
        "submit_order",
        lambda order: broker.OrderResult(
            order_id="ibkr-1",
            status="pending",
            ticker=str(order.ticker).upper(),
            action=order.action,
            quantity=order.quantity,
        ),
    )

    _guardrail, result = broker.submit_guarded_order(
        broker.OrderRequest(
            portfolio_id=int(portfolio["id"]),
            ticker="NVDA",
            action="Buy",
            quantity=1,
            order_type="limit",
            limit_price=100.0,
        ),
        adapter,
        config.DEFAULT_RISK_GUARDRAILS,
        actor="user",
    )

    assert result is not None
    events = store.get_audit_trail(portfolio_id=int(portfolio["id"]), order_id="ibkr-1", days=1, limit=10)
    assert [event["event_type"] for event in events].count("submitted") >= 1
    assert "rejected" not in [event["event_type"] for event in events]


def test_rejected_broker_submission_does_not_audit_as_submitted(temp_modules, monkeypatch) -> None:
    store, config, broker, _ibkr_market_data, _data, _paper = temp_modules
    portfolio = store.create_paper_portfolio("Regime Agent Beta - IBKR Paper", 25_000.0, broker_type="ibkr")
    adapter = broker.MockBrokerAdapter(fill_price=100.0)
    adapter.set_account_summary(
        broker.AccountSummary(
            portfolio_id=int(portfolio["id"]),
            equity=25_000.0,
            cash=25_000.0,
            market_value=0.0,
            realized_pnl=0.0,
            unrealized_pnl=0.0,
            daily_pnl=0.0,
            exposure_pct=0.0,
        )
    )
    monkeypatch.setattr(
        adapter,
        "submit_order",
        lambda order: broker.OrderResult(
            order_id="ibkr-rejected-1",
            status="rejected",
            ticker=str(order.ticker).upper(),
            action=order.action,
            quantity=order.quantity,
            message="IBKR connection unavailable.",
        ),
    )

    _guardrail, result = broker.submit_guarded_order(
        broker.OrderRequest(
            portfolio_id=int(portfolio["id"]),
            ticker="AVGO",
            action="Buy",
            quantity=1,
            order_type="limit",
            limit_price=100.0,
        ),
        adapter,
        config.DEFAULT_RISK_GUARDRAILS,
        actor="scheduler",
    )

    assert result is not None
    events = store.get_audit_trail(portfolio_id=int(portfolio["id"]), order_id="ibkr-rejected-1", days=1, limit=10)
    assert [event["event_type"] for event in events] == ["rejected"]


def test_stale_limit_price_blocked_against_fresh_quote(temp_modules, monkeypatch) -> None:
    store, config, broker, _ibkr_market_data, _data, _paper = temp_modules
    portfolio = store.create_paper_portfolio("Regime Agent Beta - IBKR Paper", 25_000.0, broker_type="ibkr")
    adapter = broker.MockBrokerAdapter(fill_price=100.0)
    adapter.set_account_summary(
        broker.AccountSummary(
            portfolio_id=int(portfolio["id"]),
            equity=25_000.0,
            cash=25_000.0,
            market_value=0.0,
            realized_pnl=0.0,
            unrealized_pnl=0.0,
            daily_pnl=0.0,
            exposure_pct=0.0,
        )
    )
    monkeypatch.setattr(adapter, "get_current_price", lambda ticker, action="": 215.59, raising=False)

    guardrail, result = broker.submit_guarded_order(
        broker.OrderRequest(
            portfolio_id=int(portfolio["id"]),
            ticker="NVDA",
            action="Buy",
            quantity=20,
            order_type="limit",
            limit_price=123.46,
            routing_strategy="Limit (Ask)",
        ),
        adapter,
        config.DEFAULT_RISK_GUARDRAILS,
        actor="scheduler",
    )

    assert result is None
    quote_check = next(check for check in guardrail.checks if check.name == "fresh_quote_limit_price_deviation")
    assert quote_check.passed is False
    assert quote_check.actual == pytest.approx(abs(123.46 - 215.59) / 215.59)
    assert quote_check.limit == pytest.approx(0.03)


def test_scheduled_ibkr_paper_backend_executes_with_live_locked(temp_modules, monkeypatch) -> None:
    store, _config, _broker, _ibkr_market_data, _data, _paper = temp_modules
    monkeypatch.setenv("IBKR_PAPER_BACKEND", "true")
    monkeypatch.setenv("IBKR_LIVE_BACKEND", "false")
    monkeypatch.setenv("IBKR_HOST", "127.0.0.1")
    monkeypatch.setenv("IBKR_PORT", "7497")
    monkeypatch.setenv("IBKR_ACCOUNT_ID", "DUP579027")
    importlib.reload(config_module)
    scheduled = importlib.reload(scheduled_runner_module)
    portfolio = store.create_paper_portfolio("Regime Agent Beta - IBKR Paper", 25_000.0, broker_type="ibkr")

    captured: dict[str, object] = {}
    monkeypatch.setattr(scheduled, "list_paper_portfolios", lambda include_closed=False: [portfolio])
    monkeypatch.setattr(scheduled, "load_payload", lambda: {"rows": []})
    monkeypatch.setattr(scheduled, "run_pre_trade_validation", lambda tickers, vix=None: {"valid": True, "issues": []})
    monkeypatch.setattr(scheduled, "check_vix_freeze", lambda: {"frozen": False, "vix": None})
    monkeypatch.setattr(scheduled, "sweep_monitoring_alerts", lambda portfolio_id: [])
    monkeypatch.setattr(scheduled, "expire_stale_plans", lambda portfolio_id: 0)
    monkeypatch.setattr(scheduled, "generate_daily_plans", lambda *args, **kwargs: {"buy_plans": [], "exit_plans": [], "holdings_plans": [], "created_count": 0})
    monkeypatch.setattr(scheduled, "auto_approve_plans", lambda portfolio_id: {"approved": 1})
    monkeypatch.setattr(scheduled, "get_operating_mode", lambda: "autonomous")
    monkeypatch.setattr(scheduled, "is_live_trading_unlocked", lambda: False)
    monkeypatch.setattr(scheduled, "poll_pending_orders", lambda adapter, portfolio_id: [])

    def fake_execute(portfolio_id, adapter, guardrails, actor="scheduler"):
        captured["adapter_type"] = adapter.__class__.__name__
        captured["actor"] = actor
        return {"executed": [{"plan_id": 1}], "skipped": []}

    monkeypatch.setattr(scheduled, "auto_execute_approved", fake_execute)

    payload = scheduled.run_scheduled_paper_plans()

    portfolio_payload = payload["portfolios"][0]
    assert portfolio_payload["broker_status"]["execution_mode"] == "ibkr_paper"
    assert portfolio_payload["auto_execution"]["executed"]
    assert captured["adapter_type"] == "IBKRBrokerAdapter"
    assert captured["actor"] == "scheduler"


def test_scheduled_runner_skips_generation_and_execution_outside_enabled_window(temp_modules, monkeypatch) -> None:
    store, _config, _broker, _ibkr_market_data, _data, _paper = temp_modules
    scheduled = importlib.reload(scheduled_runner_module)
    portfolio = store.create_paper_portfolio("Regime Agent Beta - IBKR Paper", 25_000.0, broker_type="paper")
    store.set_setting("regime_beta_schedule_enabled", "true")
    store.set_setting("regime_beta_preferred_run_window", "10:05-15:30 America/New_York")

    monkeypatch.setattr(scheduled, "list_paper_portfolios", lambda include_closed=False: [portfolio])
    monkeypatch.setattr(scheduled, "load_payload", lambda: {"rows": []})
    monkeypatch.setattr(scheduled, "run_pre_trade_validation", lambda tickers, vix=None: {"valid": True, "issues": []})
    monkeypatch.setattr(scheduled, "check_vix_freeze", lambda: {"frozen": False, "vix": None})
    monkeypatch.setattr(scheduled, "sweep_monitoring_alerts", lambda portfolio_id: [])
    monkeypatch.setattr(scheduled, "expire_stale_plans", lambda portfolio_id: 0)
    monkeypatch.setattr(scheduled, "_preferred_market_window_status", lambda now=None: {"in_window": False, "reason": "after_preferred_window"})
    monkeypatch.setattr(scheduled, "generate_daily_plans", lambda *args, **kwargs: pytest.fail("generation must be skipped outside the enabled window"))
    monkeypatch.setattr(scheduled, "auto_approve_plans", lambda portfolio_id: pytest.fail("auto approval must be skipped outside the enabled window"))
    monkeypatch.setattr(scheduled, "auto_execute_approved", lambda *args, **kwargs: pytest.fail("execution must be skipped outside the enabled window"))

    payload = scheduled.run_scheduled_paper_plans()

    portfolio_payload = payload["portfolios"][0]
    assert portfolio_payload["buy_count"] == 0
    assert portfolio_payload["auto_approval"]["skipped_reason"] == "after_preferred_window"
    assert portfolio_payload["auto_execution"]["skipped"] is True
    assert portfolio_payload["auto_execution"]["reason"] == "after_preferred_window"


def test_auto_approval_respects_autonomous_portfolio_allowlist(temp_modules) -> None:
    store, _config, _broker, _ibkr_market_data, _data, paper = temp_modules
    enabled = store.create_paper_portfolio("Regime Agent Beta - Paper", 25_000.0)
    other = store.create_paper_portfolio("Unrelated Paper Portfolio", 25_000.0)
    store.create_trade_plan(other["id"], "NVDA", "Buy", 1, "Should remain pending", proposed_price=100.0)
    store.set_operating_mode("autonomous")
    store.set_setting("autonomous_portfolio_ids", str(int(enabled["id"])))

    result = paper.auto_approve_plans(int(other["id"]))

    assert result["portfolio_autonomy_enabled"] is False
    assert result["approved"] == 0
    plans = store.get_trade_plans(int(other["id"]), status="Pending")
    assert len(plans) == 1


def test_market_session_window_uses_regular_us_market_hours(temp_modules) -> None:
    _store, _config, _broker, ibkr_market_data, _data, _paper = temp_modules
    del ibkr_market_data
    runner = importlib.reload(beta_market_session_module)
    from src.regime.ib_types import ET

    regular = runner.market_session_window_status(dt.datetime(2026, 3, 26, 10, 5, tzinfo=ET))
    pre_window = runner.market_session_window_status(dt.datetime(2026, 3, 26, 9, 44, tzinfo=ET))
    holiday = runner.market_session_window_status(dt.datetime(2026, 12, 25, 10, 5, tzinfo=ET))

    assert regular["in_window"] is True
    assert pre_window["in_window"] is False
    assert pre_window["reason"] == "before_preferred_window"
    assert holiday["in_window"] is False
    assert holiday["reason"] == "market_closed"


def test_market_session_runner_skips_duplicate_trade_date(temp_modules, monkeypatch) -> None:
    store, _config, _broker, _ibkr_market_data, _data, _paper = temp_modules
    runner = importlib.reload(beta_market_session_module)
    from src.regime.ib_types import ET

    store.set_setting("regime_beta_last_market_session_cycle_date", "2026-03-26")
    monkeypatch.setattr(runner, "_record_status", lambda payload: None)
    monkeypatch.setattr(
        "scripts.deploy_regime_beta._run_beta_paper_cycle",
        lambda portfolio_id: (_ for _ in ()).throw(AssertionError("duplicate run should skip")),
    )

    result = runner.run_market_session_cycle(now=dt.datetime(2026, 3, 26, 10, 5, tzinfo=ET))

    assert result["status"] == "skipped"
    assert result["skip_reason"] == "already_ran_for_trade_date"


def test_holdings_plans_use_fresh_current_price_over_entry_target(temp_modules, monkeypatch) -> None:
    store, _config, _broker, _ibkr_market_data, _data, paper = temp_modules
    portfolio = store.create_paper_portfolio("Regime Agent Beta - IBKR Paper", 25_000.0, broker_type="ibkr")
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 215.59})
    monkeypatch.setattr(
        paper,
        "decide_routing",
        lambda **kwargs: SimpleNamespace(
            order_type="limit",
            time_in_force="DAY",
            limit_price=round(float(kwargs["last_price"]) + 0.01, 2),
            strategy_name="Limit (Ask)",
            algo_strategy="",
        ),
    )

    plans = paper.generate_holdings_plans(
        int(portfolio["id"]),
        cached_payload={
            "rows": [
                {
                    "ticker": "NVDA",
                    "is_portfolio_holding": True,
                    "composite_signal": "Buy",
                    "current_price": 214.75,
                    "price_targets": {"entry_price": 214.75},
                    "probability": 0.91,
                    "regime": "Bull",
                }
            ]
        },
    )

    assert len(plans) == 1
    assert plans[0]["arrival_price"] == pytest.approx(215.59)
    assert plans[0]["proposed_price"] == pytest.approx(215.60)
    assert plans[0]["quantity"] == 11


def test_holdings_plans_skip_buy_when_live_price_breaks_entry_premise(temp_modules, monkeypatch) -> None:
    store, _config, _broker, _ibkr_market_data, _data, paper = temp_modules
    portfolio = store.create_paper_portfolio("Regime Agent Beta - IBKR Paper", 25_000.0, broker_type="ibkr")
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 215.59})

    plans = paper.generate_holdings_plans(
        int(portfolio["id"]),
        cached_payload={
            "rows": [
                {
                    "ticker": "NVDA",
                    "is_portfolio_holding": True,
                    "composite_signal": "Buy",
                    "current_price": 123.45,
                    "price_targets": {"entry_price": 123.46},
                    "probability": 0.91,
                    "regime": "Bull",
                }
            ]
        },
    )

    assert plans == []


def test_exit_plans_ignore_stale_cached_sell_signal(temp_modules, monkeypatch) -> None:
    store, _config, _broker, _ibkr_market_data, _data, paper = temp_modules
    portfolio = store.create_paper_portfolio("Regime Agent Beta - IBKR Paper", 25_000.0, broker_type="ibkr")
    portfolio_id = int(portfolio["id"])
    store.open_paper_position(portfolio_id, "NVDA", 5, 110.0, "2026-06-01T14:00:00+00:00", stop_price=90.0)
    stale_timestamp = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=5)).isoformat()
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 100.0})
    monkeypatch.setattr(paper, "_quick_regime_screen", lambda ticker: ("Neutral", 0.62, 99.0, 90.0))

    plans = paper.generate_exit_plans(
        portfolio_id,
        cached_regime={
            "last_run_timestamp": stale_timestamp,
            "rows": [
                {
                    "ticker": "NVDA",
                    "regime": "Bear",
                    "probability": 0.91,
                    "composite_signal": "Strong Sell",
                }
            ],
        },
    )

    assert plans == []


def test_exit_plans_allow_stop_sell_even_when_cached_signal_is_stale(temp_modules, monkeypatch) -> None:
    store, _config, _broker, _ibkr_market_data, _data, paper = temp_modules
    portfolio = store.create_paper_portfolio("Regime Agent Beta - IBKR Paper", 25_000.0, broker_type="ibkr")
    portfolio_id = int(portfolio["id"])
    store.open_paper_position(portfolio_id, "NVDA", 5, 110.0, "2026-06-01T14:00:00+00:00", stop_price=101.0)
    stale_timestamp = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=5)).isoformat()
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 100.0})

    plans = paper.generate_exit_plans(
        portfolio_id,
        cached_regime={
            "last_run_timestamp": stale_timestamp,
            "rows": [{"ticker": "NVDA", "regime": "Bull", "probability": 0.82, "composite_signal": "Buy"}],
        },
    )

    assert len(plans) == 1
    assert plans[0]["action"] == "Sell"
    assert plans[0]["signal_quality_grade"] == "actionable"
    assert plans[0]["signal_quality_score"] == pytest.approx(100.0)


def test_exit_plans_sell_at_profit_target_without_cached_signal(temp_modules, monkeypatch) -> None:
    store, _config, _broker, _ibkr_market_data, _data, paper = temp_modules
    portfolio = store.create_paper_portfolio("Regime Agent Beta - IBKR Paper", 25_000.0, broker_type="ibkr")
    portfolio_id = int(portfolio["id"])
    store.open_paper_position(
        portfolio_id,
        "NVDA",
        5,
        100.0,
        dt.datetime.now(dt.timezone.utc).isoformat(),
        stop_price=90.0,
        target_price=120.0,
    )
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 121.0})

    plans = paper.generate_exit_plans(portfolio_id)

    assert len(plans) == 1
    assert plans[0]["action"] == "Sell"
    assert plans[0]["quantity"] == pytest.approx(5)
    assert "Profit target hit" in plans[0]["rationale"]
    assert plans[0]["target_price"] == pytest.approx(120.0)
    assert plans[0]["signal_quality_score"] == pytest.approx(100.0)


def test_exit_plans_ratchet_trailing_stop_without_forcing_exit(temp_modules, monkeypatch) -> None:
    store, _config, _broker, _ibkr_market_data, _data, paper = temp_modules
    portfolio = store.create_paper_portfolio("Regime Agent Beta - IBKR Paper", 25_000.0, broker_type="ibkr")
    portfolio_id = int(portfolio["id"])
    position = store.open_paper_position(
        portfolio_id,
        "NVDA",
        5,
        100.0,
        dt.datetime.now(dt.timezone.utc).isoformat(),
        stop_price=90.0,
        target_price=200.0,
    )
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 130.0})
    monkeypatch.setattr(paper, "_lookup_atr", lambda ticker: 5.0)
    monkeypatch.setattr(paper, "_quick_regime_screen", lambda ticker: ("Neutral", 0.70, 120.0, 110.0))

    plans = paper.generate_exit_plans(portfolio_id)
    updated = store.get_paper_position(int(position["id"]))

    assert plans == []
    assert updated["stop_price"] == pytest.approx(120.0)


def test_exit_plans_create_time_stop_at_vertical_barrier(temp_modules, monkeypatch) -> None:
    store, _config, _broker, _ibkr_market_data, _data, paper = temp_modules
    portfolio = store.create_paper_portfolio("Regime Agent Beta - IBKR Paper", 25_000.0, broker_type="ibkr")
    portfolio_id = int(portfolio["id"])
    entry_date = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=22)).isoformat()
    store.open_paper_position(portfolio_id, "NVDA", 5, 100.0, entry_date, stop_price=90.0, target_price=200.0)
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 101.0})
    monkeypatch.setattr(paper, "_lookup_atr", lambda ticker: 5.0)

    plans = paper.generate_exit_plans(portfolio_id)

    assert len(plans) == 1
    assert "Time stop reached" in plans[0]["rationale"]
    assert plans[0]["timeframe_days"] == 21
    assert plans[0]["signal_quality_score"] == pytest.approx(100.0)


def test_exit_plans_neutral_deterioration_reduces_position(temp_modules, monkeypatch) -> None:
    store, _config, _broker, _ibkr_market_data, _data, paper = temp_modules
    portfolio = store.create_paper_portfolio("Regime Agent Beta - IBKR Paper", 25_000.0, broker_type="ibkr")
    portfolio_id = int(portfolio["id"])
    store.open_paper_position(
        portfolio_id,
        "NVDA",
        10,
        100.0,
        dt.datetime.now(dt.timezone.utc).isoformat(),
        stop_price=90.0,
        target_price=200.0,
    )
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 105.0})
    monkeypatch.setattr(paper, "_lookup_atr", lambda ticker: 5.0)

    plans = paper.generate_exit_plans(
        portfolio_id,
        cached_regime={
            "last_run_timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
            "rows": [
                {
                    "ticker": "NVDA",
                    "regime": "Neutral",
                    "previous_regime": "Bull",
                    "probability": 0.82,
                    "composite_signal": "Hold",
                    "p_bull_day5": 0.40,
                }
            ],
        },
    )

    assert len(plans) == 1
    assert plans[0]["quantity"] == pytest.approx(5)
    assert "Regime deteriorated from Bull to Neutral" in plans[0]["rationale"]


def test_buy_plan_geometry_is_recomputed_from_actual_fill(temp_modules, monkeypatch) -> None:
    store, _config, _broker, _ibkr_market_data, _data, paper = temp_modules
    portfolio = store.create_paper_portfolio("Regime Agent Beta - IBKR Paper", 25_000.0, broker_type="ibkr")
    theme = store.create_theme("AI Enablers", conviction=5)
    today = dt.datetime.now(dt.timezone.utc).date().isoformat()
    store.upsert_watchlist_candidate(
        int(theme["id"]),
        "NVDA",
        regime_label="Bull",
        regime_probability=0.90,
        crowd_score=20,
        status="Entry Signal",
        suggested_entry_price=119.0,
        suggested_stop_price=90.0,
    )
    store.save_signal_snapshot(
        ticker="NVDA",
        snapshot_date=today,
        action="Buy",
        regime_label="Bull",
        regime_probability=0.90,
        composite_strength=0.80,
        benchmark="SPY",
        current_price=120.0,
        entry_price=119.0,
        exit_price=150.0,
        stop_price=90.0,
        risk_reward_ratio=1.0,
        timeframe_days=21,
        expected_regime_duration=30.0,
    )
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 120.0})
    monkeypatch.setattr(paper, "_lookup_atr", lambda ticker: 5.0)
    monkeypatch.setattr(paper, "_lookup_beta", lambda ticker: 1.0)

    plans = paper.generate_buy_plans(int(portfolio["id"]))

    assert len(plans) == 1
    # Routing may improve the limit below the last price (patient buy), so derive
    # expectations from the routed price the plan actually carries: the whole point
    # of the fix is that geometry must be anchored to the actual proposed fill.
    routed_price = float(plans[0]["proposed_price"])
    assert routed_price == pytest.approx(120.0, rel=0.005)
    assert routed_price != pytest.approx(119.0)  # not the stale discovery entry
    assert plans[0]["stop_price"] == pytest.approx(routed_price - 2.0 * 5.0)
    assert plans[0]["target_price"] == pytest.approx(150.0)
    assert plans[0]["risk_reward_ratio"] == pytest.approx((150.0 - routed_price) / (2.0 * 5.0), rel=1e-3)
    assert plans[0]["trade_geometry_source"] == "actual_fill_atr"


def test_executed_buy_carries_plan_target_to_open_position(temp_modules, monkeypatch) -> None:
    store, _config, broker, _ibkr_market_data, _data, paper = temp_modules
    portfolio = store.create_paper_portfolio("Regime Agent Beta - IBKR Paper", 25_000.0, broker_type="ibkr")
    portfolio_id = int(portfolio["id"])
    plan = store.create_trade_plan(
        portfolio_id,
        "NVDA",
        "Buy",
        2,
        "Entry with managed exits",
        proposed_price=100.0,
        stop_price=95.0,
        target_price=112.0,
        risk_reward_ratio=2.4,
        timeframe_days=21,
    )
    store.update_trade_plan_status(int(plan["id"]), "Approved")
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 100.0})

    result = paper.execute_approved_plans_via_adapter(portfolio_id, broker.PaperBrokerAdapter(portfolio_id))
    positions = store.get_paper_positions(portfolio_id, status="Open")

    assert len(result["executed"]) == 1
    assert len(positions) == 1
    assert positions[0]["stop_price"] == pytest.approx(95.0)
    assert positions[0]["target_price"] == pytest.approx(112.0)


def test_regime_price_history_prefers_ibkr_when_available(temp_modules, monkeypatch) -> None:
    store, _config, _broker, ibkr_market_data, data, _paper = temp_modules
    store.set_setting(
        "market_data_provider_config",
        '{"regime_provider_order":["ibkr","yfinance"],"regime_enabled":{"ibkr":true,"yfinance":true}}',
    )

    class FakeIBKRProvider:
        def is_available(self) -> bool:
            return True

        def fetch(self, *, symbol, start, end):
            del symbol, start, end
            return pd.DataFrame(
                {
                    "open": [99.0, 100.0],
                    "high": [101.0, 102.0],
                    "low": [98.0, 99.0],
                    "close": [100.0, 101.0],
                    "volume": [1_000_000.0, 1_100_000.0],
                },
                index=pd.to_datetime(["2026-01-02", "2026-01-05"]),
            )

    monkeypatch.setattr(ibkr_market_data, "IBKRMarketDataProvider", FakeIBKRProvider)
    monkeypatch.setattr(data, "download_daily_bars", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("yfinance should not be used")))

    resolved, history = data._download_price_history("NVDA", "1mo", "1d")

    assert resolved == "NVDA"
    assert list(history.columns) == ["Open", "High", "Low", "Close", "Volume"]
    assert history["Close"].iloc[-1] == pytest.approx(101.0)


def test_agent_portfolio_dashboard_reports_shared_beta_portfolio_by_agent(temp_modules) -> None:
    store, _config, _broker, _ibkr_market_data, _data, _paper = temp_modules
    from src.regime import agent_dashboard as agent_dashboard_module

    agent_dashboard = importlib.reload(agent_dashboard_module)
    portfolio = store.create_paper_portfolio("Regime Agent Beta - Paper", 25_000.0)
    portfolio_id = int(portfolio["id"])
    store.set_setting("regime_beta_status", "active")
    store.set_setting("regime_beta_preferred_run_window", "10:05-15:30 America/New_York")
    store.create_trade_plan(
        portfolio_id,
        "NVDA",
        "Buy",
        2,
        "agent plan [agents: quant:signal=Buy | fundamental:verdict=Pass,vetoed=false | portfolio:decision=approved]",
        proposed_price=100.0,
        source="discovery",
        meta_labeler_score=0.80,
        agent_trace="quant:signal=Buy | fundamental:verdict=Pass,vetoed=false | portfolio:decision=approved",
        hurdle_passed=True,
        duration_gate_passed=True,
        anti_churn_passed=True,
        agent_key="quant",
        llm_used=True,
        llm_influenced=True,
        llm_influence="confirmed",
        llm_provider="ollama",
        llm_model="deepseek-v4-pro:cloud",
        llm_model_display="Ollama: deepseek-v4-pro:cloud",
        llm_verdict="Buy",
        llm_confidence=8,
    )
    store.create_trade_plan(
        portfolio_id,
        "TSM",
        "Buy",
        1,
        "blocked by hurdle",
        proposed_price=90.0,
        source="holdings",
        meta_labeler_score=0.40,
        hurdle_passed=False,
        anti_churn_passed=False,
        ltcg_override_active=True,
        ltcg_tax_savings=24.0,
    )
    store.log_audit_event(
        order_id="test-1",
        portfolio_id=portfolio_id,
        event_type="submitted",
        ticker="NVDA",
        action="Buy",
        quantity=2,
        price=100.0,
        actor="scheduler",
    )
    store.log_audit_event(
        order_id="test-1",
        portfolio_id=portfolio_id,
        event_type="filled",
        ticker="NVDA",
        action="Buy",
        quantity=2,
        price=100.0,
        actor="scheduler",
    )
    agents_status = [
        {"name": "quant", "enabled": True, "subscriptions": ["analysis_request"]},
        {"name": "fundamental", "enabled": True, "subscriptions": []},
        {"name": "portfolio_tax", "enabled": True, "subscriptions": []},
        {"name": "execution", "enabled": True, "subscriptions": ["trade_decision"]},
        {"name": "orchestrator", "enabled": True, "subscriptions": ["enriched_signal"]},
    ]

    dashboard = agent_dashboard.compute_agent_portfolio_dashboard(portfolio_id, agents_status=agents_status)
    by_agent = {row["name"]: row for row in dashboard["agents"]}

    assert dashboard["broker"]["type"] == "paper"
    assert dashboard["broker"]["is_internal_simulated"] is True
    assert "Internal simulated" in dashboard["portfolio_summary"]["portfolio_scope"]
    assert dashboard["target"]["target_monthly_return"] == pytest.approx(0.02)
    assert by_agent["quant"]["metrics"][0]["value"] == 2
    assert by_agent["portfolio_tax"]["attention_count"] == 2
    assert by_agent["execution"]["metrics"][1]["value"] == 1
    assert dashboard["pending_action_count"] == 2
    assert dashboard["llm_attribution"][0]["agent_key"] == "quant"
    assert dashboard["llm_attribution"][0]["model"] == "Ollama: deepseek-v4-pro:cloud"
    assert dashboard["llm_attribution"][0]["influenced"] == 1


def test_beta_agent_dashboard_aggregates_four_portfolio_target(temp_modules) -> None:
    store, _config, _broker, _ibkr_market_data, _data, _paper = temp_modules
    from src.regime import agent_dashboard as agent_dashboard_module
    from src.regime.beta_agents import BETA_AGENT_PORTFOLIOS

    agent_dashboard = importlib.reload(agent_dashboard_module)
    created_at = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=30.4375)).isoformat()
    portfolio_ids: list[int] = []
    for agent in BETA_AGENT_PORTFOLIOS:
        portfolio = store.create_paper_portfolio(str(agent["name"]), 25_000.0, broker_type="ibkr")
        portfolio_ids.append(int(portfolio["id"]))
        with store._connect() as conn:
            conn.execute("UPDATE paper_portfolio SET created_at = ? WHERE id = ?", (created_at, int(portfolio["id"])))
    store.set_setting("regime_beta_portfolio_ids", ",".join(str(item) for item in portfolio_ids))

    dashboard = agent_dashboard.compute_beta_agent_dashboard(agents_status=[])

    assert dashboard["agent_portfolio_count"] == 4
    assert dashboard["portfolio_summary"]["starting_budget"] == pytest.approx(100_000.0)
    assert dashboard["target"]["starting_budget"] == pytest.approx(100_000.0)
    assert dashboard["target"]["target_return"] == pytest.approx(0.02, rel=1e-3)
    assert dashboard["target"]["target_equity"] == pytest.approx(102_000.0, rel=1e-3)


def test_beta_agent_dashboard_reports_current_open_plan_readiness(temp_modules) -> None:
    store, _config, _broker, _ibkr_market_data, _data, _paper = temp_modules
    from src.regime import agent_dashboard as agent_dashboard_module
    from src.regime.beta_agents import BETA_AGENT_PORTFOLIOS

    agent_dashboard = importlib.reload(agent_dashboard_module)
    portfolio_ids: list[int] = []
    for agent in BETA_AGENT_PORTFOLIOS:
        portfolio = store.create_paper_portfolio(str(agent["name"]), 25_000.0, broker_type="ibkr")
        portfolio_ids.append(int(portfolio["id"]))
    store.set_setting("regime_beta_portfolio_ids", ",".join(str(item) for item in portfolio_ids))
    store.open_paper_position(portfolio_ids[0], "NVDA", 1, 100.0, "2026-06-08T14:00:00+00:00")
    store.create_trade_plan(portfolio_ids[0], "NVDA", "Sell", 1, "exit", proposed_price=101.0)

    dashboard = agent_dashboard.compute_beta_agent_dashboard(agents_status=[])
    readiness = dashboard["open_plan_readiness"]

    assert readiness["counts"]["ready"] == 1
    assert readiness["counts"]["total"] == 1
    assert readiness["rows"][0]["ticker"] == "NVDA"
    assert readiness["rows"][0]["ready"] is True


def test_agent_monitor_funnel_counts_candidates_plans_and_executions(temp_modules, monkeypatch) -> None:
    store, _config, _broker, _ibkr_market_data, _data, _paper = temp_modules
    from src.regime import agent_candidate_intake as intake_module
    from src.regime import agent_dashboard as agent_dashboard_module

    agent_dashboard = importlib.reload(agent_dashboard_module)
    portfolio = store.create_paper_portfolio("Regime Agent Beta - Agent 1 Quant", 25_000.0, broker_type="ibkr")
    portfolio_id = int(portfolio["id"])
    store.set_setting("regime_beta_portfolio_ids", str(portfolio_id))
    store.create_trade_plan(portfolio_id, "NVDA", "Buy", 3, "entry candidate", proposed_price=100.0)
    store.log_audit_event(
        order_id="fill-1",
        portfolio_id=portfolio_id,
        event_type="filled",
        ticker="NVDA",
        action="Buy",
        quantity=3,
        price=100.0,
        actor="scheduler",
        details="paper fill",
    )

    monkeypatch.setattr(
        intake_module,
        "compute_agent_candidate_intake",
        lambda portfolio_id, limit=500: {
            "total_candidates": 3,
            "counts": {"would_create_buy_plan": 1, "blocked_signal_quality": 2},
            "candidates": [
                {"ticker": "NVDA", "decision": "would_create_buy_plan", "reason": "passes"},
                {"ticker": "ARW", "decision": "blocked_signal_quality", "reason": "Signal is stale."},
                {"ticker": "SLAB", "decision": "blocked_signal_quality", "reason": "Current price is above the entry premise."},
            ],
        },
    )

    funnel = agent_dashboard.compute_agent_monitor_funnel([portfolio_id], date="today")
    stages = {row["key"]: row["count"] for row in funnel["stages"]}

    assert stages["candidates"] == 3
    assert stages["entry_gates"] == 1
    assert stages["plans_created"] == 1
    assert stages["executed"] == 1
    assert funnel["blockers"][0]["reason"] in {"signal stale", "price away from entry"}


def test_agent_monitor_feed_composes_server_side_sentences(temp_modules) -> None:
    store, _config, _broker, _ibkr_market_data, _data, _paper = temp_modules
    from src.regime import agent_dashboard as agent_dashboard_module

    agent_dashboard = importlib.reload(agent_dashboard_module)
    portfolio = store.create_paper_portfolio("Regime Agent Beta - Agent 1 Quant", 25_000.0, broker_type="ibkr")
    portfolio_id = int(portfolio["id"])
    store.set_setting("regime_beta_portfolio_ids", str(portfolio_id))
    store.create_trade_plan(portfolio_id, "AVGO", "Buy", 2, "hurdle +1.9% net", proposed_price=187.0)
    store.log_audit_event(
        order_id="fill-2",
        portfolio_id=portfolio_id,
        event_type="filled",
        ticker="AVGO",
        action="Buy",
        quantity=2,
        price=187.0,
        actor="scheduler",
        details="ML 0.58, hurdle +1.9% net",
    )

    feed = agent_dashboard.compute_agent_monitor_feed([portfolio_id], limit=10)
    texts = [row["text"] for row in feed["items"]]

    assert any("Quant bought 2 AVGO @ $187.00 - ML 0.58" in text for text in texts)
    assert any("Quant planned buy 2 AVGO @ $187.00 - hurdle +1.9% net" in text for text in texts)


def test_beta_agent_dashboard_ranks_profit_and_flags_overlap(temp_modules, monkeypatch) -> None:
    store, _config, _broker, _ibkr_market_data, _data, paper = temp_modules
    from src.regime import agent_competition as agent_competition_module
    from src.regime import agent_dashboard as agent_dashboard_module
    from src.regime.beta_agents import BETA_AGENT_PORTFOLIOS

    importlib.reload(agent_competition_module)
    agent_dashboard = importlib.reload(agent_dashboard_module)
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 110.0})
    portfolio_ids: list[int] = []
    cash_values = [24_960.0, 24_910.0, 25_000.0, 25_000.0]
    for agent, cash in zip(BETA_AGENT_PORTFOLIOS, cash_values, strict=True):
        portfolio = store.create_paper_portfolio(str(agent["name"]), 25_000.0, broker_type="ibkr")
        portfolio_ids.append(int(portfolio["id"]))
        store.update_paper_portfolio(int(portfolio["id"]), current_cash=cash)
    store.set_setting("regime_beta_portfolio_ids", ",".join(str(item) for item in portfolio_ids))
    store.set_setting("agent_competition_enabled", "true")
    store.set_setting("agent_diversification_enabled", "true")
    store.set_setting("agent_max_active_portfolios_per_ticker", "1")
    store.open_paper_position(portfolio_ids[0], "NVDA", 1, 100.0, "2026-06-03T14:00:00+00:00")
    store.open_paper_position(portfolio_ids[1], "NVDA", 1, 100.0, "2026-06-03T14:01:00+00:00")
    avgo_1 = store.create_trade_plan(portfolio_ids[2], "AVGO", "Buy", 1, "submitted overlap", proposed_price=486.0)
    avgo_2 = store.create_trade_plan(portfolio_ids[3], "AVGO", "Buy", 1, "approved overlap", proposed_price=486.0)
    store.update_trade_plan_status(int(avgo_1["id"]), "Submitted", broker_order_id="avgo-1", broker_status="Submitted")
    store.update_trade_plan_status(int(avgo_2["id"]), "Approved")

    dashboard = agent_dashboard.compute_beta_agent_dashboard(agents_status=[])
    competition = dashboard["competition"]
    overlap_by_ticker = {row["ticker"]: row for row in competition["overlap"]["risk_tickers"]}

    assert competition["winner"]["portfolio_id"] == portfolio_ids[0]
    assert competition["winner"]["profit"] == pytest.approx(70.0)
    assert competition["winner"]["after_tax_profit"] == pytest.approx(66.8)
    assert competition["basis"] == "estimated_after_tax_profit"
    assert [row["rank"] for row in competition["leaderboard"]] == [1, 2, 3, 4]
    assert dashboard["agents"][0]["rank"] == 1
    assert competition["overlap"]["risk_count"] == 2
    assert overlap_by_ticker["NVDA"]["active_portfolio_count"] == 2
    assert overlap_by_ticker["AVGO"]["active_portfolio_count"] == 2


def test_auto_approval_blocks_cross_agent_ticker_overlap(temp_modules) -> None:
    store, _config, _broker, _ibkr_market_data, _data, paper = temp_modules
    first = store.create_paper_portfolio("Regime Agent Beta - Agent 1 Quant", 25_000.0, broker_type="ibkr")
    second = store.create_paper_portfolio("Regime Agent Beta - Agent 2 Fundamental", 25_000.0, broker_type="ibkr")
    first_id = int(first["id"])
    second_id = int(second["id"])
    store.set_operating_mode("autonomous")
    store.set_setting("autonomous_portfolio_ids", f"{first_id},{second_id}")
    store.set_setting("regime_beta_portfolio_ids", f"{first_id},{second_id}")
    store.set_setting("agent_diversification_enabled", "true")
    store.set_setting("agent_diversification_enforce_orders", "true")
    store.set_setting("agent_max_active_portfolios_per_ticker", "1")
    store.set_setting("agent_mandate_diversification_enabled", "false")
    store.open_paper_position(first_id, "NVDA", 1, 100.0, "2026-06-03T14:00:00+00:00")
    plan = store.create_trade_plan(
        second_id,
        "NVDA",
        "Buy",
        1,
        "duplicate beta-agent ticker",
        proposed_price=101.0,
        meta_labeler_score=0.95,
    )

    result = paper.auto_approve_plans(second_id)
    updated_plan = store.get_trade_plan(int(plan["id"]))
    audit = store.get_audit_trail(portfolio_id=second_id, ticker="NVDA", event_type="guardrail_blocked", days=1, limit=5)

    assert result["approved"] == 0
    assert result["blocked"] == 1
    assert result["details"][0]["result"] == "blocked_cross_agent_overlap"
    assert result["details"][0]["owners"][0]["portfolio_id"] == first_id
    assert updated_plan["status"] == "Pending"
    assert audit
    assert "Cross-agent diversification blocked NVDA" in audit[0]["details"]


def test_auto_approval_monitors_overlap_without_default_order_block(temp_modules) -> None:
    store, _config, _broker, _ibkr_market_data, _data, paper = temp_modules
    first = store.create_paper_portfolio("Regime Agent Beta - Agent 1 Quant", 25_000.0, broker_type="ibkr")
    second = store.create_paper_portfolio("Regime Agent Beta - Agent 2 Fundamental", 25_000.0, broker_type="ibkr")
    first_id = int(first["id"])
    second_id = int(second["id"])
    store.set_operating_mode("autonomous")
    store.set_setting("autonomous_portfolio_ids", f"{first_id},{second_id}")
    store.set_setting("regime_beta_portfolio_ids", f"{first_id},{second_id}")
    store.set_setting("agent_diversification_enabled", "true")
    store.set_setting("agent_max_active_portfolios_per_ticker", "1")
    store.set_setting("earnings_blackout_enabled", "false")
    store.open_paper_position(first_id, "NVDA", 1, 100.0, "2026-06-03T14:00:00+00:00")
    plan = store.create_trade_plan(
        second_id,
        "NVDA",
        "Buy",
        1,
        "duplicate beta-agent ticker monitored only",
        proposed_price=101.0,
        meta_labeler_score=0.95,
    )

    result = paper.auto_approve_plans(second_id)
    updated_plan = store.get_trade_plan(int(plan["id"]))

    assert result["approved"] == 1
    assert result["blocked"] == 0
    assert result["details"][0]["result"] == "approved"
    assert updated_plan["status"] == "Approved"


def test_execution_blocks_already_approved_cross_agent_overlap(temp_modules) -> None:
    store, _config, broker, _ibkr_market_data, _data, paper = temp_modules
    first = store.create_paper_portfolio("Regime Agent Beta - Agent 1 Quant", 25_000.0, broker_type="ibkr")
    second = store.create_paper_portfolio("Regime Agent Beta - Agent 2 Fundamental", 25_000.0, broker_type="ibkr")
    first_id = int(first["id"])
    second_id = int(second["id"])
    store.set_setting("regime_beta_portfolio_ids", f"{first_id},{second_id}")
    store.set_setting("agent_diversification_enabled", "true")
    store.set_setting("agent_diversification_enforce_orders", "true")
    store.set_setting("agent_max_active_portfolios_per_ticker", "1")
    store.set_setting("agent_mandate_diversification_enabled", "false")
    store.set_setting("earnings_blackout_enabled", "false")
    store.open_paper_position(first_id, "AVGO", 5, 486.0, "2026-06-03T14:00:00+00:00")
    plan = store.create_trade_plan(
        second_id,
        "AVGO",
        "Buy",
        5,
        "already approved duplicate beta-agent ticker",
        proposed_price=486.0,
        meta_labeler_score=0.95,
    )
    store.update_trade_plan_status(int(plan["id"]), "Approved")
    adapter = broker.MockBrokerAdapter(fill_price=486.0)

    result = paper.execute_approved_plans_via_adapter(second_id, adapter, actor="scheduler")
    updated_plan = store.get_trade_plan(int(plan["id"]))

    assert result["executed"] == []
    assert result["skipped"][0]["status"] == "blocked_cross_agent_overlap"
    assert adapter.submitted_orders == []
    assert updated_plan["status"] == "Rejected"


def test_auto_approval_blocks_earnings_blackout(temp_modules, monkeypatch) -> None:
    store, _config, _broker, _ibkr_market_data, _data, paper = temp_modules
    portfolio = store.create_paper_portfolio("Regime Agent Beta - Agent 1 Quant", 25_000.0, broker_type="ibkr")
    portfolio_id = int(portfolio["id"])
    store.set_operating_mode("autonomous")
    store.set_setting("autonomous_portfolio_ids", str(portfolio_id))
    store.set_setting("earnings_blackout_enabled", "true")
    store.set_setting("earnings_blackout_days", "2")
    store.create_trade_plan(
        portfolio_id,
        "MSFT",
        "Buy",
        1,
        "earnings blackout test",
        proposed_price=100.0,
        meta_labeler_score=0.95,
    )
    upcoming = dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=1)
    monkeypatch.setattr("src.regime.data.get_next_earnings_date", lambda ticker: upcoming)

    result = paper.auto_approve_plans(portfolio_id)

    assert result["approved"] == 0
    assert result["blocked"] == 1
    assert result["details"][0]["result"] == "blocked_earnings_blackout"


def test_auto_approval_blocks_portfolio_drawdown_pause(temp_modules) -> None:
    store, _config, _broker, _ibkr_market_data, _data, paper = temp_modules
    portfolio = store.create_paper_portfolio("Regime Agent Beta - Agent 1 Quant", 25_000.0, broker_type="ibkr")
    portfolio_id = int(portfolio["id"])
    store.update_paper_portfolio(portfolio_id, current_cash=23_500.0)
    store.set_operating_mode("autonomous")
    store.set_setting("autonomous_portfolio_ids", str(portfolio_id))
    store.set_setting("agent_drawdown_pause_enabled", "true")
    store.set_setting("agent_max_drawdown_pause_pct", "0.05")
    store.set_setting("earnings_blackout_enabled", "false")
    store.create_trade_plan(
        portfolio_id,
        "MSFT",
        "Buy",
        1,
        "drawdown pause test",
        proposed_price=100.0,
        meta_labeler_score=0.95,
    )

    result = paper.auto_approve_plans(portfolio_id)

    assert result["approved"] == 0
    assert result["blocked"] == 1
    assert result["details"][0]["result"] == "blocked_buy_pause"
    assert result["details"][0]["reasons"][0]["code"] == "portfolio_drawdown_pause"


def test_policy_cancels_duplicate_submitted_buy_orders(temp_modules, monkeypatch) -> None:
    store, _config, broker, _ibkr_market_data, _data, paper = temp_modules
    first = store.create_paper_portfolio("Regime Agent Beta - Agent 1 Quant", 25_000.0, broker_type="ibkr")
    second = store.create_paper_portfolio("Regime Agent Beta - Agent 2 Fundamental", 25_000.0, broker_type="ibkr")
    first_id = int(first["id"])
    second_id = int(second["id"])
    store.set_setting("regime_beta_portfolio_ids", f"{first_id},{second_id}")
    store.set_setting("agent_submitted_order_cancel_enabled", "true")
    store.set_setting("agent_diversification_enforce_orders", "true")
    keep = store.create_trade_plan(first_id, "AVGO", "Buy", 5, "better limit", proposed_price=485.00)
    cancel = store.create_trade_plan(second_id, "AVGO", "Buy", 5, "worse limit", proposed_price=486.00)
    store.update_trade_plan_status(int(keep["id"]), "Submitted", broker_order_id="101", broker_status="Submitted")
    store.update_trade_plan_status(int(cancel["id"]), "Submitted", broker_order_id="102", broker_status="Submitted")
    adapter = broker.MockBrokerAdapter(fill_price=486.0)
    adapter._results["102"] = broker.OrderResult(
        order_id="102",
        status="submitted",
        ticker="AVGO",
        action="Buy",
        quantity=5,
    )
    monkeypatch.setattr(adapter, "get_current_price", lambda ticker, action="": 486.0, raising=False)

    result = paper.cancel_submitted_orders_by_policy(second_id, adapter)

    assert result["cancelled"][0]["plan_id"] == int(cancel["id"])
    assert "cross_agent_overlap" in result["cancelled"][0]["reasons"]
    assert store.get_trade_plan(int(cancel["id"]))["status"] == "Cancelled"
    assert store.get_trade_plan(int(keep["id"]))["status"] == "Submitted"


def test_paper_performance_estimates_after_tax_open_gain(temp_modules, monkeypatch) -> None:
    store, _config, _broker, _ibkr_market_data, _data, paper = temp_modules
    portfolio = store.create_paper_portfolio("Regime Agent Beta - Agent 1 Quant", 25_000.0, broker_type="ibkr")
    portfolio_id = int(portfolio["id"])
    store.update_paper_portfolio(portfolio_id, current_cash=24_900.0)
    store.open_paper_position(portfolio_id, "NVDA", 1, 100.0, "2026-06-03T14:00:00+00:00")
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 120.0})
    monkeypatch.setattr(
        paper,
        "download_daily_bars",
        lambda *args, **kwargs: pd.DataFrame({"Close": [100.0, 101.0]}),
    )

    performance = paper.compute_paper_performance(portfolio_id)

    assert performance["total_equity"] == pytest.approx(25_020.0)
    assert performance["estimated_unrealized_tax"] == pytest.approx(6.4)
    assert performance["after_tax_equity"] == pytest.approx(25_013.6)
    assert performance["after_tax_profit"] == pytest.approx(13.6)


def test_size_only_veto_mode_scales_buy_plan_quantity(temp_modules, monkeypatch) -> None:
    """size_only must scale entry quantity by 0.5 + 0.5*ML probability; gate mode must not."""
    store, _config, _broker, _ibkr_market_data, _data, paper = temp_modules
    theme = store.create_theme("AI Enablers", conviction=5)
    today = dt.datetime.now(dt.timezone.utc).date().isoformat()
    store.save_signal_snapshot(
        ticker="NVDA",
        snapshot_date=today,
        action="Buy",
        regime_label="Bull",
        regime_probability=0.90,
        composite_strength=0.80,
        benchmark="SPY",
        current_price=120.0,
        entry_price=119.0,
        exit_price=150.0,
        stop_price=90.0,
        risk_reward_ratio=1.0,
        timeframe_days=21,
        expected_regime_duration=30.0,
    )

    def _watchlist(status=None):
        del status
        return [
            {
                "id": 1,
                "ticker": "NVDA",
                "theme_id": int(theme["id"]),
                "suggested_role": "Critical-Path",
                "suggested_entry_price": 119.0,
                "suggested_exit_price": 150.0,
                "suggested_stop_price": 90.0,
                "regime_label": "Bull",
                "regime_probability": 0.90,
                "crowd_score": 20,
                "status": "Entry Signal",
                "meta_labeler_probability": 0.5,
                "discovery_rationale": "test candidate",
            }
        ]

    monkeypatch.setattr(paper, "get_watchlist", _watchlist)
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 120.0})
    monkeypatch.setattr(paper, "_lookup_atr", lambda ticker: 5.0)
    monkeypatch.setattr(paper, "_lookup_beta", lambda ticker: 1.0)

    portfolio_gate = store.create_paper_portfolio("Gate Mode", 25_000.0, broker_type="ibkr")
    gate_plans = paper.generate_buy_plans(int(portfolio_gate["id"]))
    assert len(gate_plans) == 1
    gate_quantity = float(gate_plans[0]["quantity"])
    assert gate_quantity > 0
    assert "ML size scaling" not in str(gate_plans[0]["rationale"])

    store.set_setting("meta_labeler_veto_mode", "size_only")
    portfolio_size = store.create_paper_portfolio("Size Only Mode", 25_000.0, broker_type="ibkr")
    size_plans = paper.generate_buy_plans(int(portfolio_size["id"]))
    assert len(size_plans) == 1
    size_quantity = float(size_plans[0]["quantity"])
    # probability 0.5 -> multiplier 0.75
    assert size_quantity == float(math.floor(gate_quantity * 0.75))
    assert "ML size scaling" in str(size_plans[0]["rationale"])
