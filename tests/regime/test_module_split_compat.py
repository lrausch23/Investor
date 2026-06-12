from __future__ import annotations

import datetime as dt
import importlib
from pathlib import Path

import pytest


PERSISTENCE_PUBLIC_NAMES = [
    "ALERT_TYPES",
    "Any",
    "DB_PATH",
    "DEFAULT_AUTO_APPROVE_THRESHOLD",
    "DEFAULT_DAILY_CAPITAL_CEILING_PCT",
    "DEFAULT_LOT_SELECTION_METHOD",
    "DEFAULT_LTCG_DEFER_WINDOW_DAYS",
    "DEFAULT_OPERATING_MODE",
    "DataValidationError",
    "DuplicateThemeError",
    "LOT_SELECTION_METHODS",
    "NOTIFICATION_CHANNELS",
    "OPERATING_MODES",
    "Path",
    "PersistenceError",
    "_PersistenceModule",
    "_core",
    "_loaded_core",
    "acknowledge_alert",
    "acknowledge_all_alerts",
    "add_ticker_to_theme",
    "add_wash_sale_restriction",
    "annotations",
    "asdict",
    "close_paper_position",
    "close_tax_lot",
    "core",
    "count_executed_sell_plans",
    "count_todays_trades",
    "create_paper_portfolio",
    "create_tax_lot",
    "create_theme",
    "create_trade_plan",
    "datetime",
    "delete_paper_portfolio",
    "delete_setting",
    "delete_supply_chain",
    "delete_theme",
    "delete_thesis",
    "delete_watchlist_entry",
    "get_alerts",
    "get_all_settings",
    "get_audit_trail",
    "get_auto_approve_threshold",
    "get_cached_earnings_date",
    "get_cached_sector",
    "get_calibration_data",
    "get_channels_for_alert",
    "get_daily_audit_summary",
    "get_daily_capital_ceiling_pct",
    "get_daily_capital_deployed",
    "get_daily_snapshots",
    "get_execution_quality_history",
    "get_execution_quality_snapshot",
    "get_historical_regime_durations",
    "get_latest_regime_label",
    "get_latest_signal_snapshot",
    "get_latest_thesis_monitor_run",
    "get_llm_attribution_summary",
    "get_lot_selection_method",
    "get_ltcg_defer_window_days",
    "get_notification_preferences",
    "get_oldest_executed_sell_at",
    "get_operating_mode",
    "get_paper_portfolio",
    "get_paper_portfolio_summary",
    "get_paper_position",
    "get_paper_positions",
    "get_pending_outcomes",
    "get_pending_transition_outcomes",
    "get_performance_timeseries",
    "get_recent_regime_changes",
    "get_sentiment_history",
    "get_setting",
    "get_signal_effectiveness",
    "get_stress_test_result_by_id",
    "get_stress_test_results",
    "get_supply_chain",
    "get_tax_lot",
    "get_tax_lots",
    "get_theme",
    "get_theme_health_data",
    "get_theme_tickers",
    "get_thesis_monitor_runs",
    "get_ticker_themes",
    "get_trade_plan",
    "get_trade_plans",
    "get_training_history",
    "get_training_run",
    "get_transition_journal",
    "get_transition_statistics",
    "get_wash_sale_restriction",
    "get_wash_sale_restrictions",
    "get_watchlist",
    "get_watchlist_by_ticker",
    "get_watchlist_entry",
    "get_watchlist_stats",
    "importlib",
    "is_dataclass",
    "is_live_trading_unlocked",
    "is_wash_sale_restricted",
    "json",
    "list_paper_portfolios",
    "list_themes",
    "list_theses",
    "log_audit_event",
    "log_barrier_override",
    "log_training_run",
    "logger",
    "logging",
    "mark_stress_test_status",
    "open_paper_position",
    "os",
    "remove_ticker_from_theme",
    "save_alert",
    "save_daily_snapshot",
    "save_earnings_cache",
    "save_execution_quality_snapshot",
    "save_regime_change_with_price",
    "save_regime_event",
    "save_sector_cache",
    "save_sentiment",
    "save_signal_snapshot",
    "save_stress_test_result",
    "save_supply_chain_layers",
    "save_thesis_monitor_run",
    "set_auto_approve_threshold",
    "set_daily_capital_ceiling_pct",
    "set_live_trading_unlocked",
    "set_lot_selection_method",
    "set_ltcg_defer_window_days",
    "set_notification_preference",
    "set_operating_mode",
    "set_setting",
    "setup_regime_logging",
    "sqlite3",
    "sys",
    "timedelta",
    "timezone",
    "types",
    "update_paper_portfolio",
    "update_paper_position_quantity",
    "update_paper_position_risk",
    "update_signal_outcome",
    "update_theme",
    "update_ticker_in_theme",
    "update_trade_plan_benchmarks",
    "update_trade_plan_status",
    "update_training_status",
    "update_transition_outcome",
    "update_watchlist_cross_sectional",
    "update_watchlist_fundamental_gate",
    "update_watchlist_status",
    "upsert_thesis",
    "upsert_watchlist_candidate",
]


PAPER_TRADING_PUBLIC_NAMES = [
    "ACTIONABLE_SIGNAL_SCORE",
    "Any",
    "BrokerAdapter",
    "CachedRegimeMap",
    "CachedRegimeValue",
    "DEFAULT_BETA_TARGET_BENCHMARKS",
    "DEFAULT_BETA_TARGET_MONTHLY_RETURN",
    "DEFAULT_BETA_TARGET_ROLLING_MONTHS",
    "DEFAULT_EXIT_TIME_STOP_DAYS",
    "DEFAULT_NEUTRAL_REDUCE_FRACTION",
    "DEFAULT_PAPER_TRADING_CONFIG",
    "DEFAULT_RISK_GUARDRAILS",
    "DEFAULT_SIZING_ATR_MULTIPLIER",
    "DEFAULT_SIZING_BASE_RISK_FRACTION",
    "DEFAULT_SIZING_METHOD",
    "ET",
    "LONG_TERM_HOLDING_DAYS",
    "OrderRequest",
    "PaperBrokerAdapter",
    "PaperTradingConfig",
    "RiskGuardrails",
    "SignalQuality",
    "TRAILING_STOP_ACTIVATION_ATR",
    "_PaperTradingModule",
    "_actual_fill_trade_geometry",
    "_apply_filled_execution",
    "_batch_current_prices",
    "_core",
    "_loaded_core",
    "_lookup_atr",
    "_lookup_beta",
    "_neutral_reduce_reason",
    "_open_position_index",
    "_pending_plan_index",
    "_reduced_exit_quantity",
    "_risk_adjusted_quantity",
    "active_ticker_owners",
    "agent_candidate_policy",
    "allocate_budget",
    "annotations",
    "asdict",
    "auto_approve_plans",
    "auto_execute_approved",
    "buy_pause_status",
    "cancel_submitted_orders_by_policy",
    "check_duration_gate",
    "check_hurdle_rate",
    "close_paper_position",
    "compute_benchmark_comparison",
    "compute_benchmark_set",
    "compute_beta_target_progress",
    "compute_daily_snapshot",
    "compute_paper_performance",
    "compute_position_budget",
    "compute_theme_budget",
    "configured_beta_portfolio_ids",
    "core",
    "count_todays_trades",
    "create_trade_plan",
    "decide_routing",
    "diversification_settings",
    "download_daily_bars",
    "dt",
    "earnings_blackout_status",
    "estimate_after_tax_performance",
    "evaluate_signal_quality",
    "execute_approved_plans",
    "execute_approved_plans_via_adapter",
    "expire_stale_plans",
    "fetch_financial_statements",
    "generate_buy_plans",
    "generate_daily_plans",
    "generate_exit_plans",
    "generate_holdings_plans",
    "get_audit_trail",
    "get_auto_approve_threshold",
    "get_beta_target_settings",
    "get_daily_capital_ceiling_pct",
    "get_daily_capital_deployed",
    "get_daily_snapshots",
    "get_hurdle_settings",
    "get_latest_signal_snapshot",
    "get_ltcg_override_settings",
    "get_operating_mode",
    "get_paper_dashboard",
    "get_paper_portfolio",
    "get_paper_portfolio_summary",
    "get_paper_positions",
    "get_performance_timeseries",
    "get_setting",
    "get_sizing_settings",
    "get_ticker_info",
    "get_trade_plans",
    "get_watchlist",
    "get_watchlist_by_ticker",
    "importlib",
    "is_portfolio_autonomy_enabled",
    "json",
    "kill_switch",
    "list_paper_portfolios",
    "list_themes",
    "log_audit_event",
    "log_barrier_override",
    "logger",
    "logging",
    "math",
    "near_close_cancel_active",
    "open_paper_position",
    "pd",
    "policy_setting_bool",
    "policy_setting_float",
    "policy_setting_int",
    "record_trade_outcome",
    "save_alert",
    "save_daily_snapshot",
    "set_auto_approve_threshold",
    "set_daily_capital_ceiling_pct",
    "set_operating_mode",
    "submit_guarded_order",
    "sys",
    "trailing_stop_level",
    "types",
    "update_paper_portfolio",
    "update_paper_position_quantity",
    "update_paper_position_risk",
    "update_trade_plan_status",
    "uuid",
    "validate_guardrails",
]


@pytest.fixture()
def temp_modules(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    import src.regime.paper_trading as paper_trading
    import src.regime.persistence as persistence

    store = importlib.reload(persistence)
    store.DB_PATH = tmp_path / "regime_watch.db"
    paper = importlib.reload(paper_trading)
    return store, paper


def test_persistence_package_preserves_pinned_public_names(temp_modules) -> None:
    store, _paper = temp_modules

    assert sorted(name for name in dir(store) if not name.startswith("__")) == PERSISTENCE_PUBLIC_NAMES
    for name in PERSISTENCE_PUBLIC_NAMES:
        assert hasattr(store, name), name


def test_paper_trading_package_preserves_pinned_public_names(temp_modules) -> None:
    _store, paper = temp_modules

    assert sorted(name for name in dir(paper) if not name.startswith("__")) == PAPER_TRADING_PUBLIC_NAMES
    for name in PAPER_TRADING_PUBLIC_NAMES:
        assert hasattr(paper, name), name


def test_cross_module_function_identities_are_preserved(temp_modules) -> None:
    _store, paper = temp_modules
    import src.regime.pipeline_backtest as pipeline_backtest
    import src.regime.triple_barrier as triple_barrier

    assert pipeline_backtest.trailing_stop_level is paper.trailing_stop_level
    assert triple_barrier.trailing_stop_level is paper.trailing_stop_level
    assert pipeline_backtest._actual_fill_trade_geometry is paper._actual_fill_trade_geometry


def test_persistence_db_path_patch_reaches_domain_module(temp_modules, tmp_path: Path) -> None:
    store, _paper = temp_modules
    import src.regime.persistence.settings as settings

    store.DB_PATH = tmp_path / "settings_domain.db"
    settings.set_setting("split.compat", "ok")

    assert store.core.DB_PATH == tmp_path / "settings_domain.db"
    assert settings.get_setting("split.compat") == "ok"
    assert (tmp_path / "settings_domain.db").exists()


def test_paper_trading_package_patches_reach_planning_domain(
    temp_modules,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, paper = temp_modules
    import src.regime.paper_trading.planning as planning

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
    calls: list[str] = []

    def patched_watchlist(status=None):
        calls.append(f"watchlist:{status}")
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
                "discovery_rationale": "compat candidate",
            }
        ]

    def patched_prices(tickers):
        calls.append(f"prices:{','.join(tickers)}")
        return {"NVDA": 120.0}

    def patched_atr(ticker):
        calls.append(f"atr:{ticker}")
        return 5.0

    def patched_beta(ticker):
        calls.append(f"beta:{ticker}")
        return 1.0

    monkeypatch.setattr(paper, "get_watchlist", patched_watchlist)
    monkeypatch.setattr(paper, "_batch_current_prices", patched_prices)
    monkeypatch.setattr(paper, "_lookup_atr", patched_atr)
    monkeypatch.setattr(paper, "_lookup_beta", patched_beta)
    portfolio = store.create_paper_portfolio("Compat", 25_000.0, broker_type="ibkr")

    plans = planning.generate_buy_plans(int(portfolio["id"]))

    assert len(plans) == 1
    assert plans[0]["ticker"] == "NVDA"
    assert any(call.startswith("watchlist:") for call in calls)
    assert "prices:NVDA" in calls
    assert "atr:NVDA" in calls
    assert "beta:NVDA" in calls
