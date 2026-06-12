from __future__ import annotations

import importlib
import json
import logging
import os
import sqlite3
import sys
import types
from dataclasses import asdict, is_dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from ..exceptions import DataValidationError, DuplicateThemeError, PersistenceError
from ..logging_config import setup_regime_logging
from . import core as _loaded_core

_core = importlib.reload(_loaded_core)
from . import audit as _audit
from . import plans as _plans
from . import portfolios as _portfolios
from . import positions as _positions
from . import schema as _schema
from . import settings as _settings
from . import signals_cache as _signals_cache
from . import snapshots as _snapshots

_DOMAIN_MODULES = [
    _schema,
    _settings,
    _audit,
    _snapshots,
    _portfolios,
    _positions,
    _plans,
    _signals_cache,
]

DB_PATH = _core.DB_PATH
DEFAULT_OPERATING_MODE = _core.DEFAULT_OPERATING_MODE
DEFAULT_AUTO_APPROVE_THRESHOLD = _core.DEFAULT_AUTO_APPROVE_THRESHOLD
DEFAULT_DAILY_CAPITAL_CEILING_PCT = _core.DEFAULT_DAILY_CAPITAL_CEILING_PCT
LOT_SELECTION_METHODS = _core.LOT_SELECTION_METHODS
DEFAULT_LOT_SELECTION_METHOD = _core.DEFAULT_LOT_SELECTION_METHOD
DEFAULT_LTCG_DEFER_WINDOW_DAYS = _core.DEFAULT_LTCG_DEFER_WINDOW_DAYS
OPERATING_MODES = _core.OPERATING_MODES
ALERT_TYPES = _core.ALERT_TYPES
NOTIFICATION_CHANNELS = _core.NOTIFICATION_CHANNELS
logger = _core.logger
core = _core

from .settings import (
    delete_setting,
    get_all_settings,
    get_auto_approve_threshold,
    get_daily_capital_ceiling_pct,
    get_lot_selection_method,
    get_ltcg_defer_window_days,
    get_operating_mode,
    get_setting,
    is_live_trading_unlocked,
    set_auto_approve_threshold,
    set_daily_capital_ceiling_pct,
    set_live_trading_unlocked,
    set_lot_selection_method,
    set_ltcg_defer_window_days,
    set_operating_mode,
    set_setting,
)
from .audit import (
    acknowledge_alert,
    acknowledge_all_alerts,
    get_alerts,
    get_audit_trail,
    get_channels_for_alert,
    get_daily_audit_summary,
    get_daily_capital_deployed,
    get_latest_thesis_monitor_run,
    get_llm_attribution_summary,
    get_notification_preferences,
    get_stress_test_result_by_id,
    get_stress_test_results,
    get_thesis_monitor_runs,
    get_training_history,
    get_training_run,
    log_audit_event,
    log_barrier_override,
    log_training_run,
    mark_stress_test_status,
    save_alert,
    save_stress_test_result,
    save_thesis_monitor_run,
    set_notification_preference,
    count_todays_trades,
    update_training_status,
)
from .signals_cache import (
    add_ticker_to_theme,
    create_theme,
    delete_supply_chain,
    delete_theme,
    delete_thesis,
    delete_watchlist_entry,
    get_cached_earnings_date,
    get_cached_sector,
    get_historical_regime_durations,
    get_latest_regime_label,
    get_pending_transition_outcomes,
    get_recent_regime_changes,
    get_sentiment_history,
    get_supply_chain,
    get_theme,
    get_theme_health_data,
    get_theme_tickers,
    get_ticker_themes,
    get_transition_journal,
    get_transition_statistics,
    get_watchlist,
    get_watchlist_by_ticker,
    get_watchlist_entry,
    get_watchlist_stats,
    list_themes,
    list_theses,
    remove_ticker_from_theme,
    save_earnings_cache,
    save_regime_change_with_price,
    save_regime_event,
    save_sector_cache,
    save_sentiment,
    save_supply_chain_layers,
    update_theme,
    update_ticker_in_theme,
    update_transition_outcome,
    update_watchlist_cross_sectional,
    update_watchlist_fundamental_gate,
    update_watchlist_status,
    upsert_thesis,
    upsert_watchlist_candidate,
)
from .portfolios import (
    create_paper_portfolio,
    delete_paper_portfolio,
    get_paper_portfolio,
    get_paper_portfolio_summary,
    list_paper_portfolios,
    update_paper_portfolio,
)
from .positions import (
    add_wash_sale_restriction,
    close_paper_position,
    close_tax_lot,
    create_tax_lot,
    get_paper_position,
    get_paper_positions,
    get_tax_lot,
    get_tax_lots,
    get_wash_sale_restriction,
    get_wash_sale_restrictions,
    is_wash_sale_restricted,
    open_paper_position,
    update_paper_position_quantity,
    update_paper_position_risk,
)
from .plans import (
    count_executed_sell_plans,
    create_trade_plan,
    get_oldest_executed_sell_at,
    get_trade_plan,
    get_trade_plans,
    update_trade_plan_benchmarks,
    update_trade_plan_status,
)
from .snapshots import (
    get_calibration_data,
    get_daily_snapshots,
    get_execution_quality_history,
    get_execution_quality_snapshot,
    get_latest_signal_snapshot,
    get_pending_outcomes,
    get_performance_timeseries,
    get_signal_effectiveness,
    save_daily_snapshot,
    save_execution_quality_snapshot,
    save_signal_snapshot,
    update_signal_outcome,
)


_PUBLIC_NAMES = ['ALERT_TYPES', 'Any', 'DB_PATH', 'DEFAULT_AUTO_APPROVE_THRESHOLD', 'DEFAULT_DAILY_CAPITAL_CEILING_PCT', 'DEFAULT_LOT_SELECTION_METHOD', 'DEFAULT_LTCG_DEFER_WINDOW_DAYS', 'DEFAULT_OPERATING_MODE', 'DataValidationError', 'DuplicateThemeError', 'LOT_SELECTION_METHODS', 'NOTIFICATION_CHANNELS', 'OPERATING_MODES', 'Path', 'PersistenceError', '_PersistenceModule', '_core', '_loaded_core', 'acknowledge_alert', 'acknowledge_all_alerts', 'add_ticker_to_theme', 'add_wash_sale_restriction', 'annotations', 'asdict', 'close_paper_position', 'close_tax_lot', 'core', 'count_executed_sell_plans', 'count_todays_trades', 'create_paper_portfolio', 'create_tax_lot', 'create_theme', 'create_trade_plan', 'datetime', 'delete_paper_portfolio', 'delete_setting', 'delete_supply_chain', 'delete_theme', 'delete_thesis', 'delete_watchlist_entry', 'get_alerts', 'get_all_settings', 'get_audit_trail', 'get_auto_approve_threshold', 'get_cached_earnings_date', 'get_cached_sector', 'get_calibration_data', 'get_channels_for_alert', 'get_daily_audit_summary', 'get_daily_capital_ceiling_pct', 'get_daily_capital_deployed', 'get_daily_snapshots', 'get_execution_quality_history', 'get_execution_quality_snapshot', 'get_historical_regime_durations', 'get_latest_regime_label', 'get_latest_signal_snapshot', 'get_latest_thesis_monitor_run', 'get_llm_attribution_summary', 'get_lot_selection_method', 'get_ltcg_defer_window_days', 'get_notification_preferences', 'get_oldest_executed_sell_at', 'get_operating_mode', 'get_paper_portfolio', 'get_paper_portfolio_summary', 'get_paper_position', 'get_paper_positions', 'get_pending_outcomes', 'get_pending_transition_outcomes', 'get_performance_timeseries', 'get_recent_regime_changes', 'get_sentiment_history', 'get_setting', 'get_signal_effectiveness', 'get_stress_test_result_by_id', 'get_stress_test_results', 'get_supply_chain', 'get_tax_lot', 'get_tax_lots', 'get_theme', 'get_theme_health_data', 'get_theme_tickers', 'get_thesis_monitor_runs', 'get_ticker_themes', 'get_trade_plan', 'get_trade_plans', 'get_training_history', 'get_training_run', 'get_transition_journal', 'get_transition_statistics', 'get_wash_sale_restriction', 'get_wash_sale_restrictions', 'get_watchlist', 'get_watchlist_by_ticker', 'get_watchlist_entry', 'get_watchlist_stats', 'importlib', 'is_dataclass', 'is_live_trading_unlocked', 'is_wash_sale_restricted', 'json', 'list_paper_portfolios', 'list_themes', 'list_theses', 'log_audit_event', 'log_barrier_override', 'log_training_run', 'logger', 'logging', 'mark_stress_test_status', 'open_paper_position', 'os', 'remove_ticker_from_theme', 'save_alert', 'save_daily_snapshot', 'save_earnings_cache', 'save_execution_quality_snapshot', 'save_regime_change_with_price', 'save_regime_event', 'save_sector_cache', 'save_sentiment', 'save_signal_snapshot', 'save_stress_test_result', 'save_supply_chain_layers', 'save_thesis_monitor_run', 'set_auto_approve_threshold', 'set_daily_capital_ceiling_pct', 'set_live_trading_unlocked', 'set_lot_selection_method', 'set_ltcg_defer_window_days', 'set_notification_preference', 'set_operating_mode', 'set_setting', 'setup_regime_logging', 'sqlite3', 'sys', 'timedelta', 'timezone', 'types', 'update_paper_portfolio', 'update_paper_position_quantity', 'update_paper_position_risk', 'update_signal_outcome', 'update_theme', 'update_ticker_in_theme', 'update_trade_plan_benchmarks', 'update_trade_plan_status', 'update_training_status', 'update_transition_outcome', 'update_watchlist_cross_sectional', 'update_watchlist_fundamental_gate', 'update_watchlist_status', 'upsert_thesis', 'upsert_watchlist_candidate']
__all__ = list(_PUBLIC_NAMES)


def _iter_forward_modules():
    return [_core, *_DOMAIN_MODULES]


class _PersistenceModule(types.ModuleType):
    def __dir__(self):
        return sorted(_PUBLIC_NAMES)

    def __getattr__(self, name: str):
        for module in _iter_forward_modules():
            if hasattr(module, name):
                return getattr(module, name)
        raise AttributeError(name)

    def __setattr__(self, name: str, value):
        if name in {"_core", "_loaded_core", "_DOMAIN_MODULES", "_PUBLIC_NAMES"} or name.startswith("__"):
            super().__setattr__(name, value)
            return
        for module in _iter_forward_modules():
            if hasattr(module, name):
                setattr(module, name, value)
        super().__setattr__(name, value)


sys.modules[__name__].__class__ = _PersistenceModule
