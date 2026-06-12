from __future__ import annotations

import datetime as dt
import importlib
import json
import logging
import math
import sys
import types
import uuid
from dataclasses import asdict
from typing import Any

import pandas as pd

from ..agent_competition import active_ticker_owners, configured_beta_portfolio_ids, diversification_settings
from ..agent_policy import (
    agent_candidate_policy,
    buy_pause_status,
    earnings_blackout_status,
    near_close_cancel_active,
    setting_bool as policy_setting_bool,
    setting_float as policy_setting_float,
    setting_int as policy_setting_int,
)
from ..broker_adapter import BrokerAdapter, OrderRequest, PaperBrokerAdapter, submit_guarded_order, validate_guardrails
from ..config import DEFAULT_PAPER_TRADING_CONFIG, DEFAULT_RISK_GUARDRAILS, PaperTradingConfig, RiskGuardrails
from ..decision_constants import DEFAULT_EXIT_TIME_STOP_DAYS, DEFAULT_NEUTRAL_REDUCE_FRACTION, TRAILING_STOP_ACTIVATION_ATR
from ..fundamental_data import fetch_financial_statements
from ..hurdle_rate import check_duration_gate, check_hurdle_rate, get_hurdle_settings
from ..ltcg_override import get_ltcg_override_settings
from ..market_data_client import download_daily_bars, get_ticker_info
from ..order_routing import decide_routing
from ..persistence import (
    close_paper_position, count_todays_trades, create_trade_plan, get_audit_trail,
    get_auto_approve_threshold, get_daily_capital_ceiling_pct, get_daily_capital_deployed,
    get_daily_snapshots, get_latest_signal_snapshot, get_operating_mode, get_paper_portfolio,
    get_paper_portfolio_summary, get_paper_positions, get_performance_timeseries, get_setting,
    get_trade_plans, get_watchlist, get_watchlist_by_ticker, list_paper_portfolios, list_themes,
    log_audit_event, log_barrier_override, open_paper_position, save_alert, save_daily_snapshot,
    set_auto_approve_threshold, set_daily_capital_ceiling_pct, set_operating_mode, update_paper_portfolio,
    update_paper_position_quantity, update_paper_position_risk, update_trade_plan_status,
)
from ..ib_types import ET
from ..signal_quality import ACTIONABLE_SIGNAL_SCORE, SignalQuality, evaluate_signal_quality
from . import core as _loaded_core

_core = importlib.reload(_loaded_core)
from . import execution as _execution
from . import performance as _performance
from . import planning as _planning
from . import sizing as _sizing

_DOMAIN_MODULES = [
    importlib.reload(_sizing),
    importlib.reload(_performance),
    importlib.reload(_planning),
    importlib.reload(_execution),
]

core = _core
logger = _core.logger
CachedRegimeValue = _core.CachedRegimeValue
CachedRegimeMap = _core.CachedRegimeMap
DEFAULT_SIZING_METHOD = _core.DEFAULT_SIZING_METHOD
DEFAULT_SIZING_BASE_RISK_FRACTION = _core.DEFAULT_SIZING_BASE_RISK_FRACTION
DEFAULT_SIZING_ATR_MULTIPLIER = _core.DEFAULT_SIZING_ATR_MULTIPLIER
DEFAULT_BETA_TARGET_MONTHLY_RETURN = _core.DEFAULT_BETA_TARGET_MONTHLY_RETURN
DEFAULT_BETA_TARGET_ROLLING_MONTHS = _core.DEFAULT_BETA_TARGET_ROLLING_MONTHS
DEFAULT_BETA_TARGET_BENCHMARKS = _core.DEFAULT_BETA_TARGET_BENCHMARKS
LONG_TERM_HOLDING_DAYS = _core.LONG_TERM_HOLDING_DAYS

from .core import (
    _batch_current_prices, _lookup_atr, _lookup_beta, _open_position_index, _pending_plan_index,
    get_beta_target_settings, get_sizing_settings, is_portfolio_autonomy_enabled,
)
from .sizing import allocate_budget, compute_position_budget, compute_theme_budget, _risk_adjusted_quantity
from .planning import (
    _actual_fill_trade_geometry, _neutral_reduce_reason, _reduced_exit_quantity, generate_buy_plans,
    generate_daily_plans, generate_exit_plans, generate_holdings_plans, trailing_stop_level,
)
from .execution import (
    _apply_filled_execution, auto_approve_plans, auto_execute_approved, cancel_submitted_orders_by_policy,
    execute_approved_plans, execute_approved_plans_via_adapter, expire_stale_plans, kill_switch,
)
from .performance import (
    compute_benchmark_comparison, compute_benchmark_set, compute_beta_target_progress, compute_daily_snapshot,
    compute_paper_performance, estimate_after_tax_performance, get_paper_dashboard, record_trade_outcome,
)


def _refresh_consumer_bindings() -> None:
    for module_name in ("src.regime.pipeline_backtest", "src.regime.triple_barrier"):
        module = sys.modules.get(module_name)
        if module is None:
            continue
        if hasattr(module, "trailing_stop_level"):
            setattr(module, "trailing_stop_level", trailing_stop_level)
        if hasattr(module, "_actual_fill_trade_geometry"):
            setattr(module, "_actual_fill_trade_geometry", _actual_fill_trade_geometry)


_refresh_consumer_bindings()

_PUBLIC_NAMES = ['ACTIONABLE_SIGNAL_SCORE', 'Any', 'BrokerAdapter', 'CachedRegimeMap', 'CachedRegimeValue', 'DEFAULT_BETA_TARGET_BENCHMARKS', 'DEFAULT_BETA_TARGET_MONTHLY_RETURN', 'DEFAULT_BETA_TARGET_ROLLING_MONTHS', 'DEFAULT_EXIT_TIME_STOP_DAYS', 'DEFAULT_NEUTRAL_REDUCE_FRACTION', 'DEFAULT_PAPER_TRADING_CONFIG', 'DEFAULT_RISK_GUARDRAILS', 'DEFAULT_SIZING_ATR_MULTIPLIER', 'DEFAULT_SIZING_BASE_RISK_FRACTION', 'DEFAULT_SIZING_METHOD', 'ET', 'LONG_TERM_HOLDING_DAYS', 'OrderRequest', 'PaperBrokerAdapter', 'PaperTradingConfig', 'RiskGuardrails', 'SignalQuality', 'TRAILING_STOP_ACTIVATION_ATR', '_PaperTradingModule', '_actual_fill_trade_geometry', '_apply_filled_execution', '_batch_current_prices', '_core', '_loaded_core', '_lookup_atr', '_lookup_beta', '_neutral_reduce_reason', '_open_position_index', '_pending_plan_index', '_reduced_exit_quantity', '_risk_adjusted_quantity', 'active_ticker_owners', 'agent_candidate_policy', 'allocate_budget', 'annotations', 'asdict', 'auto_approve_plans', 'auto_execute_approved', 'buy_pause_status', 'cancel_submitted_orders_by_policy', 'check_duration_gate', 'check_hurdle_rate', 'close_paper_position', 'compute_benchmark_comparison', 'compute_benchmark_set', 'compute_beta_target_progress', 'compute_daily_snapshot', 'compute_paper_performance', 'compute_position_budget', 'compute_theme_budget', 'configured_beta_portfolio_ids', 'core', 'count_todays_trades', 'create_trade_plan', 'decide_routing', 'diversification_settings', 'download_daily_bars', 'dt', 'earnings_blackout_status', 'estimate_after_tax_performance', 'evaluate_signal_quality', 'execute_approved_plans', 'execute_approved_plans_via_adapter', 'expire_stale_plans', 'fetch_financial_statements', 'generate_buy_plans', 'generate_daily_plans', 'generate_exit_plans', 'generate_holdings_plans', 'get_audit_trail', 'get_auto_approve_threshold', 'get_beta_target_settings', 'get_daily_capital_ceiling_pct', 'get_daily_capital_deployed', 'get_daily_snapshots', 'get_hurdle_settings', 'get_latest_signal_snapshot', 'get_ltcg_override_settings', 'get_operating_mode', 'get_paper_dashboard', 'get_paper_portfolio', 'get_paper_portfolio_summary', 'get_paper_positions', 'get_performance_timeseries', 'get_setting', 'get_sizing_settings', 'get_ticker_info', 'get_trade_plans', 'get_watchlist', 'get_watchlist_by_ticker', 'importlib', 'is_portfolio_autonomy_enabled', 'json', 'kill_switch', 'list_paper_portfolios', 'list_themes', 'log_audit_event', 'log_barrier_override', 'logger', 'logging', 'math', 'near_close_cancel_active', 'open_paper_position', 'pd', 'policy_setting_bool', 'policy_setting_float', 'policy_setting_int', 'record_trade_outcome', 'save_alert', 'save_daily_snapshot', 'set_auto_approve_threshold', 'set_daily_capital_ceiling_pct', 'set_operating_mode', 'submit_guarded_order', 'sys', 'trailing_stop_level', 'types', 'update_paper_portfolio', 'update_paper_position_quantity', 'update_paper_position_risk', 'update_trade_plan_status', 'uuid', 'validate_guardrails']
__all__ = list(_PUBLIC_NAMES)


def _iter_forward_modules():
    return [_core, *_DOMAIN_MODULES]


class _PaperTradingModule(types.ModuleType):
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


sys.modules[__name__].__class__ = _PaperTradingModule
