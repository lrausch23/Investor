# mypy: ignore-errors
from __future__ import annotations

import datetime as dt
import json
import logging
import math
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
from ..config import (
    DEFAULT_PAPER_TRADING_CONFIG,
    DEFAULT_RISK_GUARDRAILS,
    PaperTradingConfig,
    RiskGuardrails,
)
from ..decision_constants import (
    DEFAULT_EXIT_TIME_STOP_DAYS,
    DEFAULT_NEUTRAL_REDUCE_FRACTION,
    TRAILING_STOP_ACTIVATION_ATR,
)
from ..discovery import _quick_regime_screen
from ..fundamental_data import fetch_financial_statements
from ..hurdle_rate import check_duration_gate, check_hurdle_rate, get_hurdle_settings
from ..ltcg_override import get_ltcg_override_settings
from ..market_data_client import download_daily_bars, get_ticker_info
from ..order_routing import decide_routing
from ..persistence import (
    close_paper_position,
    count_todays_trades,
    create_trade_plan,
    get_auto_approve_threshold,
    get_daily_capital_ceiling_pct,
    get_daily_capital_deployed,
    get_daily_snapshots,
    get_audit_trail,
    get_operating_mode,
    get_performance_timeseries,
    get_paper_portfolio,
    get_paper_portfolio_summary,
    get_paper_positions,
    get_latest_signal_snapshot,
    get_setting,
    get_trade_plans,
    get_watchlist,
    get_watchlist_by_ticker,
    log_barrier_override,
    log_audit_event,
    list_paper_portfolios,
    list_themes,
    open_paper_position,
    save_alert,
    save_daily_snapshot,
    set_auto_approve_threshold,
    set_daily_capital_ceiling_pct,
    set_operating_mode,
    update_paper_portfolio,
    update_paper_position_quantity,
    update_paper_position_risk,
    update_trade_plan_status,
)
from ..ib_types import ET
from ..signal_quality import ACTIONABLE_SIGNAL_SCORE, SignalQuality, evaluate_signal_quality
from . import core

logger = core.logger
CachedRegimeValue = core.CachedRegimeValue
CachedRegimeMap = core.CachedRegimeMap
DEFAULT_SIZING_METHOD = core.DEFAULT_SIZING_METHOD
DEFAULT_SIZING_BASE_RISK_FRACTION = core.DEFAULT_SIZING_BASE_RISK_FRACTION
DEFAULT_SIZING_ATR_MULTIPLIER = core.DEFAULT_SIZING_ATR_MULTIPLIER
DEFAULT_BETA_TARGET_MONTHLY_RETURN = core.DEFAULT_BETA_TARGET_MONTHLY_RETURN
DEFAULT_BETA_TARGET_ROLLING_MONTHS = core.DEFAULT_BETA_TARGET_ROLLING_MONTHS
DEFAULT_BETA_TARGET_BENCHMARKS = core.DEFAULT_BETA_TARGET_BENCHMARKS
LONG_TERM_HOLDING_DAYS = core.LONG_TERM_HOLDING_DAYS

_routing_time_in_force_from_plan = core._routing_time_in_force_from_plan
_guardrail_block_details = core._guardrail_block_details
_normalize_close_series = core._normalize_close_series
_last_price_from_series = core._last_price_from_series
_now = core._now
_theme_map = core._theme_map
_parse_timestamp = core._parse_timestamp
_aware_utc = core._aware_utc
_holding_days = core._holding_days
_cached_regime_map = core._cached_regime_map
_batch_current_prices = core._batch_current_prices
_pending_plan_index = core._pending_plan_index
_open_position_index = core._open_position_index
get_sizing_settings = core.get_sizing_settings
get_beta_target_settings = core.get_beta_target_settings
is_portfolio_autonomy_enabled = core.is_portfolio_autonomy_enabled
_positive_float = core._positive_float
_entry_signal_max_age_days = core._entry_signal_max_age_days
_lookup_atr = core._lookup_atr
_lookup_beta = core._lookup_beta
_truthy = core._truthy
_serialize_rows = core._serialize_rows


def compute_theme_budget(
    total_budget: float,
    conviction: int,
    config: PaperTradingConfig = DEFAULT_PAPER_TRADING_CONFIG,
) -> float:
    conviction_index = max(0, min(int(conviction), len(config.conviction_allocation) - 1))
    return float(total_budget) * float(config.conviction_allocation[conviction_index])


def compute_position_budget(
    theme_budget: float,
    role: str,
    total_budget: float,
    config: PaperTradingConfig = DEFAULT_PAPER_TRADING_CONFIG,
) -> float:
    normalized_role = str(role or "Critical-Path")
    if normalized_role == "Core":
        budget = float(theme_budget) * float(config.core_max_pct)
    elif normalized_role == "Speculative":
        budget = float(theme_budget) * float(config.speculative_max_pct)
        budget = min(budget, float(total_budget) * float(config.speculative_absolute_cap_pct))
    else:
        budget = float(theme_budget) * float(config.critical_path_max_pct)
    return max(0.0, budget)


def allocate_budget(
    portfolio_id: int,
    themes: list[dict[str, Any]] | None = None,
    config: PaperTradingConfig = DEFAULT_PAPER_TRADING_CONFIG,
) -> dict[str, Any]:
    portfolio = get_paper_portfolio(portfolio_id)
    if portfolio is None:
        return {}
    total_budget = float(portfolio.get("starting_budget") or config.default_budget)
    cash_reserve = total_budget * float(config.min_cash_reserve_pct)
    allocatable = max(0.0, total_budget - cash_reserve)
    active_themes = themes if themes is not None else [
        theme for theme in list_themes(include_closed=False) if str(theme.get("status") or "") == "Active"
    ]

    theme_rows: list[dict[str, Any]] = []
    total_requested = 0.0
    for theme in active_themes:
        conviction = int(theme.get("conviction") or 0)
        allocated = compute_theme_budget(total_budget, conviction, config=config)
        total_requested += allocated
        theme_rows.append(
            {
                "theme_id": int(theme.get("id") or 0),
                "theme_name": str(theme.get("name") or ""),
                "conviction": conviction,
                "allocated": allocated,
                "by_role": {
                    "Core": compute_position_budget(allocated, "Core", total_budget, config=config),
                    "Critical-Path": compute_position_budget(allocated, "Critical-Path", total_budget, config=config),
                    "Speculative": compute_position_budget(allocated, "Speculative", total_budget, config=config),
                },
            }
        )

    scale = (allocatable / total_requested) if total_requested > allocatable and total_requested > 0 else 1.0
    if scale != 1.0:
        for theme_row in theme_rows:
            theme_row["allocated"] = float(theme_row["allocated"]) * scale
            theme_row["by_role"] = {
                key: float(value) * scale
                for key, value in theme_row["by_role"].items()
            }

    allocated_total = sum(float(theme_row["allocated"]) for theme_row in theme_rows)
    return {
        "total_budget": total_budget,
        "cash_reserve": cash_reserve,
        "allocatable": allocatable,
        "themes": theme_rows,
        "unallocated": max(0.0, allocatable - allocated_total),
    }


def _risk_adjusted_quantity(
    role_budget: float,
    proposed_price: float,
    atr_14: float | None,
    beta: float | None,
    *,
    risk_per_share_multiplier: float = DEFAULT_SIZING_ATR_MULTIPLIER,
    base_risk_fraction: float = DEFAULT_SIZING_BASE_RISK_FRACTION,
) -> int:
    max_shares_by_capital = math.floor(float(role_budget) / float(proposed_price)) if proposed_price > 0 else 0
    if max_shares_by_capital <= 0:
        return 0
    if atr_14 is None or atr_14 <= 0:
        return max(0, math.floor(max_shares_by_capital * 0.5))
    effective_beta = max(float(beta or 1.0), 0.3)
    effective_risk_fraction = float(base_risk_fraction) / effective_beta
    risk_per_share = float(atr_14) * float(risk_per_share_multiplier)
    if risk_per_share <= 0:
        return max_shares_by_capital
    max_shares_by_risk = math.floor((float(role_budget) * effective_risk_fraction) / risk_per_share)
    return max(0, min(max_shares_by_capital, max_shares_by_risk))
