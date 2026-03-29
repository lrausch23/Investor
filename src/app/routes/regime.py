from __future__ import annotations

# HMM regime analysis is now built-in under src/regime/

import asyncio
import datetime as dt
import json
import logging
import os
from pathlib import Path
import re
import sqlite3
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError, as_completed
from dataclasses import asdict, dataclass, is_dataclass
from typing import Any
from urllib.parse import parse_qs

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from src.app.auth import auth_banner_message, require_actor
from src.app.db import db_session
from src.core.external_holdings import get_available_portfolio_scopes, get_current_tickers_by_scope, get_lot_details_by_scope
from src.app.routes.regime_cache import (
    archive_previous_payload,
    load_backtest_cache,
    load_payload,
    load_previous_payload,
    load_qualitative_cache,
    save_backtest_cache,
    save_payload,
    save_qualitative_cache,
)
from src.db.session import get_session
from src.regime.exceptions import DuplicateThemeError


router = APIRouter(prefix="/regime", tags=["regime"])
logger = logging.getLogger(__name__)

_MAX_TICKERS = 50
_JOB_TTL_SECONDS = 600
_MIN_REGIME_DAYS = 5
_MIN_SIGNAL_PROBABILITY = 0.70
_DEFAULT_FRONTIER_BATCH_SIZE = 5
_ADAPTER_TIMEOUT = 15
_MODEL_CACHE_TTL_SECONDS = 300
_JOBS: dict[str, "RegimeJob"] = {}
_JOBS_LOCK = threading.Lock()
_DISCOVERY_JOBS: dict[str, "DiscoveryJob"] = {}
_DISCOVERY_JOBS_LOCK = threading.Lock()
_EXECUTOR = ThreadPoolExecutor(max_workers=1, thread_name_prefix="regime-analysis")


@dataclass
class RegimeJob:
    job_id: str
    status: str
    tickers: list[str]
    benchmark: str
    period: str
    progress: int
    total: int
    payload: dict[str, Any] | None
    error: str | None
    created_at: dt.datetime
    show_all: bool = False
    frontier_enabled: bool = False
    account_id: int | None = None
    progress_text: str | None = None
    current_ticker: str | None = None
    eta_seconds: float | None = None
    cache_hits: int = 0
    cache_misses: int = 0
    partial_results: dict[str, Any] | None = None


@dataclass
class DiscoveryJob:
    job_id: str
    status: str
    theme_ids: list[int]
    progress: int
    total: int
    current_theme: str | None
    results: dict[str, Any] | None
    error: str | None
    created_at: dt.datetime
    frontier_provider: str = "auto"
    regenerate_supply_chain: bool = False


def _static_version() -> str:
    try:
        from pathlib import Path

        p = Path(__file__).resolve().parents[1] / "static"
        css_m = int((p / "app.css").stat().st_mtime) if (p / "app.css").exists() else 0
        js_m = int((p / "regime.js").stat().st_mtime) if (p / "regime.js").exists() else 0
        return str(max(css_m, js_m, 0))
    except Exception as exc:
        logger.debug("Unable to compute regime static asset version.", exc_info=exc)
        return "0"


def _load_hmm_runtime() -> tuple[dict[str, Any] | None, str | None]:
    try:
        from src.regime.config import (
            DEFAULT_IBKR_CONFIG,
            DEFAULT_RISK_GUARDRAILS,
            DEFAULT_TICKERS,
            IBKRConfig,
            RiskGuardrails,
            validate_ibkr_readiness,
        )
        from src.regime.broker_adapter import (
            AccountSummary,
            BrokerAdapter,
            GuardrailCheck,
            GuardrailResult,
            MockBrokerAdapter,
            OrderRequest,
            OrderResult,
            PaperBrokerAdapter,
            PositionInfo,
            submit_guarded_order,
            validate_guardrails,
        )
        from src.regime.ib_connection import MockIBBackend, get_ib_backend, get_mock_ib_backend
        from src.regime.ibkr_adapter import IBKRBrokerAdapter, poll_pending_orders
        from src.regime.ib_types import get_market_hours_status
        from src.regime.charts import build_confidence_timeline, build_regime_price_chart, build_transition_heatmap
        from src.regime.data import download_market_frame, get_next_earnings_date
        from src.regime.digest import generate_weekly_digest
        from src.regime.hmm_engine import fit_regime_model, fit_regime_model_weekly
        from src.regime.investor_adapter import (
            get_investor_db_path,
            get_latest_prices,
            get_portfolio_positions,
            get_portfolio_tickers,
            get_sector_map,
            get_tax_assumptions,
            get_wash_sale_risk,
            positions_by_ticker_and_account,
        )
        from src.regime.llm_layer import build_qualitative_assessment, configured_frontier_model, list_provider_models
        from src.regime.diagnostics import calibration_payload, duration_accuracy
        from src.regime.diagnostics import fit_probability_calibrator
        from src.regime.backtest import compare_to_benchmark, run_backtest
        from src.regime.portfolio import compute_return_correlations, portfolio_risk_summary_dict
        from src.regime.persistence import (
            close_paper_position,
            add_ticker_to_theme,
            create_paper_portfolio,
            create_trade_plan,
            create_theme,
            delete_supply_chain,
            delete_thesis,
            delete_paper_portfolio,
            delete_theme,
            delete_watchlist_entry,
            get_audit_trail,
            get_calibration_data,
            get_daily_audit_summary,
            get_daily_snapshots,
            get_performance_timeseries,
            get_paper_portfolio,
            get_paper_portfolio_summary,
            get_paper_position,
            get_paper_positions,
            get_pending_outcomes,
            get_pending_transition_outcomes,
            get_signal_effectiveness,
            get_historical_regime_durations,
            get_training_history,
            get_training_run,
            get_trade_plan,
            get_trade_plans,
            count_todays_trades,
            delete_setting,
            get_auto_approve_threshold,
            get_daily_capital_ceiling_pct,
            get_daily_capital_deployed,
            get_supply_chain,
            get_theme,
            get_theme_tickers,
            get_theme_health_data,
            get_all_settings,
            get_setting,
            get_ticker_themes,
            get_transition_journal,
            get_transition_statistics,
            get_watchlist,
            get_watchlist_by_ticker,
            get_watchlist_entry,
            get_watchlist_stats,
            log_audit_event,
            log_training_run,
            list_paper_portfolios,
            list_theses,
            list_themes,
            open_paper_position,
            remove_ticker_from_theme,
            save_supply_chain_layers,
            save_regime_change_with_price,
            save_daily_snapshot,
            save_regime_event,
            save_sentiment,
            save_signal_snapshot,
            set_auto_approve_threshold,
            set_daily_capital_ceiling_pct,
            set_operating_mode,
            set_setting,
            update_paper_portfolio,
            update_theme,
            update_trade_plan_status,
            update_training_status,
            update_ticker_in_theme,
            update_watchlist_status,
            update_transition_outcome,
            update_signal_outcome,
            upsert_watchlist_candidate,
            upsert_thesis,
            get_operating_mode,
            OPERATING_MODES,
        )
        from src.regime.discovery import (
            check_entry_signals,
            compute_crowd_score,
            expire_stale_candidates,
            generate_supply_chain,
            promote_candidate,
            run_discovery_scan,
            run_full_discovery,
        )
        from src.regime.ensemble import (
            PassthroughAnalyst,
            aggregate_analysts,
            get_registry,
        )
        from src.regime.meta_labeler import (
            DEFAULT_META_LABELER_CONFIG,
            META_FEATURES,
            MetaLabelerConfig,
            MetaLabelerEngine,
            auto_load_active_model,
            create_and_register,
            extract_meta_features,
            get_next_version,
            list_saved_versions,
            should_retrain,
            _version_path,
        )
        from src.regime.attribution import (
            compute_attribution_summary,
            compute_ml_accuracy,
            compute_regime_attribution,
            compute_source_attribution,
            compute_theme_attribution,
        )
        from src.regime.alerts import format_alert_summary
        from src.regime.paper_trading import (
            allocate_budget,
            auto_approve_plans,
            auto_execute_approved,
            compute_benchmark_comparison,
            compute_daily_snapshot,
            compute_paper_performance,
            execute_approved_plans,
            execute_approved_plans_via_adapter,
            expire_stale_plans as expire_stale_trade_plans,
            generate_buy_plans,
            generate_daily_plans,
            generate_exit_plans,
            generate_holdings_plans,
            kill_switch,
            record_trade_outcome,
        )
        from src.regime.signals import (
            apply_signal_context,
            build_composite_signal,
            compute_position_size,
            compute_price_targets,
            compute_technicals,
            concentration_adjusted_strength,
            confidence_trajectory,
            divergence_severity,
            earnings_warning,
            forward_regime_curve,
            intra_regime_signal,
            multi_timeframe_signal,
            sentiment_momentum,
            signal_from_forward_curve,
            tax_adjusted_signals,
            compute_unified_confidence,
        )
        from src.regime.triple_barrier import BarrierConfig, apply_triple_barrier_labels, build_labeled_frame
    except ImportError:
        return None, (
            "Regime analytics are unavailable because hmm-market-regime-tool is not installed "
            "in the Investor environment."
        )

    runtime = {
        "DEFAULT_TICKERS": DEFAULT_TICKERS,
        "IBKRConfig": IBKRConfig,
        "DEFAULT_IBKR_CONFIG": DEFAULT_IBKR_CONFIG,
        "validate_ibkr_readiness": validate_ibkr_readiness,
        "RiskGuardrails": RiskGuardrails,
        "DEFAULT_RISK_GUARDRAILS": DEFAULT_RISK_GUARDRAILS,
        "BrokerAdapter": BrokerAdapter,
        "PaperBrokerAdapter": PaperBrokerAdapter,
        "MockBrokerAdapter": MockBrokerAdapter,
        "MockIBBackend": MockIBBackend,
        "get_ib_backend": get_ib_backend,
        "get_mock_ib_backend": get_mock_ib_backend,
        "IBKRBrokerAdapter": IBKRBrokerAdapter,
        "poll_pending_orders": poll_pending_orders,
        "get_market_hours_status": get_market_hours_status,
        "OrderRequest": OrderRequest,
        "OrderResult": OrderResult,
        "PositionInfo": PositionInfo,
        "AccountSummary": AccountSummary,
        "GuardrailCheck": GuardrailCheck,
        "GuardrailResult": GuardrailResult,
        "validate_guardrails": validate_guardrails,
        "submit_guarded_order": submit_guarded_order,
        "download_market_frame": download_market_frame,
        "build_regime_price_chart": build_regime_price_chart,
        "build_transition_heatmap": build_transition_heatmap,
        "build_confidence_timeline": build_confidence_timeline,
        "get_next_earnings_date": get_next_earnings_date,
        "generate_weekly_digest": generate_weekly_digest,
        "fit_regime_model": fit_regime_model,
        "fit_regime_model_weekly": fit_regime_model_weekly,
        "build_qualitative_assessment": build_qualitative_assessment,
        "configured_frontier_model": configured_frontier_model,
        "list_provider_models": list_provider_models,
        "calibration_payload": calibration_payload,
        "compare_to_benchmark": compare_to_benchmark,
        "portfolio_risk_summary_dict": portfolio_risk_summary_dict,
        "compute_return_correlations": compute_return_correlations,
        "create_theme": create_theme,
        "create_paper_portfolio": create_paper_portfolio,
        "create_trade_plan": create_trade_plan,
        "update_theme": update_theme,
        "update_paper_portfolio": update_paper_portfolio,
        "update_trade_plan_status": update_trade_plan_status,
        "delete_theme": delete_theme,
        "delete_paper_portfolio": delete_paper_portfolio,
        "list_themes": list_themes,
        "list_paper_portfolios": list_paper_portfolios,
        "get_theme": get_theme,
        "get_paper_portfolio": get_paper_portfolio,
        "get_paper_portfolio_summary": get_paper_portfolio_summary,
        "get_paper_position": get_paper_position,
        "get_paper_positions": get_paper_positions,
        "get_trade_plans": get_trade_plans,
        "get_audit_trail": get_audit_trail,
        "count_todays_trades": count_todays_trades,
        "get_setting": get_setting,
        "set_setting": set_setting,
        "get_all_settings": get_all_settings,
        "delete_setting": delete_setting,
        "get_operating_mode": get_operating_mode,
        "set_operating_mode": set_operating_mode,
        "get_auto_approve_threshold": get_auto_approve_threshold,
        "set_auto_approve_threshold": set_auto_approve_threshold,
        "get_daily_capital_ceiling_pct": get_daily_capital_ceiling_pct,
        "set_daily_capital_ceiling_pct": set_daily_capital_ceiling_pct,
        "get_daily_capital_deployed": get_daily_capital_deployed,
        "OPERATING_MODES": OPERATING_MODES,
        "get_daily_audit_summary": get_daily_audit_summary,
        "get_daily_snapshots": get_daily_snapshots,
        "get_performance_timeseries": get_performance_timeseries,
        "save_supply_chain_layers": save_supply_chain_layers,
        "get_supply_chain": get_supply_chain,
        "delete_supply_chain": delete_supply_chain,
        "add_ticker_to_theme": add_ticker_to_theme,
        "remove_ticker_from_theme": remove_ticker_from_theme,
        "update_ticker_in_theme": update_ticker_in_theme,
        "get_ticker_themes": get_ticker_themes,
        "get_theme_tickers": get_theme_tickers,
        "get_theme_health_data": get_theme_health_data,
        "upsert_watchlist_candidate": upsert_watchlist_candidate,
        "get_watchlist": get_watchlist,
        "update_watchlist_status": update_watchlist_status,
        "get_watchlist_entry": get_watchlist_entry,
        "get_watchlist_by_ticker": get_watchlist_by_ticker,
        "get_watchlist_stats": get_watchlist_stats,
        "delete_watchlist_entry": delete_watchlist_entry,
        "run_discovery_scan": run_discovery_scan,
        "run_full_discovery": run_full_discovery,
        "generate_supply_chain": generate_supply_chain,
        "compute_crowd_score": compute_crowd_score,
        "check_entry_signals": check_entry_signals,
        "expire_stale_candidates": expire_stale_candidates,
        "promote_candidate": promote_candidate,
        "get_registry": get_registry,
        "aggregate_analysts": aggregate_analysts,
        "apply_triple_barrier_labels": apply_triple_barrier_labels,
        "build_labeled_frame": build_labeled_frame,
        "BarrierConfig": BarrierConfig,
        "PassthroughAnalyst": PassthroughAnalyst,
        "MetaLabelerEngine": MetaLabelerEngine,
        "MetaLabelerConfig": MetaLabelerConfig,
        "DEFAULT_META_LABELER_CONFIG": DEFAULT_META_LABELER_CONFIG,
        "extract_meta_features": extract_meta_features,
        "create_and_register_meta_labeler": create_and_register,
        "auto_load_active_model": auto_load_active_model,
        "should_retrain": should_retrain,
        "get_next_version": get_next_version,
        "list_saved_versions": list_saved_versions,
        "_version_path": _version_path,
        "META_FEATURES": META_FEATURES,
        "delete_thesis": delete_thesis,
        "format_alert_summary": format_alert_summary,
        "get_investor_db_path": get_investor_db_path,
        "get_latest_prices": get_latest_prices,
        "allocate_budget": allocate_budget,
        "auto_approve_plans": auto_approve_plans,
        "auto_execute_approved": auto_execute_approved,
        "generate_buy_plans": generate_buy_plans,
        "generate_exit_plans": generate_exit_plans,
        "generate_holdings_plans": generate_holdings_plans,
        "generate_daily_plans": generate_daily_plans,
        "kill_switch": kill_switch,
        "execute_approved_plans": execute_approved_plans,
        "execute_approved_plans_via_adapter": execute_approved_plans_via_adapter,
        "expire_stale_trade_plans": expire_stale_trade_plans,
        "compute_paper_performance": compute_paper_performance,
        "compute_benchmark_comparison": compute_benchmark_comparison,
        "compute_theme_attribution": compute_theme_attribution,
        "compute_source_attribution": compute_source_attribution,
        "compute_regime_attribution": compute_regime_attribution,
        "compute_ml_accuracy": compute_ml_accuracy,
        "compute_attribution_summary": compute_attribution_summary,
        "compute_daily_snapshot": compute_daily_snapshot,
        "record_trade_outcome": record_trade_outcome,
        "save_daily_snapshot": save_daily_snapshot,
        "get_calibration_data": get_calibration_data,
        "get_historical_regime_durations": get_historical_regime_durations,
        "get_trade_plan": get_trade_plan,
        "get_pending_outcomes": get_pending_outcomes,
        "get_pending_transition_outcomes": get_pending_transition_outcomes,
        "get_signal_effectiveness": get_signal_effectiveness,
        "get_training_history": get_training_history,
        "get_training_run": get_training_run,
        "get_transition_journal": get_transition_journal,
        "get_transition_statistics": get_transition_statistics,
        "list_theses": list_theses,
        "open_paper_position": open_paper_position,
        "close_paper_position": close_paper_position,
        "log_audit_event": log_audit_event,
        "log_training_run": log_training_run,
        "get_portfolio_positions": get_portfolio_positions,
        "get_portfolio_tickers": get_portfolio_tickers,
        "get_sector_map": get_sector_map,
        "save_regime_change_with_price": save_regime_change_with_price,
        "save_regime_event": save_regime_event,
        "save_sentiment": save_sentiment,
        "save_signal_snapshot": save_signal_snapshot,
        "run_backtest": run_backtest,
        "get_tax_assumptions": get_tax_assumptions,
        "update_transition_outcome": update_transition_outcome,
        "update_signal_outcome": update_signal_outcome,
        "update_training_status": update_training_status,
        "upsert_thesis": upsert_thesis,
        "get_wash_sale_risk": get_wash_sale_risk,
        "positions_by_ticker_and_account": positions_by_ticker_and_account,
        "build_composite_signal": build_composite_signal,
        "earnings_warning": earnings_warning,
        "concentration_adjusted_strength": concentration_adjusted_strength,
        "compute_price_targets": compute_price_targets,
        "compute_technicals": compute_technicals,
        "confidence_trajectory": confidence_trajectory,
        "divergence_severity": divergence_severity,
        "forward_regime_curve": forward_regime_curve,
        "intra_regime_signal": intra_regime_signal,
        "apply_signal_context": apply_signal_context,
        "sentiment_momentum": sentiment_momentum,
        "signal_from_forward_curve": signal_from_forward_curve,
        "tax_adjusted_signals": tax_adjusted_signals,
        "multi_timeframe_signal": multi_timeframe_signal,
        "duration_accuracy": duration_accuracy,
        "fit_probability_calibrator": fit_probability_calibrator,
        "compute_unified_confidence": compute_unified_confidence,
        "compute_position_size": compute_position_size,
    }
    try:
        registry = runtime["get_registry"]()
        ml_engine = registry.get("xgboost_meta_labeler")
        if ml_engine is None:
            ml_engine = runtime["create_and_register_meta_labeler"](runtime["DEFAULT_META_LABELER_CONFIG"])
        if not ml_engine.is_ready():
            active_version = runtime["get_setting"]("meta_labeler_active_version")
            load_result = runtime["auto_load_active_model"](ml_engine, active_version)
            if load_result.get("loaded"):
                logger.info("Auto-loaded meta-labeler model v%s on startup", load_result.get("version"))
            else:
                logger.debug("No meta-labeler model auto-loaded: %s", load_result.get("status"))
    except Exception as exc:
        logger.debug("Meta-labeler auto-load skipped: %s", exc)
    return runtime, None


def _json_ready(value: Any) -> Any:
    if is_dataclass(value):
        return _json_ready(asdict(value))
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    if hasattr(value, "to_dict"):
        try:
            return _json_ready(value.to_dict(orient="records"))
        except Exception as exc:
            logger.debug("Failed dataframe to_dict orient=records conversion in _json_ready.", exc_info=exc)
            try:
                return _json_ready(value.to_dict())
            except Exception as inner_exc:
                logger.debug("Failed generic to_dict conversion in _json_ready.", exc_info=inner_exc)
                pass
    if hasattr(value, "__dict__") and not isinstance(value, type):
        try:
            return _json_ready(vars(value))
        except Exception as exc:
            logger.debug("Failed vars() conversion in _json_ready.", exc_info=exc)
            pass
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception as exc:
            logger.debug("Failed scalar item() conversion in _json_ready.", exc_info=exc)
            pass
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception as exc:
            logger.debug("Failed isoformat() conversion in _json_ready.", exc_info=exc)
            pass
    return value


def _signal_class(action: str) -> str:
    if action in {"Strong Buy", "Buy"}:
        return "cell-ok"
    if action in {"Strong Sell", "Sell"}:
        return "cell-bad"
    return ""


def _regime_class(label: str) -> str:
    if label == "Bull":
        return "cell-ok"
    if label == "Bear":
        return "cell-bad"
    return ""


def _kpi_tone_for_regime(label: str) -> str:
    if label == "Bull":
        return "positive"
    if label == "Bear":
        return "negative"
    return "neutral"


def _relative_strength_text(label: str, benchmark_label: str) -> str:
    if label == "Bull" and benchmark_label in {"Neutral", "Bear"}:
        return "Outperforming"
    if label == "Bear" and benchmark_label in {"Bull", "Neutral"}:
        return "Lagging"
    return "In-line"


def _extract_ai_verdict(frontier: dict[str, Any] | None) -> str | None:
    if not frontier:
        return None
    verdict = str(frontier.get("display_verdict") or "").strip()
    if verdict:
        return verdict
    institutional = frontier.get("institutional_report") or {}
    verdict = str(institutional.get("verdict") or "").strip()
    return verdict or None


def _verdict_to_action(verdict: str | None) -> str | None:
    """Derive a concrete action from an AI verdict when tax signals are unavailable."""
    if not verdict:
        return None
    normalized = str(verdict).strip().lower()
    if normalized == "entry":
        return "Buy"
    if normalized == "exit":
        return "Sell"
    return None


def _stop_proximity(row: dict[str, Any]) -> dict[str, Any] | None:
    targets = row.get("price_targets") or {}
    try:
        current_price = float(targets.get("current_price") or row.get("current_price") or 0.0)
        stop_price = float(row.get("theme_stop_price") or targets.get("stop_price") or 0.0)
    except Exception as exc:
        logger.debug("Unable to coerce stop proximity inputs.", exc_info=exc)
        return None
    if current_price <= 0 or stop_price <= 0:
        return None
    distance_pct = abs(current_price - stop_price) / abs(stop_price)
    if distance_pct <= 0.03:
        level = "critical"
    elif distance_pct <= 0.07:
        level = "warning"
    else:
        level = "safe"
    return {
        "level": level,
        "distance_pct": distance_pct,
        "label": f"{distance_pct * 100:.1f}% from stop",
    }


def _compute_run_diff(current: dict[str, Any], previous: dict[str, Any] | None) -> dict[str, Any]:
    if not previous:
        return {"has_previous": False, "changes": [], "summary": "No previous run available for comparison."}
    prev_rows = {
        str(row.get("ticker") or "").upper(): row
        for row in (previous.get("rows") or [])
        if isinstance(row, dict) and str(row.get("ticker") or "").strip()
    }
    changes: list[dict[str, Any]] = []
    for row in current.get("rows") or []:
        ticker = str(row.get("ticker") or "").upper()
        if not ticker:
            continue
        previous_row = prev_rows.get(ticker)
        if not previous_row:
            changes.append({"ticker": ticker, "type": "new", "message": f"{ticker} is new in this run."})
            continue
        if row.get("regime") != previous_row.get("regime"):
            changes.append(
                {
                    "ticker": ticker,
                    "type": "regime",
                    "message": f"{ticker}: regime changed {previous_row.get('regime', '—')} → {row.get('regime', '—')}.",
                }
            )
        if row.get("composite_signal") != previous_row.get("composite_signal"):
            changes.append(
                {
                    "ticker": ticker,
                    "type": "signal",
                    "message": f"{ticker}: signal changed {previous_row.get('composite_signal', '—')} → {row.get('composite_signal', '—')}.",
                }
            )
        previous_stop = (((previous_row.get("stop_proximity") or {}) if isinstance(previous_row, dict) else {}) or {}).get("level")
        current_stop = (((row.get("stop_proximity") or {}) if isinstance(row, dict) else {}) or {}).get("level")
        if current_stop != previous_stop and current_stop in {"critical", "warning"}:
            changes.append(
                {
                    "ticker": ticker,
                    "type": "stop",
                    "message": f"{ticker}: stop proximity is now {current_stop}.",
                }
            )
        previous_verdict = _extract_ai_verdict(previous_row.get("frontier") if isinstance(previous_row, dict) else None)
        current_verdict = _extract_ai_verdict(row.get("frontier"))
        if current_verdict and previous_verdict and current_verdict != previous_verdict:
            changes.append(
                {
                    "ticker": ticker,
                    "type": "ai_verdict",
                    "message": f"{ticker}: AI verdict changed {previous_verdict} → {current_verdict}.",
                }
            )
    summary = f"{len(changes)} change{'s' if len(changes) != 1 else ''} vs previous run." if changes else "No material changes vs previous run."
    return {"has_previous": True, "changes": changes, "summary": summary}


def _compute_theme_health(themes: list[dict[str, Any]], rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_ticker = {str(row.get("ticker") or "").upper(): row for row in rows if str(row.get("ticker") or "").strip()}
    results: list[dict[str, Any]] = []
    for theme in themes:
        if str(theme.get("status") or "Active") != "Active":
            continue
        tickers = theme.get("tickers") or []
        if not tickers:
            continue
        matched = [item for item in tickers if str(item.get("ticker") or "").upper() in by_ticker]
        if not matched:
            continue
        summary = {"Bull": {"count": 0, "weight_pct": 0.0}, "Neutral": {"count": 0, "weight_pct": 0.0}, "Bear": {"count": 0, "weight_pct": 0.0}}
        total_weight = 0.0
        core_labels: list[str] = []
        critical_labels: list[str] = []
        stop_distances: list[float] = []
        target_distances: list[float] = []
        role_counts = {"Core": 0, "Critical-Path": 0, "Speculative": 0}
        for association in matched:
            ticker = str(association.get("ticker") or "").upper()
            row = by_ticker.get(ticker)
            if row is None:
                continue
            label = str(row.get("regime") or "Neutral")
            weight = float(association.get("entry_price") or 1.0)
            total_weight += weight
            if label in summary:
                summary[label]["count"] += 1
                summary[label]["weight_pct"] += weight
            role = str(association.get("role") or "Core")
            role_counts[role] = role_counts.get(role, 0) + 1
            if role == "Core":
                core_labels.append(label)
            elif role == "Critical-Path":
                critical_labels.append(label)
            current_price = float(row.get("current_price") or 0.0)
            if current_price > 0 and association.get("target_price"):
                target_distances.append((float(association["target_price"]) - current_price) / current_price)
            stop_reference = float(association.get("stop_price") or 0.0)
            if current_price > 0 and stop_reference > 0:
                stop_distances.append(abs(current_price - stop_reference) / current_price)
        if total_weight > 0:
            for label in summary:
                summary[label]["weight_pct"] = (summary[label]["weight_pct"] / total_weight) * 100.0
        warning = None
        critical_bearish = len([label for label in critical_labels if label in {"Bear", "Neutral"}])
        core_bullish = len([label for label in core_labels if label == "Bull"])
        bear_share = summary["Bear"]["count"] / max(1, len(matched))
        if critical_labels and core_labels and critical_bearish / max(1, len(critical_labels)) >= 0.5 and core_bullish / max(1, len(core_labels)) >= 0.5:
            warning = "Critical-Path names deteriorating while Core holds — possible early rotation signal"
        elif bear_share >= 0.5:
            warning = "Theme under pressure"
        results.append(
            {
                "theme_id": theme.get("id"),
                "name": theme.get("name"),
                "conviction": theme.get("conviction"),
                "status": theme.get("status"),
                "ticker_count": len(matched),
                "role_counts": role_counts,
                "regime_summary": summary,
                "core_regime": core_labels[0] if core_labels else None,
                "critical_path_regime": critical_labels[0] if critical_labels else None,
                "health_warning": warning,
                "avg_distance_to_target_pct": (sum(target_distances) / len(target_distances) * 100.0) if target_distances else None,
                "avg_distance_to_stop_pct": (sum(stop_distances) / len(stop_distances) * 100.0) if stop_distances else None,
            }
        )
    return results


def _qualitative_confidence(qualitative: dict[str, Any] | None) -> int:
    if not qualitative:
        return 0
    llm_response = qualitative.get("llm_response") or {}
    if llm_response.get("confidence") is not None:
        return int(llm_response["confidence"])
    return int(qualitative.get("fallback_confidence") or 0)


def _qualitative_confidence_gauge(qualitative: dict[str, Any] | None) -> int:
    if not qualitative:
        return 0
    llm_response = qualitative.get("llm_response") or {}
    institutional = llm_response.get("institutional_report", {})
    if institutional.get("confidence_score") is not None:
        return int(institutional["confidence_score"])
    if llm_response.get("confidence_gauge") is not None:
        return int(llm_response["confidence_gauge"])
    confidence = _qualitative_confidence(qualitative)
    return max(1, min(10, round(confidence / 10))) if confidence else 0


def _verdict_display(
    *,
    regime_days: int,
    latest_probability: float,
    qualitative: dict[str, Any] | None,
) -> tuple[str, bool]:
    conditions: list[str] = []
    if regime_days < _MIN_REGIME_DAYS:
        conditions.append(f"Regime too new ({regime_days}d < {_MIN_REGIME_DAYS}d minimum)")
    if latest_probability < _MIN_SIGNAL_PROBABILITY:
        conditions.append(
            f"Low confidence ({latest_probability:.0%} < {_MIN_SIGNAL_PROBABILITY:.0%} threshold)"
        )
    if conditions:
        return "Hold — " + " | ".join(conditions), True
    institutional = ((qualitative or {}).get("llm_response") or {}).get("institutional_report", {})
    return str(institutional.get("verdict") or "Hold"), False


def _sizing_guidance(label: str, probability: float, signal_suppressed: bool) -> tuple[str, str]:
    if label == "Bull":
        if probability >= 0.90:
            text = "Suggested sizing: Full position (90%+ confidence)"
            color = "#1b5e20"
        elif probability >= 0.80:
            text = "Suggested sizing: 75% position (80-90% confidence)"
            color = "#1b5e20"
        elif probability >= 0.70:
            text = "Suggested sizing: 50% position (70-80% confidence)"
            color = "#b26a00"
        elif probability >= 0.60:
            text = "Suggested sizing: 25% position (60-70% confidence)"
            color = "#b26a00"
        else:
            text = "Suggested sizing: No new position (sub-60% confidence)"
            color = "#8b1e1e"
    elif label == "Neutral":
        text = "Suggested sizing: Hold / reduce to 25%"
        color = "#b26a00"
    else:
        text = "Suggested sizing: Exit or short (if strategy permits)"
        color = "#8b1e1e"
    if signal_suppressed:
        text += " - signal suppressed by filter"
    return text, color


def _regime_entry_date(regime: Any) -> str | None:
    try:
        if getattr(regime.price_frame, "empty", True):
            return None
        tail = regime.price_frame.tail(max(1, int(regime.regime_days)))
        if tail.empty:
            return None
        first_idx = tail.index[0]
        if hasattr(first_idx, "date"):
            return first_idx.date().isoformat()
        return str(first_idx)[:10]
    except Exception as exc:
        logger.debug("Unable to derive regime entry date.", exc_info=exc)
        return None


def _math_panel(regime: Any) -> dict[str, Any]:
    state_stats = getattr(regime, "state_statistics", None)
    records = _json_ready(state_stats.to_dict(orient="records")) if hasattr(state_stats, "to_dict") else []
    current_row: dict[str, Any] = {}
    if hasattr(state_stats, "loc") and state_stats is not None and not state_stats.empty:
        current = state_stats.loc[state_stats["state_id"] == regime.latest_state_id]
        if not current.empty:
            current_row = _json_ready(current.iloc[0].to_dict())
    return {
        "state_statistics": records,
        "current_state_statistics": current_row,
        "mean_return": current_row.get("mean_return"),
        "expected_volatility": current_row.get("expected_volatility"),
        "volume_zscore": current_row.get("volume_zscore"),
        "recent_state_mean_return": regime.recent_state_mean_return,
        "regime_entry_date": _regime_entry_date(regime),
        "regime_streak_days": int(regime.regime_days),
        "regime_signal": regime.regime_signal,
        "regime_inconsistency_warning": regime.regime_inconsistency_warning,
    }


def _frontier_panel(
    *,
    qualitative: dict[str, Any] | None,
    label: str,
    probability: float,
    regime_days: int,
    model_name: str | None,
) -> dict[str, Any] | None:
    if not qualitative:
        return None
    llm_response = qualitative.get("llm_response") or {}
    institutional = llm_response.get("institutional_report", {})
    qual_source = qualitative.get("source") if isinstance(qualitative, dict) else None
    override_reason = None
    if qual_source == "meta_labeler_override":
        override_reason = str(
            institutional.get("risk_trigger")
            or llm_response.get("rationale")
            or "Meta-labeler confidence below threshold"
        )
    displayed_verdict, overridden = _verdict_display(
        regime_days=regime_days,
        latest_probability=probability,
        qualitative=qualitative,
    )
    sizing_text, sizing_color = _sizing_guidance(label, probability, overridden)
    return {
        "thesis_check": qualitative.get("thesis_check_response"),
        "catalysts": qualitative.get("catalysts") or [],
        "institutional_report": institutional,
        "display_verdict": displayed_verdict,
        "verdict_overridden": overridden,
        "confidence_pct": _qualitative_confidence(qualitative),
        "confidence_score": _qualitative_confidence_gauge(qualitative),
        "sizing_guidance": {"text": sizing_text, "color": sizing_color},
        "model_name": model_name,
        "source": qual_source or "llm",
        "llm_override": qual_source == "meta_labeler_override",
        "llm_override_reason": override_reason,
    }


def _primary_tax_signal(account_signals: list[Any]) -> Any | None:
    if not account_signals:
        return None
    taxable = [signal for signal in account_signals if getattr(signal, "account_type", "") == "TAXABLE"]
    return taxable[0] if taxable else account_signals[0]


def _has_material_tax_adjustment(signal: Any) -> bool:
    if signal is None:
        return False
    try:
        estimated_tax_impact = float(getattr(signal, "estimated_tax_impact", 0.0) or 0.0)
    except Exception as exc:
        logger.debug("Unable to coerce estimated tax impact for tax signal.", exc_info=exc)
        estimated_tax_impact = 0.0
    adjusted_action = str(getattr(signal, "adjusted_action", "") or "").strip()
    original_action = str(getattr(signal, "original_action", "") or "").strip()
    wash_sale_warning = str(getattr(signal, "wash_sale_warning", "") or "").strip()
    ltcg_threshold_date = str(getattr(signal, "ltcg_threshold_date", "") or "").strip()
    return any(
        (
            abs(estimated_tax_impact) > 0.009,
            bool(wash_sale_warning),
            bool(ltcg_threshold_date),
            adjusted_action and original_action and adjusted_action != original_action,
        )
    )


def _default_digest() -> dict[str, list[Any]]:
    return {
        "entries": [],
        "regime_changes": [],
        "sentiment_divergences": [],
        "tax_alerts": [],
        "action_items": [],
    }


def _default_exposure() -> dict[str, float]:
    return {"Bull": 0.0, "Neutral": 0.0, "Bear": 0.0}


def _default_effectiveness() -> dict[str, Any]:
    return {
        "summary": {
            "1w": {"count": 0, "hit_rate": None, "avg_return": None},
            "1m": {"count": 0, "hit_rate": None, "avg_return": None},
            "3m": {"count": 0, "hit_rate": None, "avg_return": None},
        },
        "by_action": {"1w": {}, "1m": {}, "3m": {}},
        "rows": [],
    }


def _empty_dashboard_payload(
    *,
    benchmark: str = "SOXX",
    period: str = "3y",
    show_all: bool = False,
    runtime: dict[str, Any] | None = None,
    runtime_error: str | None = None,
) -> dict[str, Any]:
    now = dt.datetime.now().astimezone()
    return {
        "benchmark": benchmark,
        "period": period,
        "show_all": show_all,
        "frontier_enabled": False,
        "force_refresh": False,
        "hmm_available": runtime is not None,
        "warnings": [runtime_error] if runtime_error else [],
        "rows": [],
        "digest": _default_digest(),
        "portfolio_count": 0,
        "action_items_count": 0,
        "last_run_timestamp": "",
        "last_run_display": "Not started",
        "benchmark_regime": "Unavailable",
        "benchmark_regime_tone": "neutral",
        "portfolio_mode": "All holdings" if show_all else "Filtered holdings",
        "portfolio_scope": "household",
        "selected_tickers": [],
        "selected_count": 0,
        "job_status": "idle",
        "job_id": None,
        "generated_at": now.isoformat(timespec="seconds"),
        "cached_note": None,
        "frontier_provider": "auto",
        "frontier_model": None,
        "regime_exposure": _default_exposure(),
        "total_market_value": 0.0,
        "regime_history": [],
        "portfolio_summary": None,
        "recent_alerts": [],
        "unread_alert_count": 0,
        "model_diagnostics": None,
        "relative_strength": {"summary": None, "outperforming": [], "lagging": []},
        "signal_effectiveness": _default_effectiveness(),
        "run_diff": {"has_previous": False, "changes": [], "summary": "No previous run available for comparison."},
        "snapshots_saved": 0,
        "themes": [],
        "theme_health": [],
        "watchlist": [],
        "watchlist_stats": {},
    }


def _normalize_selected_tickers(raw: str | None) -> list[str]:
    def expand_token(token: str) -> list[str]:
        normalized = token.strip().upper()
        if not normalized:
            return []
        parts = [part.strip().upper() for part in normalized.split() if part.strip()]
        if len(parts) <= 1:
            return [normalized]
        if len(parts) == 2 and len(parts[1]) == 1 and len(parts[0]) <= 5:
            return [normalized]
        return parts

    tickers: list[str] = []
    for token in re.split(r"[,;\n]+", raw or ""):
        for candidate in expand_token(token):
            if candidate and candidate not in tickers:
                tickers.append(candidate)
    return tickers


def _parse_account_id(raw: Any) -> int | None:
    value = str(raw or "").strip()
    return int(value) if value.isdigit() else None


def _normalize_thesis_text(raw: Any) -> str:
    thesis = str(raw or "").strip()
    if not thesis:
        raise HTTPException(status_code=422, detail="Thesis text is required.")
    if len(thesis) > 2000:
        raise HTTPException(status_code=422, detail="Thesis text must be 2000 characters or fewer.")
    return thesis


def _normalize_theme_name(raw: Any) -> str:
    name = str(raw or "").strip()
    if not name:
        raise HTTPException(status_code=422, detail="Theme name is required.")
    if len(name) > 100:
        raise HTTPException(status_code=422, detail="Theme name must be 100 characters or fewer.")
    return name


def _normalize_theme_narrative(raw: Any) -> str:
    narrative = str(raw or "").strip()
    if len(narrative) > 4000:
        raise HTTPException(status_code=422, detail="Theme narrative must be 4000 characters or fewer.")
    return narrative


def _normalize_theme_conviction(raw: Any) -> int:
    try:
        conviction = int(str(raw or "3").strip() or "3")
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="Conviction must be an integer from 1 to 5.") from exc
    if conviction < 1 or conviction > 5:
        raise HTTPException(status_code=422, detail="Conviction must be an integer from 1 to 5.")
    return conviction


def _normalize_theme_status(raw: Any) -> str:
    status = str(raw or "Active").strip() or "Active"
    if status not in {"Active", "Monitoring", "Closed"}:
        raise HTTPException(status_code=422, detail="Status must be Active, Monitoring, or Closed.")
    return status


def _normalize_theme_sector_hint(raw: Any) -> str:
    value = str(raw or "").strip()
    if len(value) > 200:
        raise HTTPException(status_code=422, detail="Sector hint must be 200 characters or fewer.")
    return value


def _normalize_role(raw: Any) -> str:
    role = str(raw or "Core").strip() or "Core"
    if role not in {"Core", "Critical-Path", "Speculative"}:
        raise HTTPException(status_code=422, detail="Role must be Core, Critical-Path, or Speculative.")
    return role


def _normalize_time_horizon(raw: Any) -> str:
    value = str(raw or "strategic").strip().lower() or "strategic"
    if value not in {"trade", "tactical", "strategic"}:
        raise HTTPException(status_code=422, detail="Time horizon must be trade, tactical, or strategic.")
    return value


def _normalize_optional_positive_float(raw: Any, *, field_name: str) -> float | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        value = float(text)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=f"{field_name} must be a positive number.") from exc
    if value <= 0:
        raise HTTPException(status_code=422, detail=f"{field_name} must be a positive number.")
    return value


def _load_themes(runtime: dict[str, Any] | None) -> list[dict[str, Any]]:
    if runtime is None:
        return []
    try:
        return _json_ready(runtime["list_themes"]())
    except Exception as exc:
        logger.debug("Unable to load saved themes for regime page.", exc_info=exc)
        return []


def _load_theses(runtime: dict[str, Any] | None) -> list[dict[str, Any]]:
    if runtime is None:
        return []
    try:
        list_theses_fn = runtime.get("list_theses")
        return _json_ready(list_theses_fn()) if callable(list_theses_fn) else []
    except Exception as exc:
        logger.debug("Unable to load legacy theses for regime page.", exc_info=exc)
        return []


def _build_theme_context(ticker: str, runtime: dict[str, Any]) -> str | None:
    get_ticker_themes_fn = runtime.get("get_ticker_themes")
    if not callable(get_ticker_themes_fn):
        upsert_thesis_fn = runtime.get("upsert_thesis")
        if callable(upsert_thesis_fn):
            return upsert_thesis_fn(ticker, None)
        return None
    try:
        themes = get_ticker_themes_fn(ticker)
    except Exception as exc:
        logger.debug("Unable to load theme context for %s.", ticker, exc_info=exc)
        return None
    if not themes:
        return None
    parts: list[str] = []
    for theme in themes:
        parts.append(
            f"Theme: {theme['theme_name']} (conviction {theme['conviction']}/5)\n"
            f"Role: {theme.get('role') or 'Core'}\n"
            f"Rationale: {theme.get('rationale') or 'Not specified'}\n"
            f"Time Horizon: {theme.get('time_horizon') or 'strategic'}\n"
            f"Theme Narrative: {theme.get('narrative') or 'Not specified'}"
        )
    return "\n---\n".join(parts)


def _portfolio_tickers(
    runtime: dict[str, Any],
    session: Session | None,
    show_all: bool,
    portfolio_scope: str,
    account_id: int | None = None,
) -> tuple[str, list[str]]:
    investor_db_path = runtime["get_investor_db_path"]()
    tickers: list[str] = []
    if session is not None:
        tickers = get_current_tickers_by_scope(session, scope=portfolio_scope, account_id=account_id)
    if not tickers and show_all:
        tickers = runtime["get_portfolio_tickers"](investor_db_path)
    if not tickers and show_all:
        tickers = list(runtime["DEFAULT_TICKERS"])
    return investor_db_path, list(tickers)


def _cached_regime_for_paper_trading(payload: dict[str, Any] | None = None) -> dict[str, tuple[str, float]]:
    cached = payload if isinstance(payload, dict) else load_payload()
    rows = cached.get("rows") if isinstance(cached, dict) else []
    mapping: dict[str, tuple[str, float]] = {}
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        ticker = str(row.get("ticker") or "").strip().upper()
        if not ticker:
            continue
        label = str(row.get("regime") or "").strip()
        try:
            probability = float(row.get("probability") or 0.0)
        except Exception:
            probability = 0.0
        if label:
            mapping[ticker] = (label, probability)
    return mapping


def _get_broker_adapter(runtime: dict[str, Any], portfolio_id: int) -> Any:
    portfolio = runtime["get_paper_portfolio"](portfolio_id)
    if portfolio is None:
        return None
    broker_type = str(portfolio.get("broker_type") or "paper").strip().lower()
    if broker_type == "ibkr":
        config = runtime["DEFAULT_IBKR_CONFIG"]
        backend = runtime["get_ib_backend"](
            portfolio_id,
            live=bool(config.live_backend),
            account_id=str(config.account_id),
            starting_cash=float(portfolio.get("current_cash") or portfolio.get("starting_budget") or 100000.0),
        )
        return runtime["IBKRBrokerAdapter"](
            backend,
            portfolio_id,
            host=str(config.host),
            port=int(config.port),
            client_id=int(getattr(backend, "_client_id", config.client_id)),
        )
    return runtime["PaperBrokerAdapter"](portfolio_id)


def _get_broker_adapter_safe(runtime: dict[str, Any], portfolio_id: int) -> Any:
    pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="ibkr-adapter")
    try:
        future = pool.submit(_get_broker_adapter, runtime, portfolio_id)
        return future.result(timeout=_ADAPTER_TIMEOUT)
    except FuturesTimeoutError:
        logger.warning("IBKR adapter connection timed out for portfolio %s after %ss", portfolio_id, _ADAPTER_TIMEOUT)
        return None
    except Exception as exc:
        logger.warning("IBKR adapter connection failed for portfolio %s: %s", portfolio_id, exc)
        return None
    finally:
        pool.shutdown(wait=False, cancel_futures=True)


async def _get_broker_adapter_safe_async(runtime: dict[str, Any], portfolio_id: int) -> Any:
    return await asyncio.to_thread(_get_broker_adapter_safe, runtime, portfolio_id)


def _prune_jobs(now: dt.datetime | None = None) -> None:
    cutoff = (now or dt.datetime.now(dt.timezone.utc)) - dt.timedelta(seconds=_JOB_TTL_SECONDS)
    with _JOBS_LOCK:
        expired = [job_id for job_id, job in _JOBS.items() if job.created_at <= cutoff]
        for job_id in expired:
            _JOBS.pop(job_id, None)
    with _DISCOVERY_JOBS_LOCK:
        expired = [job_id for job_id, job in _DISCOVERY_JOBS.items() if job.created_at <= cutoff]
        for job_id in expired:
            _DISCOVERY_JOBS.pop(job_id, None)


def _serialize_job(job: RegimeJob) -> dict[str, Any]:
    return {
        "job_id": job.job_id,
        "status": job.status,
        "tickers": job.tickers,
        "benchmark": job.benchmark,
        "period": job.period,
        "progress": job.progress,
        "total": job.total,
        "payload": _json_ready(job.payload),
        "error": job.error,
        "created_at": job.created_at.isoformat(),
        "show_all": job.show_all,
        "frontier_enabled": job.frontier_enabled,
        "portfolio_scope": getattr(job, "portfolio_scope", "household"),
        "account_id": getattr(job, "account_id", None),
        "frontier_provider": getattr(job, "frontier_provider", "auto"),
        "force_refresh": bool(getattr(job, "force_refresh", False)),
        "progress_text": job.progress_text,
        "current_ticker": job.current_ticker,
        "eta_seconds": job.eta_seconds,
        "cache_hits": job.cache_hits,
        "cache_misses": job.cache_misses,
        "partial_results": _json_ready(job.partial_results or {}),
    }


def _serialize_discovery_job(job: DiscoveryJob) -> dict[str, Any]:
    return {
        "job_id": job.job_id,
        "status": job.status,
        "theme_ids": job.theme_ids,
        "progress": job.progress,
        "total": job.total,
        "current_theme": job.current_theme,
        "results": _json_ready(job.results),
        "error": job.error,
        "created_at": job.created_at.isoformat(),
        "frontier_provider": job.frontier_provider,
        "regenerate_supply_chain": bool(job.regenerate_supply_chain),
    }


async def _read_run_request(request: Request) -> dict[str, str]:
    raw = (await request.body()).decode("utf-8", errors="ignore")
    parsed = parse_qs(raw, keep_blank_values=True)
    return {key: values[-1] if values else "" for key, values in parsed.items()}


def _env_file_path() -> Path:
    return Path(__file__).resolve().parents[3] / ".env"


def _update_env_file(ibkr_vars: dict[str, str]) -> None:
    """Update IBKR variables in .env while preserving all other lines."""
    env_path = _env_file_path()
    existing_lines: list[str] = []
    existing_keys: set[str] = set()

    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if stripped and not stripped.startswith("#") and "=" in stripped:
                key = stripped.split("=", 1)[0].strip()
                if key in ibkr_vars:
                    existing_keys.add(key)
                    existing_lines.append(f"{key}={ibkr_vars[key]}")
                    continue
            existing_lines.append(line)

    for key, value in ibkr_vars.items():
        if key not in existing_keys:
            existing_lines.append(f"{key}={value}")

    env_path.write_text("\n".join(existing_lines) + "\n", encoding="utf-8")


def _identify_threshold_path(*, regime: str, transition_risk: float, technical_signal: str) -> str:
    """Return a human-readable explanation of which signal path was taken."""
    if regime == "Bear":
        if transition_risk < 0.05:
            return "Strong Sell path: transition_risk < 0.05 (persistent Bear)"
        if transition_risk < 0.15:
            return "Sell path: transition_risk < 0.15 (established Bear)"
        path = "Hold fallback: transition_risk >= 0.15 (mixed forward probabilities)"
        if technical_signal and "tactical bounce" in str(technical_signal).lower():
            path += " + Bear tactical override (Cover short / tactical bounce)"
        return path
    if regime == "Bull":
        if transition_risk < 0.05:
            return "Strong Buy path: transition_risk < 0.05 (persistent Bull)"
        if transition_risk < 0.15:
            return "Buy path: transition_risk < 0.15 (established Bull)"
        path = "Hold fallback: transition_risk >= 0.15 (mixed forward probabilities)"
        if technical_signal and "partial profits" in str(technical_signal).lower():
            path += " + Bull profit-taking override"
        return path
    return "Neutral regime: forward action based on tilt probability"


def _history_db_candidates() -> list[Path]:
    configured = os.getenv("HMM_DATA_DIR")
    roots = [Path(configured)] if configured else [Path("/tmp/hmm_data")]
    candidates: list[Path] = []
    for root in roots:
        candidates.append(root / "regime_watch.db")
        candidates.append(root / "hmm_data.db")
    return candidates


def _open_history_db(path: Path) -> sqlite3.Connection:
    uri = f"file:{path}?mode=ro&immutable=1"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _fetch_regime_change_history(tickers: list[str], days: int = 90) -> list[dict[str, Any]]:
    normalized = [str(ticker or "").strip().upper() for ticker in tickers if str(ticker or "").strip()]
    if not normalized:
        return []
    db_path = next((candidate for candidate in _history_db_candidates() if candidate.exists()), None)
    if db_path is None:
        return []
    placeholders = ",".join("?" for _ in normalized)
    cutoff = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=int(days))).isoformat()
    query = f"""
        SELECT ticker, previous_label, current_label, changed_at,
               {{price_at_change}},
               {{return_5d}},
               {{return_10d}},
               {{return_21d}}
        FROM regime_change_history
        WHERE ticker IN ({placeholders})
          AND changed_at >= ?
        ORDER BY changed_at DESC
    """
    try:
        with _open_history_db(db_path) as conn:
            columns = {str(row["name"]) for row in conn.execute("PRAGMA table_info(regime_change_history)").fetchall()}
            resolved_query = query.format(
                price_at_change="price_at_change" if "price_at_change" in columns else "NULL AS price_at_change",
                return_5d="return_5d" if "return_5d" in columns else "NULL AS return_5d",
                return_10d="return_10d" if "return_10d" in columns else "NULL AS return_10d",
                return_21d="return_21d" if "return_21d" in columns else "NULL AS return_21d",
            )
            rows = conn.execute(resolved_query, [*normalized, cutoff]).fetchall()
    except Exception as exc:
        logger.debug("Unable to load regime change history from persistence database.", exc_info=exc)
        return []
    return [dict(row) for row in rows]


def _fetch_recent_alerts(days: int = 7) -> list[dict[str, Any]]:
    db_path = next((candidate for candidate in _history_db_candidates() if candidate.exists()), None)
    if db_path is None:
        return []
    cutoff = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=int(days))).isoformat()
    try:
        with _open_history_db(db_path) as conn:
            columns = {str(row["name"]) for row in conn.execute("PRAGMA table_info(regime_change_history)").fetchall()}
            rows = conn.execute(
                f"""
                SELECT ticker, previous_label, current_label, changed_at,
                       {"price_at_change" if "price_at_change" in columns else "NULL AS price_at_change"},
                       {"return_5d" if "return_5d" in columns else "NULL AS return_5d"},
                       {"return_10d" if "return_10d" in columns else "NULL AS return_10d"},
                       {"return_21d" if "return_21d" in columns else "NULL AS return_21d"}
                FROM regime_change_history
                WHERE changed_at >= ?
                ORDER BY changed_at DESC
                """,
                (cutoff,),
            ).fetchall()
    except Exception as exc:
        logger.debug("Unable to load recent regime alerts.", exc_info=exc)
        return []
    return [dict(row) for row in rows]


def _sector_by_ticker(db_path: str | None, tickers: list[str]) -> dict[str, str]:
    if not db_path or not tickers:
        return {}
    normalized = sorted({str(ticker or "").strip().upper() for ticker in tickers if str(ticker or "").strip()})
    placeholders = ",".join("?" for _ in normalized)
    try:
        with _open_history_db(Path(db_path)) as conn:
            rows = conn.execute(
                f"""
                SELECT UPPER(COALESCE(s.ticker, tc.ticker)) AS ticker,
                       COALESCE(tc.sector, s.sector, 'Unknown') AS sector
                FROM securities s
                LEFT JOIN ticker_classification tc ON UPPER(tc.ticker) = UPPER(s.ticker)
                WHERE UPPER(s.ticker) IN ({placeholders})
                """,
                normalized,
            ).fetchall()
    except Exception as exc:
        logger.debug("Unable to load sector map from investor database.", exc_info=exc)
        return {}
    return {str(row["ticker"]).upper(): str(row["sector"] or "Unknown") for row in rows if row["ticker"]}


def _fit_regime_with_adaptive_window(runtime: dict[str, Any], *, ticker: str, market_frame: Any) -> Any:
    try:
        return runtime["fit_regime_model"](ticker=ticker, market_frame=market_frame)
    except Exception as exc:
        if "Insufficient history for walk-forward analysis" not in str(exc):
            raise
        row_count = len(market_frame) if hasattr(market_frame, "__len__") else 0
        adaptive_window = max(63, min(504, max(63, int(row_count) - 21)))
        if adaptive_window >= 504:
            raise
        logger.info("Retrying HMM fit for %s with adaptive training_window=%d", ticker, adaptive_window)
        return runtime["fit_regime_model"](
            ticker=ticker,
            market_frame=market_frame,
            training_window=adaptive_window,
        )


def _position_market_value(position: Any) -> float | None:
    for attr in ("market_value", "current_value"):
        value = getattr(position, attr, None)
        if value is None:
            continue
        try:
            return float(value)
        except Exception as exc:
            logger.debug("Unable to coerce position market value attribute %s.", attr, exc_info=exc)
            continue
    qty = getattr(position, "quantity", None)
    price = getattr(position, "current_price", None)
    if qty is not None and price is not None:
        try:
            return float(qty) * float(price)
        except Exception as exc:
            logger.debug("Unable to derive market value from quantity and price.", exc_info=exc)
            return None
    return None


def _lot_term_status(lot_details: list[dict[str, Any]]) -> tuple[str, int, int]:
    lot_count_st = sum(1 for lot in lot_details if str(lot.get("term") or "").upper() == "ST")
    lot_count_lt = sum(1 for lot in lot_details if str(lot.get("term") or "").upper() == "LT")
    if lot_count_st and lot_count_lt:
        return f"{lot_count_st} ST · {lot_count_lt} LT", lot_count_st, lot_count_lt
    if lot_count_lt:
        return "LT", 0, lot_count_lt
    if lot_count_st:
        return "ST", lot_count_st, 0
    return "—", 0, 0


def _compute_regime_exposure(rows: list[dict[str, Any]]) -> tuple[dict[str, float], float]:
    buckets = _default_exposure()
    total_market_value = 0.0
    weighted = False
    for row in rows:
        label = str(row.get("regime") or "Neutral")
        if label not in buckets:
            continue
        try:
            market_value = float(row.get("market_value") or 0.0)
        except Exception as exc:
            logger.debug("Unable to coerce market value while computing regime exposure.", exc_info=exc)
            market_value = 0.0
        if market_value > 0:
            buckets[label] += market_value
            total_market_value += market_value
            weighted = True
    if weighted and total_market_value > 0:
        return ({key: value / total_market_value for key, value in buckets.items()}, total_market_value)

    count_total = float(len(rows))
    if count_total <= 0:
        return buckets, 0.0
    for row in rows:
        label = str(row.get("regime") or "Neutral")
        if label in buckets:
            buckets[label] += 1.0 / count_total
    return buckets, 0.0


def _load_qualitative_result(
    runtime: dict[str, Any],
    *,
    ticker: str,
    state_id: int,
    regime_signal: str,
    state_name: str,
    latest_probability: float,
    benchmark: str,
    benchmark_state: str,
    frontier_provider: str,
    frontier_enabled: bool,
    force_refresh: bool,
    meta_labeler_score: float | None = None,
) -> tuple[dict[str, Any] | None, bool]:
    save_regime_event_fn = runtime.get("save_regime_event")
    previous_event = (
        save_regime_event_fn(ticker, state_name, state_id)
        if callable(save_regime_event_fn)
        else {"previous_label": None}
    )
    if not frontier_enabled:
        return None, False
    provider_key = str(frontier_provider or "auto").strip().lower() or "auto"
    if not force_refresh:
        cached = load_qualitative_cache(ticker, provider=provider_key)
        if cached is not None:
            return cached, False
    qualitative_obj = runtime["build_qualitative_assessment"](
        ticker=ticker,
        regime_signal=regime_signal,
        state_name=state_name,
        latest_probability=latest_probability,
        context_symbols=[benchmark, "SPY", "^TNX"],
        frontier_enabled=True,
        frontier_provider=frontier_provider,
        initial_thesis=_build_theme_context(ticker, runtime),
        previous_label=(previous_event or {}).get("previous_label"),
        benchmark_state=benchmark_state,
        meta_labeler_score=meta_labeler_score,
    )
    qualitative = _json_ready(qualitative_obj)
    save_qualitative_cache(ticker, provider=provider_key, data=qualitative)
    return qualitative, True


def _refresh_transition_outcomes(runtime: dict[str, Any], investor_db_path: str) -> None:
    pending_transition_fn = runtime.get("get_pending_transition_outcomes")
    update_transition_outcome_fn = runtime.get("update_transition_outcome")
    get_latest_prices_fn = runtime.get("get_latest_prices")
    if not (
        callable(pending_transition_fn)
        and callable(update_transition_outcome_fn)
        and callable(get_latest_prices_fn)
    ):
        return
    try:
        pending_rows = pending_transition_fn()
        pending_tickers = sorted(
            {
                str(row.get("ticker") or "").upper()
                for row in pending_rows
                if str(row.get("ticker") or "").strip()
            }
        )
        latest_prices = get_latest_prices_fn(investor_db_path, pending_tickers) if pending_tickers else {}
        for row in pending_rows:
            base_price = row.get("price_at_change")
            current_price = latest_prices.get(str(row.get("ticker") or "").upper())
            if base_price in (None, 0, 0.0) or current_price is None:
                continue
            try:
                realized = (float(current_price) - float(base_price)) / float(base_price)
            except Exception as exc:
                logger.debug("Unable to compute transition outcome return for %s.", row.get("ticker"), exc_info=exc)
                continue
            update_transition_outcome_fn(
                int(row["id"]),
                return_5d=realized,
                return_10d=realized,
                return_21d=realized,
            )
    except Exception as exc:
        logger.warning("Unable to refresh transition journal outcomes.", exc_info=exc)


def _progress_event(
    progress_callback: Any | None,
    *,
    progress: int,
    total: int,
    ticker: str | None,
    stage: str,
    text: str,
    eta_seconds: float | None = None,
    cache_hits: int = 0,
    cache_misses: int = 0,
    partial_result: dict[str, Any] | None = None,
) -> None:
    if callable(progress_callback):
        progress_callback(
            progress,
            total,
            ticker,
            stage=stage,
            text=text,
            eta_seconds=eta_seconds,
            cache_hits=cache_hits,
            cache_misses=cache_misses,
            partial_result=partial_result,
        )


def _build_regime_dashboard_payload(
    *,
    session: Session | None = None,
    benchmark: str = "SOXX",
    period: str = "3y",
    show_all: bool = False,
    portfolio_scope: str = "household",
    account_id: int | None = None,
    tickers: list[str] | None = None,
    frontier_enabled: bool = False,
    frontier_provider: str = "auto",
    frontier_batch_size: int = _DEFAULT_FRONTIER_BATCH_SIZE,
    force_refresh: bool = False,
    progress_callback: Any | None = None,
) -> dict[str, Any]:
    runtime, runtime_error = _load_hmm_runtime()
    payload = _empty_dashboard_payload(
        benchmark=benchmark,
        period=period,
        show_all=show_all,
        runtime=runtime,
        runtime_error=runtime_error,
    )
    payload["frontier_enabled"] = frontier_enabled
    payload["frontier_provider"] = frontier_provider
    payload["force_refresh"] = force_refresh
    payload["portfolio_scope"] = portfolio_scope
    payload["account_id"] = account_id
    if runtime is None:
        return payload
    previous_payload = load_previous_payload()
    payload["themes"] = _load_themes(runtime)
    registry = runtime["get_registry"]() if callable(runtime.get("get_registry")) else None
    get_setting_fn = runtime.get("get_setting")
    get_setting = get_setting_fn if callable(get_setting_fn) else None
    active_version_raw = get_setting("meta_labeler_active_version") if get_setting else None
    meta_labeler_engine = registry.get("xgboost_meta_labeler") if registry else None
    meta_labeler_active = bool(
        meta_labeler_engine is not None
        and callable(getattr(meta_labeler_engine, "is_ready", None))
        and meta_labeler_engine.is_ready()
    )
    payload["ensemble_status"] = {
        "meta_labeler_active": meta_labeler_active,
        "meta_labeler_name": getattr(meta_labeler_engine, "name", "xgboost_meta_labeler") if meta_labeler_engine else "xgboost_meta_labeler",
        "meta_labeler_version": int(active_version_raw) if active_version_raw else None,
    }

    investor_db_path, available_tickers = _portfolio_tickers(runtime, session, show_all, portfolio_scope, account_id)
    _refresh_transition_outcomes(runtime, investor_db_path)
    available_ticker_set = {str(symbol).upper() for symbol in available_tickers}
    pending_outcomes_fn = runtime.get("get_pending_outcomes")
    update_signal_outcome_fn = runtime.get("update_signal_outcome")
    get_latest_prices_fn = runtime.get("get_latest_prices")
    if callable(pending_outcomes_fn) and callable(update_signal_outcome_fn) and callable(get_latest_prices_fn):
        try:
            pending_rows = pending_outcomes_fn()
            pending_tickers = sorted({str(row.get("ticker") or "").upper() for row in pending_rows if str(row.get("ticker") or "").strip()})
            latest_prices = get_latest_prices_fn(investor_db_path, pending_tickers) if pending_tickers else {}
            for row in pending_rows:
                ticker_key = str(row.get("ticker") or "").upper()
                current_price = latest_prices.get(ticker_key)
                if current_price is not None:
                    update_signal_outcome_fn(int(row["id"]), str(row["interval"]), float(current_price))
        except Exception as exc:
            logger.warning("Unable to refresh historical signal outcomes.", exc_info=exc)
    calibrator = None
    get_calibration_data_fn = runtime.get("get_calibration_data")
    fit_probability_calibrator_fn = runtime.get("fit_probability_calibrator")
    if callable(get_calibration_data_fn) and callable(fit_probability_calibrator_fn):
        try:
            calibration_rows = get_calibration_data_fn(lookback_days=365)
            predicted_probs: list[float] = []
            actual_outcomes: list[float] = []
            for row in calibration_rows:
                probability = row.get("regime_probability")
                realized = row.get("hit_1m")
                if probability is None or realized is None:
                    continue
                predicted_probs.append(float(probability))
                actual_outcomes.append(float(realized))
            if len(predicted_probs) >= 20:
                calibrator = fit_probability_calibrator_fn(predicted_probs, actual_outcomes)
        except Exception as exc:
            logger.debug("Unable to fit probability calibrator for current dashboard run.", exc_info=exc)
    selected_tickers = list(tickers or available_tickers)
    payload["portfolio_count"] = len(available_tickers)
    payload["selected_tickers"] = selected_tickers
    payload["selected_count"] = len(selected_tickers)
    if not selected_tickers:
        payload["warnings"].append("No portfolio tickers are available for regime analysis.")
        return payload

    relevant_tickers = sorted({*selected_tickers, benchmark})
    positions = runtime["get_portfolio_positions"](investor_db_path, relevant_tickers, account_id=account_id)
    positions_by_account = runtime["positions_by_ticker_and_account"](positions)
    tax_assumptions = runtime["get_tax_assumptions"](investor_db_path)
    current_prices_by_ticker: dict[str, float] = {}
    for ticker_key, position_entries in positions_by_account.items():
        if not position_entries:
            continue
        try:
            current_prices_by_ticker[ticker_key.upper()] = float(getattr(position_entries[0], "current_price", 0.0) or 0.0)
        except Exception as exc:
            logger.debug("Unable to derive current price for ORM lot query.", exc_info=exc)
    lot_details_by_ticker = (
        get_lot_details_by_scope(
            session,
            tickers=selected_tickers,
            scope=portfolio_scope,
            account_id=account_id,
            current_prices=current_prices_by_ticker,
        )
        if session is not None
        else {ticker.upper(): [] for ticker in selected_tickers}
    )

    try:
        benchmark_market = runtime["download_market_frame"](ticker=benchmark, period=period, interval="1d").frame
        benchmark_regime = _fit_regime_with_adaptive_window(runtime, ticker=benchmark, market_frame=benchmark_market)
        payload["benchmark_regime"] = benchmark_regime.latest_label
        payload["benchmark_regime_tone"] = _kpi_tone_for_regime(benchmark_regime.latest_label)
        payload["frontier_model"] = runtime["configured_frontier_model"](frontier_provider)
    except Exception as exc:
        logger.warning("Unable to build benchmark regime data for %s.", benchmark, exc_info=exc)
        payload["warnings"].append(f"{benchmark}: unable to build benchmark regime data ({exc}).")
        return payload

    try:
        digest = runtime["generate_weekly_digest"](
            tickers=selected_tickers,
            benchmark=benchmark,
            investor_db_path=investor_db_path,
            persist=False,
        )
        payload["digest"] = _json_ready(digest)
        payload["action_items_count"] = len(digest.action_items)
    except Exception as exc:
        logger.warning("Unable to build weekly digest for current regime run.", exc_info=exc)
        payload["warnings"].append(f"Weekly digest is unavailable for this run ({exc}).")

    total = len(selected_tickers)
    if frontier_enabled and total > 20:
        payload["warnings"].append(
            f"Analyzing {total} tickers with Frontier may take several minutes. Cached results will be reused where available."
        )

    fitted_rows: list[dict[str, Any]] = []
    fit_started = time.monotonic()
    snapshots_saved_count = 0
    for index, ticker in enumerate(selected_tickers, start=1):
        elapsed = time.monotonic() - fit_started
        eta_seconds = (elapsed / max(1, index - 1)) * (total - (index - 1)) if index > 1 else None
        _progress_event(
            progress_callback,
            progress=index - 1,
            total=total,
            ticker=ticker,
            stage="fit",
            text=f"Analyzing ticker {index} of {total}: {ticker}",
            eta_seconds=eta_seconds,
        )
        try:
            market_frame = runtime["download_market_frame"](ticker=ticker, period=period, interval="1d").frame
            regime = _fit_regime_with_adaptive_window(runtime, ticker=ticker, market_frame=market_frame)
            fit_regime_model_weekly_fn = runtime.get("fit_regime_model_weekly")
            if callable(fit_regime_model_weekly_fn):
                try:
                    weekly_regime = fit_regime_model_weekly_fn(ticker=ticker, market_frame=market_frame)
                except Exception as exc:
                    logger.warning("Unable to build weekly regime overlay for %s; falling back to daily regime.", ticker)
                    logger.debug("Weekly regime overlay failed for %s.", ticker, exc_info=exc)
                    weekly_regime = regime
            else:
                weekly_regime = regime
        except Exception as exc:
            logger.warning("Unable to analyze regime for %s.", ticker, exc_info=exc)
            payload["warnings"].append(f"{ticker}: unable to analyze holding ({exc})")
            _progress_event(
                progress_callback,
                progress=index,
                total=total,
                ticker=ticker,
                stage="fit",
                text=f"Skipped {ticker}: analysis failed",
                eta_seconds=eta_seconds,
            )
            continue

        forward_curve = runtime["forward_regime_curve"](
            regime.transition_matrix,
            regime.latest_state_vector,
            horizon=21,
        )
        earnings_date = runtime["get_next_earnings_date"](ticker) if callable(runtime.get("get_next_earnings_date")) else None
        try:
            forward_signal = runtime["signal_from_forward_curve"](
                forward_curve,
                regime.latest_label,
                regime.transition_risk,
                regime.expected_regime_duration,
                regime.latest_probability,
                earnings_date,
            )
        except TypeError:
            forward_signal = runtime["signal_from_forward_curve"](
                forward_curve,
                regime.latest_label,
                regime.transition_risk,
                regime.expected_regime_duration,
                regime.latest_probability,
            )
        technicals = runtime["compute_technicals"](
            market_frame["price"],
            market_frame["volume"],
            market_frame["high"] if "high" in market_frame.columns else None,
            market_frame["low"] if "low" in market_frame.columns else None,
        )
        technical_signal = runtime["intra_regime_signal"](technicals, regime.latest_label)
        composite_signal = runtime["build_composite_signal"](
            regime.latest_label,
            regime.latest_probability,
            forward_signal,
            technical_signal,
        )
        multi_timeframe_signal_fn = runtime.get("multi_timeframe_signal")
        multi_tf_note = (
            multi_timeframe_signal_fn(regime.latest_label, weekly_regime.latest_label)
            if callable(multi_timeframe_signal_fn)
            else (
                "Aligned"
                if regime.latest_label == weekly_regime.latest_label
                else f"Daily {regime.latest_label} vs Weekly {weekly_regime.latest_label}"
            )
        )
        composite_signal.weekly_regime = weekly_regime.latest_label
        composite_signal.multi_timeframe_note = multi_tf_note
        earnings_note = runtime["earnings_warning"](earnings_date) if callable(runtime.get("earnings_warning")) else None
        price_targets = None
        price_targets_error = None
        compute_price_targets_fn = runtime.get("compute_price_targets")
        if callable(compute_price_targets_fn):
            try:
                price_targets = compute_price_targets_fn(
                    current_price=float(getattr(regime, "latest_price", 0.0) or 0.0),
                    technicals_df=technicals,
                    composite_signal=composite_signal,
                    expected_duration=float(regime.expected_regime_duration),
                    state_mean_return=float(getattr(regime, "recent_state_mean_return", 0.0) or 0.0),
                )
            except Exception as exc:
                price_targets_error = str(exc) or exc.__class__.__name__
                logger.warning("Unable to compute price targets for %s.", ticker, exc_info=exc)
        apply_signal_context_fn = runtime.get("apply_signal_context")
        if callable(apply_signal_context_fn):
            try:
                composite_signal = apply_signal_context_fn(
                    composite_signal,
                    price_targets=price_targets,
                    earnings_warning_text=earnings_note,
                )
            except Exception as exc:
                logger.debug("Unable to apply signal context for %s.", ticker, exc_info=exc)
        unified_confidence = None
        compute_unified_confidence_fn = runtime.get("compute_unified_confidence")
        if callable(compute_unified_confidence_fn):
            try:
                unified_confidence = compute_unified_confidence_fn(
                    float(regime.latest_probability),
                    float(getattr(composite_signal, "composite_strength", 0.0) or 0.0),
                    calibrator=calibrator,
                )
            except Exception as exc:
                logger.debug("Unable to compute unified confidence for %s.", ticker, exc_info=exc)
        confidence = runtime["confidence_trajectory"](regime.price_frame["state_probability"], window=10)
        sentiment_info, sentiment_history = runtime["sentiment_momentum"](ticker, regime.latest_label)
        account_positions = positions_by_account.get(ticker.upper(), [])
        market_value = sum(
            value for value in (_position_market_value(position) for position in account_positions) if value is not None
        )
        wash_sale_risk = runtime["get_wash_sale_risk"](investor_db_path, ticker)
        account_tax_signals = (
            runtime["tax_adjusted_signals"](
                composite_signal,
                account_positions,
                tax_assumptions,
                wash_sale_risk=wash_sale_risk,
            )
            if account_positions
            else []
        )
        theme_membership: list[dict[str, Any]] = []
        get_ticker_themes_fn = runtime.get("get_ticker_themes")
        if callable(get_ticker_themes_fn):
            try:
                theme_membership = _json_ready(get_ticker_themes_fn(ticker))
            except Exception as exc:
                logger.debug("Unable to load theme membership for %s.", ticker, exc_info=exc)
        theme_target_price = next((item.get("target_price") for item in theme_membership if item.get("target_price") is not None), None)
        theme_stop_price = next((item.get("stop_price") for item in theme_membership if item.get("stop_price") is not None), None)
        material_tax_signals = [signal for signal in account_tax_signals if _has_material_tax_adjustment(signal)]
        primary_tax_signal = _primary_tax_signal(material_tax_signals)
        display_tax_signal = primary_tax_signal or _primary_tax_signal(account_tax_signals)
        lot_details = _json_ready(lot_details_by_ticker.get(ticker.upper(), []))
        for lot in lot_details:
            if lot.get("unrealized_gain") is None and getattr(regime, "latest_price", None) is not None:
                try:
                    qty = float(lot.get("qty") or 0.0)
                    basis_total = float(lot.get("cost_basis") or lot.get("basis_total") or 0.0)
                    latest_price = float(regime.latest_price)
                    lot["market_value"] = latest_price * qty if qty else None
                    lot["unrealized_gain"] = (latest_price - (basis_total / qty if qty else 0.0)) * qty if qty else None
                except Exception as exc:
                    logger.debug("Unable to backfill unrealized gain for lot detail on %s.", ticker, exc_info=exc)
        tax_status, lot_count_st, lot_count_lt = _lot_term_status(lot_details)
        duration_context = None
        historical_duration_fn = runtime.get("get_historical_regime_durations")
        duration_accuracy_fn = runtime.get("duration_accuracy")
        if callable(historical_duration_fn) and callable(duration_accuracy_fn):
            try:
                duration_context = duration_accuracy_fn(
                    float(regime.expected_regime_duration),
                    historical_duration_fn(ticker),
                    str(regime.latest_label),
                )
            except Exception as exc:
                logger.debug("Unable to compute duration accuracy for %s.", ticker, exc_info=exc)
        confidence_points = [
            {"day": idx + 1, "probability": float(val)}
            for idx, val in enumerate(regime.price_frame["state_probability"].tail(21).tolist())
        ]
        fitted_rows.append(
            {
                "ticker": ticker,
                "regime_obj": regime,
                "market_frame": market_frame,
                "forward_curve": forward_curve,
                "forward_signal": forward_signal,
                "technicals": technicals,
                "technical_signal": technical_signal,
                "composite_signal": composite_signal,
                "price_targets": price_targets,
                "price_targets_error": price_targets_error,
                "earnings_date": earnings_date.isoformat() if earnings_date else None,
                "earnings_warning": earnings_note,
                "confidence": confidence,
                "sentiment_info": sentiment_info,
                "sentiment_history": sentiment_history,
                "account_positions": account_positions,
                "market_value": float(market_value) if market_value > 0 else None,
                "material_tax_signals": material_tax_signals,
                "primary_tax_signal": primary_tax_signal,
                "display_tax_signal": display_tax_signal,
                "lot_details": lot_details,
                "open_lot_count": len(lot_details),
                "tax_status": tax_status if tax_status != "—" else (getattr(display_tax_signal, "tax_status", None) or "—"),
                "lot_count_st": lot_count_st,
                "lot_count_lt": lot_count_lt,
                "confidence_points": confidence_points,
                "duration_accuracy": duration_context,
                "unified_confidence": _json_ready(unified_confidence),
                "theme_membership": theme_membership,
                "theme_target_price": theme_target_price,
                "theme_stop_price": theme_stop_price,
            }
        )
        save_signal_snapshot_fn = runtime.get("save_signal_snapshot")
        if callable(save_signal_snapshot_fn) and price_targets is not None:
            try:
                save_signal_snapshot_fn(
                    ticker=ticker,
                    snapshot_date=dt.date.today().isoformat(),
                    action=str(composite_signal.composite_action),
                    regime_label=str(regime.latest_label),
                    regime_probability=float(regime.latest_probability),
                    composite_strength=float(composite_signal.composite_strength),
                    benchmark=benchmark,
                    current_price=float(getattr(regime, "latest_price", 0.0) or 0.0),
                    entry_price=getattr(price_targets, "entry_price", None),
                    exit_price=getattr(price_targets, "exit_price", None),
                    stop_price=getattr(price_targets, "stop_price", None),
                    risk_reward_ratio=getattr(price_targets, "risk_reward_ratio", None),
                    timeframe_days=int(getattr(price_targets, "timeframe_days", 0) or 0),
                )
                snapshots_saved_count += 1
            except Exception as exc:
                logger.warning("Unable to persist signal snapshot for %s.", ticker, exc_info=exc)

    cache_hits = 0
    cache_misses = 0
    save_sentiment_fn = runtime.get("save_sentiment")
    save_regime_event_fn = runtime.get("save_regime_event")
    save_regime_change_with_price_fn = runtime.get("save_regime_change_with_price")
    runtime_sector_map_fn = runtime.get("get_sector_map")
    sector_map = runtime_sector_map_fn(investor_db_path, selected_tickers) if callable(runtime_sector_map_fn) else _sector_by_ticker(investor_db_path, selected_tickers)
    payload["regime_history"] = _fetch_regime_change_history(selected_tickers, days=90)
    compute_return_correlations_fn = runtime.get("compute_return_correlations")
    concentration_adjusted_strength_fn = runtime.get("concentration_adjusted_strength")
    if callable(compute_return_correlations_fn) and callable(concentration_adjusted_strength_fn) and fitted_rows:
        try:
            market_frames = {str(item["ticker"]).upper(): item["market_frame"] for item in fitted_rows}
            correlations = compute_return_correlations_fn(market_frames)
            regime_map = {str(item["ticker"]).upper(): str(item["regime_obj"].latest_label) for item in fitted_rows}
            portfolio_tickers = list(regime_map.keys())
            for item in fitted_rows:
                ticker_key = str(item["ticker"]).upper()
                original_strength = float(getattr(item["composite_signal"], "composite_strength", 0.0) or 0.0)
                adjusted_strength, concentration_warning, concentration_penalty = concentration_adjusted_strength_fn(
                    ticker_key,
                    original_strength,
                    str(item["regime_obj"].latest_label),
                    str(sector_map.get(ticker_key, "Unknown")),
                    portfolio_tickers,
                    correlations,
                    sector_map,
                    regime_map,
                )
                item["unadjusted_strength"] = original_strength
                item["concentration_warning"] = concentration_warning
                item["concentration_penalty"] = concentration_penalty
                item["composite_signal"].composite_strength = adjusted_strength
        except Exception as exc:
            logger.warning("Unable to compute concentration-adjusted signal strengths.", exc_info=exc)
    should_retrain_fn = runtime.get("should_retrain")
    get_training_history_fn = runtime.get("get_training_history")
    get_next_version_fn = runtime.get("get_next_version")
    version_path_fn = runtime.get("_version_path")
    log_training_run_fn = runtime.get("log_training_run")
    update_training_status_fn = runtime.get("update_training_status")
    set_setting_fn = get_setting_fn if callable(get_setting_fn) else None
    if (
        get_setting
        and callable(should_retrain_fn)
        and callable(get_training_history_fn)
        and callable(get_next_version_fn)
        and callable(version_path_fn)
        and callable(log_training_run_fn)
        and callable(set_setting_fn)
        and str(get_setting("meta_labeler_auto_retrain") or "").lower() in {"true", "1", "yes"}
    ):
        retrain_ticker = str(get_setting("meta_labeler_retrain_ticker") or "").strip().upper()
        retrain_period = str(get_setting("meta_labeler_retrain_period") or "3y")
        retrain_day = str(get_setting("meta_labeler_retrain_day") or "Sunday")
        if retrain_ticker:
            history = get_training_history_fn(limit=1)
            last_trained = history[0]["trained_at"] if history else None
            if should_retrain_fn(last_trained, retrain_day):
                try:
                    logger.info("Auto-retrain triggered for meta-labeler on %s", retrain_ticker)
                    market_series = runtime["download_market_frame"](ticker=retrain_ticker, period=retrain_period)
                    market_frame = getattr(market_series, "frame", market_series)
                    regime_result = runtime["fit_regime_model"](ticker=retrain_ticker, market_frame=market_frame)
                    labeled_frame = runtime["build_labeled_frame"](retrain_ticker, market_frame, regime_result)
                    registry = runtime["get_registry"]()
                    engine = registry.get("xgboost_meta_labeler")
                    if engine is None:
                        engine = runtime["create_and_register_meta_labeler"](_meta_labeler_config_from_runtime(runtime))
                    metrics = engine.train(labeled_frame)
                    if engine.is_ready():
                        active_str = get_setting("meta_labeler_active_version")
                        if active_str and callable(update_training_status_fn):
                            try:
                                update_training_status_fn(int(active_str), "superseded")
                            except Exception:
                                pass
                        new_version = get_next_version_fn()
                        model_path = version_path_fn(new_version)
                        engine.save_model(model_path)
                        log_training_run_fn(
                            version=new_version,
                            ticker=retrain_ticker,
                            model_path=model_path,
                            metrics=metrics,
                            notes="auto-retrain",
                        )
                        set_setting_fn("meta_labeler_active_version", str(new_version))
                        payload["ensemble_status"]["meta_labeler_version"] = new_version
                        payload["ensemble_status"]["meta_labeler_active"] = True
                        logger.info("Auto-retrain complete: meta-labeler v%d saved", new_version)
                except Exception as exc:
                    logger.warning("Auto-retrain failed: %s", exc, exc_info=True)
    meta_scores: dict[str, float] = {}
    meta_results: dict[str, Any] = {}
    if meta_labeler_active and callable(runtime.get("extract_meta_features")):
        for item in fitted_rows:
            try:
                ticker_key = str(item["ticker"]).upper()
                features = runtime["extract_meta_features"](item["regime_obj"].price_frame.iloc[-1])
                ml_result = meta_labeler_engine.analyze(
                    ticker=ticker_key,
                    features=features,
                    regime_result=item["regime_obj"],
                )
                meta_results[ticker_key] = ml_result
                meta_scores[ticker_key] = float(getattr(ml_result, "confidence", 0.0) or 0.0)
            except Exception as exc:
                logger.debug("Unable to compute meta-labeler score for %s.", item.get("ticker"), exc_info=exc)
    frontier_batch_size = max(1, int(frontier_batch_size or _DEFAULT_FRONTIER_BATCH_SIZE))

    def load_frontier(item: dict[str, Any]) -> tuple[str, dict[str, Any] | None, bool]:
        regime = item["regime_obj"]
        qualitative, fresh = _load_qualitative_result(
            runtime,
            ticker=item["ticker"],
            state_id=int(regime.latest_state_id),
            regime_signal=regime.regime_signal,
            state_name=regime.latest_label,
            latest_probability=float(regime.latest_probability),
            benchmark=benchmark,
            benchmark_state=payload["benchmark_regime"],
            frontier_provider=frontier_provider,
            frontier_enabled=frontier_enabled,
            force_refresh=force_refresh,
            meta_labeler_score=meta_scores.get(str(item["ticker"]).upper()),
        )
        return item["ticker"], qualitative, fresh

    frontier_results: dict[str, tuple[dict[str, Any] | None, bool]] = {}
    if frontier_enabled:
        for start in range(0, len(fitted_rows), frontier_batch_size):
            batch = fitted_rows[start : start + frontier_batch_size]
            with ThreadPoolExecutor(max_workers=min(frontier_batch_size, len(batch))) as batch_executor:
                future_map = {batch_executor.submit(load_frontier, item): item for item in batch}
                for future in as_completed(future_map):
                    item = future_map[future]
                    ticker = item["ticker"]
                    try:
                        _, qualitative, fresh = future.result()
                        frontier_results[ticker] = (qualitative, fresh)
                        if fresh:
                            cache_misses += 1
                            if callable(save_sentiment_fn) and qualitative:
                                save_sentiment_fn(
                                    ticker,
                                    int((qualitative.get("sentiment_score") or 0)),
                                    str(qualitative.get("catalyst_sentiment") or "Neutral"),
                                    len(qualitative.get("catalysts") or []),
                                )
                        else:
                            cache_hits += 1
                        _progress_event(
                            progress_callback,
                            progress=len(frontier_results),
                            total=len(fitted_rows),
                            ticker=ticker,
                            stage="frontier",
                            text=f"Analyzing ticker {start + len(frontier_results)} of {len(fitted_rows)}: {ticker} ({'running Frontier' if fresh else 'cached'})",
                            cache_hits=cache_hits,
                            cache_misses=cache_misses,
                        )
                    except Exception as exc:
                        logger.warning("Frontier analysis failed for %s.", ticker, exc_info=exc)
                        payload["warnings"].append(f"{ticker}: frontier analysis unavailable ({exc})")
                        frontier_results[ticker] = (None, False)
    else:
        for item in fitted_rows:
            if callable(save_regime_event_fn):
                previous_event = save_regime_event_fn(item["ticker"], item["regime_obj"].latest_label, int(item["regime_obj"].latest_state_id))
                previous_label = (previous_event or {}).get("previous_label")
                if previous_label and previous_label != item["regime_obj"].latest_label and callable(save_regime_change_with_price_fn):
                    try:
                        save_regime_change_with_price_fn(
                            item["ticker"],
                            previous_label,
                            item["regime_obj"].latest_label,
                            int(item["regime_obj"].latest_state_id),
                            float(getattr(item["regime_obj"], "latest_price", 0.0) or 0.0),
                        )
                    except Exception as exc:
                        logger.debug("Unable to persist explicit regime change history for %s.", item["ticker"], exc_info=exc)

    total_selected_market_value = sum(
        float(item["market_value"])
        for item in fitted_rows
        if item.get("market_value") is not None
    ) or None

    for index, item in enumerate(fitted_rows, start=1):
        ticker = item["ticker"]
        regime = item["regime_obj"]
        qualitative, _fresh = frontier_results.get(ticker, (None, False))
        ticker_key = str(ticker).upper()
        ml_result = meta_results.get(ticker_key)
        meta_prob = meta_scores.get(ticker_key)
        frontier_panel = _frontier_panel(
            qualitative=qualitative,
            label=regime.latest_label,
            probability=float(regime.latest_probability),
            regime_days=int(regime.regime_days),
            model_name=payload.get("frontier_model"),
        )
        ai_verdict = _extract_ai_verdict(frontier_panel)
        derived_action = _verdict_to_action(ai_verdict)
        divergence_info = None
        divergence_severity_fn = runtime.get("divergence_severity")
        weekly_label = getattr(item["composite_signal"], "weekly_regime", None) or item["regime_obj"].latest_label
        if callable(divergence_severity_fn) and weekly_label != item["regime_obj"].latest_label:
            try:
                divergence_info = divergence_severity_fn(
                    daily_label=item["regime_obj"].latest_label,
                    weekly_label=weekly_label,
                    regime_history=payload.get("regime_history", []),
                    ticker=ticker,
                )
            except Exception as exc:
                logger.debug("Unable to compute divergence severity for %s.", ticker, exc_info=exc)
        signal_diagnostics = {
            "forward_action": item["forward_signal"].action,
            "forward_strength": round(float(getattr(item["forward_signal"], "strength", 0.0) or 0.0), 3),
            "forward_transition_risk": round(float(getattr(item["forward_signal"], "transition_risk", 0.0)), 3),
            "forward_expected_duration": round(float(getattr(item["forward_signal"], "expected_duration", 0.0)), 1),
            "technical_signal": item["technical_signal"],
            "composite_action": item["composite_signal"].composite_action,
            "composite_strength": round(float(getattr(item["composite_signal"], "composite_strength", 0.0) or 0.0), 3),
            "regime": regime.latest_label,
            "probability": round(float(regime.latest_probability), 4),
            "regime_days": int(regime.regime_days),
            "weekly_regime": getattr(item["composite_signal"], "weekly_regime", None),
            "multi_timeframe_note": getattr(item["composite_signal"], "multi_timeframe_note", None),
            "meta_labeler_probability": round(float(meta_prob), 4) if meta_prob is not None else None,
            "meta_labeler_signal": str(getattr(ml_result, "signal", "") or "") if ml_result is not None else None,
            "meta_labeler_details": _json_ready(getattr(ml_result, "details", {}) or {}) if ml_result is not None else None,
            "thresholds_applied": _identify_threshold_path(
                regime=regime.latest_label,
                transition_risk=float(getattr(item["forward_signal"], "transition_risk", 0.0)),
                technical_signal=item["technical_signal"],
            ),
        }
        payload["rows"].append(
            {
                "ticker": ticker,
                "regime": regime.latest_label,
                "state_id": int(regime.latest_state_id),
                "regime_class": _regime_class(regime.latest_label),
                "probability": float(regime.latest_probability),
                "probability_pct": float(regime.latest_probability * 100.0),
                "composite_signal": item["composite_signal"].composite_action,
                "composite_signal_class": _signal_class(item["composite_signal"].composite_action),
                "weekly_regime": getattr(item["composite_signal"], "weekly_regime", None) or item["regime_obj"].latest_label,
                "multi_timeframe_note": getattr(item["composite_signal"], "multi_timeframe_note", None),
                "multi_timeframe_aligned": (getattr(item["composite_signal"], "weekly_regime", None) or item["regime_obj"].latest_label) == item["regime_obj"].latest_label,
                "forward_signal": item["forward_signal"].action,
                "forward_signal_class": _signal_class(item["forward_signal"].action),
                "technical_signal": item["technical_signal"],
                "price_targets": _json_ready(item["price_targets"]),
                "price_targets_error": item.get("price_targets_error"),
                "current_price": float(getattr(regime, "latest_price", 0.0) or 0.0),
                "action": item["primary_tax_signal"].adjusted_action if item["primary_tax_signal"] else (derived_action or "—"),
                "action_class": _signal_class(item["primary_tax_signal"].adjusted_action) if item["primary_tax_signal"] else _signal_class(derived_action or ""),
                "tax_status": item["tax_status"],
                "lot_count_st": item["lot_count_st"],
                "lot_count_lt": item["lot_count_lt"],
                "tax_note": item["primary_tax_signal"].tax_note if item["primary_tax_signal"] else "—",
                "days_in_regime": int(regime.regime_days),
                "market_value": item["market_value"],
                "transition_risk_pct": float(regime.transition_risk * 100.0),
                "expected_duration": float(regime.expected_regime_duration),
                "confidence_trend": item["confidence"].trend,
                "sentiment_trend": item["sentiment_info"].trend,
                "sentiment_class": _signal_class("Buy" if str(item["sentiment_info"].trend).lower() == "improving" else "Sell" if str(item["sentiment_info"].trend).lower() == "declining" else "Hold"),
                "account_tax_signals": _json_ready(item["material_tax_signals"]),
                "account_tax_count": len(item["material_tax_signals"]),
                "lot_details": item["lot_details"],
                "open_lot_count": item.get("open_lot_count", len(item["lot_details"])),
                "is_portfolio_holding": ticker.upper() in available_ticker_set,
                "theme_membership": item.get("theme_membership", []),
                "theme_target_price": item.get("theme_target_price"),
                "theme_stop_price": item.get("theme_stop_price"),
                "qualitative": qualitative,
                "math": _math_panel(regime),
                "frontier": frontier_panel,
                "ai_verdict": ai_verdict,
                "relative_strength": _relative_strength_text(regime.latest_label, payload["benchmark_regime"]),
                "relative_strength_class": (
                    "cell-ok" if _relative_strength_text(regime.latest_label, payload["benchmark_regime"]) == "Outperforming"
                    else "cell-bad" if _relative_strength_text(regime.latest_label, payload["benchmark_regime"]) == "Lagging"
                    else ""
                ),
                "concentration_warning": item.get("concentration_warning"),
                "concentration_penalty": item.get("concentration_penalty", 0.0),
                "unadjusted_strength": item.get("unadjusted_strength"),
                "divergence_severity": _json_ready(divergence_info),
                "earnings_date": item.get("earnings_date"),
                "earnings_warning": item.get("earnings_warning"),
                "risk_reward_conflict": getattr(item["composite_signal"], "risk_reward_conflict", False),
                "risk_reward_warning": getattr(item["composite_signal"], "risk_reward_warning", None),
                "duration_accuracy": item.get("duration_accuracy"),
                "unified_confidence": item.get("unified_confidence"),
                "meta_labeler_probability": meta_prob,
                "meta_labeler_signal": str(getattr(ml_result, "signal", "") or "") if ml_result is not None else None,
                "meta_labeler_details": _json_ready(getattr(ml_result, "details", {}) or {}) if ml_result is not None else None,
                "forward_curve_json": json.dumps(_json_ready(item["forward_curve"])),
                "confidence_curve_json": json.dumps(item["confidence_points"]),
                "sentiment_history_json": json.dumps(
                    _json_ready(
                        item["sentiment_history"].tail(10)[["recorded_at", "score"]]
                        if hasattr(item["sentiment_history"], "__getitem__") and not item["sentiment_history"].empty
                        else []
                    )
                ),
                "sector": sector_map.get(ticker.upper(), "Unknown"),
                "signal_diagnostics": signal_diagnostics,
            }
        )
        row_payload = payload["rows"][-1]
        row_payload["stop_proximity"] = _stop_proximity(row_payload)
        compute_position_size_fn = runtime.get("compute_position_size")
        if callable(compute_position_size_fn):
            try:
                price_targets = row_payload.get("price_targets") or {}
                ticker_sector = str(sector_map.get(ticker.upper(), "Unknown"))
                sector_value = sum(
                    float(candidate.get("market_value") or 0.0)
                    for candidate in payload["rows"]
                    if str(candidate.get("sector") or "Unknown") == ticker_sector
                )
                sector_exposure_pct = (sector_value / total_selected_market_value * 100.0) if total_selected_market_value else None
                row_payload["position_size"] = _json_ready(
                    compute_position_size_fn(
                        regime_probability=float(regime.latest_probability),
                        composite_action=str(item["composite_signal"].composite_action),
                        risk_reward_ratio=(price_targets or {}).get("risk_reward_ratio") if isinstance(price_targets, dict) else None,
                        atr_value=(price_targets or {}).get("atr_value") if isinstance(price_targets, dict) else None,
                        current_price=float(getattr(regime, "latest_price", 0.0) or 0.0),
                        portfolio_value=total_selected_market_value,
                        regime_exposure=payload.get("regime_exposure"),
                        sector_exposure_pct=sector_exposure_pct,
                        correlation_penalty=float(row_payload.get("concentration_penalty") or 0.0),
                        meta_labeler_probability=meta_prob,
                    )
                )
            except Exception as exc:
                logger.debug("Unable to compute position size for %s.", ticker, exc_info=exc)
        if callable(runtime.get("build_regime_price_chart")):
            try:
                row_payload["charts"] = {
                    "price": runtime["build_regime_price_chart"](regime.price_frame, ticker),
                    "transition": runtime["build_transition_heatmap"](regime.transition_matrix.tolist()),
                    "confidence": runtime["build_confidence_timeline"](regime.price_frame),
                }
            except Exception as exc:
                logger.debug("Unable to build plotly charts for %s.", ticker, exc_info=exc)
        _progress_event(
            progress_callback,
            progress=index,
            total=len(fitted_rows),
            ticker=ticker,
            stage="complete",
            text=f"Completed {ticker}",
            cache_hits=cache_hits,
            cache_misses=cache_misses,
            partial_result=row_payload,
        )

    payload["recent_alerts"] = _fetch_recent_alerts(days=7)
    payload["unread_alert_count"] = len(payload["recent_alerts"])
    payload["regime_exposure"], payload["total_market_value"] = _compute_regime_exposure(payload["rows"])
    payload["theme_health"] = _compute_theme_health(payload.get("themes") or [], payload["rows"])
    compute_position_size_fn = runtime.get("compute_position_size")
    if callable(compute_position_size_fn):
        for row in payload["rows"]:
            try:
                price_targets = row.get("price_targets") or {}
                ticker_sector = str(row.get("sector") or "Unknown")
                sector_value = sum(
                    float(candidate.get("market_value") or 0.0)
                    for candidate in payload["rows"]
                    if str(candidate.get("sector") or "Unknown") == ticker_sector
                )
                sector_exposure_pct = (sector_value / total_selected_market_value * 100.0) if total_selected_market_value else None
                row["position_size"] = _json_ready(
                    compute_position_size_fn(
                        regime_probability=float(row.get("probability") or 0.0),
                        composite_action=str(row.get("composite_signal") or "Hold"),
                        risk_reward_ratio=(price_targets or {}).get("risk_reward_ratio") if isinstance(price_targets, dict) else None,
                        atr_value=(price_targets or {}).get("atr_value") if isinstance(price_targets, dict) else None,
                        current_price=float(row.get("current_price") or 0.0),
                        portfolio_value=total_selected_market_value,
                        regime_exposure=payload.get("regime_exposure"),
                        sector_exposure_pct=sector_exposure_pct,
                        correlation_penalty=float(row.get("concentration_penalty") or 0.0),
                        meta_labeler_probability=float(row.get("meta_labeler_probability")) if row.get("meta_labeler_probability") is not None else None,
                    )
                )
            except Exception as exc:
                logger.debug("Unable to refresh portfolio-aware position size for %s.", row.get("ticker"), exc_info=exc)
    portfolio_summary_fn = runtime.get("portfolio_risk_summary_dict")
    if callable(portfolio_summary_fn):
        try:
            regime_results = {
                row["ticker"]: {
                    "label": row["regime"],
                    "transition_risk": float(row["transition_risk_pct"] or 0.0) / 100.0,
                    "composite_action": row["composite_signal"],
                    "sector": row.get("sector") or "Unknown",
                }
                for row in payload["rows"]
            }
            all_positions = [position for positions in positions_by_account.values() for position in positions]
            try:
                payload["portfolio_summary"] = _json_ready(portfolio_summary_fn(all_positions, regime_results, sector_map=sector_map))
            except TypeError:
                payload["portfolio_summary"] = _json_ready(portfolio_summary_fn(all_positions, regime_results))
        except Exception as exc:
            logger.warning("Unable to build portfolio regime summary.", exc_info=exc)
    outperforming = [row["ticker"] for row in payload["rows"] if row.get("relative_strength") == "Outperforming"]
    lagging = [row["ticker"] for row in payload["rows"] if row.get("relative_strength") == "Lagging"]
    summary = None
    if outperforming:
        summary = f"Outperforming vs {benchmark}: {', '.join(outperforming)}"
    elif lagging:
        summary = f"Lagging vs {benchmark}: {', '.join(lagging)}"
    payload["relative_strength"] = {"summary": summary, "outperforming": outperforming, "lagging": lagging}
    get_signal_effectiveness_fn = runtime.get("get_signal_effectiveness")
    if callable(get_signal_effectiveness_fn):
        try:
            payload["signal_effectiveness"] = _json_ready(get_signal_effectiveness_fn())
        except Exception as exc:
            logger.warning("Unable to load signal effectiveness summary.", exc_info=exc)
    get_calibration_data_fn = runtime.get("get_calibration_data")
    calibration_payload_fn = runtime.get("calibration_payload")
    if callable(get_calibration_data_fn) and callable(calibration_payload_fn):
        try:
            calibration_rows = get_calibration_data_fn(lookback_days=365)
            if len(calibration_rows) >= 30:
                payload["model_diagnostics"] = _json_ready(calibration_payload_fn(calibration_rows))
        except Exception as exc:
            logger.warning("Unable to build model diagnostics payload.", exc_info=exc)
    now = dt.datetime.now().astimezone()
    payload["run_diff"] = _compute_run_diff(payload, previous_payload)
    payload["snapshots_saved"] = snapshots_saved_count
    payload["last_run_timestamp"] = now.isoformat(timespec="seconds")
    payload["last_run_display"] = now.strftime("%Y-%m-%d %H:%M:%S %Z")
    payload["job_status"] = "done"
    return payload


def _set_job_state(
    job_id: str,
    *,
    status: str | None = None,
    progress: int | None = None,
    payload: dict[str, Any] | None = None,
    error: str | None = None,
    progress_text: str | None = None,
    current_ticker: str | None = None,
    eta_seconds: float | None = None,
    cache_hits: int | None = None,
    cache_misses: int | None = None,
    partial_result: dict[str, Any] | None = None,
) -> None:
    with _JOBS_LOCK:
        job = _JOBS.get(job_id)
        if job is None:
            return
        if status is not None:
            job.status = status
        if progress is not None:
            job.progress = progress
        if payload is not None:
            job.payload = payload
        if error is not None:
            job.error = error
        if progress_text is not None:
            job.progress_text = progress_text
        if current_ticker is not None:
            job.current_ticker = current_ticker
        if eta_seconds is not None:
            job.eta_seconds = eta_seconds
        if cache_hits is not None:
            job.cache_hits = cache_hits
        if cache_misses is not None:
            job.cache_misses = cache_misses
        if partial_result is not None:
            if job.partial_results is None:
                job.partial_results = {}
            ticker = str(partial_result.get("ticker") or "").upper()
            if ticker:
                job.partial_results[ticker] = partial_result


def _run_analysis(job_id: str) -> None:
    with _JOBS_LOCK:
        job = _JOBS.get(job_id)
        if job is None:
            return
        job.status = "running"
        benchmark = job.benchmark
        period = job.period
        show_all = job.show_all
        frontier_enabled = job.frontier_enabled
        portfolio_scope = getattr(job, "portfolio_scope", "household")
        account_id = getattr(job, "account_id", None)
        frontier_provider = getattr(job, "frontier_provider", "auto")
        force_refresh = bool(getattr(job, "force_refresh", False))
        frontier_batch_size = int(getattr(job, "frontier_batch_size", _DEFAULT_FRONTIER_BATCH_SIZE) or _DEFAULT_FRONTIER_BATCH_SIZE)
        tickers = list(job.tickers)

    run_started = time.monotonic()

    def progress_callback(
        progress: int,
        total: int,
        ticker: str | None,
        *,
        stage: str = "",
        text: str = "",
        eta_seconds: float | None = None,
        cache_hits: int = 0,
        cache_misses: int = 0,
        partial_result: dict[str, Any] | None = None,
    ) -> None:
        del stage
        resolved_eta = eta_seconds
        if total > 0 and progress > 0 and resolved_eta is None:
            elapsed = time.monotonic() - run_started
            resolved_eta = (elapsed / progress) * max(0, total - progress)
        _set_job_state(
            job_id,
            progress=progress,
            progress_text=text,
            current_ticker=ticker,
            eta_seconds=resolved_eta,
            cache_hits=cache_hits,
            cache_misses=cache_misses,
            partial_result=partial_result,
        )

    session: Session | None = None
    try:
        session = get_session()
        payload = _build_regime_dashboard_payload(
            session=session,
            benchmark=benchmark,
            period=period,
            show_all=show_all,
            portfolio_scope=portfolio_scope,
            account_id=account_id,
            tickers=tickers,
            frontier_enabled=frontier_enabled,
            frontier_provider=frontier_provider,
            frontier_batch_size=frontier_batch_size,
            force_refresh=force_refresh,
            progress_callback=progress_callback,
        )
        payload["job_id"] = job_id
        payload["job_status"] = "done"
        archive_previous_payload()
        save_payload(_json_ready(payload))
        _set_job_state(job_id, status="done", progress=len(tickers), payload=payload, error=None)
    except Exception as exc:
        logger.warning("Background regime analysis job %s failed.", job_id, exc_info=exc)
        _set_job_state(job_id, status="error", error=str(exc), progress=0)
    finally:
        if session is not None:
            session.close()


def _submit_regime_job(
    *,
    tickers: list[str],
    benchmark: str,
    period: str,
    show_all: bool,
    frontier_enabled: bool,
    portfolio_scope: str,
    account_id: int | None,
    frontier_provider: str,
    frontier_batch_size: int,
    force_refresh: bool,
) -> RegimeJob:
    _prune_jobs()
    job = RegimeJob(
        job_id=uuid.uuid4().hex,
        status="pending",
        tickers=tickers,
        benchmark=benchmark,
        period=period,
        progress=0,
        total=len(tickers),
        payload=None,
        error=None,
        created_at=dt.datetime.now(dt.timezone.utc),
        show_all=show_all,
        frontier_enabled=frontier_enabled,
        account_id=account_id,
    )
    setattr(job, "portfolio_scope", portfolio_scope)
    setattr(job, "frontier_provider", frontier_provider)
    setattr(job, "frontier_batch_size", int(frontier_batch_size or _DEFAULT_FRONTIER_BATCH_SIZE))
    setattr(job, "force_refresh", bool(force_refresh))
    with _JOBS_LOCK:
        _JOBS[job.job_id] = job
    _EXECUTOR.submit(_run_analysis, job.job_id)
    return job


def _set_discovery_job_state(
    job_id: str,
    *,
    status: str | None = None,
    progress: int | None = None,
    current_theme: str | None = None,
    results: dict[str, Any] | None = None,
    error: str | None = None,
) -> None:
    with _DISCOVERY_JOBS_LOCK:
        job = _DISCOVERY_JOBS.get(job_id)
        if job is None:
            return
        if status is not None:
            job.status = status
        if progress is not None:
            job.progress = progress
        if current_theme is not None:
            job.current_theme = current_theme
        if results is not None:
            job.results = results
        if error is not None:
            job.error = error


def _run_discovery_job(job_id: str) -> None:
    with _DISCOVERY_JOBS_LOCK:
        job = _DISCOVERY_JOBS.get(job_id)
        if job is None:
            return
        job.status = "running"
        theme_ids = list(job.theme_ids)
        frontier_provider = job.frontier_provider
        regenerate_supply_chain = bool(job.regenerate_supply_chain)
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        _set_discovery_job_state(job_id, status="error", error=runtime_error or "Regime analytics are unavailable.")
        return
    try:
        results: list[dict[str, Any]] = []
        themes = [runtime["get_theme"](theme_id) for theme_id in theme_ids] if theme_ids else runtime["list_themes"](include_closed=False)
        themes = [theme for theme in themes if theme and str(theme.get("status") or "") == "Active"]
        for idx, theme in enumerate(themes, start=1):
            theme_id = int(theme["id"])
            theme_name = str(theme.get("name") or "")
            _set_discovery_job_state(job_id, progress=idx - 1, current_theme=theme_name)
            theme_has_sector_hint = bool(str(theme.get("sector_hint") or "").strip())
            if not theme_has_sector_hint:
                if regenerate_supply_chain or not runtime["get_supply_chain"](theme_id):
                    runtime["generate_supply_chain"](theme_id, frontier_enabled=True, frontier_provider=frontier_provider)
            theme_results = runtime["run_discovery_scan"](theme_id, frontier_enabled=True, frontier_provider=frontier_provider)
            entry_signals = runtime["check_entry_signals"](theme_id)
            results.append(
                {
                    "theme_id": theme_id,
                    "theme_name": theme_name,
                    "new_candidates": len(theme_results),
                    "updated_candidates": len(theme_results),
                    "entry_signals": [item.get("ticker") for item in entry_signals],
                    "errors": [],
                }
            )
            _set_discovery_job_state(job_id, progress=idx, current_theme=theme_name)
        runtime["expire_stale_candidates"](90)
        payload = {
            "themes_scanned": len(themes),
            "candidates_found": sum(int(item.get("new_candidates") or 0) for item in results),
            "entry_signals": sum(len(item.get("entry_signals") or []) for item in results),
            "results": results,
            "watchlist": runtime["get_watchlist"](),
            "watchlist_stats": runtime["get_watchlist_stats"](),
        }
        _set_discovery_job_state(job_id, status="done", progress=len(themes), results=payload)
    except Exception as exc:
        logger.warning("Background discovery job %s failed.", job_id, exc_info=exc)
        _set_discovery_job_state(job_id, status="error", error=str(exc))


def _submit_discovery_job(
    *,
    theme_ids: list[int],
    frontier_provider: str,
    regenerate_supply_chain: bool,
) -> DiscoveryJob:
    _prune_jobs()
    total = len(theme_ids)
    job = DiscoveryJob(
        job_id=uuid.uuid4().hex,
        status="pending",
        theme_ids=theme_ids,
        progress=0,
        total=total,
        current_theme=None,
        results=None,
        error=None,
        created_at=dt.datetime.now(dt.timezone.utc),
        frontier_provider=frontier_provider,
        regenerate_supply_chain=bool(regenerate_supply_chain),
    )
    with _DISCOVERY_JOBS_LOCK:
        _DISCOVERY_JOBS[job.job_id] = job
    _EXECUTOR.submit(_run_discovery_job, job.job_id)
    return job


def _build_shell_context(
    request: Request,
    *,
    session: Session | None,
    actor: str,
    benchmark: str = "SOXX",
    period: str = "3y",
    show_all: bool = False,
    frontier_enabled: bool = False,
    frontier_provider: str = "auto",
    frontier_batch_size: int = _DEFAULT_FRONTIER_BATCH_SIZE,
    portfolio_scope: str = "household",
    account_id: int | None = None,
    force_refresh: bool = False,
) -> dict[str, Any]:
    runtime, runtime_error = _load_hmm_runtime()
    payload = _empty_dashboard_payload(
        benchmark=benchmark,
        period=period,
        show_all=show_all,
        runtime=runtime,
        runtime_error=runtime_error,
    )
    payload["frontier_enabled"] = frontier_enabled
    payload["frontier_provider"] = frontier_provider
    payload["frontier_batch_size"] = frontier_batch_size
    payload["portfolio_scope"] = portfolio_scope
    payload["account_id"] = account_id
    payload["force_refresh"] = force_refresh
    payload["themes"] = _load_themes(runtime)
    payload["recent_alerts"] = _fetch_recent_alerts(days=7)
    payload["unread_alert_count"] = len(payload["recent_alerts"])
    cached_payload = load_payload()
    if isinstance(cached_payload, dict) and cached_payload.get("rows"):
        payload = {**payload, **cached_payload}
        payload["benchmark"] = benchmark
        payload["period"] = period
        payload["show_all"] = show_all
        payload["frontier_enabled"] = frontier_enabled
        payload["frontier_provider"] = frontier_provider
        payload["frontier_batch_size"] = frontier_batch_size
        payload["portfolio_scope"] = portfolio_scope
        payload["account_id"] = account_id
        payload["force_refresh"] = force_refresh
        payload["hmm_available"] = runtime is not None
        payload["job_status"] = "cached"
        payload["cached_note"] = f"Showing cached results from {payload.get('last_run_display') or payload.get('last_run_timestamp') or 'last run'}."
        current_warnings = [warning for warning in payload.get("warnings", []) if warning]
        if runtime_error:
            current_warnings = [runtime_error, *[warning for warning in current_warnings if warning != runtime_error]]
        payload["warnings"] = current_warnings
    config = {
        "benchmark": benchmark,
        "period": period,
        "show_all": show_all,
        "frontier_enabled": frontier_enabled,
        "frontier_provider": frontier_provider,
        "frontier_batch_size": frontier_batch_size,
        "force_refresh": force_refresh,
        "portfolio_scope": portfolio_scope,
        "account_id": account_id,
        "max_tickers": _MAX_TICKERS,
        "endpoints": {
            "holdings": "/regime/holdings",
            "portfolios": "/regime/portfolios",
            "run": "/regime/run",
            "status": "/regime/status/__JOB_ID__",
            "digest": "/regime/digest",
            "effectiveness": "/regime/effectiveness",
            "backtest": "/regime/backtest/__TICKER__",
            "alerts": "/regime/alerts",
            "journal": "/regime/journal",
            "journal_stats": "/regime/journal/stats",
            "themes": "/regime/themes",
            "theme": "/regime/themes/__THEME_ID__",
            "theme_tickers": "/regime/themes/__THEME_ID__/tickers",
            "theme_health": "/regime/theme-health",
            "supply_chain": "/regime/themes/__THEME_ID__/supply-chain",
            "discovery_scan": "/regime/discovery/scan",
            "discovery_scan_status": "/regime/discovery/scan/__JOB_ID__",
            "discovery_signals": "/regime/discovery/signals",
            "watchlist": "/regime/watchlist",
            "watchlist_entry": "/regime/watchlist/__WATCHLIST_ID__",
            "watchlist_promote": "/regime/watchlist/__WATCHLIST_ID__/promote",
            "watchlist_pass": "/regime/watchlist/__WATCHLIST_ID__/pass",
            "paper_portfolios": "/regime/paper-portfolio",
            "paper_portfolio": "/regime/paper-portfolio/__PORTFOLIO_ID__",
            "paper_budget": "/regime/paper-portfolio/__PORTFOLIO_ID__/budget",
            "paper_positions": "/regime/paper-portfolio/__PORTFOLIO_ID__/positions",
            "paper_plans": "/regime/paper-portfolio/__PORTFOLIO_ID__/plans",
            "paper_monitoring": "/regime/paper-portfolio/__PORTFOLIO_ID__/monitoring",
            "paper_generate": "/regime/paper-portfolio/__PORTFOLIO_ID__/plans/generate",
            "paper_precheck": "/regime/paper-portfolio/__PORTFOLIO_ID__/plans/precheck",
            "paper_plan": "/regime/paper-portfolio/__PORTFOLIO_ID__/plans/__PLAN_ID__",
            "paper_execute": "/regime/paper-portfolio/__PORTFOLIO_ID__/plans/execute",
            "paper_pending_orders": "/regime/paper-portfolio/__PORTFOLIO_ID__/orders/pending",
            "paper_cancel_order": "/regime/paper-portfolio/__PORTFOLIO_ID__/orders/__PLAN_ID__/cancel",
            "paper_auto_approve": "/regime/paper-portfolio/__PORTFOLIO_ID__/auto-approve",
            "paper_autonomy_status": "/regime/paper-portfolio/__PORTFOLIO_ID__/autonomy/status",
            "paper_kill_switch": "/regime/paper-portfolio/__PORTFOLIO_ID__/kill-switch",
            "paper_performance": "/regime/paper-portfolio/__PORTFOLIO_ID__/performance",
            "paper_attribution_summary": "/regime/paper-portfolio/__PORTFOLIO_ID__/attribution/summary",
            "paper_audit": "/regime/paper-portfolio/__PORTFOLIO_ID__/audit",
            "paper_audit_summary": "/regime/paper-portfolio/__PORTFOLIO_ID__/audit/summary",
            "ibkr_settings": "/regime/ibkr/settings",
            "ibkr_test_connection": "/regime/ibkr/test-connection",
            "frontier_models": "/regime/frontier/models",
            "frontier_settings": "/regime/frontier/settings",
            "autonomy_settings": "/regime/autonomy/settings",
        },
        "initial_payload": payload,
        "portfolio_scopes": get_available_portfolio_scopes(session) if session is not None else [],
    }
    return {
        "request": request,
        "actor": actor,
        "auth_banner": None,
        "auth_banner_detail": auth_banner_message(),
        "static_version": _static_version(),
        "title": "Regime",
        "page_badge": "Dashboard",
        "warnings": payload["warnings"],
        "benchmark": payload["benchmark"],
        "period": payload["period"],
        "show_all": payload["show_all"],
        "frontier_enabled": bool(payload.get("frontier_enabled")),
        "frontier_provider": payload.get("frontier_provider") or "auto",
        "force_refresh": bool(payload.get("force_refresh")),
        "hmm_available": payload["hmm_available"],
        "portfolio_scope": payload.get("portfolio_scope") or "household",
        "account_id": payload.get("account_id"),
        "portfolio_mode": payload["portfolio_mode"],
        "page_context": (
            f"<b>Benchmark:</b> {payload['benchmark']} <span class='ui-muted'>·</span> "
            f"<b>Period:</b> {payload['period']} <span class='ui-muted'>·</span> "
            f"<b>Mode:</b> {payload['portfolio_mode']}"
        ),
        "regime_config_json": json.dumps(_json_ready(config)),
    }


def build_regime_page_context(
    request: Request,
    *,
    session: Session | None,
    actor: str,
    benchmark: str = "SOXX",
    period: str = "3y",
    show_all: bool = False,
    frontier_enabled: bool = False,
    frontier_provider: str = "auto",
    frontier_batch_size: int = _DEFAULT_FRONTIER_BATCH_SIZE,
    portfolio_scope: str = "household",
    account_id: int | None = None,
    force_refresh: bool = False,
) -> dict[str, Any]:
    return _build_shell_context(
        request,
        session=session,
        actor=actor,
        benchmark=benchmark,
        period=period,
        show_all=show_all,
        frontier_enabled=frontier_enabled,
        frontier_provider=frontier_provider,
        frontier_batch_size=frontier_batch_size,
        portfolio_scope=portfolio_scope,
        account_id=account_id,
        force_refresh=force_refresh,
    )


def build_digest_response_payload(
    *,
    benchmark: str = "SOXX",
    period: str = "3y",
    show_all: bool = False,
    tickers: list[str] | None = None,
) -> dict[str, Any]:
    payload = _build_regime_dashboard_payload(
        benchmark=benchmark,
        period=period,
        show_all=show_all,
        tickers=tickers,
    )
    return {
        "benchmark": payload["benchmark"],
        "period": payload["period"],
        "show_all": payload["show_all"],
        "warnings": payload["warnings"],
        "digest": payload["digest"],
        "portfolio_count": payload["portfolio_count"],
        "action_items_count": payload["action_items_count"],
        "last_run_timestamp": payload["last_run_timestamp"],
        "last_run_display": payload["last_run_display"],
        "benchmark_regime": payload["benchmark_regime"],
        "benchmark_regime_tone": payload["benchmark_regime_tone"],
        "portfolio_mode": payload["portfolio_mode"],
        "hmm_available": payload["hmm_available"],
        "selected_tickers": payload["selected_tickers"],
        "selected_count": payload["selected_count"],
    }


@router.get("")
def regime_dashboard(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    benchmark: str = "SOXX",
    period: str = "3y",
    show_all: bool = False,
    frontier_enabled: bool = False,
    frontier_provider: str = "auto",
    frontier_batch_size: int = _DEFAULT_FRONTIER_BATCH_SIZE,
    portfolio_scope: str = "household",
    account_id: str = "",
    force_refresh: bool = False,
):
    from src.app.main import templates

    return templates.TemplateResponse(
        "regime.html",
        build_regime_page_context(
            request,
            session=session,
            actor=actor,
            benchmark=benchmark,
            period=period,
            show_all=show_all,
            frontier_enabled=frontier_enabled,
            frontier_provider=frontier_provider,
            frontier_batch_size=frontier_batch_size,
            portfolio_scope=portfolio_scope,
            account_id=_parse_account_id(account_id),
            force_refresh=force_refresh,
        ),
    )


@router.get("/ibkr/settings")
def regime_ibkr_settings(
    actor: str = Depends(require_actor),
):
    del actor
    from src.regime.config import IBKRConfig, validate_ibkr_readiness

    config = IBKRConfig()
    readiness = validate_ibkr_readiness()
    return JSONResponse(
        content={
            "config": {
                "host": config.host,
                "port": config.port,
                "client_id": config.client_id,
                "account_id": config.account_id,
                "live_backend": config.live_backend,
                "timeout": config.timeout,
            },
            "readiness": readiness,
        }
    )


@router.post("/ibkr/settings")
async def regime_ibkr_settings_update(
    request: Request,
    actor: str = Depends(require_actor),
):
    del actor
    form = await _read_run_request(request)
    host = str(form.get("host") or "127.0.0.1").strip()
    port = int(form.get("port") or 7497)
    client_id = int(form.get("client_id") or 1)
    account_id = str(form.get("account_id") or "").strip()
    live_backend = str(form.get("live_backend") or "false").lower() in ("true", "1", "yes", "on")
    timeout = int(form.get("timeout") or 10)

    if port not in (7497, 4002):
        raise HTTPException(status_code=422, detail="Port must be 7497 (TWS paper) or 4002 (Gateway paper). Live ports (7496/4001) are blocked.")
    if live_backend and not account_id.startswith("DU"):
        raise HTTPException(status_code=422, detail="Cannot enable live backend with non-paper account (must start with DU).")
    if host not in ("127.0.0.1", "localhost"):
        raise HTTPException(status_code=422, detail="Host must be 127.0.0.1 or localhost. Remote connections are not supported.")
    if not (1 <= client_id <= 32):
        raise HTTPException(status_code=422, detail="Client ID must be between 1 and 32.")
    if not (5 <= timeout <= 60):
        raise HTTPException(status_code=422, detail="Timeout must be between 5 and 60 seconds.")

    _update_env_file(
        {
            "IBKR_HOST": host,
            "IBKR_PORT": str(port),
            "IBKR_CLIENT_ID": str(client_id),
            "IBKR_ACCOUNT_ID": account_id,
            "IBKR_LIVE_BACKEND": "true" if live_backend else "false",
            "IBKR_TIMEOUT": str(timeout),
        }
    )
    return JSONResponse(
        content={
            "saved": True,
            "restart_required": True,
            "message": "Settings saved to .env. Restart the server to apply changes.",
        }
    )


@router.get("/frontier/models")
def regime_frontier_models(
    provider: str = "openai",
    refresh: str = "",
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")

    force_refresh = str(refresh).strip().lower() in {"1", "true", "yes"}
    provider_key = str(provider or "openai").strip().lower() or "openai"

    if not force_refresh:
        cache_json = runtime["get_setting"]("frontier_models_cache") or "{}"
        try:
            cache = json.loads(cache_json)
        except json.JSONDecodeError:
            cache = {}
        entry = cache.get(provider_key)
        if isinstance(entry, dict):
            fetched_at = str(entry.get("fetched_at", "") or "")
            if fetched_at:
                try:
                    age = (dt.datetime.now(dt.timezone.utc) - dt.datetime.fromisoformat(fetched_at)).total_seconds()
                except ValueError:
                    age = _MODEL_CACHE_TTL_SECONDS + 1
                if age < _MODEL_CACHE_TTL_SECONDS:
                    return JSONResponse(
                        content={
                            "provider": provider_key,
                            "models": entry.get("models", []),
                            "cached": True,
                            "fetched_at": fetched_at,
                        }
                    )

    try:
        models = runtime["list_provider_models"](provider_key)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to query {provider_key} models: {exc}")

    cache_json = runtime["get_setting"]("frontier_models_cache") or "{}"
    try:
        cache = json.loads(cache_json)
    except json.JSONDecodeError:
        cache = {}
    fetched_at = dt.datetime.now(dt.timezone.utc).isoformat()
    cache[provider_key] = {
        "models": models,
        "fetched_at": fetched_at,
    }
    runtime["set_setting"]("frontier_models_cache", json.dumps(cache))
    return JSONResponse(
        content={
            "provider": provider_key,
            "models": models,
            "cached": False,
            "fetched_at": fetched_at,
        }
    )


@router.get("/frontier/settings")
def regime_frontier_settings(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    return JSONResponse(
        content={
            "provider": runtime["get_setting"]("frontier_provider") or "auto",
            "model": runtime["get_setting"]("frontier_model") or "",
        }
    )


@router.put("/frontier/settings")
async def regime_frontier_settings_update(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    form = await _read_run_request(request)
    provider = str(form.get("provider", "") or "").strip().lower()
    model = str(form.get("model", "") or "").strip()
    if provider:
        runtime["set_setting"]("frontier_provider", provider)
    if provider and not model:
        runtime["delete_setting"]("frontier_model")
    elif model:
        runtime["set_setting"]("frontier_model", model)
    return JSONResponse(
        content={
            "provider": runtime["get_setting"]("frontier_provider") or "auto",
            "model": runtime["get_setting"]("frontier_model") or "",
        }
    )


def _ensemble_settings_payload(runtime: dict[str, Any]) -> dict[str, str]:
    payload = {
        "ensemble_enabled": "false",
        "ensemble_veto_threshold": "0.50",
        "ensemble_confirm_threshold": "0.65",
        "ensemble_aggregation_method": "mean",
        "barrier_profit_target_atr_mult": "2.0",
        "barrier_stop_loss_atr_mult": "2.0",
        "barrier_max_holding_days": "21",
        "meta_compute_backend": "local",
    }
    payload.update(runtime["get_all_settings"]("ensemble_"))
    payload.update(runtime["get_all_settings"]("barrier_"))
    payload.update(runtime["get_all_settings"]("meta_"))
    return payload


def _meta_labeler_config_from_runtime(runtime: dict[str, Any]) -> Any:
    config_cls = runtime["MetaLabelerConfig"]
    defaults = runtime["DEFAULT_META_LABELER_CONFIG"]

    def _int_setting(key: str, default: int) -> int:
        try:
            return int(runtime["get_setting"](key) or default)
        except Exception:
            return default

    def _float_setting(key: str, default: float) -> float:
        try:
            return float(runtime["get_setting"](key) or default)
        except Exception:
            return default

    return config_cls(
        n_estimators=_int_setting("meta_n_estimators", defaults.n_estimators),
        learning_rate=_float_setting("meta_learning_rate", defaults.learning_rate),
        max_depth=_int_setting("meta_max_depth", defaults.max_depth),
        subsample=_float_setting("meta_subsample", defaults.subsample),
        colsample_bytree=_float_setting("meta_colsample_bytree", defaults.colsample_bytree),
        random_state=defaults.random_state,
        min_training_samples=_int_setting("meta_min_training_samples", defaults.min_training_samples),
        walk_forward_gap=_int_setting("meta_walk_forward_gap", defaults.walk_forward_gap),
    )


@router.get("/ensemble/settings")
def regime_ensemble_settings_get(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    return JSONResponse(content=_ensemble_settings_payload(runtime))


@router.put("/ensemble/settings")
async def regime_ensemble_settings_put(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    try:
        body = await request.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        raise HTTPException(status_code=422, detail="Request body must be a JSON object.")
    allowed_prefixes = ("ensemble_", "barrier_", "meta_")
    for key, value in body.items():
        if any(str(key).startswith(prefix) for prefix in allowed_prefixes):
            runtime["set_setting"](str(key), str(value))
    return JSONResponse(content=_ensemble_settings_payload(runtime))


@router.get("/ensemble/analysts")
def regime_ensemble_analysts_list(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    registry = runtime["get_registry"]()
    return JSONResponse(
        content={
            "analysts": [
                {"name": analyst.name, "ready": analyst.is_ready()}
                for analyst in (registry.get(name) for name in registry.list_analysts())
                if analyst is not None
            ]
        }
    )


@router.post("/ensemble/meta-labeler/train")
async def regime_meta_labeler_train(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics unavailable.")

    body = await request.json()
    ticker = str(body.get("ticker", "") or "").strip().upper()
    if not ticker:
        raise HTTPException(status_code=400, detail="Ticker is required.")
    period = str(body.get("period", "3y") or "3y")
    training_window = int(body.get("training_window", 504) or 504)
    refit_step = int(body.get("refit_step", 21) or 21)

    def _train_sync() -> dict[str, Any]:
        market_series = runtime["download_market_frame"](ticker=ticker, period=period)
        market_frame = getattr(market_series, "frame", market_series)
        regime_result = runtime["fit_regime_model"](
            ticker=ticker,
            market_frame=market_frame,
            training_window=training_window,
            refit_step=refit_step,
        )
        labeled_frame = runtime["build_labeled_frame"](ticker, market_frame, regime_result)
        registry = runtime["get_registry"]()
        engine = registry.get("xgboost_meta_labeler")
        if engine is None:
            engine = runtime["create_and_register_meta_labeler"](_meta_labeler_config_from_runtime(runtime))
        metrics = engine.train(labeled_frame)
        save_result: dict[str, Any] = {}
        if engine.is_ready():
            active_version_str = runtime["get_setting"]("meta_labeler_active_version")
            if active_version_str:
                try:
                    runtime["update_training_status"](int(active_version_str), "superseded")
                except Exception:
                    pass
            new_version = runtime["get_next_version"]()
            model_path = runtime["_version_path"](new_version)
            save_result = engine.save_model(model_path)
            save_result["version"] = new_version
            config_dict = {
                "n_estimators": engine._config.n_estimators,
                "learning_rate": engine._config.learning_rate,
                "max_depth": engine._config.max_depth,
                "subsample": engine._config.subsample,
                "colsample_bytree": engine._config.colsample_bytree,
                "min_training_samples": engine._config.min_training_samples,
                "walk_forward_gap": engine._config.walk_forward_gap,
            }
            runtime["log_training_run"](
                version=new_version,
                ticker=ticker,
                model_path=model_path,
                metrics=metrics,
                config=config_dict,
            )
            runtime["set_setting"]("meta_labeler_active_version", str(new_version))
        return {"ticker": ticker, "metrics": metrics, "ready": engine.is_ready(), **save_result}

    result = await asyncio.to_thread(_train_sync)
    return JSONResponse(content=_json_ready(result))


@router.get("/ensemble/meta-labeler/status")
def regime_meta_labeler_status(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics unavailable.")

    registry = runtime["get_registry"]()
    engine = registry.get("xgboost_meta_labeler")
    if engine is None:
        return JSONResponse(content={"ready": False, "status": "not_created"})
    ready = bool(engine.is_ready())
    active_version_str = runtime["get_setting"]("meta_labeler_active_version")
    active_version = int(active_version_str) if active_version_str else None
    return JSONResponse(
        content={
            "ready": ready,
            "status": "trained" if ready else "not_trained",
            "active_version": active_version,
            "metrics": getattr(engine, "_training_metrics", {}) if ready else {},
            "feature_importances": getattr(engine, "_feature_importances", {}) if ready else {},
        }
    )


@router.post("/ensemble/meta-labeler/rollback")
async def regime_meta_labeler_rollback(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics unavailable.")
    body = await request.json()
    target_version = body.get("version")
    if target_version is None:
        raise HTTPException(status_code=400, detail="Target version is required.")
    target_version = int(target_version)

    def _rollback_sync() -> dict[str, Any]:
        path = runtime["_version_path"](target_version)
        if not os.path.isfile(path):
            return {"error": f"Model v{target_version} not found on disk.", "success": False}
        current_version_str = runtime["get_setting"]("meta_labeler_active_version")
        current_version = int(current_version_str) if current_version_str else None
        registry = runtime["get_registry"]()
        engine = registry.get("xgboost_meta_labeler")
        if engine is None:
            engine = runtime["create_and_register_meta_labeler"](_meta_labeler_config_from_runtime(runtime))
        load_result = runtime["auto_load_active_model"](engine, target_version)
        if not load_result.get("loaded"):
            return {"error": f"Failed to load model v{target_version}: {load_result.get('status')}", "success": False}
        if current_version is not None and current_version != target_version:
            runtime["update_training_status"](current_version, "rolled_back")
        runtime["update_training_status"](target_version, "active")
        runtime["set_setting"]("meta_labeler_active_version", str(target_version))
        return {
            "success": True,
            "rolled_back_from": current_version,
            "active_version": target_version,
            "load_result": load_result,
        }

    result = await asyncio.to_thread(_rollback_sync)
    if result.get("error"):
        raise HTTPException(status_code=404, detail=result["error"])
    return JSONResponse(content=_json_ready(result))


@router.get("/ensemble/meta-labeler/versions")
def regime_meta_labeler_versions(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics unavailable.")
    active_version_str = runtime["get_setting"]("meta_labeler_active_version")
    active_version = int(active_version_str) if active_version_str else None
    versions = runtime["list_saved_versions"]()
    history = runtime["get_training_history"](limit=50)
    history_by_version = {int(row["version"]): row for row in history}
    for version in versions:
        log_entry = history_by_version.get(int(version["version"]))
        if log_entry:
            version["accuracy"] = log_entry.get("accuracy")
            version["f1"] = log_entry.get("f1")
            version["train_samples"] = log_entry.get("train_samples")
            version["test_samples"] = log_entry.get("test_samples")
            version["status"] = log_entry.get("status", "unknown")
            version["trained_at"] = log_entry.get("trained_at")
            version["ticker"] = log_entry.get("ticker")
        version["is_active"] = int(version["version"]) == active_version
    comparison = None
    if len(versions) >= 2 and active_version is not None:
        active_entry = history_by_version.get(active_version)
        previous_entries = [row for row in history if int(row["version"]) != active_version and row.get("status") != "rolled_back"]
        if active_entry and previous_entries:
            prev = previous_entries[0]
            comparison = {
                "active_version": active_version,
                "compare_version": int(prev["version"]),
                "accuracy_delta": round((active_entry.get("accuracy") or 0) - (prev.get("accuracy") or 0), 4),
                "f1_delta": round((active_entry.get("f1") or 0) - (prev.get("f1") or 0), 4),
                "sample_delta": (active_entry.get("train_samples") or 0) - (prev.get("train_samples") or 0),
            }
            active_fi = json.loads(active_entry.get("feature_importances") or "{}")
            prev_fi = json.loads(prev.get("feature_importances") or "{}")
            if active_fi and prev_fi:
                drift: dict[str, float] = {}
                for feat in runtime["META_FEATURES"]:
                    drift[feat] = round(float(active_fi.get(feat, 0.0)) - float(prev_fi.get(feat, 0.0)), 4)
                comparison["feature_importance_drift"] = drift
    return JSONResponse(content=_json_ready({"active_version": active_version, "versions": versions, "comparison": comparison}))


@router.get("/ensemble/meta-labeler/training-history")
def regime_meta_labeler_training_history(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics unavailable.")
    history = runtime["get_training_history"](limit=50)
    return JSONResponse(content=_json_ready({"history": history}))


@router.post("/ibkr/test-connection")
def regime_ibkr_test_connection(
    actor: str = Depends(require_actor),
):
    del actor
    import socket

    from src.regime.config import IBKRConfig

    config = IBKRConfig()
    result: dict[str, Any] = {
        "host": config.host,
        "port": config.port,
        "account_id": config.account_id,
    }
    try:
        sock = socket.create_connection((config.host, config.port), timeout=config.timeout)
        sock.close()
        result["tcp_reachable"] = True
    except (OSError, ConnectionRefusedError, TimeoutError):
        result["tcp_reachable"] = False
        result["error"] = f"Cannot reach {config.host}:{config.port}. Is TWS/IB Gateway running?"
        return JSONResponse(content=result)

    try:
        from src.regime.ib_live_backend import LiveIBBackend
        import asyncio

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        backend = LiveIBBackend(account_id=config.account_id)
        connected = backend.connect(config.host, config.port, int(config.client_id) + 90)
        if connected:
            summary = backend.get_account_summary()
            result["ibkr_connected"] = True
            result["account_verified"] = str(summary.account_id) == str(config.account_id)
            result["net_liquidation"] = float(summary.net_liquidation)
            backend.disconnect()
        else:
            result["ibkr_connected"] = False
            result["error"] = "TCP reachable but IBKR handshake failed. Check TWS API settings."
    except ImportError:
        result["ibkr_connected"] = None
        result["note"] = "ib_insync not available — TCP check only."
    except Exception as exc:
        result["ibkr_connected"] = False
        result["error"] = f"Connection test failed: {exc}"
    return JSONResponse(content=result)


@router.get("/holdings")
def regime_holdings(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    show_all: bool = False,
    portfolio_scope: str = "household",
    account_id: str = "",
):
    del actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        return JSONResponse(content={"tickers": [], "warning": runtime_error, "hmm_available": False})

    selected_account_id = _parse_account_id(account_id)
    _investor_db_path, tickers = _portfolio_tickers(runtime, session, show_all, portfolio_scope, selected_account_id)
    groups = {"All Holdings": tickers} if show_all else {"Current Holdings": tickers}
    return JSONResponse(
        content={
            "tickers": tickers,
            "groups": groups,
            "portfolio_mode": "All holdings" if show_all else "Filtered holdings",
            "portfolio_scope": portfolio_scope,
            "account_id": selected_account_id,
            "hmm_available": True,
        }
    )


@router.get("/portfolios")
def regime_portfolios(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del actor
    return JSONResponse(content={"scopes": get_available_portfolio_scopes(session)})


@router.get("/theme-health")
def regime_theme_health(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    cached = load_payload() or {}
    rows = cached.get("rows") if isinstance(cached, dict) else []
    themes = _load_themes(runtime)
    return JSONResponse(content={"themes": _compute_theme_health(themes, rows or [])})


@router.get("/themes")
def regime_themes_list(
    include_closed: bool = False,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    themes = _json_ready(runtime["list_themes"](include_closed=include_closed))
    get_supply_chain_fn = runtime.get("get_supply_chain")
    for theme in themes:
        theme["supply_chain"] = _json_ready(get_supply_chain_fn(int(theme["id"]))) if callable(get_supply_chain_fn) else []
    return JSONResponse(content={"themes": themes})


@router.post("/themes")
async def regime_themes_create(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    form = await _read_run_request(request)
    try:
        theme = runtime["create_theme"](
            _normalize_theme_name(form.get("name")),
            _normalize_theme_narrative(form.get("narrative")),
            _normalize_theme_conviction(form.get("conviction")),
            _normalize_theme_status(form.get("status")),
            sector_hint=_normalize_theme_sector_hint(form.get("sector_hint", "")),
        )
    except DuplicateThemeError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return JSONResponse(content=_json_ready(theme))


@router.get("/themes/{theme_id}")
def regime_theme_get(
    theme_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    theme = runtime["get_theme"](theme_id)
    if theme is None:
        raise HTTPException(status_code=404, detail="Theme not found.")
    get_supply_chain_fn = runtime.get("get_supply_chain")
    theme["supply_chain"] = _json_ready(get_supply_chain_fn(theme_id)) if callable(get_supply_chain_fn) else []
    return JSONResponse(content=_json_ready(theme))


@router.put("/themes/{theme_id}")
async def regime_theme_update(
    theme_id: int,
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    form = await _read_run_request(request)
    try:
        updated = runtime["update_theme"](
            theme_id,
            name=_normalize_theme_name(form["name"]) if "name" in form else None,
            narrative=_normalize_theme_narrative(form["narrative"]) if "narrative" in form else None,
            sector_hint=_normalize_theme_sector_hint(form["sector_hint"]) if "sector_hint" in form else None,
            conviction=_normalize_theme_conviction(form["conviction"]) if "conviction" in form else None,
            status=_normalize_theme_status(form["status"]) if "status" in form else None,
        )
    except DuplicateThemeError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    if updated is None:
        raise HTTPException(status_code=404, detail="Theme not found.")
    get_supply_chain_fn = runtime.get("get_supply_chain")
    updated["supply_chain"] = _json_ready(get_supply_chain_fn(theme_id)) if callable(get_supply_chain_fn) else []
    return JSONResponse(content=_json_ready(updated))


@router.delete("/themes/{theme_id}")
def regime_theme_delete(
    theme_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    return JSONResponse(content={"deleted": bool(runtime["delete_theme"](theme_id))})


@router.post("/themes/{theme_id}/tickers")
async def regime_theme_ticker_add(
    theme_id: int,
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    form = await _read_run_request(request)
    record = runtime["add_ticker_to_theme"](
        theme_id,
        str(form.get("ticker") or "").strip().upper(),
        role=_normalize_role(form.get("role")),
        rationale=_normalize_thesis_text(form.get("rationale", "")) if str(form.get("rationale", "")).strip() else "",
        entry_price=_normalize_optional_positive_float(form.get("entry_price"), field_name="Entry price"),
        target_price=_normalize_optional_positive_float(form.get("target_price"), field_name="Target price"),
        stop_price=_normalize_optional_positive_float(form.get("stop_price"), field_name="Stop price"),
        time_horizon=_normalize_time_horizon(form.get("time_horizon")),
    )
    return JSONResponse(content=_json_ready(record))


@router.put("/themes/{theme_id}/tickers/{ticker}")
async def regime_theme_ticker_update(
    theme_id: int,
    ticker: str,
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    form = await _read_run_request(request)
    updated = runtime["update_ticker_in_theme"](
        theme_id,
        ticker,
        **{
            key: value
            for key, value in {
                "role": _normalize_role(form["role"]) if "role" in form else None,
                "rationale": _normalize_thesis_text(form["rationale"]) if "rationale" in form and str(form.get("rationale", "")).strip() else ("" if "rationale" in form else None),
                "entry_price": _normalize_optional_positive_float(form.get("entry_price"), field_name="Entry price") if "entry_price" in form else None,
                "target_price": _normalize_optional_positive_float(form.get("target_price"), field_name="Target price") if "target_price" in form else None,
                "stop_price": _normalize_optional_positive_float(form.get("stop_price"), field_name="Stop price") if "stop_price" in form else None,
                "time_horizon": _normalize_time_horizon(form["time_horizon"]) if "time_horizon" in form else None,
            }.items()
            if value is not None
        },
    )
    if updated is None:
        raise HTTPException(status_code=404, detail="Theme ticker not found.")
    return JSONResponse(content=_json_ready(updated))


@router.delete("/themes/{theme_id}/tickers/{ticker}")
def regime_theme_ticker_delete(
    theme_id: int,
    ticker: str,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    return JSONResponse(content={"deleted": bool(runtime["remove_ticker_from_theme"](theme_id, ticker))})


@router.post("/themes/{theme_id}/supply-chain")
async def regime_supply_chain_generate(
    theme_id: int,
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    form = await _read_run_request(request)
    frontier_provider = str(form.get("frontier_provider", "auto") or "auto").strip().lower() or "auto"
    layers = runtime["generate_supply_chain"](theme_id, frontier_enabled=True, frontier_provider=frontier_provider)
    return JSONResponse(content={"theme_id": theme_id, "layers": _json_ready(layers)})


@router.get("/themes/{theme_id}/supply-chain")
def regime_supply_chain_get(
    theme_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    return JSONResponse(content={"theme_id": theme_id, "layers": _json_ready(runtime["get_supply_chain"](theme_id))})


@router.delete("/themes/{theme_id}/supply-chain")
def regime_supply_chain_delete(
    theme_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    deleted = runtime["delete_supply_chain"](theme_id)
    return JSONResponse(content={"theme_id": theme_id, "deleted": deleted})


@router.post("/discovery/scan")
async def regime_discovery_scan(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    form = await _read_run_request(request)
    theme_ids = [int(value) for value in str(form.get("theme_ids", "")).split(",") if value.strip().isdigit()]
    frontier_provider = str(form.get("frontier_provider", "auto") or "auto").strip().lower() or "auto"
    regenerate_supply_chain = str(form.get("regenerate_supply_chain", "")).strip().lower() in {"1", "true", "yes", "on"}
    if not theme_ids:
        theme_ids = [int(theme["id"]) for theme in runtime["list_themes"](include_closed=False) if str(theme.get("status") or "") == "Active"]
    job = _submit_discovery_job(
        theme_ids=theme_ids,
        frontier_provider=frontier_provider,
        regenerate_supply_chain=regenerate_supply_chain,
    )
    return JSONResponse(content={"job_id": job.job_id, "status": job.status})


@router.get("/discovery/scan/{job_id}")
def regime_discovery_status(
    job_id: str,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    _prune_jobs()
    with _DISCOVERY_JOBS_LOCK:
        job = _DISCOVERY_JOBS.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Unknown discovery job.")
        payload = _serialize_discovery_job(job)
    return JSONResponse(content=payload)


@router.get("/autonomy/settings")
def regime_autonomy_settings(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    return JSONResponse(
        content={
            "operating_mode": runtime["get_operating_mode"](),
            "auto_approve_threshold": runtime["get_auto_approve_threshold"](),
            "daily_capital_ceiling_pct": runtime["get_daily_capital_ceiling_pct"](),
            "operating_modes": list(runtime["OPERATING_MODES"]),
        }
    )


@router.put("/autonomy/settings")
async def regime_autonomy_settings_update(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    body = await request.json()
    if "operating_mode" in body:
        mode = str(body["operating_mode"] or "").strip().lower()
        if mode not in runtime["OPERATING_MODES"]:
            raise HTTPException(status_code=422, detail=f"Invalid mode. Must be one of: {', '.join(runtime['OPERATING_MODES'])}")
        runtime["set_operating_mode"](mode)
    if "auto_approve_threshold" in body:
        try:
            runtime["set_auto_approve_threshold"](float(body["auto_approve_threshold"]))
        except (ValueError, TypeError) as exc:
            raise HTTPException(status_code=422, detail="Threshold must be a number between 0 and 1.") from exc
    if "daily_capital_ceiling_pct" in body:
        try:
            runtime["set_daily_capital_ceiling_pct"](float(body["daily_capital_ceiling_pct"]))
        except (ValueError, TypeError) as exc:
            raise HTTPException(status_code=422, detail="Ceiling must be a number between 0 and 1.") from exc
    return JSONResponse(
        content={
            "operating_mode": runtime["get_operating_mode"](),
            "auto_approve_threshold": runtime["get_auto_approve_threshold"](),
            "daily_capital_ceiling_pct": runtime["get_daily_capital_ceiling_pct"](),
            "operating_modes": list(runtime["OPERATING_MODES"]),
        }
    )


@router.get("/watchlist")
def regime_watchlist(
    theme_id: str = "",
    status: str = "",
    max_crowd_score: str = "",
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    theme_filter = int(theme_id) if str(theme_id).isdigit() else None
    crowd_filter = int(max_crowd_score) if str(max_crowd_score).isdigit() else None
    rows = runtime["get_watchlist"](theme_id=theme_filter, status=status or None, max_crowd_score=crowd_filter)
    return JSONResponse(content={"watchlist": _json_ready(rows), "stats": _json_ready(runtime["get_watchlist_stats"]())})


@router.get("/watchlist/{watchlist_id}")
def regime_watchlist_entry_get(
    watchlist_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    entry = runtime["get_watchlist_entry"](watchlist_id)
    if entry is None:
        raise HTTPException(status_code=404, detail="Watchlist entry not found.")
    return JSONResponse(content=_json_ready(entry))


@router.put("/watchlist/{watchlist_id}")
async def regime_watchlist_entry_update(
    watchlist_id: int,
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    form = await _read_run_request(request)
    fields: dict[str, Any] = {}
    if "status" in form:
        fields["status"] = str(form.get("status") or "").strip()
    if "notes" in form:
        fields["notes"] = str(form.get("notes") or "")
    entry = runtime["update_watchlist_status"](watchlist_id, fields.pop("status", "Watching"), **fields)
    if entry is None:
        raise HTTPException(status_code=404, detail="Watchlist entry not found.")
    return JSONResponse(content=_json_ready(entry))


@router.delete("/watchlist/{watchlist_id}")
def regime_watchlist_entry_delete(
    watchlist_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    return JSONResponse(content={"deleted": bool(runtime["delete_watchlist_entry"](watchlist_id))})


@router.post("/watchlist/{watchlist_id}/promote")
def regime_watchlist_promote(
    watchlist_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    payload = runtime["promote_candidate"](watchlist_id)
    if not payload:
        raise HTTPException(status_code=404, detail="Watchlist entry not found.")
    return JSONResponse(content=_json_ready(payload))


@router.post("/watchlist/{watchlist_id}/pass")
def regime_watchlist_pass(
    watchlist_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    payload = runtime["update_watchlist_status"](watchlist_id, "Passed")
    if payload is None:
        raise HTTPException(status_code=404, detail="Watchlist entry not found.")
    return JSONResponse(content=_json_ready(payload))


@router.get("/discovery/signals")
def regime_discovery_signals(
    theme_id: str = "",
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    theme_filter = int(theme_id) if str(theme_id).isdigit() else None
    signals = runtime["check_entry_signals"](theme_filter)
    return JSONResponse(content={"signals": _json_ready(signals), "count": len(signals)})


def _paper_portfolio_payload(runtime: dict[str, Any], portfolio_id: int) -> dict[str, Any]:
    portfolio = runtime["get_paper_portfolio"](portfolio_id)
    if portfolio is None:
        raise HTTPException(status_code=404, detail="Paper portfolio not found.")
    broker_status = None
    if str(portfolio.get("broker_type") or "paper").lower() == "ibkr":
        adapter = _get_broker_adapter_safe(runtime, portfolio_id)
        if adapter is not None:
            try:
                health = adapter.health()
                broker_status = {
                    "connection": "connected" if health.get("connected") else "disconnected",
                    "market_hours": health.get("market_hours", "closed"),
                }
            except Exception:
                broker_status = {"connection": "disconnected", "market_hours": "closed"}
        else:
            broker_status = {"connection": "unavailable", "market_hours": "closed"}
    return {
        "portfolio": _json_ready(portfolio),
        "summary": _json_ready(runtime["get_paper_portfolio_summary"](portfolio_id)),
        "broker_status": _json_ready(broker_status),
    }


async def _paper_portfolio_payload_async(runtime: dict[str, Any], portfolio_id: int) -> dict[str, Any]:
    portfolio = runtime["get_paper_portfolio"](portfolio_id)
    if portfolio is None:
        raise HTTPException(status_code=404, detail="Paper portfolio not found.")
    broker_status = None
    if str(portfolio.get("broker_type") or "paper").lower() == "ibkr":
        adapter = await _get_broker_adapter_safe_async(runtime, portfolio_id)
        if adapter is not None:
            try:
                health = await asyncio.to_thread(adapter.health)
                broker_status = {
                    "connection": "connected" if health.get("connected") else "disconnected",
                    "market_hours": health.get("market_hours", "closed"),
                }
            except Exception:
                broker_status = {"connection": "disconnected", "market_hours": "closed"}
        else:
            broker_status = {"connection": "unavailable", "market_hours": "closed"}
    return {
        "portfolio": _json_ready(portfolio),
        "summary": _json_ready(runtime["get_paper_portfolio_summary"](portfolio_id)),
        "broker_status": _json_ready(broker_status),
    }


@router.post("/paper-portfolio")
async def regime_paper_portfolio_create(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    form = await _read_run_request(request)
    name = str(form.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=422, detail="Portfolio name is required.")
    budget = _normalize_optional_positive_float(form.get("starting_budget"), field_name="Starting budget")
    broker_type = str(form.get("broker_type", "paper") or "paper").strip().lower() or "paper"
    if broker_type not in {"paper", "ibkr"}:
        raise HTTPException(status_code=422, detail="Broker type must be paper or ibkr.")
    portfolio = runtime["create_paper_portfolio"](name, budget or 100000.0, broker_type=broker_type)
    return JSONResponse(content=_json_ready(portfolio))


@router.get("/paper-portfolio")
def regime_paper_portfolios(
    include_closed: bool = False,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    portfolios = _json_ready(runtime["list_paper_portfolios"](include_closed=include_closed))
    return JSONResponse(content={"portfolios": portfolios})


@router.get("/paper-portfolio/{portfolio_id}")
async def regime_paper_portfolio_get(
    portfolio_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    return JSONResponse(content=await _paper_portfolio_payload_async(runtime, portfolio_id))


@router.put("/paper-portfolio/{portfolio_id}")
async def regime_paper_portfolio_update(
    portfolio_id: int,
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    form = await _read_run_request(request)
    fields: dict[str, Any] = {}
    if "name" in form:
        name = str(form.get("name") or "").strip()
        if not name:
            raise HTTPException(status_code=422, detail="Portfolio name is required.")
        fields["name"] = name
    if "status" in form:
        status = str(form.get("status") or "").strip()
        if status not in {"Active", "Paused", "Closed"}:
            raise HTTPException(status_code=422, detail="Status must be Active, Paused, or Closed.")
        fields["status"] = status
    if "broker_type" in form:
        broker_type = str(form.get("broker_type") or "").strip().lower()
        if broker_type not in {"paper", "ibkr"}:
            raise HTTPException(status_code=422, detail="Broker type must be paper or ibkr.")
        fields["broker_type"] = broker_type
    if "starting_budget" in form:
        fields["starting_budget"] = _normalize_optional_positive_float(form.get("starting_budget"), field_name="Starting budget")
    updated = runtime["update_paper_portfolio"](portfolio_id, **fields)
    if updated is None:
        raise HTTPException(status_code=404, detail="Paper portfolio not found.")
    return JSONResponse(content=_json_ready(updated))


@router.delete("/paper-portfolio/{portfolio_id}")
def regime_paper_portfolio_delete(
    portfolio_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    deleted = bool(runtime["delete_paper_portfolio"](portfolio_id))
    if not deleted:
        raise HTTPException(status_code=404, detail="Paper portfolio not found.")
    return JSONResponse(content={"deleted": True})


@router.get("/paper-portfolio/{portfolio_id}/positions")
def regime_paper_positions(
    portfolio_id: int,
    status: str = "Open",
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    positions = runtime["get_paper_positions"](portfolio_id, status=status)
    return JSONResponse(content={"positions": _json_ready(positions), "summary": _json_ready(runtime["get_paper_portfolio_summary"](portfolio_id))})


@router.get("/paper-portfolio/{portfolio_id}/monitoring")
async def regime_paper_monitoring(
    portfolio_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    portfolio = runtime["get_paper_portfolio"](portfolio_id)
    if portfolio is None:
        raise HTTPException(status_code=404, detail="Paper portfolio not found.")
    adapter = await _get_broker_adapter_safe_async(runtime, portfolio_id)
    connected = False
    if adapter is not None:
        try:
            def _load_live_monitoring() -> tuple[bool, Any, Any, dict[str, Any] | None]:
                backend = getattr(getattr(adapter, "_manager", None), "backend", None)
                connected_local = bool(backend and backend.is_connected())
                if not connected_local:
                    return False, None, None, None
                return True, adapter.get_account_summary(), adapter.get_positions(), adapter.health()

            connected, summary, positions, connection = await asyncio.to_thread(_load_live_monitoring)
        except Exception as exc:
            logger.warning("Monitoring data fetch failed for portfolio %s: %s", portfolio_id, exc)
            connected = False
    if not connected:
        summary_data = runtime["get_paper_portfolio_summary"](portfolio_id)
        summary = {
            "portfolio_id": portfolio_id,
            "equity": float(summary_data.get("current_value") or portfolio.get("starting_budget") or 100000.0),
            "cash": float(portfolio.get("current_cash") or portfolio.get("starting_budget") or 100000.0),
            "buying_power": 0.0,
            "exposure_pct": float(summary_data.get("exposure_pct") or 0.0),
            "maintenance_margin": 0.0,
            "unrealized_pnl": float(summary_data.get("unrealized_pnl") or 0.0),
            "daily_pnl": 0.0,
            "net_liquidation": float(summary_data.get("current_value") or portfolio.get("starting_budget") or 100000.0),
            "total_cash": float(portfolio.get("current_cash") or portfolio.get("starting_budget") or 100000.0),
        }
        positions = [_json_ready(p) for p in runtime["get_paper_positions"](portfolio_id, status="Open")]
        connection = {"connected": False, "market_hours": "closed", "note": "IBKR connection unavailable — showing cached data"}
    pending_orders = [
        plan for plan in runtime["get_trade_plans"](portfolio_id, status="all")
        if str(plan.get("status") or "") in {"Submitted", "Partially Filled"}
    ]
    guardrails = runtime["DEFAULT_RISK_GUARDRAILS"]
    summary_exposure = float(summary.get("exposure_pct") if isinstance(summary, dict) else getattr(summary, "exposure_pct", 0.0) or 0.0)
    summary_daily_pnl = float(summary.get("daily_pnl") if isinstance(summary, dict) else getattr(summary, "daily_pnl", 0.0) or 0.0)
    max_total_exposure_pct = float(getattr(guardrails, "max_total_exposure_pct", 0.0) or 0.0)
    daily_loss_limit = float(getattr(guardrails, "daily_loss_limit", 0.0) or 0.0)
    max_trades_per_day = int(getattr(guardrails, "max_trades_per_day", 0) or 0)
    payload = {
        "account": _json_ready(summary),
        "positions": _json_ready(positions),
        "pending_orders": _json_ready(pending_orders),
        "guardrails": {
            "exposure_pct": {
                "current": summary_exposure,
                "limit": max_total_exposure_pct,
                "ok": summary_exposure <= max_total_exposure_pct if max_total_exposure_pct > 0 else True,
            },
            "daily_pnl": {
                "current": summary_daily_pnl,
                "limit": daily_loss_limit,
                "ok": abs(summary_daily_pnl) <= daily_loss_limit or summary_daily_pnl >= 0.0 if daily_loss_limit > 0 else True,
            },
            "trades_today": {
                "current": int(runtime["count_todays_trades"](portfolio_id)),
                "limit": max_trades_per_day,
                "ok": int(runtime["count_todays_trades"](portfolio_id)) <= max_trades_per_day if max_trades_per_day > 0 else True,
            },
        },
        "connection": _json_ready(connection),
        "readiness": _json_ready(runtime["validate_ibkr_readiness"]()) if "validate_ibkr_readiness" in runtime else None,
    }
    return JSONResponse(content=payload)


@router.get("/paper-portfolio/{portfolio_id}/plans")
def regime_paper_plans(
    portfolio_id: int,
    status: str = "Pending",
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    plans = runtime["get_trade_plans"](portfolio_id, status=status)
    return JSONResponse(content={"plans": _json_ready(plans)})


@router.get("/paper-portfolio/{portfolio_id}/orders/pending")
async def regime_paper_pending_orders(
    portfolio_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    portfolio = runtime["get_paper_portfolio"](portfolio_id)
    if portfolio is None:
        raise HTTPException(status_code=404, detail="Paper portfolio not found.")
    adapter = await _get_broker_adapter_safe_async(runtime, portfolio_id)
    if adapter is None:
        return JSONResponse(content={"orders": [], "changed": [], "connection": "unavailable"})
    changed = []
    if str(portfolio.get("broker_type") or "paper").lower() == "ibkr":
        changed = await asyncio.to_thread(runtime["poll_pending_orders"], adapter, portfolio_id)
    pending = [
        plan for plan in runtime["get_trade_plans"](portfolio_id, status="all")
        if str(plan.get("status") or "") in {"Submitted", "Partially Filled"}
    ]
    return JSONResponse(content={"orders": _json_ready(pending), "changed": _json_ready(changed)})


@router.post("/paper-portfolio/{portfolio_id}/orders/{plan_id}/cancel")
async def regime_paper_cancel_order(
    portfolio_id: int,
    plan_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    plan = runtime["get_trade_plan"](plan_id)
    if not plan or int(plan.get("portfolio_id") or 0) != int(portfolio_id):
        raise HTTPException(status_code=404, detail="Plan not found")
    if str(plan.get("status") or "") not in {"Submitted", "Partially Filled"}:
        raise HTTPException(status_code=400, detail=f"Cannot cancel plan in status: {plan.get('status')}")
    adapter = await _get_broker_adapter_safe_async(runtime, portfolio_id)
    if adapter is None:
        raise HTTPException(status_code=503, detail="IBKR connection unavailable.")
    broker_order_id = str(plan.get("broker_order_id") or "").strip()
    if not broker_order_id:
        raise HTTPException(status_code=400, detail="No broker order ID — plan not yet submitted")
    cancelled = await asyncio.to_thread(lambda: bool(adapter.cancel_order(broker_order_id)))
    if cancelled:
        runtime["update_trade_plan_status"](plan_id, "Cancelled", broker_status="cancelled")
        runtime["log_audit_event"](
            order_id=broker_order_id,
            portfolio_id=portfolio_id,
            event_type="cancelled",
            ticker=str(plan.get("ticker") or ""),
            action=str(plan.get("action") or ""),
            quantity=float(plan.get("quantity") or 0.0),
            actor=actor,
            details="operator_request",
        )
    return JSONResponse(content={"cancelled": cancelled, "plan_id": int(plan_id)})


@router.post("/paper-portfolio/{portfolio_id}/plans/generate")
async def regime_paper_generate_plans(
    portfolio_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    cached_regime = _cached_regime_for_paper_trading()
    saved_payload = load_payload()

    def _gen_sync() -> dict[str, Any]:
        return runtime["generate_daily_plans"](
            portfolio_id,
            cached_regime=cached_regime,
            cached_payload=saved_payload,
        )

    payload = await asyncio.to_thread(_gen_sync)
    return JSONResponse(content=_json_ready(payload))


@router.post("/paper-portfolio/{portfolio_id}/plans/precheck")
async def regime_paper_plan_precheck(
    portfolio_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    portfolio = runtime["get_paper_portfolio"](portfolio_id)
    if portfolio is None:
        raise HTTPException(status_code=404, detail="Paper portfolio not found.")
    plans = runtime["get_trade_plans"](portfolio_id, status="Pending")
    adapter = await _get_broker_adapter_safe_async(runtime, portfolio_id)
    if adapter is None:
        return JSONResponse(
            content={
                "plans": [],
                "error": "IBKR connection unavailable. Cannot validate guardrails without a live connection.",
            }
        )
    def _run_precheck_sync() -> list[dict[str, Any]]:
        fallback_adapter = None
        checked: list[dict[str, Any]] = []
        for plan in plans:
            ticker = str(plan.get("ticker") or "").upper()
            try:
                order = runtime["OrderRequest"](
                    portfolio_id=portfolio_id,
                    ticker=ticker,
                    action=str(plan.get("action") or ""),
                    quantity=float(plan.get("quantity") or 0.0),
                    limit_price=float(plan.get("proposed_price") or 0.0) or None,
                    theme_id=int(plan["theme_id"]) if plan.get("theme_id") is not None else None,
                    source=str(plan.get("source") or "manual"),
                    notes=str(plan.get("rationale") or ""),
                )
                result = runtime["validate_guardrails"](order, adapter, runtime["DEFAULT_RISK_GUARDRAILS"])
                checked.append(
                    {
                        "plan_id": int(plan["id"]),
                        "ticker": ticker,
                        "action": str(plan.get("action") or ""),
                        "guardrail_passed": bool(result.allowed),
                        "guardrail_checks": _json_ready(result.checks),
                        "guardrail_result": _json_ready(result),
                        "broker_type": str(portfolio.get("broker_type") or "paper"),
                    }
                )
            except Exception as exc:
                logger.warning(
                    "Guardrail precheck failed for portfolio %s plan %s (%s), retrying with local data: %s",
                    portfolio_id,
                    plan.get("id"),
                    ticker,
                    exc,
                )
                try:
                    if fallback_adapter is None:
                        fallback_adapter = runtime["PaperBrokerAdapter"](portfolio_id)
                    result = runtime["validate_guardrails"](order, fallback_adapter, runtime["DEFAULT_RISK_GUARDRAILS"])
                    checked.append(
                        {
                            "plan_id": int(plan["id"]),
                            "ticker": ticker,
                            "action": str(plan.get("action") or ""),
                            "guardrail_passed": bool(result.allowed),
                            "guardrail_checks": _json_ready(result.checks),
                            "guardrail_result": _json_ready(result),
                            "broker_type": "paper_fallback",
                            "fallback": True,
                        }
                    )
                except Exception as fallback_exc:
                    logger.warning(
                        "Fallback precheck also failed for plan %s (%s): %s",
                        plan.get("id"),
                        ticker,
                        fallback_exc,
                    )
                    checked.append(
                        {
                            "plan_id": int(plan["id"]),
                            "ticker": ticker,
                            "action": str(plan.get("action") or ""),
                            "guardrail_passed": False,
                            "guardrail_checks": [],
                            "guardrail_result": {"allowed": False, "error": str(fallback_exc)},
                            "broker_type": str(portfolio.get("broker_type") or "paper"),
                            "error": str(fallback_exc),
                        }
                    )
        return checked

    try:
        checked = await asyncio.to_thread(_run_precheck_sync)
    except Exception as exc:
        logger.warning("Precheck failed for portfolio %s: %s", portfolio_id, exc)
        return JSONResponse(
            content={
                "plans": [],
                "error": str(exc) or "IBKR connection unavailable. Cannot validate guardrails right now.",
            }
        )
    return JSONResponse(content={"plans": checked})


@router.put("/paper-portfolio/{portfolio_id}/plans/{plan_id}")
async def regime_paper_plan_update(
    portfolio_id: int,
    plan_id: int,
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del portfolio_id, session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    form = await _read_run_request(request)
    status = str(form.get("status") or "").strip()
    if status not in {"Approved", "Rejected", "Modified"}:
        raise HTTPException(status_code=422, detail="Status must be Approved, Rejected, or Modified.")
    fields: dict[str, Any] = {"reviewed_at": dt.datetime.now(dt.timezone.utc).isoformat()}
    if "notes" in form:
        fields["notes"] = str(form.get("notes") or "")
    if "quantity" in form:
        try:
            qty = float(str(form.get("quantity") or "").strip())
        except ValueError as exc:
            raise HTTPException(status_code=422, detail="Quantity must be a positive number.") from exc
        if qty <= 0:
            raise HTTPException(status_code=422, detail="Quantity must be a positive number.")
        fields["quantity"] = qty
    if "proposed_price" in form:
        fields["proposed_price"] = _normalize_optional_positive_float(form.get("proposed_price"), field_name="Proposed price")
    plan = runtime["update_trade_plan_status"](plan_id, status, **fields)
    if plan is None:
        raise HTTPException(status_code=404, detail="Trade plan not found.")
    return JSONResponse(content=_json_ready(plan))


@router.post("/paper-portfolio/{portfolio_id}/plans/execute")
async def regime_paper_execute(
    portfolio_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    portfolio = runtime["get_paper_portfolio"](portfolio_id)
    if portfolio is None:
        raise HTTPException(status_code=404, detail="Paper portfolio not found.")
    status = str(portfolio.get("status") or "")
    if status == "Paused":
        raise HTTPException(status_code=409, detail="Paper portfolio is paused.")
    if status == "Closed":
        raise HTTPException(status_code=409, detail="Paper portfolio is closed.")
    adapter = await _get_broker_adapter_safe_async(runtime, portfolio_id)
    if adapter is None:
        return JSONResponse(content={"executed": [], "errors": ["IBKR connection unavailable."], "submitted": 0, "filled": 0})
    used_fallback = False
    try:
        payload = await asyncio.to_thread(
            runtime["execute_approved_plans_via_adapter"],
            portfolio_id,
            adapter,
            guardrails=runtime["DEFAULT_RISK_GUARDRAILS"],
            actor="user",
        )
    except Exception as exc:
        logger.warning(
            "Execute via IBKR adapter failed for portfolio %s, retrying with local paper adapter: %s",
            portfolio_id,
            exc,
        )
        fallback_adapter = runtime["PaperBrokerAdapter"](portfolio_id)
        payload = await asyncio.to_thread(
            runtime["execute_approved_plans_via_adapter"],
            portfolio_id,
            fallback_adapter,
            guardrails=runtime["DEFAULT_RISK_GUARDRAILS"],
            actor="user",
        )
        used_fallback = True

    if not used_fallback:
        ibkr_adapter_cls = runtime.get("IBKRBrokerAdapter")
        if ibkr_adapter_cls is not None and isinstance(adapter, ibkr_adapter_cls):
            await asyncio.sleep(1)
            poll_results = await asyncio.to_thread(runtime["poll_pending_orders"], adapter, portfolio_id)
            payload["immediate_fills"] = len([row for row in poll_results if str(getattr(row, "status", "")).lower() == "filled"])
    if used_fallback:
        payload["fallback"] = True
        payload["fallback_reason"] = "IBKR adapter unavailable in thread context; executed with local paper adapter."
    return JSONResponse(content=_json_ready(payload))


@router.post("/paper-portfolio/{portfolio_id}/kill-switch")
async def regime_paper_kill_switch(
    portfolio_id: int,
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    form = await _read_run_request(request)
    reason = str(form.get("reason") or "Manual kill switch activated").strip() or "Manual kill switch activated"
    payload = runtime["kill_switch"](portfolio_id, actor="user", reason=reason)
    if payload is None:
        raise HTTPException(status_code=404, detail="Paper portfolio not found.")
    return JSONResponse(content=_json_ready(payload))


@router.post("/paper-portfolio/{portfolio_id}/auto-approve")
def regime_paper_auto_approve(
    portfolio_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    _require_paper_portfolio(runtime, portfolio_id)
    result = runtime["auto_approve_plans"](portfolio_id)
    return JSONResponse(content=_json_ready(result))


@router.get("/paper-portfolio/{portfolio_id}/autonomy/status")
def regime_paper_autonomy_status(
    portfolio_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    portfolio = _require_paper_portfolio(runtime, portfolio_id)
    summary = runtime["get_paper_portfolio_summary"](portfolio_id)
    daily_summary = runtime["get_daily_audit_summary"](portfolio_id)
    capital_deployed = runtime["get_daily_capital_deployed"](portfolio_id)
    ceiling_pct = runtime["get_daily_capital_ceiling_pct"]()
    equity = float(summary.get("total_equity") or 0.0)
    if equity <= 0:
        equity = float(summary.get("current_cash") or 0.0) + float(summary.get("total_market_value") or 0.0)
    if equity <= 0:
        equity = float(summary.get("current_value") or portfolio.get("starting_budget") or 0.0)
    max_daily_capital = equity * ceiling_pct
    return JSONResponse(
        content=_json_ready(
            {
                "operating_mode": runtime["get_operating_mode"](),
                "auto_approve_threshold": runtime["get_auto_approve_threshold"](),
                "daily_capital_ceiling_pct": ceiling_pct,
                "max_daily_capital": max_daily_capital,
                "capital_deployed_today": capital_deployed,
                "capital_remaining": max(0.0, max_daily_capital - capital_deployed),
                "trades_today": int(daily_summary.get("trades_today") or 0),
                "auto_approved_today": int((daily_summary.get("counts") or {}).get("auto_approved", 0)),
                "guardrail_blocks_today": int(daily_summary.get("guardrail_blocks") or 0),
            }
        )
    )


@router.get("/paper-portfolio/{portfolio_id}/performance")
def regime_paper_performance(
    portfolio_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    performance = runtime["compute_paper_performance"](portfolio_id)
    benchmark = performance.get("benchmark") if isinstance(performance, dict) else None
    if not benchmark:
        benchmark = runtime["compute_benchmark_comparison"](portfolio_id)
    return JSONResponse(content={"performance": _json_ready(performance), "benchmark": _json_ready(benchmark)})


def _require_paper_portfolio(runtime: dict[str, Any], portfolio_id: int) -> dict[str, Any]:
    portfolio = runtime["get_paper_portfolio"](portfolio_id)
    if portfolio is None:
        raise HTTPException(status_code=404, detail="Paper portfolio not found.")
    return portfolio


@router.get("/paper-portfolio/{portfolio_id}/attribution/theme")
def regime_paper_attribution_theme(
    portfolio_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    _require_paper_portfolio(runtime, portfolio_id)
    return JSONResponse(content=_json_ready(runtime["compute_theme_attribution"](portfolio_id)))


@router.get("/paper-portfolio/{portfolio_id}/attribution/source")
def regime_paper_attribution_source(
    portfolio_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    _require_paper_portfolio(runtime, portfolio_id)
    return JSONResponse(content=_json_ready(runtime["compute_source_attribution"](portfolio_id)))


@router.get("/paper-portfolio/{portfolio_id}/attribution/regime")
def regime_paper_attribution_regime(
    portfolio_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    _require_paper_portfolio(runtime, portfolio_id)
    return JSONResponse(content=_json_ready(runtime["compute_regime_attribution"](portfolio_id)))


@router.get("/paper-portfolio/{portfolio_id}/attribution/ml")
def regime_paper_attribution_ml(
    portfolio_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    _require_paper_portfolio(runtime, portfolio_id)
    return JSONResponse(content=_json_ready(runtime["compute_ml_accuracy"](portfolio_id)))


@router.get("/paper-portfolio/{portfolio_id}/attribution/summary")
def regime_paper_attribution_summary(
    portfolio_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    portfolio = _require_paper_portfolio(runtime, portfolio_id)
    performance = runtime["compute_paper_performance"](portfolio_id)
    payload = runtime["compute_attribution_summary"](portfolio_id, performance=performance)
    payload["portfolio"] = portfolio
    return JSONResponse(content=_json_ready(payload))


@router.get("/paper-portfolio/{portfolio_id}/budget")
def regime_paper_budget(
    portfolio_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    budget = runtime["allocate_budget"](portfolio_id)
    return JSONResponse(content=_json_ready(budget))


@router.get("/paper-portfolio/{portfolio_id}/audit")
def regime_paper_audit(
    portfolio_id: int,
    order_id: str | None = None,
    ticker: str | None = None,
    event_type: str | None = None,
    days: int = 30,
    limit: int = 200,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    trail = runtime["get_audit_trail"](
        portfolio_id=portfolio_id,
        order_id=order_id,
        ticker=ticker,
        event_type=event_type,
        days=days,
        limit=limit,
    )
    summary = runtime["get_daily_audit_summary"](portfolio_id)
    return JSONResponse(content={"audit": _json_ready(trail), "summary": _json_ready(summary)})


@router.get("/paper-portfolio/{portfolio_id}/audit/summary")
def regime_paper_audit_summary(
    portfolio_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    summary = runtime["get_daily_audit_summary"](portfolio_id)
    return JSONResponse(content=_json_ready(summary))


@router.get("/theses")
def regime_theses(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    return JSONResponse(content={"theses": _load_theses(runtime)})


@router.get("/thesis/{ticker}")
def regime_thesis_get(
    ticker: str,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    thesis = runtime["upsert_thesis"](ticker, None)
    return JSONResponse(content={"ticker": ticker.upper(), "thesis": thesis})


@router.post("/thesis/{ticker}")
async def regime_thesis_save(
    ticker: str,
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    form = await _read_run_request(request)
    thesis = _normalize_thesis_text(form.get("thesis", ""))
    saved = runtime["upsert_thesis"](ticker, thesis)
    return JSONResponse(content={"ticker": ticker.upper(), "thesis": saved})


@router.delete("/thesis/{ticker}")
def regime_thesis_delete(
    ticker: str,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    deleted = bool(runtime["delete_thesis"](ticker))
    return JSONResponse(content={"ticker": ticker.upper(), "deleted": deleted})


@router.post("/run")
async def regime_run(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")

    form = await _read_run_request(request)
    tickers = str(form.get("tickers", ""))
    benchmark = str(form.get("benchmark", "SOXX"))
    period = str(form.get("period", "3y"))
    show_all = str(form.get("show_all", "")).strip().lower() in {"1", "true", "on", "yes"}
    frontier_enabled = str(form.get("frontier_enabled", "")).strip().lower() in {"1", "true", "on", "yes"}
    frontier_provider = str(form.get("frontier_provider", "auto") or "auto").strip().lower() or "auto"
    raw_batch_size = str(form.get("frontier_batch_size", _DEFAULT_FRONTIER_BATCH_SIZE) or _DEFAULT_FRONTIER_BATCH_SIZE).strip()
    frontier_batch_size = int(raw_batch_size) if raw_batch_size.isdigit() else _DEFAULT_FRONTIER_BATCH_SIZE
    portfolio_scope = str(form.get("portfolio_scope", "household") or "household").strip().lower() or "household"
    account_id = _parse_account_id(form.get("account_id"))
    force_refresh = str(form.get("force_refresh", "")).strip().lower() in {"1", "true", "on", "yes"}
    selected_tickers = _normalize_selected_tickers(tickers)
    if not selected_tickers:
        raise HTTPException(status_code=422, detail="Select at least one ticker.")
    if len(selected_tickers) > _MAX_TICKERS:
        raise HTTPException(status_code=422, detail=f"Select no more than {_MAX_TICKERS} tickers per run.")

    job = _submit_regime_job(
        tickers=selected_tickers,
        benchmark=(benchmark or "SOXX").strip().upper() or "SOXX",
        period=(period or "3y").strip() or "3y",
        show_all=show_all,
        frontier_enabled=frontier_enabled,
        portfolio_scope=portfolio_scope,
        account_id=account_id,
        frontier_provider=frontier_provider,
        frontier_batch_size=frontier_batch_size,
        force_refresh=force_refresh,
    )
    return JSONResponse(content={"job_id": job.job_id, "status": job.status})


@router.get("/status/{job_id}")
def regime_status(
    job_id: str,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    _prune_jobs()
    with _JOBS_LOCK:
        job = _JOBS.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Unknown regime job.")
        payload = _serialize_job(job)
    return JSONResponse(content=payload)


@router.get("/effectiveness")
def regime_effectiveness(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    get_signal_effectiveness_fn = runtime.get("get_signal_effectiveness")
    if not callable(get_signal_effectiveness_fn):
        return JSONResponse(content=_default_effectiveness())
    return JSONResponse(content=_json_ready(get_signal_effectiveness_fn()))


@router.get("/backtest/{ticker}")
def regime_backtest(
    ticker: str,
    period: str = "5y",
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    cached = load_backtest_cache(ticker, period)
    if cached is not None:
        return JSONResponse(content={"ticker": ticker.upper(), "period": period, "cached": True, "result": cached})
    run_backtest_fn = runtime.get("run_backtest")
    compare_fn = runtime.get("compare_to_benchmark")
    if not callable(run_backtest_fn):
        raise HTTPException(status_code=501, detail="Backtest support is unavailable.")
    raw_result = run_backtest_fn(ticker=ticker, period=period)
    result = _json_ready(raw_result)
    comparison = _json_ready(compare_fn(raw_result, benchmark_ticker="SPY", period=period)) if callable(compare_fn) else None
    payload = {"ticker": ticker.upper(), "period": period, "result": result, "comparison": comparison}
    save_backtest_cache(ticker, period, payload)
    return JSONResponse(content={"ticker": ticker.upper(), "period": period, "cached": False, **payload})


@router.get("/alerts")
def regime_alerts(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    return JSONResponse(content={"alerts": _fetch_recent_alerts(days=7), "count": len(_fetch_recent_alerts(days=7))})


@router.get("/journal")
def regime_journal(
    ticker: str = "",
    limit: int = 50,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    fn = runtime.get("get_transition_journal")
    if not callable(fn):
        return JSONResponse(content={"rows": []})
    return JSONResponse(content={"rows": _json_ready(fn(ticker=ticker or None, limit=max(1, min(int(limit), 250))))})


@router.get("/journal/stats")
def regime_journal_stats(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    runtime, runtime_error = _load_hmm_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail=runtime_error or "Regime analytics are unavailable.")
    fn = runtime.get("get_transition_statistics")
    if not callable(fn):
        return JSONResponse(content={"rows": []})
    return JSONResponse(content=_json_ready(fn()))


@router.get("/digest")
# DEPRECATED: retained for backward compatibility; the UI now uses /export-pdf.
def regime_digest(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    benchmark: str = "SOXX",
    period: str = "3y",
    show_all: bool = False,
    format: str = "html",
    tickers: str = "",
):
    del session
    payload = build_digest_response_payload(
        benchmark=benchmark,
        period=period,
        show_all=show_all,
        tickers=_normalize_selected_tickers(tickers),
    )
    if (format or "").strip().lower() == "json":
        return JSONResponse(content=_json_ready(payload["digest"]))

    from src.app.main import templates

    return templates.TemplateResponse(
        "regime_digest.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": None,
            "auth_banner_detail": auth_banner_message(),
            "static_version": _static_version(),
            "title": "Regime Digest",
            "page_badge": "Weekly Digest",
            "warnings": payload["warnings"],
            "digest": payload["digest"],
            "benchmark": payload["benchmark"],
            "period": payload["period"],
            "show_all": payload["show_all"],
            "portfolio_count": payload["portfolio_count"],
            "action_items_count": payload["action_items_count"],
            "last_run_display": payload["last_run_display"],
            "benchmark_regime": payload["benchmark_regime"],
            "benchmark_regime_tone": payload["benchmark_regime_tone"],
            "portfolio_mode": payload["portfolio_mode"],
            "hmm_available": payload["hmm_available"],
            "selected_tickers": payload["selected_tickers"],
            "selected_count": payload["selected_count"],
            "page_context": (
                f"<b>Benchmark:</b> {payload['benchmark']} <span class='ui-muted'>·</span> "
                f"<b>Period:</b> {payload['period']} <span class='ui-muted'>·</span> "
                f"<b>Mode:</b> {payload['portfolio_mode']} <span class='ui-muted'>·</span> "
                f"<b>Tickers:</b> {payload['selected_count'] or payload['portfolio_count']}"
            ),
        },
    )


@router.get("/export-pdf")
def regime_export_pdf(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    del session, actor
    payload = load_payload()
    if payload is None:
        raise HTTPException(status_code=404, detail="No analysis results available. Run an analysis first.")
    from src.app.routes.regime_pdf import generate_regime_pdf
    from starlette.responses import Response

    pdf_bytes = generate_regime_pdf(payload)
    scope = str(payload.get("portfolio_scope") or "portfolio").replace(" ", "_")
    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M")
    filename = f"regime_report_{scope}_{timestamp}.pdf"
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename=\"{filename}\"'},
    )
