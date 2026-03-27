from __future__ import annotations

import datetime as dt
import logging
import math
from dataclasses import asdict
from typing import Any

import pandas as pd
import yfinance as yf

from .broker_adapter import BrokerAdapter, OrderRequest, PaperBrokerAdapter, submit_guarded_order
from .config import (
    DEFAULT_PAPER_TRADING_CONFIG,
    DEFAULT_RISK_GUARDRAILS,
    PaperTradingConfig,
    RiskGuardrails,
)
from .discovery import _quick_regime_screen
from .persistence import (
    close_paper_position,
    count_todays_trades,
    create_trade_plan,
    get_daily_snapshots,
    get_paper_portfolio,
    get_paper_portfolio_summary,
    get_paper_positions,
    get_trade_plans,
    get_watchlist,
    get_watchlist_by_ticker,
    log_audit_event,
    list_paper_portfolios,
    list_themes,
    open_paper_position,
    save_daily_snapshot,
    update_paper_portfolio,
    update_trade_plan_status,
)
from .ib_types import ET

logger = logging.getLogger(__name__)

CachedRegimeValue = tuple[str, float] | dict[str, Any]
CachedRegimeMap = dict[str, CachedRegimeValue]


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


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _theme_map() -> dict[int, dict[str, Any]]:
    return {int(theme.get("id") or 0): theme for theme in list_themes(include_closed=False)}


def _parse_timestamp(raw: Any) -> dt.datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        return dt.datetime.fromisoformat(text)
    except ValueError:
        return None


def _cached_regime_map(cached_regime: CachedRegimeMap | dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if not isinstance(cached_regime, dict):
        return {}
    if "rows" in cached_regime:
        rows = cached_regime.get("rows") or []
        return {
            str(row.get("ticker") or "").upper(): row
            for row in rows
            if isinstance(row, dict) and str(row.get("ticker") or "").strip()
        }
    mapped: dict[str, dict[str, Any]] = {}
    for ticker, value in cached_regime.items():
        symbol = str(ticker or "").strip().upper()
        if not symbol:
            continue
        if isinstance(value, dict):
            mapped[symbol] = value
        elif isinstance(value, (tuple, list)) and len(value) >= 2:
            mapped[symbol] = {"regime": value[0], "probability": value[1]}
    return mapped


def _batch_current_prices(tickers: list[str]) -> dict[str, float]:
    normalized = []
    seen: set[str] = set()
    for ticker in tickers:
        symbol = str(ticker or "").strip().upper()
        if symbol and symbol not in seen:
            seen.add(symbol)
            normalized.append(symbol)
    if not normalized:
        return {}
    try:
        frame = yf.download(
            tickers=normalized if len(normalized) > 1 else normalized[0],
            period="5d",
            interval="1d",
            auto_adjust=False,
            progress=False,
            threads=False,
            group_by="column",
        )
    except Exception as exc:
        logger.warning("Batch price download failed for paper trading.", exc_info=exc)
        frame = None
    prices: dict[str, float] = {}
    if frame is not None and not getattr(frame, "empty", True):
        try:
            if isinstance(frame.columns, pd.MultiIndex):
                close_frame = frame["Close"] if "Close" in frame.columns.get_level_values(0) else None
                if close_frame is not None:
                    for ticker in normalized:
                        series = close_frame.get(ticker)
                        if series is None:
                            continue
                        cleaned = series.dropna()
                        if not cleaned.empty:
                            prices[ticker] = float(cleaned.iloc[-1])
            else:
                close_series = frame["Close"] if "Close" in frame.columns else None
                if close_series is not None:
                    cleaned = close_series.dropna()
                    if not cleaned.empty:
                        prices[normalized[0]] = float(cleaned.iloc[-1])
        except Exception as exc:
            logger.warning("Unable to parse batch prices for paper trading.", exc_info=exc)
    missing = [ticker for ticker in normalized if ticker not in prices]
    for ticker in missing:
        try:
            history = yf.Ticker(ticker).history(period="5d", interval="1d", auto_adjust=False)
            if history is not None and not history.empty and "Close" in history.columns:
                cleaned = history["Close"].dropna()
                if not cleaned.empty:
                    prices[ticker] = float(cleaned.iloc[-1])
                    continue
        except Exception as exc:
            logger.debug("Ticker history fallback failed for %s.", ticker, exc_info=exc)
        try:
            info_price = yf.Ticker(ticker).info.get("currentPrice")
            if info_price is not None:
                prices[ticker] = float(info_price)
        except Exception as exc:
            logger.debug("Ticker info fallback failed for %s.", ticker, exc_info=exc)
    return prices


def _pending_plan_index(portfolio_id: int, action: str) -> set[str]:
    return {
        str(plan.get("ticker") or "").upper()
        for plan in get_trade_plans(portfolio_id, status="Pending")
        if str(plan.get("action") or "") == action
    }


def _open_position_index(portfolio_id: int) -> dict[str, list[dict[str, Any]]]:
    by_ticker: dict[str, list[dict[str, Any]]] = {}
    for row in get_paper_positions(portfolio_id, status="Open"):
        by_ticker.setdefault(str(row.get("ticker") or "").upper(), []).append(row)
    return by_ticker


def generate_buy_plans(
    portfolio_id: int,
    *,
    config: PaperTradingConfig = DEFAULT_PAPER_TRADING_CONFIG,
) -> list[dict[str, Any]]:
    portfolio = get_paper_portfolio(portfolio_id)
    if portfolio is None:
        return []
    allocation = allocate_budget(portfolio_id, config=config)
    theme_budgets = {int(item["theme_id"]): item for item in allocation.get("themes", [])}
    pending_buys = _pending_plan_index(portfolio_id, "Buy")
    open_positions = _open_position_index(portfolio_id)
    created: list[dict[str, Any]] = []
    for item in get_watchlist(status="Entry Signal"):
        ticker = str(item.get("ticker") or "").upper()
        if not ticker or ticker in pending_buys or ticker in open_positions:
            continue
        theme_id = int(item.get("theme_id") or 0)
        theme_budget = theme_budgets.get(theme_id)
        if not theme_budget:
            continue
        role = str(item.get("suggested_role") or "Critical-Path")
        role_budget = float((theme_budget.get("by_role") or {}).get(role) or 0.0)
        proposed_price = float(item.get("suggested_entry_price") or 0.0)
        if role_budget <= 0 or proposed_price <= 0:
            continue
        quantity = math.floor(role_budget / proposed_price)
        if quantity <= 0:
            continue
        rationale = (
            f"Entry Signal from discovery watchlist. "
            f"{item.get('discovery_rationale') or 'Candidate meets paper-trading entry criteria.'}"
        )
        created.append(
            create_trade_plan(
                portfolio_id,
                ticker,
                "Buy",
                quantity,
                rationale,
                theme_id=theme_id or None,
                proposed_price=proposed_price,
                regime_label=str(item.get("regime_label") or ""),
                regime_probability=float(item.get("regime_probability") or 0.0) if item.get("regime_probability") is not None else None,
                crowd_score=int(item.get("crowd_score")) if item.get("crowd_score") is not None else None,
                source="discovery",
            )
        )
    return created


def generate_exit_plans(
    portfolio_id: int,
    *,
    cached_regime: CachedRegimeMap | dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    portfolio = get_paper_portfolio(portfolio_id)
    if portfolio is None:
        return []
    pending_sells = _pending_plan_index(portfolio_id, "Sell")
    open_positions = get_paper_positions(portfolio_id, status="Open")
    if not open_positions:
        return []
    prices = _batch_current_prices([str(row.get("ticker") or "") for row in open_positions])
    cached_rows = _cached_regime_map(cached_regime)
    created: list[dict[str, Any]] = []
    for position in open_positions:
        ticker = str(position.get("ticker") or "").upper()
        if not ticker or ticker in pending_sells:
            continue
        current_price = prices.get(ticker)
        stop_price = float(position.get("stop_price") or 0.0)
        trigger_reason: str | None = None
        regime_label: str | None = None
        regime_probability: float | None = None
        if current_price is not None and stop_price > 0 and current_price <= stop_price:
            trigger_reason = f"Stop price hit (${current_price:.2f} <= ${stop_price:.2f})."
        row = cached_rows.get(ticker)
        if row:
            regime_label = str(row.get("regime") or "").strip() or None
            try:
                regime_probability = float(row.get("probability")) if row.get("probability") is not None else None
            except Exception:
                regime_probability = None
            action = str(row.get("composite_signal") or "").strip()
            if trigger_reason is None and regime_label == "Bear":
                trigger_reason = "Cached regime is Bear."
            if trigger_reason is None and action in {"Sell", "Strong Sell"}:
                trigger_reason = f"Cached composite signal is {action}."
        if trigger_reason is None:
            try:
                quick_label, quick_prob, _entry, _stop = _quick_regime_screen(ticker)
                regime_label = quick_label
                regime_probability = quick_prob
                if quick_label == "Bear":
                    trigger_reason = "Fallback regime screen flipped to Bear."
            except Exception as exc:
                logger.warning("Fallback regime screen failed for paper exit plan %s.", ticker, exc_info=exc)
        if trigger_reason is None:
            continue
        quantity = float(position.get("quantity") or 0.0)
        if quantity <= 0:
            continue
        proposed_price = current_price or float(position.get("entry_price") or 0.0)
        created.append(
            create_trade_plan(
                portfolio_id,
                ticker,
                "Sell",
                quantity,
                trigger_reason,
                theme_id=int(position["theme_id"]) if position.get("theme_id") is not None else None,
                proposed_price=proposed_price if proposed_price > 0 else None,
                regime_label=regime_label,
                regime_probability=regime_probability,
                source="exit_signal",
            )
        )
    return created


def generate_daily_plans(
    portfolio_id: int,
    *,
    cached_regime: CachedRegimeMap | dict[str, Any] | None = None,
    config: PaperTradingConfig = DEFAULT_PAPER_TRADING_CONFIG,
) -> dict[str, Any]:
    buy_plans = generate_buy_plans(portfolio_id, config=config)
    exit_plans = generate_exit_plans(portfolio_id, cached_regime=cached_regime)
    return {
        "buy_plans": buy_plans,
        "exit_plans": exit_plans,
        "created_count": len(buy_plans) + len(exit_plans),
        "generated_at": _now().isoformat(),
    }


def execute_approved_plans(portfolio_id: int) -> dict[str, Any]:
    adapter = PaperBrokerAdapter(portfolio_id)
    return execute_approved_plans_via_adapter(portfolio_id, adapter)


def execute_approved_plans_via_adapter(
    portfolio_id: int,
    adapter: BrokerAdapter,
    *,
    guardrails: RiskGuardrails = DEFAULT_RISK_GUARDRAILS,
    actor: str = "user",
) -> dict[str, Any]:
    portfolio = get_paper_portfolio(portfolio_id)
    if portfolio is None:
        return {"executed": [], "skipped": [], "portfolio": None}
    approved = get_trade_plans(portfolio_id, status="Approved")
    if not approved:
        return {"executed": [], "skipped": [], "portfolio": portfolio}
    executed: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for plan in approved:
        plan_id = int(plan["id"])
        ticker = str(plan.get("ticker") or "").upper()
        action = str(plan.get("action") or "")
        quantity = float(plan.get("quantity") or 0.0)
        related_watchlist = get_watchlist_by_ticker(ticker)
        stop_price = None
        role = str(plan.get("role") or "Critical-Path")
        if related_watchlist:
            latest = related_watchlist[0]
            stop_price = latest.get("suggested_stop_price")
            role = str(latest.get("suggested_role") or role)
        order = OrderRequest(
            portfolio_id=portfolio_id,
            ticker=ticker,
            action=action,
            quantity=quantity,
            limit_price=float(plan.get("proposed_price") or 0.0) or None,
            stop_price=float(stop_price) if stop_price is not None else None,
            theme_id=int(plan["theme_id"]) if plan.get("theme_id") is not None else None,
            role=role,
            source=str(plan.get("source") or "manual"),
            notes=str(plan.get("rationale") or ""),
        )
        guardrail_result, result = submit_guarded_order(order, adapter, guardrails=guardrails, actor=actor)
        if result is None:
            note = "; ".join(check.message for check in guardrail_result.checks if not check.passed) or "Blocked by guardrails."
            update_trade_plan_status(plan_id, "Rejected", notes=note, reviewed_at=_now().isoformat())
            skipped.append(
                {
                    "plan_id": plan_id,
                    "ticker": ticker,
                    "reason": note,
                    "status": "guardrail_blocked",
                    "guardrail_result": asdict(guardrail_result),
                }
            )
            continue
        normalized_status = str(result.status or "").lower()
        if normalized_status in {"submitted", "pending", "partially_filled"}:
            mapped_status = "Partially Filled" if normalized_status == "partially_filled" else "Submitted"
            updated_plan = update_trade_plan_status(
                plan_id,
                mapped_status,
                reviewed_at=plan.get("reviewed_at") or _now().isoformat(),
                broker_order_id=result.order_id,
                broker_status=result.status,
                filled_quantity=float(result.quantity or 0.0) if normalized_status == "partially_filled" else 0.0,
                notes=result.message or "",
            )
            executed.append(
                {
                    "plan_id": plan_id,
                    "ticker": ticker,
                    "action": action,
                    "execution_price": result.filled_price,
                    "quantity": quantity,
                    "order_id": result.order_id,
                    "status": result.status,
                    "guardrail_result": asdict(guardrail_result),
                    "plan": updated_plan,
                }
            )
            continue
        if normalized_status != "filled":
            mapped_status = "Cancelled" if normalized_status == "cancelled" else "Rejected"
            update_trade_plan_status(
                plan_id,
                mapped_status,
                notes=result.message or "Adapter rejected order.",
                reviewed_at=_now().isoformat(),
                broker_order_id=result.order_id,
                broker_status=result.status,
            )
            skipped.append(
                {
                    "plan_id": plan_id,
                    "ticker": ticker,
                    "reason": result.message or "Adapter rejected order.",
                    "status": result.status,
                    "guardrail_result": asdict(guardrail_result),
                }
            )
            continue
        if not isinstance(adapter, PaperBrokerAdapter):
            _apply_filled_execution(
                portfolio_id,
                plan,
                result,
            )
        update_trade_plan_status(
            plan_id,
            "Executed",
            executed_at=result.filled_at or _now().isoformat(),
            execution_price=result.filled_price,
            reviewed_at=plan.get("reviewed_at") or _now().isoformat(),
            broker_order_id=result.order_id,
            broker_status=result.status,
            filled_quantity=quantity,
        )
        executed.append(
            {
                "plan_id": plan_id,
                "ticker": ticker,
                "action": action,
                "execution_price": result.filled_price,
                "quantity": quantity,
                "order_id": result.order_id,
                "guardrail_result": asdict(guardrail_result),
            }
        )
    return {"executed": executed, "skipped": skipped, "portfolio": get_paper_portfolio(portfolio_id)}


def expire_stale_plans(portfolio_id: int | None = None, *, max_age_days: int = 2) -> int:
    portfolios = [get_paper_portfolio(portfolio_id)] if portfolio_id is not None else list_paper_portfolios(include_closed=False)
    cutoff = _now() - dt.timedelta(days=max_age_days)
    expired = 0
    for portfolio in portfolios:
        if not portfolio:
            continue
        for plan in get_trade_plans(int(portfolio["id"]), status="Pending"):
            created_at = _parse_timestamp(plan.get("created_at"))
            if created_at is None or created_at > cutoff:
                continue
            if update_trade_plan_status(int(plan["id"]), "Expired"):
                expired += 1
    return expired


def compute_benchmark_comparison(
    portfolio_id: int,
    benchmark_ticker: str = "SPY",
    *,
    benchmark_data: pd.DataFrame | None = None,
) -> dict[str, Any]:
    portfolio = get_paper_portfolio(portfolio_id)
    if portfolio is None:
        return {}
    started_at = _parse_timestamp(portfolio.get("created_at")) or _now()
    days = max(30, (_now() - started_at).days + 5)
    try:
        frame = benchmark_data
        if frame is None:
            frame = yf.download(
                benchmark_ticker,
                period=f"{days}d",
                interval="1d",
                auto_adjust=False,
                progress=False,
                threads=False,
            )
        close = frame["Close"].dropna() if frame is not None and not frame.empty and "Close" in frame.columns else pd.Series(dtype=float)
        _close_first = float(close.iloc[0].item() if hasattr(close.iloc[0], "item") else close.iloc[0]) if len(close) else 0.0
        _close_last = float(close.iloc[-1].item() if hasattr(close.iloc[-1], "item") else close.iloc[-1]) if len(close) else 0.0
        benchmark_return = float((_close_last - _close_first) / _close_first) if len(close) >= 2 and _close_first else None
    except Exception as exc:
        logger.warning("Unable to compute paper-trading benchmark comparison.", exc_info=exc)
        benchmark_return = None
    summary = get_paper_portfolio_summary(portfolio_id)
    portfolio_return = float(summary.get("total_return_pct") or 0.0) / 100.0
    alpha = (portfolio_return - benchmark_return) if benchmark_return is not None else None
    return {
        "benchmark_ticker": benchmark_ticker,
        "benchmark_return": benchmark_return,
        "benchmark_return_pct": (benchmark_return * 100.0) if benchmark_return is not None else None,
        "portfolio_return": portfolio_return,
        "paper_return_pct": portfolio_return * 100.0,
        "alpha": alpha,
        "alpha_pct": (alpha * 100.0) if alpha is not None else None,
    }


def compute_paper_performance(portfolio_id: int) -> dict[str, Any]:
    portfolio = get_paper_portfolio(portfolio_id)
    if portfolio is None:
        return {}
    summary = get_paper_portfolio_summary(portfolio_id)
    open_positions = get_paper_positions(portfolio_id, status="Open")
    closed_positions = get_paper_positions(portfolio_id, status="Closed")
    prices = _batch_current_prices([str(row.get("ticker") or "") for row in open_positions])
    market_value = 0.0
    unrealized_pnl = 0.0
    marked_positions: list[dict[str, Any]] = []
    for row in open_positions:
        ticker = str(row.get("ticker") or "").upper()
        quantity = float(row.get("quantity") or 0.0)
        entry_price = float(row.get("entry_price") or 0.0)
        current_price = prices.get(ticker, entry_price)
        value = quantity * current_price
        pnl = (current_price - entry_price) * quantity
        market_value += value
        unrealized_pnl += pnl
        marked_positions.append({**row, "current_price": current_price, "market_value": value, "unrealized_pnl": pnl})
    realized_pnl = sum(float(row.get("realized_pnl") or 0.0) for row in closed_positions)
    total_equity = float(portfolio.get("current_cash") or 0.0) + market_value
    starting_budget = float(portfolio.get("starting_budget") or 0.0)
    total_return_pct = ((total_equity - starting_budget) / starting_budget * 100.0) if starting_budget > 0 else 0.0
    wins = [row for row in closed_positions if float(row.get("realized_pnl") or 0.0) > 0]
    started_at = _parse_timestamp(portfolio.get("created_at")) or _now()
    days = max(30, (_now() - started_at).days + 5)
    benchmark_data = None
    try:
        benchmark_data = yf.download(
            "SPY",
            period=f"{days}d",
            interval="1d",
            auto_adjust=False,
            progress=False,
            threads=False,
        )
    except Exception as exc:
        logger.warning("Unable to prefetch benchmark data for paper trading.", exc_info=exc)
    try:
        benchmark = compute_benchmark_comparison(portfolio_id, benchmark_data=benchmark_data)
    except TypeError:
        benchmark = compute_benchmark_comparison(portfolio_id)
    snapshots = get_daily_snapshots(portfolio_id)
    return {
        **summary,
        "portfolio_id": portfolio_id,
        "positions": _serialize_rows(marked_positions),
        "current_cash": float(portfolio.get("current_cash") or 0.0),
        "total_market_value": market_value,
        "unrealized_pnl": unrealized_pnl,
        "realized_pnl": realized_pnl,
        "total_equity": total_equity,
        "total_return_pct": total_return_pct,
        "win_rate": (len(wins) / len(closed_positions)) if closed_positions else None,
        "closed_trade_count": len(closed_positions),
        "benchmark": benchmark,
        "snapshots": snapshots,
    }


def get_paper_dashboard(portfolio_id: int, *, cached_regime: dict[str, Any] | None = None) -> dict[str, Any]:
    portfolio = get_paper_portfolio(portfolio_id)
    if portfolio is None:
        return {}
    return {
        "portfolio": portfolio,
        "allocation": allocate_budget(portfolio_id),
        "summary": get_paper_portfolio_summary(portfolio_id),
        "positions": get_paper_positions(portfolio_id, status="all"),
        "plans": get_trade_plans(portfolio_id, status="all"),
        "performance": compute_paper_performance(portfolio_id),
        "cached_regime_available": bool(_cached_regime_map(cached_regime)),
    }


def compute_daily_snapshot(portfolio_id: int) -> dict[str, Any]:
    summary = get_paper_portfolio_summary(portfolio_id)
    portfolio = get_paper_portfolio(portfolio_id)
    if portfolio is None or not summary:
        return {}
    positions = get_paper_positions(portfolio_id, status="Open")
    return {
        "snapshot_date": dt.datetime.now(ET).date().isoformat(),
        "portfolio_id": int(portfolio_id),
        "equity": float(summary.get("current_cash") or 0.0) + float(summary.get("total_market_value") or 0.0),
        "cash": float(summary.get("current_cash") or 0.0),
        "market_value": float(summary.get("total_market_value") or 0.0),
        "realized_pnl": float(summary.get("realized_pnl") or 0.0),
        "unrealized_pnl": float(summary.get("unrealized_pnl") or 0.0),
        "position_count": len(positions),
        "trades_today": count_todays_trades(portfolio_id),
    }


def record_trade_outcome(portfolio_id: int, position: dict[str, Any], close_price: float) -> dict[str, Any]:
    del portfolio_id
    entry_price = float(position.get("entry_price") or 0.0)
    exit_date = _parse_timestamp(position.get("exit_date")) or _now()
    entry_date = _parse_timestamp(position.get("entry_date")) or exit_date
    return_pct = ((float(close_price) - entry_price) / entry_price) if entry_price > 0 else 0.0
    holding_days = max(0, (exit_date - entry_date).days)
    return {
        "ticker": str(position.get("ticker") or "").upper(),
        "return_pct": return_pct,
        "holding_days": holding_days,
        "outcome": "win" if return_pct > 0 else "loss",
    }


def kill_switch(
    portfolio_id: int,
    *,
    actor: str = "user",
    reason: str = "Manual kill switch activated",
) -> dict[str, Any] | None:
    portfolio = get_paper_portfolio(portfolio_id)
    if portfolio is None:
        return None
    rejected_count = 0
    now_text = _now().isoformat()
    for status in ("Pending", "Approved"):
        for plan in get_trade_plans(portfolio_id, status=status):
            updated = update_trade_plan_status(
                int(plan["id"]),
                "Rejected",
                reviewed_at=now_text,
                notes=f"Kill switch: {reason}",
            )
            if updated is not None:
                rejected_count += 1
    update_paper_portfolio(portfolio_id, status="Paused")
    log_audit_event(
        order_id=f"kill-switch-{portfolio_id}-{int(_now().timestamp())}",
        portfolio_id=portfolio_id,
        event_type="cancelled",
        ticker="*",
        action="kill_switch",
        actor=actor,
        details=reason,
        created_at=now_text,
    )
    return {
        "rejected_count": rejected_count,
        "portfolio_status": "Paused",
        "reason": reason,
        "killed_at": now_text,
    }


def _apply_filled_execution(
    portfolio_id: int,
    plan: dict[str, Any],
    result: Any,
) -> None:
    ticker = str(plan.get("ticker") or "").upper()
    action = str(plan.get("action") or "")
    quantity = float(plan.get("quantity") or 0.0)
    fill_price = float(result.filled_price or 0.0)
    if quantity <= 0 or fill_price <= 0:
        return
    portfolio = get_paper_portfolio(portfolio_id)
    if portfolio is None:
        return
    current_cash = float(portfolio.get("current_cash") or 0.0)
    now_text = result.filled_at or _now().isoformat()
    if action == "Buy":
        open_paper_position(
            portfolio_id,
            ticker,
            quantity,
            fill_price,
            now_text,
            theme_id=int(plan["theme_id"]) if plan.get("theme_id") is not None else None,
            role=str(plan.get("role") or "Critical-Path"),
        )
        update_paper_portfolio(portfolio_id, current_cash=current_cash - (quantity * fill_price))
        return
    open_positions = [
        row for row in get_paper_positions(portfolio_id, status="Open")
        if str(row.get("ticker") or "").upper() == ticker
    ]
    remaining = quantity
    credited = 0.0
    for position in sorted(open_positions, key=lambda row: str(row.get("entry_date") or "")):
        if remaining <= 0:
            break
        pos_qty = float(position.get("quantity") or 0.0)
        if pos_qty <= 0:
            continue
        if remaining < pos_qty - 1e-9:
            continue
        close_paper_position(int(position["id"]), fill_price, now_text, str(plan.get("source") or "broker_adapter"))
        credited += pos_qty * fill_price
        remaining -= pos_qty
    if credited > 0:
        update_paper_portfolio(portfolio_id, current_cash=current_cash + credited)


def _serialize_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{str(key): value for key, value in row.items()} for row in rows]
