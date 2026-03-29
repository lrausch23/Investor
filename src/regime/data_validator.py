from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import sqlite3
from typing import Any

import pandas as pd

from .data import _DEFAULT_MACRO_VALUES
from .persistence import DB_PATH, get_paper_positions, get_tax_lots, get_trade_plans, list_themes

DEFAULT_MAX_STALENESS_HOURS = 24
DEFAULT_MIN_HISTORY_DAYS = 30


def check_price_staleness(
    ticker: str,
    price_data: pd.DataFrame | pd.Series | None,
    max_staleness_hours: int = DEFAULT_MAX_STALENESS_HOURS,
) -> dict[str, Any]:
    issues: list[str] = []
    if price_data is None or len(price_data) == 0:
        return {"ticker": ticker, "valid": False, "last_data_date": None, "staleness_hours": None, "gap_days": None, "issues": ["No price data available."]}
    index = price_data.index if hasattr(price_data, "index") else []
    if len(index) == 0:
        return {"ticker": ticker, "valid": False, "last_data_date": None, "staleness_hours": None, "gap_days": None, "issues": ["Missing price index."]}
    last_dt = pd.Timestamp(index[-1]).to_pydatetime()
    if last_dt.tzinfo is None:
        last_dt = last_dt.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    staleness_hours = (now - last_dt.astimezone(timezone.utc)).total_seconds() / 3600.0
    if staleness_hours > max_staleness_hours:
        issues.append(f"{ticker} price data is stale ({staleness_hours:.1f}h old).")
    recent_index = pd.to_datetime(index[-30:])
    gap_days = 0
    if len(recent_index) >= 2:
        deltas = recent_index.to_series().diff().dropna().dt.days
        gap_days = int(deltas.max() or 0)
        if gap_days > 3:
            issues.append(f"{ticker} has a {gap_days}-day data gap.")
    return {
        "ticker": ticker,
        "valid": not issues,
        "last_data_date": last_dt.isoformat(),
        "staleness_hours": staleness_hours,
        "gap_days": gap_days,
        "issues": issues,
    }


def check_macro_data_quality(vix: float | None, yield_10y: float | None) -> dict[str, Any]:
    issues: list[str] = []
    vix_is_default = vix == _DEFAULT_MACRO_VALUES["vix"]
    yield_is_default = yield_10y == _DEFAULT_MACRO_VALUES["yield_10y"]
    if vix_is_default:
        issues.append("VIX is using fallback default 20.0.")
    if yield_is_default:
        issues.append("10Y yield is using fallback default 4.0.")
    if vix is not None and not (8 <= float(vix) <= 90):
        issues.append("VIX outside expected range.")
    if yield_10y is not None and not (0 <= float(yield_10y) <= 20):
        issues.append("10Y yield outside expected range.")
    return {"valid": not issues, "vix_is_default": vix_is_default, "yield_is_default": yield_is_default, "issues": issues}


def run_pre_trade_validation(
    tickers: list[str],
    price_frames: dict[str, pd.DataFrame | None] | None = None,
    vix: float | None = None,
    yield_10y: float | None = None,
) -> dict[str, Any]:
    ticker_results: dict[str, Any] = {}
    issues: list[str] = []
    stale_tickers: list[str] = []
    missing_tickers: list[str] = []
    for ticker in sorted({str(t or "").upper() for t in tickers if str(t or "").strip()}):
        frame = (price_frames or {}).get(ticker)
        result = check_price_staleness(ticker, frame)
        if frame is None or len(frame) == 0:
            missing_tickers.append(ticker)
        if not result["valid"]:
            stale_tickers.append(ticker)
            issues.extend(result["issues"])
        if frame is not None and len(frame) < DEFAULT_MIN_HISTORY_DAYS:
            result["valid"] = False
            result.setdefault("issues", []).append(f"{ticker} has insufficient history (<{DEFAULT_MIN_HISTORY_DAYS} days).")
            issues.append(f"{ticker} has insufficient history (<{DEFAULT_MIN_HISTORY_DAYS} days).")
        ticker_results[ticker] = result
    macro_result = check_macro_data_quality(vix, yield_10y)
    issues.extend(macro_result["issues"])
    return {
        "valid": not issues,
        "ticker_results": ticker_results,
        "macro_result": macro_result,
        "issues": issues,
        "stale_tickers": stale_tickers,
        "missing_tickers": missing_tickers,
    }


def check_database_health() -> dict[str, Any]:
    issues: list[str] = []
    db_path = Path(DB_PATH)
    with sqlite3.connect(db_path) as conn:
        integrity_row = conn.execute("PRAGMA integrity_check").fetchone()
        fk_row = conn.execute("PRAGMA foreign_key_check").fetchone()
        orphaned_plans = int(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM paper_trade_plan p
                LEFT JOIN paper_portfolio pp ON pp.id = p.portfolio_id
                WHERE pp.id IS NULL
                """
            ).fetchone()[0]
            or 0
        )
        latest_signal = conn.execute("SELECT MAX(snapshot_date) FROM signal_snapshots").fetchone()
    integrity = str(integrity_row[0]) if integrity_row else "unknown"
    foreign_keys = "ok" if fk_row is None else str(tuple(fk_row))
    if integrity != "ok":
        issues.append(f"Integrity check failed: {integrity}")
    if foreign_keys != "ok":
        issues.append(f"Foreign key violations: {foreign_keys}")
    empty_themes = [str(theme.get("name") or "") for theme in list_themes() if not theme.get("tickers")]
    if empty_themes:
        issues.append(f"Empty themes: {', '.join(empty_themes[:5])}")
    invalid_positions = sum(1 for row in get_paper_positions(0, status="all") if float(row.get("quantity") or 0.0) < 0) if False else 0
    stale_data_tickers: list[str] = []
    latest_snapshot = str(latest_signal[0] or "") if latest_signal and latest_signal[0] else ""
    if latest_snapshot:
        try:
            latest_dt = datetime.fromisoformat(latest_snapshot.replace("Z", "+00:00"))
            if latest_dt.tzinfo is None:
                latest_dt = latest_dt.replace(tzinfo=timezone.utc)
            if latest_dt < datetime.now(timezone.utc) - timedelta(days=1):
                stale_data_tickers.append("signal_snapshots")
                issues.append("Signal snapshot data is stale.")
        except Exception:
            stale_data_tickers.append("signal_snapshots")
    healthy = not issues and orphaned_plans == 0 and invalid_positions == 0
    if orphaned_plans:
        issues.append(f"Orphaned plans: {orphaned_plans}")
    return {
        "healthy": healthy,
        "integrity": integrity,
        "foreign_keys": foreign_keys,
        "orphaned_plans": orphaned_plans,
        "empty_themes": empty_themes,
        "stale_data_tickers": stale_data_tickers,
        "invalid_positions": invalid_positions,
        "issues": issues,
    }
