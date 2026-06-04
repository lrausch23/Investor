#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

load_dotenv(ROOT / ".env")

DEFAULT_WINDOW_START = dt.time(10, 5)
DEFAULT_WINDOW_END = dt.time(15, 30)
DEFAULT_SCHEDULE_LABEL = "com.investor.regime-beta"


def _parse_time(value: str) -> dt.time:
    hour, minute = value.split(":", maxsplit=1)
    return dt.time(int(hour), int(minute))


def _parse_now(value: str | None):
    if not value:
        return None
    parsed = dt.datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        from src.regime.ib_types import ET

        parsed = parsed.replace(tzinfo=ET)
    return parsed


def market_session_window_status(
    now: dt.datetime | None = None,
    *,
    window_start: dt.time = DEFAULT_WINDOW_START,
    window_end: dt.time = DEFAULT_WINDOW_END,
) -> dict[str, Any]:
    from src.regime.ib_types import ET, MarketHoursStatus, get_market_hours_status, next_market_open

    current = now.astimezone(ET) if now else dt.datetime.now(ET)
    market_status = get_market_hours_status(current)
    wall = current.timetz().replace(tzinfo=None)
    in_window = market_status == MarketHoursStatus.REGULAR and window_start <= wall <= window_end
    reason = "inside_preferred_market_window"
    if market_status != MarketHoursStatus.REGULAR:
        reason = f"market_{market_status.value}"
    elif wall < window_start:
        reason = "before_preferred_window"
    elif wall > window_end:
        reason = "after_preferred_window"
    return {
        "current_et": current.isoformat(),
        "trade_date": current.date().isoformat(),
        "market_status": market_status.value,
        "window_start": window_start.strftime("%H:%M"),
        "window_end": window_end.strftime("%H:%M"),
        "timezone": "America/New_York",
        "in_window": in_window,
        "reason": reason,
        "next_market_open": next_market_open(current).isoformat() if not in_window else None,
    }


def _record_status(payload: dict[str, Any]) -> None:
    from src.regime.persistence import set_setting

    set_setting("regime_beta_market_session_last_status", json.dumps(payload, sort_keys=True, default=str))
    set_setting("regime_beta_market_session_last_checked_at", dt.datetime.now(dt.timezone.utc).isoformat())
    set_setting("regime_beta_preferred_run_window", "10:05-15:30 America/New_York")
    set_setting("regime_beta_schedule_label", DEFAULT_SCHEDULE_LABEL)
    set_setting("regime_beta_schedule_enabled", "true")


def run_market_session_cycle(
    *,
    now: dt.datetime | None = None,
    window_start: dt.time = DEFAULT_WINDOW_START,
    window_end: dt.time = DEFAULT_WINDOW_END,
    force: bool = False,
    dry_run: bool = False,
    budget: float = 25_000.0,
    name: str = "Regime Agent Beta - IBKR Paper",
) -> dict[str, Any]:
    from src.regime.persistence import get_setting, set_setting

    window = market_session_window_status(now, window_start=window_start, window_end=window_end)
    trade_date = str(window["trade_date"])
    last_run_date = get_setting("regime_beta_last_market_session_cycle_date")

    if not force and not bool(window["in_window"]):
        payload = {"status": "skipped", "skip_reason": window["reason"], "window": window}
        _record_status(payload)
        return payload

    if not force and last_run_date == trade_date:
        payload = {"status": "skipped", "skip_reason": "already_ran_for_trade_date", "window": window}
        _record_status(payload)
        return payload

    if dry_run:
        payload = {"status": "dry_run", "window": window, "would_run": True}
        _record_status(payload)
        return payload

    from scripts.deploy_regime_beta import (
        _apply_settings,
        _ensure_agent_portfolios,
        _ensure_agent_topology,
        _run_beta_paper_cycle,
        _save_initial_snapshot,
    )

    started_at = dt.datetime.now(dt.timezone.utc).isoformat()
    set_setting("regime_beta_market_session_started_at", started_at)
    try:
        agents = _ensure_agent_topology()
        portfolios = _ensure_agent_portfolios(float(budget), broker_type="ibkr")
        portfolio = portfolios[0] if portfolios else {}
        settings = _apply_settings(include_deployed_at=False)
        scheduled = [_run_beta_paper_cycle(int(item["id"])) for item in portfolios]
        snapshots = [_save_initial_snapshot(int(item["id"])) for item in portfolios]
        payload = {
            "status": "completed",
            "window": window,
            "portfolio_id": int(portfolio["id"]) if portfolio else None,
            "portfolio_name": portfolio.get("name"),
            "portfolios": portfolios,
            "agents": agents,
            "settings": settings,
            "scheduled_run": scheduled,
            "snapshot": snapshots[0] if snapshots else {},
            "snapshots": snapshots,
            "started_at": started_at,
            "completed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        }
        set_setting("regime_beta_last_market_session_cycle_date", trade_date)
        set_setting("regime_beta_last_market_session_cycle_at", payload["completed_at"])
        set_setting("regime_beta_schedule_enabled", "true")
        _record_status(payload)
        return payload
    except Exception as exc:
        payload = {
            "status": "failed",
            "window": window,
            "started_at": started_at,
            "failed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "error": str(exc),
        }
        _record_status(payload)
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the Regime Agent beta paper cycle during regular US market hours.")
    parser.add_argument("--window-start", default=DEFAULT_WINDOW_START.strftime("%H:%M"))
    parser.add_argument("--window-end", default=DEFAULT_WINDOW_END.strftime("%H:%M"))
    parser.add_argument("--now", help="ISO timestamp override for tests or manual dry runs.")
    parser.add_argument("--budget", type=float, default=25_000.0)
    parser.add_argument("--name", default="Regime Agent Beta - IBKR Paper")
    parser.add_argument("--force", action="store_true", help="Bypass market-window and duplicate-run checks.")
    parser.add_argument("--dry-run", action="store_true", help="Validate scheduling without placing a paper cycle.")
    args = parser.parse_args()

    payload = run_market_session_cycle(
        now=_parse_now(args.now),
        window_start=_parse_time(args.window_start),
        window_end=_parse_time(args.window_end),
        force=bool(args.force),
        dry_run=bool(args.dry_run),
        budget=float(args.budget),
        name=str(args.name),
    )
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 0 if payload.get("status") != "failed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
