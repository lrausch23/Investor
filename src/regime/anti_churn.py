from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from .persistence import count_executed_sell_plans, get_oldest_executed_sell_at, get_setting, set_setting

DEFAULT_ANTI_CHURN_ENABLED = True
DEFAULT_MAX_ROUND_TRIPS_30D = 2
DEFAULT_ANTI_CHURN_COOLDOWN_DAYS = 30


@dataclass
class AntiChurnResult:
    """Result of the anti-churn velocity check."""

    ticker: str
    round_trip_count: int
    max_round_trips: int
    passed: bool
    cooldown_expires: str | None
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _bool_setting(key: str, default: bool) -> bool:
    raw = get_setting(key)
    if raw in (None, ""):
        return default
    return str(raw).strip().lower() in {"true", "1", "yes", "on"}


def _int_setting(key: str, default: int, *, min_value: int, max_value: int) -> int:
    raw = get_setting(key)
    try:
        value = int(str(raw)) if raw not in (None, "") else default
    except Exception:
        value = default
    return max(min_value, min(max_value, value))


def get_anti_churn_settings() -> dict[str, Any]:
    return {
        "anti_churn_enabled": _bool_setting("anti_churn_enabled", DEFAULT_ANTI_CHURN_ENABLED),
        "anti_churn_max_round_trips_30d": _int_setting(
            "anti_churn_max_round_trips_30d",
            DEFAULT_MAX_ROUND_TRIPS_30D,
            min_value=1,
            max_value=20,
        ),
        "anti_churn_cooldown_days": _int_setting(
            "anti_churn_cooldown_days",
            DEFAULT_ANTI_CHURN_COOLDOWN_DAYS,
            min_value=7,
            max_value=90,
        ),
    }


def set_anti_churn_settings(settings: dict[str, Any]) -> dict[str, Any]:
    if "anti_churn_enabled" in settings:
        set_setting("anti_churn_enabled", "true" if settings["anti_churn_enabled"] else "false")
    if "anti_churn_max_round_trips_30d" in settings:
        value = max(1, min(20, int(settings["anti_churn_max_round_trips_30d"])))
        set_setting("anti_churn_max_round_trips_30d", str(value))
    if "anti_churn_cooldown_days" in settings:
        value = max(7, min(90, int(settings["anti_churn_cooldown_days"])))
        set_setting("anti_churn_cooldown_days", str(value))
    return get_anti_churn_settings()


def count_round_trips(
    portfolio_id: int,
    ticker: str,
    days: int = 30,
) -> int:
    return count_executed_sell_plans(int(portfolio_id), str(ticker or "").upper(), days=max(1, int(days)))


def check_anti_churn(
    portfolio_id: int,
    ticker: str,
    *,
    max_round_trips: int | None = None,
) -> AntiChurnResult:
    settings = get_anti_churn_settings()
    cooldown_days = int(settings["anti_churn_cooldown_days"])
    threshold = (
        max(1, min(20, int(max_round_trips)))
        if max_round_trips is not None
        else int(settings["anti_churn_max_round_trips_30d"])
    )
    normalized_ticker = str(ticker or "").upper()
    round_trip_count = count_round_trips(int(portfolio_id), normalized_ticker, days=cooldown_days)
    passed = round_trip_count < threshold
    cooldown_expires: str | None = None
    if not passed:
        oldest = get_oldest_executed_sell_at(int(portfolio_id), normalized_ticker, days=cooldown_days)
        if oldest:
            try:
                oldest_dt = datetime.fromisoformat(str(oldest).replace("Z", "+00:00"))
                if oldest_dt.tzinfo is None:
                    oldest_dt = oldest_dt.replace(tzinfo=timezone.utc)
                cooldown_expires = (oldest_dt.astimezone(timezone.utc) + timedelta(days=cooldown_days)).isoformat()
            except Exception:
                cooldown_expires = None
    if passed:
        reason = f"Round-trip count {round_trip_count} is below max {threshold} in trailing {cooldown_days} days"
    else:
        reason = (
            f"Round-trip count {round_trip_count} reached max {threshold} in trailing {cooldown_days} days"
            + (f"; cooldown until {cooldown_expires}" if cooldown_expires else "")
        )
    return AntiChurnResult(
        ticker=normalized_ticker,
        round_trip_count=round_trip_count,
        max_round_trips=threshold,
        passed=passed,
        cooldown_expires=cooldown_expires,
        reason=reason,
    )


def is_churn_restricted(portfolio_id: int, ticker: str) -> bool:
    settings = get_anti_churn_settings()
    if not bool(settings["anti_churn_enabled"]):
        return False
    result = check_anti_churn(int(portfolio_id), str(ticker or "").upper())
    return not result.passed
