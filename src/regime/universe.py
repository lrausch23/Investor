from __future__ import annotations

import datetime as dt
import logging
import re
from dataclasses import asdict, dataclass
from typing import Any

import pandas as pd

from .config import EXCLUDED_TICKER_PATTERNS, HMM_ELIGIBLE_ASSET_CLASSES
from .data import download_market_frame
from .market_data_client import get_ticker_info
from .persistence import get_setting

logger = logging.getLogger(__name__)


DEFAULT_UNIVERSE_MIN_PRICE = 5.0
DEFAULT_UNIVERSE_MIN_HISTORY_DAYS = 756
DEFAULT_UNIVERSE_MIN_DOLLAR_ADV = 10_000_000.0
DEFAULT_UNIVERSE_ADV_WINDOW = 30

_CACHE: dict[tuple[str, str], "UniverseEligibility"] = {}


@dataclass(frozen=True)
class UniverseEligibility:
    ticker: str
    eligible: bool
    reasons: list[str]
    measured_price: float | None = None
    measured_history_days: int = 0
    measured_dollar_adv: float | None = None
    asset_class: str = "UNKNOWN"
    measured_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def universe_screen_enabled() -> bool:
    return _setting_bool("universe_screen_enabled", True)


def agent_theme_budgets_enabled() -> bool:
    return _setting_bool("agent_theme_budgets_enabled", False)


def get_universe_settings() -> dict[str, Any]:
    return {
        "universe_screen_enabled": universe_screen_enabled(),
        "universe_min_price": _setting_float("universe_min_price", DEFAULT_UNIVERSE_MIN_PRICE, minimum=0.01),
        "universe_min_history_days": _setting_int("universe_min_history_days", DEFAULT_UNIVERSE_MIN_HISTORY_DAYS, minimum=1),
        "universe_min_dollar_adv": _setting_float("universe_min_dollar_adv", DEFAULT_UNIVERSE_MIN_DOLLAR_ADV, minimum=0.0),
        "agent_theme_budgets_enabled": agent_theme_budgets_enabled(),
    }


def check_universe_eligibility(
    ticker: str,
    *,
    market_frame: pd.DataFrame | None = None,
    asset_class: str | None = None,
    use_cache: bool = True,
) -> UniverseEligibility:
    symbol = str(ticker or "").strip().upper()
    today_key = dt.date.today().isoformat()
    cache_key = (symbol, today_key)
    if use_cache and market_frame is None and asset_class is None and cache_key in _CACHE:
        return _CACHE[cache_key]

    reasons: list[str] = []
    if not symbol:
        result = UniverseEligibility(ticker=symbol, eligible=False, reasons=["missing_ticker"], measured_at=_now_iso())
        return result
    if _excluded_by_pattern(symbol):
        reasons.append("excluded_ticker_pattern")

    frame = _normalize_market_frame(market_frame)
    if frame is None:
        try:
            frame = _normalize_market_frame(download_market_frame(symbol, period="5y", interval="1d").frame)
        except Exception as exc:
            logger.warning("Universe screen market frame fetch failed for %s: %s", symbol, exc)
            frame = pd.DataFrame()
            reasons.append("market_frame_unavailable")

    resolved_asset_class = _resolve_asset_class(symbol, asset_class)
    if resolved_asset_class not in HMM_ELIGIBLE_ASSET_CLASSES:
        reasons.append("asset_class_ineligible")

    min_price = float(get_universe_settings()["universe_min_price"])
    min_history = int(get_universe_settings()["universe_min_history_days"])
    min_dollar_adv = float(get_universe_settings()["universe_min_dollar_adv"])
    measured_price = _latest_price(frame)
    measured_history_days = int(len(frame)) if frame is not None else 0
    measured_dollar_adv = _dollar_adv(frame)

    if measured_price is None:
        reasons.append("price_unavailable")
    elif measured_price < min_price:
        reasons.append("price_below_min")

    if measured_history_days < min_history:
        reasons.append("insufficient_history")

    if measured_dollar_adv is None:
        reasons.append("dollar_adv_unavailable")
    elif measured_dollar_adv < min_dollar_adv:
        reasons.append("dollar_adv_below_min")

    result = UniverseEligibility(
        ticker=symbol,
        eligible=not reasons,
        reasons=reasons,
        measured_price=measured_price,
        measured_history_days=measured_history_days,
        measured_dollar_adv=measured_dollar_adv,
        asset_class=resolved_asset_class,
        measured_at=_now_iso(),
    )
    if use_cache and market_frame is None and asset_class is None:
        _CACHE[cache_key] = result
    return result


def _normalize_market_frame(frame: pd.DataFrame | None) -> pd.DataFrame:
    if frame is None:
        return pd.DataFrame()
    normalized = frame.copy()
    if normalized.empty:
        return normalized
    rename = {
        "Close": "price",
        "Adj Close": "price",
        "Volume": "volume",
        "close": "price",
    }
    normalized = normalized.rename(columns={column: rename.get(str(column), str(column)) for column in normalized.columns})
    return normalized.dropna(subset=[column for column in ("price", "volume") if column in normalized.columns])


def _latest_price(frame: pd.DataFrame) -> float | None:
    if frame is None or frame.empty or "price" not in frame.columns:
        return None
    series = pd.to_numeric(frame["price"], errors="coerce").dropna()
    if series.empty:
        return None
    value = float(series.iloc[-1])
    return value if value == value and value > 0 else None


def _dollar_adv(frame: pd.DataFrame) -> float | None:
    if frame is None or frame.empty or "price" not in frame.columns or "volume" not in frame.columns:
        return None
    price = pd.to_numeric(frame["price"], errors="coerce")
    volume = pd.to_numeric(frame["volume"], errors="coerce")
    dollar_volume = (price * volume).dropna()
    if dollar_volume.empty:
        return None
    window = max(1, min(DEFAULT_UNIVERSE_ADV_WINDOW, len(dollar_volume)))
    value = float(dollar_volume.tail(window).mean())
    return value if value == value and value >= 0 else None


def _resolve_asset_class(ticker: str, provided: str | None) -> str:
    if provided:
        return _normalize_asset_class(provided)
    try:
        info = get_ticker_info(ticker) or {}
    except Exception:
        info = {}
    for key in ("quoteType", "assetClass", "typeDisp", "category"):
        value = info.get(key) if isinstance(info, dict) else None
        if value:
            return _normalize_asset_class(value)
    return "UNKNOWN"


def _normalize_asset_class(value: Any) -> str:
    normalized = str(value or "UNKNOWN").strip().upper().replace(" ", "_")
    if normalized in {"EQUITY", "COMMON_STOCK", "STOCK", "ETF"}:
        return "STOCK" if normalized == "COMMON_STOCK" else normalized
    if "EQUITY" in normalized:
        return "EQUITY"
    if "ETF" in normalized:
        return "ETF"
    return normalized or "UNKNOWN"


def _excluded_by_pattern(ticker: str) -> bool:
    symbol = str(ticker or "").upper()
    if symbol in EXCLUDED_TICKER_PATTERNS:
        return True
    for pattern in EXCLUDED_TICKER_PATTERNS:
        token = str(pattern or "").upper().strip()
        if not token:
            continue
        if token.startswith("RE:") and re.search(token[3:], symbol):
            return True
        if token.endswith("*") and symbol.startswith(token[:-1]):
            return True
        if token.startswith("*") and symbol.endswith(token[1:]):
            return True
    return False


def _setting_bool(key: str, default: bool) -> bool:
    raw = get_setting(key)
    if raw in (None, ""):
        return bool(default)
    normalized = str(raw).strip().lower()
    if normalized in {"1", "true", "yes", "on", "enabled"}:
        return True
    if normalized in {"0", "false", "no", "off", "disabled"}:
        return False
    return bool(default)


def _setting_float(key: str, default: float, *, minimum: float = 0.0) -> float:
    try:
        value = float(get_setting(key) or default)
    except Exception:
        value = float(default)
    return max(float(minimum), value)


def _setting_int(key: str, default: int, *, minimum: int = 0) -> int:
    try:
        value = int(float(get_setting(key) or default))
    except Exception:
        value = int(default)
    return max(int(minimum), value)


def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()
