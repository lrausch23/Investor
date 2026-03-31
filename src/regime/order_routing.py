from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from .market_data_client import download_daily_bars
from .persistence import get_setting, set_setting

DEFAULT_ADV_HIGH_THRESHOLD = 1_000_000.0
DEFAULT_ADV_LOW_THRESHOLD = 500_000.0
DEFAULT_ADV_LOOKBACK_DAYS = 20
DEFAULT_PRICE_IMPROVEMENT_PCT = 0.001
_ADV_CACHE_TTL = timedelta(hours=1)
_ADV_CACHE: dict[str, tuple[float | None, datetime]] = {}


@dataclass(frozen=True)
class NBBOEstimate:
    bid: float
    ask: float
    mid: float
    spread: float
    source: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RoutingDecision:
    order_type: str
    time_in_force: str
    limit_price: float | None
    strategy_name: str
    rationale: str
    adv: float | None
    adv_bucket: str
    urgency: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _float_setting(key: str, default: float, *, min_value: float, max_value: float) -> float:
    raw = get_setting(key)
    try:
        value = float(str(raw)) if raw not in (None, "") else default
    except Exception:
        value = default
    return max(min_value, min(max_value, value))


def get_routing_settings() -> dict[str, Any]:
    return {
        "adv_high_threshold": _float_setting("routing_adv_high_threshold", DEFAULT_ADV_HIGH_THRESHOLD, min_value=10_000.0, max_value=500_000_000.0),
        "adv_low_threshold": _float_setting("routing_adv_low_threshold", DEFAULT_ADV_LOW_THRESHOLD, min_value=1_000.0, max_value=100_000_000.0),
        "adv_lookback_days": int(_float_setting("routing_adv_lookback_days", float(DEFAULT_ADV_LOOKBACK_DAYS), min_value=5.0, max_value=90.0)),
        "price_improvement_pct": _float_setting("routing_price_improvement_pct", DEFAULT_PRICE_IMPROVEMENT_PCT, min_value=0.0, max_value=0.05),
    }


def set_routing_settings(settings: dict[str, Any]) -> dict[str, Any]:
    if "adv_high_threshold" in settings:
        value = float(settings["adv_high_threshold"])
        if value <= 0:
            raise ValueError("adv_high_threshold must be positive")
        set_setting("routing_adv_high_threshold", str(value))
    if "adv_low_threshold" in settings:
        value = float(settings["adv_low_threshold"])
        if value <= 0:
            raise ValueError("adv_low_threshold must be positive")
        set_setting("routing_adv_low_threshold", str(value))
    if "adv_lookback_days" in settings:
        value = int(settings["adv_lookback_days"])
        if value <= 0:
            raise ValueError("adv_lookback_days must be positive")
        set_setting("routing_adv_lookback_days", str(value))
    if "price_improvement_pct" in settings:
        value = float(settings["price_improvement_pct"])
        if value < 0:
            raise ValueError("price_improvement_pct must be non-negative")
        set_setting("routing_price_improvement_pct", str(value))
    return get_routing_settings()


def _tick_size(price: float) -> float:
    del price
    return 0.01


def compute_adv(ticker: str, lookback_days: int = 20) -> float | None:
    normalized = str(ticker or "").upper()
    if not normalized:
        return None
    cached = _ADV_CACHE.get(normalized)
    if cached and (_now() - cached[1]) <= _ADV_CACHE_TTL:
        return cached[0]
    try:
        frame = download_daily_bars(normalized, period="1mo", auto_adjust=False)
        if frame is None or frame.empty or "Volume" not in frame.columns:
            value = None
        else:
            volumes = frame["Volume"].dropna().tail(max(1, int(lookback_days)))
            mean_value = volumes.mean() if not volumes.empty else None
            if hasattr(mean_value, "iloc"):
                mean_value = mean_value.iloc[0]
            value = float(mean_value) if mean_value is not None else None
    except Exception:
        value = None
    _ADV_CACHE[normalized] = (value, _now())
    return value


def estimate_nbbo(ticker: str, last_price: float, adv: float | None = None) -> NBBOEstimate:
    del ticker
    price = max(float(last_price or 0.0), 0.01)
    if adv is None:
        spread_pct = 0.001
    elif adv > 1_000_000:
        spread_pct = 0.0001
    elif adv > 500_000:
        spread_pct = 0.0005
    elif adv > 100_000:
        spread_pct = 0.001
    else:
        spread_pct = 0.0025
    raw_spread = price * spread_pct
    spread = max(_tick_size(price), round(raw_spread, 4))
    half = spread / 2.0
    bid = max(price - half, 0.01)
    ask = price + half
    return NBBOEstimate(
        bid=round(bid, 4),
        ask=round(ask, 4),
        mid=round(price, 4),
        spread=round(spread, 4),
        source="estimated",
    )


def _adv_bucket(adv: float | None, settings: dict[str, Any]) -> str:
    if adv is None:
        return "unknown"
    if adv > float(settings["adv_high_threshold"]):
        return "high_liquidity"
    if adv > float(settings["adv_low_threshold"]):
        return "medium_liquidity"
    return "low_liquidity"


def decide_routing(
    ticker: str,
    action: str,
    quantity: float,
    last_price: float,
    *,
    urgency: str = "normal",
    is_stop_triggered: bool = False,
    adv_override: float | None = None,
    nbbo_override: NBBOEstimate | None = None,
) -> RoutingDecision:
    del quantity
    settings = get_routing_settings()
    normalized_action = "Buy" if str(action or "").lower() == "buy" else "Sell"
    normalized_urgency = str(urgency or "normal").strip().lower()
    if normalized_urgency not in {"patient", "normal", "urgent"}:
        normalized_urgency = "normal"
    adv = adv_override if adv_override is not None else compute_adv(ticker, int(settings["adv_lookback_days"]))
    nbbo = nbbo_override or estimate_nbbo(ticker, last_price, adv)
    adv_bucket = _adv_bucket(adv, settings)
    bucket = "low_liquidity" if adv_bucket == "unknown" else adv_bucket
    tick = _tick_size(last_price)
    price_improvement = float(nbbo.mid) * float(settings["price_improvement_pct"])

    if bucket == "high_liquidity":
        if normalized_action == "Buy" and normalized_urgency == "patient":
            return RoutingDecision(
                order_type="limit",
                time_in_force="GTC",
                limit_price=round(nbbo.mid, 4),
                strategy_name="Passive Limit (Mid)",
                rationale=f"High liquidity (ADV {((adv or 0.0) / 1_000_000):.1f}M), patient entry -> limit at midpoint",
                adv=adv,
                adv_bucket=adv_bucket,
                urgency=normalized_urgency,
            )
        if normalized_action == "Buy":
            return RoutingDecision(
                order_type="limit",
                time_in_force="DAY",
                limit_price=round(max(nbbo.ask, nbbo.mid + tick), 4),
                strategy_name="Limit (Ask)",
                rationale=f"High liquidity (ADV {((adv or 0.0) / 1_000_000):.1f}M), {normalized_urgency} entry -> limit at ask",
                adv=adv,
                adv_bucket=adv_bucket,
                urgency=normalized_urgency,
            )
        if is_stop_triggered or normalized_urgency == "urgent":
            return RoutingDecision(
                order_type="marketable_limit",
                time_in_force="IOC",
                limit_price=round(max(nbbo.bid - tick, 0.01), 4),
                strategy_name="Marketable Limit (IOC)",
                rationale=f"High liquidity (ADV {((adv or 0.0) / 1_000_000):.1f}M), urgent exit -> IOC marketable limit near bid",
                adv=adv,
                adv_bucket=adv_bucket,
                urgency="urgent",
            )
        return RoutingDecision(
            order_type="limit",
            time_in_force="DAY",
            limit_price=round(nbbo.bid, 4),
            strategy_name="Limit (Bid)",
            rationale=f"High liquidity (ADV {((adv or 0.0) / 1_000_000):.1f}M), normal exit -> limit at bid",
            adv=adv,
            adv_bucket=adv_bucket,
            urgency=normalized_urgency,
        )

    if bucket == "medium_liquidity":
        if normalized_action == "Buy":
            return RoutingDecision(
                order_type="limit",
                time_in_force="DAY",
                limit_price=round(nbbo.mid, 4),
                strategy_name="Limit (Mid)",
                rationale="Medium liquidity -> limit at midpoint",
                adv=adv,
                adv_bucket=adv_bucket,
                urgency=normalized_urgency,
            )
        return RoutingDecision(
            order_type="limit",
            time_in_force="IOC" if normalized_urgency == "urgent" or is_stop_triggered else "DAY",
            limit_price=round(nbbo.bid, 4),
            strategy_name="Limit (Bid, IOC)" if normalized_urgency == "urgent" or is_stop_triggered else "Limit (Bid)",
            rationale="Medium liquidity exit -> bid-side limit",
            adv=adv,
            adv_bucket=adv_bucket,
            urgency="urgent" if normalized_urgency == "urgent" or is_stop_triggered else normalized_urgency,
        )

    if normalized_action == "Buy":
        improved = max(nbbo.mid - price_improvement, 0.01)
        return RoutingDecision(
            order_type="limit",
            time_in_force="GTC",
            limit_price=round(improved, 4),
            strategy_name="Patient Limit (Price Improvement)",
            rationale="Low liquidity entry -> patient GTC limit below midpoint",
            adv=adv,
            adv_bucket=adv_bucket,
            urgency=normalized_urgency,
        )
    return RoutingDecision(
        order_type="limit",
        time_in_force="DAY",
        limit_price=round(nbbo.bid, 4),
        strategy_name="Limit (Bid)",
        rationale="Low liquidity exit -> bid-side limit only",
        adv=adv,
        adv_bucket=adv_bucket,
        urgency=normalized_urgency,
    )
