from __future__ import annotations

from dataclasses import asdict, dataclass, field
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
    algo_strategy: str = ""
    algo_params: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AlgoDecision:
    algo_strategy: str
    algo_params: dict[str, str]
    rationale: str
    adv_pct: float

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
        "algo_adv_pct_threshold": _float_setting("routing_algo_adv_pct_threshold", 0.01, min_value=0.001, max_value=0.10),
        "algo_max_volume_rate": _float_setting("routing_algo_max_volume_rate", 0.20, min_value=0.01, max_value=0.50),
        "algo_enabled": str(get_setting("routing_algo_enabled") or "true").strip().lower() not in {"false", "0", "no", "off"},
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
    if "algo_adv_pct_threshold" in settings:
        value = float(settings["algo_adv_pct_threshold"])
        if value <= 0:
            raise ValueError("algo_adv_pct_threshold must be positive")
        set_setting("routing_algo_adv_pct_threshold", str(value))
    if "algo_max_volume_rate" in settings:
        value = float(settings["algo_max_volume_rate"])
        if value <= 0:
            raise ValueError("algo_max_volume_rate must be positive")
        set_setting("routing_algo_max_volume_rate", str(value))
    if "algo_enabled" in settings:
        set_setting("routing_algo_enabled", "true" if settings["algo_enabled"] else "false")
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
            if mean_value is not None and hasattr(mean_value, "iloc"):
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


def needs_algo_execution(
    quantity: float,
    adv: float | None,
    *,
    adv_pct_threshold: float = 0.01,
) -> bool:
    if adv is None or adv <= 0:
        return False
    return float(quantity or 0.0) > (float(adv) * float(adv_pct_threshold))


def select_algo(
    ticker: str,
    action: str,
    quantity: float,
    adv: float | None,
    *,
    urgency: str = "normal",
    adv_bucket: str = "unknown",
    max_volume_rate: float = 0.20,
    adv_pct_threshold: float = 0.01,
) -> AlgoDecision:
    del ticker, action
    if not needs_algo_execution(quantity, adv, adv_pct_threshold=adv_pct_threshold):
        return AlgoDecision(algo_strategy="", algo_params={}, rationale="Order below ADV threshold for algo execution", adv_pct=0.0 if not adv else float(quantity) / float(adv))
    assert adv is not None
    adv_pct = float(quantity) / float(adv)
    capped_rate = float(max_volume_rate)
    if adv_pct > 0.10:
        capped_rate = min(capped_rate, 0.05)
    elif adv_pct > 0.05:
        capped_rate = min(capped_rate, 0.10)
    normalized_urgency = str(urgency or "normal").lower()
    if adv_bucket == "high_liquidity" and normalized_urgency in {"patient", "normal"}:
        return AlgoDecision(
            algo_strategy="VWAP",
            algo_params={
                "maxPctVol": f"{capped_rate:.2f}",
                "startTime": "09:30:00 US/Eastern",
                "endTime": "16:00:00 US/Eastern",
                "allowPastEndTime": "1",
                "noTakeLiq": "0",
            },
            rationale="Volume-weighted distribution for better average price in liquid name",
            adv_pct=adv_pct,
        )
    return AlgoDecision(
        algo_strategy="TWAP",
        algo_params={
            "strategyType": "Twap",
            "startTime": "09:30:00 US/Eastern",
            "endTime": "16:00:00 US/Eastern",
            "allowPastEndTime": "1",
            "maxPctVol": f"{capped_rate:.2f}",
        },
        rationale=(
            "Uniform time distribution for minimal information leakage on urgent exit"
            if adv_bucket == "high_liquidity" and normalized_urgency == "urgent"
            else "Time-weighted distribution to minimize footprint"
        ),
        adv_pct=adv_pct,
    )


def _attach_algo(
    decision: RoutingDecision,
    *,
    ticker: str,
    action: str,
    quantity: float,
    adv: float | None,
    settings: dict[str, Any],
) -> RoutingDecision:
    if not bool(settings.get("algo_enabled", True)):
        return decision
    algo = select_algo(
        ticker=ticker,
        action=action,
        quantity=quantity,
        adv=adv,
        urgency=decision.urgency,
        adv_bucket=decision.adv_bucket,
        max_volume_rate=float(settings.get("algo_max_volume_rate", 0.20)),
        adv_pct_threshold=float(settings.get("algo_adv_pct_threshold", 0.01)),
    )
    if not algo.algo_strategy:
        return decision
    return RoutingDecision(
        order_type=decision.order_type,
        time_in_force=decision.time_in_force,
        limit_price=decision.limit_price,
        strategy_name=f"{algo.algo_strategy} Algo ({decision.strategy_name})",
        rationale=f"{decision.rationale}; {algo.rationale}",
        adv=decision.adv,
        adv_bucket=decision.adv_bucket,
        urgency=decision.urgency,
        algo_strategy=algo.algo_strategy,
        algo_params=algo.algo_params,
    )


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
            return _attach_algo(RoutingDecision(
                order_type="limit",
                time_in_force="GTC",
                limit_price=round(nbbo.mid, 4),
                strategy_name="Passive Limit (Mid)",
                rationale=f"High liquidity (ADV {((adv or 0.0) / 1_000_000):.1f}M), patient entry -> limit at midpoint",
                adv=adv,
                adv_bucket=adv_bucket,
                urgency=normalized_urgency,
            ), ticker=ticker, action=normalized_action, quantity=quantity, adv=adv, settings=settings)
        if normalized_action == "Buy":
            return _attach_algo(RoutingDecision(
                order_type="limit",
                time_in_force="DAY",
                limit_price=round(max(nbbo.ask, nbbo.mid + tick), 4),
                strategy_name="Limit (Ask)",
                rationale=f"High liquidity (ADV {((adv or 0.0) / 1_000_000):.1f}M), {normalized_urgency} entry -> limit at ask",
                adv=adv,
                adv_bucket=adv_bucket,
                urgency=normalized_urgency,
            ), ticker=ticker, action=normalized_action, quantity=quantity, adv=adv, settings=settings)
        if is_stop_triggered or normalized_urgency == "urgent":
            return _attach_algo(RoutingDecision(
                order_type="marketable_limit",
                time_in_force="IOC",
                limit_price=round(max(nbbo.bid - tick, 0.01), 4),
                strategy_name="Marketable Limit (IOC)",
                rationale=f"High liquidity (ADV {((adv or 0.0) / 1_000_000):.1f}M), urgent exit -> IOC marketable limit near bid",
                adv=adv,
                adv_bucket=adv_bucket,
                urgency="urgent",
            ), ticker=ticker, action=normalized_action, quantity=quantity, adv=adv, settings=settings)
        return _attach_algo(RoutingDecision(
            order_type="limit",
            time_in_force="DAY",
            limit_price=round(nbbo.bid, 4),
            strategy_name="Limit (Bid)",
            rationale=f"High liquidity (ADV {((adv or 0.0) / 1_000_000):.1f}M), normal exit -> limit at bid",
            adv=adv,
            adv_bucket=adv_bucket,
            urgency=normalized_urgency,
        ), ticker=ticker, action=normalized_action, quantity=quantity, adv=adv, settings=settings)

    if bucket == "medium_liquidity":
        if normalized_action == "Buy":
            return _attach_algo(RoutingDecision(
                order_type="limit",
                time_in_force="DAY",
                limit_price=round(nbbo.mid, 4),
                strategy_name="Limit (Mid)",
                rationale="Medium liquidity -> limit at midpoint",
                adv=adv,
                adv_bucket=adv_bucket,
                urgency=normalized_urgency,
            ), ticker=ticker, action=normalized_action, quantity=quantity, adv=adv, settings=settings)
        return _attach_algo(RoutingDecision(
            order_type="limit",
            time_in_force="IOC" if normalized_urgency == "urgent" or is_stop_triggered else "DAY",
            limit_price=round(nbbo.bid, 4),
            strategy_name="Limit (Bid, IOC)" if normalized_urgency == "urgent" or is_stop_triggered else "Limit (Bid)",
            rationale="Medium liquidity exit -> bid-side limit",
            adv=adv,
            adv_bucket=adv_bucket,
            urgency="urgent" if normalized_urgency == "urgent" or is_stop_triggered else normalized_urgency,
        ), ticker=ticker, action=normalized_action, quantity=quantity, adv=adv, settings=settings)

    if normalized_action == "Buy":
        improved = max(nbbo.mid - price_improvement, 0.01)
        return _attach_algo(RoutingDecision(
            order_type="limit",
            time_in_force="GTC",
            limit_price=round(improved, 4),
            strategy_name="Patient Limit (Price Improvement)",
            rationale="Low liquidity entry -> patient GTC limit below midpoint",
            adv=adv,
            adv_bucket=adv_bucket,
            urgency=normalized_urgency,
        ), ticker=ticker, action=normalized_action, quantity=quantity, adv=adv, settings=settings)
    return _attach_algo(RoutingDecision(
        order_type="limit",
        time_in_force="DAY",
        limit_price=round(nbbo.bid, 4),
        strategy_name="Limit (Bid)",
        rationale="Low liquidity exit -> bid-side limit only",
        adv=adv,
        adv_bucket=adv_bucket,
        urgency=normalized_urgency,
    ), ticker=ticker, action=normalized_action, quantity=quantity, adv=adv, settings=settings)
