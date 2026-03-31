from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .persistence import get_setting, set_setting

DEFAULT_ESTIMATED_STCG_RATE = 0.32
DEFAULT_MIN_NET_RETURN_PCT = 3.0
DEFAULT_MIN_REGIME_DURATION_DAYS = 7.0
DEFAULT_HURDLE_ENABLED = True
DEFAULT_DURATION_GATE_ENABLED = True


@dataclass
class HurdleRateResult:
    ticker: str
    gross_return_pct: float | None
    estimated_stcg_rate: float
    net_return_pct: float | None
    min_net_return_pct: float
    passed: bool
    reason: str


@dataclass
class DurationGateResult:
    ticker: str
    expected_regime_duration: float | None
    min_regime_duration_days: float
    regime_label: str
    passed: bool
    reason: str


def _bool_setting(key: str, default: bool) -> bool:
    raw = get_setting(key)
    if raw in (None, ""):
        return default
    return str(raw).strip().lower() in {"true", "1", "yes", "on"}


def _float_setting(key: str, default: float, *, min_value: float, max_value: float) -> float:
    raw = get_setting(key)
    try:
        value = float(str(raw)) if raw not in (None, "") else default
    except Exception:
        value = default
    return max(min_value, min(max_value, value))


def get_hurdle_settings() -> dict[str, Any]:
    return {
        "hurdle_enabled": _bool_setting("hurdle_enabled", DEFAULT_HURDLE_ENABLED),
        "duration_gate_enabled": _bool_setting("duration_gate_enabled", DEFAULT_DURATION_GATE_ENABLED),
        "estimated_stcg_rate": _float_setting(
            "estimated_stcg_rate",
            DEFAULT_ESTIMATED_STCG_RATE,
            min_value=0.0,
            max_value=0.99,
        ),
        "hurdle_min_net_return_pct": _float_setting(
            "hurdle_min_net_return_pct",
            DEFAULT_MIN_NET_RETURN_PCT,
            min_value=0.0,
            max_value=50.0,
        ),
        "min_regime_duration_days": _float_setting(
            "min_regime_duration_days",
            DEFAULT_MIN_REGIME_DURATION_DAYS,
            min_value=1.0,
            max_value=90.0,
        ),
    }


def set_hurdle_settings(settings: dict[str, Any]) -> dict[str, Any]:
    if "hurdle_enabled" in settings:
        set_setting("hurdle_enabled", "true" if settings["hurdle_enabled"] else "false")
    if "duration_gate_enabled" in settings:
        set_setting("duration_gate_enabled", "true" if settings["duration_gate_enabled"] else "false")
    if "estimated_stcg_rate" in settings:
        value = max(0.0, min(0.99, float(settings["estimated_stcg_rate"])))
        set_setting("estimated_stcg_rate", str(value))
    if "hurdle_min_net_return_pct" in settings:
        value = max(0.0, min(50.0, float(settings["hurdle_min_net_return_pct"])))
        set_setting("hurdle_min_net_return_pct", str(value))
    if "min_regime_duration_days" in settings:
        value = max(1.0, min(90.0, float(settings["min_regime_duration_days"])))
        set_setting("min_regime_duration_days", str(value))
    return get_hurdle_settings()


def check_hurdle_rate(
    ticker: str,
    entry_price: float | None,
    exit_price: float | None,
    *,
    estimated_stcg_rate: float | None = None,
    min_net_return_pct: float | None = None,
) -> HurdleRateResult:
    settings = get_hurdle_settings()
    tax_rate = settings["estimated_stcg_rate"] if estimated_stcg_rate is None else max(0.0, min(0.99, float(estimated_stcg_rate)))
    minimum = settings["hurdle_min_net_return_pct"] if min_net_return_pct is None else max(0.0, min(50.0, float(min_net_return_pct)))
    normalized_ticker = str(ticker or "").upper()
    if entry_price is None or exit_price is None or float(entry_price) <= 0:
        return HurdleRateResult(
            ticker=normalized_ticker,
            gross_return_pct=None,
            estimated_stcg_rate=tax_rate,
            net_return_pct=None,
            min_net_return_pct=minimum,
            passed=True,
            reason="Insufficient price data - pass by default",
        )
    entry_value = float(entry_price)
    exit_value = float(exit_price)
    gross_return_pct = ((exit_value - entry_value) / entry_value) * 100.0
    net_return_pct = gross_return_pct * (1.0 - tax_rate)
    passed = net_return_pct >= minimum
    comparator = ">=" if passed else "<"
    return HurdleRateResult(
        ticker=normalized_ticker,
        gross_return_pct=gross_return_pct,
        estimated_stcg_rate=tax_rate,
        net_return_pct=net_return_pct,
        min_net_return_pct=minimum,
        passed=passed,
        reason=f"Net return {net_return_pct:.2f}% {comparator} minimum {minimum:.2f}% (gross {gross_return_pct:.2f}% @ tax {tax_rate:.0%})",
    )


def check_duration_gate(
    ticker: str,
    expected_regime_duration: float | None,
    regime_label: str,
    *,
    min_regime_duration_days: float | None = None,
) -> DurationGateResult:
    settings = get_hurdle_settings()
    minimum = settings["min_regime_duration_days"] if min_regime_duration_days is None else max(1.0, min(90.0, float(min_regime_duration_days)))
    normalized_ticker = str(ticker or "").upper()
    normalized_regime = str(regime_label or "")
    if expected_regime_duration is None or float(expected_regime_duration) <= 0:
        return DurationGateResult(
            ticker=normalized_ticker,
            expected_regime_duration=None,
            min_regime_duration_days=minimum,
            regime_label=normalized_regime,
            passed=True,
            reason="No duration estimate - pass by default",
        )
    duration_value = float(expected_regime_duration)
    if normalized_regime != "Bull":
        return DurationGateResult(
            ticker=normalized_ticker,
            expected_regime_duration=duration_value,
            min_regime_duration_days=minimum,
            regime_label=normalized_regime,
            passed=True,
            reason="Duration gate only applies to Bull regime entries",
        )
    passed = duration_value >= minimum
    comparator = ">=" if passed else "<"
    return DurationGateResult(
        ticker=normalized_ticker,
        expected_regime_duration=duration_value,
        min_regime_duration_days=minimum,
        regime_label=normalized_regime,
        passed=passed,
        reason=f"Expected duration {duration_value:.1f} days {comparator} minimum {minimum:.1f} days",
    )
