from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from .persistence import get_setting, get_tax_lots, set_setting

DEFAULT_LTCG_OVERRIDE_ENABLED = True
DEFAULT_LTCG_TRIGGER_DAYS_TO_THRESHOLD = 16
DEFAULT_LTCG_MAX_ADDITIONAL_RISK_ATR = 2.0
DEFAULT_LTCG_ORDINARY_RATE = 0.32
DEFAULT_LTCG_RATE = 0.15


@dataclass
class LotOverrideResult:
    """Override decision for a single tax lot."""

    lot_id: int
    ticker: str
    days_held: int
    days_to_ltcg: int
    cost_basis_per_share: float
    remaining_quantity: float
    unrealized_gain_per_share: float
    tax_savings_estimate: float
    additional_risk: float
    override_active: bool
    original_stop: float | None
    overridden_stop: float | None
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class LTCGOverrideResult:
    """Aggregate override decision for a ticker's position."""

    ticker: str
    portfolio_id: int
    lots_checked: int
    lots_overridden: int
    override_active: bool
    protected_quantity: float
    sellable_quantity: float
    total_tax_savings: float
    overridden_stop: float | None
    lot_details: list[LotOverrideResult] = field(default_factory=list)
    reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["lot_details"] = [detail.to_dict() for detail in self.lot_details]
        return payload


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


def _float_setting(key: str, default: float, *, min_value: float, max_value: float) -> float:
    raw = get_setting(key)
    try:
        value = float(str(raw)) if raw not in (None, "") else default
    except Exception:
        value = default
    return max(min_value, min(max_value, value))


def get_ltcg_override_settings() -> dict[str, Any]:
    return {
        "ltcg_override_enabled": _bool_setting("ltcg_override_enabled", DEFAULT_LTCG_OVERRIDE_ENABLED),
        "ltcg_trigger_days_to_threshold": _int_setting(
            "ltcg_trigger_days_to_threshold",
            DEFAULT_LTCG_TRIGGER_DAYS_TO_THRESHOLD,
            min_value=1,
            max_value=60,
        ),
        "ltcg_max_additional_risk_atr": _float_setting(
            "ltcg_max_additional_risk_atr",
            DEFAULT_LTCG_MAX_ADDITIONAL_RISK_ATR,
            min_value=0.5,
            max_value=5.0,
        ),
        "ltcg_ordinary_rate": _float_setting(
            "ltcg_ordinary_rate",
            DEFAULT_LTCG_ORDINARY_RATE,
            min_value=0.0,
            max_value=0.99,
        ),
        "ltcg_rate": _float_setting(
            "ltcg_rate",
            DEFAULT_LTCG_RATE,
            min_value=0.0,
            max_value=0.99,
        ),
    }


def set_ltcg_override_settings(settings: dict[str, Any]) -> dict[str, Any]:
    if "ltcg_override_enabled" in settings:
        set_setting("ltcg_override_enabled", "true" if settings["ltcg_override_enabled"] else "false")
    if "ltcg_trigger_days_to_threshold" in settings:
        int_value = max(1, min(60, int(settings["ltcg_trigger_days_to_threshold"])))
        set_setting("ltcg_trigger_days_to_threshold", str(int_value))
    if "ltcg_max_additional_risk_atr" in settings:
        float_value = max(0.5, min(5.0, float(settings["ltcg_max_additional_risk_atr"])))
        set_setting("ltcg_max_additional_risk_atr", str(float_value))
    if "ltcg_ordinary_rate" in settings:
        float_value = max(0.0, min(0.99, float(settings["ltcg_ordinary_rate"])))
        set_setting("ltcg_ordinary_rate", str(float_value))
    if "ltcg_rate" in settings:
        float_value = max(0.0, min(0.99, float(settings["ltcg_rate"])))
        set_setting("ltcg_rate", str(float_value))
    return get_ltcg_override_settings()


def _lot_expiry(days_to_ltcg: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(days=max(0, int(days_to_ltcg)))).isoformat()


def check_ltcg_override(
    portfolio_id: int,
    ticker: str,
    current_price: float,
    position_stop: float | None = None,
    atr_14: float | None = None,
) -> LTCGOverrideResult:
    settings = get_ltcg_override_settings()
    normalized_ticker = str(ticker or "").upper()
    if not bool(settings["ltcg_override_enabled"]):
        return LTCGOverrideResult(
            ticker=normalized_ticker,
            portfolio_id=int(portfolio_id),
            lots_checked=0,
            lots_overridden=0,
            override_active=False,
            protected_quantity=0.0,
            sellable_quantity=0.0,
            total_tax_savings=0.0,
            overridden_stop=None,
            reason="LTCG override disabled",
        )

    lots = [
        row
        for row in get_tax_lots(int(portfolio_id), ticker=normalized_ticker, status="all")
        if float(row.get("remaining_quantity") or 0.0) > 0
    ]
    total_quantity = sum(float(row.get("remaining_quantity") or 0.0) for row in lots)
    trigger_days = int(settings["ltcg_trigger_days_to_threshold"])
    ordinary_rate = float(settings["ltcg_ordinary_rate"])
    ltcg_rate = float(settings["ltcg_rate"])
    max_additional_risk_atr = float(settings["ltcg_max_additional_risk_atr"])

    lot_details: list[LotOverrideResult] = []
    protected_quantity = 0.0
    total_tax_savings = 0.0
    overridden_stops: list[float] = []
    lots_checked = 0
    current_value = float(current_price or 0.0)

    for lot in lots:
        days_to_ltcg = int(lot.get("days_to_ltcg") or 0)
        if days_to_ltcg <= 0 or days_to_ltcg > trigger_days:
            continue
        lots_checked += 1
        remaining_quantity = float(lot.get("remaining_quantity") or 0.0)
        cost_basis = float(lot.get("cost_basis_per_share") or 0.0)
        unrealized_gain_per_share = current_value - cost_basis
        if unrealized_gain_per_share <= 0 or remaining_quantity <= 0 or current_value <= 0:
            lot_details.append(
                LotOverrideResult(
                    lot_id=int(lot.get("id") or 0),
                    ticker=normalized_ticker,
                    days_held=int(lot.get("days_held") or 0),
                    days_to_ltcg=days_to_ltcg,
                    cost_basis_per_share=cost_basis,
                    remaining_quantity=remaining_quantity,
                    unrealized_gain_per_share=unrealized_gain_per_share,
                    tax_savings_estimate=0.0,
                    additional_risk=0.0,
                    override_active=False,
                    original_stop=float(position_stop) if position_stop is not None else None,
                    overridden_stop=float(position_stop) if position_stop is not None else None,
                    reason="Lot is not profitable; no LTCG protection needed",
                )
            )
            continue

        tax_savings = unrealized_gain_per_share * remaining_quantity * max(0.0, ordinary_rate - ltcg_rate)
        overridden_stop: float | None
        if atr_14 is not None and float(atr_14) > 0:
            overridden_stop = current_value - (max_additional_risk_atr * float(atr_14))
            additional_risk = max(0.0, ((float(position_stop) if position_stop is not None else 0.0) - overridden_stop)) * remaining_quantity
        else:
            overridden_stop = float(position_stop) if position_stop is not None else None
            additional_risk = 0.0
        override_active = tax_savings > additional_risk
        if override_active and overridden_stop is not None and current_value <= overridden_stop:
            override_active = False
            reason = f"Current price {current_value:.2f} is already below overridden stop {overridden_stop:.2f}"
        elif override_active:
            reason = f"Tax savings ${tax_savings:.2f} > additional risk ${additional_risk:.2f}"
        else:
            reason = f"Tax savings ${tax_savings:.2f} <= additional risk ${additional_risk:.2f}"
        detail = LotOverrideResult(
            lot_id=int(lot.get("id") or 0),
            ticker=normalized_ticker,
            days_held=int(lot.get("days_held") or 0),
            days_to_ltcg=days_to_ltcg,
            cost_basis_per_share=cost_basis,
            remaining_quantity=remaining_quantity,
            unrealized_gain_per_share=unrealized_gain_per_share,
            tax_savings_estimate=tax_savings,
            additional_risk=additional_risk,
            override_active=override_active,
            original_stop=float(position_stop) if position_stop is not None else None,
            overridden_stop=overridden_stop,
            reason=reason,
        )
        lot_details.append(detail)
        if override_active:
            protected_quantity += remaining_quantity
            total_tax_savings += tax_savings
            if overridden_stop is not None:
                overridden_stops.append(overridden_stop)

    sellable_quantity = max(0.0, total_quantity - protected_quantity)
    lots_overridden = sum(1 for detail in lot_details if detail.override_active)
    override_active = lots_overridden > 0
    aggregate_overridden_stop: float | None = min(overridden_stops) if overridden_stops else None
    if not lots and current_value > 0:
        reason = "No open tax lots found"
    elif lots_checked == 0:
        reason = f"No profitable lots within {trigger_days} days of LTCG threshold"
    elif override_active:
        reason = (
            f"Protecting {protected_quantity:.0f} shares across {lots_overridden} lot(s); "
            f"sellable quantity {sellable_quantity:.0f}"
        )
    else:
        reason = "No near-threshold lots met LTCG override criteria"
    return LTCGOverrideResult(
        ticker=normalized_ticker,
        portfolio_id=int(portfolio_id),
        lots_checked=lots_checked,
        lots_overridden=lots_overridden,
        override_active=override_active,
        protected_quantity=protected_quantity,
        sellable_quantity=sellable_quantity,
        total_tax_savings=total_tax_savings,
        overridden_stop=aggregate_overridden_stop,
        lot_details=lot_details,
        reason=reason,
    )
