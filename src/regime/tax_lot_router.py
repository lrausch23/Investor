from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .persistence import (
    DEFAULT_LTCG_DEFER_WINDOW_DAYS,
    DEFAULT_LOT_SELECTION_METHOD,
    get_alerts,
    get_lot_selection_method,
    get_ltcg_defer_window_days,
    get_paper_positions,
    get_tax_lots,
    get_wash_sale_restrictions,
    save_alert,
)


def _now_text() -> str:
    return datetime.now(timezone.utc).isoformat()


def _implicit_lots(portfolio_id: int, ticker: str) -> list[dict[str, Any]]:
    positions = [
        row for row in get_paper_positions(portfolio_id, status="Open")
        if str(row.get("ticker") or "").upper() == str(ticker or "").upper()
    ]
    payload: list[dict[str, Any]] = []
    for position in positions:
        quantity = float(position.get("quantity") or 0.0)
        payload.append(
            {
                "id": -int(position.get("id") or 0),
                "position_id": int(position.get("id") or 0),
                "ticker": str(position.get("ticker") or "").upper(),
                "quantity": quantity,
                "remaining_quantity": quantity,
                "cost_basis_per_share": float(position.get("entry_price") or 0.0),
                "acquisition_date": str(position.get("entry_date") or ""),
                "status": "open",
                "days_held": max(
                    0,
                    (
                        datetime.now(timezone.utc)
                        - datetime.fromisoformat(str(position.get("entry_date") or _now_text()).replace("Z", "+00:00")).astimezone(timezone.utc)
                    ).days,
                ) if position.get("entry_date") else 0,
                "term": "LT" if position.get("entry_date") and (
                    datetime.now(timezone.utc)
                    - datetime.fromisoformat(str(position.get("entry_date") or _now_text()).replace("Z", "+00:00")).astimezone(timezone.utc)
                ).days >= 366 else "ST",
                "days_to_ltcg": 0,
                "implicit": True,
            }
        )
    return payload


def _available_lots(portfolio_id: int, ticker: str) -> list[dict[str, Any]]:
    lots = get_tax_lots(portfolio_id, ticker=ticker, status="all")
    open_lots = [lot for lot in lots if float(lot.get("remaining_quantity") or 0.0) > 0]
    return open_lots or _implicit_lots(portfolio_id, ticker)


def _sort_key(method: str, lot: dict[str, Any]) -> Any:
    if method == "FIFO":
        return (str(lot.get("acquisition_date") or ""), int(lot.get("id") or 0))
    if method == "LIFO":
        return (str(lot.get("acquisition_date") or ""), int(lot.get("id") or 0))
    return (-float(lot.get("cost_basis_per_share") or 0.0), str(lot.get("acquisition_date") or ""), int(lot.get("id") or 0))


def select_lots(
    portfolio_id: int,
    ticker: str,
    quantity: float,
    *,
    method: str | None = None,
    ltcg_defer_window_days: int | None = None,
) -> list[dict[str, Any]]:
    normalized_method = str(method or get_lot_selection_method() or DEFAULT_LOT_SELECTION_METHOD).upper()
    defer_window = DEFAULT_LTCG_DEFER_WINDOW_DAYS if ltcg_defer_window_days is None else max(0, int(ltcg_defer_window_days))
    if method is None:
        defer_window = get_ltcg_defer_window_days()
    lots = _available_lots(portfolio_id, ticker)
    if normalized_method == "HIFO_LTCG":
        preferred = [lot for lot in lots if int(lot.get("days_to_ltcg") or 0) > defer_window or str(lot.get("term") or "ST") == "LT"]
        deferred = [lot for lot in lots if lot not in preferred]
        ordered = sorted(preferred, key=lambda lot: _sort_key("HIFO", lot)) + sorted(deferred, key=lambda lot: _sort_key("HIFO", lot))
    elif normalized_method == "LIFO":
        ordered = sorted(lots, key=lambda lot: _sort_key("LIFO", lot), reverse=True)
    else:
        ordered = sorted(lots, key=lambda lot: _sort_key(normalized_method, lot), reverse=False)
    remaining = float(quantity or 0.0)
    selections: list[dict[str, Any]] = []
    for lot in ordered:
        if remaining <= 1e-9:
            break
        available = float(lot.get("remaining_quantity") or 0.0)
        if available <= 1e-9:
            continue
        take = min(available, remaining)
        selections.append(
            {
                "lot_id": int(lot.get("id") or 0),
                "position_id": int(lot.get("position_id") or 0),
                "quantity": take,
                "cost_basis_per_share": float(lot.get("cost_basis_per_share") or 0.0),
                "term": str(lot.get("term") or "ST"),
                "days_to_ltcg": int(lot.get("days_to_ltcg") or 0),
                "implicit": bool(lot.get("implicit")),
            }
        )
        remaining -= take
    if remaining > 1e-9:
        raise ValueError("Sell quantity exceeds available tax lots.")
    return selections


def estimate_tax_impact(lot_selections: list[dict[str, Any]], exit_price: float) -> dict[str, Any]:
    summary = {
        "estimated_pnl": 0.0,
        "short_term_gain": 0.0,
        "short_term_loss": 0.0,
        "long_term_gain": 0.0,
        "long_term_loss": 0.0,
        "wash_sale_warning": False,
    }
    for selection in lot_selections:
        pnl = (float(exit_price) - float(selection.get("cost_basis_per_share") or 0.0)) * float(selection.get("quantity") or 0.0)
        summary["estimated_pnl"] += pnl
        if str(selection.get("term") or "ST") == "LT":
            if pnl >= 0:
                summary["long_term_gain"] += pnl
            else:
                summary["long_term_loss"] += pnl
        else:
            if pnl >= 0:
                summary["short_term_gain"] += pnl
            else:
                summary["short_term_loss"] += pnl
                summary["wash_sale_warning"] = True
    return summary


def log_wash_sale_block(
    portfolio_id: int,
    ticker: str,
    plan_id: int | None,
    meta_labeler_score: float | None,
    regime_label: str | None,
    proposed_price: float | None,
) -> dict[str, Any]:
    restrictions = get_wash_sale_restrictions(portfolio_id, ticker=ticker, active_only=True)
    expires = restrictions[0]["restriction_expires"] if restrictions else None
    return save_alert(
        "wash_sale_block",
        f"Wash-sale block: {str(ticker or '').upper()}",
        severity="warning",
        ticker=str(ticker or "").upper(),
        portfolio_id=portfolio_id,
        message=f"Buy signal for {str(ticker or '').upper()} blocked — wash-sale restriction until {expires}.",
        data={
            "plan_id": plan_id,
            "meta_labeler_score": meta_labeler_score,
            "regime_label": regime_label,
            "proposed_price": proposed_price,
            "restriction_expires": expires,
        },
    )


def compute_wash_sale_opportunity_cost(
    portfolio_id: int,
    ticker: str | None = None,
) -> list[dict[str, Any]]:
    alerts = get_alerts(
        portfolio_id=portfolio_id,
        alert_type="wash_sale_block",
        limit=500,
    )
    filtered = [row for row in alerts if not ticker or str(row.get("ticker") or "").upper() == str(ticker).upper()]
    from .paper_trading import _batch_current_prices
    prices = _batch_current_prices([str(row.get("ticker") or "") for row in filtered])
    payload: list[dict[str, Any]] = []
    for row in filtered:
        data = row.get("data") or {}
        proposed_price = float(data.get("proposed_price") or 0.0)
        current_price = float(prices.get(str(row.get("ticker") or "").upper()) or 0.0)
        if proposed_price <= 0 or current_price <= 0:
            hypothetical_return_pct = 0.0
        else:
            hypothetical_return_pct = ((current_price - proposed_price) / proposed_price) * 100.0
        payload.append(
            {
                "ticker": str(row.get("ticker") or "").upper(),
                "blocked_date": row.get("created_at"),
                "proposed_price": proposed_price,
                "current_price": current_price,
                "hypothetical_return_pct": hypothetical_return_pct,
                "restriction_expired": data.get("restriction_expires"),
                "meta_labeler_score": data.get("meta_labeler_score"),
            }
        )
    return payload
