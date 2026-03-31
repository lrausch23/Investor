from __future__ import annotations

import logging

from .broker_adapter import AccountSummary, OrderRequest, OrderResult, PositionInfo
from .ib_types import (
    IBAccountSummary,
    IBOrder,
    IBOrderAction,
    IBOrderState,
    IBOrderStatus,
    IBOrderType,
    IBPosition,
    IBTimeInForce,
)

logger = logging.getLogger(__name__)

_ORDER_TYPE_MAP = {
    "market": IBOrderType.MARKET,
    "limit": IBOrderType.LIMIT,
    "marketable_limit": IBOrderType.LIMIT,
    "stop": IBOrderType.STOP,
}

_TIF_MAP = {
    "DAY": IBTimeInForce.DAY,
    "GTC": IBTimeInForce.GTC,
    "IOC": IBTimeInForce.IOC,
    "GTD": IBTimeInForce.GTD,
}


def translate_order_request(request: OrderRequest, next_order_id: int) -> IBOrder:
    action = IBOrderAction.BUY if str(request.action or "").lower() == "buy" else IBOrderAction.SELL
    order_type = _ORDER_TYPE_MAP.get(str(request.order_type or "").lower(), IBOrderType.LIMIT)
    limit_price = float(request.limit_price) if request.limit_price is not None and float(request.limit_price) > 0 else None
    if order_type == IBOrderType.LIMIT and limit_price is None:
        order_type = IBOrderType.MARKET
        logger.warning("Limit order for %s has no limit_price, falling back to MARKET", request.ticker)
    tif = _TIF_MAP.get(str(getattr(request, "time_in_force", "DAY") or "DAY").upper(), IBTimeInForce.DAY)
    return IBOrder(
        order_id=int(next_order_id),
        contract_symbol=str(request.ticker or "").upper(),
        action=action,
        order_type=order_type,
        quantity=float(request.quantity or 0.0),
        limit_price=limit_price,
        stop_price=float(request.stop_price) if request.stop_price is not None else None,
        time_in_force=tif,
        outside_rth=False,
    )


def translate_order_state(state: IBOrderState, *, ticker: str, action: str) -> OrderResult:
    filled_qty = float(state.filled_qty or 0.0)
    remaining_qty = float(state.remaining_qty or 0.0)
    status_map = {
        IBOrderStatus.FILLED: "filled",
        IBOrderStatus.CANCELLED: "cancelled",
        IBOrderStatus.API_CANCELLED: "cancelled",
        IBOrderStatus.INACTIVE: "rejected",
        IBOrderStatus.PARTIALLY_FILLED: "partially_filled",
        IBOrderStatus.SUBMITTED: "submitted",
        IBOrderStatus.PRE_SUBMITTED: "submitted",
        IBOrderStatus.PENDING_SUBMIT: "pending",
    }
    status = status_map.get(state.status)
    if status is None:
        if filled_qty > 0 and remaining_qty <= 0:
            status = "filled"
        elif filled_qty > 0 and remaining_qty > 0:
            status = "partially_filled"
        elif str(state.message or "").strip().lower() == "filled":
            status = "filled"
        else:
            status = "pending"
    return OrderResult(
        order_id=str(state.order_id),
        status=status,
        ticker=str(ticker or "").upper(),
        action=action,
        quantity=filled_qty if status == "partially_filled" else float((filled_qty + remaining_qty) or 0.0),
        filled_price=float(state.avg_fill_price or state.last_fill_price or 0.0) or None,
        filled_at=state.timestamp,
        message=state.message or None,
    )


def translate_position(position: IBPosition, *, position_id: int = 0) -> PositionInfo:
    current_price = (float(position.market_value) / float(position.quantity)) if float(position.quantity or 0.0) else None
    return PositionInfo(
        position_id=position_id,
        ticker=position.contract_symbol,
        quantity=float(position.quantity),
        side="long",
        entry_price=float(position.avg_cost),
        current_price=current_price,
        market_value=float(position.market_value),
        unrealized_pnl=float(position.unrealized_pnl),
        stop_price=None,
        target_price=None,
        role=None,
        theme_id=None,
    )


def translate_account_summary(summary: IBAccountSummary, *, portfolio_id: int) -> AccountSummary:
    equity = float(summary.net_liquidation)
    market_value = float(summary.gross_position_value)
    cash = float(summary.total_cash)
    unrealized = float(summary.unrealized_pnl) if getattr(summary, "unrealized_pnl", None) is not None else (equity - cash)
    return AccountSummary(
        portfolio_id=int(portfolio_id),
        equity=equity,
        cash=cash,
        market_value=market_value,
        realized_pnl=0.0,
        unrealized_pnl=unrealized,
        daily_pnl=0.0,
        exposure_pct=(market_value / equity) if equity > 0 else 0.0,
    )
