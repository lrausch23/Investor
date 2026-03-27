from __future__ import annotations

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


def translate_order_request(request: OrderRequest, next_order_id: int) -> IBOrder:
    action = IBOrderAction.BUY if str(request.action or "").lower() == "buy" else IBOrderAction.SELL
    order_type = IBOrderType.MARKET
    limit_price = None
    if request.limit_price is not None and float(request.limit_price) > 0:
        order_type = IBOrderType.LIMIT
        limit_price = float(request.limit_price)
    return IBOrder(
        order_id=int(next_order_id),
        contract_symbol=str(request.ticker or "").upper(),
        action=action,
        order_type=order_type,
        quantity=float(request.quantity or 0.0),
        limit_price=limit_price,
        stop_price=float(request.stop_price) if request.stop_price is not None else None,
        time_in_force=IBTimeInForce.DAY,
        outside_rth=False,
    )


def translate_order_state(state: IBOrderState, *, ticker: str, action: str) -> OrderResult:
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
    return OrderResult(
        order_id=str(state.order_id),
        status=status_map.get(state.status, "pending"),
        ticker=str(ticker or "").upper(),
        action=action,
        quantity=float(state.filled_qty or 0.0) if state.status == IBOrderStatus.PARTIALLY_FILLED else float((state.filled_qty + state.remaining_qty) or 0.0),
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
