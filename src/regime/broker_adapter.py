from __future__ import annotations

import datetime as dt
import logging
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from .config import DEFAULT_RISK_GUARDRAILS, RiskGuardrails
from .persistence import (
    add_wash_sale_restriction,
    close_paper_position,
    close_tax_lot,
    count_todays_trades,
    create_tax_lot,
    get_paper_portfolio,
    get_paper_portfolio_summary,
    get_paper_position,
    get_paper_positions,
    get_tax_lots,
    is_wash_sale_restricted,
    log_audit_event,
    open_paper_position,
    update_paper_position_quantity,
    update_paper_portfolio,
)
from .tax_lot_router import log_wash_sale_block, select_lots

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OrderRequest:
    portfolio_id: int
    ticker: str
    action: str
    quantity: float
    order_type: str = "market"
    limit_price: float | None = None
    stop_price: float | None = None
    theme_id: int | None = None
    role: str | None = None
    source: str = "manual"
    notes: str | None = None


@dataclass(frozen=True)
class OrderResult:
    order_id: str
    status: str
    ticker: str
    action: str
    quantity: float
    filled_price: float | None = None
    filled_at: str | None = None
    message: str | None = None


@dataclass(frozen=True)
class PositionInfo:
    position_id: int
    ticker: str
    quantity: float
    side: str
    entry_price: float
    current_price: float | None
    market_value: float | None
    unrealized_pnl: float | None
    stop_price: float | None
    target_price: float | None
    role: str | None
    theme_id: int | None


@dataclass(frozen=True)
class AccountSummary:
    portfolio_id: int
    equity: float
    cash: float
    market_value: float
    realized_pnl: float
    unrealized_pnl: float
    daily_pnl: float
    exposure_pct: float


@dataclass(frozen=True)
class GuardrailCheck:
    name: str
    passed: bool
    message: str
    actual: float | int | None = None
    limit: float | int | None = None


@dataclass(frozen=True)
class GuardrailResult:
    allowed: bool
    estimated_price: float | None
    estimated_order_value: float | None
    checks: list[GuardrailCheck] = field(default_factory=list)


class BrokerAdapter(ABC):
    """Abstract execution adapter.

    Implementations may have different fill semantics:
    - instant fill (`PaperBrokerAdapter`)
    - async submit/fill (`IBKRBrokerAdapter`)
    - partial fill / reject (`MockIBBackend`-driven adapters)
    """

    @abstractmethod
    def submit_order(self, order: OrderRequest) -> OrderResult:
        raise NotImplementedError

    @abstractmethod
    def cancel_order(self, order_id: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def get_order_status(self, order_id: str) -> OrderResult | None:
        raise NotImplementedError

    @abstractmethod
    def get_positions(self) -> list[PositionInfo]:
        raise NotImplementedError

    @abstractmethod
    def get_account_summary(self) -> AccountSummary:
        raise NotImplementedError


def _now_text() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


class PaperBrokerAdapter(BrokerAdapter):
    def __init__(self, portfolio_id: int):
        self.portfolio_id = int(portfolio_id)
        self._completed_orders: dict[str, OrderResult] = {}

    def submit_order(self, order: OrderRequest) -> OrderResult:
        """Submit an order with immediate simulated fill semantics.

        Paper trading intentionally fills synchronously at the latest cached market
        price. This is simpler than real broker behavior and is distinct from the
        async / partial lifecycle supported by the IBKR adapter.
        """
        from .paper_trading import _batch_current_prices

        ticker = str(order.ticker or "").upper()
        price_map = _batch_current_prices([ticker])
        fill_price = float(price_map.get(ticker) or order.limit_price or 0.0)
        order_id = str(uuid.uuid4())
        if fill_price <= 0:
            result = OrderResult(
                order_id=order_id,
                status="rejected",
                ticker=ticker,
                action=order.action,
                quantity=order.quantity,
                message="No market price available.",
            )
            self._completed_orders[order_id] = result
            return result

        portfolio = get_paper_portfolio(self.portfolio_id)
        if portfolio is None:
            result = OrderResult(
                order_id=order_id,
                status="rejected",
                ticker=ticker,
                action=order.action,
                quantity=order.quantity,
                message="Paper portfolio not found.",
            )
            self._completed_orders[order_id] = result
            return result

        now_text = _now_text()
        current_cash = float(portfolio.get("current_cash") or 0.0)
        quantity = float(order.quantity or 0.0)
        if quantity <= 0:
            result = OrderResult(
                order_id=order_id,
                status="rejected",
                ticker=ticker,
                action=order.action,
                quantity=quantity,
                message="Order quantity must be positive.",
            )
            self._completed_orders[order_id] = result
            return result

        if str(order.action or "").lower() == "buy":
            total_cost = quantity * fill_price
            if total_cost > current_cash:
                result = OrderResult(
                    order_id=order_id,
                    status="rejected",
                    ticker=ticker,
                    action=order.action,
                    quantity=quantity,
                    message="Insufficient cash.",
                )
                self._completed_orders[order_id] = result
                return result
            position = open_paper_position(
                self.portfolio_id,
                ticker,
                quantity,
                fill_price,
                now_text,
                theme_id=order.theme_id,
                role=order.role or "Critical-Path",
                stop_price=order.stop_price,
            )
            create_tax_lot(
                portfolio_id=self.portfolio_id,
                position_id=int(position.get("id") or 0) if position.get("id") is not None else None,
                ticker=ticker,
                quantity=quantity,
                cost_basis_per_share=fill_price,
                acquisition_date=now_text,
            )
            update_paper_portfolio(self.portfolio_id, current_cash=current_cash - total_cost)
        else:
            open_positions = [
                row for row in get_paper_positions(self.portfolio_id, status="Open")
                if str(row.get("ticker") or "").upper() == ticker
            ]
            if not open_positions:
                result = OrderResult(
                    order_id=order_id,
                    status="rejected",
                    ticker=ticker,
                    action=order.action,
                    quantity=quantity,
                    message="No open position found.",
                )
                self._completed_orders[order_id] = result
                return result
            remaining = quantity
            credited = 0.0
            lot_selections = select_lots(self.portfolio_id, ticker, quantity)
            affected_position_ids: set[int] = set()
            for selection in lot_selections:
                position_id = int(selection.get("position_id") or 0)
                affected_position_ids.add(position_id)
                if selection.get("implicit"):
                    position = get_paper_position(position_id)
                    if position is None:
                        continue
                    pos_qty = float(position.get("quantity") or 0.0)
                    if abs(pos_qty - float(selection.get("quantity") or 0.0)) > 1e-9:
                        result = OrderResult(
                            order_id=order_id,
                            status="rejected",
                            ticker=ticker,
                            action=order.action,
                            quantity=quantity,
                            message="Partial exits are not supported for legacy positions without tax lots.",
                        )
                        self._completed_orders[order_id] = result
                        return result
                    closed_position = close_paper_position(position_id, fill_price, now_text, order.source or "broker_adapter")
                    pnl = float(closed_position.get("realized_pnl") or 0.0) if closed_position else 0.0
                    if pnl < 0:
                        add_wash_sale_restriction(self.portfolio_id, ticker, now_text, pnl)
                else:
                    lot_result = close_tax_lot(
                        lot_id=int(selection.get("lot_id") or 0),
                        quantity_to_close=float(selection.get("quantity") or 0.0),
                        exit_price=fill_price,
                        exit_date=now_text,
                    )
                    pnl = float(lot_result.get("realized_pnl") or 0.0) if lot_result else 0.0
                    if pnl < 0:
                        add_wash_sale_restriction(
                            self.portfolio_id,
                            ticker,
                            now_text,
                            pnl,
                            lot_id=int(selection.get("lot_id") or 0),
                        )
                credited += float(selection.get("quantity") or 0.0) * fill_price
                remaining -= float(selection.get("quantity") or 0.0)
            if remaining > 1e-9:
                result = OrderResult(
                    order_id=order_id,
                    status="rejected",
                    ticker=ticker,
                    action=order.action,
                    quantity=quantity,
                    message="Sell quantity exceeds open position size.",
                )
                self._completed_orders[order_id] = result
                return result
            for position_id in affected_position_ids:
                remaining_lots = [
                    row for row in get_tax_lots(self.portfolio_id, ticker=ticker, status="all")
                    if int(row.get("position_id") or 0) == position_id and float(row.get("remaining_quantity") or 0.0) > 1e-9
                ]
                if not remaining_lots:
                    position = get_paper_position(position_id)
                    if position and str(position.get("status") or "") == "Open":
                        close_paper_position(position_id, fill_price, now_text, order.source or "broker_adapter")
                else:
                    update_paper_position_quantity(
                        position_id,
                        sum(float(row.get("remaining_quantity") or 0.0) for row in remaining_lots),
                    )
            update_paper_portfolio(self.portfolio_id, current_cash=current_cash + credited)

        result = OrderResult(
            order_id=order_id,
            status="filled",
            ticker=ticker,
            action=order.action,
            quantity=quantity,
            filled_price=fill_price,
            filled_at=now_text,
        )
        self._completed_orders[order_id] = result
        return result

    def cancel_order(self, order_id: str) -> bool:
        del order_id
        return False

    def get_order_status(self, order_id: str) -> OrderResult | None:
        return self._completed_orders.get(str(order_id))

    def get_positions(self) -> list[PositionInfo]:
        from .paper_trading import _batch_current_prices

        positions = get_paper_positions(self.portfolio_id, status="Open")
        prices = _batch_current_prices([str(row.get("ticker") or "") for row in positions])
        payload: list[PositionInfo] = []
        for row in positions:
            ticker = str(row.get("ticker") or "").upper()
            quantity = float(row.get("quantity") or 0.0)
            entry_price = float(row.get("entry_price") or 0.0)
            current_price = prices.get(ticker, entry_price if entry_price > 0 else None)
            market_value = (quantity * current_price) if current_price is not None else None
            unrealized = ((current_price - entry_price) * quantity) if current_price is not None else None
            payload.append(
                PositionInfo(
                    position_id=int(row.get("id") or 0),
                    ticker=ticker,
                    quantity=quantity,
                    side=str(row.get("side") or "long"),
                    entry_price=entry_price,
                    current_price=current_price,
                    market_value=market_value,
                    unrealized_pnl=unrealized,
                    stop_price=float(row["stop_price"]) if row.get("stop_price") is not None else None,
                    target_price=float(row["target_price"]) if row.get("target_price") is not None else None,
                    role=str(row.get("role")) if row.get("role") is not None else None,
                    theme_id=int(row["theme_id"]) if row.get("theme_id") is not None else None,
                )
            )
        return payload

    def get_account_summary(self) -> AccountSummary:
        summary = get_paper_portfolio_summary(self.portfolio_id)
        total_equity = float(summary.get("current_cash") or 0.0) + float(summary.get("total_market_value") or 0.0)
        exposure_pct = (
            float(summary.get("total_market_value") or 0.0) / total_equity
            if total_equity > 0
            else 0.0
        )
        realized = float(summary.get("realized_pnl") or 0.0)
        unrealized = float(summary.get("unrealized_pnl") or 0.0)
        return AccountSummary(
            portfolio_id=self.portfolio_id,
            equity=total_equity,
            cash=float(summary.get("current_cash") or 0.0),
            market_value=float(summary.get("total_market_value") or 0.0),
            realized_pnl=realized,
            unrealized_pnl=unrealized,
            daily_pnl=realized + unrealized,
            exposure_pct=exposure_pct,
        )


class MockBrokerAdapter(BrokerAdapter):
    def __init__(self, *, should_fill: bool = True, latency_seconds: float = 0.0, fill_price: float = 100.0):
        self.should_fill = should_fill
        self.latency_seconds = latency_seconds
        self.fill_price = fill_price
        self.submitted_orders: list[OrderRequest] = []
        self._results: dict[str, OrderResult] = {}
        self._positions: list[PositionInfo] = []
        self._summary = AccountSummary(
            portfolio_id=0,
            equity=100000.0,
            cash=100000.0,
            market_value=0.0,
            realized_pnl=0.0,
            unrealized_pnl=0.0,
            daily_pnl=0.0,
            exposure_pct=0.0,
        )

    def submit_order(self, order: OrderRequest) -> OrderResult:
        self.submitted_orders.append(order)
        order_id = str(uuid.uuid4())
        result = OrderResult(
            order_id=order_id,
            status="filled" if self.should_fill else "rejected",
            ticker=str(order.ticker or "").upper(),
            action=order.action,
            quantity=float(order.quantity or 0.0),
            filled_price=self.fill_price if self.should_fill else None,
            filled_at=_now_text() if self.should_fill else None,
            message=None if self.should_fill else "Mock rejection",
        )
        self._results[order_id] = result
        return result

    def cancel_order(self, order_id: str) -> bool:
        return str(order_id) in self._results

    def get_order_status(self, order_id: str) -> OrderResult | None:
        return self._results.get(str(order_id))

    def get_positions(self) -> list[PositionInfo]:
        return list(self._positions)

    def get_account_summary(self) -> AccountSummary:
        return self._summary

    def set_account_summary(self, summary: AccountSummary) -> None:
        self._summary = summary

    def set_positions(self, positions: list[PositionInfo]) -> None:
        self._positions = list(positions)


def _estimate_order_price(order: OrderRequest, adapter: BrokerAdapter) -> float | None:
    if order.limit_price is not None and float(order.limit_price) > 0:
        return float(order.limit_price)
    if hasattr(adapter, "fill_price"):
        try:
            fill_price = float(getattr(adapter, "fill_price"))
            if fill_price > 0:
                return fill_price
        except Exception:
            pass
    ticker = str(order.ticker or "").upper()
    for position in adapter.get_positions():
        if position.ticker == ticker and position.current_price is not None:
            return float(position.current_price)
    if isinstance(adapter, PaperBrokerAdapter):
        from .paper_trading import _batch_current_prices

        return _batch_current_prices([ticker]).get(ticker)
    return None


def validate_guardrails(
    order: OrderRequest,
    adapter: BrokerAdapter,
    guardrails: RiskGuardrails = DEFAULT_RISK_GUARDRAILS,
) -> GuardrailResult:
    estimated_price = _estimate_order_price(order, adapter)
    quantity = float(order.quantity or 0.0)
    order_value = (estimated_price * quantity) if estimated_price is not None else None
    summary = adapter.get_account_summary()
    checks: list[GuardrailCheck] = []

    if order_value is not None and summary.equity > 0:
        position_pct = order_value / summary.equity
        checks.append(
            GuardrailCheck(
                name="max_position_pct",
                passed=position_pct <= guardrails.max_position_pct,
                message=f"Position size {position_pct:.1%} vs max {guardrails.max_position_pct:.1%}.",
                actual=position_pct,
                limit=guardrails.max_position_pct,
            )
        )
    if order_value is not None:
        checks.append(
            GuardrailCheck(
                name="max_single_order_value",
                passed=order_value <= guardrails.max_single_order_value,
                message=f"Order value ${order_value:,.2f} vs max ${guardrails.max_single_order_value:,.2f}.",
                actual=order_value,
                limit=guardrails.max_single_order_value,
            )
        )

    checks.append(
        GuardrailCheck(
            name="daily_loss_limit",
            passed=abs(float(summary.daily_pnl or 0.0)) <= guardrails.daily_loss_limit or float(summary.daily_pnl or 0.0) >= 0,
            message=f"Daily P&L ${float(summary.daily_pnl or 0.0):,.2f} vs loss limit ${guardrails.daily_loss_limit:,.2f}.",
            actual=float(summary.daily_pnl or 0.0),
            limit=guardrails.daily_loss_limit,
        )
    )

    today_trades = count_todays_trades(summary.portfolio_id)
    checks.append(
        GuardrailCheck(
            name="max_trades_per_day",
            passed=today_trades < guardrails.max_trades_per_day,
            message=f"Today's trades {today_trades} vs max {guardrails.max_trades_per_day}.",
            actual=today_trades,
            limit=guardrails.max_trades_per_day,
        )
    )

    if order_value is not None:
        exposure = float(summary.market_value or 0.0)
        if str(order.action or "").lower() == "buy":
            exposure += order_value
        else:
            exposure = max(0.0, exposure - order_value)
        exposure_pct = (exposure / summary.equity) if summary.equity > 0 else 0.0
        checks.append(
            GuardrailCheck(
                name="max_total_exposure_pct",
                passed=exposure_pct <= guardrails.max_total_exposure_pct,
                message=f"Projected exposure {exposure_pct:.1%} vs max {guardrails.max_total_exposure_pct:.1%}.",
                actual=exposure_pct,
                limit=guardrails.max_total_exposure_pct,
            )
        )

    if str(order.action or "").lower() == "buy":
        restricted = is_wash_sale_restricted(summary.portfolio_id, order.ticker)
        checks.append(
            GuardrailCheck(
                name="wash_sale_restricted",
                passed=not restricted,
                message=(
                    "No wash-sale restriction."
                    if not restricted
                    else f"{order.ticker} is on the wash-sale restricted list. Buy blocked for 31 days after loss sale."
                ),
            )
        )
        if restricted:
            try:
                log_wash_sale_block(
                    summary.portfolio_id,
                    order.ticker,
                    None,
                    None,
                    None,
                    estimated_price,
                )
            except Exception:
                logger.debug("Unable to log wash-sale block for %s", order.ticker, exc_info=True)
        from .anti_churn import is_churn_restricted

        churn_restricted = is_churn_restricted(summary.portfolio_id, order.ticker)
        checks.append(
            GuardrailCheck(
                name="anti_churn_velocity",
                passed=not churn_restricted,
                message=(
                    "No anti-churn restriction."
                    if not churn_restricted
                    else f"Round-trip velocity limit reached for {order.ticker}. Cooldown active."
                ),
            )
        )

    return GuardrailResult(
        allowed=all(check.passed for check in checks),
        estimated_price=estimated_price,
        estimated_order_value=order_value,
        checks=checks,
    )


def submit_guarded_order(
    order: OrderRequest,
    adapter: BrokerAdapter,
    guardrails: RiskGuardrails = DEFAULT_RISK_GUARDRAILS,
    *,
    actor: str = "user",
) -> tuple[GuardrailResult, OrderResult | None]:
    guardrail_result = validate_guardrails(order, adapter, guardrails=guardrails)
    order_id = str(uuid.uuid4())
    log_audit_event(
        order_id=order_id,
        portfolio_id=order.portfolio_id,
        event_type="guardrail_check",
        ticker=order.ticker,
        action=order.action,
        quantity=order.quantity,
        price=guardrail_result.estimated_price,
        actor=actor,
        details=order.notes or "",
        guardrail_result=guardrail_result,
    )
    if not guardrail_result.allowed:
        log_audit_event(
            order_id=order_id,
            portfolio_id=order.portfolio_id,
            event_type="guardrail_blocked",
            ticker=order.ticker,
            action=order.action,
            quantity=order.quantity,
            price=guardrail_result.estimated_price,
            actor=actor,
            details="Order blocked by risk guardrails.",
            guardrail_result=guardrail_result,
        )
        return guardrail_result, None

    result = adapter.submit_order(order)
    log_audit_event(
        order_id=result.order_id or order_id,
        portfolio_id=order.portfolio_id,
        event_type="submitted",
        ticker=order.ticker,
        action=order.action,
        quantity=order.quantity,
        price=guardrail_result.estimated_price,
        actor=actor,
        details=order.notes or "",
        guardrail_result=guardrail_result,
    )
    event_type = "filled" if result.status == "filled" else "rejected"
    log_audit_event(
        order_id=result.order_id or order_id,
        portfolio_id=order.portfolio_id,
        event_type=event_type,
        ticker=result.ticker,
        action=result.action,
        quantity=result.quantity,
        price=result.filled_price,
        actor=actor,
        details=result.message or "",
        guardrail_result=guardrail_result,
    )
    return guardrail_result, result
