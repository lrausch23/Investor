"""IBKR order and market-hours types.

This module intentionally combines the IB type system and market-hours helpers.
The Sprint 29a spec originally split market-hours logic into a separate file,
but keeping the small business-logic helpers here keeps the IBKR adapter stack
more discoverable.

Holiday source for future updates:
https://www.nyse.com/markets/hours-calendars
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from enum import Enum
from zoneinfo import ZoneInfo


ET = ZoneInfo("America/New_York")


class IBOrderType(Enum):
    MARKET = "MKT"
    LIMIT = "LMT"
    STOP = "STP"
    STOP_LIMIT = "STP_LMT"


class IBOrderAction(Enum):
    BUY = "BUY"
    SELL = "SELL"


class IBOrderStatus(Enum):
    PENDING_SUBMIT = "PendingSubmit"
    PRE_SUBMITTED = "PreSubmitted"
    SUBMITTED = "Submitted"
    PARTIALLY_FILLED = "PartiallyFilled"
    FILLED = "Filled"
    CANCELLED = "Cancelled"
    INACTIVE = "Inactive"
    API_CANCELLED = "ApiCancelled"


class IBTimeInForce(Enum):
    DAY = "DAY"
    GTC = "GTC"
    IOC = "IOC"
    GTD = "GTD"


@dataclass(frozen=True)
class IBOrder:
    order_id: int
    contract_symbol: str
    action: IBOrderAction
    order_type: IBOrderType
    quantity: float
    limit_price: float | None = None
    stop_price: float | None = None
    time_in_force: IBTimeInForce = IBTimeInForce.DAY
    outside_rth: bool = False


@dataclass(frozen=True)
class IBOrderState:
    order_id: int
    status: IBOrderStatus
    filled_qty: float
    remaining_qty: float
    avg_fill_price: float
    last_fill_price: float
    commission: float
    realized_pnl: float
    timestamp: str
    message: str = ""


@dataclass(frozen=True)
class IBPosition:
    account_id: str
    contract_symbol: str
    quantity: float
    avg_cost: float
    market_value: float
    unrealized_pnl: float


@dataclass(frozen=True)
class IBAccountSummary:
    account_id: str
    net_liquidation: float
    total_cash: float
    buying_power: float
    gross_position_value: float
    maintenance_margin: float
    available_funds: float
    unrealized_pnl: float | None = None


class MarketHoursStatus(Enum):
    PRE_MARKET = "pre_market"
    REGULAR = "regular"
    AFTER_HOURS = "after_hours"
    CLOSED = "closed"


US_MARKET_HOLIDAYS_2026 = {
    dt.date(2026, 1, 1),
    dt.date(2026, 1, 19),
    dt.date(2026, 2, 16),
    dt.date(2026, 4, 3),
    dt.date(2026, 5, 25),
    dt.date(2026, 7, 3),
    dt.date(2026, 9, 7),
    dt.date(2026, 11, 26),
    dt.date(2026, 12, 25),
}

US_MARKET_HOLIDAYS_2027 = {
    dt.date(2027, 1, 1),
    dt.date(2027, 1, 18),
    dt.date(2027, 2, 15),
    dt.date(2027, 3, 26),
    dt.date(2027, 5, 31),
    dt.date(2027, 7, 5),
    dt.date(2027, 9, 6),
    dt.date(2027, 11, 25),
    dt.date(2027, 12, 24),
}


def get_market_hours_status(now: dt.datetime | None = None) -> MarketHoursStatus:
    current = now.astimezone(ET) if now else dt.datetime.now(ET)
    holidays = US_MARKET_HOLIDAYS_2026 | US_MARKET_HOLIDAYS_2027
    if current.date() in holidays or current.weekday() >= 5:
        return MarketHoursStatus.CLOSED
    wall = current.timetz().replace(tzinfo=None)
    if dt.time(4, 0) <= wall < dt.time(9, 30):
        return MarketHoursStatus.PRE_MARKET
    if dt.time(9, 30) <= wall < dt.time(16, 0):
        return MarketHoursStatus.REGULAR
    if dt.time(16, 0) <= wall < dt.time(20, 0):
        return MarketHoursStatus.AFTER_HOURS
    return MarketHoursStatus.CLOSED


def is_market_open(now: dt.datetime | None = None) -> bool:
    return get_market_hours_status(now) == MarketHoursStatus.REGULAR


def next_market_open(now: dt.datetime | None = None) -> dt.datetime:
    current = now.astimezone(ET) if now else dt.datetime.now(ET)
    holidays = US_MARKET_HOLIDAYS_2026 | US_MARKET_HOLIDAYS_2027
    probe = current
    while True:
        if probe.weekday() < 5 and probe.date() not in holidays:
            candidate = probe.replace(hour=9, minute=30, second=0, microsecond=0)
            if candidate > current:
                return candidate
        probe = (probe + dt.timedelta(days=1)).replace(hour=9, minute=30, second=0, microsecond=0)
