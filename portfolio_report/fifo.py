from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

from portfolio_report.transactions import NormalizedTransaction


@dataclass
class Lot:
    open_date: dt.date
    qty: float
    unit_cost: float  # includes per-share fees if known


@dataclass(frozen=True)
class RealizedMatch:
    symbol: str
    sell_date: dt.date
    qty: float
    proceeds: float
    cost: float | None
    pnl: float | None
    carry_in_basis_unknown: bool


def fifo_realized_pnl(
    txs: list[NormalizedTransaction],
    *,
    symbol: str,
) -> tuple[list[RealizedMatch], list[str]]:
    """
    FIFO realized P&L for a single symbol.

    Notes:
    - If a SELL has no available lots (carry-in holdings), we flag basis unknown and set cost/pnl to None.
    - Fees handling is best-effort and depends on whether the input provides them.
    """
    warnings: list[str] = []
    lots: list[Lot] = []
    matches: list[RealizedMatch] = []
    carry_in_sells = 0
    carry_in_shares = 0.0
    carry_in_first: dt.date | None = None
    carry_in_last: dt.date | None = None

    for t in sorted([x for x in txs if x.symbol == symbol], key=lambda x: x.date):
        if t.tx_type not in {"BUY", "SELL"}:
            continue
        qty = float(t.qty or 0.0)
        if qty == 0:
            continue
        px = float(t.price or 0.0)
        # If amount is present, infer average unit price from proceeds/cost.
        # For buys, amount is typically negative cash impact.
        if (px == 0.0 or px is None) and t.amount is not None and abs(qty) > 0:
            gross = abs(float(t.amount))
            px = gross / abs(qty)
        if px == 0:
            continue

        if t.tx_type == "BUY":
            lots.append(Lot(open_date=t.date, qty=abs(qty), unit_cost=abs(px)))
        else:  # SELL
            sell_qty = abs(qty)
            proceeds = abs(px) * sell_qty
            remaining = sell_qty
            cost = 0.0
            carry_in = False
            while remaining > 1e-12 and lots:
                lot = lots[0]
                take = min(lot.qty, remaining)
                cost += take * lot.unit_cost
                lot.qty -= take
                remaining -= take
                if lot.qty <= 1e-12:
                    lots.pop(0)
            if remaining > 1e-12:
                # Sold more than we can match: carry-in basis unknown.
                carry_in = True
                carry_in_sells += 1
                carry_in_shares += float(remaining)
                carry_in_first = t.date if carry_in_first is None else min(carry_in_first, t.date)
                carry_in_last = t.date if carry_in_last is None else max(carry_in_last, t.date)
                # Do not invent basis: mark cost/pnl unknown so downstream reporting doesn't show misleading P&L.
                matches.append(
                    RealizedMatch(
                        symbol=symbol,
                        sell_date=t.date,
                        qty=sell_qty,
                        proceeds=proceeds,
                        cost=None,
                        pnl=None,
                        carry_in_basis_unknown=True,
                    )
                )
                continue
            pnl = proceeds - cost
            matches.append(
                RealizedMatch(
                    symbol=symbol,
                    sell_date=t.date,
                    qty=sell_qty,
                    proceeds=proceeds,
                    cost=cost,
                    pnl=pnl,
                    carry_in_basis_unknown=carry_in,
                )
            )
    if carry_in_sells > 0:
        span = ""
        if carry_in_first and carry_in_last:
            span = f" ({carry_in_first.isoformat()} → {carry_in_last.isoformat()})"
        warnings.append(
            f"{symbol}: {carry_in_sells} SELL(s) exceed available lots by total {carry_in_shares:.6g} shares (carry-in basis unknown){span}."
        )
    return matches, warnings
