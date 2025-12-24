from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Literal, Optional

from src.core.portfolio import LotView


@dataclass(frozen=True)
class SelectedLot:
    lot_id: int
    acquisition_date: dt.date
    qty: float
    basis_allocated: float
    unrealized: float
    term: Literal["ST", "LT"]
    wash_risk: str


def holding_term(acquisition_date: dt.date, sale_date: dt.date) -> Literal["ST", "LT"]:
    return "LT" if (sale_date - acquisition_date).days >= 365 else "ST"


def _basis_total_for_lot(lot: LotView) -> float:
    return float(lot.adjusted_basis_total) if lot.adjusted_basis_total is not None else float(lot.basis_total)


def select_lots_tax_min(
    *,
    lots: list[LotView],
    sell_qty: float,
    sale_price: float,
    sale_date: dt.date,
    wash_risk_by_lot_id: Optional[dict[int, str]] = None,
    avoid_definite_wash_loss_sales: bool = True,
) -> list[SelectedLot]:
    remaining = float(sell_qty)
    if remaining <= 0:
        return []

    wash_risk_by_lot_id = wash_risk_by_lot_id or {}

    enriched = []
    for l in lots:
        qty = float(l.qty)
        if qty <= 0:
            continue
        basis_total = _basis_total_for_lot(l)
        basis_per_share = basis_total / qty if qty else 0.0
        unrealized = (sale_price - basis_per_share) * qty
        term = holding_term(l.acquisition_date, sale_date)
        wash = wash_risk_by_lot_id.get(l.id, "NONE")
        enriched.append((l, qty, basis_per_share, unrealized, term, wash))

    loss_lots = [e for e in enriched if e[3] < 0]
    lt_gain_lots = [e for e in enriched if e[3] > 0 and e[4] == "LT"]
    lt_flat_lots = [e for e in enriched if abs(e[3]) < 1e-6 and e[4] == "LT"]
    st_lots = [e for e in enriched if e[4] == "ST"]

    loss_lots.sort(key=lambda x: x[3])  # most negative first
    lt_gain_lots.sort(key=lambda x: x[3])  # smallest gain first
    st_lots.sort(key=lambda x: x[3])  # smallest gain first (still avoided)

    ordered = loss_lots + lt_gain_lots + lt_flat_lots + st_lots

    picks: list[SelectedLot] = []
    for l, qty, basis_per_share, unrealized_total, term, wash in ordered:
        if remaining <= 0:
            break

        take = min(qty, remaining)
        basis_alloc = basis_per_share * take
        unrealized_alloc = (sale_price - basis_per_share) * take

        if unrealized_alloc < 0 and wash == "DEFINITE" and avoid_definite_wash_loss_sales:
            continue

        picks.append(
            SelectedLot(
                lot_id=l.id,
                acquisition_date=l.acquisition_date,
                qty=take,
                basis_allocated=basis_alloc,
                unrealized=unrealized_alloc,
                term=term,
                wash_risk=wash,
            )
        )
        remaining -= take

    return picks


def select_lots_fifo(*, lots: list[LotView], sell_qty: float, sale_price: float, sale_date: dt.date) -> list[SelectedLot]:
    remaining = float(sell_qty)
    ordered = sorted(lots, key=lambda l: l.acquisition_date)
    picks: list[SelectedLot] = []
    for l in ordered:
        if remaining <= 0:
            break
        take = min(float(l.qty), remaining)
        basis_total = _basis_total_for_lot(l)
        basis_per_share = basis_total / float(l.qty) if float(l.qty) else 0.0
        picks.append(
            SelectedLot(
                lot_id=l.id,
                acquisition_date=l.acquisition_date,
                qty=take,
                basis_allocated=basis_per_share * take,
                unrealized=(sale_price - basis_per_share) * take,
                term=holding_term(l.acquisition_date, sale_date),
                wash_risk="UNKNOWN",
            )
        )
        remaining -= take
    return picks


def select_lots_lifo(*, lots: list[LotView], sell_qty: float, sale_price: float, sale_date: dt.date) -> list[SelectedLot]:
    remaining = float(sell_qty)
    ordered = sorted(lots, key=lambda l: l.acquisition_date, reverse=True)
    picks: list[SelectedLot] = []
    for l in ordered:
        if remaining <= 0:
            break
        take = min(float(l.qty), remaining)
        basis_total = _basis_total_for_lot(l)
        basis_per_share = basis_total / float(l.qty) if float(l.qty) else 0.0
        picks.append(
            SelectedLot(
                lot_id=l.id,
                acquisition_date=l.acquisition_date,
                qty=take,
                basis_allocated=basis_per_share * take,
                unrealized=(sale_price - basis_per_share) * take,
                term=holding_term(l.acquisition_date, sale_date),
                wash_risk="UNKNOWN",
            )
        )
        remaining -= take
    return picks

