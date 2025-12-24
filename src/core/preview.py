from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy.orm import Session

from src.core.policy_engine import compute_bucket_totals
from src.db.models import Bucket


@dataclass(frozen=True)
class PreviewRow:
    bucket: str
    current_value: float
    target_value: float
    delta: float


@dataclass(frozen=True)
class PlannerPreview:
    total_value: float
    rows: list[PreviewRow]
    sells: list[PreviewRow]
    buys: list[PreviewRow]
    notes: list[str]
    st_sensitivity: str
    b1_delta: float
    b1_excess: float


def planner_preview(session: Session, *, policy_id: int, scope: str) -> PlannerPreview:
    buckets = session.query(Bucket).filter(Bucket.policy_id == policy_id).order_by(Bucket.code).all()
    totals, warnings = compute_bucket_totals(session=session, policy_id=policy_id, scope=scope, include_cash=True)
    total = float(totals.total_value)
    total_for_calc = total if total > 0 else 1.0

    rows: list[PreviewRow] = []
    for b in buckets:
        current_value = float(totals.by_bucket.get(b.code, 0.0))
        target_value = float(b.target_pct) * total_for_calc
        rows.append(PreviewRow(bucket=b.code, current_value=current_value, target_value=target_value, delta=target_value - current_value))

    sells = [r for r in rows if r.delta < -1e-6]
    buys = [r for r in rows if r.delta > 1e-6]
    sells.sort(key=lambda r: r.delta)  # most negative first
    buys.sort(key=lambda r: -r.delta)

    cash_bucket = next((r for r in rows if r.bucket == "B1"), None)
    cash_current = float(cash_bucket.current_value) if cash_bucket else 0.0
    cash_target = float(cash_bucket.target_value) if cash_bucket else 0.0
    b1_delta = (cash_target - cash_current)
    cash_excess = max(0.0, -b1_delta)
    buy_need = sum(r.delta for r in buys if r.bucket != "B1")

    if scope == "PERSONAL":
        st_line = "ST-sale avoidance: N/A (Personal scope may include IRA)."
    else:
        if cash_excess + 1e-6 >= buy_need:
            st_line = "ST-sale avoidance: OK"
        else:
            st_line = "ST-sale avoidance: May require selling taxable positions; review Planner to select lots and avoid ST gains."

    notes: list[str] = list(warnings)
    if cash_bucket and cash_bucket.delta < 0:
        notes.append("B1 is above target; excess liquidity could fund bucket deficits.")

    return PlannerPreview(
        total_value=total,
        rows=rows,
        sells=sells,
        buys=buys,
        notes=notes,
        st_sensitivity=st_line,
        b1_delta=b1_delta,
        b1_excess=cash_excess,
    )
