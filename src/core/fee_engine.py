from __future__ import annotations

from sqlalchemy.orm import Session

from src.core.portfolio import holdings_snapshot
from src.core.types import FeeRow, FeeSummary
from src.db.models import BucketPolicy


def fee_summary(session: Session, *, policy_id: int, scope: str = "BOTH") -> FeeSummary:
    _ = session.query(BucketPolicy).filter(BucketPolicy.id == policy_id).one()
    holdings, cash, warnings = holdings_snapshot(session, policy_id=policy_id, scope=scope)
    _ = cash

    total = sum(h.market_value for h in holdings) or 1.0
    weighted_er_total = sum(h.market_value * float(h.expense_ratio) for h in holdings) / total
    cost_drag_total = sum(h.market_value * float(h.expense_ratio) for h in holdings)

    by_tp: dict[str, dict[str, float]] = {}
    for h in holdings:
        scope = h.taxpayer_name
        by_tp.setdefault(scope, {"mv": 0.0, "er": 0.0})
        by_tp[scope]["mv"] += h.market_value
        by_tp[scope]["er"] += h.market_value * float(h.expense_ratio)

    scope_label = "Household" if scope == "BOTH" else ("Trust only" if scope == "TRUST" else "Personal only")
    rows = [FeeRow(scope=scope_label, weighted_expense_ratio=weighted_er_total, cost_drag=cost_drag_total)]
    for scope, agg in by_tp.items():
        mv = agg["mv"] or 1.0
        rows.append(FeeRow(scope=scope, weighted_expense_ratio=agg["er"] / mv, cost_drag=agg["er"]))
    return FeeSummary(policy_id=policy_id, rows=rows, warnings=warnings)
