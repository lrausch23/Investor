from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from sqlalchemy.orm import Session

from src.core.portfolio import holdings_snapshot
from src.core.types import DriftBucketRow, DriftReport
from src.db.models import Bucket, BucketPolicy


def create_policy_version(
    session: Session,
    *,
    name: str,
    effective_date,
    json_definition: dict[str, Any],
    buckets: list[tuple[str, str, float, float, float, list[str], dict[str, Any]]],
) -> BucketPolicy:
    policy = BucketPolicy(name=name, effective_date=effective_date, json_definition=json_definition or {})
    session.add(policy)
    session.flush()
    for code, bname, min_pct, target_pct, max_pct, allowed, constraints in buckets:
        session.add(
            Bucket(
                policy_id=policy.id,
                code=code,
                name=bname,
                min_pct=float(min_pct),
                target_pct=float(target_pct),
                max_pct=float(max_pct),
                allowed_asset_classes_json=allowed or [],
                constraints_json=constraints or {},
            )
        )
    return policy


@dataclass(frozen=True)
class BucketTotals:
    total_value: float
    by_bucket: dict[str, float]


def compute_bucket_totals(
    session: Session,
    *,
    policy_id: int,
    scope: str = "BOTH",
    include_cash: bool = True,
    overrides_values: Optional[dict[str, float]] = None,
) -> tuple[BucketTotals, list[str]]:
    holdings, cash, warnings = holdings_snapshot(session, policy_id=policy_id, scope=scope)
    by_bucket: dict[str, float] = {"B1": 0.0, "B2": 0.0, "B3": 0.0, "B4": 0.0, "UNASSIGNED": 0.0}

    for h in holdings:
        code = h.bucket_code or "UNASSIGNED"
        by_bucket[code] = by_bucket.get(code, 0.0) + float(h.market_value)

    if include_cash:
        cash_total = sum(float(c.amount) for c in cash)
        by_bucket["B1"] += cash_total

    if overrides_values:
        for k, v in overrides_values.items():
            by_bucket[k] = float(v)

    total_value = sum(v for k, v in by_bucket.items() if k != "UNASSIGNED") + by_bucket.get("UNASSIGNED", 0.0)
    if total_value <= 0:
        warnings.append("Total market value is zero; add lots/cash and security prices.")
    if by_bucket.get("UNASSIGNED", 0.0) > 0:
        warnings.append("Some holdings are UNASSIGNED to buckets; set bucket assignments for all tickers.")
    return BucketTotals(total_value=total_value, by_bucket=by_bucket), warnings


def _status_and_reason(actual: float, min_pct: float, max_pct: float, target_pct: float) -> tuple[str, str]:
    eps = 0.0025  # 0.25%
    if actual < min_pct:
        return "RED", "Below min"
    if actual > max_pct:
        return "RED", "Over max"
    if target_pct > 0 and actual < eps:
        return "RED", "Structural under-allocation"

    tolerance = max(eps, 0.15 * max(target_pct, 0.01))
    if abs(actual - target_pct) > tolerance:
        return "YELLOW", "Off target"
    return "GREEN", "On target"


def compute_drift_report(session: Session, *, policy_id: int, scope: str = "BOTH") -> DriftReport:
    policy = session.query(BucketPolicy).filter(BucketPolicy.id == policy_id).one()
    buckets = session.query(Bucket).filter(Bucket.policy_id == policy_id).order_by(Bucket.code).all()
    totals, warnings = compute_bucket_totals(session, policy_id=policy_id, scope=scope)
    total = totals.total_value or 1.0

    rows: list[DriftBucketRow] = []
    for b in buckets:
        value = float(totals.by_bucket.get(b.code, 0.0))
        actual_pct = value / total if total > 0 else 0.0
        status, reason = _status_and_reason(actual_pct, float(b.min_pct), float(b.max_pct), float(b.target_pct))
        rows.append(
            DriftBucketRow(
                code=b.code,  # type: ignore[arg-type]
                name=b.name,
                min_pct=float(b.min_pct),
                target_pct=float(b.target_pct),
                max_pct=float(b.max_pct),
                value=value,
                actual_pct=actual_pct,
                traffic_light=status,
                reason=reason,
            )
        )

    return DriftReport(policy_id=policy.id, total_value=totals.total_value, bucket_rows=rows, warnings=warnings)


def compute_drift_report_with_overrides(
    session: Session,
    *,
    policy_id: int,
    scope: str,
    bucket_value_overrides: dict[str, float],
) -> DriftReport:
    policy = session.query(BucketPolicy).filter(BucketPolicy.id == policy_id).one()
    buckets = session.query(Bucket).filter(Bucket.policy_id == policy_id).order_by(Bucket.code).all()
    totals, warnings = compute_bucket_totals(
        session, policy_id=policy_id, scope=scope, include_cash=True, overrides_values=bucket_value_overrides
    )
    total = totals.total_value or 1.0
    rows: list[DriftBucketRow] = []
    for b in buckets:
        value = float(totals.by_bucket.get(b.code, 0.0))
        actual_pct = value / total if total > 0 else 0.0
        status, reason = _status_and_reason(actual_pct, float(b.min_pct), float(b.max_pct), float(b.target_pct))
        rows.append(
            DriftBucketRow(
                code=b.code,  # type: ignore[arg-type]
                name=b.name,
                min_pct=float(b.min_pct),
                target_pct=float(b.target_pct),
                max_pct=float(b.max_pct),
                value=value,
                actual_pct=actual_pct,
                traffic_light=status,
                reason=reason,
            )
        )
    return DriftReport(policy_id=policy.id, total_value=totals.total_value, bucket_rows=rows, warnings=warnings)


def policy_constraints(session: Session, *, policy_id: int) -> dict[str, Any]:
    policy = session.query(BucketPolicy).filter(BucketPolicy.id == policy_id).one()
    return policy.json_definition or {}
