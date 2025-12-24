from __future__ import annotations

import datetime as dt
import os
from dataclasses import dataclass
from typing import Literal, Optional

from sqlalchemy.orm import Session
from sqlalchemy import case, func

from src.core.analytics import allocation_breakdown, cashflow_summary, st_exposure, wash_risk_summary
from src.core.connection_preference import preferred_active_connection_ids_for_taxpayers
from src.core.fee_engine import fee_summary
from src.core.policy_engine import compute_drift_report
from src.core.preview import planner_preview
from src.core.tax_engine import get_or_create_tax_assumptions, tax_summary_ytd_with_net
from src.db.models import (
    BrokerLotClosure,
    BrokerWashSaleEvent,
    BucketPolicy,
    ExternalConnection,
    SyncRun,
    TaxpayerEntity,
)


DashboardScope = Literal["household", "trust", "personal"]


def parse_scope(raw: Optional[str]) -> DashboardScope:
    v = (raw or "").strip().lower()
    if v in {"trust", "personal", "household"}:
        return v  # type: ignore[return-value]
    return "household"


def scope_to_internal(scope: DashboardScope) -> str:
    if scope == "trust":
        return "TRUST"
    if scope == "personal":
        return "PERSONAL"
    return "BOTH"


def scope_label(scope: DashboardScope) -> str:
    if scope == "trust":
        return "Trust only"
    if scope == "personal":
        return "Personal only"
    return "Household"


def partial_dataset_threshold() -> float:
    try:
        return float(os.environ.get("PARTIAL_DATASET_THRESHOLD", "100000"))
    except Exception:
        return 100000.0


@dataclass(frozen=True)
class DashboardData:
    scope: DashboardScope
    scope_label: str
    policy: Optional[BucketPolicy]
    drift: Optional[object]
    tax: object
    fees: Optional[object]
    breakdown: Optional[object]
    st_exposure: list[object]
    wash: object
    cashflows: list[object]
    preview: Optional[object]
    sync_connections: list[dict]
    partial_dataset_warning: Optional[str]


def build_dashboard(session: Session, *, scope: DashboardScope, as_of: dt.date) -> DashboardData:
    policy = session.query(BucketPolicy).order_by(BucketPolicy.effective_date.desc()).first()
    if policy is None:
        return DashboardData(
            scope=scope,
            scope_label=scope_label(scope),
            policy=None,
            drift=None,
            tax=tax_summary_ytd_with_net(session=session, as_of=as_of, scope=scope, assumptions=None),
            fees=None,
            breakdown=None,
            st_exposure=[],
            wash=wash_risk_summary(session=session, as_of=as_of, scope=scope, lookback_days=30),
            cashflows=cashflow_summary(session=session, as_of=as_of, scope=scope),
            preview=None,
            sync_connections=[],
            partial_dataset_warning=None,
        )

    internal = scope_to_internal(scope)
    drift = compute_drift_report(session=session, policy_id=policy.id, scope=internal)

    assumptions = get_or_create_tax_assumptions(session=session)
    tax = tax_summary_ytd_with_net(session=session, as_of=as_of, scope=scope, assumptions=assumptions)
    fees = fee_summary(session=session, policy_id=policy.id, scope=internal)
    breakdown = allocation_breakdown(session=session, policy_id=policy.id, scope=internal)
    st = st_exposure(session=session, as_of=as_of, scope=scope)
    wash = wash_risk_summary(session=session, as_of=as_of, scope=scope, lookback_days=30)
    cashflows = cashflow_summary(session=session, as_of=as_of, scope=scope)
    preview = planner_preview(session=session, policy_id=policy.id, scope=internal)
    sync_conns = sync_connections_coverage(session=session, scope=scope, as_of=as_of)

    warn = None
    thresh = partial_dataset_threshold()
    if thresh > 0 and drift.total_value < thresh:
        warn = "This appears to be a partial dataset; metrics reflect imported subset only."

    return DashboardData(
        scope=scope,
        scope_label=scope_label(scope),
        policy=policy,
        drift=drift,
        tax=tax,
        fees=fees,
        breakdown=breakdown,
        st_exposure=st,
        wash=wash,
        cashflows=cashflows,
        preview=preview,
        sync_connections=sync_conns,
        partial_dataset_warning=warn,
    )


def sync_connections_coverage(session: Session, *, scope: DashboardScope, as_of: dt.date) -> list[dict]:
    tq = session.query(TaxpayerEntity)
    if scope == "trust":
        tq = tq.filter(TaxpayerEntity.type == "TRUST")
    elif scope == "personal":
        tq = tq.filter(TaxpayerEntity.type == "PERSONAL")
    tp_ids = [t.id for t in tq.all()]
    if not tp_ids:
        return []

    conns = (
        session.query(ExternalConnection)
        .filter(ExternalConnection.taxpayer_entity_id.in_(tp_ids))
        .filter(ExternalConnection.status == "ACTIVE")
        .order_by(ExternalConnection.id.desc())
        .all()
    )
    if not conns:
        return []

    # Prefer IB_FLEX_WEB per taxpayer when present (avoid mixing offline+web in coverage tiles).
    preferred_ids = preferred_active_connection_ids_for_taxpayers(session, taxpayer_ids=tp_ids)
    if preferred_ids:
        conns = [c for c in conns if int(c.id) in preferred_ids]
        if not conns:
            return []

    year_start = dt.date(as_of.year, 1, 1)
    conn_ids = [c.id for c in conns]

    closed_lot_counts = {
        cid: int(cnt)
        for cid, cnt in (
            session.query(BrokerLotClosure.connection_id, func.count(BrokerLotClosure.id))
            .filter(BrokerLotClosure.connection_id.in_(conn_ids), BrokerLotClosure.trade_date >= year_start, BrokerLotClosure.trade_date <= as_of)
            .group_by(BrokerLotClosure.connection_id)
            .all()
        )
    }
    wash_counts_rows = (
        session.query(
            BrokerWashSaleEvent.connection_id,
            func.count(BrokerWashSaleEvent.id).label("wash_cnt"),
            func.sum(case((BrokerWashSaleEvent.linked_closure_id.is_not(None), 1), else_=0)).label("linked_cnt"),
        )
        .filter(BrokerWashSaleEvent.connection_id.in_(conn_ids), BrokerWashSaleEvent.trade_date >= year_start, BrokerWashSaleEvent.trade_date <= as_of)
        .group_by(BrokerWashSaleEvent.connection_id)
        .all()
    )
    wash_counts = {int(r[0]): int(r[1] or 0) for r in wash_counts_rows}
    wash_linked = {int(r[0]): int(r[2] or 0) for r in wash_counts_rows}

    # Preload recent runs and pick latest per connection.
    runs = (
        session.query(SyncRun)
        .filter(SyncRun.connection_id.in_(conn_ids))
        .order_by(SyncRun.started_at.desc())
        .limit(300)
        .all()
    )
    latest_by = {}
    for r in runs:
        if r.connection_id not in latest_by:
            latest_by[r.connection_id] = r

    out: list[dict] = []
    for c in conns:
        r = latest_by.get(c.id)
        meta = c.metadata_json or {}
        is_fixtures = bool(meta.get("fixture_dir") or meta.get("fixture_accounts") or meta.get("fixture_transactions_pages"))
        source_label = None
        if is_fixtures:
            source_label = "fixtures/test"
        elif (c.provider or "").upper() == "IB" and (c.connector or "").upper() == "IB_FLEX_OFFLINE":
            source_label = "IB Flex/local files"
        ytd_closed = int(closed_lot_counts.get(c.id, 0))
        ytd_wash = int(wash_counts.get(c.id, 0))
        ytd_linked = int(wash_linked.get(c.id, 0))
        linked_pct = int(round((ytd_linked / ytd_wash) * 100)) if ytd_wash else None
        out.append(
            {
                "id": c.id,
                "name": c.name,
                "provider": c.provider,
                "broker": c.broker,
                "connector": c.connector,
                "source_label": source_label,
                "coverage_status": c.coverage_status,
                "last_successful_sync_at": c.last_successful_sync_at.isoformat() if c.last_successful_sync_at else None,
                "txn_earliest_available": c.txn_earliest_available.isoformat() if c.txn_earliest_available else None,
                "last_successful_txn_end": c.last_successful_txn_end.isoformat() if c.last_successful_txn_end else None,
                "holdings_last_asof": c.holdings_last_asof.isoformat() if c.holdings_last_asof else None,
                "last_full_sync_at": c.last_full_sync_at.isoformat() if c.last_full_sync_at else None,
                "last_run_status": r.status if r else None,
                "last_run_started_at": r.started_at.isoformat() if r else None,
                "last_run_coverage": r.coverage_json if r else None,
                "broker_closed_lot_ytd_count": ytd_closed,
                "broker_wash_ytd_count": ytd_wash,
                "broker_wash_linked_ytd_pct": linked_pct,
            }
        )
    return out
