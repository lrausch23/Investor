from __future__ import annotations

from collections import defaultdict
from typing import Iterable

from sqlalchemy.orm import Session

from src.db.models import ExternalConnection, TaxpayerEntity


def preferred_active_connection_ids_for_taxpayers(session: Session, *, taxpayer_ids: Iterable[int]) -> set[int]:
    """
    Select ACTIVE connections, preferring live connectors over offline duplicates per taxpayer.

    Rationale: it's common for the same brokerage account to be imported via multiple connectors
    (e.g. offline files + web service). When both are active, holdings/tax metrics can double-count.
    For MVP correctness, we prefer the most authoritative live feed when available.
    """
    tp_ids = [int(x) for x in taxpayer_ids if x is not None]
    if not tp_ids:
        return set()

    conns = (
        session.query(ExternalConnection)
        .filter(ExternalConnection.taxpayer_entity_id.in_(tp_ids), ExternalConnection.status == "ACTIVE")
        .all()
    )
    by_tp: dict[int, list[ExternalConnection]] = defaultdict(list)
    for c in conns:
        by_tp[int(c.taxpayer_entity_id)].append(c)

    selected: list[ExternalConnection] = []
    for _tp_id, cs in by_tp.items():
        # Group by broker to avoid mixing provider naming (e.g., provider=YODLEE broker=CHASE).
        by_broker: dict[str, list[ExternalConnection]] = defaultdict(list)
        for c in cs:
            by_broker[(c.broker or "").upper()].append(c)

        for broker_u, bs in by_broker.items():
            if broker_u == "IB":
                ib_web = [c for c in bs if (c.connector or "").upper() == "IB_FLEX_WEB"]
                selected.extend(ib_web if ib_web else bs)
                continue
            if broker_u == "CHASE":
                # Prefer offline CSV connector by default (avoids relying on Yodlee credentials).
                chase_offline = [c for c in bs if (c.connector or "").upper() == "CHASE_OFFLINE"]
                if chase_offline:
                    selected.extend(chase_offline)
                else:
                    # If no offline connector exists, still avoid selecting CHASE_YODLEE by default.
                    selected.extend([c for c in bs if (c.connector or "").upper() != "CHASE_YODLEE"])
                continue
            # Default: include all (no preference rules yet).
            selected.extend(bs)

    return {int(c.id) for c in selected}


def preferred_active_connection_ids_for_scope(session: Session, *, scope: str) -> set[int]:
    """
    Scope is one of: household | trust | personal (same semantics as dashboard/holdings).
    """
    v = (scope or "").strip().lower()
    tq = session.query(TaxpayerEntity.id)
    if v == "trust":
        tq = tq.filter(TaxpayerEntity.type == "TRUST")
    elif v == "personal":
        tq = tq.filter(TaxpayerEntity.type == "PERSONAL")
    tp_ids = [int(r[0]) for r in tq.all()]
    return preferred_active_connection_ids_for_taxpayers(session, taxpayer_ids=tp_ids)
