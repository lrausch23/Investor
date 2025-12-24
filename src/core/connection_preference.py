from __future__ import annotations

from collections import defaultdict
from typing import Iterable

from sqlalchemy.orm import Session

from src.db.models import ExternalConnection, TaxpayerEntity


def preferred_active_connection_ids_for_taxpayers(session: Session, *, taxpayer_ids: Iterable[int]) -> set[int]:
    """
    Select ACTIVE connections, preferring IB_FLEX_WEB per taxpayer when present.

    Rationale: it's common for the same brokerage account to be imported via multiple connectors
    (e.g. offline files + web service). When both are active, holdings/tax metrics can double-count.
    For MVP correctness, we prefer IB_FLEX_WEB as the authoritative feed when available.
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
        # Always include non-IB providers (e.g. CHASE IRA) so household views remain complete.
        non_ib = [c for c in cs if (c.provider or "").upper() != "IB"]

        # For IB, prefer IB_FLEX_WEB when present (avoids double-counting from offline + web).
        ib = [c for c in cs if (c.provider or "").upper() == "IB"]
        ib_web = [c for c in ib if (c.connector or "").upper() == "IB_FLEX_WEB"]
        ib_selected = ib_web if ib_web else ib

        selected.extend(non_ib)
        selected.extend(ib_selected)

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
