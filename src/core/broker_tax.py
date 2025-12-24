from __future__ import annotations

import csv
import datetime as dt
import io
from dataclasses import dataclass
from typing import Any, Iterable, Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from src.core.dashboard_service import DashboardScope, parse_scope
from src.core.connection_preference import preferred_active_connection_ids_for_scope
from src.db.models import (
    Account,
    BrokerLotClosure,
    BrokerWashSaleEvent,
    ExternalAccountMap,
    ExternalConnection,
    TaxAssumptionsSet,
    TaxpayerEntity,
)


def _parse_ib_date(value: str | None) -> dt.date | None:
    if not value:
        return None
    s = str(value).strip()
    if not s:
        return None
    if ";" in s:
        s = s.split(";", 1)[0].strip()
    if len(s) >= 8 and s[:8].isdigit():
        try:
            return dt.datetime.strptime(s[:8], "%Y%m%d").date()
        except Exception:
            return None
    try:
        return dt.date.fromisoformat(s[:10])
    except Exception:
        return None


def _term_from_open_date(trade_date: dt.date, open_date_raw: str | None) -> str:
    od = _parse_ib_date(open_date_raw)
    if od is None:
        return "UNKNOWN"
    return "LT" if (trade_date - od).days >= 365 else "ST"


def _float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _proceeds(basis: float | None, realized: float | None) -> float | None:
    if basis is None or realized is None:
        return None
    return float(basis) + float(realized)


def _connection_ids_for_scope(session: Session, scope: DashboardScope) -> list[int]:
    # Prefer IB_FLEX_WEB per taxpayer when present.
    ids = preferred_active_connection_ids_for_scope(session, scope=str(scope))
    return sorted(ids)


def _account_ids_for_scope(session: Session, scope: DashboardScope) -> list[int]:
    q = session.query(Account).join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
    if scope == "trust":
        q = q.filter(TaxpayerEntity.type == "TRUST")
    elif scope == "personal":
        q = q.filter(TaxpayerEntity.type == "PERSONAL")
    return [a.id for a in q.all()]


@dataclass(frozen=True)
class BrokerRealizedRow:
    trade_date: dt.date
    provider_account_id: str
    account_name: str | None
    symbol: str
    quantity_closed: float | None
    open_date_raw: str | None
    proceeds: float | None
    basis: float | None
    realized: float | None
    term: str
    closure_id: int
    ib_trade_id: str | None
    ib_transaction_id: str | None


@dataclass(frozen=True)
class BrokerRealizedSummary:
    proceeds: float
    basis: float
    realized: float
    st: float
    lt: float
    unknown: float
    rows_count: int
    missing_proceeds_count: int


def broker_realized_gains(
    session: Session,
    *,
    scope: str | DashboardScope,
    year: int,
    account_id: int | None = None,
) -> tuple[BrokerRealizedSummary, list[tuple[str, str, float | None, float | None, float | None]], list[BrokerRealizedRow], dict[str, Any]]:
    """
    Broker-based realized gains, preferred source: BrokerLotClosure (CLOSED_LOT).
    Returns:
      - summary totals
      - by_symbol rows: (symbol, term, proceeds, basis, realized)
      - detail rows
      - coverage dict
    """
    sc = parse_scope(scope if isinstance(scope, str) else scope)
    start = dt.date(int(year), 1, 1)
    end = dt.date(int(year), 12, 31)

    conn_ids = _connection_ids_for_scope(session, sc)
    if not conn_ids:
        empty = BrokerRealizedSummary(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, 0)
        return empty, [], [], {"closed_lot_rows_count": 0}

    # Map provider_account_id -> internal account for display/filtering.
    maps = session.query(ExternalAccountMap).filter(ExternalAccountMap.connection_id.in_(conn_ids)).all()
    map_key_to_acct_id = {(m.connection_id, m.provider_account_id): m.account_id for m in maps}
    acct_by_id = {a.id: a for a in session.query(Account).all()}

    q = session.query(BrokerLotClosure).filter(
        BrokerLotClosure.connection_id.in_(conn_ids),
        BrokerLotClosure.trade_date >= start,
        BrokerLotClosure.trade_date <= end,
    )
    closures = q.all()

    # Filter by internal account_id if provided.
    if account_id is not None:
        filtered: list[BrokerLotClosure] = []
        for r in closures:
            aid = map_key_to_acct_id.get((r.connection_id, r.provider_account_id))
            if aid == account_id:
                filtered.append(r)
        closures = filtered

    detail: list[BrokerRealizedRow] = []
    totals = {"proceeds": 0.0, "basis": 0.0, "realized": 0.0, "st": 0.0, "lt": 0.0, "unknown": 0.0}
    missing_proceeds = 0

    for r in closures:
        basis = _float(r.cost_basis)
        realized = _float(r.realized_pl_fifo)
        proceeds = _float(r.proceeds_derived)
        if proceeds is None:
            proceeds = _proceeds(basis, realized)
        if proceeds is None:
            missing_proceeds += 1

        term = _term_from_open_date(r.trade_date, r.open_datetime_raw)
        if proceeds is not None:
            totals["proceeds"] += float(proceeds)
        if basis is not None:
            totals["basis"] += float(basis)
        if realized is not None:
            totals["realized"] += float(realized)
            if term == "LT":
                totals["lt"] += float(realized)
            elif term == "ST":
                totals["st"] += float(realized)
            else:
                totals["unknown"] += float(realized)

        aid = map_key_to_acct_id.get((r.connection_id, r.provider_account_id))
        acct_name = acct_by_id.get(aid).name if aid and aid in acct_by_id else None
        detail.append(
            BrokerRealizedRow(
                trade_date=r.trade_date,
                provider_account_id=r.provider_account_id,
                account_name=acct_name,
                symbol=r.symbol,
                quantity_closed=_float(r.quantity_closed),
                open_date_raw=r.open_datetime_raw,
                proceeds=proceeds,
                basis=basis,
                realized=realized,
                term=term,
                closure_id=r.id,
                ib_trade_id=r.ib_trade_id,
                ib_transaction_id=r.ib_transaction_id,
            )
        )

    by_symbol: dict[tuple[str, str], dict[str, float]] = {}
    for d in detail:
        key = (d.symbol, d.term)
        a = by_symbol.setdefault(key, {"proceeds": 0.0, "basis": 0.0, "realized": 0.0, "missing_proceeds": 0.0})
        if d.proceeds is not None:
            a["proceeds"] += float(d.proceeds)
        else:
            a["missing_proceeds"] += 1.0
        if d.basis is not None:
            a["basis"] += float(d.basis)
        if d.realized is not None:
            a["realized"] += float(d.realized)

    by_symbol_rows = [(sym, term, a["proceeds"], a["basis"], a["realized"]) for (sym, term), a in sorted(by_symbol.items())]

    summary = BrokerRealizedSummary(
        proceeds=float(totals["proceeds"]),
        basis=float(totals["basis"]),
        realized=float(totals["realized"]),
        st=float(totals["st"]),
        lt=float(totals["lt"]),
        unknown=float(totals["unknown"]),
        rows_count=len(detail),
        missing_proceeds_count=int(missing_proceeds),
    )
    coverage = {
        "closed_lot_rows_count": len(detail),
        "missing_proceeds_count": int(missing_proceeds),
    }
    return summary, by_symbol_rows, sorted(detail, key=lambda r: (r.trade_date, r.symbol, r.closure_id)), coverage


def link_broker_wash_sales(
    session: Session,
    *,
    connection_id: int,
    start_date: dt.date,
    end_date: dt.date,
) -> dict[str, int]:
    """
    Best-effort linking of broker WASH_SALE rows to broker CLOSED_LOT rows.
    Writes back to BrokerWashSaleEvent: linked_closure_id, basis_effective, proceeds_derived, disallowed_loss.
    """
    washes = (
        session.query(BrokerWashSaleEvent)
        .filter(
            BrokerWashSaleEvent.connection_id == connection_id,
            BrokerWashSaleEvent.trade_date >= start_date,
            BrokerWashSaleEvent.trade_date <= end_date,
        )
        .all()
    )
    if not washes:
        return {"wash_rows": 0, "linked": 0, "updated": 0, "with_basis": 0, "with_proceeds": 0, "with_disallowed": 0}

    closures = (
        session.query(BrokerLotClosure)
        .filter(
            BrokerLotClosure.connection_id == connection_id,
            BrokerLotClosure.trade_date >= start_date,
            BrokerLotClosure.trade_date <= end_date,
        )
        .all()
    )

    # Index by conservative match key.
    idx: dict[tuple[str, str, dt.date, int], list[BrokerLotClosure]] = {}
    for c in closures:
        qty = _float(c.quantity_closed) or 0.0
        k = (c.provider_account_id, c.symbol, c.trade_date, int(round(qty * 1_000_000)))
        idx.setdefault(k, []).append(c)

    linked = 0
    updated = 0
    with_basis = 0
    with_proceeds = 0
    with_disallowed = 0

    def _num_equal(a: Any, b: Any, tol: float = 0.005) -> bool:
        fa = _float(a)
        fb = _float(b)
        if fa is None and fb is None:
            return True
        if fa is None or fb is None:
            return False
        return abs(float(fa) - float(fb)) <= tol

    for w in washes:
        qty = _float(w.quantity) or 0.0
        k = (w.provider_account_id, w.symbol, w.trade_date, int(round(abs(qty) * 1_000_000)))
        candidates = idx.get(k) or []

        chosen: BrokerLotClosure | None = None
        confidence = 0
        if candidates:
            if w.realized_pl_fifo is not None:
                target = float(w.realized_pl_fifo)
                chosen = min(candidates, key=lambda c: abs(float(c.realized_pl_fifo or 0.0) - target))
            else:
                chosen = candidates[0]
            confidence = 100 if len(candidates) == 1 else 70

        # Backfill closure proceeds if missing.
        if chosen is not None and chosen.proceeds_derived is None:
            p = _proceeds(_float(chosen.cost_basis), _float(chosen.realized_pl_fifo))
            if p is not None:
                chosen.proceeds_derived = p

        basis_effective = _float(w.cost_basis)
        realized_effective = _float(getattr(w, "realized_pl_effective", None)) or _float(w.realized_pl_fifo)
        proceeds = _float(getattr(w, "proceeds_effective", None)) or _float(w.proceeds_derived)

        if chosen is not None:
            if w.linked_closure_id is None:
                w.linked_closure_id = chosen.id
            if basis_effective is None:
                basis_effective = _float(chosen.cost_basis)
            if proceeds is None:
                proceeds = _float(chosen.proceeds_derived) or _proceeds(_float(chosen.cost_basis), _float(chosen.realized_pl_fifo))
            if realized_effective is None:
                realized_effective = _float(chosen.realized_pl_fifo)

        if proceeds is None:
            proceeds = _proceeds(basis_effective, realized_effective)

        disallowed = None
        if realized_effective is not None and float(realized_effective) < 0:
            disallowed = abs(float(realized_effective))

        changed = False
        if chosen is not None and w.linked_closure_id == chosen.id:
            linked += 1
        if getattr(w, "link_confidence", None) != confidence:
            w.link_confidence = confidence
            changed = True
        if basis_effective is not None and not _num_equal(w.basis_effective, basis_effective):
            w.basis_effective = basis_effective
            changed = True
        if realized_effective is not None and hasattr(w, "realized_pl_effective") and not _num_equal(getattr(w, "realized_pl_effective", None), realized_effective):
            w.realized_pl_effective = realized_effective
            changed = True
        if proceeds is not None and hasattr(w, "proceeds_effective") and not _num_equal(getattr(w, "proceeds_effective", None), proceeds):
            w.proceeds_effective = proceeds
            changed = True
        # Back-compat: keep old column populated too.
        if proceeds is not None and not _num_equal(w.proceeds_derived, proceeds):
            w.proceeds_derived = proceeds
            changed = True
        if disallowed is not None and not _num_equal(w.disallowed_loss, disallowed):
            w.disallowed_loss = disallowed
            changed = True

        if hasattr(w, "reason_notes"):
            rn = dict(getattr(w, "reason_notes") or {})
            rn["link_rule"] = "acct+symbol+date+qty"
            rn["candidates"] = len(candidates)
            rn["confidence"] = confidence
            if chosen is not None:
                rn["chosen_closure_id"] = chosen.id
            w.reason_notes = rn

        if changed:
            updated += 1

        if w.basis_effective is not None:
            with_basis += 1
        if getattr(w, "proceeds_effective", None) is not None or w.proceeds_derived is not None:
            with_proceeds += 1
        if w.disallowed_loss is not None:
            with_disallowed += 1

    return {
        "wash_rows": len(washes),
        "linked": int(linked),
        "updated": int(updated),
        "with_basis": int(with_basis),
        "with_proceeds": int(with_proceeds),
        "with_disallowed": int(with_disallowed),
    }


@dataclass(frozen=True)
class TaxRateProfile:
    st_rate: float
    lt_rate: float
    niit_enabled: bool
    niit_rate: float


def load_tax_rate_profiles(session: Session) -> dict[str, TaxRateProfile]:
    """
    Profiles keyed by taxpayer type: TRUST, PERSONAL.
    Falls back to global TaxAssumptions.
    """
    row = session.query(TaxAssumptionsSet).filter(TaxAssumptionsSet.name == "Default").one_or_none()
    data = (row.json_definition or {}) if row else {}

    ordinary = float(data.get("ordinary_rate") or 0.37)
    ltcg = float(data.get("ltcg_rate") or 0.20)
    niit_enabled = bool(data.get("niit_enabled") or False)
    niit_rate = float(data.get("niit_rate") or 0.038)

    rates = data.get("tax_rates") if isinstance(data.get("tax_rates"), dict) else {}

    def _profile(key: str) -> TaxRateProfile:
        cfg = rates.get(key.lower()) if isinstance(rates, dict) else None
        if not isinstance(cfg, dict):
            return TaxRateProfile(st_rate=ordinary, lt_rate=ltcg, niit_enabled=niit_enabled, niit_rate=niit_rate)
        st = float(cfg.get("st_rate") or ordinary)
        lt = float(cfg.get("lt_rate") or ltcg)
        n_on = bool(cfg.get("niit_enabled")) if "niit_enabled" in cfg else niit_enabled
        n = float(cfg.get("niit_rate") or niit_rate)
        return TaxRateProfile(st_rate=st, lt_rate=lt, niit_enabled=n_on, niit_rate=n)

    return {
        "TRUST": _profile("trust"),
        "PERSONAL": _profile("personal"),
    }


def broker_tax_summary(
    session: Session,
    *,
    scope: str | DashboardScope,
    year: int,
) -> dict[str, Any]:
    """
    PRO FORMA / PLANNING tax summary using broker CLOSED_LOT + broker WASH_SALE (disallowed losses).
    Conservative convention: UNKNOWN term treated as ST for tax-rate purposes.
    """
    sc = parse_scope(scope if isinstance(scope, str) else scope)
    year = int(year)
    start = dt.date(year, 1, 1)
    end = dt.date(year, 12, 31)

    conn_ids = _connection_ids_for_scope(session, sc)
    if not conn_ids:
        return {"scope": sc, "year": year, "rows": [], "totals": {}}

    # Join closures to taxpayer entity for per-taxpayer rollups.
    closures = (
        session.query(BrokerLotClosure, ExternalConnection, TaxpayerEntity)
        .join(ExternalConnection, ExternalConnection.id == BrokerLotClosure.connection_id)
        .join(TaxpayerEntity, TaxpayerEntity.id == ExternalConnection.taxpayer_entity_id)
        .filter(
            BrokerLotClosure.connection_id.in_(conn_ids),
            BrokerLotClosure.trade_date >= start,
            BrokerLotClosure.trade_date <= end,
        )
        .all()
    )

    washes = (
        session.query(BrokerWashSaleEvent, ExternalConnection, TaxpayerEntity)
        .join(ExternalConnection, ExternalConnection.id == BrokerWashSaleEvent.connection_id)
        .join(TaxpayerEntity, TaxpayerEntity.id == ExternalConnection.taxpayer_entity_id)
        .filter(
            BrokerWashSaleEvent.connection_id.in_(conn_ids),
            BrokerWashSaleEvent.trade_date >= start,
            BrokerWashSaleEvent.trade_date <= end,
        )
        .all()
    )

    profiles = load_tax_rate_profiles(session)

    by_taxpayer: dict[int, dict[str, Any]] = {}
    for c, _conn, tp in closures:
        row = by_taxpayer.setdefault(
            tp.id,
            {
                "taxpayer_id": tp.id,
                "taxpayer": tp.name,
                "taxpayer_type": tp.type,
                "st_realized": 0.0,
                "lt_realized": 0.0,
                "unknown_realized": 0.0,
                "closed_lot_rows": 0,
                "wash_rows": 0,
                "wash_linked": 0,
                "wash_disallowed": 0.0,
                "wash_disallowed_unknown": 0.0,
            },
        )
        realized = _float(c.realized_pl_fifo) or 0.0
        term = _term_from_open_date(c.trade_date, c.open_datetime_raw)
        if term == "LT":
            row["lt_realized"] += float(realized)
        elif term == "ST":
            row["st_realized"] += float(realized)
        else:
            row["unknown_realized"] += float(realized)
        row["closed_lot_rows"] += 1

    for w, _conn, tp in washes:
        row = by_taxpayer.setdefault(
            tp.id,
            {
                "taxpayer_id": tp.id,
                "taxpayer": tp.name,
                "taxpayer_type": tp.type,
                "st_realized": 0.0,
                "lt_realized": 0.0,
                "unknown_realized": 0.0,
                "closed_lot_rows": 0,
                "wash_rows": 0,
                "wash_linked": 0,
                "wash_disallowed": 0.0,
                "wash_disallowed_unknown": 0.0,
            },
        )
        row["wash_rows"] += 1
        if w.linked_closure_id is not None:
            row["wash_linked"] += 1
        dloss = _float(w.disallowed_loss)
        if dloss is None:
            realized = _float(w.realized_pl_fifo)
            if realized is None and w.linked_closure is not None:
                realized = _float(w.linked_closure.realized_pl_fifo)
            if realized is not None and realized < 0:
                dloss = abs(float(realized))
        if dloss is None:
            continue

        # Try to classify by linked closure open date; else UNKNOWN.
        term = "UNKNOWN"
        if w.linked_closure is not None:
            term = _term_from_open_date(w.trade_date, w.linked_closure.open_datetime_raw)
        if term == "LT":
            row["wash_disallowed"] += float(dloss)
        elif term == "ST":
            row["wash_disallowed"] += float(dloss)
        else:
            row["wash_disallowed_unknown"] += float(dloss)

    rows_out: list[dict[str, Any]] = []
    totals = {
        "st_realized": 0.0,
        "lt_realized": 0.0,
        "unknown_realized": 0.0,
        "disallowed_loss": 0.0,
        "net_taxable": 0.0,
        "additional_tax_due": 0.0,
    }

    for _tp_id, r in sorted(by_taxpayer.items(), key=lambda kv: str(kv[1].get("taxpayer"))):
        tp_type = str(r.get("taxpayer_type") or "PERSONAL").upper()
        prof = profiles.get(tp_type) or profiles["PERSONAL"]

        st_realized = float(r.get("st_realized") or 0.0)
        lt_realized = float(r.get("lt_realized") or 0.0)
        unknown_realized = float(r.get("unknown_realized") or 0.0)
        realized_total = st_realized + lt_realized + unknown_realized

        disallowed = float(r.get("wash_disallowed") or 0.0) + float(r.get("wash_disallowed_unknown") or 0.0)

        # Conservative: disallowed losses are losses you cannot take now, so taxable gains are higher by that amount.
        net_taxable = realized_total + disallowed

        st_net = st_realized + unknown_realized + disallowed  # UNKNOWN treated as ST
        lt_net = lt_realized

        niit = prof.niit_rate if prof.niit_enabled else 0.0
        st_tax = max(0.0, st_net) * (prof.st_rate + niit)
        lt_tax = max(0.0, lt_net) * (prof.lt_rate + niit)
        additional_tax_due = st_tax + lt_tax

        carryforward_note = None
        if net_taxable < 0:
            carryforward_note = "Net capital loss (planning). Carryforward rules not modeled; tax due shown as 0."
            additional_tax_due = 0.0

        out = {
            "taxpayer": r.get("taxpayer"),
            "taxpayer_type": tp_type,
            "st_realized": st_realized,
            "lt_realized": lt_realized,
            "unknown_realized": unknown_realized,
            "realized_total": realized_total,
            "disallowed_loss": disallowed,
            "net_taxable": net_taxable,
            "st_tax_due": st_tax,
            "lt_tax_due": lt_tax,
            "additional_tax_due": additional_tax_due,
            "rates": {
                "st_rate": prof.st_rate,
                "lt_rate": prof.lt_rate,
                "niit_enabled": prof.niit_enabled,
                "niit_rate": prof.niit_rate,
            },
            "coverage": {
                "closed_lot_rows_count": int(r.get("closed_lot_rows") or 0),
                "wash_rows_count": int(r.get("wash_rows") or 0),
                "wash_linked_rows_count": int(r.get("wash_linked") or 0),
            },
            "note": carryforward_note,
        }
        rows_out.append(out)

        totals["st_realized"] += st_realized
        totals["lt_realized"] += lt_realized
        totals["unknown_realized"] += unknown_realized
        totals["disallowed_loss"] += disallowed
        totals["net_taxable"] += net_taxable
        totals["additional_tax_due"] += additional_tax_due

    totals["realized_total"] = totals["st_realized"] + totals["lt_realized"] + totals["unknown_realized"]
    return {
        "scope": sc,
        "year": year,
        "rows": rows_out,
        "totals": totals,
        "disclaimer": "PRO FORMA / PLANNING ONLY. Not tax filing advice. Uses broker CLOSED_LOT + broker WASH_SALE rows when present.",
    }


def rows_to_csv(headers: list[str], rows: Iterable[Iterable[Any]]) -> str:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(headers)
    for r in rows:
        w.writerow(list(r))
    return buf.getvalue()
