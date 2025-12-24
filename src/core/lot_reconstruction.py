from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any, Optional

from sqlalchemy import and_, func
from sqlalchemy.orm import Session

from src.db.audit import log_change
from src.db.models import (
    Account,
    CorporateActionEvent,
    LotDisposal,
    Security,
    TaxLot,
    TaxpayerEntity,
    Transaction,
    WashSaleAdjustment,
)


@dataclass(frozen=True)
class RebuildResult:
    taxpayer_id: int
    accounts_included: list[int]
    txns_scanned: int
    lots_created: int
    disposals_created: int
    wash_adjustments_created: int
    warnings: list[str]

    def as_json(self) -> dict[str, Any]:
        return {
            "taxpayer_id": self.taxpayer_id,
            "accounts_included": self.accounts_included,
            "txns_scanned": self.txns_scanned,
            "lots_created": self.lots_created,
            "disposals_created": self.disposals_created,
            "wash_adjustments_created": self.wash_adjustments_created,
            "warnings": self.warnings,
        }


def _ensure_security(session: Session, *, ticker: str) -> Security:
    t = ticker.strip().upper()
    sec = session.query(Security).filter(Security.ticker == t).one_or_none()
    if sec is not None:
        return sec
    sec = Security(ticker=t, name=t, asset_class="UNKNOWN", expense_ratio=0.0, substitute_group_id=None, metadata_json={})
    session.add(sec)
    session.flush()
    return sec


def _term(acquired: dt.date, sold: dt.date) -> str:
    # LT if >= 365 days.
    return "LT" if (sold - acquired).days >= 365 else "ST"


def _apply_split_to_open_lots(
    session: Session,
    *,
    taxpayer_id: int,
    security_id: Optional[int],
    account_id: Optional[int],
    ratio: float,
    as_of: dt.date,
    warnings: list[str],
) -> int:
    if ratio <= 0:
        warnings.append("Corporate action split ratio <= 0; skipped.")
        return 0
    q = session.query(TaxLot).filter(TaxLot.taxpayer_id == taxpayer_id, TaxLot.source == "RECONSTRUCTED")
    if security_id is not None:
        q = q.filter(TaxLot.security_id == security_id)
    if account_id is not None:
        q = q.filter(TaxLot.account_id == account_id)
    q = q.filter(TaxLot.quantity_open > 0)
    rows = q.all()
    touched = 0
    for lot in rows:
        try:
            qty = float(lot.quantity_open)
            lot.quantity_open = qty * ratio
            meta = lot.metadata_json or {}
            meta.setdefault("corporate_actions", []).append({"type": "SPLIT", "ratio": ratio, "as_of": as_of.isoformat()})
            lot.metadata_json = meta
            touched += 1
        except Exception:
            warnings.append("Failed to apply split to a lot; skipped.")
            continue
    return touched


def _apply_corporate_actions(
    session: Session,
    *,
    taxpayer_id: int,
    up_to: dt.date,
    actor: str,
    warnings: list[str],
) -> None:
    events = (
        session.query(CorporateActionEvent)
        .filter(
            CorporateActionEvent.taxpayer_id == taxpayer_id,
            CorporateActionEvent.applied == False,  # noqa: E712
            CorporateActionEvent.action_date <= up_to,
        )
        .order_by(CorporateActionEvent.action_date.asc(), CorporateActionEvent.id.asc())
        .all()
    )
    for ev in events:
        if ev.action_type not in ("SPLIT", "REVERSE_SPLIT"):
            ev.apply_notes = (ev.apply_notes or "") + " Unsupported action_type in MVP; not applied."
            ev.applied = True
            continue
        if ev.ratio is None:
            ev.apply_notes = (ev.apply_notes or "") + " Missing ratio; not applied."
            ev.applied = True
            warnings.append("Corporate action missing ratio; marked applied but no change made.")
            continue
        ratio = float(ev.ratio)
        if ev.action_type == "REVERSE_SPLIT":
            if ratio == 0:
                warnings.append("Reverse split ratio=0; skipped.")
                ev.apply_notes = "Invalid ratio"
                ev.applied = True
                continue
            ratio = 1.0 / ratio
        touched = _apply_split_to_open_lots(
            session,
            taxpayer_id=taxpayer_id,
            security_id=ev.security_id,
            account_id=ev.account_id,
            ratio=ratio,
            as_of=ev.action_date,
            warnings=warnings,
        )
        ev.applied = True
        ev.apply_notes = f"Applied {ev.action_type} ratio={float(ev.ratio)} (effective factor {ratio}) to {touched} open lot(s)."
        log_change(
            session,
            actor=actor,
            action="APPLY_CORP_ACTION",
            entity="CorporateActionEvent",
            entity_id=str(ev.id),
            old=None,
            new={"action_type": ev.action_type, "ratio": float(ev.ratio), "touched_lots": touched},
            note="Applied corporate action to reconstructed lots",
        )


def _identical_tickers(session: Session, *, ticker: str) -> set[str]:
    t = ticker.strip().upper()
    out = {t}
    sec = session.query(Security).filter(Security.ticker == t).one_or_none()
    if sec is None or sec.substitute_group_id is None:
        return out
    members = session.query(Security.ticker).filter(Security.substitute_group_id == sec.substitute_group_id).all()
    for m in members:
        if m and m[0]:
            out.add(str(m[0]).upper())
    return out


def rebuild_reconstructed_tax_lots_for_taxpayer(
    session: Session,
    *,
    taxpayer_id: int,
    actor: str,
    note: str = "",
    fifo: bool = True,
    wash_include_ira: bool = False,
) -> RebuildResult:
    """
    Deterministically reconstruct planning-grade tax lots from full transaction history.

    Defaults:
    - FIFO disposal
    - Wash sale checks within taxpayer taxable accounts (optional IRA inclusion flagged)
    """
    tp = session.query(TaxpayerEntity).filter(TaxpayerEntity.id == taxpayer_id).one()
    taxable_accounts = session.query(Account).filter(
        Account.taxpayer_entity_id == taxpayer_id,
        Account.account_type == "TAXABLE",
    ).order_by(Account.id.asc()).all()
    account_ids = [a.id for a in taxable_accounts]
    warnings: list[str] = []

    # Wipe prior planning-grade reconstruction for this taxpayer (idempotent rebuild).
    # Delete wash adj -> disposals -> lots.
    if account_ids:
        sell_txn_ids = [
            r[0]
            for r in session.query(Transaction.id)
            .filter(Transaction.account_id.in_(account_ids), Transaction.type == "SELL")
            .all()
        ]
        if sell_txn_ids:
            session.query(WashSaleAdjustment).filter(WashSaleAdjustment.loss_sale_txn_id.in_(sell_txn_ids)).delete(
                synchronize_session=False
            )
            session.query(LotDisposal).filter(LotDisposal.sell_txn_id.in_(sell_txn_ids)).delete(synchronize_session=False)

        session.query(TaxLot).filter(TaxLot.taxpayer_id == taxpayer_id, TaxLot.source == "RECONSTRUCTED").delete(
            synchronize_session=False
        )
        session.flush()

    if not account_ids:
        warnings.append("No taxable accounts for taxpayer; no lots built.")
        res = RebuildResult(
            taxpayer_id=taxpayer_id,
            accounts_included=[],
            txns_scanned=0,
            lots_created=0,
            disposals_created=0,
            wash_adjustments_created=0,
            warnings=warnings,
        )
        log_change(
            session,
            actor=actor,
            action="REBUILD_LOTS",
            entity="TaxLotRebuild",
            entity_id=str(taxpayer_id),
            old=None,
            new=res.as_json(),
            note=note or f"Rebuild reconstructed lots for {tp.name}",
        )
        session.commit()
        return res

    txns = (
        session.query(Transaction)
        .filter(
            Transaction.account_id.in_(account_ids),
            Transaction.ticker.is_not(None),
            Transaction.type.in_(["BUY", "SELL", "TRANSFER", "OTHER"]),
        )
        .order_by(Transaction.date.asc(), Transaction.id.asc())
        .all()
    )

    # Pending corporate actions applied during the rebuild in chronological order.
    corp_events = (
        session.query(CorporateActionEvent)
        .filter(CorporateActionEvent.taxpayer_id == taxpayer_id, CorporateActionEvent.applied == False)  # noqa: E712
        .order_by(CorporateActionEvent.action_date.asc(), CorporateActionEvent.id.asc())
        .all()
    )
    corp_idx = 0

    # Open lots indexed by (account_id, ticker) in FIFO order.
    open_lots: dict[tuple[int, str], list[TaxLot]] = {}
    unknown_basis_lot_id_by_key: dict[tuple[int, str], int] = {}

    lots_created = 0
    disposals_created = 0

    for tx in txns:
        # Apply corporate actions up to current txn date.
        while corp_idx < len(corp_events) and corp_events[corp_idx].action_date <= tx.date:
            ev = corp_events[corp_idx]
            corp_idx += 1
            if ev.action_type not in ("SPLIT", "REVERSE_SPLIT"):
                ev.apply_notes = (ev.apply_notes or "") + " Unsupported action_type in MVP; not applied."
                ev.applied = True
                continue
            if ev.ratio is None:
                ev.apply_notes = (ev.apply_notes or "") + " Missing ratio; not applied."
                ev.applied = True
                warnings.append("Corporate action missing ratio; marked applied but no change made.")
                continue
            ratio = float(ev.ratio)
            if ev.action_type == "REVERSE_SPLIT":
                if ratio == 0:
                    warnings.append("Reverse split ratio=0; skipped.")
                    ev.apply_notes = "Invalid ratio"
                    ev.applied = True
                    continue
                ratio = 1.0 / ratio
            touched = _apply_split_to_open_lots(
                session,
                taxpayer_id=taxpayer_id,
                security_id=ev.security_id,
                account_id=ev.account_id,
                ratio=ratio,
                as_of=ev.action_date,
                warnings=warnings,
            )
            ev.applied = True
            ev.apply_notes = f"Applied {ev.action_type} ratio={float(ev.ratio)} (effective factor {ratio}) to {touched} open lot(s)."
            log_change(
                session,
                actor=actor,
                action="APPLY_CORP_ACTION",
                entity="CorporateActionEvent",
                entity_id=str(ev.id),
                old=None,
                new={"action_type": ev.action_type, "ratio": float(ev.ratio), "touched_lots": touched},
                note="Applied corporate action to reconstructed lots",
            )

        ticker = (tx.ticker or "").strip().upper()
        if not ticker:
            continue
        sec = _ensure_security(session, ticker=ticker)
        qty = float(tx.qty) if tx.qty is not None else None
        amount = float(tx.amount)

        key = (tx.account_id, ticker)
        if key not in open_lots:
            open_lots[key] = []

        if tx.type == "BUY":
            if qty is None or qty <= 0:
                warnings.append(f"BUY txn missing qty: txn_id={tx.id}")
                continue
            basis = abs(amount)
            lot = TaxLot(
                taxpayer_id=taxpayer_id,
                account_id=tx.account_id,
                security_id=sec.id,
                acquired_date=tx.date,
                quantity_open=qty,
                basis_open=basis,
                source="RECONSTRUCTED",
                created_from_txn_id=tx.id,
                metadata_json={
                    "original_qty": qty,
                    "original_basis": basis,
                    "commission_included": True,
                    "estimated": True,
                },
            )
            session.add(lot)
            session.flush()
            open_lots[key].append(lot)
            lots_created += 1
        elif tx.type == "TRANSFER":
            # Best-effort: if qty>0 and basis known in lot_links_json, create a lot; else create basis-unknown lot.
            if qty is None or qty <= 0:
                continue
            links = tx.lot_links_json or {}
            basis_known = links.get("basis_total")
            basis = float(basis_known) if basis_known not in (None, "") else None
            if basis is None and abs(amount) > 1e-9:
                basis = abs(amount)
            lot = TaxLot(
                taxpayer_id=taxpayer_id,
                account_id=tx.account_id,
                security_id=sec.id,
                acquired_date=tx.date,
                quantity_open=qty,
                basis_open=basis,
                source="RECONSTRUCTED",
                created_from_txn_id=tx.id,
                metadata_json={
                    "original_qty": qty,
                    "original_basis": basis,
                    "basis_unknown": basis is None,
                    "estimated": True,
                    "notes": "Transfer-in lot reconstructed; verify basis.",
                },
            )
            session.add(lot)
            session.flush()
            open_lots[key].append(lot)
            lots_created += 1
        elif tx.type == "SELL":
            if qty is None or qty <= 0:
                warnings.append(f"SELL txn missing qty: txn_id={tx.id}")
                continue
            proceeds = abs(amount)
            remaining = qty
            # FIFO: lots in acquisition order.
            lots = open_lots[key]
            lots.sort(key=lambda l: (l.acquired_date, l.id))
            while remaining > 1e-9:
                lot = next((l for l in lots if float(l.quantity_open) > 1e-9), None)
                if lot is None:
                    # Not enough lots: basis unknown for remainder.
                    warnings.append(f"Insufficient lots for SELL txn_id={tx.id} ticker={ticker}; basis unknown for {remaining}.")
                    ub_key = (tx.account_id, ticker)
                    ub_id = unknown_basis_lot_id_by_key.get(ub_key)
                    if ub_id is None:
                        ub = TaxLot(
                            taxpayer_id=taxpayer_id,
                            account_id=tx.account_id,
                            security_id=sec.id,
                            acquired_date=tx.date,
                            quantity_open=0.0,
                            basis_open=None,
                            source="RECONSTRUCTED",
                            created_from_txn_id=None,
                            metadata_json={"basis_unknown": True, "estimated": True, "notes": "Synthetic lot for missing history / short position."},
                        )
                        session.add(ub)
                        session.flush()
                        unknown_basis_lot_id_by_key[ub_key] = ub.id
                        ub_id = ub.id
                    d = LotDisposal(
                        sell_txn_id=tx.id,
                        tax_lot_id=int(ub_id),
                        quantity_sold=remaining,
                        proceeds_allocated=proceeds * (remaining / qty),
                        basis_allocated=None,
                        realized_gain=None,
                        term="—",
                        as_of_date=tx.date,
                        metadata_json={"basis_unknown": True, "estimated": True},
                    )
                    session.add(d)
                    disposals_created += 1
                    break
                take = min(remaining, float(lot.quantity_open))
                portion_proceeds = proceeds * (take / qty)
                basis_alloc = None
                gain = None
                term = "—"
                if lot.basis_open is not None and float(lot.quantity_open) > 1e-9:
                    bps = float(lot.basis_open) / float(lot.quantity_open)
                    basis_alloc = bps * take
                    gain = portion_proceeds - basis_alloc
                    term = _term(lot.acquired_date, tx.date)
                    lot.basis_open = float(lot.basis_open) - basis_alloc
                else:
                    warnings.append(f"Basis unknown lot used for SELL txn_id={tx.id} ticker={ticker}.")
                lot.quantity_open = float(lot.quantity_open) - take
                d = LotDisposal(
                    sell_txn_id=tx.id,
                    tax_lot_id=lot.id,
                    quantity_sold=take,
                    proceeds_allocated=portion_proceeds,
                    basis_allocated=basis_alloc,
                    realized_gain=gain,
                    term=term,
                    as_of_date=tx.date,
                    metadata_json={"estimated": True},
                )
                session.add(d)
                disposals_created += 1
                remaining -= take
        else:
            # OTHER: no lot changes in MVP. Corporate actions should be entered via CorporateActionEvent.
            continue

    session.flush()

    # Wash sales: apply for loss sales based on executed BUYs around the sale date.
    wash_adjustments_created = _apply_wash_sales_from_disposals(
        session,
        taxpayer_id=taxpayer_id,
        taxable_account_ids=account_ids,
        actor=actor,
        include_ira=wash_include_ira,
        warnings=warnings,
    )
    session.flush()

    res = RebuildResult(
        taxpayer_id=taxpayer_id,
        accounts_included=account_ids,
        txns_scanned=len(txns),
        lots_created=lots_created,
        disposals_created=disposals_created,
        wash_adjustments_created=wash_adjustments_created,
        warnings=warnings,
    )
    log_change(
        session,
        actor=actor,
        action="REBUILD_LOTS",
        entity="TaxLotRebuild",
        entity_id=str(taxpayer_id),
        old=None,
        new=res.as_json(),
        note=note or f"Rebuild reconstructed lots for {tp.name}",
    )
    session.commit()
    return res


def _apply_wash_sales_from_disposals(
    session: Session,
    *,
    taxpayer_id: int,
    taxable_account_ids: list[int],
    actor: str,
    include_ira: bool,
    warnings: list[str],
) -> int:
    """
    Apply wash sale adjustments for loss sales and adjust replacement lots basis when possible.
    Conservative + planning-grade:
      - uses executed BUY txns only (no future trades)
      - allocates to replacement buys chronologically
      - IRA replacements are flagged (no basis adjustment) unless future authoritative rules added
    """
    created = 0
    # Load taxable and optionally IRA accounts in the same taxpayer.
    acct_q = session.query(Account).filter(Account.taxpayer_entity_id == taxpayer_id)
    if include_ira:
        scope_account_ids = [a.id for a in acct_q.all()]
    else:
        scope_account_ids = taxable_account_ids

    # Sale totals per txn within taxpayer taxable accounts.
    sale_ids = [
        r[0]
        for r in session.query(LotDisposal.sell_txn_id)
        .join(Transaction, Transaction.id == LotDisposal.sell_txn_id)
        .filter(Transaction.account_id.in_(taxable_account_ids))
        .distinct()
        .all()
    ]
    if not sale_ids:
        return 0
    sales = session.query(Transaction).filter(Transaction.id.in_(sale_ids)).all()
    sale_by_id = {s.id: s for s in sales}

    # Sum realized gain per sale txn.
    sums = (
        session.query(LotDisposal.sell_txn_id, func.sum(LotDisposal.realized_gain))
        .filter(LotDisposal.sell_txn_id.in_(sale_ids))
        .group_by(LotDisposal.sell_txn_id)
        .all()
    )
    for sell_txn_id, gain_sum in sums:
        sale = sale_by_id.get(int(sell_txn_id))
        if sale is None or sale.ticker is None or sale.type != "SELL":
            continue
        if gain_sum is None:
            continue
        total_gain = float(gain_sum)
        if total_gain >= -0.01:
            continue
        qty_sold = float(sale.qty or 0.0)
        if qty_sold <= 0:
            continue
        loss_abs = -total_gain
        window_start = sale.date - dt.timedelta(days=30)
        window_end = sale.date + dt.timedelta(days=30)
        tickers = _identical_tickers(session, ticker=str(sale.ticker))

        # Replacement buys within window, chronological.
        buys = (
            session.query(Transaction, Account)
            .join(Account, Account.id == Transaction.account_id)
            .filter(
                Transaction.account_id.in_(scope_account_ids),
                Transaction.type == "BUY",
                Transaction.ticker.in_(sorted(tickers)),
                Transaction.date >= window_start,
                Transaction.date <= window_end,
            )
            .order_by(Transaction.date.asc(), Transaction.id.asc())
            .all()
        )
        if not buys:
            continue

        # Map buy txn -> created lot (if exists).
        buy_ids = [t.id for t, _a in buys]
        lots = session.query(TaxLot).filter(TaxLot.created_from_txn_id.in_(buy_ids)).all()
        lot_by_buy_id = {int(l.created_from_txn_id or 0): l for l in lots}

        remaining_shares = qty_sold
        for buy_txn, acct in buys:
            bqty = float(buy_txn.qty or 0.0)
            if bqty <= 0 or remaining_shares <= 1e-9:
                continue
            take = min(remaining_shares, bqty)
            deferred = loss_abs * (take / qty_sold)
            status = "APPLIED"
            basis_increase = deferred
            repl_lot = lot_by_buy_id.get(buy_txn.id)
            notes: dict[str, Any] = {"estimated": True, "replacement_shares": take}

            if acct.account_type == "IRA":
                status = "FLAGGED"
                basis_increase = 0.0
                notes["ira_replacement"] = True
                notes["note"] = "Replacement buy in IRA; wash loss may be permanently disallowed (not modeled)."
            if repl_lot is None:
                status = "FLAGGED"
                basis_increase = 0.0
                notes["missing_replacement_lot"] = True

            if status == "APPLIED" and repl_lot is not None:
                repl_lot.basis_open = float(repl_lot.basis_open or 0.0) + deferred
                meta = repl_lot.metadata_json or {}
                meta.setdefault("wash_adjustments", []).append(
                    {"loss_sale_txn_id": int(sell_txn_id), "deferred_loss": deferred, "applied_at": dt.date.today().isoformat()}
                )
                repl_lot.metadata_json = meta

            session.add(
                WashSaleAdjustment(
                    loss_sale_txn_id=int(sell_txn_id),
                    replacement_buy_txn_id=buy_txn.id,
                    replacement_lot_id=repl_lot.id if repl_lot is not None else None,
                    deferred_loss=deferred,
                    basis_increase=basis_increase,
                    window_start=window_start,
                    window_end=window_end,
                    status=status,
                    notes_json=notes,
                )
            )
            created += 1
            remaining_shares -= take

        if remaining_shares <= qty_sold - 1e-9 and remaining_shares > 1e-9:
            warnings.append(f"Wash sale: not enough replacement shares to defer full loss for sale txn_id={sell_txn_id}.")

    if created:
        log_change(
            session,
            actor=actor,
            action="WASH_APPLY",
            entity="TaxLotRebuild",
            entity_id=str(taxpayer_id),
            old=None,
            new={"wash_adjustments_created": created},
            note="Applied wash sale adjustments (planning-grade, reconstructed)",
        )
    return created
