from __future__ import annotations

import csv
import io
from typing import Any

from sqlalchemy.orm import Session

from src.app.utils import jsonable
from src.core.defaults import ensure_default_setup
from src.db.audit import log_change
from src.db.models import Account, CashBalance, IncomeEvent, PositionLot, Security, SubstituteGroup, Transaction
from src.importers.schemas import (
    CashBalanceRow,
    IncomeEventRow,
    LotsRow,
    SecurityRow,
    TransactionRow,
)


def _account_by_name(session: Session) -> dict[str, Account]:
    rows = session.query(Account).all()
    return {a.name: a for a in rows}


def import_csv(session: Session, *, kind: str, content: str, actor: str, note: str = "") -> dict[str, Any]:
    kind = kind.strip()
    reader = csv.DictReader(io.StringIO(content))
    rows = list(reader)
    errors: list[str] = []
    imported = 0

    acct_map = _account_by_name(session)
    if not acct_map and kind in {"lots", "cash_balances", "income_events", "transactions"}:
        # Convenience for first-run CLI imports: bootstrap default taxpayers/accounts/policy.
        import datetime as dt

        ensure_default_setup(session=session, effective_date=dt.date.today())
        session.flush()
        acct_map = _account_by_name(session)

    if kind == "lots":
        for i, r in enumerate(rows, start=2):
            try:
                row = LotsRow.model_validate(r)
            except Exception as e:
                errors.append(f"line {i}: {e}")
                continue
            acct = acct_map.get(row.account_name)
            if acct is None:
                errors.append(
                    f"line {i}: unknown account_name={row.account_name} (create it in Setup or run Setup > Create Defaults)"
                )
                continue
            lot = PositionLot(
                account_id=acct.id,
                ticker=row.ticker,
                acquisition_date=row.acquisition_date,
                qty=row.qty,
                basis_total=row.basis_total,
                adjusted_basis_total=row.adjusted_basis_total,
            )
            session.add(lot)
            session.flush()
            log_change(
                session,
                actor=actor,
                action="IMPORT_CREATE",
                entity="PositionLot",
                entity_id=str(lot.id),
                old=None,
                new=jsonable(
                    {
                        "account_id": lot.account_id,
                        "ticker": lot.ticker,
                        "acquisition_date": lot.acquisition_date.isoformat(),
                        "qty": float(lot.qty),
                        "basis_total": float(lot.basis_total),
                    }
                ),
                note=note or "CSV import lots",
            )
            imported += 1

    elif kind == "cash_balances":
        for i, r in enumerate(rows, start=2):
            try:
                row = CashBalanceRow.model_validate(r)
            except Exception as e:
                errors.append(f"line {i}: {e}")
                continue
            acct = acct_map.get(row.account_name)
            if acct is None:
                errors.append(
                    f"line {i}: unknown account_name={row.account_name} (create it in Setup or run Setup > Create Defaults)"
                )
                continue
            existing = (
                session.query(CashBalance)
                .filter(CashBalance.account_id == acct.id, CashBalance.as_of_date == row.as_of_date)
                .one_or_none()
            )
            old = jsonable({"amount": float(existing.amount)}) if existing else None
            if existing:
                existing.amount = row.amount
                entity_id = str(existing.id)
                action = "IMPORT_UPDATE"
                new = jsonable({"amount": float(existing.amount)})
            else:
                cb = CashBalance(account_id=acct.id, as_of_date=row.as_of_date, amount=row.amount)
                session.add(cb)
                session.flush()
                entity_id = str(cb.id)
                action = "IMPORT_CREATE"
                new = jsonable({"amount": float(cb.amount)})
            log_change(
                session,
                actor=actor,
                action=action,
                entity="CashBalance",
                entity_id=entity_id,
                old=old,
                new=new,
                note=note or "CSV import cash",
            )
            imported += 1

    elif kind == "income_events":
        for i, r in enumerate(rows, start=2):
            try:
                row = IncomeEventRow.model_validate(r)
            except Exception as e:
                errors.append(f"line {i}: {e}")
                continue
            acct = acct_map.get(row.account_name)
            if acct is None:
                errors.append(
                    f"line {i}: unknown account_name={row.account_name} (create it in Setup or run Setup > Create Defaults)"
                )
                continue
            ev = IncomeEvent(account_id=acct.id, date=row.date, type=row.type, ticker=row.ticker, amount=row.amount)
            session.add(ev)
            session.flush()
            log_change(
                session,
                actor=actor,
                action="IMPORT_CREATE",
                entity="IncomeEvent",
                entity_id=str(ev.id),
                old=None,
                new=jsonable({"account_id": ev.account_id, "date": ev.date.isoformat(), "type": ev.type, "amount": float(ev.amount)}),
                note=note or "CSV import income",
            )
            imported += 1

    elif kind == "transactions":
        for i, r in enumerate(rows, start=2):
            try:
                row = TransactionRow.model_validate(r)
            except Exception as e:
                errors.append(f"line {i}: {e}")
                continue
            acct = acct_map.get(row.account_name)
            if acct is None:
                errors.append(
                    f"line {i}: unknown account_name={row.account_name} (create it in Setup or run Setup > Create Defaults)"
                )
                continue
            links: dict[str, Any] = {}
            if row.lot_basis_total is not None:
                links["basis_total"] = float(row.lot_basis_total)
            if row.lot_acquisition_date is not None:
                links["acquisition_date"] = row.lot_acquisition_date.isoformat()
            if row.term is not None:
                links["term"] = row.term
            tx = Transaction(
                account_id=acct.id,
                date=row.date,
                type=row.type,
                ticker=row.ticker,
                qty=row.qty,
                amount=row.amount,
                lot_links_json=links,
            )
            session.add(tx)
            session.flush()
            log_change(
                session,
                actor=actor,
                action="IMPORT_CREATE",
                entity="Transaction",
                entity_id=str(tx.id),
                old=None,
                new=jsonable({"account_id": tx.account_id, "date": tx.date.isoformat(), "type": tx.type, "amount": float(tx.amount)}),
                note=note or "CSV import transactions",
            )
            imported += 1

    elif kind == "securities":
        for i, r in enumerate(rows, start=2):
            try:
                row = SecurityRow.model_validate(r)
            except Exception as e:
                errors.append(f"line {i}: {e}")
                continue
            group_id = None
            if row.substitute_group:
                grp = session.query(SubstituteGroup).filter(SubstituteGroup.name == row.substitute_group).one_or_none()
                if grp is None:
                    grp = SubstituteGroup(name=row.substitute_group, description="Imported group")
                    session.add(grp)
                    session.flush()
                    log_change(
                        session,
                        actor=actor,
                        action="IMPORT_CREATE",
                        entity="SubstituteGroup",
                        entity_id=str(grp.id),
                        old=None,
                        new=jsonable({"name": grp.name}),
                        note=note or "CSV import substitute group",
                    )
                group_id = grp.id
            existing = session.query(Security).filter(Security.ticker == row.ticker).one_or_none()
            old = jsonable(existing.metadata_json) if existing else None
            meta = {"last_price": row.last_price}
            if row.low_cost_ticker:
                meta["low_cost_ticker"] = row.low_cost_ticker
            if existing:
                existing.name = row.name
                existing.asset_class = row.asset_class
                existing.expense_ratio = row.expense_ratio
                existing.substitute_group_id = group_id
                existing.metadata_json = meta
                entity_id = str(existing.id)
                action = "IMPORT_UPDATE"
                new = jsonable({"ticker": existing.ticker, "metadata": existing.metadata_json})
            else:
                sec = Security(
                    ticker=row.ticker,
                    name=row.name,
                    asset_class=row.asset_class,
                    expense_ratio=row.expense_ratio,
                    substitute_group_id=group_id,
                    metadata_json=meta,
                )
                session.add(sec)
                session.flush()
                entity_id = str(sec.id)
                action = "IMPORT_CREATE"
                new = jsonable({"ticker": sec.ticker, "metadata": sec.metadata_json})
            log_change(
                session,
                actor=actor,
                action=action,
                entity="Security",
                entity_id=entity_id,
                old=old,
                new=new,
                note=note or "CSV import securities",
            )
            imported += 1
    else:
        errors.append(f"Unknown import kind: {kind}")

    if errors:
        log_change(
            session,
            actor=actor,
            action="IMPORT_ERRORS",
            entity="CSVImport",
            entity_id=kind,
            old=None,
            new=jsonable({"errors": errors[:50]}),
            note=note or "CSV import errors",
        )

    return {"kind": kind, "rows": imported, "errors": errors}
