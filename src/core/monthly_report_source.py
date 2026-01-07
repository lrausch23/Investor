from __future__ import annotations

import csv
import datetime as dt
import io
from dataclasses import dataclass
from typing import Any

from sqlalchemy.orm import Session

from src.db.models import (
    Account,
    ExternalAccountMap,
    ExternalConnection,
    ExternalHoldingSnapshot,
    TaxpayerEntity,
    Transaction,
)


@dataclass(frozen=True)
class MonthlyReportInputs:
    transactions_csv_bytes: bytes
    monthly_perf_csv_bytes: bytes
    holdings_csv_bytes: bytes | None
    warnings: list[str]


def _as_of_date_utc(x: dt.datetime) -> dt.date:
    if x.tzinfo is None:
        return x.date()
    return x.astimezone(dt.timezone.utc).date()


def _is_internal_transfer_like(links: dict[str, Any] | None) -> bool:
    links = links or {}
    desc = str(links.get("description") or "").upper()
    addl = str(links.get("additional_detail") or "").upper()
    raw = str(links.get("raw_type") or "").upper()
    txt = f"{desc} {addl}"
    if "DEPOSIT SWEEP" in txt:
        return True
    if "SHADO" in txt:
        return True
    if "REC FR SIS" in txt or "REC TRSF SIS" in txt:
        return True
    if "TRSF SIS" in txt:
        return True
    if raw == "UNKNOWN" and ("MULTI" in txt and "CURRENCY" in txt):
        return True
    if "FX" in txt and ("SETTLEMENT" in txt or "TRAD" in txt or "TRADE" in txt):
        return True
    return False


def _scope_account_filter(scope: str):
    s = (scope or "").strip().lower()
    if s == "ira":
        return [Account.account_type == "IRA"]
    if s == "trust":
        return [TaxpayerEntity.type == "TRUST", Account.account_type != "IRA"]
    if s == "personal":
        return [TaxpayerEntity.type == "PERSONAL", Account.account_type != "IRA"]
    return []


def _sum_snapshot_total(payload_json: dict[str, Any]) -> float | None:
    items = payload_json.get("items") if isinstance(payload_json, dict) else None
    if not isinstance(items, list):
        return None
    total = 0.0
    seen = False
    for it in items:
        if not isinstance(it, dict):
            continue
        mv = it.get("market_value")
        try:
            if mv is None:
                continue
            total += float(mv)
            seen = True
        except Exception:
            continue
    return float(total) if seen else None


def _choose_snapshot(
    snaps: list[tuple[dt.date, int, float]],
    *,
    target: dt.date,
    grace_days: int,
) -> tuple[dt.date, int, float] | None:
    if not snaps:
        return None
    window_start = target - dt.timedelta(days=int(grace_days))
    window_end = target + dt.timedelta(days=int(grace_days))
    cand = [s for s in snaps if window_start <= s[0] <= window_end]
    if not cand:
        return None
    before = [s for s in cand if s[0] <= target]
    after = [s for s in cand if s[0] > target]
    return max(before, key=lambda x: x[0]) if before else min(after, key=lambda x: x[0])


def _month_ends(start: dt.date, end: dt.date) -> list[dt.date]:
    out: list[dt.date] = []
    cur = dt.date(start.year, start.month, 1)
    while cur <= end:
        if cur.month == 12:
            me = dt.date(cur.year, 12, 31)
        else:
            me = dt.date(cur.year, cur.month + 1, 1) - dt.timedelta(days=1)
        if start <= me <= end:
            out.append(me)
        cur = dt.date(cur.year + 1, 1, 1) if cur.month == 12 else dt.date(cur.year, cur.month + 1, 1)
    return out


def build_monthly_report_inputs_from_db(
    session: Session,
    *,
    scope: str,
    connection_id: int,
    start_date: dt.date,
    end_date: dt.date,
    asof_date: dt.date,
    grace_days: int = 14,
) -> MonthlyReportInputs:
    warnings: list[str] = []

    # Ensure connection exists and is active.
    conn = (
        session.query(ExternalConnection)
        .filter(ExternalConnection.id == int(connection_id), ExternalConnection.status == "ACTIVE")
        .one_or_none()
    )
    if conn is None:
        return MonthlyReportInputs(b"", b"", None, [f"Connection {connection_id} not found or inactive."])

    # Accounts in this portfolio/scope.
    acct_rows = (
        session.query(Account.id, Account.name)
        .select_from(ExternalAccountMap)
        .join(Account, Account.id == ExternalAccountMap.account_id)
        .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
        .filter(ExternalAccountMap.connection_id == int(connection_id))
        .filter(*_scope_account_filter(scope))
        .order_by(Account.name.asc(), Account.id.asc())
        .all()
    )
    account_ids = [int(r[0]) for r in acct_rows]
    account_name_by_id = {int(i): str(n) for i, n in acct_rows}
    if not account_ids:
        return MonthlyReportInputs(b"", b"", None, [f"No accounts mapped to connection {connection_id} for scope={scope}."])

    # Transactions for selected accounts.
    txs = (
        session.query(Transaction)
        .filter(Transaction.account_id.in_(account_ids), Transaction.date >= start_date, Transaction.date <= end_date)
        .order_by(Transaction.date.asc(), Transaction.id.asc())
        .all()
    )
    if not txs:
        warnings.append("No transactions found in selected period; realized P&L and flow timing may be limited.")

    # Holdings snapshots to compute NAV.
    snaps_raw = (
        session.query(ExternalHoldingSnapshot)
        .filter(ExternalHoldingSnapshot.connection_id == int(connection_id))
        .order_by(ExternalHoldingSnapshot.as_of.asc(), ExternalHoldingSnapshot.id.asc())
        .all()
    )
    snaps: list[tuple[dt.date, int, float]] = []
    for s in snaps_raw:
        try:
            d = _as_of_date_utc(s.as_of)
            total = _sum_snapshot_total(s.payload_json)
            if total is None:
                continue
            snaps.append((d, int(s.id), float(total)))
        except Exception:
            continue
    if not snaps:
        return MonthlyReportInputs(b"", b"", None, warnings + ["No holdings snapshots found for this portfolio; cannot compute NAV series."])

    # Baseline near period start.
    baseline = _choose_snapshot(snaps, target=start_date, grace_days=grace_days)
    if baseline is None:
        warnings.append(f"No baseline holdings snapshot within ±{grace_days} days of {start_date}.")

    month_ends = _month_ends(start_date, end_date)
    if not month_ends:
        return MonthlyReportInputs(b"", b"", None, warnings + ["No month-ends in selected range."])

    # Choose month-end snapshot per month.
    month_end_snap: dict[dt.date, tuple[dt.date, int, float]] = {}
    for me in month_ends:
        pick = _choose_snapshot(snaps, target=me, grace_days=grace_days)
        if pick is None:
            warnings.append(f"No holdings snapshot within ±{grace_days} days of month-end {me}.")
            continue
        month_end_snap[me] = pick

    # Build monthly perf CSV.
    monthly_out = io.StringIO()
    w = csv.writer(monthly_out)
    w.writerow(
        [
            "Date",
            "Beginning market value",
            "Ending market value",
            "Contributions",
            "Withdrawals",
            "Taxes withheld",
            "Fees",
            "Income",
        ]
    )

    prev_value: float | None = baseline[2] if baseline is not None else None
    for me in month_ends:
        snap = month_end_snap.get(me)
        if snap is None:
            continue
        end_value = float(snap[2])
        begin_value = float(prev_value) if prev_value is not None else None

        # Flows for the month (portfolio perspective).
        ms = dt.date(me.year, me.month, 1)
        contrib = withdraw = taxes = fees = income = 0.0
        other_cash = 0.0
        for tx in txs:
            if tx.date < ms or tx.date > me:
                continue
            if tx.type == "TRANSFER":
                if _is_internal_transfer_like(tx.lot_links_json):
                    continue
                if float(tx.amount) > 0:
                    contrib += float(tx.amount)
                elif float(tx.amount) < 0:
                    withdraw += abs(float(tx.amount))
            elif tx.type == "WITHHOLDING":
                taxes += abs(float(tx.amount))
            elif tx.type == "FEE":
                if float(tx.amount) < 0:
                    fees += abs(float(tx.amount))
            elif tx.type in {"DIV", "INT"}:
                income += abs(float(tx.amount))
            elif tx.type == "OTHER":
                # Not modeled in monthly table; surface as warning if present.
                if float(tx.amount) != 0:
                    other_cash += abs(float(tx.amount))

        if other_cash > 0:
            warnings.append(f"Found OTHER cashflows in {me:%Y-%m}; these are not included in monthly net flows.")

        if begin_value is None:
            warnings.append(f"Missing begin value for {me:%Y-%m}; returns for this month may be skipped.")
        w.writerow([me.isoformat(), begin_value if begin_value is not None else "", end_value, contrib, withdraw, taxes, fees, income])
        prev_value = end_value

    monthly_bytes = monthly_out.getvalue().encode("utf-8")

    # Build transactions CSV for attribution and dated cashflows.
    tx_out = io.StringIO()
    wtx = csv.writer(tx_out)
    wtx.writerow(["Date", "Symbol", "Type", "Quantity", "Price", "Amount", "Fees", "Account", "Description"])
    for tx in txs:
        if tx.type == "TRANSFER" and _is_internal_transfer_like(tx.lot_links_json):
            continue
        amt = float(tx.amount)
        # Our system stores WITHHOLDING as a positive credit, but economically it is a cash out.
        # For pipeline conventions, encode it as a negative portfolio cash impact so investor cashflow is positive.
        if tx.type == "WITHHOLDING":
            amt = -abs(amt)
        # Map Account name for readability.
        acct = account_name_by_id.get(int(tx.account_id)) or f"Account {tx.account_id}"
        desc = ""
        try:
            desc = str((tx.lot_links_json or {}).get("description") or "")
        except Exception:
            desc = ""
        wtx.writerow(
            [
                tx.date.isoformat(),
                (tx.ticker or "").strip().upper(),
                tx.type,
                float(tx.qty) if tx.qty is not None else "",
                "",
                amt,
                "",
                acct,
                desc,
            ]
        )
    tx_bytes = tx_out.getvalue().encode("utf-8")

    # Holdings CSV (as-of) from nearest snapshot.
    hold_bytes: bytes | None = None
    hold_pick = _choose_snapshot(snaps, target=asof_date, grace_days=grace_days)
    if hold_pick is None:
        warnings.append(f"No holdings snapshot within ±{grace_days} days of as-of {asof_date}; position guidance may be limited.")
    else:
        # Use the raw snapshot payload for positions.
        snap_obj = next((s for s in snaps_raw if int(s.id) == int(hold_pick[1])), None)
        if snap_obj is not None:
            items = (snap_obj.payload_json or {}).get("items") if isinstance(snap_obj.payload_json, dict) else None
            if isinstance(items, list):
                by_sym: dict[str, dict[str, float]] = {}
                for it in items:
                    if not isinstance(it, dict):
                        continue
                    sym = str(it.get("symbol") or it.get("ticker") or "").strip().upper()
                    if not sym:
                        continue
                    if sym in {"TOTAL", "UNKNOWN"}:
                        continue
                    qty = it.get("quantity")
                    mv = it.get("market_value")
                    try:
                        qf = float(qty) if qty is not None else 0.0
                    except Exception:
                        qf = 0.0
                    try:
                        mvf = float(mv) if mv is not None else 0.0
                    except Exception:
                        mvf = 0.0
                    rec = by_sym.setdefault(sym, {"qty": 0.0, "mv": 0.0})
                    rec["qty"] += qf
                    rec["mv"] += mvf
                if by_sym:
                    hout = io.StringIO()
                    wh = csv.writer(hout)
                    wh.writerow(["Symbol", "Quantity", "MarketValue", "CostBasis"])
                    for sym in sorted(by_sym.keys()):
                        wh.writerow([sym, by_sym[sym]["qty"], by_sym[sym]["mv"], ""])
                    hold_bytes = hout.getvalue().encode("utf-8")
            else:
                warnings.append("Holdings snapshot payload did not contain position items; guidance may be limited.")

    return MonthlyReportInputs(
        transactions_csv_bytes=tx_bytes,
        monthly_perf_csv_bytes=monthly_bytes,
        holdings_csv_bytes=hold_bytes,
        warnings=warnings,
    )
