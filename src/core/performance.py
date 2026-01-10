from __future__ import annotations

import csv
import datetime as dt
import math
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from sqlalchemy.orm import Session
from sqlalchemy import func

from src.core.connection_preference import preferred_active_connection_ids_for_taxpayers
from src.db.models import (
    Account,
    CashBalance,
    ExternalAccountMap,
    ExternalConnection,
    ExternalHoldingSnapshot,
    ExternalTransactionMap,
    TaxpayerEntity,
    Transaction,
)


def _is_internal_cash_mechanic_expr():
    """
    SQL predicate to exclude internal sweep/FX/multi-currency mechanics from cash-out reporting.

    Note: This mirrors (but does not import) `src/app/routes/reports.py::_is_internal_transfer_expr`
    to keep core reporting independent of the web layer.
    """
    desc = func.upper(func.coalesce(func.json_extract(Transaction.lot_links_json, "$.description"), ""))
    addl = func.upper(func.coalesce(func.json_extract(Transaction.lot_links_json, "$.additional_detail"), ""))
    raw = func.upper(func.coalesce(func.json_extract(Transaction.lot_links_json, "$.raw_type"), ""))
    txt = desc + " " + addl
    return (
        (func.instr(txt, "DEPOSIT SWEEP") > 0)
        | (func.instr(txt, "SHADO") > 0)
        | (func.instr(txt, "REC FR SIS") > 0)
        | (func.instr(txt, "REC TRSF SIS") > 0)
        | (func.instr(txt, "TRSF SIS") > 0)
        | ((raw == "UNKNOWN") & (func.instr(txt, "MULTI") > 0) & (func.instr(txt, "CURRENCY") > 0))
        | ((func.instr(txt, "FX") > 0) & ((func.instr(txt, "SETTLEMENT") > 0) | (func.instr(txt, "TRAD") > 0) | (func.instr(txt, "TRADE") > 0)))
    )


@dataclass(frozen=True)
class PerformanceRow:
    portfolio_id: int
    portfolio_name: str
    taxpayer_name: str
    taxpayer_type: str
    period_start: dt.date
    period_end: dt.date
    coverage_start: dt.date | None
    coverage_end: dt.date | None
    valuation_points: int
    txn_start: dt.date | None
    txn_end: dt.date | None
    txn_count: int
    begin_value: float | None
    end_value: float | None
    contributions: float | None
    withdrawals: float | None
    net_flow: float | None
    fees: float | None
    withholding: float | None
    other_cash_out: float | None
    total_cash_out: float | None
    gain_value: float | None
    irr: float | None
    xirr: float | None
    twr: float | None
    sharpe: float | None
    benchmark_twr: float | None
    benchmark_sharpe: float | None
    excess_twr: float | None
    excess_sharpe: float | None
    warnings: list[str]


def _parse_date(s: Any) -> dt.date | None:
    if s is None:
        return None
    t = str(s).strip()
    if not t:
        return None
    try:
        return dt.date.fromisoformat(t[:10])
    except Exception:
        pass
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y/%m/%d"):
        try:
            return dt.datetime.strptime(t.split()[0], fmt).date()
        except Exception:
            continue
    return None


def _as_float(v: Any) -> float | None:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        try:
            return float(v)
        except Exception:
            return None
    s = str(v).strip()
    if not s:
        return None
    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1]
    s = s.replace("$", "").replace(",", "").replace("*", "").strip()
    try:
        out = float(s)
    except Exception:
        return None
    return -out if neg else out


def _sniff_delimiter(text: str) -> str:
    sample = "\n".join((text or "").splitlines()[:30])
    if not sample:
        return ","
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",\t;")
        return getattr(dialect, "delimiter", ",") or ","
    except Exception:
        return ","


def load_price_series(path: Path) -> list[tuple[dt.date, float]]:
    text = path.read_text(encoding="utf-8-sig", errors="replace")
    delim = _sniff_delimiter(text)
    reader = csv.DictReader(text.splitlines(), delimiter=delim)
    out: list[tuple[dt.date, float]] = []
    for r in reader:
        if not r:
            continue
        def _norm_col(k: str) -> str:
            s = str(k or "").strip().lower()
            s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
            return s

        keys_norm = {_norm_col(str(k)): str(k) for k in r.keys() if k}
        dk = keys_norm.get("date") or keys_norm.get("day") or keys_norm.get("as_of") or keys_norm.get("timestamp")
        if not dk:
            continue
        d = _parse_date(r.get(dk))
        if d is None:
            continue
        # Prefer adjusted close when available (better proxy for total return).
        ck = keys_norm.get("adj_close") or keys_norm.get("adjclose") or keys_norm.get("close") or keys_norm.get("value") or keys_norm.get("price")
        if not ck:
            continue
        c = _as_float(r.get(ck))
        if c is None or c <= 0:
            continue
        out.append((d, float(c)))
    out.sort(key=lambda x: x[0])
    # Deduplicate by date (keep last).
    dedup: dict[dt.date, float] = {}
    for d, c in out:
        dedup[d] = c
    return sorted(dedup.items(), key=lambda x: x[0])


def load_sp500_prices(path: Path) -> list[tuple[dt.date, float]]:
    # Backward-compatible alias (older UI labeled this as "S&P 500").
    return load_price_series(path)


def price_on_or_before(series: list[tuple[dt.date, float]], d: dt.date) -> float | None:
    if not series:
        return None
    lo = 0
    hi = len(series) - 1
    best = None
    while lo <= hi:
        mid = (lo + hi) // 2
        md, mv = series[mid]
        if md <= d:
            best = mv
            lo = mid + 1
        else:
            hi = mid - 1
    return best


def price_point_on_or_before(series: list[tuple[dt.date, float]], d: dt.date) -> tuple[dt.date, float] | None:
    if not series:
        return None
    lo = 0
    hi = len(series) - 1
    best: tuple[dt.date, float] | None = None
    while lo <= hi:
        mid = (lo + hi) // 2
        md, mv = series[mid]
        if md <= d:
            best = (md, float(mv))
            lo = mid + 1
        else:
            hi = mid - 1
    return best


def price_point_on_or_after(series: list[tuple[dt.date, float]], d: dt.date) -> tuple[dt.date, float] | None:
    if not series:
        return None
    lo = 0
    hi = len(series) - 1
    best: tuple[dt.date, float] | None = None
    while lo <= hi:
        mid = (lo + hi) // 2
        md, mv = series[mid]
        if md >= d:
            best = (md, float(mv))
            hi = mid - 1
        else:
            lo = mid + 1
    return best


def _npv(rate: float, cashflows: list[tuple[dt.date, float]]) -> float:
    if rate <= -0.999999:
        return float("inf")
    d0 = cashflows[0][0]
    out = 0.0
    for d, amt in cashflows:
        years = (d - d0).days / 365.0
        out += float(amt) / ((1.0 + rate) ** years)
    return out


def xirr(cashflows: list[tuple[dt.date, float]]) -> float | None:
    cfs = [(d, float(a)) for d, a in cashflows if d is not None and a is not None]
    cfs.sort(key=lambda x: x[0])
    if len(cfs) < 2:
        return None
    has_pos = any(a > 0 for _d, a in cfs)
    has_neg = any(a < 0 for _d, a in cfs)
    if not (has_pos and has_neg):
        return None

    # Try Newton-Raphson with a few starting guesses.
    guesses = [0.1, 0.05, 0.2, 0.0, -0.2]
    for guess in guesses:
        r = guess
        for _ in range(50):
            f = _npv(r, cfs)
            if abs(f) < 1e-6:
                return r
            # Numerical derivative
            eps = 1e-6
            f1 = _npv(r + eps, cfs)
            df = (f1 - f) / eps
            if df == 0 or not math.isfinite(df):
                break
            step = f / df
            r2 = r - step
            if r2 <= -0.999999 or not math.isfinite(r2):
                break
            if abs(r2 - r) < 1e-9:
                return r2
            r = r2

    # Fallback: bisection (wide bounds).
    lo = -0.95
    hi = 10.0
    f_lo = _npv(lo, cfs)
    f_hi = _npv(hi, cfs)
    if not (math.isfinite(f_lo) and math.isfinite(f_hi)):
        return None
    if f_lo == 0:
        return lo
    if f_hi == 0:
        return hi
    if f_lo * f_hi > 0:
        return None
    for _ in range(200):
        mid = (lo + hi) / 2.0
        f_mid = _npv(mid, cfs)
        if not math.isfinite(f_mid):
            hi = mid
            continue
        if abs(f_mid) < 1e-6:
            return mid
        if f_lo * f_mid <= 0:
            hi = mid
            f_hi = f_mid
        else:
            lo = mid
            f_lo = f_mid
        if abs(hi - lo) < 1e-9:
            return (lo + hi) / 2.0
    return None


def twr_from_series(
    *,
    values: list[tuple[dt.date, float]],
    flows: list[tuple[dt.date, float]],
) -> tuple[float | None, list[float], list[str]]:
    warnings: list[str] = []
    if len(values) < 2:
        return None, [], ["Need at least 2 valuation points."]
    values = sorted(values, key=lambda x: x[0])
    flows = sorted(flows, key=lambda x: x[0])

    # Net flows by date (portfolio perspective: contributions are positive).
    flow_by_date: dict[dt.date, float] = {}
    for d, a in flows:
        flow_by_date[d] = float(flow_by_date.get(d) or 0.0) + float(a or 0.0)

    rets: list[float] = []
    prod = 1.0
    for i in range(1, len(values)):
        d0, v0 = values[i - 1]
        d1, v1 = values[i]
        if v0 is None or float(v0) <= 1e-9:
            warnings.append(f"Skipped period starting {d0}: begin value is zero.")
            continue
        if d1 <= d0:
            warnings.append(f"Skipped period starting {d0}: invalid date ordering.")
            continue
        total_days = float((d1 - d0).days)
        if total_days <= 0:
            warnings.append(f"Skipped period starting {d0}: zero-length period.")
            continue

        # Modified Dietz for the interval (best-effort TWR approximation when flows occur inside the interval).
        net_flow = 0.0
        weighted_flow = 0.0
        for fd, fa in flow_by_date.items():
            if d0 < fd <= d1:
                amt = float(fa or 0.0)
                net_flow += amt
                # Weight by fraction of period remaining after the cashflow date.
                w = float((d1 - fd).days) / total_days
                if w < 0:
                    w = 0.0
                elif w > 1:
                    w = 1.0
                weighted_flow += amt * w

        denom = float(v0) + float(weighted_flow)
        if abs(denom) <= 1e-9:
            warnings.append(f"Skipped period starting {d0}: denominator is zero (begin value + weighted flows).")
            continue
        r = (float(v1) - float(v0) - net_flow) / denom
        rets.append(r)
        prod *= (1.0 + r)
    if not rets:
        return None, [], warnings or ["No valid subperiod returns."]
    return prod - 1.0, rets, warnings


def sharpe_ratio(
    *,
    period_returns: list[float],
    risk_free_annual: float = 0.0,
    periods_per_year: float = 12.0,
) -> float | None:
    if len(period_returns) < 2:
        return None
    mean_r = sum(period_returns) / float(len(period_returns))
    rf_p = float(risk_free_annual) / float(periods_per_year)
    excess = [r - rf_p for r in period_returns]
    mean_excess = sum(excess) / float(len(excess))
    var = sum((r - mean_excess) ** 2 for r in excess) / float(len(excess) - 1)
    if var <= 0:
        return None
    std = math.sqrt(var)
    return (mean_excess / std) * math.sqrt(periods_per_year)


def _month_key(d: dt.date) -> tuple[int, int]:
    return (int(d.year), int(d.month))


def _downsample(values: dict[dt.date, float], *, frequency: str) -> list[tuple[dt.date, float]]:
    pts = sorted(values.items(), key=lambda x: x[0])
    if frequency != "month_end":
        return pts
    by_month: dict[tuple[int, int], tuple[dt.date, float]] = {}
    for d, v in pts:
        k = _month_key(d)
        if k not in by_month or d > by_month[k][0]:
            by_month[k] = (d, v)
    out = sorted(by_month.values(), key=lambda x: x[0])
    # Ensure first and last valuation are included if present.
    if pts:
        first = pts[0]
        last = pts[-1]
        if out and out[0][0] != first[0]:
            out.insert(0, first)
        if out and out[-1][0] != last[0]:
            out.append(last)
    return out


def _bench_series_for_period(
    series: list[tuple[dt.date, float]],
    *,
    start_date: dt.date,
    end_date: dt.date,
    frequency: str,
) -> list[tuple[dt.date, float]]:
    if not series:
        return []
    if end_date < start_date:
        return []
    series_sorted = sorted([(d, float(v)) for d, v in series if d is not None and v is not None], key=lambda x: x[0])
    # Prefer an anchor on/before the period start (e.g., 12/31 close for calendar-year returns),
    # but fall back to the first point on/after when the series doesn't include prior dates.
    start_pt = price_point_on_or_before(series_sorted, start_date) or price_point_on_or_after(series_sorted, start_date)
    end_pt = price_point_on_or_before(series_sorted, end_date)
    if start_pt is None or end_pt is None:
        return []

    pts = [(d, float(v)) for d, v in series_sorted if start_date <= d <= end_date]
    pts.sort(key=lambda x: x[0])
    if frequency == "month_end":
        by_month: dict[tuple[int, int], tuple[dt.date, float]] = {}
        for d, v in pts:
            k = _month_key(d)
            if k not in by_month or d > by_month[k][0]:
                by_month[k] = (d, v)
        out = sorted(by_month.values(), key=lambda x: x[0])
        # Ensure anchors are included using actual trading dates.
        if not out or out[0][0] != start_pt[0]:
            out.insert(0, start_pt)
        if not out or out[-1][0] != end_pt[0]:
            out.append(end_pt)
        # Deduplicate any identical dates (keep last).
        dedup: dict[dt.date, float] = {}
        for d, v in out:
            dedup[d] = float(v)
        return sorted(dedup.items(), key=lambda x: x[0])
    # daily (all points)
    out = pts
    if not out or out[0][0] != start_pt[0]:
        out = [start_pt] + out
    if not out or out[-1][0] != end_pt[0]:
        out = out + [end_pt]
    dedup: dict[dt.date, float] = {}
    for d, v in out:
        dedup[d] = float(v)
    return sorted(dedup.items(), key=lambda x: x[0])


def _cash_series_by_account(session: Session, account_id: int, *, end_date: dt.date) -> list[tuple[dt.date, float]]:
    rows = (
        session.query(CashBalance)
        .filter(CashBalance.account_id == account_id, CashBalance.as_of_date <= end_date)
        .order_by(CashBalance.as_of_date.asc(), CashBalance.id.asc())
        .all()
    )
    out: list[tuple[dt.date, float]] = []
    for r in rows:
        try:
            out.append((r.as_of_date, float(r.amount or 0.0)))
        except Exception:
            continue
    return out


def _cash_on_or_before(cash_series: list[tuple[dt.date, float]], d: dt.date) -> float | None:
    if not cash_series:
        return None
    lo = 0
    hi = len(cash_series) - 1
    best = None
    while lo <= hi:
        mid = (lo + hi) // 2
        md, mv = cash_series[mid]
        if md <= d:
            best = mv
            lo = mid + 1
        else:
            hi = mid - 1
    return best


def _flow_key(tx: Transaction, etm: ExternalTransactionMap | None) -> str | None:
    links = tx.lot_links_json or {}
    provider_acct = str(links.get("provider_account_id") or "").strip()
    provider_txn_id = str(
        (getattr(etm, "provider_txn_id", None) if etm is not None else None) or links.get("provider_txn_id") or ""
    ).strip()
    if provider_acct and provider_txn_id:
        return f"{provider_acct}|{provider_txn_id}"
    return None


def _round_cents(v: float) -> float:
    # Avoid float drift when matching internal transfer pairs.
    return float(round(float(v), 2))


def _looks_like_internal_fx_transfer(raw_type: str | None, description: str | None) -> bool:
    rt = (raw_type or "").strip().upper()
    d = (description or "").strip().upper()
    if rt == "UNKNOWN" and ("MULTI" in d and "CURRENCY" in d):
        return True
    # RJ: internal multi-currency/settlement shuttles (SHADO/SIS naming).
    if "SHADO" in d:
        return True
    if "REC FR SIS" in d or "REC TRSF SIS" in d:
        return True
    if "TRSF TO SIS" in d or "TRSF SIS" in d:
        return True
    # Common internal sweep/FX settlement descriptors across brokers.
    if "FX" in d and ("SETTLEMENT" in d or "TRAD" in d or "TRADE" in d):
        return True
    if ("MULTI" in d and "CURRENCY" in d) or "MULTICURRENCY" in d:
        return True
    if "INTERNAL" in d and "TRANSFER" in d:
        return True
    return False


def _filter_internal_transfer_pairs(
    transfers: list[tuple[dt.date, float, str | None, str | None]],
) -> list[tuple[dt.date, float]]:
    """
    Remove internal transfer pairs that net to ~0 on the same date (e.g., cash<->multi-currency sweeps).

    We pair +X and -X (rounded to cents) on the same date and drop matched pairs.
    """
    # First drop transfers that look like internal FX/multi-currency sweeps.
    remaining: list[tuple[dt.date, float, str | None, str | None]] = []
    for d, amt, raw_type, desc in transfers:
        if _looks_like_internal_fx_transfer(raw_type, desc):
            continue
        remaining.append((d, amt, raw_type, desc))

    by_key: dict[tuple[dt.date, float], dict[str, list[tuple[float, str | None, str | None]]]] = {}
    for d, amt, raw_type, desc in remaining:
        a = _round_cents(float(amt or 0.0))
        if abs(a) <= 1e-9:
            continue
        k = (d, _round_cents(abs(a)))
        bucket = by_key.setdefault(k, {"pos": [], "neg": []})
        if a >= 0:
            bucket["pos"].append((a, raw_type, desc))
        else:
            bucket["neg"].append((a, raw_type, desc))

    out: list[tuple[dt.date, float]] = []
    for (d, abs_amt), bucket in by_key.items():
        pos = bucket.get("pos") or []
        neg = bucket.get("neg") or []
        # Drop exact canceling pairs (likely internal sweeps). Keep any residuals.
        drop_pairs = min(len(pos), len(neg))
        pos_keep = pos[drop_pairs:] if drop_pairs else pos
        neg_keep = neg[drop_pairs:] if drop_pairs else neg

        for a, rt, desc in pos_keep:
            out.append((d, float(a)))
        for a, rt, desc in neg_keep:
            out.append((d, float(a)))

    out.sort(key=lambda x: (x[0], x[1]))
    return out


def _account_ids_in_scope(session: Session, scope: str) -> list[int]:
    q = session.query(Account).join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
    if scope == "trust":
        q = q.filter(TaxpayerEntity.type == "TRUST", Account.account_type != "IRA")
    elif scope == "personal":
        q = q.filter(TaxpayerEntity.type == "PERSONAL", Account.account_type != "IRA")
    elif scope == "ira":
        q = q.filter(Account.account_type == "IRA")
    return [int(a.id) for a in q.order_by(Account.name.asc()).all()]

def _connection_ids_in_scope(session: Session, scope: str) -> list[int]:
    q = (
        session.query(ExternalAccountMap.connection_id)
        .join(Account, Account.id == ExternalAccountMap.account_id)
        .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
        .join(ExternalConnection, ExternalConnection.id == ExternalAccountMap.connection_id)
        .filter(ExternalConnection.status == "ACTIVE")
    )
    if scope == "trust":
        q = q.filter(TaxpayerEntity.type == "TRUST", Account.account_type != "IRA")
    elif scope == "personal":
        q = q.filter(TaxpayerEntity.type == "PERSONAL", Account.account_type != "IRA")
    elif scope == "ira":
        q = q.filter(Account.account_type == "IRA")
    rows = q.distinct().all()
    return [int(r[0]) for r in rows if r and r[0] is not None]

def _preferred_connection_ids(session: Session, scope: str) -> list[int]:
    conn_ids = _connection_ids_in_scope(session, scope)
    if not conn_ids:
        return []
    # Preferences are keyed by taxpayer. For mixed connections, best-effort: use taxpayers of in-scope accounts.
    tp_ids_q = (
        session.query(TaxpayerEntity.id)
        .select_from(ExternalAccountMap)
        .join(Account, Account.id == ExternalAccountMap.account_id)
        .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
        .filter(ExternalAccountMap.connection_id.in_(conn_ids))
    )
    if scope == "trust":
        tp_ids_q = tp_ids_q.filter(TaxpayerEntity.type == "TRUST", Account.account_type != "IRA")
    elif scope == "personal":
        tp_ids_q = tp_ids_q.filter(TaxpayerEntity.type == "PERSONAL", Account.account_type != "IRA")
    elif scope == "ira":
        tp_ids_q = tp_ids_q.filter(Account.account_type == "IRA")
    tp_ids = [int(r[0]) for r in tp_ids_q.distinct().all() if r and r[0] is not None]
    preferred = preferred_active_connection_ids_for_taxpayers(session, taxpayer_ids=tp_ids) if tp_ids else []
    if preferred:
        return [int(cid) for cid in conn_ids if int(cid) in preferred]
    return [int(cid) for cid in conn_ids]


def _valuation_points_from_snapshots(
    session: Session,
    *,
    scope: str,
    start_date: dt.date,
    end_date: dt.date,
    connection_ids: list[int] | None = None,
    account_ids: list[int] | None = None,
) -> dict[int, dict[dt.date, float]]:
    conn_ids = [int(x) for x in (connection_ids or _preferred_connection_ids(session, scope))]
    if not conn_ids:
        return {}

    # Map provider accounts -> internal accounts (optionally filter to IRA accounts).
    mq = (
        session.query(ExternalAccountMap, Account)
        .join(Account, Account.id == ExternalAccountMap.account_id)
        .filter(ExternalAccountMap.connection_id.in_(conn_ids))
    )
    if account_ids:
        mq = mq.filter(Account.id.in_([int(x) for x in account_ids]))
    if scope == "ira":
        mq = mq.filter(Account.account_type == "IRA")
    elif scope == "trust":
        mq = mq.join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id).filter(
            TaxpayerEntity.type == "TRUST",
            Account.account_type != "IRA",
        )
    elif scope == "personal":
        mq = mq.join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id).filter(
            TaxpayerEntity.type == "PERSONAL",
            Account.account_type != "IRA",
        )
    maps = mq.all()
    map_by_conn_provider: dict[tuple[int, str], int] = {(int(m.connection_id), m.provider_account_id): int(a.id) for (m, a) in maps}
    acct_to_conn: dict[int, int] = {int(a.id): int(m.connection_id) for (m, a) in maps}

    start_dt = dt.datetime.combine(start_date, dt.time.min, tzinfo=dt.timezone.utc)
    end_dt = dt.datetime.combine(end_date, dt.time.max, tzinfo=dt.timezone.utc)
    snaps = (
        session.query(ExternalHoldingSnapshot)
        .filter(
            ExternalHoldingSnapshot.connection_id.in_(conn_ids),
            ExternalHoldingSnapshot.as_of >= start_dt,
            ExternalHoldingSnapshot.as_of <= end_dt,
        )
        .order_by(ExternalHoldingSnapshot.as_of.asc(), ExternalHoldingSnapshot.id.asc())
        .all()
    )

    # For each (account_id, date), keep the latest snapshot seen that day.
    # Store (as_of_dt, positions_value, cash_value_snapshot)
    latest: dict[tuple[int, dt.date], tuple[dt.datetime, float, float]] = {}
    totals_keys: set[tuple[int, dt.date]] = set()
    for snap in snaps:
        payload = snap.payload_json or {}
        items = payload.get("items") or []
        if not isinstance(items, list):
            continue
        positions: dict[int, float] = {}
        cash: dict[int, float] = {}
        totals: dict[int, float] = {}
        for it in items:
            if not isinstance(it, dict):
                continue
            provider_acct = str(it.get("provider_account_id") or "").strip()
            acct_id = map_by_conn_provider.get((int(snap.connection_id), provider_acct))
            if acct_id is None:
                continue
            sym = str(it.get("symbol") or it.get("ticker") or "").strip().upper()
            mv = _as_float(it.get("market_value") or it.get("value") or it.get("qty") or 0.0) or 0.0
            if bool(it.get("is_total")) and mv and mv > 0:
                totals[acct_id] = float(mv)
                continue
            if sym.startswith("CASH:"):
                cash[acct_id] = float(cash.get(acct_id) or 0.0) + float(mv)
            elif sym:
                positions[acct_id] = float(positions.get(acct_id) or 0.0) + float(mv)
        day = snap.as_of.date()
        totals_accts = set(totals.keys())
        # If a snapshot provides an explicit total (e.g., parsed from a PDF statement), use it and avoid
        # double-counting by summing positions/cash.
        for acct_id, tv in totals.items():
            k = (acct_id, day)
            prev = latest.get(k)
            if prev is None or snap.as_of >= prev[0]:
                latest[k] = (snap.as_of, float(tv), 0.0)
                totals_keys.add(k)
        for acct_id, pv in positions.items():
            if acct_id in totals_accts:
                continue
            cv = float(cash.get(acct_id) or 0.0)
            k = (acct_id, day)
            prev = latest.get(k)
            if prev is None or snap.as_of >= prev[0]:
                latest[k] = (snap.as_of, float(pv), cv)
        # Also allow cash-only rows (all-cash account).
        for acct_id, cv in cash.items():
            if acct_id in totals_accts:
                continue
            k = (acct_id, day)
            if k in latest:
                continue
            prev = latest.get(k)
            if prev is None or snap.as_of >= prev[0]:
                latest[k] = (snap.as_of, 0.0, float(cv))

    out: dict[int, dict[dt.date, float]] = {}
    for (acct_id, day), (_asof, pos_v, cash_v) in latest.items():
        out.setdefault(acct_id, {})[day] = float(pos_v + cash_v)

    # Prefer imported CashBalance (USD) when available; it is often more reliable than treating cash
    # as a position row in snapshots (some brokers omit cash from holdings).
    cash_series_by_acct: dict[int, list[tuple[dt.date, float]]] = {}
    for acct_id in {int(aid) for (aid, _day) in latest.keys()}:
        cash_series_by_acct[int(acct_id)] = _cash_series_by_account(session, int(acct_id), end_date=end_date)
    for (acct_id, day), (_asof, pos_v, cash_v) in latest.items():
        if (int(acct_id), day) in totals_keys:
            continue
        # If the holdings snapshot already includes explicit cash positions, prefer that.
        if abs(float(cash_v or 0.0)) > 1e-9:
            continue
        cb = _cash_on_or_before(cash_series_by_acct.get(int(acct_id)) or [], day)
        if cb is None:
            continue
        out.setdefault(int(acct_id), {})[day] = float(pos_v + float(cb))

    # Roll up to connection totals per day.
    by_conn: dict[int, dict[dt.date, float]] = {}
    for acct_id, series in out.items():
        cid = acct_to_conn.get(int(acct_id))
        if cid is None:
            continue
        for d, v in series.items():
            prev = float(by_conn.setdefault(int(cid), {}).get(d) or 0.0)
            by_conn[int(cid)][d] = prev + float(v)
    return by_conn


def _transfer_flows(
    session: Session,
    *,
    connection_id: int,
    scope: str,
    start_date: dt.date,
    end_date: dt.date,
    account_ids: list[int] | None = None,
    include_withholding_as_flow: bool = False,
) -> list[tuple[dt.date, float]]:
    q = (
        session.query(Transaction, ExternalTransactionMap)
        .join(ExternalTransactionMap, ExternalTransactionMap.transaction_id == Transaction.id)
        .join(Account, Account.id == Transaction.account_id)
        .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
        .filter(
            ExternalTransactionMap.connection_id == connection_id,
            Transaction.date >= start_date,
            Transaction.date <= end_date,
        )
    )
    if account_ids:
        q = q.filter(Transaction.account_id.in_([int(x) for x in account_ids]))
    if scope == "ira":
        q = q.filter(Account.account_type == "IRA")
    elif scope == "trust":
        q = q.filter(TaxpayerEntity.type == "TRUST", Account.account_type != "IRA")
    elif scope == "personal":
        q = q.filter(TaxpayerEntity.type == "PERSONAL", Account.account_type != "IRA")

    rows = q.order_by(Transaction.date.asc(), Transaction.id.asc()).all()
    transfers: list[tuple[dt.date, float, str | None, str | None]] = []
    flows: list[tuple[dt.date, float]] = []
    seen: set[str] = set()
    for tx, etm in rows:
        t = str(tx.type or "").upper()
        if t == "TRANSFER":
            k = _flow_key(tx, etm)
            if k and k in seen:
                continue
            if k:
                seen.add(k)
            raw_type = None
            desc = None
            try:
                raw_type = str((tx.lot_links_json or {}).get("raw_type") or "")
            except Exception:
                raw_type = None
            try:
                links = tx.lot_links_json or {}
                d0 = str(links.get("description") or "")
                d1 = str(links.get("additional_detail") or "")
                desc = " ".join([x for x in [d0.strip(), d1.strip()] if x]).strip()
            except Exception:
                desc = None
            transfers.append((tx.date, float(tx.amount or 0.0), raw_type, desc))
            continue
        elif include_withholding_as_flow and t == "WITHHOLDING":
            pass
        else:
            continue
        k = _flow_key(tx, etm)
        if k and k in seen:
            continue
        if k:
            seen.add(k)
        try:
            flows.append((tx.date, float(tx.amount or 0.0)))
        except Exception:
            continue
    # Filter internal transfer pairs and append.
    for d, amt in _filter_internal_transfer_pairs(transfers):
        flows.append((d, float(amt)))
    return flows


def build_performance_report(
    session: Session,
    *,
    scope: str,
    start_date: dt.date,
    end_date: dt.date,
    frequency: str = "month_end",
    benchmark_prices_path: Path | None = None,
    benchmark_series: list[tuple[dt.date, float]] | None = None,
    benchmark_label: str = "VOO",
    baseline_grace_days: int = 14,
    connection_ids: list[int] | None = None,
    account_ids: list[int] | None = None,
    include_combined: bool = True,
    include_series: bool = False,
) -> dict[str, Any]:
    warnings: list[str] = []
    grace_days = max(0, int(baseline_grace_days))
    baseline_window_start = start_date - dt.timedelta(days=grace_days)
    baseline_window_end = start_date + dt.timedelta(days=grace_days)
    end_window_start = end_date - dt.timedelta(days=grace_days)
    end_window_end = end_date + dt.timedelta(days=grace_days)
    acct_ids = [int(x) for x in (account_ids or []) if x is not None]
    conn_ids = [int(x) for x in (connection_ids or _preferred_connection_ids(session, scope))]
    if acct_ids:
        mapped_conn_ids = (
            session.query(ExternalAccountMap.connection_id)
            .filter(ExternalAccountMap.account_id.in_(acct_ids))
            .distinct()
            .all()
        )
        mapped = {int(r[0]) for r in mapped_conn_ids if r and r[0] is not None}
        conn_ids = [cid for cid in conn_ids if int(cid) in mapped] if connection_ids else sorted(mapped)
    cq = (
        session.query(ExternalConnection, TaxpayerEntity)
        .join(TaxpayerEntity, TaxpayerEntity.id == ExternalConnection.taxpayer_entity_id)
        .filter(ExternalConnection.id.in_(conn_ids), ExternalConnection.status == "ACTIVE")
        .order_by(ExternalConnection.name.asc(), ExternalConnection.id.asc())
    )
    # Apply scope filtering at the mapped-account layer, not ExternalConnection.taxpayer_entity_id.
    # This prevents (e.g.) IRA accounts from leaking into Trust scope.
    in_scope_conn_ids = set(_connection_ids_in_scope(session, scope))
    if conn_ids:
        in_scope_conn_ids = {int(cid) for cid in conn_ids if int(cid) in in_scope_conn_ids}
    cq = cq.filter(ExternalConnection.id.in_(sorted(in_scope_conn_ids)) if in_scope_conn_ids else ExternalConnection.id == -1)
    # Use the in-scope set for downstream data extraction (transactions/valuations).
    conn_ids = sorted(in_scope_conn_ids)
    conn_rows = cq.all()

    # Display taxpayer is based on the in-scope mapped account(s) for each connection (not the connection's own taxpayer).
    tp_by_conn: dict[int, tuple[str, str]] = {}
    tp_q = (
        session.query(ExternalAccountMap.connection_id, TaxpayerEntity.name, TaxpayerEntity.type)
        .select_from(ExternalAccountMap)
        .join(Account, Account.id == ExternalAccountMap.account_id)
        .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
        .filter(ExternalAccountMap.connection_id.in_(conn_ids))
    )
    if acct_ids:
        tp_q = tp_q.filter(Account.id.in_(acct_ids))
    if scope == "ira":
        tp_q = tp_q.filter(Account.account_type == "IRA")
    elif scope == "trust":
        tp_q = tp_q.filter(TaxpayerEntity.type == "TRUST", Account.account_type != "IRA")
    elif scope == "personal":
        tp_q = tp_q.filter(TaxpayerEntity.type == "PERSONAL", Account.account_type != "IRA")
    tp_rows = tp_q.all()
    names_by_conn: dict[int, set[str]] = {}
    types_by_conn: dict[int, set[str]] = {}
    for cid, nm, ty in tp_rows:
        names_by_conn.setdefault(int(cid), set()).add(str(nm or "").strip() or "—")
        types_by_conn.setdefault(int(cid), set()).add(str(ty or "").strip() or "—")
    for cid in conn_ids:
        ns = sorted(names_by_conn.get(int(cid)) or [])
        ts = sorted(types_by_conn.get(int(cid)) or [])
        if len(ns) == 1 and len(ts) == 1:
            tp_by_conn[int(cid)] = (ns[0], ts[0])
        elif ns or ts:
            tp_by_conn[int(cid)] = ("Mixed", "MIXED")

    # Display label per connection: when a connection maps 1:1 to an internal Account,
    # show the account name in reports (users think in "accounts", not connectors).
    acct_label_by_conn: dict[int, str] = {}
    aq = (
        session.query(ExternalAccountMap.connection_id, Account.name)
        .select_from(ExternalAccountMap)
        .join(Account, Account.id == ExternalAccountMap.account_id)
        .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
        .filter(ExternalAccountMap.connection_id.in_(conn_ids))
    )
    if acct_ids:
        aq = aq.filter(Account.id.in_(acct_ids))
    if scope == "ira":
        aq = aq.filter(Account.account_type == "IRA")
    elif scope == "trust":
        aq = aq.filter(TaxpayerEntity.type == "TRUST", Account.account_type != "IRA")
    elif scope == "personal":
        aq = aq.filter(TaxpayerEntity.type == "PERSONAL", Account.account_type != "IRA")
    acct_names_by_conn: dict[int, set[str]] = {}
    for cid, anm in aq.all():
        acct_names_by_conn.setdefault(int(cid), set()).add(str(anm or "").strip() or "—")
    for cid in conn_ids:
        names = sorted(acct_names_by_conn.get(int(cid)) or [])
        if len(names) == 1:
            acct_label_by_conn[int(cid)] = names[0]

    spx_series: list[tuple[dt.date, float]] = []
    if benchmark_series is not None:
        try:
            spx_series = [(d, float(v)) for d, v in (benchmark_series or []) if d is not None and v is not None]
            spx_series.sort(key=lambda x: x[0])
            if not spx_series:
                warnings.append("Benchmark series was provided but contained 0 usable rows.")
        except Exception as e:
            warnings.append(f"Failed to use benchmark series: {type(e).__name__}: {e}")
    elif benchmark_prices_path is not None:
        try:
            spx_series = load_price_series(benchmark_prices_path)
            if not spx_series:
                warnings.append("Benchmark file was loaded but contained 0 usable rows.")
        except Exception as e:
            warnings.append(f"Failed to parse benchmark CSV: {type(e).__name__}: {e}")

    # Valuations from external holdings snapshots (positions + cash when provided).
    # Then override cash with CashBalance when available (more reliable), on a per-valuation-date basis.
    snap_values_by_portfolio = _valuation_points_from_snapshots(
        session,
        scope=scope,
        start_date=baseline_window_start,
        end_date=end_window_end,
        connection_ids=conn_ids,
        account_ids=acct_ids or None,
    )
    if not snap_values_by_portfolio:
        warnings.append("No holdings snapshots found in the selected period; TWR/Sharpe will be blank.")

    rf_annual = 0.0
    try:
        rf_annual = float(os.environ.get("RISK_FREE_RATE_ANNUAL", "0.0"))
    except Exception:
        rf_annual = 0.0
    include_withholding = str(os.environ.get("PERF_INCLUDE_WITHHOLDING_AS_FLOW", "0")).strip().lower() in {"1", "true", "yes", "on"}

    # Optional charting series (does not affect any performance calculations).
    series_curves_by_portfolio: dict[int, list[tuple[dt.date, float]]] = {}
    benchmark_curve: list[tuple[dt.date, float]] = []
    # Transfer cashflows used in IRR/TWR calculations (portfolio perspective: deposits positive).
    # Exposed only for UI-level reporting (event markers) and does not affect calculations.
    transfer_flows_by_portfolio: dict[int, list[tuple[dt.date, float]]] = {}

    # Benchmark KPIs for the selected period (independent of portfolio snapshot coverage).
    benchmark_period_twr = None
    benchmark_period_sharpe = None
    b_rets: list[float] = []
    benchmark_period_prices = _bench_series_for_period(
        spx_series, start_date=start_date, end_date=end_date, frequency=frequency
    )
    benchmark_coverage_start = benchmark_period_prices[0][0] if benchmark_period_prices else None
    benchmark_coverage_end = benchmark_period_prices[-1][0] if benchmark_period_prices else None
    if len(benchmark_period_prices) >= 2:
        benchmark_period_twr, b_rets, _bw = twr_from_series(values=benchmark_period_prices, flows=[])
        if b_rets:
            benchmark_period_sharpe = sharpe_ratio(
                period_returns=b_rets,
                risk_free_annual=rf_annual,
                periods_per_year=12.0 if frequency == "month_end" else 252.0,
            )
            if include_series and len(b_rets) == (len(benchmark_period_prices) - 1):
                try:
                    g = 1.0
                    benchmark_curve = [(benchmark_period_prices[0][0], float(g))]
                    for i, rret in enumerate(b_rets):
                        g *= (1.0 + float(rret))
                        benchmark_curve.append((benchmark_period_prices[i + 1][0], float(g)))
                except Exception:
                    benchmark_curve = []
        if benchmark_coverage_start and benchmark_coverage_start > start_date:
            warnings.append(
                f"{benchmark_label} benchmark coverage starts at {benchmark_coverage_start} (missing earlier prices for selected period)."
            )
        if benchmark_coverage_end and benchmark_coverage_end < end_date:
            warnings.append(
                f"{benchmark_label} benchmark coverage ends at {benchmark_coverage_end} (missing later prices for selected period)."
            )

    rows_out: list[PerformanceRow] = []
    for conn, tp in conn_rows:
        pid = int(conn.id)
        acct_warn: list[str] = []
        raw_vals = snap_values_by_portfolio.get(pid) or {}

        # Snapshot valuation points available in the baseline+end grace windows.
        vals_window = _downsample(raw_vals, frequency=frequency)
        # For selecting the begin/end valuation anchors, use the full set of valuation points in the grace windows.
        # When `frequency == "month_end"`, `_downsample()` intentionally drops non-month-end points (e.g. 2025-01-01),
        # but those points can still be the correct baseline anchor for a calendar-year report.
        vals_for_anchors = sorted(raw_vals.items(), key=lambda x: x[0])

        cov_start = vals_window[0][0] if vals_window else None
        cov_end = vals_window[-1][0] if vals_window else None

        # Choose valuation points to use for begin/end (closest to period boundaries within grace windows).
        baseline_candidates = [p for p in vals_for_anchors if baseline_window_start <= p[0] <= baseline_window_end]
        end_candidates = [p for p in vals_for_anchors if end_window_start <= p[0] <= end_window_end]

        begin_pt: tuple[dt.date, float] | None = None
        end_pt: tuple[dt.date, float] | None = None
        if baseline_candidates:
            # Prefer last valuation on/before start_date; otherwise first valuation after start_date.
            before = [p for p in baseline_candidates if p[0] <= start_date]
            after = [p for p in baseline_candidates if p[0] > start_date]
            begin_pt = max(before, key=lambda p: p[0]) if before else min(after, key=lambda p: p[0])
        if end_candidates:
            # Prefer first valuation on/after end_date; otherwise last valuation before end_date.
            after = [p for p in end_candidates if p[0] >= end_date]
            before = [p for p in end_candidates if p[0] < end_date]
            end_pt = min(after, key=lambda p: p[0]) if after else max(before, key=lambda p: p[0])

        begin_d = begin_pt[0] if begin_pt else None
        end_d = end_pt[0] if end_pt else None
        begin_v = float(begin_pt[1]) if begin_pt else None
        end_v = float(end_pt[1]) if end_pt else None
        has_baseline = bool(begin_d is not None)
        has_end = bool(end_d is not None)

        # Transfer flows should align to the valuation window we actually use.
        # When we have to anchor begin/end valuations within grace windows, use those anchor dates for flows
        # (otherwise flows in the anchor gap get misclassified as "performance").
        flow_start = begin_d or start_date
        flow_end = end_d or end_date
        flows = _transfer_flows(
            session,
            connection_id=pid,
            scope=scope,
            start_date=flow_start,
            end_date=flow_end,
            account_ids=acct_ids or None,
            include_withholding_as_flow=bool(include_withholding),
        )
        transfer_flows_by_portfolio[int(pid)] = list(flows or [])

        # Valuation series used for return calculations (bounded by chosen begin/end valuation dates).
        vals_ds: list[tuple[dt.date, float]] = []
        if begin_d is not None and end_d is not None and begin_v is not None and end_v is not None and begin_d <= end_d:
            vals_ds = [p for p in vals_window if begin_d <= p[0] <= end_d]
            if vals_ds and vals_ds[0][0] != begin_d:
                vals_ds.insert(0, (begin_d, float(begin_v)))
            if vals_ds and vals_ds[-1][0] != end_d:
                vals_ds.append((end_d, float(end_v)))
        contrib = 0.0
        withdraw = 0.0
        net_flow = 0.0
        if flows:
            for _d, amt in flows:
                a = float(amt or 0.0)
                net_flow += a
                if a >= 0:
                    contrib += a
                else:
                    withdraw += (-a)

        # Cash-out categories that should be reflected as performance drag (NOT treated as investor flows).
        def _cash_out_sum(*, txn_type: str, only_negative: bool = False, exclude_internal: bool = False) -> float | None:
            q = (
                session.query(func.sum(func.abs(Transaction.amount)))
                .select_from(Transaction)
                .join(ExternalTransactionMap, ExternalTransactionMap.transaction_id == Transaction.id)
                .join(Account, Account.id == Transaction.account_id)
                .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
                .filter(
                    ExternalTransactionMap.connection_id == pid,
                    Transaction.date >= flow_start,
                    Transaction.date <= flow_end,
                    Transaction.type == txn_type,
                )
            )
            if only_negative:
                q = q.filter(Transaction.amount < 0)
            if exclude_internal:
                q = q.filter(~_is_internal_cash_mechanic_expr())
            if acct_ids:
                q = q.filter(Transaction.account_id.in_(acct_ids))
            if scope == "ira":
                q = q.filter(Account.account_type == "IRA")
            elif scope == "trust":
                q = q.filter(TaxpayerEntity.type == "TRUST", Account.account_type != "IRA")
            elif scope == "personal":
                q = q.filter(TaxpayerEntity.type == "PERSONAL", Account.account_type != "IRA")
            v = q.scalar()
            if v is None:
                return None
            try:
                out = float(v or 0.0)
            except Exception:
                return None
            return out if out != 0 else 0.0

        fees_out = float(_cash_out_sum(txn_type="FEE", only_negative=True) or 0.0)
        withholding_out = float(_cash_out_sum(txn_type="WITHHOLDING", only_negative=False) or 0.0)
        other_cash_out = float(_cash_out_sum(txn_type="OTHER", only_negative=True, exclude_internal=True) or 0.0)

        total_cash_out = float(withdraw) + float(fees_out) + float(withholding_out) + float(other_cash_out)

        gain_value = None
        if begin_v is not None and end_v is not None and has_baseline and has_end:
            # Gain includes money taken out (withdrawals) and excludes new contributions.
            # Using portfolio-perspective flows: deposits positive, withdrawals negative.
            gain_value = float(end_v) - float(begin_v) - float(net_flow)

        tx_q = (
            session.query(
                func.min(Transaction.date),
                func.max(Transaction.date),
                func.count(Transaction.id),
            )
            .select_from(Transaction)
            .join(ExternalTransactionMap, ExternalTransactionMap.transaction_id == Transaction.id)
            .join(Account, Account.id == Transaction.account_id)
            .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
            .filter(
                ExternalTransactionMap.connection_id == pid,
                Transaction.date >= flow_start,
                Transaction.date <= flow_end,
            )
        )
        if acct_ids:
            tx_q = tx_q.filter(Transaction.account_id.in_(acct_ids))
        if scope == "ira":
            tx_q = tx_q.filter(Account.account_type == "IRA")
        elif scope == "trust":
            tx_q = tx_q.filter(TaxpayerEntity.type == "TRUST", Account.account_type != "IRA")
        elif scope == "personal":
            tx_q = tx_q.filter(TaxpayerEntity.type == "PERSONAL", Account.account_type != "IRA")
        tx_min, tx_max, tx_cnt = tx_q.one()
        txn_start = tx_min
        txn_end = tx_max
        txn_count = int(tx_cnt or 0)
        if txn_count == 0:
            acct_warn.append(f"No transactions found in valuation window ({flow_start} → {flow_end}).")
        else:
            if txn_start and txn_start > flow_start:
                acct_warn.append(f"Transactions start at {txn_start} (missing earlier activity in valuation window).")
            if txn_end and txn_end < flow_end:
                acct_warn.append(f"Transactions end at {txn_end} (missing later activity in valuation window).")

        if cov_start is None:
            acct_warn.append("No valuation points (holdings snapshots) found in this period.")
        elif not has_baseline:
            acct_warn.append(
                f"Coverage starts at {cov_start}; upload a holdings snapshot near {start_date} (±~{grace_days} days) for true period-to-date performance."
            )
            if (conn.connector or "").upper() == "IB_FLEX_WEB":
                acct_warn.append(
                    "Tip (IB): run a one-day FULL sync with end date near period start (e.g., 12/31 prior year) to backfill a baseline holdings snapshot."
                )
        if has_baseline and begin_d is not None and begin_d != start_date:
            acct_warn.append(f"Using begin snapshot at {begin_d} (target {start_date}).")
        if has_end and end_d is not None and end_d != end_date:
            acct_warn.append(f"Using end snapshot at {end_d} (target {end_date}).")
        if cov_end is not None and cov_end < end_window_start:
            acct_warn.append(
                f"Coverage ends at {cov_end}; upload a holdings snapshot near {end_date} (±~{grace_days} days) for true period-end performance."
            )

        if begin_v is not None and end_v is not None:
            try:
                if begin_v >= 0 and end_v > 10000 and begin_v < 1000 and (begin_v / max(1.0, end_v)) < 0.001:
                    acct_warn.append("Begin value looks unusually small vs end value; verify holdings snapshot totals (statement parsing).")
            except Exception:
                pass

        # IRR cashflows use investor perspective: deposits are negative, withdrawals are positive.
        irr = None
        if (
            has_baseline
            and has_end
            and begin_d is not None
            and end_d is not None
            and begin_d != end_d
            and begin_v is not None
            and end_v is not None
            and begin_v > 0
            and end_v >= 0
        ):
            cfs: list[tuple[dt.date, float]] = [(begin_d, -float(begin_v))]
            for d, amt in flows:
                cfs.append((d, -float(amt)))
            cfs.append((end_d, float(end_v)))
            irr = xirr(cfs)
        elif has_baseline:
            acct_warn.append("IRR/XIRR needs at least 2 valuation points in the period.")

        twr = None
        subrets: list[float] = []
        if has_baseline and has_end and len(vals_ds) >= 2:
            twr, subrets, twr_warn = twr_from_series(values=vals_ds, flows=flows)
            acct_warn.extend(twr_warn)

        sharpe = None
        if subrets:
            sharpe = sharpe_ratio(
                period_returns=subrets,
                risk_free_annual=rf_annual,
                periods_per_year=12.0 if frequency == "month_end" else 252.0,
            )
        if sharpe is None and has_baseline and has_end and len(vals_ds) >= 2:
            # Sharpe requires a return series; with only start/end valuations we only have 1 return,
            # so volatility (std dev) is undefined.
            if len(subrets) < 2:
                acct_warn.append("Sharpe requires at least 2 period returns (≥3 valuation points).")
            else:
                acct_warn.append("Sharpe is undefined for this period (insufficient return variability).")

        bench_twr = benchmark_period_twr
        bench_sharpe = benchmark_period_sharpe
        excess = None
        if spx_series and benchmark_period_twr is not None and len(vals_ds) < 2:
            acct_warn.append(f"{benchmark_label} benchmark shown for selected period; portfolio has <2 valuation points.")
        if twr is not None and bench_twr is not None:
            excess = float(twr) - float(bench_twr)

        display_name = acct_label_by_conn.get(pid) or str(conn.name or f"Connection {pid}")
        if include_series:
            try:
                curve: list[tuple[dt.date, float]] = []
                if vals_ds and subrets and len(subrets) == (len(vals_ds) - 1):
                    g = 1.0
                    curve = [(vals_ds[0][0], float(g))]
                    for i, rret in enumerate(subrets):
                        g *= (1.0 + float(rret))
                        curve.append((vals_ds[i + 1][0], float(g)))
                series_curves_by_portfolio[int(pid)] = curve
            except Exception:
                series_curves_by_portfolio[int(pid)] = []
        rows_out.append(
            PerformanceRow(
                portfolio_id=pid,
                portfolio_name=display_name,
                taxpayer_name=str((tp_by_conn.get(pid) or (tp.name, tp.type))[0]),
                taxpayer_type=str((tp_by_conn.get(pid) or (tp.name, tp.type))[1]),
                period_start=start_date,
                period_end=end_date,
                coverage_start=cov_start,
                coverage_end=cov_end,
                valuation_points=int(len(vals_window)),
                txn_start=txn_start,
                txn_end=txn_end,
                txn_count=txn_count,
                begin_value=begin_v,
                end_value=end_v,
                contributions=float(contrib),
                withdrawals=float(withdraw),
                net_flow=float(net_flow),
                fees=fees_out,
                withholding=withholding_out,
                other_cash_out=other_cash_out,
                total_cash_out=total_cash_out,
                gain_value=gain_value,
                irr=irr,
                xirr=irr,
                twr=twr,
                sharpe=sharpe,
                benchmark_twr=bench_twr,
                benchmark_sharpe=bench_sharpe,
                excess_twr=excess,
                excess_sharpe=(float(sharpe) - float(bench_sharpe)) if (sharpe is not None and bench_sharpe is not None) else None,
                warnings=acct_warn,
            )
        )

    combined_row = None
    if include_combined:
        # Combined row (best-effort): union of valuation dates, carry-forward missing values.
        combined_warn: list[str] = []
        baseline_portfolio_ids = [
            int(r.portfolio_id)
            for r in rows_out
            if (r.coverage_start is not None and baseline_window_start <= r.coverage_start <= baseline_window_end) and r.begin_value is not None
        ]
        excluded = [r.portfolio_name for r in rows_out if int(r.portfolio_id) not in baseline_portfolio_ids]
        if excluded:
            combined_warn.append(
                "Combined metrics exclude portfolios without a baseline snapshot near period start: " + ", ".join(excluded)
            )

        combined_vals: dict[dt.date, float] = {}
        for pid in baseline_portfolio_ids:
            vals = snap_values_by_portfolio.get(pid) or {}
            for d in vals.keys():
                combined_vals.setdefault(d, 0.0)
        if combined_vals:
            # Build per-portfolio sorted series for carry-forward.
            per_port_pts: dict[int, list[tuple[dt.date, float]]] = {}
            for pid in baseline_portfolio_ids:
                pts = sorted((snap_values_by_portfolio.get(pid) or {}).items(), key=lambda x: x[0])
                per_port_pts[pid] = pts

            dates = sorted(combined_vals.keys())
            for d in dates:
                total = 0.0
                missing = 0
                for pid, pts in per_port_pts.items():
                    v = None
                    if pts:
                        v = price_on_or_before(pts, d)  # type: ignore[arg-type]
                    if v is None:
                        missing += 1
                        continue
                    total += float(v)
                if missing:
                    combined_warn.append(f"Combined value on {d} missing {missing} portfolio(s); using carry-forward where available.")
                combined_vals[d] = float(total)

        # Choose combined begin/end anchors from the full (non-downsampled) valuation series.
        # This avoids month-end downsampling dropping important anchor points (e.g., an exact 2025-01-01 snapshot).
        combined_pts = sorted(combined_vals.items(), key=lambda x: x[0])
        combined_begin_pt: tuple[dt.date, float] | None = None
        combined_end_pt: tuple[dt.date, float] | None = None
        baseline_candidates = [p for p in combined_pts if baseline_window_start <= p[0] <= baseline_window_end]
        end_candidates = [p for p in combined_pts if end_window_start <= p[0] <= end_window_end]
        if baseline_candidates:
            before = [p for p in baseline_candidates if p[0] <= start_date]
            after = [p for p in baseline_candidates if p[0] > start_date]
            combined_begin_pt = max(before, key=lambda p: p[0]) if before else min(after, key=lambda p: p[0])
        if end_candidates:
            after = [p for p in end_candidates if p[0] >= end_date]
            before = [p for p in end_candidates if p[0] < end_date]
            combined_end_pt = min(after, key=lambda p: p[0]) if after else max(before, key=lambda p: p[0])

        combined_begin_d = combined_begin_pt[0] if combined_begin_pt else None
        combined_end_d = combined_end_pt[0] if combined_end_pt else None
        combined_begin_v = float(combined_begin_pt[1]) if combined_begin_pt else None
        combined_end_v = float(combined_end_pt[1]) if combined_end_pt else None

        combined_ds: list[tuple[dt.date, float]] = []
        if (
            combined_begin_d is not None
            and combined_end_d is not None
            and combined_begin_v is not None
            and combined_end_v is not None
            and combined_begin_d <= combined_end_d
        ):
            in_range = {d: float(v) for d, v in combined_vals.items() if combined_begin_d <= d <= combined_end_d}
            combined_ds = _downsample(in_range, frequency=frequency)
            if not combined_ds:
                combined_ds = [(combined_begin_d, float(combined_begin_v)), (combined_end_d, float(combined_end_v))]
            if combined_ds and combined_ds[0][0] != combined_begin_d:
                combined_ds.insert(0, (combined_begin_d, float(combined_begin_v)))
            if combined_ds and combined_ds[-1][0] != combined_end_d:
                combined_ds.append((combined_end_d, float(combined_end_v)))
        else:
            combined_ds = _downsample(combined_vals, frequency=frequency)
        combined_flows: list[tuple[dt.date, float]] = []
        for pid in baseline_portfolio_ids:
            combined_flows.extend(
                _transfer_flows(
                    session,
                    connection_id=pid,
                    scope=scope,
                    start_date=start_date,
                    end_date=end_date,
                    account_ids=acct_ids or None,
                    include_withholding_as_flow=bool(include_withholding),
                )
            )
        combined_twr = None
        combined_subrets: list[float] = []
        if len(combined_ds) >= 2:
            combined_twr, combined_subrets, cw = twr_from_series(values=combined_ds, flows=combined_flows)
            combined_warn.extend(cw)
        elif combined_ds:
            combined_warn.append("Combined TWR needs at least 2 valuation points in the period.")
        combined_contrib = 0.0
        combined_withdraw = 0.0
        combined_net_flow = 0.0
        if combined_flows:
            for _d, amt in combined_flows:
                a = float(amt or 0.0)
                combined_net_flow += a
                if a >= 0:
                    combined_contrib += a
                else:
                    combined_withdraw += (-a)

        combined_fees = 0.0
        combined_withholding = 0.0
        combined_other_cash_out = 0.0
        if baseline_portfolio_ids:
            fee_sum = 0.0
            wh_sum = 0.0
            other_sum = 0.0
            for r in rows_out:
                if int(r.portfolio_id) not in baseline_portfolio_ids:
                    continue
                if r.fees is not None:
                    fee_sum += float(r.fees or 0.0)
                if r.withholding is not None:
                    wh_sum += float(r.withholding or 0.0)
                if r.other_cash_out is not None:
                    other_sum += float(r.other_cash_out or 0.0)
            combined_fees = float(fee_sum)
            combined_withholding = float(wh_sum)
            combined_other_cash_out = float(other_sum)

        combined_total_cash_out = float(combined_withdraw) + float(combined_fees) + float(combined_withholding) + float(combined_other_cash_out)

        combined_gain = None
        if combined_begin_v is not None and combined_end_v is not None:
            combined_gain = float(combined_end_v) - float(combined_begin_v) - float(combined_net_flow)

        combined_irr = None
        if (
            combined_begin_d is not None
            and combined_end_d is not None
            and combined_begin_v is not None
            and combined_end_v is not None
            and combined_begin_d != combined_end_d
        ):
            cfs = [(combined_begin_d, -float(combined_begin_v))]
            for d, amt in combined_flows:
                cfs.append((d, -float(amt)))
            cfs.append((combined_end_d, float(combined_end_v)))
            combined_irr = xirr(cfs)
        elif combined_ds:
            combined_warn.append("Combined IRR/XIRR needs at least 2 valuation points in the period.")
        combined_sharpe = None
        if combined_subrets:
            combined_sharpe = sharpe_ratio(
                period_returns=combined_subrets,
                risk_free_annual=rf_annual,
                periods_per_year=12.0 if frequency == "month_end" else 252.0,
            )
        combined_bench = benchmark_period_twr
        combined_bench_sharpe = benchmark_period_sharpe
        if include_series:
            try:
                curve: list[tuple[dt.date, float]] = []
                if combined_ds and combined_subrets and len(combined_subrets) == (len(combined_ds) - 1):
                    g = 1.0
                    curve = [(combined_ds[0][0], float(g))]
                    for i, rret in enumerate(combined_subrets):
                        g *= (1.0 + float(rret))
                        curve.append((combined_ds[i + 1][0], float(g)))
                series_curves_by_portfolio[0] = curve
            except Exception:
                series_curves_by_portfolio[0] = []

        if baseline_portfolio_ids:
            combined_row = PerformanceRow(
                portfolio_id=0,
                portfolio_name="Combined",
                taxpayer_name=scope,
                taxpayer_type="—",
                period_start=start_date,
                period_end=end_date,
                coverage_start=combined_begin_d if combined_begin_d is not None else (combined_ds[0][0] if combined_ds else None),
                coverage_end=combined_end_d if combined_end_d is not None else (combined_ds[-1][0] if combined_ds else None),
                valuation_points=int(len(combined_ds)),
                txn_start=None,
                txn_end=None,
                txn_count=0,
                begin_value=float(combined_begin_v) if combined_begin_v is not None else (float(combined_ds[0][1]) if combined_ds else None),
                end_value=float(combined_end_v) if combined_end_v is not None else (float(combined_ds[-1][1]) if combined_ds else None),
                contributions=float(combined_contrib),
                withdrawals=float(combined_withdraw),
                net_flow=float(combined_net_flow),
                fees=combined_fees,
                withholding=combined_withholding,
                other_cash_out=combined_other_cash_out,
                total_cash_out=combined_total_cash_out,
                gain_value=combined_gain,
                irr=combined_irr,
                xirr=combined_irr,
                twr=combined_twr,
                sharpe=combined_sharpe,
                benchmark_twr=combined_bench,
                benchmark_sharpe=combined_bench_sharpe,
                excess_twr=(float(combined_twr) - float(combined_bench)) if (combined_twr is not None and combined_bench is not None) else None,
                excess_sharpe=(float(combined_sharpe) - float(combined_bench_sharpe)) if (combined_sharpe is not None and combined_bench_sharpe is not None) else None,
                warnings=combined_warn,
            )
            transfer_flows_by_portfolio[0] = list(combined_flows or [])
        else:
            # Leave combined blank; warnings already surfaced above.
            pass

    out = {
        "warnings": warnings,
        "rows": rows_out,
        "combined": combined_row,
        "requested_start": start_date.isoformat(),
        "requested_end": end_date.isoformat(),
        "baseline_grace_days": grace_days,
        "benchmark_label": benchmark_label,
        "benchmark_period_twr": benchmark_period_twr,
        "benchmark_period_sharpe": benchmark_period_sharpe,
        "benchmark_period_start": start_date.isoformat(),
        "benchmark_period_end": end_date.isoformat(),
        "benchmark_coverage_start": benchmark_coverage_start.isoformat() if benchmark_coverage_start else None,
        "benchmark_coverage_end": benchmark_coverage_end.isoformat() if benchmark_coverage_end else None,
        "frequency": frequency,
        "risk_free_rate_annual": rf_annual,
        "include_withholding_as_flow": bool(include_withholding),
    }
    if include_series:
        out["twr_curves"] = {int(pid): [(d.isoformat(), float(v)) for d, v in (pts or [])] for pid, pts in series_curves_by_portfolio.items()}
        out["benchmark_curve"] = [(d.isoformat(), float(v)) for d, v in (benchmark_curve or [])]
        out["transfer_flows"] = {
            int(pid): [(d.isoformat(), float(a)) for d, a in (flows or [])]
            for pid, flows in (transfer_flows_by_portfolio or {}).items()
        }
    return out
