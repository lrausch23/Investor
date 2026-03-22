from __future__ import annotations

import datetime as dt
import os
from dataclasses import asdict, is_dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from src.core.dashboard_service import build_dashboard, parse_scope
from src.core.external_holdings import build_holdings_view
from src.db.models import BucketAssignment, BucketPolicy
from src.investor.expenses.reports import category_summary, merchants_by_spend, monthly_summary


def _to_plain(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    if isinstance(value, list):
        return [_to_plain(v) for v in value]
    if isinstance(value, tuple):
        return [_to_plain(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _to_plain(v) for k, v in value.items()}
    if is_dataclass(value):
        return _to_plain(asdict(value))
    if hasattr(value, "model_dump"):
        return _to_plain(value.model_dump())
    if hasattr(value, "dict"):
        return _to_plain(value.dict())
    if hasattr(value, "__dict__"):
        return _to_plain(vars(value))
    return str(value)


def _safe_float(value: Any, fallback: float = 0.0) -> float:
    try:
        if value is None:
            return fallback
        return float(value)
    except Exception:
        return fallback


def _compute_health_score(
    *,
    bucket_statuses: list[str],
    wash_flagged_count: int,
    cash_buffer_pct: float,
    net_tax_due: float,
    sync_problem_count: int,
) -> int:
    score = 100.0
    reds = sum(1 for s in bucket_statuses if str(s).upper() == "RED")
    yellows = sum(1 for s in bucket_statuses if str(s).upper() == "YELLOW")

    score -= reds * 13
    score -= yellows * 5
    score -= min(20.0, wash_flagged_count * 4.0)
    if cash_buffer_pct < 0.12:
        score -= 10
    if net_tax_due > 0:
        score -= min(18.0, (net_tax_due / 75_000.0) * 18.0)
    score -= min(12.0, sync_problem_count * 4.0)

    return max(0, min(99, int(round(score))))


def build_native_snapshot(
    session: Session,
    *,
    scope: str = "household",
    as_of: dt.date | None = None,
    prices_dir: Path | None = None,
) -> dict[str, Any]:
    scoped = parse_scope(scope)
    today = as_of or dt.date.today()
    prices = Path(prices_dir) if prices_dir is not None else Path("./data/prices")

    dashboard = build_dashboard(session=session, scope=scoped, as_of=today)
    holdings = build_holdings_view(
        session,
        scope=scoped,
        account_id=None,
        today=today,
        prices_dir=prices,
    )

    policy = session.query(BucketPolicy).order_by(BucketPolicy.effective_date.desc()).first()
    assignment_by_symbol: dict[str, str] = {}
    if policy is not None:
        rows = session.query(BucketAssignment).filter(BucketAssignment.policy_id == policy.id).all()
        assignment_by_symbol = {str(r.ticker).upper(): str(r.bucket_code).upper() for r in rows}

    drift_rows = []
    bucket_statuses: list[str] = []
    if dashboard.drift is not None:
        for row in dashboard.drift.bucket_rows:
            row_value = _to_plain(row)
            code = str(row_value.get("code") or "").upper()
            target_pct = _safe_float(row_value.get("target_pct"))
            total_value = _safe_float(getattr(dashboard.drift, "total_value", 0.0))
            target_value = target_pct * total_value
            drift_rows.append(
                {
                    "code": code,
                    "name": str(row_value.get("name") or code),
                    "min_pct": _safe_float(row_value.get("min_pct")),
                    "target_pct": target_pct,
                    "max_pct": _safe_float(row_value.get("max_pct")),
                    "current_value": _safe_float(row_value.get("value")),
                    "target_value": target_value,
                    "actual_pct": _safe_float(row_value.get("actual_pct")),
                    "status": str(row_value.get("traffic_light") or "").upper(),
                    "reason": str(row_value.get("reason") or ""),
                }
            )
            bucket_statuses.append(str(row_value.get("traffic_light") or "").upper())

    tax_rows = []
    tax_total_net = 0.0
    if dashboard.tax is not None:
        tax_data = _to_plain(dashboard.tax)
        for row in tax_data.get("rows", []):
            est = _safe_float(row.get("estimated_tax"))
            net = _safe_float(row.get("net_tax_due"))
            tax_total_net += net
            tax_rows.append(
                {
                    "taxpayer": str(row.get("taxpayer") or "Unknown"),
                    "st_gains": _safe_float(row.get("st_gains")),
                    "lt_gains": _safe_float(row.get("lt_gains")),
                    "income": _safe_float(row.get("income")),
                    "withholding": _safe_float(row.get("withholding")),
                    "estimated_tax": est,
                    "net_tax_due": net,
                }
            )

    holdings_rows = []
    for idx, pos in enumerate(holdings.positions):
        symbol = str(pos.symbol or "").upper()
        bucket = assignment_by_symbol.get(symbol, "B1" if symbol.startswith("CASH") or symbol == "USD" else "UNASSIGNED")
        qty = _safe_float(pos.qty)
        price = _safe_float(pos.latest_price)
        market_value = _safe_float(pos.market_value)
        cost_basis = _safe_float(pos.cost_basis_total)
        pnl = _safe_float(pos.pnl_amount)
        pnl_pct = _safe_float(pos.pnl_pct)

        if price <= 0 and qty > 0 and market_value > 0:
            price = market_value / qty

        wash_status = "UNKNOWN"
        wash_safe_date = None
        if pos.wash_safe_exit_date is not None:
            wash_safe_date = pos.wash_safe_exit_date.isoformat()
            wash_status = "SAFE" if pos.wash_safe_exit_date <= today else "RISK"

        term = "N/A"
        if pos.entered_date is not None:
            term = "LT" if (today - pos.entered_date).days >= 365 else "ST"

        holdings_rows.append(
            {
                "id": f"{pos.account_id or 0}-{symbol}-{idx}",
                "account_id": (int(pos.account_id) if pos.account_id is not None else None),
                "account": str(pos.account_name or "Unknown account"),
                "taxpayer": str(pos.taxpayer_type or "Unknown"),
                "symbol": symbol,
                "bucket": bucket,
                "quantity": qty,
                "price": price,
                "market_value": market_value,
                "cost_basis": cost_basis,
                "pnl": pnl,
                "pnl_pct": pnl_pct,
                "term": term,
                "wash_status": wash_status,
                "wash_safe_date": wash_safe_date,
            }
        )

    holdings_rows.sort(key=lambda row: float(row.get("market_value") or 0.0), reverse=True)

    if not drift_rows:
        default_targets = {
            "B1": 0.18,
            "B2": 0.24,
            "B3": 0.38,
            "B4": 0.10,
        }
        default_names = {
            "B1": "Liquidity",
            "B2": "Defensive",
            "B3": "Growth",
            "B4": "Alpha",
        }
        totals_by_bucket: dict[str, float] = {k: 0.0 for k in default_targets}
        for row in holdings_rows:
            b = str(row.get("bucket") or "").upper()
            if b in totals_by_bucket:
                totals_by_bucket[b] += _safe_float(row.get("market_value"))

        fallback_total = sum(totals_by_bucket.values())
        fallback_total = fallback_total if fallback_total > 0 else _safe_float(holdings.total_value, 1.0)
        for code, target_pct in default_targets.items():
            current_value = totals_by_bucket.get(code, 0.0)
            actual_pct = (current_value / fallback_total) if fallback_total > 0 else 0.0
            min_pct = max(0.0, target_pct - 0.06)
            max_pct = min(1.0, target_pct + 0.06)
            if actual_pct < min_pct or actual_pct > max_pct:
                status = "RED"
                reason = "Outside synthetic band"
            elif abs(actual_pct - target_pct) > 0.02:
                status = "YELLOW"
                reason = "Off synthetic target"
            else:
                status = "GREEN"
                reason = "Near synthetic target"
            drift_rows.append(
                {
                    "code": code,
                    "name": default_names[code],
                    "min_pct": min_pct,
                    "target_pct": target_pct,
                    "max_pct": max_pct,
                    "current_value": current_value,
                    "target_value": target_pct * fallback_total,
                    "actual_pct": actual_pct,
                    "status": status,
                    "reason": reason,
                }
            )
            bucket_statuses.append(status)

    planner_preview = {
        "st_sensitivity": "",
        "notes": [],
        "sells": [],
        "buys": [],
    }
    if dashboard.preview is not None:
        preview = _to_plain(dashboard.preview)
        planner_preview = {
            "st_sensitivity": str(preview.get("st_sensitivity") or ""),
            "notes": [str(n) for n in preview.get("notes", [])],
            "sells": [
                {
                    "bucket": str(r.get("bucket") or ""),
                    "current_value": _safe_float(r.get("current_value")),
                    "target_value": _safe_float(r.get("target_value")),
                    "delta": _safe_float(r.get("delta")),
                }
                for r in preview.get("sells", [])
            ],
            "buys": [
                {
                    "bucket": str(r.get("bucket") or ""),
                    "current_value": _safe_float(r.get("current_value")),
                    "target_value": _safe_float(r.get("target_value")),
                    "delta": _safe_float(r.get("delta")),
                }
                for r in preview.get("buys", [])
            ],
        }

    expenses_category = category_summary(session=session, year=today.year, month=today.month)
    expenses_merchants = merchants_by_spend(session=session, year=today.year, month=today.month, limit=20)
    expenses_monthly = monthly_summary(session=session, year=today.year, month=today.month)

    category_rows = [
        {
            "category": str(r.key),
            "spend": _safe_float(r.spend),
            "txn_count": int(r.txn_count or 0),
        }
        for r in expenses_category.rows
        if _safe_float(r.spend) > 0
    ]
    merchant_rows = [
        {
            "merchant": str(r.merchant),
            "spend": _safe_float(r.spend),
            "txn_count": int(r.txn_count or 0),
            "category": str(r.category or "Unknown"),
        }
        for r in expenses_merchants
        if _safe_float(r.spend) > 0
    ]
    monthly_net = sum(_safe_float(r.net) for r in expenses_monthly.rows)

    alerts: list[dict[str, str]] = []
    if dashboard.partial_dataset_warning:
        alerts.append(
            {
                "id": "partial-dataset",
                "severity": "high",
                "title": "Partial dataset detected",
                "detail": str(dashboard.partial_dataset_warning),
                "context": "Coverage",
            }
        )

    if dashboard.drift is not None:
        for idx, warning in enumerate(dashboard.drift.warnings or []):
            alerts.append(
                {
                    "id": f"drift-warning-{idx}",
                    "severity": "medium",
                    "title": "Policy warning",
                    "detail": str(warning),
                    "context": "Bucket policy",
                }
            )

    wash_data = _to_plain(dashboard.wash)
    wash_flagged_count = int(wash_data.get("flagged_count") or 0)
    wash_message = str(wash_data.get("message") or "")
    if wash_message:
        alerts.append(
            {
                "id": "wash-summary",
                "severity": "high" if wash_flagged_count > 0 else "low",
                "title": "Wash-sale risk summary",
                "detail": wash_message,
                "context": "Tax risk",
            }
        )

    sync_connections = []
    sync_problem_count = 0
    for conn in dashboard.sync_connections or []:
        run_status = str(conn.get("last_run_status") or "UNKNOWN")
        if run_status.upper() not in {"SUCCESS", "UNKNOWN"}:
            sync_problem_count += 1
        sync_connections.append(
            {
                "id": int(conn.get("id") or 0),
                "name": str(conn.get("name") or ""),
                "provider": str(conn.get("provider") or ""),
                "connector": str(conn.get("connector") or ""),
                "coverage_status": str(conn.get("coverage_status") or ""),
                "last_run_status": run_status,
                "last_successful_sync_at": conn.get("last_successful_sync_at"),
                "holdings_last_as_of": conn.get("holdings_last_asof"),
                "broker_closed_lot_ytd_count": int(conn.get("broker_closed_lot_ytd_count") or 0),
                "broker_wash_ytd_count": int(conn.get("broker_wash_ytd_count") or 0),
            }
        )

    total_value = _safe_float(getattr(dashboard.drift, "total_value", None), _safe_float(holdings.total_value))
    cash_buffer_pct = (_safe_float(holdings.cash_total) / total_value) if total_value > 0 else 0.0
    weighted_er = 0.0
    if dashboard.fees is not None:
        fee_data = _to_plain(dashboard.fees)
        fee_rows = fee_data.get("rows") or []
        if fee_rows:
            weighted_er = _safe_float(fee_rows[0].get("weighted_expense_ratio"))

    health_score = _compute_health_score(
        bucket_statuses=bucket_statuses,
        wash_flagged_count=wash_flagged_count,
        cash_buffer_pct=cash_buffer_pct,
        net_tax_due=tax_total_net,
        sync_problem_count=sync_problem_count,
    )

    ytd_return_pct = _safe_float(holdings.ytd_return_pct)

    return {
        "version": 1,
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "source": {
            "db_path": os.environ.get("DATABASE_URL", "sqlite:///./data/investor.db"),
            "policy_name": getattr(policy, "name", "(none)"),
            "notes": "Generated from Investor core native snapshot builder",
        },
        "scope": scoped,
        "as_of": today.isoformat(),
        "health_score": health_score,
        "kpis": {
            "total_value": total_value,
            "ytd_return_pct": ytd_return_pct,
            "cash_buffer_pct": cash_buffer_pct,
            "net_tax_due": tax_total_net,
            "weighted_expense_ratio": weighted_er,
        },
        "alerts": alerts,
        "buckets": drift_rows,
        "holdings": holdings_rows,
        "planner": planner_preview,
        "tax_rows": tax_rows,
        "expenses": {
            "monthly_net": monthly_net,
            "category_rows": category_rows,
            "merchant_rows": merchant_rows,
        },
        "sync_connections": sync_connections,
    }
