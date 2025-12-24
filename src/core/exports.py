from __future__ import annotations

import json
from typing import Any

from src.db.models import Plan


def render_plan_trade_csv(plan: Plan) -> list[dict[str, Any]]:
    trades = (plan.outputs_json or {}).get("trades", [])
    rows: list[dict[str, Any]] = []
    for t in trades:
        rows.append(
            {
                "action": t.get("action"),
                "account": t.get("account_name") or t.get("account_id"),
                "ticker": t.get("ticker"),
                "qty": t.get("qty"),
                "est_price": t.get("est_price"),
                "est_value": t.get("est_value"),
                "bucket": t.get("bucket_code"),
                "rationale": t.get("rationale"),
            }
        )
    return rows


def render_plan_html_report(plan: Plan) -> str:
    outputs = plan.outputs_json or {}
    trades = outputs.get("trades", [])
    picks = outputs.get("lot_picks", [])
    warnings = outputs.get("warnings", [])
    tax = outputs.get("tax_impact", {})
    drift = outputs.get("post_trade_drift", {})

    def _pre(obj: Any) -> str:
        return "<pre>" + json.dumps(obj, indent=2, default=str) + "</pre>"

    parts = [
        "<html><head><meta charset='utf-8'/>"
        "<title>Plan Report</title>"
        "<style>body{font-family:system-ui;margin:24px} table{border-collapse:collapse;width:100%} th,td{border:1px solid #ddd;padding:6px}</style>"
        "</head><body>",
        f"<h1>Plan #{plan.id} — {plan.status}</h1>",
        f"<p>Created: {plan.created_at} — Scope: {plan.taxpayer_scope} — Policy: {plan.policy_id}</p>",
        "<h2>Goal</h2>",
        _pre(plan.goal_json),
        "<h2>Warnings</h2>",
        _pre(warnings),
        "<h2>Proposed Trades</h2>",
        "<table><tr><th>Action</th><th>Account</th><th>Ticker</th><th>Qty</th><th>Est Price</th><th>Est Value</th><th>Rationale</th></tr>",
    ]
    for t in trades:
        parts.append(
            "<tr>"
            f"<td>{t.get('action')}</td>"
            f"<td>{t.get('account_name') or t.get('account_id')}</td>"
            f"<td>{t.get('ticker')}</td>"
            f"<td>{t.get('qty')}</td>"
            f"<td>{t.get('est_price')}</td>"
            f"<td>{t.get('est_value')}</td>"
            f"<td>{t.get('rationale')}</td>"
            "</tr>"
        )
    parts.append("</table>")

    parts.append("<h2>Lot Picks (Sells)</h2>")
    parts.append(
        "<table><tr><th>Ticker</th><th>Lot</th><th>Acq</th><th>Qty</th><th>Basis</th><th>Unrealized</th><th>Term</th><th>Wash</th></tr>"
    )
    for p in picks:
        parts.append(
            "<tr>"
            f"<td>{p.get('ticker')}</td>"
            f"<td>{p.get('lot_id')}</td>"
            f"<td>{p.get('acquisition_date')}</td>"
            f"<td>{p.get('qty')}</td>"
            f"<td>{p.get('basis_allocated')}</td>"
            f"<td>{p.get('unrealized')}</td>"
            f"<td>{p.get('term')}</td>"
            f"<td>{p.get('wash_risk')}</td>"
            "</tr>"
        )
    parts.append("</table>")

    parts.append("<h2>Tax Impact (Projected)</h2>")
    parts.append(_pre(tax))
    parts.append("<h2>Post-Trade Drift (Projected)</h2>")
    parts.append(_pre(drift))
    parts.append("<h2>Raw Inputs / Outputs</h2>")
    parts.append("<h3>Inputs</h3>")
    parts.append(_pre(plan.inputs_json))
    parts.append("<h3>Outputs</h3>")
    parts.append(_pre(plan.outputs_json))
    parts.append("</body></html>")
    return "\n".join(parts)

