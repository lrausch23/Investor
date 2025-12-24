from __future__ import annotations

import csv
import io

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, Response
from sqlalchemy.orm import Session

from src.app.auth import auth_banner_message, require_actor
from src.app.db import db_session
from src.core.exports import render_plan_html_report, render_plan_trade_csv
from src.db.models import Plan
import json

router = APIRouter(prefix="/plans", tags=["plans"])


@router.get("")
def plans_list(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    plans = session.query(Plan).order_by(Plan.created_at.desc()).limit(200).all()
    from src.app.main import templates

    return templates.TemplateResponse(
        "plans.html",
        {"request": request, "actor": actor, "auth_banner": auth_banner_message(), "plans": plans},
    )


@router.get("/{plan_id}")
def plan_view(
    plan_id: int,
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    plan = session.query(Plan).filter(Plan.id == plan_id).one()
    from src.app.main import templates

    return templates.TemplateResponse(
        "plan_view.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "plan": plan,
            "goal_pretty": json.dumps(plan.goal_json, indent=2, default=str),
            "warnings_pretty": json.dumps((plan.outputs_json or {}).get("warnings", []), indent=2, default=str),
        },
    )


@router.get("/{plan_id}/report")
def plan_report(
    plan_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    plan = session.query(Plan).filter(Plan.id == plan_id).one()
    html = render_plan_html_report(plan=plan)
    return HTMLResponse(html)


@router.get("/{plan_id}/trades.csv")
def plan_trades_csv(
    plan_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    plan = session.query(Plan).filter(Plan.id == plan_id).one()
    rows = render_plan_trade_csv(plan=plan)
    out = io.StringIO()
    w = csv.DictWriter(out, fieldnames=list(rows[0].keys()) if rows else ["action", "account", "ticker", "qty", "est_price"])
    w.writeheader()
    for r in rows:
        w.writerow(r)
    return Response(
        out.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=plan_{plan_id}_trades.csv"},
    )
