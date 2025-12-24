from __future__ import annotations

import datetime as dt
import json

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import RedirectResponse
from fastapi import HTTPException
from sqlalchemy.orm import Session

from src.app.auth import auth_banner_message, require_actor
from src.app.db import db_session
from src.app.utils import jsonable
from src.core.trade_planner import PlannerConfig, plan_trades
from src.db.audit import log_change
from src.db.models import BucketPolicy, Plan

router = APIRouter(prefix="/planner", tags=["planner"])


@router.get("")
def planner_home(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    policy = session.query(BucketPolicy).order_by(BucketPolicy.effective_date.desc()).first()
    from src.app.main import templates

    return templates.TemplateResponse(
        "planner.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "policy": policy,
            "default_assumptions": json.dumps(PlannerConfig().model_dump(), indent=2),
            "today": dt.date.today().isoformat(),
        },
    )


@router.post("")
def planner_run(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    goal: str = Form(...),
    scope: str = Form(...),
    cash_amount: float = Form(default=0.0),
    harvest_loss_target: float = Form(default=0.0),
    assumptions_json: str = Form(default=""),
):
    policy = session.query(BucketPolicy).order_by(BucketPolicy.effective_date.desc()).first()
    if policy is None:
        return RedirectResponse(url="/policy/new", status_code=303)

    cfg = PlannerConfig()
    if assumptions_json.strip():
        cfg = PlannerConfig.model_validate(json.loads(assumptions_json))

    result = plan_trades(
        session=session,
        policy_id=policy.id,
        goal={
            "type": goal,
            "cash_amount": cash_amount,
            "harvest_loss_target": harvest_loss_target,
            "as_of": dt.date.today().isoformat(),
        },
        scope=scope,
        config=cfg,
    )

    from src.app.main import templates

    return templates.TemplateResponse(
        "planner_result.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "policy": policy,
            "result": result,
            "goal": goal,
            "scope": scope,
            "assumptions_json": json.dumps(cfg.model_dump(), indent=2),
            "goal_json_str": json.dumps(result.goal_json),
            "inputs_json_str": json.dumps(result.inputs_json),
            "outputs_json_str": json.dumps(result.outputs_json),
        },
    )


@router.post("/save")
def planner_save(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    policy_id: int = Form(...),
    scope: str = Form(...),
    goal_json: str = Form(...),
    inputs_json: str = Form(...),
    outputs_json: str = Form(...),
    finalize: str = Form(default=""),
    note: str = Form(default=""),
):
    parsed_outputs = json.loads(outputs_json)
    has_overrides = any(t.get("requires_override") for t in (parsed_outputs.get("trades") or []))
    if finalize == "on" and has_overrides and not note.strip():
        raise HTTPException(status_code=400, detail="Finalizing a plan with overrides requires a reason/note.")
    status = "FINAL" if finalize == "on" else "DRAFT"
    plan = Plan(
        policy_id=policy_id,
        taxpayer_scope=scope,
        goal_json=json.loads(goal_json),
        inputs_json=json.loads(inputs_json),
        outputs_json=parsed_outputs,
        status=status,
    )
    session.add(plan)
    session.flush()
    log_change(
        session,
        actor=actor,
        action="CREATE",
        entity="Plan",
        entity_id=str(plan.id),
        old=None,
        new=jsonable({"id": plan.id, "status": plan.status}),
        note=note or ("Finalize plan" if status == "FINAL" else "Save plan draft"),
    )
    session.commit()
    return RedirectResponse(url=f"/plans/{plan.id}", status_code=303)
