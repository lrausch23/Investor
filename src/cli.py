from __future__ import annotations

import datetime as dt
import csv
import json
import io
from pathlib import Path
from typing import Optional

import typer
from dotenv import load_dotenv

app = typer.Typer(help="Investor MVP CLI")


def _check_runtime() -> None:
    try:
        import sqlalchemy  # noqa: F401
    except Exception as e:
        typer.echo(
            "Runtime dependency error: SQLAlchemy failed to import.\n"
            "If you're using system Python 3.13, create a venv (prefer Python 3.11/3.12) and run:\n"
            "  python -m venv .venv\n"
            "  source .venv/bin/activate\n"
            "  pip install -r requirements.txt\n\n"
            f"Original error: {type(e).__name__}: {e}",
            err=True,
        )
        raise typer.Exit(code=1)


def _get_active_policy(session) -> BucketPolicy:
    from src.db.models import BucketPolicy

    policy = session.query(BucketPolicy).order_by(BucketPolicy.effective_date.desc()).first()
    if policy is None:
        raise typer.Exit(code=2)
    return policy


@app.command("import-csv")
def import_csv_cmd(
    kind: str = typer.Option(..., help="lots|cash_balances|income_events|transactions|securities"),
    path: Path = typer.Option(..., exists=True, dir_okay=False),
    actor: str = typer.Option("cli", help="Audit actor"),
    note: str = typer.Option("", help="Audit note"),
):
    load_dotenv()
    _check_runtime()
    from src.db.session import get_session
    from src.importers.csv_import import import_csv

    with get_session() as session:
        result = import_csv(session=session, kind=kind, content=path.read_text(), actor=actor, note=note)
        session.commit()
        typer.echo(json.dumps(result, indent=2))


@app.command("run-planner")
def run_planner_cmd(
    goal: str = typer.Option("rebalance", help="rebalance|raise_cash|reduce_alpha|harvest_losses"),
    scope: str = typer.Option("BOTH", help="TRUST|PERSONAL|BOTH"),
    cash_amount: float = typer.Option(0.0, help="Used for raise_cash"),
    harvest_loss_target: float = typer.Option(0.0, help="Used for harvest_losses"),
    config_json: Optional[str] = typer.Option(None, help="PlannerConfig JSON override"),
    save: bool = typer.Option(False, help="Save the plan to DB (DRAFT)"),
    actor: str = typer.Option("cli", help="Audit actor"),
):
    load_dotenv()
    _check_runtime()
    from src.core.trade_planner import PlannerConfig, plan_trades
    from src.db.audit import log_change
    from src.db.models import Plan
    from src.db.session import get_session

    with get_session() as session:
        policy = _get_active_policy(session)
        cfg = PlannerConfig()
        if config_json:
            cfg = PlannerConfig.model_validate(json.loads(config_json))
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
        typer.echo(json.dumps(result.outputs_json, indent=2, default=str))
        if save:
            plan = Plan(
                policy_id=policy.id,
                taxpayer_scope=scope,
                goal_json=result.goal_json,
                inputs_json=result.inputs_json,
                outputs_json=result.outputs_json,
                status="DRAFT",
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
                new={"id": plan.id, "status": plan.status},
                note="CLI save plan draft",
            )
            session.commit()
            typer.echo(f"Saved plan id={plan.id}")


@app.command("export-plan")
def export_plan_cmd(
    plan_id: int = typer.Option(...),
    out: Path = typer.Option(Path("data/exports"), help="Output directory"),
):
    load_dotenv()
    _check_runtime()
    out.mkdir(parents=True, exist_ok=True)
    from src.core.exports import render_plan_html_report, render_plan_trade_csv
    from src.db.models import Plan
    from src.db.session import get_session

    with get_session() as session:
        plan = session.query(Plan).filter(Plan.id == plan_id).one()
        csv_rows = render_plan_trade_csv(plan)
        csv_path = out / f"plan_{plan_id}_trades.csv"
        buf = io.StringIO()
        headers = list(csv_rows[0].keys()) if csv_rows else ["action", "account", "ticker", "qty", "est_price"]
        w = csv.DictWriter(buf, fieldnames=headers)
        w.writeheader()
        for r in csv_rows:
            w.writerow(r)
        csv_path.write_text(buf.getvalue())
        html_path = out / f"plan_{plan_id}_report.html"
        html_path.write_text(render_plan_html_report(plan))
        typer.echo(f"Wrote {csv_path}")
        typer.echo(f"Wrote {html_path}")


if __name__ == "__main__":
    app()
