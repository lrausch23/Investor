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
benchmarks_app = typer.Typer(help="Benchmark price cache utilities (daily candles).")
sync_app = typer.Typer(help="Run connector syncs (local/offline).")
app.add_typer(benchmarks_app, name="benchmarks")
app.add_typer(sync_app, name="sync")


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


@sync_app.command("rj")
def sync_rj_cmd(
    qfx: Path = typer.Option(..., "--qfx", exists=True, help="QFX/OFX file or directory to import"),
    connection_id: int = typer.Option(..., "--connection-id", "--account", help="RJ connection id to sync (alias: --account)"),
    mode: str = typer.Option("INCREMENTAL", help="INCREMENTAL|FULL"),
    since: str = typer.Option("", help="Optional start date override (YYYY-MM-DD)"),
    dry_run: bool = typer.Option(False, help="Parse and report counts without writing DB"),
    reprocess_files: bool = typer.Option(False, help="Reprocess already ingested file hashes (rare)"),
    actor: str = typer.Option("cli", help="Audit actor"),
):
    """
    Import Raymond James Quicken Downloads (.qfx/.ofx) files and trigger a sync.
    """
    load_dotenv()
    _check_runtime()
    from src.db.session import get_session
    from src.core.sync_runner import run_sync

    mode_u = mode.strip().upper()
    if mode_u not in {"INCREMENTAL", "FULL"}:
        raise typer.BadParameter("--mode must be INCREMENTAL or FULL")

    files: list[Path] = []
    if qfx.is_dir():
        files = sorted([p for p in qfx.glob("**/*") if p.is_file() and p.suffix.lower() in {".qfx", ".ofx"}])
    else:
        if qfx.suffix.lower() not in {".qfx", ".ofx"}:
            raise typer.BadParameter("--qfx must be a .qfx/.ofx file or a directory containing them")
        files = [qfx]
    if not files:
        raise typer.BadParameter("No .qfx/.ofx files found")

    if dry_run:
        from src.adapters.rj_offline.qfx_parser import parse_positions, parse_security_list, parse_transactions

        total_tx = 0
        total_pos = 0
        for p in files:
            txt = p.read_text(encoding="utf-8-sig", errors="ignore")
            try:
                sec = parse_security_list(txt)
            except Exception:
                sec = {}
            _asof, pos, _meta = parse_positions(txt, securities=sec)
            tx = parse_transactions(txt)
            total_pos += len(pos)
            total_tx += len(tx)
            typer.echo(f"{p.name}: txns={len(tx)} positions={len(pos)}")
        typer.echo(f"TOTAL: files={len(files)} txns={total_tx} positions={total_pos}")
        raise typer.Exit(code=0)

    with get_session() as session:
        from src.db.models import ExternalConnection

        conn = session.query(ExternalConnection).filter(ExternalConnection.id == int(connection_id)).one_or_none()
        if conn is None:
            raise typer.BadParameter(f"connection_id not found: {connection_id}")
        meta = dict(getattr(conn, "metadata_json", {}) or {})
        dd = str(meta.get("data_dir") or "").strip()
        base_dir = Path(dd).expanduser() if dd else (Path("data") / "external" / f"conn_{int(connection_id)}")
        base_dir.mkdir(parents=True, exist_ok=True)
        # Copy into the connection data dir; sync will ingest new hashes idempotently.
        for p in files:
            dest = base_dir / p.name
            if dest.exists():
                # Avoid clobbering; keep both files.
                dest = base_dir / f"{p.stem}_{p.stat().st_mtime_ns}{p.suffix}"
            dest.write_bytes(p.read_bytes())
        meta.setdefault("data_dir", str(base_dir))
        setattr(conn, "metadata_json", meta)
        session.flush()

        sd = dt.date.fromisoformat(since) if since.strip() else None
        run = run_sync(
            session,
            connection_id=int(connection_id),
            mode=mode_u,
            start_date=sd,
            end_date=None,
            actor=actor,
            reprocess_files=bool(reprocess_files),
        )
        typer.echo(json.dumps({"run_id": run.id, "status": run.status, "coverage": run.coverage_json}, default=str, indent=2))


@benchmarks_app.command("warm")
def benchmarks_warm_cmd(
    symbols: str = typer.Option("VOO,SPY,QQQ", help="Comma-separated symbols to fetch (e.g. VOO,SPY,QQQ)"),
    start: str = typer.Option("2000-01-01", help="Start date (YYYY-MM-DD)"),
    end: str = typer.Option("", help="End date (YYYY-MM-DD); defaults to today"),
    refresh: bool = typer.Option(False, help="Force refresh of missing ranges (still cache-first)"),
):
    """
    Warm the benchmark candles cache so reports can run offline.
    """
    load_dotenv()
    from src.investor.marketdata.benchmarks import BenchmarkDataClient, _as_date
    from src.investor.marketdata.config import load_marketdata_config

    s0 = _as_date(start)
    if s0 is None:
        raise typer.BadParameter("Invalid --start (expected YYYY-MM-DD)")
    e0 = _as_date(end) if end.strip() else dt.date.today()
    if e0 is None:
        raise typer.BadParameter("Invalid --end (expected YYYY-MM-DD)")
    if e0 < s0:
        raise typer.BadParameter("--end must be >= --start")

    cfg, cfg_path = load_marketdata_config()
    if cfg_path:
        typer.echo(f"Using config: {cfg_path}")
    client = BenchmarkDataClient(config=cfg.benchmarks)

    syms = [x.strip().upper() for x in symbols.split(",") if x.strip()]
    if not syms:
        raise typer.BadParameter("No symbols provided")

    for sym in syms:
        try:
            df, meta = client.get(symbol=sym, start=s0, end=e0, refresh=bool(refresh))
            typer.echo(
                f"{sym}: OK ({len(df)} rows) canonical={meta.canonical_symbol} providers={','.join(meta.used_providers)} wrote={meta.cached_rows_written}"
                + (f" warning={meta.warning}" if meta.warning else "")
            )
        except Exception as e:
            typer.echo(f"{sym}: ERROR {type(e).__name__}: {e}", err=True)


@benchmarks_app.command("status")
def benchmarks_status_cmd(
    symbols: str = typer.Option("VOO,SPY,QQQ", help="Comma-separated symbols to inspect"),
):
    """
    Show cache coverage for benchmark symbols.
    """
    load_dotenv()
    from src.investor.marketdata.benchmarks import SQLiteCacheProvider, canonicalize_symbol
    from src.investor.marketdata.config import load_marketdata_config

    cfg, cfg_path = load_marketdata_config()
    if cfg_path:
        typer.echo(f"Using config: {cfg_path}")
    cache = SQLiteCacheProvider(path=Path(cfg.benchmarks.cache.path))

    syms = [x.strip().upper() for x in symbols.split(",") if x.strip()]
    if not syms:
        raise typer.BadParameter("No symbols provided")

    for sym in syms:
        canon, req = canonicalize_symbol(sym, proxy_sp500=cfg.benchmarks.benchmark_proxy)
        if not canon:
            typer.echo(f"{sym}: invalid", err=True)
            continue
        st = cache.status(symbol=canon)
        typer.echo(
            f"{req} → {canon}: rows={st.get('rows')} range={st.get('min_date') or '—'} → {st.get('max_date') or '—'}"
        )


if __name__ == "__main__":
    app()
