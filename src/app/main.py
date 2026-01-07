from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from src.app.routes.audit import router as audit_router
from src.app.routes.dashboard import router as dashboard_router
from src.app.routes.holdings import router as holdings_router
from src.app.routes.imports import router as imports_router
from src.app.routes.expenses import router as expenses_router
from src.app.routes.planner import router as planner_router
from src.app.routes.plans import router as plans_router
from src.app.routes.policy import router as policy_router
from src.app.routes.reports import router as reports_router
from src.app.routes.setup import router as setup_router
from src.app.routes.tax import router as tax_router
from src.app.routes.taxlots import router as taxlots_router
from src.app.routes.sync import router as sync_router
from src.db.init_db import init_db
from src.utils.time import format_local, format_local_date
from src.utils.money import format_usd


load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
templates.env.filters["local_dt"] = format_local
templates.env.filters["local_date"] = format_local_date
templates.env.filters["usd"] = format_usd


def create_app() -> FastAPI:
    app = FastAPI(title="Investor MVP", version="0.1.0")

    static_dir = BASE_DIR / "static"
    static_dir.mkdir(parents=True, exist_ok=True)
    css_path = static_dir / "app.css"
    try:
        templates.env.globals["static_version"] = str(int(css_path.stat().st_mtime))
    except Exception:
        templates.env.globals["static_version"] = "0"
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    @app.on_event("startup")
    def _startup() -> None:
        Path("data").mkdir(parents=True, exist_ok=True)
        init_db()

    app.include_router(dashboard_router)
    app.include_router(setup_router)
    app.include_router(policy_router)
    app.include_router(holdings_router)
    app.include_router(imports_router)
    app.include_router(expenses_router)
    app.include_router(planner_router)
    app.include_router(plans_router)
    app.include_router(audit_router)
    app.include_router(reports_router)
    app.include_router(tax_router)
    app.include_router(taxlots_router)
    app.include_router(sync_router)
    return app


app = create_app()
