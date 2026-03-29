from __future__ import annotations

import logging
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from src.app.security import SecurityHeadersMiddleware
from src.app.routes.audit import router as audit_router
from src.app.routes.cash_bills import api_router as cash_bills_api_router
from src.app.routes.cash_bills import router as cash_bills_router
from src.app.routes.dashboard import router as dashboard_router
from src.app.routes.docs import router as docs_router
from src.app.routes.holdings import router as holdings_router
from src.app.routes.imports import router as imports_router
from src.app.routes.expenses import router as expenses_router
from src.app.routes.momentum import router as momentum_router
try:
    from src.app.routes.regime import router as regime_router
    _HAS_REGIME = True
except ImportError:
    _HAS_REGIME = False
from src.app.routes.planner import router as planner_router
from src.app.routes.plans import router as plans_router
from src.app.routes.policy import router as policy_router
from src.app.routes.maintenance import router as maintenance_router
from src.app.routes.reports import router as reports_router
from src.app.routes.setup import router as setup_router
from src.app.routes.tax import router as tax_router
from src.app.routes.taxes import router as taxes_router
from src.app.routes.tax_documents import router as tax_documents_router
from src.app.routes.taxlots import router as taxlots_router
from src.app.routes.sync import router as sync_router
from src.app.routes.api_rj import router as api_rj_router
from src.app.routes.api_native import router as api_native_router
from src.app.auth import auth_status_label
from src.db.init_db import init_db
from src.regime.logging_config import setup_regime_logging
from src.utils.money import format_usd
from src.utils.time import format_local, format_local_date

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
templates.env.filters["local_dt"] = format_local
templates.env.filters["local_date"] = format_local_date
templates.env.filters["usd"] = format_usd
templates.env.globals["auth_status_label"] = auth_status_label
logger = logging.getLogger(__name__)
setup_regime_logging()


def create_app() -> FastAPI:
    app = FastAPI(title="Investor MVP", version="0.1.0", docs_url=None, redoc_url=None)
    try:
        app.add_middleware(SecurityHeadersMiddleware)
    except Exception as exc:
        logger.warning("Security middleware not loaded: %s", exc)

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
        try:
            from src.app.startup_checks import run_all_checks

            errors, warnings = run_all_checks()
            for warning in warnings:
                logger.warning("Startup check warning: %s", warning)
            if errors:
                for error in errors:
                    logger.error("Startup check FAILED: %s", error)
                logger.error("Pre-flight checks found %d error(s). Some features may not work.", len(errors))
        except Exception as exc:
            logger.warning("Startup checks unavailable: %s", exc)
        try:
            from src.regime.recovery import run_startup_recovery

            recovery = run_startup_recovery()
            if recovery["stuck_orders_found"] > 0:
                logger.warning(
                    "Startup recovery: %d stuck orders found, %d reconciled, %d expired",
                    recovery["stuck_orders_found"],
                    recovery["reconciled"],
                    recovery["expired"],
                )
        except Exception as exc:
            logger.warning("Startup recovery failed: %s", exc)
        try:
            from src.regime.config import IBKRConfig
            from src.regime.ib_connection import get_ib_backend, warm_shared_ib_backend
            from src.regime.watchdog import start_watchdog

            ibkr_config = IBKRConfig()
            if ibkr_config.live_backend:
                backend = get_ib_backend(
                    0,
                    live=True,
                    account_id=str(ibkr_config.account_id),
                    starting_cash=100000.0,
                )
                start_watchdog(
                    lambda: {"connected": bool(backend.is_connected())},
                    lambda: bool(backend.connect(str(ibkr_config.host), int(ibkr_config.port), int(ibkr_config.client_id))),
                )
                shared_ok = warm_shared_ib_backend(config=ibkr_config)
                if shared_ok:
                    logger.info("Shared IBKR backend connected for market data")
                else:
                    logger.warning("Shared IBKR backend warm-up failed - market data will use fallback providers")
        except Exception as exc:
            logger.warning("Watchdog startup skipped: %s", exc)

    app.include_router(dashboard_router)
    app.include_router(docs_router)
    app.include_router(setup_router)
    app.include_router(policy_router)
    app.include_router(holdings_router)
    app.include_router(imports_router)
    app.include_router(expenses_router)
    app.include_router(momentum_router)
    if _HAS_REGIME:
        app.include_router(regime_router)
    app.include_router(planner_router)
    app.include_router(plans_router)
    app.include_router(audit_router)
    app.include_router(cash_bills_router)
    app.include_router(cash_bills_api_router)
    app.include_router(maintenance_router)
    app.include_router(reports_router)
    app.include_router(tax_router)
    app.include_router(taxes_router)
    app.include_router(tax_documents_router)
    app.include_router(taxlots_router)
    app.include_router(sync_router)
    app.include_router(api_rj_router)
    app.include_router(api_native_router)
    return app


app = create_app()
