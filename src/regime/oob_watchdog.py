from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sqlite3
import time
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

from .config import DEFAULT_IBKR_CONFIG, DEFAULT_RISK_GUARDRAILS
from .logging_config import _default_log_dir
from .persistence import DB_PATH, save_alert

logger = logging.getLogger("oob_watchdog")

DEFAULT_CHECK_INTERVAL = 60
DEFAULT_DAILY_LOSS_LIMIT = float(DEFAULT_RISK_GUARDRAILS.daily_loss_limit)
DEFAULT_IB_HOST = str(DEFAULT_IBKR_CONFIG.host)
DEFAULT_IB_PORT = int(DEFAULT_IBKR_CONFIG.port)
DEFAULT_IB_CLIENT_ID = 2
MAX_HEARTBEAT_AGE_SECONDS = 300


def setup_watchdog_logging(log_dir: str | None = None) -> None:
    log_path = Path(log_dir or os.getenv("OOB_WATCHDOG_LOG_DIR") or _default_log_dir())
    log_path.mkdir(parents=True, exist_ok=True)
    logger.setLevel(logging.INFO)
    if not any(isinstance(handler, RotatingFileHandler) for handler in logger.handlers):
        handler = RotatingFileHandler(str(log_path / "watchdog.log"), maxBytes=5 * 1024 * 1024, backupCount=3)
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        logger.addHandler(handler)


def _read_setting(conn: sqlite3.Connection, key: str) -> str | None:
    try:
        row = conn.execute("SELECT value FROM regime_settings WHERE key = ?", (str(key),)).fetchone()
        return str(row[0]) if row and row[0] is not None else None
    except Exception:
        return None


def check_app_liveness(db_path: str) -> dict[str, Any]:
    result = {"alive": False, "last_heartbeat": None, "age_seconds": None, "db_accessible": False}
    try:
        conn = sqlite3.connect(str(db_path))
        try:
            result["db_accessible"] = True
            heartbeat_epoch = _read_setting(conn, "heartbeat_epoch")
            heartbeat = _read_setting(conn, "watchdog_heartbeat")
            if heartbeat_epoch not in (None, ""):
                epoch = float(heartbeat_epoch)
                result["last_heartbeat"] = datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()
                result["age_seconds"] = max(0.0, time.time() - epoch)
                result["alive"] = bool(result["age_seconds"] <= MAX_HEARTBEAT_AGE_SECONDS)
                return result
            if heartbeat:
                heartbeat_dt = datetime.fromisoformat(str(heartbeat).replace("Z", "+00:00"))
                result["last_heartbeat"] = heartbeat_dt.isoformat()
                result["age_seconds"] = max(0.0, (datetime.now(timezone.utc) - heartbeat_dt).total_seconds())
                result["alive"] = bool(result["age_seconds"] <= MAX_HEARTBEAT_AGE_SECONDS)
        finally:
            conn.close()
    except Exception as exc:
        result["error"] = str(exc)
    return result


def check_daily_pnl(db_path: str) -> dict[str, Any]:
    portfolios: list[dict[str, Any]] = []
    total_daily_pnl = 0.0
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT portfolio_id, snapshot_date, realized_pnl, unrealized_pnl
            FROM daily_snapshot
            WHERE (portfolio_id, snapshot_date) IN (
                SELECT portfolio_id, MAX(snapshot_date)
                FROM daily_snapshot
                GROUP BY portfolio_id
            )
            """
        ).fetchall()
        for row in rows:
            pnl = float(row["realized_pnl"] or 0.0) + float(row["unrealized_pnl"] or 0.0)
            portfolios.append(
                {
                    "portfolio_id": int(row["portfolio_id"]),
                    "snapshot_date": str(row["snapshot_date"]),
                    "daily_pnl": pnl,
                }
            )
            total_daily_pnl += pnl
    finally:
        conn.close()
    return {
        "total_daily_pnl": total_daily_pnl,
        "loss_limit": DEFAULT_DAILY_LOSS_LIMIT,
        "limit_breached": bool(total_daily_pnl <= -abs(DEFAULT_DAILY_LOSS_LIMIT)),
        "portfolios": portfolios,
    }


def connect_ib_direct(
    host: str = DEFAULT_IB_HOST,
    port: int = DEFAULT_IB_PORT,
    client_id: int = DEFAULT_IB_CLIENT_ID,
    timeout: int = 10,
) -> Any:
    try:
        from ib_insync import IB

        ib = IB()
        ib.connect(host, int(port), clientId=int(client_id), timeout=int(timeout))
        return ib if ib.isConnected() else None
    except Exception as exc:
        logger.warning("Direct IB connection failed: %s", exc)
        return None


def cancel_all_orders(ib) -> int:
    if ib is None:
        return 0
    cancelled = 0
    for trade in list(ib.openTrades() or []):
        try:
            ib.cancelOrder(trade.order)
            cancelled += 1
        except Exception:
            continue
    return cancelled


def flatten_all_positions(ib) -> list[dict[str, Any]]:
    if ib is None:
        return []
    from ib_insync import MarketOrder

    results: list[dict[str, Any]] = []
    for pos in list(ib.positions() or []):
        quantity = float(getattr(pos, "position", 0.0) or 0.0)
        if abs(quantity) < 0.01:
            continue
        action = "SELL" if quantity > 0 else "BUY"
        try:
            trade = ib.placeOrder(pos.contract, MarketOrder(action, abs(quantity)))
            results.append(
                {
                    "ticker": str(getattr(pos.contract, "symbol", "")),
                    "action": action,
                    "quantity": abs(quantity),
                    "order_id": int(getattr(trade.order, "orderId", 0) or 0),
                    "status": "submitted",
                }
            )
        except Exception as exc:
            results.append({"ticker": str(getattr(pos.contract, "symbol", "")), "status": "failed", "error": str(exc)})
    return results


def emergency_liquidate(
    db_path: str,
    ib_host: str = DEFAULT_IB_HOST,
    ib_port: int = DEFAULT_IB_PORT,
    ib_client_id: int = DEFAULT_IB_CLIENT_ID,
) -> dict[str, Any]:
    ib = connect_ib_direct(host=ib_host, port=ib_port, client_id=ib_client_id)
    if ib is None:
        return {"connected": False, "orders_cancelled": 0, "positions_flattened": [], "error": "Unable to connect to IB Gateway"}
    try:
        orders_cancelled = cancel_all_orders(ib)
        positions_flattened = flatten_all_positions(ib)
        save_alert(
            "execution_error",
            "Emergency liquidation executed",
            severity="critical",
            message="Out-of-band watchdog triggered liquidation.",
            data={"orders_cancelled": orders_cancelled, "positions_flattened": positions_flattened, "db_path": str(db_path)},
        )
        return {
            "connected": True,
            "orders_cancelled": orders_cancelled,
            "positions_flattened": positions_flattened,
            "error": None,
        }
    finally:
        try:
            ib.disconnect()
        except Exception:
            pass


def run_watchdog_loop(
    db_path: str,
    check_interval: int = DEFAULT_CHECK_INTERVAL,
    ib_host: str = DEFAULT_IB_HOST,
    ib_port: int = DEFAULT_IB_PORT,
    ib_client_id: int = DEFAULT_IB_CLIENT_ID,
    daily_loss_limit: float = DEFAULT_DAILY_LOSS_LIMIT,
    *,
    dry_run: bool = False,
    stop_after_one: bool = False,
) -> dict[str, Any] | None:
    while True:
        liveness = check_app_liveness(db_path)
        pnl = check_daily_pnl(db_path)
        triggered = bool(
            not liveness.get("alive")
            or float(pnl.get("total_daily_pnl") or 0.0) <= -abs(float(daily_loss_limit))
        )
        if triggered:
            reason = "app_unresponsive" if not liveness.get("alive") else "daily_loss_limit_breached"
            logger.critical("Watchdog trigger fired: %s", reason)
            save_alert(
                "execution_error",
                f"Out-of-band watchdog triggered: {reason}",
                severity="critical",
                message="Emergency liquidation condition detected.",
                data={"liveness": liveness, "pnl": pnl, "dry_run": dry_run},
            )
            result: dict[str, Any] = {"triggered": True, "reason": reason, "liveness": liveness, "pnl": pnl}
            if not dry_run:
                result["liquidation"] = emergency_liquidate(
                    db_path,
                    ib_host=ib_host,
                    ib_port=ib_port,
                    ib_client_id=ib_client_id,
                )
            if stop_after_one:
                return result
        elif stop_after_one:
            return {"triggered": False, "liveness": liveness, "pnl": pnl}
        time.sleep(max(1, int(check_interval)))


def main() -> None:
    parser = argparse.ArgumentParser(description="Out-of-band watchdog")
    parser.add_argument("--check-once", action="store_true", help="Run a single check and exit")
    parser.add_argument("--config", type=str, default=None, help="Path to JSON config file")
    parser.add_argument("--dry-run", action="store_true", help="Check only; do not liquidate")
    args = parser.parse_args()

    setup_watchdog_logging()
    config: dict[str, Any] = {}
    if args.config:
        config = json.loads(Path(args.config).read_text(encoding="utf-8"))

    should_stop = False

    def _handle_stop(signum, frame) -> None:
        del signum, frame
        nonlocal should_stop
        should_stop = True

    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    db_path = str(config.get("db_path") or DB_PATH)
    interval = int(config.get("check_interval") or os.getenv("OOB_WATCHDOG_INTERVAL", DEFAULT_CHECK_INTERVAL))
    ib_host = str(config.get("ib_host") or os.getenv("IB_HOST", DEFAULT_IB_HOST))
    ib_port = int(config.get("ib_port") or os.getenv("IB_PORT", DEFAULT_IB_PORT))
    ib_client_id = int(config.get("ib_client_id") or os.getenv("IB_CLIENT_ID", DEFAULT_IB_CLIENT_ID))
    daily_loss_limit = float(config.get("daily_loss_limit") or DEFAULT_DAILY_LOSS_LIMIT)

    if args.check_once:
        result = run_watchdog_loop(
            db_path,
            check_interval=interval,
            ib_host=ib_host,
            ib_port=ib_port,
            ib_client_id=ib_client_id,
            daily_loss_limit=daily_loss_limit,
            dry_run=args.dry_run,
            stop_after_one=True,
        )
        print(json.dumps(result, indent=2, default=str))
        return

    while not should_stop:
        run_watchdog_loop(
            db_path,
            check_interval=interval,
            ib_host=ib_host,
            ib_port=ib_port,
            ib_client_id=ib_client_id,
            daily_loss_limit=daily_loss_limit,
            dry_run=args.dry_run,
            stop_after_one=True,
        )
        time.sleep(max(1, interval))


if __name__ == "__main__":
    main()
