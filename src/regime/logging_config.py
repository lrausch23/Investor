"""Centralized logging configuration for the regime analysis subsystem."""
from __future__ import annotations

import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path


LOG_LEVEL = os.getenv("REGIME_LOG_LEVEL", "INFO").upper()


def _default_log_dir() -> str:
    configured = os.getenv("HMM_DATA_DIR")
    if configured:
        return str(Path(configured).resolve() / "logs")
    return str((Path(__file__).resolve().parents[2] / "data" / "regime" / "logs").resolve())


def setup_regime_logging(
    log_dir: str | None = None,
    max_bytes: int = 10 * 1024 * 1024,
    backup_count: int = 5,
) -> None:
    """Configure logging for app/regime modules with console + rotating file output."""
    log_path = Path(log_dir or _default_log_dir())
    log_path.mkdir(parents=True, exist_ok=True)

    regime_logger = logging.getLogger("src.regime")
    if not regime_logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s [%(name)s] %(levelname)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        regime_logger.addHandler(handler)
    root_logger = logging.getLogger()
    investor_log = log_path / "investor.log"
    if not any(
        isinstance(handler, RotatingFileHandler) and Path(getattr(handler, "baseFilename", "")) == investor_log
        for handler in root_logger.handlers
    ):
        file_handler = RotatingFileHandler(
            str(investor_log),
            maxBytes=max_bytes,
            backupCount=backup_count,
        )
        file_handler.setFormatter(
            logging.Formatter(
                "%(asctime)s [%(name)s] %(levelname)s: %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        root_logger.addHandler(file_handler)
    regime_logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
