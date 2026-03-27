"""Centralized logging configuration for the regime analysis subsystem."""
from __future__ import annotations

import logging
import os


LOG_LEVEL = os.getenv("REGIME_LOG_LEVEL", "INFO").upper()


def setup_regime_logging() -> None:
    """Configure logging for all src.regime modules."""
    regime_logger = logging.getLogger("src.regime")
    if not regime_logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s [%(name)s] %(levelname)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        regime_logger.addHandler(handler)
    regime_logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
