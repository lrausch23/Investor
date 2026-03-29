from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any

import yfinance as yf

from .persistence import get_setting, save_alert, set_setting

logger = logging.getLogger(__name__)

DEFAULT_VIX_FREEZE_THRESHOLD = 35.0
DEFAULT_VIX_RESUME_THRESHOLD = 30.0
VIX_CACHE_TTL_SECONDS = 300

_vix_cache: dict[str, Any] = {"value": None, "fetched_at": 0.0}


def fetch_current_vix() -> float | None:
    now = time.time()
    if _vix_cache["value"] is not None and (now - float(_vix_cache["fetched_at"] or 0.0)) < VIX_CACHE_TTL_SECONDS:
        return float(_vix_cache["value"])
    try:
        ticker = yf.Ticker("^VIX")
        hist = ticker.history(period="1d", interval="1m")
        if hist is not None and not hist.empty:
            value = float(hist["Close"].iloc[-1])
            _vix_cache.update({"value": value, "fetched_at": now})
            return value
        hist_daily = ticker.history(period="5d")
        if hist_daily is not None and not hist_daily.empty:
            value = float(hist_daily["Close"].iloc[-1])
            _vix_cache.update({"value": value, "fetched_at": now})
            return value
    except Exception as exc:
        logger.warning("VIX fetch failed: %s", exc)
    return float(_vix_cache["value"]) if _vix_cache["value"] is not None else None


def is_vix_frozen() -> bool:
    return get_setting("vix_freeze_active") == "true"


def get_vix_freeze_threshold() -> float:
    raw = get_setting("vix_freeze_threshold")
    try:
        return max(10.0, float(raw)) if raw is not None else DEFAULT_VIX_FREEZE_THRESHOLD
    except (TypeError, ValueError):
        return DEFAULT_VIX_FREEZE_THRESHOLD


def get_vix_resume_threshold() -> float:
    raw = get_setting("vix_resume_threshold")
    try:
        return max(5.0, float(raw)) if raw is not None else DEFAULT_VIX_RESUME_THRESHOLD
    except (TypeError, ValueError):
        return DEFAULT_VIX_RESUME_THRESHOLD


def check_vix_freeze() -> dict[str, Any]:
    vix = fetch_current_vix()
    freeze_threshold = get_vix_freeze_threshold()
    resume_threshold = get_vix_resume_threshold()
    frozen = is_vix_frozen()
    changed = False
    if vix is not None:
        if not frozen and vix >= freeze_threshold:
            set_setting("vix_freeze_active", "true")
            set_setting("vix_freeze_triggered_at", datetime.now(timezone.utc).isoformat())
            set_setting("vix_freeze_trigger_level", str(vix))
            save_alert(
                "vix_freeze",
                f"VIX freeze activated — VIX at {vix:.1f}",
                severity="critical",
                message=f"VIX {vix:.1f} exceeded freeze threshold {freeze_threshold:.0f}. All new Buy entries frozen.",
                data={"vix": vix, "freeze_threshold": freeze_threshold},
            )
            frozen = True
            changed = True
        elif frozen and vix < resume_threshold:
            set_setting("vix_freeze_active", "false")
            save_alert(
                "vix_resume",
                f"VIX freeze lifted — VIX at {vix:.1f}",
                severity="info",
                message=f"VIX {vix:.1f} dropped below resume threshold {resume_threshold:.0f}. Buy entries re-enabled.",
                data={"vix": vix, "resume_threshold": resume_threshold},
            )
            frozen = False
            changed = True
    return {
        "vix": vix,
        "frozen": frozen,
        "freeze_threshold": freeze_threshold,
        "resume_threshold": resume_threshold,
        "changed": changed,
        "triggered_at": get_setting("vix_freeze_triggered_at"),
        "trigger_level": get_setting("vix_freeze_trigger_level"),
    }


def manual_override_vix_freeze(unfreeze: bool) -> dict[str, Any]:
    set_setting("vix_freeze_active", "false" if unfreeze else "true")
    action = "manually lifted" if unfreeze else "manually activated"
    save_alert(
        "vix_resume" if unfreeze else "vix_freeze",
        f"VIX freeze {action}",
        severity="warning",
        message=f"User {action} VIX freeze override.",
        data={"override": action},
    )
    status = check_vix_freeze()
    status["frozen"] = not unfreeze
    return status
