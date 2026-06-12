from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_FALLBACK_ALERT_THRESHOLD = 10


def _setting_int(get_setting, key: str, default: int, *, minimum: int = 1, maximum: int = 10_000) -> int:
    try:
        raw = get_setting(key)
        value = int(raw if raw not in (None, "") else default)
    except Exception:
        value = int(default)
    return max(int(minimum), min(int(maximum), value))


def record_fallback(component: str, detail: str) -> dict[str, Any]:
    """Record a decision-path fallback without letting telemetry break trading."""

    normalized_component = str(component or "unknown").strip() or "unknown"
    normalized_detail = str(detail or "").strip()
    today = datetime.now(timezone.utc).date().isoformat()
    count_key = f"decision_health:{today}:{normalized_component}:count"
    detail_key = f"decision_health:{today}:{normalized_component}:details"
    alerted_key = f"decision_health:{today}:{normalized_component}:alerted"
    try:
        from .persistence import get_setting, save_alert, set_setting

        try:
            count = int(get_setting(count_key) or "0") + 1
        except Exception:
            count = 1
        set_setting(count_key, str(count))

        details: list[str] = []
        raw_details = get_setting(detail_key)
        if raw_details:
            try:
                parsed = json.loads(raw_details)
                if isinstance(parsed, list):
                    details = [str(item) for item in parsed[-9:]]
            except Exception:
                details = []
        if normalized_detail:
            details.append(normalized_detail[:500])
            set_setting(detail_key, json.dumps(details[-10:]))

        threshold = _setting_int(
            get_setting,
            "decision_health_fallback_alert_threshold",
            DEFAULT_FALLBACK_ALERT_THRESHOLD,
            minimum=1,
            maximum=10_000,
        )
        if count > threshold and get_setting(alerted_key) != "true":
            save_alert(
                "decision_health",
                f"Decision fallback threshold exceeded: {normalized_component}",
                severity="warning",
                message=f"{normalized_component} recorded {count} fallbacks today.",
                data={"component": normalized_component, "count": count, "threshold": threshold, "detail": normalized_detail},
            )
            set_setting(alerted_key, "true")
        return {"component": normalized_component, "date": today, "count": count, "alerted": count > threshold}
    except Exception:
        logger.debug("Decision fallback telemetry failed for %s.", normalized_component, exc_info=True)
        return {"component": normalized_component, "date": today, "count": 0, "alerted": False}
