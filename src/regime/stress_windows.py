from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any

from .persistence import get_setting


@dataclass(frozen=True)
class StressWindow:
    key: str
    label: str
    start: str
    end: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


DEFAULT_STRESS_WINDOWS: tuple[StressWindow, ...] = (
    StressWindow("covid_crash", "COVID Crash", "2020-02-19", "2020-04-30"),
    StressWindow("bear_2022", "2022 Bear Market", "2022-01-03", "2022-10-14"),
    StressWindow("vol_shock_aug_2024", "Aug-2024 Volatility Shock", "2024-07-31", "2024-08-19"),
    StressWindow("tariff_shock_2025", "2025 Tariff Shock", "2025-03-01", "2025-05-31"),
)


def get_stress_windows() -> list[StressWindow]:
    raw = get_setting("stress_windows")
    if not raw:
        return list(DEFAULT_STRESS_WINDOWS)
    try:
        payload = json.loads(raw)
    except Exception:
        return list(DEFAULT_STRESS_WINDOWS)
    if not isinstance(payload, list):
        return list(DEFAULT_STRESS_WINDOWS)
    windows: list[StressWindow] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        key = str(item.get("key") or "").strip()
        start = str(item.get("start") or "").strip()
        end = str(item.get("end") or "").strip()
        if not key or not start or not end:
            continue
        windows.append(
            StressWindow(
                key=key,
                label=str(item.get("label") or key).strip(),
                start=start,
                end=end,
            )
        )
    return windows or list(DEFAULT_STRESS_WINDOWS)


def stress_windows_payload(windows: list[StressWindow] | None = None) -> list[dict[str, Any]]:
    return [window.to_dict() for window in (windows if windows is not None else get_stress_windows())]
