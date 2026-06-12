from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_agent_monitor_sections_have_help_content() -> None:
    js = _read("src/app/static/regime.js")
    help_js = _read("src/app/static/regime_help.js")
    sections = set(re.findall(r'data-section="([^"]+)"', js))
    expected = {
        "agent-health-ribbon",
        "agent-leaderboard",
        "agent-decision-funnel",
        "agent-live-activity",
        "agent-position-risk",
        "agent-model-health",
        "agent-details",
    }

    assert expected <= sections
    for section in expected | {"agent-overview"}:
        assert f'"{section}"' in help_js


def test_agent_help_buttons_are_accessible_and_keyboard_bound() -> None:
    js = _read("src/app/static/regime.js")

    assert 'aria-label="About this section"' in js
    assert 'aria-expanded="false"' in js
    assert 'event.key === "Escape"' in js
    assert 'event.key === "Enter" || event.key === " "' in js


def test_agent_detail_drawer_preserves_forensic_tables() -> None:
    js = _read("src/app/static/regime.js")

    for heading in [
        "Current Agent Activity",
        "Agent Candidate Intake",
        "Agent LLM Models",
        "LLM Model Attribution",
        "LLM Verdict Outcomes",
        "IBKR Paper Reconciliation",
        "Agent Competition",
        "Agent Portfolio Status",
        "Open Positions",
        "Recent Execution Events",
    ]:
        assert heading in js
