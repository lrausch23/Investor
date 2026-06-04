from __future__ import annotations

from typing import Any


BETA_AGENT_PORTFOLIOS: tuple[dict[str, str], ...] = (
    {
        "key": "quant",
        "label": "Agent 1 - Quant",
        "name": "Regime Agent Beta - Agent 1 Quant",
        "role": "HMM regime, technical, ensemble, and ML signal generation.",
    },
    {
        "key": "fundamental",
        "label": "Agent 2 - Fundamental",
        "name": "Regime Agent Beta - Agent 2 Fundamental",
        "role": "Quality, moat, catalyst, and fundamental-gate review.",
    },
    {
        "key": "portfolio_tax",
        "label": "Agent 3 - Portfolio / Tax",
        "name": "Regime Agent Beta - Agent 3 Portfolio Tax",
        "role": "Sizing, anti-churn, tax-aware, and risk-budget decisions.",
    },
    {
        "key": "execution",
        "label": "Agent 4 - Execution",
        "name": "Regime Agent Beta - Agent 4 Execution",
        "role": "Guarded IBKR paper execution, quote collars, and fill tracking.",
    },
)


def parse_portfolio_ids(raw: Any) -> list[int]:
    ids: list[int] = []
    seen: set[int] = set()
    for item in str(raw or "").split(","):
        try:
            value = int(item.strip())
        except Exception:
            continue
        if value > 0 and value not in seen:
            seen.add(value)
            ids.append(value)
    return ids
