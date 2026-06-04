from __future__ import annotations

from collections import Counter
from typing import Any

from .beta_agents import BETA_AGENT_PORTFOLIOS, parse_portfolio_ids
from .persistence import get_paper_positions, get_setting, get_trade_plans, list_paper_portfolios


ACTIVE_BUY_PLAN_STATUSES = {"Approved", "Submitted", "Partially Filled"}


def _as_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on", "enabled"}:
        return True
    if normalized in {"0", "false", "no", "off", "disabled"}:
        return False
    return None


def setting_bool(key: str, default: bool = False) -> bool:
    raw = get_setting(key)
    if raw in (None, ""):
        return bool(default)
    parsed = _as_bool(raw)
    return bool(default) if parsed is None else bool(parsed)


def setting_int(key: str, default: int, *, minimum: int = 0, maximum: int | None = None) -> int:
    try:
        value = int(float(get_setting(key) or default))
    except Exception:
        value = int(default)
    value = max(int(minimum), value)
    if maximum is not None:
        value = min(int(maximum), value)
    return value


def configured_beta_portfolio_ids() -> list[int]:
    ids = parse_portfolio_ids(get_setting("regime_beta_portfolio_ids"))
    if ids:
        return ids
    ids = parse_portfolio_ids(get_setting("regime_beta_portfolio_id"))
    if ids:
        return ids
    by_name = {
        str(row.get("name") or ""): int(row["id"])
        for row in list_paper_portfolios(include_closed=False)
        if row.get("id") is not None
    }
    return [
        by_name[str(agent["name"])]
        for agent in BETA_AGENT_PORTFOLIOS
        if str(agent["name"]) in by_name
    ]


def beta_agent_labels_by_portfolio(portfolio_ids: list[int] | None = None) -> dict[int, str]:
    ids = list(portfolio_ids or configured_beta_portfolio_ids())
    labels: dict[int, str] = {}
    for index, portfolio_id in enumerate(ids):
        if index < len(BETA_AGENT_PORTFOLIOS):
            labels[int(portfolio_id)] = str(BETA_AGENT_PORTFOLIOS[index]["label"])
        else:
            labels[int(portfolio_id)] = f"Agent {index + 1}"
    return labels


def diversification_settings() -> dict[str, Any]:
    enforce_orders = setting_bool("agent_diversification_enforce_orders", False)
    return {
        "enabled": setting_bool("agent_diversification_enabled", True),
        "enforce_orders": enforce_orders,
        "enforcement_enabled": enforce_orders,
        "max_active_portfolios_per_ticker": setting_int(
            "agent_max_active_portfolios_per_ticker",
            1,
            minimum=1,
            maximum=10,
        ),
    }


def active_ticker_exposures(portfolio_ids: list[int] | None = None) -> dict[str, list[dict[str, Any]]]:
    ids = list(portfolio_ids or configured_beta_portfolio_ids())
    labels = beta_agent_labels_by_portfolio(ids)
    exposures: dict[str, list[dict[str, Any]]] = {}
    for portfolio_id in ids:
        pid = int(portfolio_id)
        label = labels.get(pid, f"Portfolio {pid}")
        for position in get_paper_positions(pid, status="Open"):
            ticker = str(position.get("ticker") or "").upper()
            quantity = float(position.get("quantity") or 0.0)
            if not ticker or quantity <= 0:
                continue
            exposures.setdefault(ticker, []).append(
                {
                    "portfolio_id": pid,
                    "agent_label": label,
                    "ticker": ticker,
                    "source": "position",
                    "status": "Open",
                    "quantity": quantity,
                    "position_id": int(position["id"]) if position.get("id") is not None else None,
                }
            )
        for plan in get_trade_plans(pid, status="all"):
            ticker = str(plan.get("ticker") or "").upper()
            status = str(plan.get("status") or "")
            quantity = float(plan.get("quantity") or 0.0)
            if (
                not ticker
                or str(plan.get("action") or "") != "Buy"
                or status not in ACTIVE_BUY_PLAN_STATUSES
                or quantity <= 0
            ):
                continue
            exposures.setdefault(ticker, []).append(
                {
                    "portfolio_id": pid,
                    "agent_label": label,
                    "ticker": ticker,
                    "source": "plan",
                    "status": status,
                    "quantity": quantity,
                    "plan_id": int(plan["id"]) if plan.get("id") is not None else None,
                }
            )
    return exposures


def active_ticker_owners(
    ticker: str,
    *,
    current_portfolio_id: int | None = None,
    portfolio_ids: list[int] | None = None,
) -> list[dict[str, Any]]:
    symbol = str(ticker or "").upper()
    if not symbol:
        return []
    entries = active_ticker_exposures(portfolio_ids).get(symbol, [])
    owners: dict[int, dict[str, Any]] = {}
    for entry in entries:
        portfolio_id = int(entry.get("portfolio_id") or 0)
        if portfolio_id <= 0 or (current_portfolio_id is not None and portfolio_id == int(current_portfolio_id)):
            continue
        owner = owners.setdefault(
            portfolio_id,
            {
                "portfolio_id": portfolio_id,
                "agent_label": entry.get("agent_label") or f"Portfolio {portfolio_id}",
                "sources": [],
                "statuses": [],
                "quantity": 0.0,
            },
        )
        source = str(entry.get("source") or "")
        status = str(entry.get("status") or "")
        if source and source not in owner["sources"]:
            owner["sources"].append(source)
        if status and status not in owner["statuses"]:
            owner["statuses"].append(status)
        owner["quantity"] = float(owner.get("quantity") or 0.0) + float(entry.get("quantity") or 0.0)
    return [owners[key] for key in sorted(owners)]


def cross_agent_overlap_summary(
    portfolio_ids: list[int] | None = None,
    *,
    max_active_portfolios_per_ticker: int | None = None,
) -> dict[str, Any]:
    ids = list(portfolio_ids or configured_beta_portfolio_ids())
    settings = diversification_settings()
    max_active = int(max_active_portfolios_per_ticker or settings["max_active_portfolios_per_ticker"])
    exposures = active_ticker_exposures(ids)
    tickers: list[dict[str, Any]] = []
    for ticker, entries in sorted(exposures.items()):
        by_portfolio: dict[int, dict[str, Any]] = {}
        status_counts: Counter[str] = Counter()
        source_counts: Counter[str] = Counter()
        for entry in entries:
            portfolio_id = int(entry.get("portfolio_id") or 0)
            if portfolio_id <= 0:
                continue
            owner = by_portfolio.setdefault(
                portfolio_id,
                {
                    "portfolio_id": portfolio_id,
                    "agent_label": entry.get("agent_label") or f"Portfolio {portfolio_id}",
                    "quantity": 0.0,
                    "sources": [],
                    "statuses": [],
                },
            )
            source = str(entry.get("source") or "")
            status = str(entry.get("status") or "")
            if source:
                source_counts[source] += 1
                if source not in owner["sources"]:
                    owner["sources"].append(source)
            if status:
                status_counts[status] += 1
                if status not in owner["statuses"]:
                    owner["statuses"].append(status)
            owner["quantity"] = float(owner.get("quantity") or 0.0) + float(entry.get("quantity") or 0.0)
        active_count = len(by_portfolio)
        if active_count <= 0:
            continue
        tickers.append(
            {
                "ticker": ticker,
                "active_portfolio_count": active_count,
                "max_active_portfolios": max_active,
                "status": "risk" if active_count > max_active else "ok",
                "portfolio_ids": sorted(by_portfolio),
                "agent_labels": [by_portfolio[key]["agent_label"] for key in sorted(by_portfolio)],
                "owners": [by_portfolio[key] for key in sorted(by_portfolio)],
                "source_counts": dict(sorted(source_counts.items())),
                "status_counts": dict(sorted(status_counts.items())),
            }
        )
    risk_tickers = [row for row in tickers if row["status"] == "risk"]
    return {
        "enabled": bool(settings["enabled"]),
        "enforce_orders": bool(settings.get("enforce_orders")),
        "enforcement_enabled": bool(settings.get("enforcement_enabled")),
        "max_active_portfolios_per_ticker": max_active,
        "ticker_count": len(tickers),
        "risk_count": len(risk_tickers),
        "tickers": tickers,
        "risk_tickers": risk_tickers,
    }
