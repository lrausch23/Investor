from __future__ import annotations

from typing import Any, Callable

from .beta_agents import BETA_AGENT_PORTFOLIOS, parse_portfolio_ids
from .persistence import delete_setting, get_setting, set_setting

FRONTIER_PROVIDERS = ("auto", "openai", "gemini", "claude", "ollama", "best")
SPECIFIC_FRONTIER_PROVIDERS = {"openai", "gemini", "claude", "ollama"}


def normalize_frontier_provider(provider: Any) -> str:
    value = str(provider or "auto").strip().lower() or "auto"
    return value if value in FRONTIER_PROVIDERS else "auto"


def normalize_agent_key(agent_key: Any) -> str:
    value = str(agent_key or "").strip().lower().replace(" ", "_").replace("-", "_")
    cleaned = "".join(ch for ch in value if ch.isalnum() or ch == "_")
    return cleaned or "default"


def agent_key_for_portfolio_id(
    portfolio_id: int | None,
    *,
    get_setting_fn: Callable[[str], Any] = get_setting,
) -> str:
    if portfolio_id is None:
        return ""
    portfolio_ids = parse_portfolio_ids(get_setting_fn("regime_beta_portfolio_ids"))
    try:
        index = portfolio_ids.index(int(portfolio_id))
    except ValueError:
        return f"portfolio_{int(portfolio_id)}"
    if 0 <= index < len(BETA_AGENT_PORTFOLIOS):
        return normalize_agent_key(BETA_AGENT_PORTFOLIOS[index]["key"])
    return f"agent_{index + 1}"


def _setting_key(agent_key: str, field: str) -> str:
    return f"agent_frontier_{field}_{normalize_agent_key(agent_key)}"


def get_agent_frontier_config(
    *,
    agent_key: Any = "",
    portfolio_id: int | None = None,
    get_setting_fn: Callable[[str], Any] = get_setting,
) -> dict[str, Any]:
    resolved_key = normalize_agent_key(agent_key or agent_key_for_portfolio_id(portfolio_id, get_setting_fn=get_setting_fn))
    global_provider = normalize_frontier_provider(get_setting_fn("frontier_provider"))
    global_model = str(get_setting_fn("frontier_model") or "").strip()
    agent_provider_raw = get_setting_fn(_setting_key(resolved_key, "provider"))
    provider = normalize_frontier_provider(agent_provider_raw if agent_provider_raw not in (None, "") else global_provider)
    agent_model_raw = get_setting_fn(_setting_key(resolved_key, "model"))
    if agent_model_raw is None and provider == global_provider:
        model = global_model
        inherits_model = True
    else:
        model = str(agent_model_raw or "").strip()
        inherits_model = False
    return {
        "agent_key": resolved_key,
        "portfolio_id": int(portfolio_id) if portfolio_id is not None else None,
        "provider": provider,
        "model": model,
        "inherits_global_provider": agent_provider_raw in (None, ""),
        "inherits_global_model": inherits_model,
        "specific_provider": provider in SPECIFIC_FRONTIER_PROVIDERS,
    }


def set_agent_frontier_config(
    agent_key: Any,
    *,
    provider: Any,
    model: Any = "",
    get_setting_fn: Callable[[str], Any] = get_setting,
    set_setting_fn: Callable[[str, str], Any] = set_setting,
    delete_setting_fn: Callable[[str], Any] = delete_setting,
) -> dict[str, Any]:
    key = normalize_agent_key(agent_key)
    provider_key = normalize_frontier_provider(provider)
    model_value = str(model or "").strip()
    set_setting_fn(_setting_key(key, "provider"), provider_key)
    if model_value:
        set_setting_fn(_setting_key(key, "model"), model_value)
    else:
        delete_setting_fn(_setting_key(key, "model"))
    return get_agent_frontier_config(agent_key=key, get_setting_fn=get_setting_fn)


def agent_frontier_settings_rows(
    *,
    portfolio_ids: list[int] | None = None,
    get_setting_fn: Callable[[str], Any] = get_setting,
    configured_model_fn: Callable[..., str] | None = None,
) -> list[dict[str, Any]]:
    ids = list(portfolio_ids or parse_portfolio_ids(get_setting_fn("regime_beta_portfolio_ids")))
    rows: list[dict[str, Any]] = []
    for index, agent in enumerate(BETA_AGENT_PORTFOLIOS):
        portfolio_id = ids[index] if index < len(ids) else None
        config = get_agent_frontier_config(
            agent_key=agent["key"],
            portfolio_id=portfolio_id,
            get_setting_fn=get_setting_fn,
        )
        configured_model = ""
        if configured_model_fn is not None:
            try:
                configured_model = configured_model_fn(config["provider"], config.get("model") or None)
            except TypeError:
                configured_model = configured_model_fn(config["provider"])
            except Exception:
                configured_model = ""
        rows.append(
            {
                **config,
                "label": agent["label"],
                "name": agent["name"],
                "role": agent["role"],
                "configured_model": configured_model,
                "providers": list(FRONTIER_PROVIDERS),
            }
        )
    return rows
