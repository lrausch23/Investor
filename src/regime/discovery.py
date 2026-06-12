from __future__ import annotations

from dataclasses import asdict
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, cast

import pandas as pd

from .config import DEFAULT_DISCOVERY_THRESHOLDS, DiscoveryThresholds
from .cross_sectional import (
    BetaAdjustedResult,
    VolatilityZResult,
    calculate_beta_adjusted_return,
    calculate_volatility_z_score,
    compute_peer_percentiles,
)
from .data import download_market_frame
from .hmm_engine import fit_regime_model
from .llm_layer import request_frontier_decision
from .market_data_client import get_ticker_info
from .universe import check_universe_eligibility, universe_screen_enabled
from .persistence import (
    add_ticker_to_theme,
    get_supply_chain,
    get_setting,
    get_theme,
    get_watchlist,
    get_watchlist_entry,
    get_watchlist_stats,
    get_watchlist_by_ticker,
    list_themes,
    save_supply_chain_layers,
    update_watchlist_status,
    update_watchlist_fundamental_gate,
    update_watchlist_cross_sectional,
    upsert_watchlist_candidate,
)

logger = logging.getLogger(__name__)

_CROWD_SCORE_CACHE: dict[str, tuple[float, int, dict[str, Any]]] = {}


def _prune_crowd_cache(thresholds: DiscoveryThresholds = DEFAULT_DISCOVERY_THRESHOLDS) -> None:
    now = time.time()
    stale_keys = [
        key
        for key, (timestamp, _score, _details) in _CROWD_SCORE_CACHE.items()
        if (now - float(timestamp)) > thresholds.crowd_cache_ttl_seconds
    ]
    for key in stale_keys:
        _CROWD_SCORE_CACHE.pop(key, None)
    if len(_CROWD_SCORE_CACHE) <= thresholds.crowd_cache_max_size:
        return
    overflow = len(_CROWD_SCORE_CACHE) - thresholds.crowd_cache_max_size
    oldest = sorted(_CROWD_SCORE_CACHE.items(), key=lambda item: float(item[1][0]))
    for key, _value in oldest[:overflow]:
        _CROWD_SCORE_CACHE.pop(key, None)


def _theme_existing_tickers(theme: dict[str, Any]) -> list[str]:
    return [str(item.get("ticker") or "").upper() for item in (theme.get("tickers") or []) if str(item.get("ticker") or "").strip()]


def _extract_json_list(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("layers", "results", "candidates", "items"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        return [payload]
    if isinstance(payload, str):
        try:
            decoded = json.loads(payload)
        except json.JSONDecodeError:
            return []
        return _extract_json_list(decoded)
    return []


def build_supply_chain_prompt(theme: dict) -> str:
    existing = ", ".join(_theme_existing_tickers(theme)) or "None"
    return f"""
You are an equity research analyst specializing in thematic supply-chain analysis.

Investment Theme: {theme.get("name", "")}
Theme Narrative: {theme.get("narrative", "")}
Conviction Level: {theme.get("conviction", 3)}/5
Current Holdings: {existing}

Task: Map the critical supply chain for this investment theme. Identify 5-10 distinct layers or segments that are essential for this theme to succeed. For each layer, provide:

1. layer: Short name
2. description: One sentence explaining why this layer matters to the theme
3. example_companies: 3-5 US-listed public company tickers that are key players in this layer. Exclude companies already in current holdings.

Focus on:
- Companies on the critical path
- Companies not yet widely recognized as theme beneficiaries
- US-listed equities only

Return strict JSON array:
[
  {{
    "layer": "string",
    "description": "string",
    "example_companies": "TICKER1 (Company Name), TICKER2 (Company Name)"
  }}
]
""".strip()


def generate_supply_chain(
    theme_id: int,
    *,
    frontier_enabled: bool = True,
    frontier_provider: str = "auto",
) -> list[dict]:
    theme = get_theme(theme_id)
    if not theme:
        return []
    prompt = build_supply_chain_prompt(theme)
    response = request_frontier_decision(prompt, enabled=frontier_enabled, provider=frontier_provider)
    layers = _extract_json_list(response)
    if not layers:
        logger.warning("Supply-chain generation returned no layers for theme_id=%s", theme_id)
        return get_supply_chain(theme_id)
    return save_supply_chain_layers(theme_id, layers)


def build_discovery_prompt(
    theme: dict,
    supply_chain: list[dict],
    existing_tickers: list[str],
    watchlist_tickers: list[str],
) -> str:
    supply_chain_block = "\n".join(
        f"- {item.get('layer', '')}: {item.get('description', '')} | Examples: {item.get('example_companies', '')}"
        for item in supply_chain
    ) or "- No supply-chain layers available."
    held = ", ".join(existing_tickers) or "None"
    watched = ", ".join(watchlist_tickers) or "None"
    return f"""
You are an equity research analyst identifying undiscovered investment opportunities.

Investment Theme: {theme.get("name", "")}
Theme Narrative: {theme.get("narrative", "")}
Conviction Level: {theme.get("conviction", 3)}/5

Supply Chain Map:
{supply_chain_block}

Already Held: {held}
Already on Watchlist: {watched}

Task: Identify 3-8 US-listed public companies that are on the critical path for this theme but are NOT in the "Already Held" or "Already on Watchlist" lists.

For each candidate, provide:
- ticker
- company_name
- supply_chain_layer
- rationale
- suggested_role: "Critical-Path" or "Speculative"
- crowd_assessment: 1-10

Return strict JSON array:
[
  {{
    "ticker": "string",
    "company_name": "string",
    "supply_chain_layer": "string",
    "rationale": "string",
    "suggested_role": "string",
    "crowd_assessment": 5
  }}
]
""".strip()


def compute_crowd_score(
    ticker: str,
    *,
    crowd_assessment: int | None = None,
    thresholds: DiscoveryThresholds = DEFAULT_DISCOVERY_THRESHOLDS,
) -> tuple[int, dict]:
    ticker_key = str(ticker or "").upper()
    _prune_crowd_cache(thresholds)
    cached = _CROWD_SCORE_CACHE.get(ticker_key)
    if cached is not None:
        timestamp, score, cached_details = cached
        if (time.time() - float(timestamp)) <= thresholds.crowd_cache_ttl_seconds:
            return score, dict(cached_details)
        _CROWD_SCORE_CACHE.pop(ticker_key, None)
    details: dict[str, Any] = {"ticker": ticker_key}
    info = get_ticker_info(ticker_key)

    analysts = info.get("numberOfAnalystOpinions")
    institutional_pct = info.get("heldPercentInstitutions")
    short_interest = info.get("shortPercentOfFloat")
    avg_volume = info.get("averageVolume") or info.get("averageVolume10days")
    regular_price = info.get("regularMarketPrice") or info.get("currentPrice")
    dollar_volume = float(avg_volume or 0.0) * float(regular_price or 0.0) if avg_volume and regular_price else None

    score = 0
    missing = 0
    if analysts is None:
        missing += 1
        analyst_score = None
    else:
        analysts = int(analysts)
        analyst_score = 0 if analysts <= 5 else 10 if analysts <= 15 else 20 if analysts <= 25 else 30
        score += analyst_score
    if institutional_pct is None:
        missing += 1
        institutional_score = None
    else:
        institutional_pct = float(institutional_pct) * (100.0 if float(institutional_pct) <= 1.0 else 1.0)
        institutional_score = 0 if institutional_pct < 30 else 10 if institutional_pct < 60 else 15 if institutional_pct <= 80 else 25
        score += institutional_score
    if dollar_volume is None:
        missing += 1
        volume_score = None
    else:
        volume_score = 0 if dollar_volume < 5_000_000 else 10 if dollar_volume < 50_000_000 else 15 if dollar_volume < 200_000_000 else 25
        score += volume_score
    if short_interest is None:
        missing += 1
        short_score = None
    else:
        short_interest = float(short_interest) * (100.0 if float(short_interest) <= 1.0 else 1.0)
        short_score = 0 if short_interest < 2 else 5 if short_interest < 5 else 10 if short_interest < 15 else 20
        score += short_score

    if missing >= 3:
        score = max(0, min(100, int((crowd_assessment or 5) * 10))) if crowd_assessment is not None else 50
        details["note"] = "insufficient data"
    details.update(
        {
            "analyst_coverage": analysts,
            "analyst_score": analyst_score,
            "institutional_ownership_pct": institutional_pct,
            "institutional_score": institutional_score,
            "avg_daily_dollar_volume": dollar_volume,
            "volume_score": volume_score,
            "short_interest_pct": short_interest,
            "short_interest_score": short_score,
        }
    )
    normalized_score = max(0, min(100, int(score)))
    _CROWD_SCORE_CACHE[ticker_key] = (time.time(), normalized_score, dict(details))
    _prune_crowd_cache(thresholds)
    return normalized_score, details


def _normalize_crowd_sub_scores(raw_scores: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if len(raw_scores) < 3:
        return raw_scores

    metric_keys = ("analyst_score", "institutional_score", "volume_score", "short_score")
    ranked_values: dict[str, list[float]] = {}
    for metric in metric_keys:
        values: list[float] = []
        for item in raw_scores:
            value = item.get(metric)
            if value is not None:
                values.append(float(value))
        ranked_values[metric] = values

    for item in raw_scores:
        total = 0
        normalized_details: dict[str, float | None] = {}
        for metric in metric_keys:
            value = item.get(metric)
            peers = ranked_values.get(metric) or []
            if value is None or len(peers) < 2:
                normalized_details[f"{metric}_percentile"] = None
                normalized_details[f"{metric}_normalized"] = None
                continue
            below = sum(1 for peer in peers if peer < float(value))
            equal = sum(1 for peer in peers if peer == float(value))
            percentile = ((below + 0.5 * equal) / len(peers)) * 100.0
            normalized_value = int(round((percentile / 100.0) * 25.0))
            normalized_details[f"{metric}_percentile"] = round(percentile, 2)
            normalized_details[f"{metric}_normalized"] = normalized_value
            total += normalized_value
        item["normalized_crowd_score"] = max(0, min(100, int(total)))
        item["crowd_percentiles"] = normalized_details
    return raw_scores


def build_sector_discovery_prompt(
    theme: dict,
    existing_tickers: list[str],
    watchlist_tickers: list[str],
) -> str:
    sector = str(theme.get("sector_hint") or "").strip()
    held = ", ".join(existing_tickers) or "None"
    watched = ", ".join(watchlist_tickers) or "None"
    return f"""
You are an equity research analyst identifying undiscovered investment opportunities.

Investment Theme: {theme.get("name", "")}
Theme Narrative: {theme.get("narrative", "")}
Conviction Level: {theme.get("conviction", 3)}/5
Sector / Industry Focus: {sector}

Already Held: {held}
Already on Watchlist: {watched}

Task: Identify 3-8 US-listed public companies within the "{sector}" sector or closely related industries that are strong beneficiaries of this investment theme. Focus on companies that:
- Are directly building, enabling, or benefiting from the theme narrative
- Are NOT in the "Already Held" or "Already on Watchlist" lists
- Have a market capitalization above $500 million (no micro-caps)
- Are liquid enough to trade (average daily volume > $1M)

For each candidate, provide:
- ticker: US exchange ticker symbol
- company_name: Full company name
- sector_layer: The specific sub-sector or industry niche (e.g., "Inference Infrastructure", "Foundation Models")
- rationale: One sentence explaining why this company fits the theme
- suggested_role: "Critical-Path" or "Speculative"
- crowd_assessment: 1-10 (1 = very undiscovered, 10 = widely known theme play)

Return strict JSON array:
[
  {{
    "ticker": "string",
    "company_name": "string",
    "sector_layer": "string",
    "rationale": "string",
    "suggested_role": "string",
    "crowd_assessment": 5
  }}
]
""".strip()


def _quick_regime_screen(ticker: str) -> tuple[str | None, float | None, float | None, float | None]:
    try:
        market_frame = download_market_frame(ticker=ticker, period="2y", interval="1d").frame
        regime = fit_regime_model(ticker=ticker, market_frame=market_frame, training_window=252, refit_step=21)
        price_window = market_frame.tail(126).copy()
        current_price = float(price_window["price"].iloc[-1])
        high = price_window["high"].astype(float)
        low = price_window["low"].astype(float)
        close = price_window["price"].astype(float)
        tr = pd.concat(
            [
                (high - low),
                (high - close.shift(1)).abs(),
                (low - close.shift(1)).abs(),
            ],
            axis=1,
        ).max(axis=1)
        atr = float(tr.rolling(14).mean().dropna().iloc[-1])
        if regime.latest_label == "Bear":
            return regime.latest_label, float(regime.latest_probability), current_price + 0.5 * atr, current_price + 2.0 * atr
        return regime.latest_label, float(regime.latest_probability), current_price - 0.5 * atr, current_price - 2.0 * atr
    except Exception as exc:
        logger.debug("Quick regime screen failed for %s.", ticker, exc_info=exc)
        return None, None, None, None


def _validate_ticker(ticker: str) -> bool:
    info = get_ticker_info(ticker)
    return bool(info.get("regularMarketPrice") or info.get("currentPrice") or info.get("marketCap"))


def run_discovery_scan(
    theme_id: int,
    *,
    frontier_enabled: bool = True,
    frontier_provider: str = "auto",
) -> list[dict]:
    theme = get_theme(theme_id)
    if not theme:
        return []
    existing_tickers = _theme_existing_tickers(theme)
    watchlist_tickers = [str(item.get("ticker") or "").upper() for item in get_watchlist(theme_id=theme_id, status="Watching")]
    sector_hint = str(theme.get("sector_hint") or "").strip()
    supply_chain = get_supply_chain(theme_id)
    if supply_chain:
        prompt = build_discovery_prompt(theme, supply_chain, existing_tickers, watchlist_tickers)
    elif sector_hint:
        prompt = build_sector_discovery_prompt(theme, existing_tickers, watchlist_tickers)
    else:
        supply_chain = generate_supply_chain(theme_id, frontier_enabled=frontier_enabled, frontier_provider=frontier_provider)
        prompt = build_discovery_prompt(theme, supply_chain, existing_tickers, watchlist_tickers)
    response = request_frontier_decision(prompt, enabled=frontier_enabled, provider=frontier_provider)
    candidates = _extract_json_list(response)
    if not candidates:
        logger.warning("Discovery scan returned no candidates for theme_id=%s", theme_id)
        return []
    provisional: list[dict[str, Any]] = []
    seen: set[str] = set()
    for candidate in candidates:
        ticker = str(candidate.get("ticker") or "").strip().upper()
        if not ticker or ticker in seen or ticker in existing_tickers or ticker in watchlist_tickers:
            continue
        seen.add(ticker)
        if not _validate_ticker(ticker):
            logger.warning("Skipping invalid discovery candidate %s for theme_id=%s", ticker, theme_id)
            continue
        crowd_score, crowd_details = compute_crowd_score(ticker, crowd_assessment=candidate.get("crowd_assessment"))
        regime_label, regime_probability, entry_price, stop_price = _quick_regime_screen(ticker)
        layer = str(
            candidate.get("supply_chain_layer") or candidate.get("sector_layer") or ""
        ).strip()
        provisional.append(
            {
                "theme_id": theme_id,
                "ticker": ticker,
                "company_name": str(candidate.get("company_name") or "").strip(),
                "supply_chain_layer": layer,
                "discovery_rationale": str(candidate.get("rationale") or "").strip(),
                "suggested_role": str(candidate.get("suggested_role") or "Critical-Path"),
                "suggested_entry_price": entry_price,
                "suggested_stop_price": stop_price,
                "crowd_score": crowd_score,
                "crowd_details": dict(crowd_details),
                "regime_label": regime_label,
                "regime_probability": regime_probability,
                "status": "Watching",
                "analyst_score": crowd_details.get("analyst_score"),
                "institutional_score": crowd_details.get("institutional_score"),
                "volume_score": crowd_details.get("volume_score"),
                "short_score": crowd_details.get("short_interest_score"),
            }
        )
        time.sleep(0.5)
    _normalize_crowd_sub_scores(provisional)
    results: list[dict[str, Any]] = []
    for item in provisional:
        crowd_details = dict(item.get("crowd_details") or {})
        if item.get("normalized_crowd_score") is not None:
            crowd_details["normalized_crowd_score"] = int(item["normalized_crowd_score"])
        if item.get("crowd_percentiles"):
            crowd_details["crowd_percentiles"] = item["crowd_percentiles"]
        watch = upsert_watchlist_candidate(
            int(item["theme_id"]),
            str(item["ticker"]),
            company_name=str(item.get("company_name") or ""),
            supply_chain_layer=str(item.get("supply_chain_layer") or ""),
            discovery_rationale=str(item.get("discovery_rationale") or ""),
            suggested_role=str(item.get("suggested_role") or "Critical-Path"),
            suggested_entry_price=item.get("suggested_entry_price"),
            suggested_stop_price=item.get("suggested_stop_price"),
            crowd_score=int(item.get("crowd_score") or 50),
            normalized_crowd_score=int(item["normalized_crowd_score"]) if item.get("normalized_crowd_score") is not None else None,
            crowd_details=json.dumps(crowd_details),
            regime_label=item.get("regime_label"),
            regime_probability=item.get("regime_probability"),
            status="Watching",
        )
        results.append(watch)
    return results


def check_entry_signals(
    theme_id: int | None = None,
    *,
    thresholds: DiscoveryThresholds = DEFAULT_DISCOVERY_THRESHOLDS,
    meta_labeler_engine=None,
    regime_results: dict[str, Any] | None = None,
) -> list[dict]:
    triggered: list[dict] = []
    gate_settings = None
    gate_enabled = str(get_setting("fundamental_gate_enabled") or "true").lower() == "true"
    themes = [get_theme(theme_id)] if theme_id is not None else list_themes(include_closed=False)
    for theme in themes:
        if not theme:
            continue
        if str(theme.get("status") or "") != "Active" or int(theme.get("conviction") or 0) < thresholds.entry_signal_min_conviction:
            continue
        survivors: list[dict[str, Any]] = []
        for item in get_watchlist(theme_id=int(theme["id"]), status="Watching"):
            label = str(item.get("regime_label") or "")
            probability = float(item.get("regime_probability") or 0.0)
            crowd_raw = item.get("normalized_crowd_score")
            crowd = int(crowd_raw) if crowd_raw is not None else int(item.get("crowd_score") or 50)
            if label == "Bull" and probability >= thresholds.entry_signal_min_probability and crowd <= thresholds.entry_signal_max_crowd_score:
                ticker = str(item.get("ticker") or "").upper()
                if universe_screen_enabled():
                    eligibility = check_universe_eligibility(ticker)
                    if not eligibility.eligible:
                        logger.info("Universe screen BLOCKED discovery candidate %s: %s", ticker, ", ".join(eligibility.reasons))
                        continue
                if gate_enabled:
                    try:
                        from .fundamental_gating import get_fundamental_gate_settings, run_fundamental_gate

                        if gate_settings is None:
                            gate_settings = get_fundamental_gate_settings()
                        gate = run_fundamental_gate(
                            ticker,
                            piotroski_min=int(gate_settings["piotroski_min"]),
                            require_roic_above_wacc=bool(gate_settings["require_roic_above_wacc"]),
                            roic_lookback_years=int(gate_settings["roic_lookback_years"]),
                            pass_on_insufficient_data=bool(gate_settings["pass_on_insufficient_data"]),
                            altman_z_enabled=bool(gate_settings.get("altman_z_enabled", True)),
                            altman_z_distress_threshold=float(gate_settings.get("altman_z_distress_threshold", 1.81)),
                        )
                        update_watchlist_fundamental_gate(
                            int(item["id"]),
                            passed=bool(gate.passed),
                            piotroski_score=gate.piotroski.score if gate.piotroski else None,
                            roic_pct=gate.roic.roic_avg if gate.roic else None,
                            altman_z_score=gate.altman_z.z_score if gate.altman_z else None,
                            altman_z_interpretation=gate.altman_z.interpretation if gate.altman_z else "",
                            details=gate,
                        )
                        if not gate.passed:
                            logger.info("Fundamental gate BLOCKED %s: %s", ticker, "; ".join(gate.veto_reasons))
                            continue
                    except Exception as exc:
                        logger.warning("Fundamental gate failed for discovery candidate %s; continuing without veto gate.", ticker, exc_info=exc)
                if meta_labeler_engine is not None and callable(getattr(meta_labeler_engine, "is_ready", None)) and meta_labeler_engine.is_ready():
                    try:
                        regime_result = (regime_results or {}).get(ticker)
                        if regime_result is not None:
                            from .meta_labeler import extract_meta_features

                            features = extract_meta_features(regime_result.price_frame.iloc[-1])
                            ml_result = meta_labeler_engine.analyze(ticker=ticker, features=features, regime_result=regime_result)
                            if str(getattr(ml_result, "signal", "")).lower() == "veto":
                                continue
                    except Exception as exc:
                        logger.warning("Meta-labeler veto check failed for discovery candidate %s; continuing without veto gate.", item.get("ticker"), exc_info=exc)
                survivors.append(dict(item))
        metrics_map: dict[str, dict[str, float | None]] = {}
        cross_sectional_data: dict[str, dict[str, BetaAdjustedResult | VolatilityZResult]] = {}
        for item in survivors:
            ticker = str(item.get("ticker") or "").upper()
            try:
                beta_result = calculate_beta_adjusted_return(ticker)
                vol_result = calculate_volatility_z_score(ticker)
                cross_sectional_data[ticker] = {"beta_adjusted": beta_result, "volatility_z": vol_result}
                metrics_map[ticker] = {
                    "beta_adj_return": beta_result.beta_adjusted_return if beta_result.data_quality != "insufficient" else None,
                    "vol_z": vol_result.vol_z_score if vol_result.data_quality != "insufficient" else None,
                }
            except Exception:
                logger.debug("Cross-sectional enrichment failed for %s", ticker, exc_info=True)
        peer_norms = compute_peer_percentiles([str(item.get("ticker") or "").upper() for item in survivors], metrics_map) if metrics_map else {}
        for item in survivors:
            ticker = str(item.get("ticker") or "").upper()
            stored_beta_result: BetaAdjustedResult | None = cast(BetaAdjustedResult | None, (cross_sectional_data.get(ticker) or {}).get("beta_adjusted"))
            stored_vol_result: VolatilityZResult | None = cast(VolatilityZResult | None, (cross_sectional_data.get(ticker) or {}).get("volatility_z"))
            peer_payload = [asdict(row) for row in peer_norms.get(ticker, [])]
            try:
                update_watchlist_cross_sectional(
                    int(item["id"]),
                    beta=stored_beta_result.beta if stored_beta_result and stored_beta_result.beta is not None else None,
                    beta_adjusted_return=stored_beta_result.beta_adjusted_return if stored_beta_result else None,
                    vol_z_score=stored_vol_result.vol_z_score if stored_vol_result else None,
                    vol_z_interpretation=stored_vol_result.interpretation if stored_vol_result else "",
                    normalized_crowd_score=int(item["normalized_crowd_score"]) if item.get("normalized_crowd_score") is not None else int(item.get("crowd_score") or 50),
                    peer_percentile_json=json.dumps(peer_payload),
                )
            except Exception:
                logger.debug("Persisting cross-sectional enrichment failed for %s", ticker, exc_info=True)
            updated = update_watchlist_status(int(item["id"]), "Entry Signal")
            if updated:
                triggered.append(updated)
    return triggered


def expire_stale_candidates(
    max_age_days: int | None = None,
    *,
    thresholds: DiscoveryThresholds = DEFAULT_DISCOVERY_THRESHOLDS,
) -> int:
    expired = 0
    resolved_max_age_days = int(max_age_days if max_age_days is not None else thresholds.stale_candidate_max_age_days)
    cutoff = datetime.now(timezone.utc) - timedelta(days=resolved_max_age_days)
    for item in get_watchlist(status="Watching"):
        scanned_at = str(item.get("last_scanned_at") or item.get("discovered_at") or "")
        try:
            row_dt = datetime.fromisoformat(scanned_at)
        except ValueError:
            continue
        if row_dt <= cutoff:
            if update_watchlist_status(int(item["id"]), "Expired"):
                expired += 1
    return expired


def promote_candidate(watchlist_id: int) -> dict:
    item = get_watchlist_entry(watchlist_id)
    if not item:
        return {}
    added = add_ticker_to_theme(
        int(item["theme_id"]),
        str(item["ticker"]),
        role=str(item.get("suggested_role") or "Critical-Path"),
        rationale=str(item.get("discovery_rationale") or ""),
        entry_price=item.get("suggested_entry_price"),
        stop_price=item.get("suggested_stop_price"),
        time_horizon="tactical",
    )
    update_watchlist_status(int(watchlist_id), "Added")
    return added


def run_full_discovery(
    *,
    frontier_enabled: bool = True,
    frontier_provider: str = "auto",
    theme_ids: list[int] | None = None,
    thresholds: DiscoveryThresholds = DEFAULT_DISCOVERY_THRESHOLDS,
) -> dict[str, Any]:
    results: list[dict[str, Any]] = []
    themes = [get_theme(theme_id) for theme_id in (theme_ids or [])] if theme_ids else list_themes(include_closed=False)
    themes = [theme for theme in themes if theme and str(theme.get("status") or "") == "Active"]
    total_candidates = 0
    total_signals = 0
    for theme in themes:
        errors: list[str] = []
        created: list[dict[str, Any]] = []
        try:
            created = run_discovery_scan(
                int(theme["id"]),
                frontier_enabled=frontier_enabled,
                frontier_provider=frontier_provider,
            )
        except Exception as exc:
            logger.warning("Discovery scan failed for theme %s.", theme.get("name"), exc_info=exc)
            errors.append(str(exc))
        try:
            theme_signals = [item["ticker"] for item in check_entry_signals(int(theme["id"]), thresholds=thresholds)]
        except Exception as exc:
            logger.warning("Entry signal scan failed for theme %s.", theme.get("name"), exc_info=exc)
            errors.append(str(exc))
            theme_signals = []
        total_candidates += len(created)
        total_signals += len(theme_signals)
        results.append(
            {
                "theme_id": int(theme["id"]),
                "theme_name": str(theme.get("name") or ""),
                "new_candidates": len(created),
                "updated_candidates": len(created),
                "entry_signals": theme_signals,
                "errors": errors,
            }
        )
    return {
        "themes_scanned": len(themes),
        "candidates_found": total_candidates,
        "entry_signals": total_signals,
        "results": results,
        "watchlist_stats": get_watchlist_stats(),
    }
