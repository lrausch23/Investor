from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class PortfolioRiskSummary:
    regime_exposure: dict[str, float]
    sector_concentration: list[dict[str, Any]]
    correlation_risk: dict[str, Any]
    aggregate_transition_risk: float
    portfolio_composite_signal: str
    diversification_score: float
    risk_flags: list[str]
    total_value: float


def _market_value(position: Any) -> float:
    for attr in ("market_value", "current_value"):
        value = getattr(position, attr, None)
        if value is not None:
            try:
                return float(value)
            except Exception:
                continue
    return 0.0


def compute_regime_exposure(positions: list[Any], regime_results: dict[str, dict[str, Any]]) -> dict[str, float]:
    buckets = {"Bull": 0.0, "Neutral": 0.0, "Bear": 0.0}
    total = 0.0
    for position in positions:
        ticker = str(getattr(position, "ticker", "") or "").upper()
        label = str((regime_results.get(ticker) or {}).get("label") or "Neutral")
        market_value = _market_value(position)
        if label not in buckets or market_value <= 0:
            continue
        buckets[label] += market_value
        total += market_value
    if total <= 0:
        return {**buckets, "total_value": 0.0}
    return {
        "bull_pct": buckets["Bull"] / total,
        "neutral_pct": buckets["Neutral"] / total,
        "bear_pct": buckets["Bear"] / total,
        "bull_value": buckets["Bull"],
        "neutral_value": buckets["Neutral"],
        "bear_value": buckets["Bear"],
        "total_value": total,
    }


def compute_sector_concentration(
    positions: list[Any],
    regime_results: dict[str, dict[str, Any]],
    sector_map: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    sectors: dict[str, dict[str, float]] = {}
    for position in positions:
        ticker = str(getattr(position, "ticker", "") or "").upper()
        result = regime_results.get(ticker) or {}
        sector = str((sector_map or {}).get(ticker) or result.get("sector") or "Unknown")
        label = str(result.get("label") or "Neutral")
        market_value = _market_value(position)
        bucket = sectors.setdefault(sector, {"value": 0.0, "Bull": 0.0, "Neutral": 0.0, "Bear": 0.0})
        bucket["value"] += market_value
        if label in bucket:
            bucket[label] += market_value
    rows = []
    for sector, bucket in sorted(sectors.items(), key=lambda item: item[1]["value"], reverse=True):
        total = bucket["value"] or 1.0
        bear_pct = bucket["Bear"] / total
        rows.append(
            {
                "sector": sector,
                "value": bucket["value"],
                "bull_pct": bucket["Bull"] / total,
                "neutral_pct": bucket["Neutral"] / total,
                "bear_pct": bear_pct,
                "flag": "Bear concentration" if bear_pct > 0.60 else "",
            }
        )
    return rows


def compute_correlation_risk(
    positions: list[Any] | dict[str, dict[str, Any]],
    regime_results: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if regime_results is None and isinstance(positions, dict):
        regime_results = positions
        positions = []
    assert regime_results is not None
    total = max(1, len(regime_results))
    counts = {"Bull": 0, "Neutral": 0, "Bear": 0}
    total_value = sum(max(_market_value(position), 0.0) for position in positions) or 1.0
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for result in regime_results.values():
        label = str(result.get("label") or "Neutral")
        if label in counts:
            counts[label] += 1
    for position in positions:
        ticker = str(getattr(position, "ticker", "") or "").upper()
        result = regime_results.get(ticker) or {}
        sector = str(result.get("sector") or "Unknown")
        label = str(result.get("label") or "Neutral")
        bucket = grouped.setdefault((sector, label), {"sector": sector, "regime": label, "tickers": [], "combined_value": 0.0})
        bucket["tickers"].append(ticker)
        bucket["combined_value"] += _market_value(position)
    dominant_regime, dominant_count = max(counts.items(), key=lambda item: item[1])
    dominant_pct = dominant_count / total
    diversification_score = max(0.0, min(1.0, 1.0 - dominant_pct + (1 / 3)))
    warning = "High regime correlation" if dominant_pct > 0.70 else ""
    clusters = []
    cluster_warnings: list[str] = []
    for bucket in sorted(grouped.values(), key=lambda item: item["combined_value"], reverse=True):
        if len(bucket["tickers"]) < 2:
            continue
        pct = bucket["combined_value"] / total_value if total_value else 0.0
        clusters.append({**bucket, "pct_of_portfolio": pct})
        if pct >= 0.25:
            cluster_warnings.append(
                f"{len(bucket['tickers'])} {bucket['sector']} holdings ({', '.join(bucket['tickers'])}) "
                f"share {bucket['regime']} regimes — {pct:.0%} of portfolio value."
            )
    return {
        "dominant_regime": dominant_regime,
        "dominant_pct": dominant_pct,
        "diversification_score": diversification_score,
        "warning": warning,
        "clusters": clusters,
        "cluster_warnings": cluster_warnings,
    }


def compute_return_correlations(
    market_frames: dict[str, pd.DataFrame],
    window: int = 63,
) -> dict[tuple[str, str], float]:
    returns: dict[str, pd.Series] = {}
    for ticker, frame in market_frames.items():
        if "price" in frame.columns and len(frame) >= window:
            returns[ticker] = frame["price"].pct_change().dropna().tail(window)
    correlations: dict[tuple[str, str], float] = {}
    tickers = sorted(returns.keys())
    for index, ticker in enumerate(tickers):
        for other in tickers[index + 1 :]:
            aligned = pd.concat([returns[ticker], returns[other]], axis=1, join="inner")
            if len(aligned) >= 20:
                corr = float(aligned.iloc[:, 0].corr(aligned.iloc[:, 1]))
                correlations[(ticker, other)] = corr
                correlations[(other, ticker)] = corr
    return correlations


def portfolio_risk_summary(
    positions: list[Any],
    regime_results: dict[str, dict[str, Any]],
    sector_map: dict[str, str] | None = None,
) -> PortfolioRiskSummary:
    exposure = compute_regime_exposure(positions, regime_results)
    sector_rows = compute_sector_concentration(positions, regime_results, sector_map=sector_map)
    correlation = compute_correlation_risk(positions, regime_results)
    weighted_risk_numerator = 0.0
    weighted_risk_denominator = 0.0
    action_counts: dict[str, int] = {}
    flags: list[str] = []
    for position in positions:
        ticker = str(getattr(position, "ticker", "") or "").upper()
        result = regime_results.get(ticker) or {}
        market_value = _market_value(position)
        weighted_risk_numerator += market_value * float(result.get("transition_risk") or 0.0)
        weighted_risk_denominator += market_value
        action = str(result.get("composite_action") or "Hold")
        action_counts[action] = action_counts.get(action, 0) + 1
    if correlation.get("warning"):
        flags.append(str(correlation["warning"]))
    flags.extend(str(item) for item in (correlation.get("cluster_warnings") or []))
    flags.extend(row["flag"] for row in sector_rows if row.get("flag"))
    portfolio_signal = max(action_counts.items(), key=lambda item: item[1])[0] if action_counts else "Hold"
    return PortfolioRiskSummary(
        regime_exposure=exposure,
        sector_concentration=sector_rows,
        correlation_risk=correlation,
        aggregate_transition_risk=(weighted_risk_numerator / weighted_risk_denominator) if weighted_risk_denominator else 0.0,
        portfolio_composite_signal=portfolio_signal,
        diversification_score=float(correlation.get("diversification_score") or 0.0),
        risk_flags=flags,
        total_value=float(exposure.get("total_value") or 0.0),
    )


def portfolio_risk_summary_dict(
    positions: list[Any],
    regime_results: dict[str, dict[str, Any]],
    sector_map: dict[str, str] | None = None,
) -> dict[str, Any]:
    return asdict(portfolio_risk_summary(positions, regime_results, sector_map=sector_map))
