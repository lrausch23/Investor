from __future__ import annotations

import hashlib
import logging
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta, timezone
from typing import Any

from .data import fetch_recent_news
from .persistence import (
    get_alerts,
    get_setting,
    save_alert,
    save_thesis_monitor_run,
    set_setting,
)

logger = logging.getLogger(__name__)

HBM_MONITOR_KEY = "hbm_mu"
HBM_PRIMARY_TICKER = "MU"
DEFAULT_HBM_THESIS = (
    "MU HBM thesis: HBM remains structurally supply constrained into 2027, "
    "AI accelerator demand keeps HBM content and pricing resilient, and MU benefits "
    "from that constrained supply while trading below relevant memory and AI-enabler peers."
)
DEFAULT_HBM_WATCHLIST = (
    "MU",
    "NVDA",
    "AMD",
    "AVGO",
    "MRVL",
    "TSM",
    "ASML",
    "ARM",
    "005930.KS",
    "000660.KS",
)
DEFAULT_WARNING_SCORE = 35.0
DEFAULT_CRITICAL_SCORE = 70.0
DEFAULT_NEWS_LIMIT = 8
ALERT_LOOKBACK_HOURS = 24

DOMAIN_TERMS = (
    "hbm",
    "high bandwidth memory",
    "dram",
    "memory",
    "accelerator",
    "gpu",
    "ai chip",
    "inference",
    "training",
    "data center",
)

SUPPLY_RISK_TERMS: tuple[tuple[str, float], ...] = (
    ("hbm oversupply", 34.0),
    ("hbm supply glut", 34.0),
    ("excess hbm supply", 34.0),
    ("supply glut", 22.0),
    ("oversupply", 22.0),
    ("excess supply", 22.0),
    ("availability improves", 24.0),
    ("improves availability", 24.0),
    ("supply loosening", 24.0),
    ("capacity expansion", 18.0),
    ("capacity ramp", 18.0),
    ("output ramp", 16.0),
    ("yield improves", 16.0),
    ("yields improve", 16.0),
    ("lead times shorten", 22.0),
    ("pricing pressure", 20.0),
    ("price cuts", 20.0),
    ("contract price decline", 24.0),
    ("hbm price decline", 28.0),
    ("inventory build", 18.0),
    ("inventory correction", 18.0),
    ("samsung hbm qualified", 28.0),
    ("samsung wins hbm", 24.0),
    ("hbm4 capacity", 18.0),
    ("new hbm capacity", 20.0),
    ("hbm supply expands", 26.0),
)

SUBSTITUTION_RISK_TERMS: tuple[tuple[str, float], ...] = (
    ("without hbm", 42.0),
    ("hbm-less", 42.0),
    ("hbm less", 38.0),
    ("reduce hbm", 36.0),
    ("less hbm", 32.0),
    ("lower hbm requirement", 36.0),
    ("hbm requirement falls", 38.0),
    ("reduced memory requirement", 28.0),
    ("memory efficient", 16.0),
    ("kv cache compression", 24.0),
    ("attention optimization", 18.0),
    ("mixture of experts", 12.0),
    ("sparsity", 14.0),
    ("model compression", 18.0),
    ("cxl memory pooling", 26.0),
    ("near-memory compute", 24.0),
    ("in-memory compute", 24.0),
    ("wafer-scale", 16.0),
    ("sram", 12.0),
    ("lpddr", 14.0),
    ("gddr", 14.0),
)

SUPPORT_TERMS: tuple[tuple[str, float], ...] = (
    ("sold out", 22.0),
    ("fully allocated", 24.0),
    ("supply constrained", 24.0),
    ("shortage", 20.0),
    ("limited availability", 22.0),
    ("tight supply", 22.0),
    ("capacity constrained", 20.0),
    ("demand exceeds supply", 24.0),
    ("allocated through 2027", 28.0),
    ("sold out through 2027", 30.0),
    ("sold out through 2026", 24.0),
)


@dataclass(frozen=True)
class ThesisEvidence:
    ticker: str
    title: str
    summary: str = ""
    publisher: str = ""
    link: str = ""
    published_at: str = ""
    category: str = "neutral"
    direction: str = "neutral"
    score: float = 0.0
    matched_terms: list[str] = field(default_factory=list)
    rationale: str = ""


@dataclass(frozen=True)
class ThesisMonitorResult:
    monitor_key: str
    primary_ticker: str
    thesis: str
    status: str
    severity: str
    risk_score: float
    warning_threshold: float
    critical_threshold: float
    generated_at: str
    tickers_scanned: list[str]
    evidence: list[ThesisEvidence]
    should_alert: bool
    alert_id: int | None = None
    alert_deduped: bool = False
    source: str = "yfinance_news"

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["evidence"] = [asdict(item) for item in self.evidence]
        return payload


def _split_setting_list(raw: str | None, default: tuple[str, ...]) -> list[str]:
    text = str(raw or "").strip()
    if not text:
        return list(default)
    items = [item.strip().upper() for item in text.replace("\n", ",").split(",")]
    return [item for item in items if item]


def _float_setting(key: str, default: float) -> float:
    raw = get_setting(key)
    if raw in (None, ""):
        return float(default)
    try:
        return float(str(raw))
    except (TypeError, ValueError):
        return float(default)


def _int_setting(key: str, default: int) -> int:
    raw = get_setting(key)
    if raw in (None, ""):
        return int(default)
    try:
        return max(1, int(str(raw)))
    except (TypeError, ValueError):
        return int(default)


def _contains_domain_context(text: str, matches: list[str]) -> bool:
    if any("hbm" in term for term in matches):
        return True
    return any(term in text for term in DOMAIN_TERMS)


def _matches(text: str, terms: tuple[tuple[str, float], ...]) -> tuple[list[str], float]:
    matched: list[str] = []
    score = 0.0
    for phrase, weight in terms:
        if phrase in text:
            matched.append(phrase)
            score += float(weight)
    return matched, score


def _classify_news_item(ticker: str, item: dict[str, Any]) -> ThesisEvidence | None:
    title = str(item.get("title") or "").strip()
    summary = str(item.get("summary") or "").strip()
    text = f"{title} {summary}".lower()
    if not text.strip():
        return None

    supply_terms, supply_score = _matches(text, SUPPLY_RISK_TERMS)
    substitution_terms, substitution_score = _matches(text, SUBSTITUTION_RISK_TERMS)
    support_terms, support_score = _matches(text, SUPPORT_TERMS)
    all_terms = [*supply_terms, *substitution_terms, *support_terms]
    if not all_terms or not _contains_domain_context(text, all_terms):
        return None

    risk_score = supply_score + substitution_score
    if risk_score > 0:
        if substitution_score > supply_score:
            category = "technology_substitution"
            rationale = "Potential step change that could reduce required HBM content."
        elif any(term in supply_terms for term in ("pricing pressure", "price cuts", "contract price decline", "hbm price decline", "inventory build", "inventory correction")):
            category = "pricing_or_inventory"
            rationale = "Potential early sign that HBM pricing power or inventory tightness is weakening."
        else:
            category = "supply_availability"
            rationale = "Potential early sign that HBM availability is improving faster than the thesis assumes."
        score = min(60.0, risk_score)
        return ThesisEvidence(
            ticker=str(ticker or "").upper(),
            title=title,
            summary=summary,
            publisher=str(item.get("publisher") or ""),
            link=str(item.get("link") or ""),
            published_at=str(item.get("published_at") or ""),
            category=category,
            direction="risk",
            score=score,
            matched_terms=[*supply_terms, *substitution_terms],
            rationale=rationale,
        )

    if support_score > 0:
        return ThesisEvidence(
            ticker=str(ticker or "").upper(),
            title=title,
            summary=summary,
            publisher=str(item.get("publisher") or ""),
            link=str(item.get("link") or ""),
            published_at=str(item.get("published_at") or ""),
            category="thesis_support",
            direction="support",
            score=min(40.0, support_score),
            matched_terms=support_terms,
            rationale="Evidence is consistent with ongoing HBM scarcity and allocation tightness.",
        )
    return None


def _risk_score(evidence: list[ThesisEvidence]) -> float:
    risk_items = sorted(
        [item for item in evidence if item.direction == "risk"],
        key=lambda item: item.score,
        reverse=True,
    )
    if not risk_items:
        return 0.0
    top_score = float(risk_items[0].score or 0.0)
    breadth_bonus = min(24.0, max(0, len(risk_items) - 1) * 8.0)
    category_bonus = max(0, len({item.category for item in risk_items}) - 1) * 6.0
    return min(100.0, top_score + breadth_bonus + category_bonus)


def _status_and_severity(score: float, warning_threshold: float, critical_threshold: float) -> tuple[str, str]:
    if score >= critical_threshold:
        return "reunderwrite", "critical"
    if score >= warning_threshold:
        return "watch", "warning"
    return "intact", "info"


def _fingerprint(evidence: list[ThesisEvidence]) -> str:
    parts = [
        f"{item.ticker}|{item.category}|{item.title}|{item.link}"
        for item in evidence
        if item.direction == "risk"
    ][:5]
    return hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()[:16]


def _recent_duplicate_alert(fingerprint: str) -> dict[str, Any] | None:
    since = (datetime.now(timezone.utc) - timedelta(hours=ALERT_LOOKBACK_HOURS)).isoformat()
    for alert in get_alerts(alert_type="thesis_monitor", since=since, limit=25):
        data = alert.get("data") if isinstance(alert, dict) else {}
        if isinstance(data, dict) and str(data.get("fingerprint") or "") == fingerprint:
            return alert
    return None


def _alert_title(status: str, score: float, evidence: list[ThesisEvidence]) -> str:
    risk_items = [item for item in evidence if item.direction == "risk"]
    category = risk_items[0].category.replace("_", " ") if risk_items else "thesis risk"
    return f"MU HBM thesis {status}: {category} ({score:.0f})"


def _alert_message(result: ThesisMonitorResult) -> str:
    risk_items = [item for item in result.evidence if item.direction == "risk"]
    if not risk_items:
        return f"HBM thesis remains intact. Risk score {result.risk_score:.0f}."
    top = risk_items[0]
    return (
        f"HBM thesis status is {result.status}. Risk score {result.risk_score:.0f} "
        f"versus warning {result.warning_threshold:.0f} and critical {result.critical_threshold:.0f}. "
        f"Top evidence: {top.ticker} - {top.title}"
    )


def _dispatch_alert(alert: dict[str, Any]) -> None:
    try:
        from .notifications import dispatch_notification_sync

        dispatch_notification_sync(
            str(alert.get("alert_type") or "thesis_monitor"),
            str(alert.get("title") or ""),
            str(alert.get("message") or ""),
            str(alert.get("severity") or "info"),
            ticker=str(alert.get("ticker") or "") or None,
            data=alert.get("data") if isinstance(alert.get("data"), dict) else None,
        )
    except Exception:
        logger.debug("Unable to dispatch thesis monitor alert.", exc_info=True)


class HBMThesisMonitorAgent:
    """Read-only thesis monitor for MU HBM supply and substitution risk."""

    monitor_key = HBM_MONITOR_KEY
    primary_ticker = HBM_PRIMARY_TICKER

    def run(self, *, save: bool = True, dispatch: bool = True) -> ThesisMonitorResult:
        thesis = str(get_setting("thesis_monitor_hbm_thesis") or DEFAULT_HBM_THESIS).strip()
        tickers = _split_setting_list(get_setting("thesis_monitor_hbm_tickers"), DEFAULT_HBM_WATCHLIST)
        news_limit = _int_setting("thesis_monitor_hbm_news_limit", DEFAULT_NEWS_LIMIT)
        warning_threshold = _float_setting("thesis_monitor_hbm_warning_score", DEFAULT_WARNING_SCORE)
        critical_threshold = _float_setting("thesis_monitor_hbm_critical_score", DEFAULT_CRITICAL_SCORE)

        evidence: list[ThesisEvidence] = []
        for ticker in tickers:
            try:
                items = fetch_recent_news(ticker, limit=news_limit)
            except Exception as exc:
                logger.debug("HBM thesis monitor news fetch failed for %s: %s", ticker, exc)
                items = []
            for item in items:
                classified = _classify_news_item(ticker, item)
                if classified is not None:
                    evidence.append(classified)

        evidence.sort(key=lambda item: (item.direction != "risk", -float(item.score or 0.0), item.ticker, item.title))
        score = _risk_score(evidence)
        status, severity = _status_and_severity(score, warning_threshold, critical_threshold)
        generated_at = datetime.now(timezone.utc).isoformat()
        should_alert = severity in {"warning", "critical"}
        alert_id: int | None = None
        deduped = False

        result = ThesisMonitorResult(
            monitor_key=self.monitor_key,
            primary_ticker=self.primary_ticker,
            thesis=thesis,
            status=status,
            severity=severity,
            risk_score=score,
            warning_threshold=warning_threshold,
            critical_threshold=critical_threshold,
            generated_at=generated_at,
            tickers_scanned=tickers,
            evidence=evidence[:25],
            should_alert=should_alert,
            source="yfinance_news",
        )

        fingerprint = _fingerprint(result.evidence)
        if save and should_alert:
            duplicate = _recent_duplicate_alert(fingerprint)
            if duplicate is None:
                alert = save_alert(
                    "thesis_monitor",
                    _alert_title(status, score, result.evidence),
                    severity=severity,
                    ticker=self.primary_ticker,
                    message=_alert_message(result),
                    data={
                        "monitor_key": self.monitor_key,
                        "risk_score": score,
                        "status": status,
                        "fingerprint": fingerprint,
                        "threshold_origin": "draft threshold for PM confirmation",
                        "evidence": [asdict(item) for item in result.evidence[:10]],
                        "tickers_scanned": tickers,
                    },
                )
                alert_id = int(alert.get("id") or 0) or None
                if dispatch:
                    _dispatch_alert(alert)
            else:
                deduped = True
                alert_id = int(duplicate.get("id") or 0) or None

        result = replace(result, alert_id=alert_id, alert_deduped=deduped)

        if save:
            saved = save_thesis_monitor_run(
                monitor_key=self.monitor_key,
                primary_ticker=self.primary_ticker,
                status=status,
                severity=severity,
                risk_score=score,
                thesis=thesis,
                evidence=[asdict(item) for item in result.evidence],
                tickers_scanned=tickers,
                alert_id=alert_id,
                created_at=generated_at,
            )
            set_setting("last_hbm_thesis_monitor_at", generated_at)
            logger.info(
                "Saved HBM thesis monitor run %s with status=%s score=%.1f",
                saved.get("id"),
                status,
                score,
            )
        return result


def run_hbm_thesis_monitor(*, save: bool = True, dispatch: bool = True) -> dict[str, Any]:
    return HBMThesisMonitorAgent().run(save=save, dispatch=dispatch).to_dict()


def hbm_thesis_monitor_config() -> dict[str, Any]:
    return {
        "monitor_key": HBM_MONITOR_KEY,
        "primary_ticker": HBM_PRIMARY_TICKER,
        "thesis": str(get_setting("thesis_monitor_hbm_thesis") or DEFAULT_HBM_THESIS),
        "tickers": _split_setting_list(get_setting("thesis_monitor_hbm_tickers"), DEFAULT_HBM_WATCHLIST),
        "warning_threshold": _float_setting("thesis_monitor_hbm_warning_score", DEFAULT_WARNING_SCORE),
        "critical_threshold": _float_setting("thesis_monitor_hbm_critical_score", DEFAULT_CRITICAL_SCORE),
        "news_limit": _int_setting("thesis_monitor_hbm_news_limit", DEFAULT_NEWS_LIMIT),
        "enabled": str(get_setting("thesis_monitor_hbm_enabled") or "true").strip().lower()
        not in {"0", "false", "no", "off"},
    }
