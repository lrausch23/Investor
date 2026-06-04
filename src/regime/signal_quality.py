from __future__ import annotations

import datetime as dt
import json
from dataclasses import asdict, dataclass
from typing import Any


ACTIONABLE_SIGNAL_SCORE = 65.0
WATCH_SIGNAL_SCORE = 50.0
STALE_WARNING_MINUTES = 36 * 60
STALE_BLOCK_MINUTES = 72 * 60
MAX_BUY_ENTRY_PREMIUM_PCT = 0.05
BUY_ENTRY_WARNING_PREMIUM_PCT = 0.02

_BUY_ACTIONS = {"buy", "strong buy", "entry"}
_SELL_ACTIONS = {"sell", "strong sell", "exit", "reduce"}
_TIMESTAMP_KEYS = (
    "signal_generated_at",
    "generated_at",
    "cached_at",
    "last_run_timestamp",
    "updated_at",
    "last_scanned_at",
    "entry_signal_at",
    "snapshot_date",
    "created_at",
)


@dataclass(frozen=True)
class SignalQuality:
    action: str
    score: float
    grade: str
    actionable: bool
    reasons: tuple[str, ...]
    warnings: tuple[str, ...]
    blockers: tuple[str, ...]
    source_age_minutes: float | None = None
    current_price: float | None = None
    reference_price: float | None = None
    price_distance_pct: float | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["reasons"] = list(self.reasons)
        payload["warnings"] = list(self.warnings)
        payload["blockers"] = list(self.blockers)
        return payload

    def summary(self) -> str:
        parts = list(self.blockers or self.warnings or self.reasons)
        return "; ".join(parts[:3]) if parts else self.grade


def _to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = float(value)
    except Exception:
        return None
    if parsed != parsed:
        return None
    return parsed


def _timestamp_from_date(text: str) -> dt.datetime | None:
    try:
        parsed = dt.date.fromisoformat(text)
    except Exception:
        return None
    return dt.datetime(parsed.year, parsed.month, parsed.day, tzinfo=dt.timezone.utc)


def _parse_timestamp(raw: Any) -> dt.datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    if len(text) == 10:
        return _timestamp_from_date(text)
    try:
        parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return _timestamp_from_date(text[:10])
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def _source_timestamp(signal: dict[str, Any], explicit: Any = None) -> dt.datetime | None:
    explicit_timestamp = _parse_timestamp(explicit)
    if explicit_timestamp is not None:
        return explicit_timestamp
    for key in _TIMESTAMP_KEYS:
        parsed = _parse_timestamp(signal.get(key))
        if parsed is not None:
            return parsed
    return None


def _price_targets(signal: dict[str, Any]) -> dict[str, Any]:
    targets = signal.get("price_targets")
    if isinstance(targets, dict):
        return targets
    if isinstance(targets, str):
        try:
            parsed = json.loads(targets)
        except Exception:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _candidate_action(signal: dict[str, Any], explicit_action: str | None) -> str:
    if explicit_action:
        action = explicit_action
    else:
        action = str(signal.get("composite_signal") or signal.get("action") or signal.get("ai_verdict") or "").strip()
    normalized = str(action or "").strip().lower()
    status = str(signal.get("status") or "").strip().lower()
    if normalized in _BUY_ACTIONS or status in {"entry signal", "added"}:
        return "Buy"
    if normalized in _SELL_ACTIONS:
        return "Sell"
    return "Hold"


def _composite_strength(signal: dict[str, Any]) -> float | None:
    direct = _to_float(signal.get("composite_strength"))
    if direct is not None:
        return direct
    diagnostics = signal.get("signal_diagnostics")
    if isinstance(diagnostics, dict):
        return _to_float(diagnostics.get("composite_strength"))
    return None


def _risk_reward(signal: dict[str, Any]) -> float | None:
    direct = _to_float(signal.get("risk_reward_ratio"))
    if direct is not None:
        return direct
    return _to_float(_price_targets(signal).get("risk_reward_ratio"))


def _reference_price(signal: dict[str, Any], explicit: float | None) -> float | None:
    if explicit is not None and explicit > 0:
        return explicit
    targets = _price_targets(signal)
    for key in ("entry_price", "suggested_entry_price", "proposed_price", "arrival_price"):
        value = _to_float(signal.get(key) if key not in targets else targets.get(key))
        if value is not None and value > 0:
            return value
    return None


def _current_price(signal: dict[str, Any], explicit: float | None) -> float | None:
    if explicit is not None and explicit > 0:
        return explicit
    targets = _price_targets(signal)
    for key in ("current_price", "arrival_price", "last_price", "proposed_price"):
        value = _to_float(signal.get(key) if key not in targets else targets.get(key))
        if value is not None and value > 0:
            return value
    return None


def evaluate_signal_quality(
    signal: dict[str, Any] | None,
    *,
    action: str | None = None,
    source: str = "",
    current_price: float | None = None,
    reference_price: float | None = None,
    source_timestamp: Any = None,
    stop_triggered: bool = False,
    now: dt.datetime | None = None,
) -> SignalQuality:
    """Score whether a generated equity signal is fresh and executable enough to trade."""

    signal = dict(signal or {})
    resolved_now = now or dt.datetime.now(dt.timezone.utc)
    if resolved_now.tzinfo is None:
        resolved_now = resolved_now.replace(tzinfo=dt.timezone.utc)
    resolved_now = resolved_now.astimezone(dt.timezone.utc)

    resolved_action = _candidate_action(signal, action)
    resolved_current = _current_price(signal, current_price)
    resolved_reference = _reference_price(signal, reference_price)
    reasons: list[str] = []
    warnings: list[str] = []
    blockers: list[str] = []
    score = 50.0

    if resolved_action == "Hold":
        return SignalQuality(
            action="Hold",
            score=0.0,
            grade="hold",
            actionable=False,
            reasons=("No actionable Buy/Sell signal.",),
            warnings=(),
            blockers=(),
            current_price=resolved_current,
            reference_price=resolved_reference,
        )

    if stop_triggered and resolved_action == "Sell":
        return SignalQuality(
            action="Sell",
            score=100.0,
            grade="actionable",
            actionable=True,
            reasons=("Stop price is triggered.",),
            warnings=(),
            blockers=(),
            current_price=resolved_current,
            reference_price=resolved_reference,
        )

    timestamp = _source_timestamp(signal, explicit=source_timestamp)
    source_age_minutes: float | None = None
    if timestamp is None:
        score -= 6.0
        warnings.append("No signal timestamp available.")
    else:
        source_age_minutes = max(0.0, (resolved_now - timestamp).total_seconds() / 60.0)
        if source_age_minutes > STALE_BLOCK_MINUTES:
            score -= 30.0
            blockers.append(f"Signal is stale ({source_age_minutes / 60.0:.1f}h old).")
        elif source_age_minutes > STALE_WARNING_MINUTES:
            score -= 12.0
            warnings.append(f"Signal is aging ({source_age_minutes / 60.0:.1f}h old).")
        else:
            score += 8.0
            reasons.append("Signal timestamp is fresh.")

    if resolved_current is None or resolved_current <= 0:
        score -= 22.0
        blockers.append("Current executable price is unavailable.")
    else:
        score += 5.0
        reasons.append("Current executable price is available.")

    regime_label = str(signal.get("regime_label") or signal.get("regime") or "").strip()
    probability = _to_float(signal.get("regime_probability"))
    if probability is None:
        probability = _to_float(signal.get("probability"))
    strength = _composite_strength(signal)
    meta_score = _to_float(signal.get("meta_labeler_score"))
    if meta_score is None:
        meta_score = _to_float(signal.get("meta_labeler_probability"))
    risk_reward = _risk_reward(signal)

    if probability is None:
        warnings.append("Regime probability is unavailable.")
    elif probability >= 0.75:
        score += 15.0
        reasons.append(f"High regime conviction ({probability:.0%}).")
    elif probability >= 0.65:
        score += 9.0
        reasons.append(f"Good regime conviction ({probability:.0%}).")
    elif probability >= 0.55:
        score += 2.0
        warnings.append(f"Only moderate regime conviction ({probability:.0%}).")
    else:
        score -= 18.0
        blockers.append(f"Regime conviction is too low ({probability:.0%}).")

    if strength is not None:
        if strength >= 0.75:
            score += 10.0
            reasons.append(f"Composite strength is high ({strength:.0%}).")
        elif strength >= 0.60:
            score += 5.0
            reasons.append(f"Composite strength is acceptable ({strength:.0%}).")
        elif strength < 0.45:
            score -= 12.0
            warnings.append(f"Composite strength is weak ({strength:.0%}).")

    if meta_score is not None:
        if meta_score >= 0.65:
            score += 8.0
            reasons.append(f"Meta-labeler confirms ({meta_score:.0%}).")
        elif meta_score < 0.35:
            score -= 25.0
            blockers.append(f"Meta-labeler confidence is too low ({meta_score:.0%}).")
        elif meta_score < 0.50:
            score -= 12.0
            warnings.append(f"Meta-labeler confidence is weak ({meta_score:.0%}).")

    if resolved_action == "Buy":
        if regime_label == "Bull":
            score += 8.0
            reasons.append("Daily regime is Bull.")
        elif regime_label == "Neutral":
            score += 2.0
            warnings.append("Daily regime is Neutral, not Bull.")
        elif regime_label == "Bear":
            score -= 22.0
            blockers.append("Buy signal conflicts with Bear regime.")

        weekly_regime = str(signal.get("weekly_regime") or "").strip()
        aligned = signal.get("multi_timeframe_aligned")
        if weekly_regime:
            if weekly_regime == regime_label:
                score += 6.0
                reasons.append("Daily and weekly regimes are aligned.")
            elif weekly_regime == "Bear" and regime_label == "Bull":
                score -= 14.0
                warnings.append("Weekly regime conflicts with daily Bull signal.")
        elif aligned is True:
            score += 4.0
            reasons.append("Multi-timeframe signal is aligned.")

        if risk_reward is not None:
            if risk_reward >= 1.5:
                score += 10.0
                reasons.append(f"Risk/reward is favorable ({risk_reward:.2f}).")
            elif risk_reward < 1.0:
                score -= 14.0
                warnings.append(f"Risk/reward is unattractive ({risk_reward:.2f}).")

        price_distance_pct: float | None = None
        if resolved_current is not None and resolved_reference is not None and resolved_reference > 0:
            price_distance_pct = (resolved_current - resolved_reference) / resolved_reference
            if price_distance_pct > MAX_BUY_ENTRY_PREMIUM_PCT:
                score -= 28.0
                blockers.append(f"Current price is {price_distance_pct:.1%} above the entry premise.")
            elif price_distance_pct > BUY_ENTRY_WARNING_PREMIUM_PCT:
                score -= 10.0
                warnings.append(f"Current price is {price_distance_pct:.1%} above the entry premise.")
            elif price_distance_pct < -0.10:
                score -= 6.0
                warnings.append(f"Current price is {abs(price_distance_pct):.1%} below the entry premise.")
            else:
                score += 8.0
                reasons.append("Current price is close to the entry premise.")
    else:
        price_distance_pct = None
        if regime_label == "Bear":
            score += 10.0
            reasons.append("Bear regime supports exit.")
        elif regime_label == "Bull":
            score -= 10.0
            warnings.append("Exit signal conflicts with Bull regime.")
        if risk_reward is not None and risk_reward < 0.75:
            score += 3.0
            reasons.append("Forward risk/reward is no longer attractive.")

    score = max(0.0, min(100.0, score))
    if blockers:
        grade = "blocked"
        actionable = False
    elif score >= ACTIONABLE_SIGNAL_SCORE:
        grade = "actionable"
        actionable = True
    elif score >= WATCH_SIGNAL_SCORE:
        grade = "watch"
        actionable = False
    else:
        grade = "hold"
        actionable = False

    return SignalQuality(
        action=resolved_action,
        score=round(score, 1),
        grade=grade,
        actionable=actionable,
        reasons=tuple(reasons),
        warnings=tuple(warnings),
        blockers=tuple(blockers),
        source_age_minutes=round(source_age_minutes, 1) if source_age_minutes is not None else None,
        current_price=resolved_current,
        reference_price=resolved_reference,
        price_distance_pct=round(price_distance_pct, 4) if price_distance_pct is not None else None,
    )
