from __future__ import annotations

import datetime as dt
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True)
class BaseEvent:
    """Root class for all bus events."""

    event_type: str
    correlation_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    created_at: str = field(default_factory=lambda: dt.datetime.now(dt.timezone.utc).isoformat())

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class EnrichedSignalEvent(BaseEvent):
    """
    Published by the Quant layer after regime fitting, ensemble aggregation,
    signal generation, and price target computation for one ticker.
    """

    event_type: str = field(default="enriched_signal", init=False)
    ticker: str = ""
    benchmark: str = ""
    snapshot_date: str = ""
    source: str = ""
    regime_label: str = ""
    regime_state_id: int = -1
    regime_probability: float = 0.0
    regime_state_vector: tuple[float, ...] = ()
    transition_matrix: tuple[tuple[float, ...], ...] = ()
    expected_regime_duration: float = 0.0
    transition_risk: float = 0.0
    recent_state_mean_return: float | None = None
    regime_days: int = 0
    composite_action: str = ""
    composite_strength: float = 0.0
    forward_signal_action: str = ""
    forward_signal_strength: float = 0.0
    technical_signal: str = ""
    current_price: float = 0.0
    entry_price: float | None = None
    exit_price: float | None = None
    stop_price: float | None = None
    risk_reward_ratio: float | None = None
    timeframe_days: int = 0
    atr_14: float | None = None
    unified_confidence: float = 0.0
    meta_labeler_score: float | None = None
    ensemble_signal: str = ""
    ensemble_composite_confidence: float = 0.0
    ensemble_sizing_multiplier: float = 1.0
    ensemble_veto_reason: str | None = None
    volume: float | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["regime_state_vector"] = list(self.regime_state_vector)
        payload["transition_matrix"] = [list(row) for row in self.transition_matrix]
        return payload


@dataclass(frozen=True)
class TradeIntentEvent(BaseEvent):
    """Published when a trade plan is created from a signal."""

    event_type: str = field(default="trade_intent", init=False)
    ticker: str = ""
    portfolio_id: int = 0
    action: str = ""
    source: str = ""
    plan_id: int | None = None
    meta_labeler_score: float | None = None
    regime_label: str = ""
    quantity: float = 0.0
    proposed_price: float | None = None
    rationale: str = ""


@dataclass(frozen=True)
class SignalSnapshotEvent(BaseEvent):
    """Published after a signal snapshot is persisted."""

    event_type: str = field(default="signal_snapshot", init=False)
    ticker: str = ""
    snapshot_date: str = ""
    action: str = ""
    regime_label: str = ""
    regime_probability: float = 0.0
    composite_strength: float = 0.0
    current_price: float = 0.0


@dataclass(frozen=True)
class AnalysisRequestEvent(BaseEvent):
    """Trigger event for the agent topology analysis path."""

    event_type: str = field(default="analysis_request", init=False)
    tickers: tuple[str, ...] = ()
    benchmark: str = ""
    period: str = "3y"
    requested_by: str = ""
    source: str = ""

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["tickers"] = list(self.tickers)
        return payload


@dataclass(frozen=True)
class FundamentalAssessmentEvent(BaseEvent):
    """LLM qualitative assessment emitted from the fundamental agent."""

    event_type: str = field(default="fundamental_assessment", init=False)
    ticker: str = ""
    regime_label: str = ""
    verdict: str = ""
    confidence_score: int | None = None
    catalyst_sentiment: str = ""
    vetoed: bool = False
    veto_reason: str | None = None
    source: str = ""
    enriched_signal_id: str = ""
    meta_labeler_score: float | None = None
    details: dict[str, Any] = field(default_factory=dict)
    moat_classification: str = ""
    moat_justification: str = ""


@dataclass(frozen=True)
class TradeDecisionEvent(BaseEvent):
    """Sizing and approval/veto decision from the portfolio agent."""

    event_type: str = field(default="trade_decision", init=False)
    ticker: str = ""
    portfolio_id: int = 0
    action: str = ""
    decision: str = ""
    quantity: float = 0.0
    proposed_price: float | None = None
    veto_reason: str | None = None
    source: str = ""
    regime_label: str = ""
    meta_labeler_score: float | None = None
    sizing_rationale: str | None = None
    enriched_signal_id: str = ""


@dataclass(frozen=True)
class OrderExecutionEvent(BaseEvent):
    """Fill or rejection result from the execution agent."""

    event_type: str = field(default="order_execution", init=False)
    ticker: str = ""
    portfolio_id: int = 0
    order_id: str = ""
    action: str = ""
    quantity: float = 0.0
    status: str = ""
    broker_type: str = ""
    trade_decision_id: str = ""
    filled_price: float | None = None
    filled_at: str | None = None
    message: str = ""


def enriched_signal_from_payload(
    ticker: str,
    regime_result: Any,
    composite_signal: Any,
    price_targets: Any,
    confidence: Any,
    ensemble_verdict: Any | None = None,
    *,
    benchmark: str = "",
    snapshot_date: str = "",
    source: str = "regime_analysis",
    meta_labeler_score: float | None = None,
    volume: float | None = None,
    correlation_id: str | None = None,
) -> EnrichedSignalEvent:
    """Bridge the existing signal pipeline dataclasses into an enriched event."""

    transition_source = getattr(regime_result, "transition_matrix", None)
    if transition_source is None:
        transition_matrix: tuple[tuple[float, ...], ...] = ()
    else:
        transition_matrix = tuple(
            tuple(float(value) for value in row)
            for row in tuple(transition_source)
        )
    state_source = getattr(regime_result, "latest_state_vector", None)
    state_vector = tuple(float(value) for value in tuple(state_source)) if state_source is not None else ()
    return EnrichedSignalEvent(
        correlation_id=str(correlation_id) if correlation_id else str(uuid.uuid4()),
        ticker=str(ticker or "").upper(),
        benchmark=str(benchmark or ""),
        snapshot_date=snapshot_date or dt.date.today().isoformat(),
        source=str(source or "regime_analysis"),
        regime_label=str(getattr(regime_result, "latest_label", "") or ""),
        regime_state_id=int(getattr(regime_result, "latest_state_id", -1) or -1),
        regime_probability=float(getattr(regime_result, "latest_probability", 0.0) or 0.0),
        regime_state_vector=state_vector,
        transition_matrix=transition_matrix,
        expected_regime_duration=float(getattr(regime_result, "expected_regime_duration", 0.0) or 0.0),
        transition_risk=float(getattr(regime_result, "transition_risk", 0.0) or 0.0),
        recent_state_mean_return=getattr(regime_result, "recent_state_mean_return", None),
        regime_days=int(getattr(regime_result, "regime_days", 0) or 0),
        composite_action=str(getattr(composite_signal, "composite_action", "") or ""),
        composite_strength=float(getattr(composite_signal, "composite_strength", 0.0) or 0.0),
        forward_signal_action=str(getattr(getattr(composite_signal, "forward_signal", None), "action", "") or ""),
        forward_signal_strength=float(getattr(getattr(composite_signal, "forward_signal", None), "strength", 0.0) or 0.0),
        technical_signal=str(getattr(composite_signal, "technical_signal", "") or ""),
        current_price=float(getattr(price_targets, "current_price", 0.0) or 0.0),
        entry_price=getattr(price_targets, "entry_price", None),
        exit_price=getattr(price_targets, "exit_price", None),
        stop_price=getattr(price_targets, "stop_price", None),
        risk_reward_ratio=getattr(price_targets, "risk_reward_ratio", None),
        timeframe_days=int(getattr(price_targets, "timeframe_days", 0) or 0),
        atr_14=getattr(price_targets, "atr_value", None),
        unified_confidence=float(getattr(confidence, "value", 0.0) or 0.0),
        meta_labeler_score=meta_labeler_score,
        ensemble_signal=str(getattr(ensemble_verdict, "signal", "") or "") if ensemble_verdict else "",
        ensemble_composite_confidence=float(getattr(ensemble_verdict, "composite_confidence", 0.0) or 0.0) if ensemble_verdict else 0.0,
        ensemble_sizing_multiplier=float(getattr(ensemble_verdict, "sizing_multiplier", 1.0) or 1.0) if ensemble_verdict else 1.0,
        ensemble_veto_reason=getattr(ensemble_verdict, "veto_reason", None) if ensemble_verdict else None,
        volume=volume,
    )
