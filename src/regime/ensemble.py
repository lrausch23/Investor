from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class AnalystResult:
    """Standard output from any ensemble analyst."""

    analyst_name: str
    confidence: float
    signal: str
    details: dict[str, Any] = field(default_factory=dict)


class AnalystBase(ABC):
    """Abstract base class for ensemble analysts."""

    @property
    @abstractmethod
    def name(self) -> str:
        ...

    @abstractmethod
    def is_ready(self) -> bool:
        ...

    @abstractmethod
    def analyze(
        self,
        ticker: str,
        features: dict[str, float],
        regime_result: Any,
    ) -> AnalystResult:
        ...

    @abstractmethod
    def train(self, labeled_frame: Any, **kwargs: Any) -> dict[str, Any]:
        ...


class AnalystRegistry:
    """Registry of available ensemble analysts."""

    def __init__(self) -> None:
        self._analysts: dict[str, AnalystBase] = {}

    def register(self, analyst: AnalystBase) -> None:
        self._analysts[analyst.name] = analyst

    def get(self, name: str) -> AnalystBase | None:
        return self._analysts.get(name)

    def list_analysts(self) -> list[str]:
        return list(self._analysts.keys())

    def ready_analysts(self) -> list[AnalystBase]:
        return [analyst for analyst in self._analysts.values() if analyst.is_ready()]


@dataclass(frozen=True)
class EnsembleConfig:
    """Configuration for ensemble aggregation."""

    veto_threshold: float = 0.50
    confirm_threshold: float = 0.65
    aggregation_method: str = "mean"
    analyst_weights: dict[str, float] = field(default_factory=dict)


DEFAULT_ENSEMBLE_CONFIG = EnsembleConfig()


@dataclass(frozen=True)
class EnsembleVerdict:
    """Aggregated output from all ensemble analysts."""

    composite_confidence: float
    signal: str
    analyst_results: list[AnalystResult]
    sizing_multiplier: float
    veto_reason: str | None


def _aggregate_confidence(results: list[AnalystResult], config: EnsembleConfig) -> float:
    if not results:
        return 1.0
    if config.aggregation_method == "min":
        return min(float(result.confidence) for result in results)
    if config.aggregation_method == "weighted":
        weighted_sum = 0.0
        total_weight = 0.0
        for result in results:
            weight = float(config.analyst_weights.get(result.analyst_name, 1.0) or 0.0)
            weighted_sum += float(result.confidence) * weight
            total_weight += weight
        if total_weight <= 0:
            return sum(float(result.confidence) for result in results) / len(results)
        return weighted_sum / total_weight
    return sum(float(result.confidence) for result in results) / len(results)


def aggregate_analysts(
    results: list[AnalystResult],
    config: EnsembleConfig = DEFAULT_ENSEMBLE_CONFIG,
) -> EnsembleVerdict:
    """
    Aggregate individual analyst results into a single verdict.
    """

    for result in results:
        if str(result.signal or "").lower() == "veto":
            return EnsembleVerdict(
                composite_confidence=float(result.confidence),
                signal="veto",
                analyst_results=results,
                sizing_multiplier=0.0,
                veto_reason=f"{result.analyst_name} signaled veto",
            )

    composite_confidence = _aggregate_confidence(results, config)
    if composite_confidence < config.veto_threshold:
        return EnsembleVerdict(
            composite_confidence=composite_confidence,
            signal="veto",
            analyst_results=results,
            sizing_multiplier=0.0,
            veto_reason="Composite confidence below veto threshold",
        )
    if composite_confidence >= config.confirm_threshold:
        return EnsembleVerdict(
            composite_confidence=composite_confidence,
            signal="confirm",
            analyst_results=results,
            sizing_multiplier=1.0,
            veto_reason=None,
        )

    width = max(1e-9, config.confirm_threshold - config.veto_threshold)
    progress = (composite_confidence - config.veto_threshold) / width
    sizing_multiplier = 0.25 + (0.75 * progress)
    return EnsembleVerdict(
        composite_confidence=composite_confidence,
        signal="neutral",
        analyst_results=results,
        sizing_multiplier=max(0.25, min(1.0, sizing_multiplier)),
        veto_reason=None,
    )


class PassthroughAnalyst(AnalystBase):
    """A no-op analyst that always confirms with 100% confidence."""

    @property
    def name(self) -> str:
        return "passthrough"

    def is_ready(self) -> bool:
        return True

    def analyze(self, ticker, features, regime_result) -> AnalystResult:
        del ticker, features, regime_result
        return AnalystResult(
            analyst_name=self.name,
            confidence=1.0,
            signal="confirm",
            details={"note": "passthrough — no filtering applied"},
        )

    def train(self, labeled_frame, **kwargs: Any) -> dict[str, Any]:
        del labeled_frame, kwargs
        return {"status": "passthrough — no training needed"}


_registry = AnalystRegistry()
_registry.register(PassthroughAnalyst())


def get_registry() -> AnalystRegistry:
    """Return the global analyst registry."""

    return _registry
