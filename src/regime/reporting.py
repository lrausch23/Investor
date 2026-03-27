from __future__ import annotations

from dataclasses import dataclass

from .hmm_engine import RegimeResult
from .llm_layer import QualitativeAssessment


@dataclass
class TickerReport:
    regime: RegimeResult
    qualitative: QualitativeAssessment
    regime_started_days_ago: int | None = None


def summarize_relative_strength(reports: list[TickerReport], benchmark_label: str) -> list[TickerReport]:
    return [report for report in reports if report.regime.latest_label == "Bull" and benchmark_label == "Neutral"]
