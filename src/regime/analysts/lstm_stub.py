"""Placeholder LSTM Sequence Predictor analyst."""

from __future__ import annotations

from typing import Any

from ..ensemble import AnalystBase, AnalystResult


class LSTMSequenceAnalyst(AnalystBase):
    @property
    def name(self) -> str:
        return "lstm_sequence"

    def is_ready(self) -> bool:
        return False

    def analyze(
        self,
        ticker: str,
        features: dict[str, float],
        regime_result: Any,
    ) -> AnalystResult:
        del ticker, features, regime_result
        return AnalystResult(
            analyst_name=self.name,
            confidence=0.5,
            signal="neutral",
            details={"note": "LSTM analyst not yet implemented — stub placeholder"},
        )

    def train(self, labeled_frame: Any, **kwargs: Any) -> dict[str, Any]:
        del labeled_frame, kwargs
        return {"status": "not_implemented", "analyst": self.name}
