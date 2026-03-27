from __future__ import annotations

import numpy as np
import pandas as pd


class FakeRegime:
    def __init__(
        self,
        ticker: str,
        latest_label: str = "Bull",
        *,
        latest_state_id: int | None = None,
        latest_probability: float = 0.91,
        latest_price: float = 123.45,
        latest_state_vector=None,
        transition_matrix=None,
        transition_risk: float = 0.05,
        expected_regime_duration: float = 12.0,
        regime_days: int = 5,
        regime_signal: str | None = None,
        recent_state_mean_return: float = 0.011,
        regime_inconsistency_warning=None,
        state_statistics=None,
        price_frame=None,
    ):
        self.ticker = ticker
        self.latest_label = latest_label
        self.latest_state_id = (
            latest_state_id
            if latest_state_id is not None
            else 0
            if latest_label == "Bull"
            else 2
            if latest_label == "Bear"
            else 1
        )
        self.latest_probability = latest_probability
        self.latest_price = latest_price
        self.latest_state_vector = latest_state_vector if latest_state_vector is not None else np.array([0.8, 0.1, 0.1])
        self.transition_matrix = transition_matrix if transition_matrix is not None else np.eye(3)
        self.transition_risk = transition_risk
        self.expected_regime_duration = expected_regime_duration
        self.regime_days = regime_days
        self.regime_signal = regime_signal or f"{latest_label} detected"
        self.recent_state_mean_return = recent_state_mean_return
        self.regime_inconsistency_warning = regime_inconsistency_warning
        self.state_statistics = state_statistics if state_statistics is not None else pd.DataFrame(
            [
                {"state_id": 0, "canonical_label": "Bull", "mean_return": 0.012, "expected_volatility": 0.18, "volume_zscore": 1.3},
                {"state_id": 1, "canonical_label": "Neutral", "mean_return": 0.001, "expected_volatility": 0.24, "volume_zscore": 0.1},
                {"state_id": 2, "canonical_label": "Bear", "mean_return": -0.015, "expected_volatility": 0.31, "volume_zscore": -0.8},
            ]
        )
        self.price_frame = price_frame if price_frame is not None else pd.DataFrame({"state_probability": [0.80, 0.84, 0.88, latest_probability]})
