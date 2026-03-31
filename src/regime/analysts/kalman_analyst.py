"""Kalman Filter Noise Canceller for HMM transition probability smoothing."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np

from ..ensemble import AnalystBase, AnalystResult


@dataclass(frozen=True)
class KalmanConfig:
    state_dim: int = 3
    process_noise: float = 0.01
    measurement_noise: float = 0.1
    min_observations: int = 5


class KalmanFilterAnalyst(AnalystBase):
    def __init__(self, config: KalmanConfig | None = None):
        self._config = config or KalmanConfig()
        self._max_history = 20
        self.reset()

    @property
    def name(self) -> str:
        return "kalman_filter"

    def is_ready(self) -> bool:
        return self._n_obs >= self._config.min_observations

    def reset(self) -> None:
        dim = self._config.state_dim
        self._x = np.array([1.0 / dim] * dim, dtype=float)
        self._P = np.eye(dim, dtype=float)
        self._Q = np.eye(dim, dtype=float) * self._config.process_noise
        self._R = np.eye(dim, dtype=float) * self._config.measurement_noise
        self._F = np.eye(dim, dtype=float)
        self._H = np.eye(dim, dtype=float)
        self._n_obs = 0
        self._state_history: list[str] = []

    def update(self, observation: np.ndarray) -> np.ndarray:
        x_pred = self._F @ self._x
        p_pred = self._F @ self._P @ self._F.T + self._Q
        innovation = observation - self._H @ x_pred
        s_matrix = self._H @ p_pred @ self._H.T + self._R
        gain = p_pred @ self._H.T @ np.linalg.inv(s_matrix)
        self._x = x_pred + gain @ innovation
        self._P = (np.eye(self._config.state_dim) - gain @ self._H) @ p_pred
        self._x = np.clip(self._x, 0.0, 1.0)
        total = float(self._x.sum())
        if total > 0:
            self._x = self._x / total
        self._n_obs += 1
        return self._x.copy()

    def _whipsaw_penalty(self) -> tuple[float, int]:
        if len(self._state_history) < 3:
            return 1.0, 0
        transitions = sum(
            1
            for index in range(1, len(self._state_history))
            if self._state_history[index] != self._state_history[index - 1]
        )
        if transitions <= 2:
            return 1.0, transitions
        if transitions <= 5:
            return 0.85, transitions
        return 0.65, transitions

    def _observation_from_regime(self, regime_result: Any) -> np.ndarray:
        raw = getattr(regime_result, "posterior_probs", None)
        if raw is not None:
            arr = np.asarray(raw)
            if arr.ndim == 2 and arr.shape[1] >= 3:
                return arr[-1, :3].astype(float)
            if arr.ndim == 1 and arr.shape[0] >= 3:
                return arr[:3].astype(float)
        label = str(getattr(regime_result, "latest_label", "") or "").lower()
        if label == "bull":
            return np.array([1.0, 0.0, 0.0])
        if label == "bear":
            return np.array([0.0, 0.0, 1.0])
        return np.array([0.0, 1.0, 0.0])

    def analyze(self, ticker: str, features: dict[str, float], regime_result: Any) -> AnalystResult:
        del ticker, features
        if regime_result is None:
            return AnalystResult(
                analyst_name=self.name,
                confidence=0.5,
                signal="neutral",
                details={"note": "regime_result unavailable", "observations": self._n_obs},
            )
        observation = self._observation_from_regime(regime_result)
        smoothed = self.update(observation)
        dominant_idx = int(np.argmax(smoothed))
        dominant = ("Bull", "Neutral", "Bear")[dominant_idx]
        self._state_history.append(dominant)
        self._state_history = self._state_history[-self._max_history :]
        penalty, transitions = self._whipsaw_penalty()
        confidence = float(smoothed[dominant_idx]) * penalty
        if dominant == "Bear" and confidence > 0.6:
            signal = "veto"
        elif dominant == "Bull" and confidence > 0.6:
            signal = "confirm"
        else:
            signal = "neutral"
        return AnalystResult(
            analyst_name=self.name,
            confidence=confidence,
            signal=signal,
            details={
                "smoothed_probs": {"bull": float(smoothed[0]), "neutral": float(smoothed[1]), "bear": float(smoothed[2])},
                "raw_probs": {"bull": float(observation[0]), "neutral": float(observation[1]), "bear": float(observation[2])},
                "dominant_state": dominant,
                "whipsaw_penalty": penalty,
                "transitions_recent": transitions,
                "observations": self._n_obs,
            },
        )

    def train(self, labeled_frame: Any, **kwargs: Any) -> dict[str, Any]:
        del labeled_frame, kwargs
        return {
            "status": "online_filter",
            "analyst": self.name,
            "note": "Kalman filter is an online algorithm — no offline training needed.",
            "observations": self._n_obs,
        }

    def get_state(self) -> dict[str, Any]:
        return {
            "x": self._x.tolist(),
            "P": self._P.tolist(),
            "n_obs": self._n_obs,
            "state_history": list(self._state_history),
        }

    def load_state(self, state: dict[str, Any]) -> None:
        self._x = np.array(state["x"], dtype=float)
        self._P = np.array(state["P"], dtype=float)
        self._n_obs = int(state["n_obs"])
        self._state_history = list(state.get("state_history", []))
