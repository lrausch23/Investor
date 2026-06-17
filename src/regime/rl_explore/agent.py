from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

import numpy as np


@dataclass
class RLAgentConfig:
    feature_count: int = 3
    learning_rate: float = 0.05
    exploration_sigma: float = 0.20
    baseline_decay: float = 0.90

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class SoftmaxLinearAgent:
    """Small dependency-free policy used for hypothesis generation.

    The policy scores each non-cash asset with a shared linear model over
    trailing features and scores cash with a separate scalar bias. The action
    is a softmax target weight vector: [cash, asset_1, ..., asset_K].
    """

    def __init__(
        self,
        config: RLAgentConfig | None = None,
        *,
        rng: np.random.Generator | None = None,
        weights: np.ndarray | None = None,
        cash_bias: float = 0.0,
        baseline: float = 0.0,
        episodes_seen: int = 0,
    ) -> None:
        self.config = config or RLAgentConfig()
        self.weights = np.asarray(weights, dtype=float) if weights is not None else np.zeros(self.config.feature_count, dtype=float)
        self.cash_bias = float(cash_bias)
        self.baseline = float(baseline)
        self.episodes_seen = int(episodes_seen)
        self._episode_noise: tuple[np.ndarray, float] | None = None
        self._rng = rng or np.random.default_rng(0)

    def begin_episode(self, rng: np.random.Generator | None = None, *, train: bool = True) -> None:
        source = rng or self._rng
        if train:
            self._episode_noise = (
                source.normal(0.0, self.config.exploration_sigma, size=self.weights.shape),
                float(source.normal(0.0, self.config.exploration_sigma)),
            )
        else:
            self._episode_noise = None

    def act(self, features: np.ndarray, *, rng: np.random.Generator | None = None, train: bool = True) -> np.ndarray:
        if train and self._episode_noise is None:
            self.begin_episode(rng, train=True)
        weights = self.weights
        cash_bias = self.cash_bias
        if train and self._episode_noise is not None:
            weights = weights + self._episode_noise[0]
            cash_bias = cash_bias + self._episode_noise[1]
        asset_logits = np.asarray(features, dtype=float) @ weights
        logits = np.concatenate([[cash_bias], asset_logits])
        logits = logits - float(np.max(logits))
        exp = np.exp(np.clip(logits, -50.0, 50.0))
        total = float(exp.sum())
        if total <= 0:
            out = np.zeros(len(logits), dtype=float)
            out[0] = 1.0
            return out
        return exp / total

    def learn_from_episode(self, terminal_log_wealth: float) -> dict[str, Any]:
        self.episodes_seen += 1
        if self.episodes_seen == 1:
            self.baseline = float(terminal_log_wealth)
        advantage = float(terminal_log_wealth - self.baseline)
        if self._episode_noise is not None and advantage > 0:
            scale = self.config.learning_rate * advantage / max(self.config.exploration_sigma, 1e-9)
            self.weights = self.weights + scale * self._episode_noise[0]
            self.cash_bias = self.cash_bias + scale * self._episode_noise[1]
        self.baseline = self.config.baseline_decay * self.baseline + (1.0 - self.config.baseline_decay) * float(terminal_log_wealth)
        self._episode_noise = None
        return {
            "advantage": advantage,
            "baseline": self.baseline,
            "episodes_seen": self.episodes_seen,
        }

    def to_state(self) -> dict[str, Any]:
        return {
            "schema": "rl_explore_softmax_linear_agent.v1",
            "config": self.config.to_dict(),
            "weights": self.weights.tolist(),
            "cash_bias": self.cash_bias,
            "baseline": self.baseline,
            "episodes_seen": self.episodes_seen,
        }

    @classmethod
    def from_state(cls, state: dict[str, Any], *, rng: np.random.Generator | None = None) -> "SoftmaxLinearAgent":
        config = RLAgentConfig(**dict(state.get("config") or {}))
        return cls(
            config,
            rng=rng,
            weights=np.asarray(state.get("weights") or [0.0] * config.feature_count, dtype=float),
            cash_bias=float(state.get("cash_bias") or 0.0),
            baseline=float(state.get("baseline") or 0.0),
            episodes_seen=int(state.get("episodes_seen") or 0),
        )
