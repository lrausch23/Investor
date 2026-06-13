from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

import pandas as pd


SignalValue = float | str | int | bool | None
SignalMap = dict[str, dict[str, SignalValue]]


@dataclass(frozen=True)
class ExposureOverride:
    """Machine-readable override emitted by defensive strategy layers."""

    exposure_cap: float | None = None
    exclude_tickers: tuple[str, ...] = ()
    reasons: dict[str, str] = field(default_factory=dict)
    reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "exposure_cap": self.exposure_cap,
            "exclude_tickers": list(self.exclude_tickers),
            "reasons": dict(self.reasons),
            "reason": self.reason,
        }


@runtime_checkable
class SignalProvider(Protocol):
    """Leak-free source of per-ticker signals available through decision date T."""

    def prepare(self, ticker: str, frame: pd.DataFrame) -> None:
        """Precompute any series for one ticker without using data after each row."""

    def signals(self, ticker: str, date: pd.Timestamp) -> dict[str, SignalValue]:
        """Return signals for ticker/date using information available at that date."""


@runtime_checkable
class ExposurePolicy(Protocol):
    """Portfolio-level equity fraction policy, always long-only and in [0, 1]."""

    def target_exposure(self, date: pd.Timestamp, portfolio_state: dict[str, Any], signal_map: SignalMap) -> float:
        """Return target equity exposure for the next fill."""


@runtime_checkable
class OverridePolicy(Protocol):
    """Defensive override that can exclude names or cap total exposure."""

    def override(self, date: pd.Timestamp, portfolio_state: dict[str, Any], signal_map: SignalMap) -> ExposureOverride | None:
        """Return an override for the next fill, or None when no override applies."""


@runtime_checkable
class AllocationPolicy(Protocol):
    """Within-sleeve name allocation policy."""

    def weights(self, date: pd.Timestamp, eligible_names: list[str], signal_map: SignalMap) -> dict[str, float]:
        """Return weights over eligible names that sum to 1.0 when any are eligible."""


@runtime_checkable
class RebalancePolicy(Protocol):
    """Controls scheduled rebalances and drift-band rebalances."""

    def should_rebalance(self, date: pd.Timestamp, drift_state: dict[str, Any]) -> bool:
        """Return True when the engine should schedule a next-open rebalance."""
