from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class StrategySpec:
    """Serializable composition of strategy layers.

    The engine reads only these layer names and params, so campaign results can
    be attributed to a stable content hash.
    """

    name: str
    signal_provider: str = "price_history"
    signal_params: dict[str, Any] = field(default_factory=dict)
    exposure_policy: str = "always_full"
    exposure_params: dict[str, Any] = field(default_factory=dict)
    override_policy: str | None = None
    override_params: dict[str, Any] = field(default_factory=dict)
    allocation_policy: str = "equal_weight"
    allocation_params: dict[str, Any] = field(default_factory=dict)
    rebalance_policy: str = "monthly_bands"
    rebalance_params: dict[str, Any] = field(default_factory=dict)
    description: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "signal_provider": self.signal_provider,
            "signal_params": dict(self.signal_params),
            "exposure_policy": self.exposure_policy,
            "exposure_params": dict(self.exposure_params),
            "override_policy": self.override_policy,
            "override_params": dict(self.override_params),
            "allocation_policy": self.allocation_policy,
            "allocation_params": dict(self.allocation_params),
            "rebalance_policy": self.rebalance_policy,
            "rebalance_params": dict(self.rebalance_params),
            "description": self.description,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "StrategySpec":
        return cls(
            name=str(payload.get("name") or ""),
            signal_provider=str(payload.get("signal_provider") or "price_history"),
            signal_params=dict(payload.get("signal_params") or {}),
            exposure_policy=str(payload.get("exposure_policy") or "always_full"),
            exposure_params=dict(payload.get("exposure_params") or {}),
            override_policy=(str(payload["override_policy"]) if payload.get("override_policy") else None),
            override_params=dict(payload.get("override_params") or {}),
            allocation_policy=str(payload.get("allocation_policy") or "equal_weight"),
            allocation_params=dict(payload.get("allocation_params") or {}),
            rebalance_policy=str(payload.get("rebalance_policy") or "monthly_bands"),
            rebalance_params=dict(payload.get("rebalance_params") or {}),
            description=str(payload.get("description") or ""),
        )

    @property
    def hash(self) -> str:
        canonical = json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
