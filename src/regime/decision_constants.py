from __future__ import annotations

import hashlib
import json
from typing import Any

COMPOSITE_AGREEMENT_BOOST = 0.15
COMPOSITE_CONFLICT_PENALTY = 0.20
DEFAULT_NEUTRAL_BULL_TILT_PROBABILITY = 0.40
DEFAULT_EXIT_TIME_STOP_DAYS = 21
DEFAULT_NEUTRAL_REDUCE_FRACTION = 0.5
TRAILING_STOP_ACTIVATION_ATR = 1.0
META_LABELER_VETO_MARGIN = 0.10
META_LABELER_CONFIRM_MARGIN = 0.15


def decision_constants_payload() -> dict[str, Any]:
    return {
        "composite_agreement_boost": float(COMPOSITE_AGREEMENT_BOOST),
        "composite_conflict_penalty": float(COMPOSITE_CONFLICT_PENALTY),
        "default_neutral_bull_tilt_probability": float(DEFAULT_NEUTRAL_BULL_TILT_PROBABILITY),
        "default_exit_time_stop_days": int(DEFAULT_EXIT_TIME_STOP_DAYS),
        "default_neutral_reduce_fraction": float(DEFAULT_NEUTRAL_REDUCE_FRACTION),
        "trailing_stop_activation_atr": float(TRAILING_STOP_ACTIVATION_ATR),
        "meta_labeler_veto_margin": float(META_LABELER_VETO_MARGIN),
        "meta_labeler_confirm_margin": float(META_LABELER_CONFIRM_MARGIN),
    }


def decision_constants_version() -> str:
    payload = json.dumps(decision_constants_payload(), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]
