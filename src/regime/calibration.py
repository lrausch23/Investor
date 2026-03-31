from __future__ import annotations

import logging
from dataclasses import dataclass

from .scenarios import get_scenario
from .stress_test import StressTestConfig, run_stress_test

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class GuardrailCalibrationResult:
    guardrail_name: str
    enabled_return: float
    disabled_return: float
    enabled_drawdown: float
    disabled_drawdown: float
    enabled_sharpe: float | None
    disabled_sharpe: float | None
    impact_pct: float
    recommendation: str


def _recommendation(impact_pct: float, enabled_drawdown: float, disabled_drawdown: float) -> str:
    if impact_pct > 0:
        return "keep"
    if impact_pct < -5.0 and enabled_drawdown >= disabled_drawdown:
        return "reduce"
    if impact_pct < -2.0 and enabled_drawdown < disabled_drawdown:
        return "keep"
    return "keep"


def run_guardrail_calibration(
    scenario_id: str,
    ticker: str | None = None,
) -> list[GuardrailCalibrationResult]:
    scenario = get_scenario(scenario_id)
    base = StressTestConfig(scenario_id=scenario.scenario_id, tickers=[ticker] if ticker else None)
    pairs = [
        ("hurdle_rate", "hurdle_rate_enabled"),
        ("duration_gate", "duration_gate_enabled"),
        ("anti_churn", "anti_churn_enabled"),
        ("ltcg_override", "ltcg_override_enabled"),
        ("fundamental_gate", "fundamental_gate_enabled"),
    ]
    results: list[GuardrailCalibrationResult] = []
    for guardrail_name, field_name in pairs:
        enabled_payload = {**base.__dict__, field_name: True}
        disabled_payload = {**base.__dict__, field_name: False}
        logger.info("Stress-test calibration running %s enabled scenario=%s", guardrail_name, scenario_id)
        enabled = run_stress_test(StressTestConfig(**enabled_payload))
        logger.info("Stress-test calibration running %s disabled scenario=%s", guardrail_name, scenario_id)
        disabled = run_stress_test(StressTestConfig(**disabled_payload))
        impact_pct = (enabled.portfolio_total_return - disabled.portfolio_total_return) * 100.0
        results.append(
            GuardrailCalibrationResult(
                guardrail_name=guardrail_name,
                enabled_return=enabled.portfolio_total_return,
                disabled_return=disabled.portfolio_total_return,
                enabled_drawdown=enabled.portfolio_max_drawdown,
                disabled_drawdown=disabled.portfolio_max_drawdown,
                enabled_sharpe=enabled.portfolio_sharpe,
                disabled_sharpe=disabled.portfolio_sharpe,
                impact_pct=impact_pct,
                recommendation=_recommendation(impact_pct, enabled.portfolio_max_drawdown, disabled.portfolio_max_drawdown),
            )
        )
    return results
