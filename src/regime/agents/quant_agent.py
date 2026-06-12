from __future__ import annotations

import asyncio
import datetime as dt
import logging
from types import SimpleNamespace
from typing import Any

from . import AgentBase
from ..events import AnalysisRequestEvent, BaseEvent, enriched_signal_from_payload

logger = logging.getLogger(__name__)


def _enabled_from_setting(runtime: dict[str, Any], analyst_name: str, ready: bool) -> bool:
    raw = runtime["get_setting"](f"ensemble_analyst_{analyst_name}_enabled")
    if raw is None or str(raw).strip() == "":
        return ready
    return str(raw).strip().lower() in {"true", "1", "yes", "on"}


def _weight_from_setting(runtime: dict[str, Any], analyst_name: str) -> float:
    raw = runtime["get_setting"](f"ensemble_analyst_{analyst_name}_weight")
    try:
        return max(0.0, min(5.0, float(raw if raw not in (None, "") else 1.0)))
    except Exception:
        return 1.0


def _ensemble_config_from_runtime(runtime: dict[str, Any], registry: Any) -> Any:
    config_cls = runtime["EnsembleConfig"]
    weights: dict[str, float] = {}
    for analyst_name in registry.list_analysts():
        analyst = registry.get(analyst_name)
        if analyst is None:
            continue
        ready = bool(callable(getattr(analyst, "is_ready", None)) and analyst.is_ready())
        if not ready:
            continue
        if _enabled_from_setting(runtime, analyst_name, ready):
            weights[str(analyst_name)] = _weight_from_setting(runtime, analyst_name)
    return config_cls(
        veto_threshold=float(runtime["get_setting"]("ensemble_veto_threshold") or 0.50),
        confirm_threshold=float(runtime["get_setting"]("ensemble_confirm_threshold") or 0.65),
        aggregation_method=str(runtime["get_setting"]("ensemble_aggregation_method") or "mean"),
        analyst_weights=weights,
    )


class QuantAgent(AgentBase):
    @property
    def name(self) -> str:
        return "quant"

    @property
    def subscriptions(self) -> list[str]:
        return ["analysis_request"]

    async def handle(self, event: BaseEvent) -> None:
        if not isinstance(event, AnalysisRequestEvent):
            return
        runtime = self._get_runtime()
        if runtime is None:
            logger.warning("QuantAgent skipped analysis_request: runtime unavailable")
            return
        for ticker in tuple(event.tickers or ()):
            try:
                enriched = await asyncio.to_thread(self._analyze_one, runtime, event, str(ticker).upper())
            except Exception as exc:
                logger.error("QuantAgent failed for %s: %s", ticker, exc)
                continue
            if enriched is not None:
                await self._bus.publish(enriched)

    def _analyze_one(
        self,
        runtime: dict[str, Any],
        request: AnalysisRequestEvent,
        ticker: str,
    ) -> Any:
        market_frame = runtime["download_market_frame"](ticker=ticker, period=request.period or "3y", interval="1d").frame
        regime = runtime["fit_regime_model"](ticker=ticker, market_frame=market_frame)
        forward_curve = runtime["forward_regime_curve"](
            regime.transition_matrix,
            regime.latest_state_vector,
            horizon=21,
        )
        earnings_date = runtime["get_next_earnings_date"](ticker) if callable(runtime.get("get_next_earnings_date")) else None
        try:
            forward_signal = runtime["signal_from_forward_curve"](
                forward_curve,
                regime.latest_label,
                regime.transition_risk,
                regime.expected_regime_duration,
                regime.latest_probability,
                earnings_date,
            )
        except TypeError:
            forward_signal = runtime["signal_from_forward_curve"](
                forward_curve,
                regime.latest_label,
                regime.transition_risk,
                regime.expected_regime_duration,
                regime.latest_probability,
            )
        technicals = runtime["compute_technicals"](
            market_frame["price"],
            market_frame["volume"],
            market_frame["high"] if "high" in market_frame.columns else None,
            market_frame["low"] if "low" in market_frame.columns else None,
        )
        technical_signal = runtime["intra_regime_signal"](technicals, regime.latest_label)
        composite_signal = runtime["build_composite_signal"](
            regime.latest_label,
            regime.latest_probability,
            forward_signal,
            technical_signal,
        )

        compute_price_targets = runtime.get("compute_price_targets")
        if callable(compute_price_targets):
            price_targets = compute_price_targets(
                current_price=float(getattr(regime, "latest_price", 0.0) or 0.0),
                technicals_df=technicals,
                composite_signal=composite_signal,
                expected_duration=float(getattr(regime, "expected_regime_duration", 0.0) or 0.0),
                state_mean_return=float(getattr(regime, "recent_state_mean_return", 0.0) or 0.0),
            )
        else:
            price_targets = SimpleNamespace(
                current_price=float(getattr(regime, "latest_price", 0.0) or 0.0),
                entry_price=None,
                exit_price=None,
                stop_price=None,
                risk_reward_ratio=None,
                timeframe_days=int(getattr(forward_signal, "expected_holding_days", 0) or 0),
                atr_value=None,
            )

        compute_unified_confidence = runtime.get("compute_unified_confidence")
        if callable(compute_unified_confidence):
            calibrator = None
            get_setting = runtime.get("get_setting")
            if callable(get_setting) and str(get_setting("regime_probability_calibrated") or "").strip().lower() in {"1", "true", "yes", "on"}:
                load_regime_calibrator = runtime.get("load_regime_calibrator")
                if callable(load_regime_calibrator):
                    calibrator = load_regime_calibrator(regime.latest_label)
            confidence = compute_unified_confidence(
                float(regime.latest_probability),
                float(getattr(composite_signal, "composite_strength", 0.0) or 0.0),
                calibrator=calibrator,
            )
        else:
            confidence = SimpleNamespace(
                value=float(regime.latest_probability or 0.0) * 100.0,
                label="Uncalibrated",
                calibrated=False,
                components={},
            )

        meta_score = None
        ensemble_verdict = None
        registry = runtime["get_registry"]() if callable(runtime.get("get_registry")) else None
        extract_meta_features = runtime.get("extract_meta_features")
        aggregate_analysts = runtime.get("aggregate_analysts")
        if registry is not None and callable(extract_meta_features) and callable(aggregate_analysts):
            try:
                from ..meta_labeler import meta_labeler_gate_enabled, meta_labeler_result_can_influence

                meta_gate_enabled = meta_labeler_gate_enabled(runtime.get("get_setting"))
                feature_row = dict(regime.price_frame.iloc[-1].to_dict())
                feature_row.update(
                    {
                        "current_price": float(getattr(regime, "latest_price", 0.0) or 0.0),
                        "transition_risk": float(getattr(regime, "transition_risk", 0.0) or 0.0),
                        "regime_days": int(getattr(regime, "regime_days", 1) or 1),
                        "composite_strength": float(getattr(composite_signal, "composite_strength", 0.0) or 0.0),
                        "composite_action": str(getattr(composite_signal, "composite_action", "") or ""),
                        "price_targets": dict(getattr(price_targets, "__dict__", {}) or {}),
                    }
                )
                try:
                    latest_technicals = technicals.dropna().iloc[-1]
                    feature_row["rsi_14"] = latest_technicals.get("rsi_14")
                    feature_row["macd_histogram"] = latest_technicals.get("macd_histogram")
                except Exception:
                    pass
                features = extract_meta_features(feature_row)
                analyst_results = []
                for analyst_name in registry.list_analysts():
                    analyst = registry.get(analyst_name)
                    if analyst is None or not callable(getattr(analyst, "is_ready", None)) or not analyst.is_ready():
                        continue
                    if not _enabled_from_setting(runtime, analyst_name, True):
                        continue
                    result = analyst.analyze(ticker=ticker, features=features, regime_result=regime)
                    if str(getattr(result, "analyst_name", "")) == "xgboost_meta_labeler":
                        if meta_labeler_result_can_influence(result):
                            meta_score = float(getattr(result, "confidence", 0.0) or 0.0)
                        if not meta_gate_enabled:
                            continue
                        if not meta_labeler_result_can_influence(result):
                            continue
                    analyst_results.append(result)
                if analyst_results:
                    ensemble_verdict = aggregate_analysts(
                        analyst_results,
                        _ensemble_config_from_runtime(runtime, registry),
                    )
            except Exception:
                logger.debug("QuantAgent ensemble analysis failed for %s", ticker, exc_info=True)

        try:
            latest_volume = float(market_frame["volume"].iloc[-1])
        except Exception:
            latest_volume = None

        return enriched_signal_from_payload(
            ticker=ticker,
            regime_result=regime,
            composite_signal=composite_signal,
            price_targets=price_targets,
            confidence=confidence,
            ensemble_verdict=ensemble_verdict,
            benchmark=str(request.benchmark or ""),
            snapshot_date=dt.date.today().isoformat(),
            source="quant_agent",
            meta_labeler_score=meta_score,
            volume=latest_volume,
            correlation_id=request.correlation_id,
        )
