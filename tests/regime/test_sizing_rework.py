from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from src.regime import cli
from src.regime.agents.fundamental_agent import FundamentalAgent
from src.regime.event_bus import AsyncEventBus
from src.regime.events import EnrichedSignalEvent
from src.regime.meta_labeler import META_FEATURES, MetaLabelerConfig, MetaLabelerEngine
from src.regime.probability_calibration import ProbabilityCalibrator
from src.regime.signals import compute_position_size


class _StubModel:
    feature_importances_ = np.zeros(len(META_FEATURES))

    def __init__(self, probability: float) -> None:
        self.probability = float(probability)

    def predict_proba(self, _frame):
        return np.array([[1.0 - self.probability, self.probability]])


def _ready_engine(probability: float, *, positive_rate_train: float | None = None) -> MetaLabelerEngine:
    engine = MetaLabelerEngine(MetaLabelerConfig(min_training_samples=1))
    engine._model = _StubModel(probability)
    engine._trained = True
    if positive_rate_train is not None:
        engine._training_metrics = {"positive_rate_train": positive_rate_train}
    return engine


def test_position_size_anchors_on_risk_budget_and_ml_multiplier() -> None:
    base = compute_position_size(
        regime_probability=0.95,
        composite_action="Buy",
        risk_reward_ratio=None,
        atr_value=2.0,
        current_price=100.0,
        portfolio_value=100_000.0,
        max_risk_pct=2.0,
    )
    assert base.risk_budget_anchor_pct == pytest.approx(50.0)
    assert base.suggested_dollars == pytest.approx(50_000.0)
    assert base.ml_sizing_multiplier is None
    assert "risk-budget:" in base.sizing_rationale

    half = compute_position_size(
        regime_probability=0.95,
        composite_action="Buy",
        risk_reward_ratio=None,
        atr_value=2.0,
        current_price=100.0,
        portfolio_value=100_000.0,
        max_risk_pct=2.0,
        meta_labeler_probability=0.50,
    )
    assert half.ml_sizing_multiplier == pytest.approx(0.75)
    assert half.suggested_dollars == pytest.approx(37_500.0)

    full = compute_position_size(
        regime_probability=0.10,
        composite_action="Buy",
        risk_reward_ratio=None,
        atr_value=2.0,
        current_price=100.0,
        portfolio_value=100_000.0,
        max_risk_pct=2.0,
        meta_labeler_probability=1.0,
    )
    assert full.ml_sizing_multiplier == pytest.approx(1.0)
    assert full.suggested_dollars == pytest.approx(50_000.0)


def test_position_size_hold_and_sell_are_zero() -> None:
    for action in ("Hold", "Sell", "Strong Sell"):
        sized = compute_position_size(
            regime_probability=0.99,
            composite_action=action,
            risk_reward_ratio=10.0,
            atr_value=1.0,
            current_price=100.0,
            portfolio_value=100_000.0,
            meta_labeler_probability=1.0,
        )
        assert sized.suggested_pct == 0.0
        assert sized.suggested_dollars == 0.0
        assert sized.kelly_fraction is None


def test_position_size_kelly_uses_ml_probability_only() -> None:
    no_ml = compute_position_size(
        regime_probability=0.95,
        composite_action="Buy",
        risk_reward_ratio=2.0,
        atr_value=2.0,
        current_price=100.0,
        portfolio_value=100_000.0,
    )
    assert no_ml.kelly_fraction is None

    with_ml = compute_position_size(
        regime_probability=0.95,
        composite_action="Buy",
        risk_reward_ratio=2.0,
        atr_value=2.0,
        current_price=100.0,
        portfolio_value=100_000.0,
        meta_labeler_probability=0.55,
    )
    assert with_ml.kelly_fraction == pytest.approx(0.1625)
    assert with_ml.kelly_cap_pct == pytest.approx(16.25)
    assert with_ml.suggested_pct == pytest.approx(16.25, abs=0.1)
    assert "Half-Kelly advisory cap applied" in with_ml.sizing_rationale


def test_position_size_concentration_adjustments_still_apply_after_anchor() -> None:
    result = compute_position_size(
        regime_probability=0.85,
        composite_action="Buy",
        risk_reward_ratio=None,
        atr_value=2.0,
        current_price=100.0,
        portfolio_value=100_000.0,
        regime_exposure={"Bull": 0.95, "Neutral": 0.03, "Bear": 0.02},
        sector_exposure_pct=45.0,
        correlation_penalty=0.10,
    )
    assert result.risk_budget_anchor_pct == pytest.approx(50.0)
    assert result.portfolio_adjustment < 1.0
    assert result.suggested_pct < 50.0
    assert result.adjustment_rationale is not None


def test_meta_labeler_uses_base_rate_relative_thresholds() -> None:
    features = {feature: 1.0 for feature in META_FEATURES}

    veto = _ready_engine(0.31, positive_rate_train=0.42).analyze("NVDA", features, None)
    assert veto.signal == "veto"
    assert veto.details["threshold_mode"] == "base_rate_relative"
    assert veto.details["veto_threshold"] == pytest.approx(0.32)

    neutral = _ready_engine(0.40, positive_rate_train=0.42).analyze("NVDA", features, None)
    assert neutral.signal == "neutral"

    confirm = _ready_engine(0.57, positive_rate_train=0.42).analyze("NVDA", features, None)
    assert confirm.signal == "confirm"
    assert confirm.details["confirm_threshold"] == pytest.approx(0.57)


def test_meta_labeler_without_training_metrics_uses_absolute_fallback() -> None:
    result = _ready_engine(0.49).analyze("NVDA", {feature: 1.0 for feature in META_FEATURES}, None)
    assert result.signal == "veto"
    assert result.details["threshold_mode"] == "absolute_fallback"
    assert result.details["veto_threshold"] == pytest.approx(0.50)


def test_meta_labeler_save_load_preserves_positive_rate_metadata(tmp_path) -> None:
    rows = 80
    frame = pd.DataFrame(
        {
            "canonical_state": [index % 3 for index in range(rows)],
            "return": np.linspace(-0.02, 0.02, rows),
            "volatility": np.linspace(0.10, 0.30, rows),
            "volume_zscore": np.linspace(-1.0, 1.0, rows),
            "vix": np.linspace(18.0, 24.0, rows),
            "vix_change": np.linspace(-0.2, 0.2, rows),
            "yield_10y": np.linspace(4.0, 4.5, rows),
            "yield_10y_change": np.linspace(-0.02, 0.02, rows),
            "barrier_outcome": [1 if index % 4 in (0, 1) else 0 for index in range(rows)],
        }
    )
    engine = MetaLabelerEngine(MetaLabelerConfig(n_estimators=5, min_training_samples=20, n_folds=2))
    metrics = engine.train(frame)
    assert metrics["status"] == "trained"
    target = tmp_path / "meta_labeler_v1.json"
    saved = engine.save_model(str(target))
    assert saved["metadata_path"] is not None

    loaded = MetaLabelerEngine()
    load_result = loaded.load_model(str(target))
    assert load_result["training_metrics"]["positive_rate_train"] == pytest.approx(metrics["positive_rate_train"])


def test_size_only_mode_does_not_block_entry_in_cli_provider(monkeypatch) -> None:
    date = pd.Timestamp("2026-01-02")
    signal = cli.PipelineSignal(
        date=date.date().isoformat(),
        regime="Bull",
        probability=0.80,
        composite_action="Buy",
        composite_strength=0.75,
        expected_duration=12.0,
        transition_risk=0.10,
        regime_days=4,
    )

    class BaseProvider:
        def __call__(self, *_args, **_kwargs):
            return signal

    class Engine:
        def analyze(self, *_args, **_kwargs):
            return SimpleNamespace(signal="veto", confidence=0.10, details={})

    history = pd.DataFrame(
        {
            "price": [100.0, 101.0],
            "volume": [1_000_000.0, 1_000_000.0],
            "vix": [20.0, 20.1],
            "yield_10y": [4.0, 4.0],
        },
        index=pd.date_range("2026-01-01", periods=2),
    )
    monkeypatch.setattr(cli, "_ProductionSignalProvider", lambda: BaseProvider())
    gated = cli._MetaLabelerVetoProvider(Engine(), veto_mode="gate")("NVDA", date, history, cli.PipelineBacktestConfig(), None)
    size_only = cli._MetaLabelerVetoProvider(Engine(), veto_mode="size_only")("NVDA", date, history, cli.PipelineBacktestConfig(), None)
    assert gated.composite_action == "Hold"
    assert size_only.composite_action == "Buy"
    # size_only must attach the probability so it reaches entry sizing;
    # otherwise the mode is indistinguishable from disabling the labeler.
    assert size_only.meta_labeler_probability == pytest.approx(0.10)
    assert gated.meta_labeler_probability is None


def test_size_only_probability_scales_backtest_entry_quantity() -> None:
    from src.regime.pipeline_backtest import PipelineBacktestConfig, PipelineSignal, _build_entry_order

    config = PipelineBacktestConfig(
        sizing_method="equal_dollar",
        starting_cash=10_000.0,
        max_position_pct=1.0,
    )

    def _signal(ml_probability: float | None) -> PipelineSignal:
        return PipelineSignal(
            date="2026-01-02",
            regime="Bull",
            probability=0.80,
            composite_action="Buy",
            composite_strength=0.75,
            expected_duration=12.0,
            transition_risk=0.10,
            regime_days=4,
            meta_labeler_probability=ml_probability,
        )

    baseline = _build_entry_order("NVDA", _signal(None), {}, 100.0, 10_000.0, config)
    scaled_low = _build_entry_order("NVDA", _signal(0.0), {}, 100.0, 10_000.0, config)
    scaled_mid = _build_entry_order("NVDA", _signal(0.5), {}, 100.0, 10_000.0, config)
    scaled_full = _build_entry_order("NVDA", _signal(1.0), {}, 100.0, 10_000.0, config)
    assert baseline["quantity"] == 100
    assert scaled_low["quantity"] == 50   # 0.5x floor
    assert scaled_mid["quantity"] == 75   # 0.75x
    assert scaled_full["quantity"] == 100  # 1.0x




def test_size_only_mode_does_not_block_low_score_in_fundamental_agent() -> None:
    runtime = {
        "get_setting": lambda key: "size_only" if key == "meta_labeler_veto_mode" else "false" if key == "fundamental_gate_enabled" else None,
        "build_qualitative_assessment": lambda **kwargs: SimpleNamespace(
            catalyst_sentiment="Neutral",
            catalysts=[],
            llm_response={"institutional_report": {"verdict": "Buy", "confidence_score": 7, "moat_classification": "wide"}},
            source="vader_fallback",
            llm_used=False,
        ),
    }
    agent = FundamentalAgent(AsyncEventBus(), runtime=runtime)
    result = agent._evaluate(
        runtime,
        EnrichedSignalEvent(
            ticker="NVDA",
            source="quant_agent",
            regime_label="Bull",
            regime_probability=0.80,
            composite_action="Buy",
            meta_labeler_score=0.10,
        ),
    )
    assert result is not None
    assert result.vetoed is False
    assert result.meta_labeler_score == pytest.approx(0.10)


def test_size_only_mode_disables_llm_meta_override(monkeypatch) -> None:
    from src.regime import llm_layer

    monkeypatch.setattr(llm_layer, "analyze_catalysts", lambda *args, **kwargs: ([], 0, "Neutral"))
    monkeypatch.setattr(llm_layer, "request_frontier_decision", lambda *args, **kwargs: {"institutional_report": {"verdict": "Buy", "confidence_score": 7}})
    monkeypatch.setattr(llm_layer, "_meta_labeler_override_enabled", lambda: False)
    result = llm_layer.build_qualitative_assessment(
        ticker="NVDA",
        regime_signal="Bull",
        state_name="Bull",
        latest_probability=0.8,
        frontier_enabled=True,
        meta_labeler_score=0.10,
    )
    assert result.source == "llm"
