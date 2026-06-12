from __future__ import annotations

import json
import math
from dataclasses import asdict
from types import SimpleNamespace

import pandas as pd
import pytest

from src.regime import cli
from src.regime import paper_trading
from src.regime.hurdle_rate import check_duration_gate, check_hurdle_rate
from src.regime.pipeline_backtest import (
    PipelineBacktestConfig,
    PipelineSignal,
    compute_equity_metrics,
    pure_check_duration_gate,
    pure_check_hurdle_rate,
    run_pipeline_backtest,
)
from src.regime.paper_trading import trailing_stop_level


def _frame(
    closes: list[float],
    *,
    opens: list[float] | None = None,
    highs: list[float] | None = None,
    lows: list[float] | None = None,
) -> pd.DataFrame:
    dates = pd.bdate_range("2025-01-02", periods=len(closes))
    open_values = opens or closes
    high_values = highs or [max(open_values[index], closes[index]) for index in range(len(closes))]
    low_values = lows or [min(open_values[index], closes[index]) for index in range(len(closes))]
    return pd.DataFrame(
        {
            "open": open_values,
            "high": high_values,
            "low": low_values,
            "price": closes,
            "volume": [1_000_000.0] * len(closes),
            "vix": [20.0] * len(closes),
            "yield_10y": [4.0] * len(closes),
        },
        index=dates,
    )


def _signal(
    date: pd.Timestamp,
    *,
    action: str = "Buy",
    regime: str = "Bull",
    target: float = 200.0,
    stop: float = 50.0,
    atr: float | None = None,
    previous_regime: str | None = None,
    p_bull_day5: float | None = 0.80,
    p_bear_day5: float | None = 0.05,
    expected_duration: float = 10.0,
) -> PipelineSignal:
    return PipelineSignal(
        date=date.date().isoformat(),
        regime=regime,
        probability=0.82 if regime == "Bull" else 0.62,
        composite_action=action,
        composite_strength=0.80,
        expected_duration=expected_duration,
        transition_risk=0.10,
        regime_days=5,
        previous_regime=previous_regime,
        p_bull_day5=p_bull_day5,
        p_bear_day5=p_bear_day5,
        p_neutral_day5=0.20,
        forward_action=action,
        technical_signal="Hold / add on weakness",
        price_targets={
            "entry_price": 100.0,
            "exit_price": target,
            "target_price": target,
            "stop_price": stop,
            "timeframe_days": int(expected_duration),
            "atr_value": atr,
        },
        atr_14=atr,
        beta=1.0,
    )


class _Provider:
    def __init__(self, by_index: dict[int, dict]) -> None:
        self.by_index = by_index

    def __call__(self, ticker, date, history, config, previous_regime):
        del ticker, config
        idx = len(history) - 1
        kwargs = dict(self.by_index.get(idx, self.by_index.get(-1, {"action": "Hold", "regime": "Bull"})))
        kwargs.setdefault("previous_regime", previous_regime)
        return _signal(date, **kwargs)


def _config(**overrides) -> PipelineBacktestConfig:
    defaults = {
        "training_window": 0,
        "sizing_method": "equal_dollar",
        "starting_cash": 10_000.0,
        "max_position_pct": 1.0,
        "enable_hurdle_gate": False,
        "enable_duration_gate": False,
        "enable_anti_churn_gate": False,
        "enable_signal_quality_gate": False,
        "entry_cost_bps": 0.0,
        "exit_cost_bps": 0.0,
    }
    defaults.update(overrides)
    return PipelineBacktestConfig(**defaults)


def test_profit_target_exit_uses_target_price_and_deducts_costs() -> None:
    frame = _frame(
        [100.0, 102.0, 103.0],
        opens=[100.0, 100.0, 103.0],
        highs=[100.0, 104.0, 104.0],
        lows=[100.0, 99.0, 102.0],
    )
    result = run_pipeline_backtest(
        "TEST",
        frame,
        config=_config(entry_cost_bps=10.0, exit_cost_bps=10.0),
        signal_provider=_Provider({0: {"action": "Buy", "target": 103.0, "stop": 95.0}}),
    )
    trade = result.trades[0]
    assert trade["exit_type"] == "target"
    assert trade["exit_price"] == pytest.approx(103.0 * 0.999)
    assert trade["costs_paid"] > 0
    assert result.metrics["total_costs_paid"] > trade["costs_paid"]


def test_trailing_ratchet_locks_gain_and_stop_never_moves_down() -> None:
    assert trailing_stop_level(entry_price=100.0, current_price=120.0, atr_14=5.0, existing_stop=90.0) == 110.0
    assert trailing_stop_level(entry_price=100.0, current_price=112.0, atr_14=5.0, existing_stop=110.0) == 110.0
    # Ratchet is computed from the CLOSE at end of day, effective next bar:
    # day1 closes at 111 -> stop ratchets 90 -> 101 (111 - 2*ATR). Day1's own
    # low (109.5) is tested against the PRIOR stop (90), not the ratcheted one,
    # and day1's high (120) must not produce a same-bar 110 stop. Day2's low
    # (100.5) then touches the ratcheted 101 stop.
    frame = _frame(
        [100.0, 111.0, 101.0],
        opens=[100.0, 100.0, 102.0],
        highs=[100.0, 120.0, 103.0],
        lows=[100.0, 109.5, 100.5],
    )
    result = run_pipeline_backtest(
        "TEST",
        frame,
        config=_config(),
        signal_provider=_Provider({0: {"action": "Buy", "target": 200.0, "stop": 90.0, "atr": 5.0}}),
    )
    trade = result.trades[0]
    assert trade["exit_type"] == "trailing"
    assert trade["exit_price"] == pytest.approx(101.0)
    assert trade["stop_price"] == pytest.approx(101.0)


def test_same_bar_high_does_not_raise_stop_before_low_is_tested() -> None:
    # One huge-range bar: high 130 would ratchet the stop to 120, above the
    # day's low (105). The conservative ordering must NOT exit on that bar at
    # an intraday-ratcheted stop; the position survives with the stop raised
    # from the close for the following day.
    frame = _frame(
        [100.0, 118.0, 118.0],
        opens=[100.0, 100.0, 118.0],
        highs=[100.0, 130.0, 119.0],
        lows=[100.0, 105.0, 117.0],
    )
    result = run_pipeline_backtest(
        "TEST",
        frame,
        config=_config(),
        signal_provider=_Provider({0: {"action": "Buy", "target": 200.0, "stop": 90.0, "atr": 5.0}}),
    )
    # No trailing exit on the wide bar; closed at final mark instead.
    assert [trade["exit_type"] for trade in result.trades] == ["final_mark"]


def test_flat_path_exits_at_exact_time_stop() -> None:
    frame = _frame(
        [100.0, 100.0, 100.0, 100.0, 100.0, 100.0],
        highs=[101.0] * 6,
        lows=[99.0] * 6,
    )
    result = run_pipeline_backtest(
        "TEST",
        frame,
        config=_config(time_stop_days=3),
        signal_provider=_Provider({0: {"action": "Buy", "target": 200.0, "stop": 50.0, "expected_duration": 3.0}}),
    )
    trade = result.trades[0]
    assert trade["exit_type"] == "time"
    assert trade["holding_days"] == 3


def test_stop_wins_when_stop_and_target_touch_same_day() -> None:
    frame = _frame(
        [100.0, 100.0, 100.0],
        opens=[100.0, 100.0, 100.0],
        highs=[100.0, 106.0, 100.0],
        lows=[100.0, 94.0, 100.0],
    )
    result = run_pipeline_backtest(
        "TEST",
        frame,
        config=_config(),
        signal_provider=_Provider({0: {"action": "Buy", "target": 105.0, "stop": 95.0}}),
    )
    trade = result.trades[0]
    assert trade["exit_type"] == "stop"
    assert trade["exit_price"] == pytest.approx(95.0)


def test_neutral_reduce_sells_half_with_floor_semantics() -> None:
    frame = _frame(
        [100.0, 100.0, 100.0, 100.0],
        highs=[101.0] * 4,
        lows=[99.0] * 4,
    )
    result = run_pipeline_backtest(
        "TEST",
        frame,
        config=_config(starting_cash=1_800.0, sizing_method="risk_budget", neutral_reduce_fraction=0.5),
        signal_provider=_Provider(
            {
                0: {"action": "Buy", "regime": "Bull", "target": 200.0, "stop": 50.0},
                -1: {
                    "action": "Hold",
                    "regime": "Neutral",
                    "target": 200.0,
                    "stop": 50.0,
                    "p_bull_day5": 0.40,
                    "p_bear_day5": 0.30,
                },
            }
        ),
    )
    reduce_trade = next(item for item in result.trades if item["exit_type"] == "reduce")
    assert reduce_trade["quantity"] == 4


def test_equity_metrics_use_daily_curve_for_sharpe_and_drawdown() -> None:
    curve = pd.DataFrame(
        {
            "date": ["2025-01-02", "2025-01-03", "2025-01-06"],
            "equity": [100.0, 110.0, 105.0],
            "exposure": [0.0, 1.0, 1.0],
        }
    )
    metrics = compute_equity_metrics(curve, [])
    returns = pd.Series([0.10, 105.0 / 110.0 - 1.0])
    expected_sharpe = (returns.mean() * 252.0) / (returns.std(ddof=1) * math.sqrt(252.0))
    assert metrics["sharpe_ratio"] == pytest.approx(expected_sharpe)
    assert metrics["max_drawdown"] == pytest.approx(105.0 / 110.0 - 1.0)
    assert metrics["exposure_pct"] == pytest.approx(2.0 / 3.0)


def test_config_embedded_and_result_round_trips_json() -> None:
    frame = _frame([100.0, 102.0, 103.0], opens=[100.0, 100.0, 103.0], highs=[100.0, 104.0, 104.0], lows=[100.0, 99.0, 102.0])
    config = _config(oos_start="2025-01-03")
    result = run_pipeline_backtest(
        "TEST",
        frame,
        config=config,
        signal_provider=_Provider({0: {"action": "Buy", "target": 103.0, "stop": 95.0}}),
    )
    payload = json.loads(result.to_json())
    assert payload["config"]["oos_start"] == "2025-01-03"
    assert payload["out_of_sample"] is not None
    assert payload["trades"][0]["exit_type"] == "target"


def test_pure_hurdle_and_duration_formulas_match_production_with_explicit_inputs() -> None:
    pure_hurdle = pure_check_hurdle_rate(
        "nvda",
        100.0,
        112.0,
        estimated_stcg_rate=0.32,
        min_net_return_pct=3.0,
        estimated_execution_cost_pct=0.20,
    )
    production_hurdle = check_hurdle_rate(
        "nvda",
        100.0,
        112.0,
        estimated_stcg_rate=0.32,
        min_net_return_pct=3.0,
        estimated_execution_cost_pct=0.20,
    )
    assert asdict(pure_hurdle) == asdict(production_hurdle)
    pure_duration = pure_check_duration_gate("nvda", 12.0, "Bull", min_regime_duration_days=7.0)
    production_duration = check_duration_gate("nvda", 12.0, "Bull", min_regime_duration_days=7.0)
    assert asdict(pure_duration) == asdict(production_duration)


def test_live_ratchet_uses_same_pure_trailing_formula(monkeypatch) -> None:
    expected = trailing_stop_level(entry_price=100.0, current_price=120.0, atr_14=5.0, existing_stop=90.0)
    monkeypatch.setattr(
        paper_trading,
        "update_paper_position_risk",
        lambda position_id, stop_price: {"id": position_id, "stop_price": stop_price},
    )
    actual = paper_trading._ratchet_trailing_stop({"id": 1, "entry_price": 100.0, "stop_price": 90.0}, 120.0, 5.0)
    assert actual == expected


def test_pipeline_backtest_cli_subcommand_writes_json(monkeypatch, tmp_path, capsys) -> None:
    out = tmp_path / "pipeline.json"
    fake_result = SimpleNamespace(to_json=lambda path: out.write_text('{"ok": true}\n', encoding="utf-8"), to_dict=lambda: {"ok": True})
    monkeypatch.setattr(
        cli,
        "parse_args",
        lambda: SimpleNamespace(command="pipeline-backtest", ticker="NVDA", period="5y", oos_start="2025-01-01", benchmark="SPY", json_path=str(out)),
    )
    monkeypatch.setattr(cli, "run_pipeline_backtest_for_ticker", lambda **kwargs: fake_result)
    cli.main()
    assert json.loads(out.read_text(encoding="utf-8")) == {"ok": True}
    assert str(out) in capsys.readouterr().out


def test_pipeline_backtest_cli_meta_labeler_ab_prints_delta(monkeypatch, capsys) -> None:
    fake_market = SimpleNamespace(frame=_frame([100.0, 101.0, 102.0]))
    baseline = SimpleNamespace(
        metrics={"total_return": 0.10, "sharpe_ratio": 1.2, "max_drawdown": -0.03, "trade_count": 4, "exit_type_counts": {"target": 2}},
        exit_type_counts={"target": 2},
    )
    gated = SimpleNamespace(
        metrics={"total_return": 0.08, "sharpe_ratio": 1.0, "max_drawdown": -0.02, "trade_count": 3, "exit_type_counts": {"target": 1, "time": 1}},
        exit_type_counts={"target": 1, "time": 1},
    )
    calls: list[bool] = []

    def fake_run_pipeline_backtest(*_args, **kwargs):
        calls.append(kwargs.get("signal_provider") is not None)
        return gated if kwargs.get("signal_provider") is not None else baseline

    monkeypatch.setattr(
        cli,
        "parse_args",
        lambda: SimpleNamespace(
            command="pipeline-backtest",
            ticker="NVDA",
            period="5y",
            oos_start="2025-01-01",
            benchmark="SPY",
            json_path=None,
            meta_labeler_ab=True,
        ),
    )
    monkeypatch.setattr(cli, "download_market_frame", lambda **_kwargs: fake_market)
    monkeypatch.setattr(cli, "_load_meta_labeler_for_ab", lambda: object())
    monkeypatch.setattr(cli, "run_pipeline_backtest", fake_run_pipeline_backtest)

    cli.main()

    output = capsys.readouterr().out
    assert "segment,run,total_return,sharpe_ratio,max_drawdown,trade_count,exit_type_counts" in output
    assert "full,baseline,0.100000" in output
    assert "full,meta_veto,0.080000" in output
    assert "full,diff,-0.020000" in output
    assert calls == [False, True]


def test_pipeline_backtest_cli_meta_labeler_ab_formats_is_oos_segments() -> None:
    output = cli._format_meta_labeler_ab_basket(
        {
            "results": [
                {
                    "ticker": "NVDA",
                    "mode_label": "meta_size_only",
                    "baseline": {"total_return": 0.10, "sharpe_ratio": 1.2, "max_drawdown": -0.03, "trade_count": 4},
                    "meta_veto": {"total_return": 0.09, "sharpe_ratio": 1.1, "max_drawdown": -0.02, "trade_count": 3},
                    "diff": {"total_return": -0.01, "sharpe_ratio": -0.1, "max_drawdown": 0.01, "trade_count": -1},
                    "in_sample": {
                        "baseline": {"total_return": 0.05, "sharpe_ratio": 0.8, "max_drawdown": -0.02, "trade_count": 2},
                        "meta_size_only": {"total_return": 0.04, "sharpe_ratio": 0.7, "max_drawdown": -0.01, "trade_count": 1},
                    },
                    "out_of_sample": {
                        "baseline": {"total_return": 0.05, "sharpe_ratio": 0.6, "max_drawdown": -0.01, "trade_count": 2},
                        "meta_size_only": {"total_return": 0.05, "sharpe_ratio": 0.6, "max_drawdown": -0.01, "trade_count": 2},
                    },
                }
            ]
        }
    )

    assert "ticker,segment,run,total_return,sharpe_ratio,max_drawdown,trade_count,exit_type_counts" in output
    assert "NVDA,in_sample,baseline,0.050000" in output
    assert "NVDA,out_of_sample,meta_size_only,0.050000" in output
    assert "NVDA,out_of_sample,diff,0.000000" in output
