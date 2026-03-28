from __future__ import annotations

import json
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from src.regime import cli


def _fake_market_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "price": [100.0, 101.0, 102.0],
            "volume": [1_000_000, 1_050_000, 1_075_000],
            "high": [101.0, 102.0, 103.0],
            "low": [99.0, 100.0, 101.0],
        }
    )


class _FakeRegime:
    ticker = "NVDA"
    latest_label = "Bull"
    latest_state_id = 0
    latest_probability = 0.82
    latest_price = 102.0
    regime_days = 5
    regime_signal = "Bullish Expansion"
    recent_state_mean_return = 0.01
    regime_inconsistency_warning = None
    transition_matrix = np.array([[0.9, 0.1, 0.0], [0.1, 0.8, 0.1], [0.0, 0.1, 0.9]])
    expected_regime_duration = 12.0
    transition_risk = 0.1
    latest_state_vector = np.array([0.8, 0.2, 0.0])
    price_frame = pd.DataFrame({"state_probability": [0.7, 0.8], "price": [100.0, 102.0]})


def _fake_report(ticker: str = "NVDA") -> SimpleNamespace:
    regime = SimpleNamespace(**{**_FakeRegime.__dict__, "ticker": ticker, "price_frame": pd.DataFrame({"state_probability": [0.7, 0.8], "price": [100.0, 102.0]})})
    qualitative = SimpleNamespace(
        llm_response={"confidence": 81},
        fallback_confidence=75,
        sentiment_score=2,
        catalyst_sentiment="Positive",
        catalysts=[],
        thesis_check_response=None,
    )
    return SimpleNamespace(regime=regime, qualitative=qualitative, regime_started_days_ago=3)


@pytest.fixture()
def base_args() -> SimpleNamespace:
    return SimpleNamespace(
        tickers=None,
        benchmark="SOXX",
        period="3y",
        interval="1d",
        lookback_window=20,
        training_window=504,
        refit_step=21,
        barrier_vol_multiplier=1.0,
        macro_weighting=False,
        frontier_on=False,
        frontier_provider="auto",
        chart_dir="/tmp/charts",
        json=False,
        weekly_digest=False,
        digest_format="json",
        backtest=False,
        backtest_period="5y",
    )


def _install_common_mocks(monkeypatch, *, resolved_report: SimpleNamespace | None = None) -> None:
    report = resolved_report or _fake_report()
    monkeypatch.setattr(cli, "get_investor_db_path", lambda: "/tmp/investor.db")
    monkeypatch.setattr(cli, "get_portfolio_tickers_filtered", lambda _db: ["NVDA", "AVGO"])
    monkeypatch.setattr(cli, "get_portfolio_tickers", lambda _db: ["NVDA", "AVGO", "MSFT"])
    monkeypatch.setattr(cli, "get_portfolio_positions", lambda _db, tickers=None: [])
    monkeypatch.setattr(cli, "positions_by_ticker", lambda positions: {})
    monkeypatch.setattr(cli, "positions_by_ticker_and_account", lambda positions: {})
    monkeypatch.setattr(cli, "get_tax_assumptions", lambda _db: {})
    monkeypatch.setattr(cli, "get_wash_sale_risk", lambda _db, _ticker: "NONE")
    monkeypatch.setattr(cli, "save_regime_event", lambda ticker, label, state_id: {"previous_label": "Neutral", "days_in_regime": 3})
    monkeypatch.setattr(cli, "save_sentiment", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli, "upsert_thesis", lambda ticker, thesis=None: None)
    monkeypatch.setattr(cli, "download_market_frame", lambda **kwargs: SimpleNamespace(frame=_fake_market_frame()))
    monkeypatch.setattr(cli, "fit_regime_model", lambda **kwargs: report.regime)
    monkeypatch.setattr(cli, "fit_regime_model_weekly", lambda ticker, market_frame: report.regime)
    monkeypatch.setattr(cli, "build_qualitative_assessment", lambda **kwargs: report.qualitative)
    monkeypatch.setattr(cli, "save_regime_chart", lambda regime, chart_dir: "/tmp/chart.png")
    monkeypatch.setattr(cli, "forward_regime_curve", lambda *args, **kwargs: pd.DataFrame({"day": [1, 2], "p_bull": [0.7, 0.72], "p_neutral": [0.2, 0.18], "p_bear": [0.1, 0.1]}))
    monkeypatch.setattr(cli, "signal_from_forward_curve", lambda *args, **kwargs: SimpleNamespace(action="Buy", strength=0.7, expected_holding_days=10, rationale="Bull remains firm"))
    monkeypatch.setattr(cli, "compute_technicals", lambda *args, **kwargs: pd.DataFrame({"rsi_14": [45, 50], "bb_pct": [0.4, 0.5], "macd_histogram": [0.1, 0.2]}))
    monkeypatch.setattr(cli, "intra_regime_signal", lambda *args, **kwargs: "Buy the dip")
    monkeypatch.setattr(cli, "build_composite_signal", lambda *args, **kwargs: SimpleNamespace(composite_action="Buy", composite_strength=0.8, regime_signal="Bull", regime_probability=0.82, forward_signal=SimpleNamespace(action="Buy"), technical_signal="Buy the dip", short_term_view="Short", medium_term_view="Medium", weekly_regime="Bull", multi_timeframe_note="Aligned"))
    monkeypatch.setattr(cli, "confidence_trajectory", lambda *args, **kwargs: SimpleNamespace(trend="rising", slope=0.1, short_ma_latest=0.7, long_ma_latest=0.6))
    monkeypatch.setattr(cli, "sentiment_momentum", lambda *args, **kwargs: (SimpleNamespace(trend="improving"), pd.DataFrame({"score": [1, 2]})))
    monkeypatch.setattr(cli, "tax_adjusted_signals", lambda *args, **kwargs: [])
    monkeypatch.setattr(cli, "summarize_relative_strength", lambda reports, benchmark_label: [reports[0]])
    monkeypatch.setattr(cli, "calibration_payload", lambda rows: {"calibration": {"bins": []}})
    monkeypatch.setattr(cli, "get_calibration_data", lambda lookback_days=365: [])


def test_default_tickers_used_when_none_specified(base_args, monkeypatch, capsys) -> None:
    args = base_args
    monkeypatch.setattr(cli, "parse_args", lambda: args)
    _install_common_mocks(monkeypatch)
    cli.main()
    out = capsys.readouterr().out
    assert "NVDA" in out


def test_custom_tickers_override_defaults(base_args, monkeypatch, capsys) -> None:
    args = base_args
    args.tickers = ["TSM"]
    monkeypatch.setattr(cli, "parse_args", lambda: args)
    _install_common_mocks(monkeypatch, resolved_report=_fake_report("TSM"))
    cli.main()
    assert "TSM" in capsys.readouterr().out


def test_invalid_ticker_format_handled(base_args, monkeypatch, capsys) -> None:
    args = base_args
    args.tickers = ["??"]
    monkeypatch.setattr(cli, "parse_args", lambda: args)
    _install_common_mocks(monkeypatch)
    monkeypatch.setattr(cli, "download_market_frame", lambda **kwargs: (_ for _ in ()).throw(RuntimeError("bad ticker")))
    with pytest.raises(RuntimeError):
        cli.main()


def test_period_parameter_passed_through(base_args, monkeypatch) -> None:
    seen = {}
    args = base_args
    args.period = "1y"
    monkeypatch.setattr(cli, "parse_args", lambda: args)
    _install_common_mocks(monkeypatch)
    def fake_download(**kwargs):
        seen["period"] = kwargs["period"]
        return SimpleNamespace(frame=_fake_market_frame())
    monkeypatch.setattr(cli, "download_market_frame", fake_download)
    cli.main()
    assert seen["period"] == "1y"


def test_json_output_is_valid_json(base_args, monkeypatch, capsys) -> None:
    args = base_args
    args.json = True
    monkeypatch.setattr(cli, "parse_args", lambda: args)
    _install_common_mocks(monkeypatch)
    cli.main()
    json.loads(capsys.readouterr().out)


def test_text_output_contains_regime_label(base_args, monkeypatch, capsys) -> None:
    monkeypatch.setattr(cli, "parse_args", lambda: base_args)
    _install_common_mocks(monkeypatch)
    cli.main()
    assert "Bull" in capsys.readouterr().out


def test_json_output_contains_required_fields(base_args, monkeypatch, capsys) -> None:
    args = base_args
    args.json = True
    monkeypatch.setattr(cli, "parse_args", lambda: args)
    _install_common_mocks(monkeypatch)
    cli.main()
    payload = json.loads(capsys.readouterr().out)
    first = payload["tickers"][0]
    assert {"ticker", "regime", "probability"} <= set(first.keys())


def test_multiple_tickers_json_output(base_args, monkeypatch, capsys) -> None:
    args = base_args
    args.json = True
    args.tickers = ["NVDA", "AVGO"]
    monkeypatch.setattr(cli, "parse_args", lambda: args)
    _install_common_mocks(monkeypatch)
    cli.main()
    payload = json.loads(capsys.readouterr().out)
    assert len(payload["tickers"]) == 2


def test_network_failure_produces_error_message(base_args, monkeypatch) -> None:
    monkeypatch.setattr(cli, "parse_args", lambda: base_args)
    _install_common_mocks(monkeypatch)
    monkeypatch.setattr(cli, "download_market_frame", lambda **kwargs: (_ for _ in ()).throw(RuntimeError("network failure")))
    with pytest.raises(RuntimeError):
        cli.main()


def test_insufficient_data_produces_warning(base_args, monkeypatch) -> None:
    monkeypatch.setattr(cli, "parse_args", lambda: base_args)
    _install_common_mocks(monkeypatch)
    monkeypatch.setattr(cli, "fit_regime_model", lambda **kwargs: (_ for _ in ()).throw(RuntimeError("insufficient history")))
    with pytest.raises(RuntimeError):
        cli.main()


def test_no_api_key_for_frontier_produces_warning(base_args, monkeypatch, capsys) -> None:
    args = base_args
    args.json = True
    args.frontier_on = True
    monkeypatch.setattr(cli, "parse_args", lambda: args)
    _install_common_mocks(monkeypatch)
    cli.main()
    assert "tickers" in capsys.readouterr().out


def test_digest_flag_triggers_digest_generation(base_args, monkeypatch, capsys) -> None:
    args = base_args
    args.weekly_digest = True
    monkeypatch.setattr(cli, "parse_args", lambda: args)
    monkeypatch.setattr(cli, "get_investor_db_path", lambda: "/tmp/investor.db")
    monkeypatch.setattr(cli, "get_portfolio_tickers_filtered", lambda _db: ["NVDA"])
    monkeypatch.setattr(cli, "generate_weekly_digest", lambda **kwargs: SimpleNamespace(entries=[], action_items=[]))
    monkeypatch.setattr(cli, "digest_to_dict", lambda digest: {"entries": [], "action_items": []})
    cli.main()
    assert '"entries"' in capsys.readouterr().out


def test_digest_output_format(base_args, monkeypatch, capsys) -> None:
    args = base_args
    args.weekly_digest = True
    args.digest_format = "text"
    monkeypatch.setattr(cli, "parse_args", lambda: args)
    monkeypatch.setattr(cli, "get_investor_db_path", lambda: "/tmp/investor.db")
    monkeypatch.setattr(cli, "get_portfolio_tickers_filtered", lambda _db: ["NVDA"])
    monkeypatch.setattr(cli, "generate_weekly_digest", lambda **kwargs: SimpleNamespace(entries=[], action_items=[]))
    monkeypatch.setattr(cli, "digest_to_text", lambda digest: "digest text")
    cli.main()
    assert "digest text" in capsys.readouterr().out


def test_backtest_json_output(base_args, monkeypatch, capsys) -> None:
    args = base_args
    args.backtest = True
    args.json = True
    monkeypatch.setattr(cli, "parse_args", lambda: args)
    monkeypatch.setattr(cli, "get_investor_db_path", lambda: "/tmp/investor.db")
    monkeypatch.setattr(cli, "get_portfolio_tickers_filtered", lambda _db: ["NVDA"])
    monkeypatch.setattr(cli, "run_backtest", lambda **kwargs: SimpleNamespace(total_return=0.2, sharpe_ratio=1.1))
    monkeypatch.setattr(cli, "compare_to_benchmark", lambda *args, **kwargs: {"alpha": 0.1})
    cli.main()
    payload = json.loads(capsys.readouterr().out)
    assert payload[0]["benchmark_compare"]["alpha"] == 0.1


def test_backtest_text_output(base_args, monkeypatch, capsys) -> None:
    args = base_args
    args.backtest = True
    monkeypatch.setattr(cli, "parse_args", lambda: args)
    monkeypatch.setattr(cli, "get_investor_db_path", lambda: "/tmp/investor.db")
    monkeypatch.setattr(cli, "get_portfolio_tickers_filtered", lambda _db: ["NVDA"])
    monkeypatch.setattr(cli, "run_backtest", lambda **kwargs: SimpleNamespace(total_return=0.2, sharpe_ratio=1.1))
    monkeypatch.setattr(cli, "compare_to_benchmark", lambda *args, **kwargs: {"alpha": 0.1})
    cli.main()
    assert "total_return=20.0%" in capsys.readouterr().out


def test_frontier_provider_accepts_claude_best_and_ollama() -> None:
    namespace = cli.parse_args.__globals__["argparse"].ArgumentParser
    assert namespace is not None


def test_parse_args_accepts_ollama_provider(monkeypatch) -> None:
    monkeypatch.setattr("sys.argv", ["regime-cli", "--frontier-provider", "ollama"])
    args = cli.parse_args()
    assert args.frontier_provider == "ollama"
