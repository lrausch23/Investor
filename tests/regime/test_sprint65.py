from __future__ import annotations

import datetime as dt
import importlib
import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.app.routes import regime as regime_route


@pytest.fixture
def temp_modules(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    import src.regime.cross_sectional as cross_sectional
    import src.regime.discovery as discovery
    import src.regime.fundamental_data as fundamental_data
    import src.regime.fundamental_gating as gating
    import src.regime.paper_trading as paper_trading
    import src.regime.persistence as store

    store = importlib.reload(store)
    store.DB_PATH = tmp_path / "regime_watch.db"
    cross_sectional = importlib.reload(cross_sectional)
    discovery = importlib.reload(discovery)
    fundamental_data = importlib.reload(fundamental_data)
    gating = importlib.reload(gating)
    paper_trading = importlib.reload(paper_trading)
    fundamental_data.clear_cache()
    return store, cross_sectional, discovery, paper_trading, fundamental_data, gating


def _save_buy_signal_snapshot(store, ticker: str, *, price: float) -> None:
    store.save_signal_snapshot(
        ticker=ticker,
        snapshot_date=dt.datetime.now(dt.timezone.utc).date().isoformat(),
        action="Buy",
        regime_label="Bull",
        regime_probability=0.90,
        composite_strength=0.80,
        benchmark="SPY",
        current_price=price,
        entry_price=price,
        exit_price=price * 1.2,
        stop_price=price * 0.9,
        risk_reward_ratio=2.0,
        timeframe_days=21,
        expected_regime_duration=30.0,
    )


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(regime_route.router)
    app.dependency_overrides[regime_route.require_actor] = lambda: "tester"
    app.dependency_overrides[regime_route.db_session] = lambda: iter([None])
    return TestClient(app)


def _series(values: list[float]) -> pd.Series:
    return pd.Series(values, index=pd.date_range("2025-01-01", periods=len(values), freq="B"), dtype=float)


def _series_from_returns(returns: list[float], start: float = 100.0) -> pd.Series:
    prices = [start]
    for change in returns:
        prices.append(prices[-1] * (1.0 + float(change)))
    return _series(prices)


def _bars_from_close(close: pd.Series) -> pd.DataFrame:
    return pd.DataFrame({"Close": close})


def _patch_fetch(monkeypatch: pytest.MonkeyPatch, cross_sectional, ticker: pd.Series, benchmark: pd.Series | None = None) -> None:
    benchmark_series = benchmark if benchmark is not None else ticker

    def fake_fetch(symbol: str, days: int) -> pd.Series:
        del days
        return benchmark_series if str(symbol).upper() == "SPY" else ticker

    monkeypatch.setattr(cross_sectional, "_fetch_closes", fake_fetch)


def test_beta_adjusted_return_normal(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    _store, cross_sectional, _discovery, _paper, _fundamental_data, _gating = temp_modules
    benchmark_returns = [0.001 + ((idx % 5) * 0.0004) for idx in range(260)]
    ticker_returns = [value * 1.2 for value in benchmark_returns]
    benchmark = _series_from_returns(benchmark_returns, start=100.0)
    ticker = _series_from_returns(ticker_returns, start=110.0)
    _patch_fetch(monkeypatch, cross_sectional, ticker, benchmark)
    result = cross_sectional.calculate_beta_adjusted_return("NVDA")
    assert result.data_quality == "full"
    assert result.beta is not None and result.beta == pytest.approx(1.2, rel=0.05)
    assert result.beta_adjusted_return == pytest.approx(result.raw_return / max(abs(result.beta), 0.1), rel=1e-6)


def test_beta_adjusted_return_low_beta(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    _store, cross_sectional, _discovery, _paper, _fundamental_data, _gating = temp_modules
    benchmark_returns = [0.001 + ((idx % 4) * 0.0005) for idx in range(260)]
    ticker_returns = [value * 0.5 for value in benchmark_returns]
    benchmark = _series_from_returns(benchmark_returns, start=100.0)
    ticker = _series_from_returns(ticker_returns, start=80.0)
    _patch_fetch(monkeypatch, cross_sectional, ticker, benchmark)
    result = cross_sectional.calculate_beta_adjusted_return("XLU")
    assert result.beta is not None and result.beta == pytest.approx(0.5, rel=0.05)
    assert result.beta_adjusted_return is not None and result.raw_return is not None
    assert result.beta_adjusted_return > result.raw_return


def test_beta_adjusted_return_insufficient_data(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    _store, cross_sectional, _discovery, _paper, _fundamental_data, _gating = temp_modules
    short = _series([100 + idx for idx in range(40)])
    _patch_fetch(monkeypatch, cross_sectional, short, short)
    result = cross_sectional.calculate_beta_adjusted_return("NVDA")
    assert result.data_quality == "insufficient"
    assert result.beta is None


def test_alpha_calculation(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    _store, cross_sectional, _discovery, _paper, _fundamental_data, _gating = temp_modules
    benchmark = _series(list(range(100, 352)))
    ticker = benchmark.copy() * 1.5 + 10.0
    _patch_fetch(monkeypatch, cross_sectional, ticker, benchmark)
    result = cross_sectional.calculate_beta_adjusted_return("NVDA")
    assert result.alpha == pytest.approx(float(result.raw_return or 0.0) - float(result.beta or 0.0) * float(result.benchmark_return or 0.0), rel=1e-6)


def test_vol_z_elevated(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    _store, cross_sectional, _discovery, _paper, _fundamental_data, _gating = temp_modules
    calm = [100 + 0.1 * idx + (0.2 if idx % 2 else -0.2) for idx in range(220)]
    spike = [calm[-1], calm[-1] * 1.15, calm[-1] * 0.85, calm[-1] * 1.20, calm[-1] * 0.80, calm[-1] * 1.25]
    series = _series(calm + spike)
    monkeypatch.setattr(cross_sectional, "_fetch_closes", lambda ticker, days: series)
    result = cross_sectional.calculate_volatility_z_score("NVDA")
    assert result.vol_z_score is not None and result.vol_z_score > 1.5
    assert result.interpretation == "Elevated"


def test_vol_z_normal(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    _store, cross_sectional, _discovery, _paper, _fundamental_data, _gating = temp_modules
    returns = ([0.012, -0.010, 0.008, -0.009, 0.011, -0.012] * 44)[:260]
    series = _series_from_returns(returns, start=100.0)
    monkeypatch.setattr(cross_sectional, "_fetch_closes", lambda ticker, days: series)
    result = cross_sectional.calculate_volatility_z_score("NVDA")
    assert result.interpretation == "Normal"


def test_vol_z_subdued(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    _store, cross_sectional, _discovery, _paper, _fundamental_data, _gating = temp_modules
    noisy = [100 + 0.2 * idx + (2.0 if idx % 2 else -2.0) for idx in range(220)]
    quiet_tail = [noisy[-1] + 0.05 * idx for idx in range(40)]
    series = _series(noisy + quiet_tail)
    monkeypatch.setattr(cross_sectional, "_fetch_closes", lambda ticker, days: series)
    result = cross_sectional.calculate_volatility_z_score("NVDA")
    assert result.vol_z_score is not None and result.vol_z_score < -1.0
    assert result.interpretation == "Subdued"


def test_vol_z_insufficient_data(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    _store, cross_sectional, _discovery, _paper, _fundamental_data, _gating = temp_modules
    monkeypatch.setattr(cross_sectional, "_fetch_closes", lambda ticker, days: _series([100 + idx for idx in range(30)]))
    result = cross_sectional.calculate_volatility_z_score("NVDA")
    assert result.data_quality == "insufficient"


def test_peer_percentiles_three_tickers(temp_modules) -> None:
    _store, cross_sectional, _discovery, _paper, _fundamental_data, _gating = temp_modules
    payload = cross_sectional.compute_peer_percentiles(
        ["A", "B", "C"],
        {
            "A": {"beta_adj_return": 0.1},
            "B": {"beta_adj_return": 0.2},
            "C": {"beta_adj_return": 0.3},
        },
    )
    percentiles = {item.ticker: item.percentile for item in (rows[0] for rows in payload.values())}
    assert percentiles["C"] > percentiles["B"] > percentiles["A"]


def test_peer_percentiles_single_ticker(temp_modules) -> None:
    _store, cross_sectional, _discovery, _paper, _fundamental_data, _gating = temp_modules
    payload = cross_sectional.compute_peer_percentiles(["A"], {"A": {"vol_z": 0.5}})
    assert payload["A"][0].percentile is None


def test_peer_percentiles_none_values(temp_modules) -> None:
    _store, cross_sectional, _discovery, _paper, _fundamental_data, _gating = temp_modules
    payload = cross_sectional.compute_peer_percentiles(
        ["A", "B", "C"],
        {"A": {"vol_z": 0.5}, "B": {"vol_z": None}, "C": {"vol_z": -0.5}},
    )
    results = {item.ticker: item for item in (rows[0] for rows in payload.values())}
    assert results["B"].percentile is None
    assert results["A"].peer_count == 2


def test_normalize_crowd_sub_scores(temp_modules) -> None:
    _store, _cross_sectional, discovery, _paper, _fundamental_data, _gating = temp_modules
    rows = [
        {"ticker": "A", "analyst_score": 30, "institutional_score": 0, "volume_score": 25, "short_score": 0},
        {"ticker": "B", "analyst_score": 10, "institutional_score": 25, "volume_score": 10, "short_score": 5},
        {"ticker": "C", "analyst_score": 20, "institutional_score": 15, "volume_score": 15, "short_score": 10},
        {"ticker": "D", "analyst_score": 0, "institutional_score": 10, "volume_score": 0, "short_score": 20},
        {"ticker": "E", "analyst_score": 10, "institutional_score": 10, "volume_score": 10, "short_score": 10},
    ]
    normalized = discovery._normalize_crowd_sub_scores(rows)
    assert all("normalized_crowd_score" in item for item in normalized)
    assert normalized[0]["normalized_crowd_score"] != (normalized[0]["analyst_score"] + normalized[0]["institutional_score"] + normalized[0]["volume_score"] + normalized[0]["short_score"])


def test_normalize_crowd_insufficient_peers(temp_modules) -> None:
    _store, _cross_sectional, discovery, _paper, _fundamental_data, _gating = temp_modules
    rows = [{"ticker": "A", "analyst_score": 30}, {"ticker": "B", "analyst_score": 10}]
    normalized = discovery._normalize_crowd_sub_scores(rows)
    assert all("normalized_crowd_score" not in item for item in normalized)


def test_discovery_cross_sectional_enrichment(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    store, _cross_sectional, discovery, _paper, _fundamental_data, gating = temp_modules
    theme = store.create_theme("AI", conviction=5)
    ids = []
    for idx, ticker in enumerate(["NVDA", "AMD", "AVGO"], start=1):
        row = store.upsert_watchlist_candidate(
            theme["id"],
            ticker,
            regime_label="Bull",
            regime_probability=0.85,
            crowd_score=20 + idx,
            normalized_crowd_score=15 + idx,
        )
        ids.append(row["id"])
    monkeypatch.setattr(gating, "run_fundamental_gate", lambda *args, **kwargs: gating.FundamentalGateResult("NVDA", True, None, None, []))
    monkeypatch.setattr(
        discovery,
        "calculate_beta_adjusted_return",
        lambda ticker: SimpleNamespace(
            ticker=ticker, raw_return=0.1, beta=1.1, beta_adjusted_return=0.09, benchmark_return=0.05, alpha=0.04, data_quality="full"
        ),
    )
    monkeypatch.setattr(
        discovery,
        "calculate_volatility_z_score",
        lambda ticker: SimpleNamespace(
            ticker=ticker, current_vol=0.2, baseline_vol=0.15, baseline_std=0.02, vol_z_score=1.0, interpretation="Normal", data_quality="full"
        ),
    )
    signals = discovery.check_entry_signals(theme["id"])
    assert len(signals) == 3
    refreshed = store.get_watchlist_entry(ids[0])
    assert refreshed["beta"] == pytest.approx(1.1)
    assert refreshed["beta_adjusted_return"] == pytest.approx(0.09)
    assert refreshed["peer_percentile_json"]


def test_risk_adjusted_quantity_with_atr(temp_modules) -> None:
    _store, _cross_sectional, _discovery, paper_trading, _fundamental_data, _gating = temp_modules
    qty = paper_trading._risk_adjusted_quantity(10000.0, 100.0, 3.0, 1.0)
    assert qty == 33


def test_risk_adjusted_quantity_high_beta(temp_modules) -> None:
    _store, _cross_sectional, _discovery, paper_trading, _fundamental_data, _gating = temp_modules
    base = paper_trading._risk_adjusted_quantity(10000.0, 100.0, 3.0, 1.0)
    high = paper_trading._risk_adjusted_quantity(10000.0, 100.0, 3.0, 2.0)
    assert high < base


def test_risk_adjusted_quantity_low_beta(temp_modules) -> None:
    _store, _cross_sectional, _discovery, paper_trading, _fundamental_data, _gating = temp_modules
    base = paper_trading._risk_adjusted_quantity(10000.0, 100.0, 3.0, 1.0)
    low = paper_trading._risk_adjusted_quantity(10000.0, 100.0, 3.0, 0.4)
    assert low > base


def test_risk_adjusted_quantity_no_atr(temp_modules) -> None:
    _store, _cross_sectional, _discovery, paper_trading, _fundamental_data, _gating = temp_modules
    qty = paper_trading._risk_adjusted_quantity(10000.0, 100.0, None, 1.0)
    assert qty == 50


def test_generate_buy_plans_uses_risk_sizing(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    store, _cross_sectional, _discovery, paper_trading, _fundamental_data, _gating = temp_modules
    store.set_setting("sizing_method", "risk_budget")
    store.set_setting("sizing_base_risk_fraction", "0.02")
    store.set_setting("sizing_atr_multiplier", "2.0")
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    theme = store.create_theme("AI", conviction=5)
    store.upsert_watchlist_candidate(theme["id"], "NVDA", regime_label="Bull", regime_probability=0.9, crowd_score=20, status="Entry Signal", suggested_entry_price=100.0)
    _save_buy_signal_snapshot(store, "NVDA", price=100.0)
    monkeypatch.setattr(paper_trading, "_lookup_atr", lambda ticker: 3.0)
    monkeypatch.setattr(paper_trading, "_lookup_beta", lambda ticker: 1.0)
    monkeypatch.setattr(paper_trading, "_batch_current_prices", lambda tickers: {"NVDA": 100.0})
    plans = paper_trading.generate_buy_plans(portfolio["id"])
    assert plans
    assert plans[0]["quantity"] == 35
    assert plans[0]["sizing_method"] == "risk_budget"


def test_watchlist_cross_sectional_columns_persisted(temp_modules) -> None:
    store, _cross_sectional, _discovery, _paper, _fundamental_data, _gating = temp_modules
    theme = store.create_theme("AI", conviction=5)
    row = store.upsert_watchlist_candidate(theme["id"], "NVDA", crowd_score=20)
    store.update_watchlist_cross_sectional(row["id"], beta=1.2, beta_adjusted_return=0.08, vol_z_score=1.4, vol_z_interpretation="Normal", normalized_crowd_score=18, peer_percentile_json='[{"metric_name":"vol_z"}]')
    refreshed = store.get_watchlist_entry(row["id"])
    assert refreshed["beta"] == pytest.approx(1.2)
    assert refreshed["normalized_crowd_score"] == 18


def test_plan_sizing_method_column(temp_modules) -> None:
    store, _cross_sectional, _discovery, _paper, _fundamental_data, _gating = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    plan = store.create_trade_plan(portfolio["id"], "NVDA", "Buy", 10, "test", sizing_method="risk_budget")
    assert plan["sizing_method"] == "risk_budget"


def test_cross_sectional_diagnostic_route(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    _store, cross_sectional, _discovery, _paper, _fundamental_data, _gating = temp_modules
    monkeypatch.setattr(cross_sectional, "calculate_beta_adjusted_return", lambda ticker: cross_sectional.BetaAdjustedResult(ticker, 0.1, 1.1, 0.09, 0.05, 0.04, "full"))
    monkeypatch.setattr(cross_sectional, "calculate_volatility_z_score", lambda ticker: cross_sectional.VolatilityZResult(ticker, 0.2, 0.15, 0.03, 1.2, "Normal", "full"))
    client = _client()
    response = client.get("/regime/cross-sectional/NVDA")
    assert response.status_code == 200
    payload = response.json()
    assert payload["beta_adjusted"]["beta"] == pytest.approx(1.1)
    assert payload["volatility_z"]["vol_z_score"] == pytest.approx(1.2)


def test_sizing_settings_route(temp_modules) -> None:
    store, _cross_sectional, _discovery, _paper, _fundamental_data, _gating = temp_modules
    client = _client()
    current = client.get("/regime/sizing/settings").json()
    assert current["sizing_method"] == "risk_budget"
    updated = client.put("/regime/sizing/settings", json={"sizing_method": "equal_dollar", "sizing_base_risk_fraction": 0.03, "sizing_atr_multiplier": 2.5})
    assert updated.status_code == 200
    payload = updated.json()
    assert payload["sizing_method"] == "equal_dollar"
    assert store.get_setting("sizing_method") == "equal_dollar"


def test_equal_dollar_sizing_still_works(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    store, _cross_sectional, _discovery, paper_trading, _fundamental_data, _gating = temp_modules
    store.set_setting("sizing_method", "equal_dollar")
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    theme = store.create_theme("AI", conviction=5)
    store.upsert_watchlist_candidate(theme["id"], "NVDA", regime_label="Bull", regime_probability=0.9, crowd_score=20, status="Entry Signal", suggested_entry_price=100.0)
    _save_buy_signal_snapshot(store, "NVDA", price=100.0)
    monkeypatch.setattr(paper_trading, "_lookup_atr", lambda ticker: 3.0)
    monkeypatch.setattr(paper_trading, "_lookup_beta", lambda ticker: 2.0)
    monkeypatch.setattr(paper_trading, "_batch_current_prices", lambda tickers: {"NVDA": 100.0})
    plans = paper_trading.generate_buy_plans(portfolio["id"])
    assert plans[0]["quantity"] == 105
    assert plans[0]["sizing_method"] == "equal_dollar"


def test_existing_crowd_score_unchanged(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    _store, _cross_sectional, discovery, _paper, _fundamental_data, _gating = temp_modules
    monkeypatch.setattr(discovery, "get_ticker_info", lambda ticker: {"numberOfAnalystOpinions": 20, "heldPercentInstitutions": 0.7, "shortPercentOfFloat": 0.04, "averageVolume": 1_000_000, "regularMarketPrice": 100.0})
    score, details = discovery.compute_crowd_score("NVDA")
    assert score == 55
    assert details["analyst_score"] == 20


def test_existing_fundamental_gate_unaffected(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    _store, _cross_sectional, _discovery, _paper, fundamental_data, gating = temp_modules
    statements = fundamental_data.FinancialStatements("NVDA", pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}, 0.0)
    monkeypatch.setattr(gating, "fetch_financial_statements", lambda ticker: statements)
    result = gating.run_fundamental_gate("NVDA", pass_on_insufficient_data=True)
    assert result.passed is True
