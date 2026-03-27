from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd

from src.regime import digest as digest_module
from src.regime.investor_adapter import (
    PortfolioPosition,
    TaxLotInfo,
    get_investor_db_path,
    get_portfolio_tickers,
)
from src.regime.signals import CompositeSignal, SentimentMomentum, SignalResult, sentiment_momentum, tax_adjusted_signal


def test_investor_adapter_missing_db(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("INVESTOR_DB_PATH", raising=False)
    monkeypatch.setattr("src.regime.investor_adapter.DEFAULT_INVESTOR_DB_PATH", tmp_path / "missing.db")
    assert get_investor_db_path() is None


def test_get_portfolio_tickers(tmp_path: Path) -> None:
    db_path = tmp_path / "investor.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE position_lots (ticker TEXT, qty REAL);
        CREATE TABLE securities (id INTEGER PRIMARY KEY, ticker TEXT);
        CREATE TABLE tax_lots (security_id INTEGER, source TEXT, quantity_open REAL);
        INSERT INTO position_lots VALUES ('NVDA', 10), ('AVGO', 5), ('NVDA', 2);
        INSERT INTO securities (id, ticker) VALUES (1, 'TSM');
        INSERT INTO tax_lots VALUES (1, 'RECONSTRUCTED', 3);
        """
    )
    conn.commit()
    conn.close()
    assert get_portfolio_tickers(str(db_path)) == ["AVGO", "NVDA", "TSM"]


def test_get_portfolio_tickers_fallback_empty(tmp_path: Path) -> None:
    db_path = tmp_path / "broken.db"
    sqlite3.connect(db_path).close()
    assert get_portfolio_tickers(str(db_path)) == []


def test_tax_adjusted_signal_ltcg_hold() -> None:
    composite = CompositeSignal(
        regime_signal="Bear",
        regime_probability=0.9,
        forward_signal=SignalResult("Sell", "short", 0.8, 10, "Bearish."),
        technical_signal="Stay defensive",
        composite_action="Sell",
        composite_strength=0.8,
        short_term_view="Short",
        medium_term_view="Medium",
    )
    position = PortfolioPosition(
        ticker="NVDA",
        account_name="Brokerage",
        account_type="TAXABLE",
        taxpayer_type="PERSONAL",
        qty=10,
        market_value=1500,
        current_price=150,
        cost_basis=1200,
        unrealized_gain=300,
        asset_class="EQUITY",
        lots=[
            TaxLotInfo(
                lot_id=1,
                acquisition_date="2025-04-01",
                qty=10,
                basis_total=1200,
                days_held=345,
                term="ST",
                unrealized_gain=300,
                days_to_ltcg=20,
            )
        ],
    )
    adjusted = tax_adjusted_signal(composite, position, {"ordinary_rate": 0.37, "ltcg_rate": 0.20, "niit_rate": 0.038})
    assert adjusted.adjusted_action == "Hold"
    assert adjusted.ltcg_threshold_date is not None


def test_tax_adjusted_signal_ira_passthrough() -> None:
    composite = CompositeSignal(
        regime_signal="Bull",
        regime_probability=0.9,
        forward_signal=SignalResult("Buy", "short", 0.8, 10, "Bullish."),
        technical_signal="Buy the dip",
        composite_action="Buy",
        composite_strength=0.8,
        short_term_view="Short",
        medium_term_view="Medium",
    )
    position = PortfolioPosition(
        ticker="NVDA",
        account_name="IRA",
        account_type="IRA",
        taxpayer_type="PERSONAL",
        qty=10,
        market_value=1500,
        current_price=150,
        cost_basis=1200,
        unrealized_gain=300,
        asset_class="EQUITY",
        lots=[],
    )
    adjusted = tax_adjusted_signal(composite, position, {"ordinary_rate": 0.37, "ltcg_rate": 0.20, "niit_rate": 0.038})
    assert adjusted.adjusted_action == "Buy"
    assert adjusted.tax_note.startswith("IRA")


def test_sentiment_momentum_divergence(monkeypatch) -> None:
    history = [
        {"ticker": "NVDA", "score": score, "sentiment": "Negative", "catalyst_count": 2, "recorded_at": f"2026-03-{i+1:02d}T00:00:00+00:00"}
        for i, score in enumerate([3, 2, 1, 0, -1, -2, -3, -3, -4, -4])
    ]
    monkeypatch.setattr("src.regime.signals.get_sentiment_history", lambda ticker, days=30: history)
    momentum, chart = sentiment_momentum("NVDA", "Bull")
    assert isinstance(chart, pd.DataFrame)
    assert momentum.divergence_vs_regime is True
    assert momentum.trend == "deteriorating"


def test_weekly_digest_priority(monkeypatch) -> None:
    @dataclass
    class FakeRegime:
        ticker: str
        latest_label: str
        latest_state_id: int = 0
        latest_probability: float = 0.9
        latest_state_vector: np.ndarray = field(default_factory=lambda: np.array([0.8, 0.1, 0.1]))
        transition_matrix: np.ndarray = field(default_factory=lambda: np.eye(3))
        transition_risk: float = 0.02
        expected_regime_duration: float = 20.0
        regime_signal: str = "Bullish Expansion"

    @dataclass
    class FakeQualitative:
        sentiment_score: int = 2
        catalyst_sentiment: str = "Positive"
        catalysts: list = None

        def __post_init__(self):
            if self.catalysts is None:
                self.catalysts = []

    monkeypatch.setattr(digest_module, "download_market_frame", lambda ticker, period="3y", interval="1d": type("MS", (), {"frame": pd.DataFrame({"price": [1, 2], "volume": [1, 1], "high": [1, 2], "low": [1, 2], "vix": [20, 21], "yield_10y": [4, 4.1]})})())
    monkeypatch.setattr(digest_module, "fit_regime_model", lambda ticker, market_frame: FakeRegime(ticker=ticker, latest_label="Bull"))
    monkeypatch.setattr(digest_module, "build_qualitative_assessment", lambda **kwargs: FakeQualitative())
    monkeypatch.setattr(digest_module, "save_regime_event", lambda *args, **kwargs: {"previous_label": "Bear", "days_in_regime": 1})
    monkeypatch.setattr(digest_module, "save_sentiment", lambda *args, **kwargs: None)
    monkeypatch.setattr(digest_module, "upsert_thesis", lambda *args, **kwargs: None)
    monkeypatch.setattr(digest_module, "forward_regime_curve", lambda *args, **kwargs: pd.DataFrame({"day": [1], "p_bull": [0.8], "p_neutral": [0.1], "p_bear": [0.1]}))
    monkeypatch.setattr(digest_module, "signal_from_forward_curve", lambda *args, **kwargs: SignalResult("Strong Buy", "short", 0.8, 10, "Bullish"))
    monkeypatch.setattr(digest_module, "compute_technicals", lambda *args, **kwargs: pd.DataFrame({"rsi_14": [25, 25], "bb_pct": [0.05, 0.05], "macd_histogram": [0.1, 0.1]}))
    monkeypatch.setattr(digest_module, "intra_regime_signal", lambda *args, **kwargs: "Buy the dip")
    monkeypatch.setattr(digest_module, "build_composite_signal", lambda *args, **kwargs: CompositeSignal("Bull", 0.9, SignalResult("Strong Buy", "short", 0.8, 10, "Bullish"), "Buy the dip", "Strong Buy", 0.95, "Short", "Medium"))
    monkeypatch.setattr(digest_module, "sentiment_momentum", lambda *args, **kwargs: (SentimentMomentum(1.0, 0.5, "improving", False, None), pd.DataFrame({"score": [1, 2]})))
    monkeypatch.setattr(digest_module, "get_recent_regime_changes", lambda *args, **kwargs: [{"previous_label": "Bear", "current_label": "Bull", "changed_at": "2026-03-20T00:00:00+00:00"}])
    monkeypatch.setattr(digest_module, "get_portfolio_tickers", lambda *args, **kwargs: ["NVDA"])
    monkeypatch.setattr(digest_module, "get_portfolio_positions", lambda *args, **kwargs: [])
    monkeypatch.setattr(digest_module, "positions_by_ticker", lambda positions: {})
    monkeypatch.setattr(digest_module, "get_tax_assumptions", lambda *args, **kwargs: {"ordinary_rate": 0.37, "ltcg_rate": 0.20, "niit_rate": 0.038})
    monkeypatch.setattr(digest_module, "get_wash_sale_risk", lambda *args, **kwargs: "NONE")

    digest = digest_module.generate_weekly_digest(["NVDA"], "SOXX", investor_db_path=None)
    assert digest.entries[0].priority == "ACTION REQUIRED"


def test_generate_weekly_digest_skips_short_history(monkeypatch) -> None:
    from src.regime import digest as digest_module
    from src.regime.exceptions import InsufficientDataError

    class ShortHistoryBenchmark:
        def __init__(self, ticker, latest_label):
            self.ticker = ticker
            self.latest_label = latest_label

    monkeypatch.setattr(digest_module, "download_market_frame", lambda ticker, period="3y", interval="1d": type("MS", (), {"frame": pd.DataFrame({"price": [1, 2], "volume": [1, 1], "high": [1, 2], "low": [1, 2], "vix": [20, 21], "yield_10y": [4, 4.1]})})())

    def fake_fit(ticker, market_frame, training_window=504):
        if ticker == "SOXX" and training_window < 504:
            return ShortHistoryBenchmark(ticker=ticker, latest_label="Bull")
        raise InsufficientDataError("Insufficient history")

    monkeypatch.setattr(digest_module, "fit_regime_model", fake_fit)
    monkeypatch.setattr(digest_module, "get_portfolio_positions", lambda *args, **kwargs: [])
    monkeypatch.setattr(digest_module, "positions_by_ticker", lambda positions: {})
    monkeypatch.setattr(digest_module, "positions_by_ticker_and_account", lambda positions: {})
    monkeypatch.setattr(digest_module, "get_tax_assumptions", lambda *args, **kwargs: {})
    digest = digest_module.generate_weekly_digest(["NVDA"], "SOXX", investor_db_path=None, persist=False)
    assert digest.entries == []
