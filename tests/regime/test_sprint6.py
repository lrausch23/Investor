from __future__ import annotations

import argparse
import io
import json
import sqlite3
from contextlib import redirect_stdout
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd

from src.regime import cli as cli_module
from src.regime.investor_adapter import (
    PortfolioPosition,
    TaxLotInfo,
    _connect,
    _resolve_current_price,
    get_latest_prices,
    get_portfolio_tickers_filtered,
)
from src.regime.signals import CompositeSignal, SignalResult, tax_adjusted_signals


def _build_price_db(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE price_daily (
            ticker TEXT,
            date TEXT,
            close REAL,
            adj_close REAL
        );
        CREATE TABLE taxpayer_entities (
            id INTEGER PRIMARY KEY,
            type TEXT
        );
        CREATE TABLE accounts (
            id INTEGER PRIMARY KEY,
            name TEXT,
            account_type TEXT,
            taxpayer_entity_id INTEGER
        );
        CREATE TABLE securities (
            id INTEGER PRIMARY KEY,
            ticker TEXT,
            asset_class TEXT,
            metadata_json TEXT
        );
        CREATE TABLE position_lots (
            id INTEGER PRIMARY KEY,
            account_id INTEGER,
            ticker TEXT,
            acquisition_date TEXT,
            qty REAL,
            basis_total REAL,
            adjusted_basis_total REAL
        );
        CREATE TABLE tax_lots (
            id INTEGER PRIMARY KEY,
            account_id INTEGER,
            security_id INTEGER,
            source TEXT,
            acquired_date TEXT,
            quantity_open REAL,
            basis_open REAL
        );
        """
    )
    conn.commit()
    conn.close()


def test_get_latest_prices_uses_price_daily(tmp_path: Path) -> None:
    db_path = tmp_path / "investor.db"
    _build_price_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.execute("INSERT INTO price_daily VALUES ('NVDA', '2026-03-20', 118.0, 120.5)")
    conn.execute("INSERT INTO price_daily VALUES ('NVDA', '2026-03-19', 116.0, 117.0)")
    conn.execute("INSERT INTO price_daily VALUES ('AVGO', '2026-03-20', 180.0, NULL)")
    conn.commit()
    conn.close()

    prices = get_latest_prices(str(db_path), ["NVDA", "AVGO"])
    assert prices == {"AVGO": 180.0, "NVDA": 120.5}


def test_resolve_current_price_falls_back_to_metadata(tmp_path: Path) -> None:
    db_path = tmp_path / "investor.db"
    _build_price_db(db_path)

    with _connect(str(db_path)) as conn:
        price = _resolve_current_price(conn, "PLAB", json.dumps({"last_price": 44.25}))

    assert price == 44.25


def test_connect_uses_immutable_uri(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    class FakeConnection:
        row_factory = None

    def fake_connect(target: str, uri: bool = False):
        captured["target"] = target
        captured["uri"] = uri
        return FakeConnection()

    monkeypatch.setattr("src.regime.investor_adapter.sqlite3.connect", fake_connect)
    _connect(str(tmp_path / "investor.db"))

    assert captured["uri"] is True
    assert "mode=ro" in str(captured["target"])
    assert "immutable=1" in str(captured["target"])


def test_get_portfolio_tickers_filtered_excludes_non_hmm_assets(monkeypatch) -> None:
    monkeypatch.setattr("src.regime.investor_adapter.get_portfolio_tickers", lambda db_path: ["NVDA", "SPY", "GLD", "EUR", "TSM"])

    class FakeConnection:
        class FakeResult:
            def fetchall(self):
                return [
                    {"ticker": "NVDA", "asset_class": "EQUITY"},
                    {"ticker": "SPY", "asset_class": "ETF"},
                    {"ticker": "GLD", "asset_class": "COMMODITY"},
                    {"ticker": "EUR", "asset_class": "CURRENCY"},
                    {"ticker": "TSM", "asset_class": "EQUITY"},
                ]

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params):
            return self.FakeResult()

    monkeypatch.setattr("src.regime.investor_adapter._connect", lambda db_path: FakeConnection())
    assert get_portfolio_tickers_filtered("/tmp/fake.db") == ["NVDA", "TSM"]


def test_tax_adjusted_signals_preserve_per_account_logic() -> None:
    composite = CompositeSignal(
        regime_signal="Bear",
        regime_probability=0.92,
        forward_signal=SignalResult("Sell", "short", 0.9, 15, "Bear regime remains dominant."),
        technical_signal="Stay defensive",
        composite_action="Sell",
        composite_strength=0.9,
        short_term_view="Short",
        medium_term_view="Medium",
    )
    positions = [
        PortfolioPosition(
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
        ),
        PortfolioPosition(
            ticker="NVDA",
            account_name="IRA",
            account_type="IRA",
            taxpayer_type="PERSONAL",
            qty=5,
            market_value=750,
            current_price=150,
            cost_basis=700,
            unrealized_gain=50,
            asset_class="EQUITY",
            lots=[],
        ),
    ]

    signals = tax_adjusted_signals(
        composite,
        positions,
        {"ordinary_rate": 0.37, "ltcg_rate": 0.20, "niit_rate": 0.038},
        wash_sale_risk="DEFINITE",
    )

    assert [signal.account_name for signal in signals] == ["Brokerage", "IRA"]
    assert signals[0].adjusted_action == "Hold"
    assert signals[1].adjusted_action == "Sell"
    assert signals[1].tax_note.startswith("IRA")


def test_cli_text_output_mode(monkeypatch) -> None:
    @dataclass
    class FakeRegime:
        ticker: str
        latest_label: str = "Bull"
        latest_state_id: int = 0
        latest_probability: float = 0.91
        latest_price: float = 125.0
        transition_matrix: np.ndarray = field(default_factory=lambda: np.eye(3))
        latest_state_vector: np.ndarray = field(default_factory=lambda: np.array([0.8, 0.1, 0.1]))
        transition_risk: float = 0.05
        expected_regime_duration: float = 12.0
        price_frame: pd.DataFrame = field(default_factory=lambda: pd.DataFrame({"state_probability": [0.8, 0.85]}))

    @dataclass
    class FakeReport:
        regime: FakeRegime

    fake_args = argparse.Namespace(
        tickers=["NVDA"],
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
        chart_dir="/tmp",
        json=False,
        weekly_digest=False,
        digest_format="json",
    )

    monkeypatch.setattr(cli_module, "parse_args", lambda: fake_args)
    monkeypatch.setattr(cli_module, "get_investor_db_path", lambda: "/tmp/investor.db")
    monkeypatch.setattr(cli_module, "get_portfolio_tickers_filtered", lambda db_path: ["NVDA"])
    monkeypatch.setattr(cli_module, "get_portfolio_positions", lambda db_path, tickers=None: [])
    monkeypatch.setattr(cli_module, "positions_by_ticker", lambda positions: {})
    monkeypatch.setattr(cli_module, "positions_by_ticker_and_account", lambda positions: {})
    monkeypatch.setattr(cli_module, "get_tax_assumptions", lambda db_path: {})
    monkeypatch.setattr(cli_module, "summarize_relative_strength", lambda reports, benchmark_label: [])
    monkeypatch.setattr(cli_module, "forward_regime_curve", lambda *args, **kwargs: pd.DataFrame({"day": [1], "p_bull": [0.6], "p_neutral": [0.2], "p_bear": [0.2]}))
    monkeypatch.setattr(
        cli_module,
        "signal_from_forward_curve",
        lambda *args, **kwargs: SignalResult("Hold", "medium", 0.5, 10, "Mixed forward curve."),
    )
    monkeypatch.setattr(
        cli_module,
        "compute_technicals",
        lambda *args, **kwargs: pd.DataFrame({"rsi_14": [40, 45], "bb_pct": [0.3, 0.4], "macd_histogram": [0.1, 0.2]}),
    )
    monkeypatch.setattr(cli_module, "intra_regime_signal", lambda *args, **kwargs: "Hold / add on weakness")
    monkeypatch.setattr(
        cli_module,
        "build_composite_signal",
        lambda *args, **kwargs: CompositeSignal("Bear", 0.9, SignalResult("Hold", "medium", 0.5, 10, "Mixed"), "Hold / add on weakness", "Hold", 0.5, "Short", "Medium"),
    )
    monkeypatch.setattr(
        cli_module,
        "confidence_trajectory",
        lambda *args, **kwargs: type("Trajectory", (), {"__dict__": {"slope": 0.0, "trend": "stable", "days_declining": 0, "days_rising": 0}})(),
    )
    monkeypatch.setattr(
        cli_module,
        "_build_report",
        lambda ticker, *args, **kwargs: (
            FakeReport(regime=FakeRegime(ticker=ticker, latest_label="Bear" if ticker == "SOXX" else "Bull")),
            f"/tmp/{ticker}.png",
            pd.DataFrame({"price": [100.0, 101.0], "volume": [1_000_000, 1_100_000], "high": [101.0, 102.0], "low": [99.0, 100.0]}),
        ),
    )
    monkeypatch.setattr(
        cli_module,
        "_build_ticker_payload",
        lambda report, market_frame, chart_paths, position, account_positions, tax_assumptions, investor_db_path: {
            "ticker": report.regime.ticker,
            "regime": report.regime.latest_label,
            "probability": 0.91,
            "technical_signal": "Buy the dip",
            "days_in_regime": 5,
            "chart": chart_paths[report.regime.ticker],
            "composite_signal": {"composite_action": "Buy", "forward_signal": {"action": "Buy"}},
            "tax_adjusted_signals": [{"account_name": "Brokerage", "account_type": "TAXABLE", "adjusted_action": "Hold", "tax_note": "Wait for LTCG."}],
        },
    )

    output = io.StringIO()
    with redirect_stdout(output):
        cli_module.main()

    rendered = output.getvalue()
    assert "Benchmark: SOXX | regime=Bear | probability=91.0%" in rendered
    assert "NVDA: regime=Bull | probability=91.0% | signal=Buy | days_in_regime=5" in rendered
    assert "Tax [Brokerage / TAXABLE]: Hold | Wait for LTCG." in rendered
