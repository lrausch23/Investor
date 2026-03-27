from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

INVESTOR_ROOT = Path("/Volumes/T9/Projects/Dev/Investor")
if str(INVESTOR_ROOT) not in sys.path:
    sys.path.insert(0, str(INVESTOR_ROOT))

from src.app.routes import regime as regime_route
from src.core.external_holdings import get_lot_details_by_scope
from src.db.models import Account, Base, PositionLot, Security, TaxLot, TaxpayerEntity
from _fixtures import FakeRegime


def _session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)
    return SessionLocal()


def _seed_scope_data(session):
    personal = TaxpayerEntity(name="Personal", type="PERSONAL")
    trust = TaxpayerEntity(name="Trust", type="TRUST")
    session.add_all([personal, trust])
    session.flush()
    acct_personal = Account(name="RJ-Taxable", broker="RJ", account_type="TAXABLE", taxpayer_entity_id=personal.id)
    acct_trust = Account(name="Trust-1", broker="IB", account_type="TAXABLE", taxpayer_entity_id=trust.id)
    session.add_all([acct_personal, acct_trust])
    session.flush()
    nvda = Security(ticker="NVDA", name="NVIDIA", asset_class="EQUITY", expense_ratio=0.0, metadata_json={})
    avgo = Security(ticker="AVGO", name="Broadcom", asset_class="EQUITY", expense_ratio=0.0, metadata_json={})
    session.add_all([nvda, avgo])
    session.flush()
    return personal, trust, acct_personal, acct_trust, nvda, avgo


def _runtime() -> dict:
    return {
        "DEFAULT_TICKERS": ["NVDA"],
        "download_market_frame": lambda **kwargs: type("MarketSeries", (), {"frame": pd.DataFrame({"price": [100.0, 101.0], "volume": [1_000_000, 1_050_000], "high": [101.0, 102.0], "low": [99.0, 100.0]})})(),
        "generate_weekly_digest": lambda **kwargs: type("Digest", (), {"action_items": [], "entries": [], "regime_changes": [], "sentiment_divergences": [], "tax_alerts": [], "generated_at": "2026-03-24T12:00:00+00:00"})(),
        "fit_regime_model": lambda ticker, market_frame: FakeRegime(ticker, "Bear" if ticker == "SOXX" else "Bull"),
        "configured_frontier_model": lambda provider="auto": "OpenAI: gpt-4o-mini",
        "get_investor_db_path": lambda: "/tmp/investor.db",
        "get_portfolio_positions": lambda db_path, tickers=None, account_id=None: [],
        "get_portfolio_tickers": lambda db_path: ["NVDA"],
        "get_tax_assumptions": lambda db_path: {},
        "get_wash_sale_risk": lambda db_path, ticker: "NONE",
        "positions_by_ticker_and_account": lambda positions: {"NVDA": [SimpleNamespace(current_price=125.0)]},
        "save_regime_event": lambda ticker, label, state_id: {"previous_label": "Neutral", "days_in_regime": 3},
        "build_composite_signal": lambda *args, **kwargs: type("Composite", (), {"composite_action": "Sell"})(),
        "compute_technicals": lambda *args, **kwargs: pd.DataFrame({"rsi_14": [45, 50], "bb_pct": [0.4, 0.5], "macd_histogram": [0.1, 0.2]}),
        "confidence_trajectory": lambda *args, **kwargs: type("Trajectory", (), {"trend": "rising"})(),
        "forward_regime_curve": lambda *args, **kwargs: pd.DataFrame({"day": [1, 2], "p_bull": [0.7, 0.72], "p_neutral": [0.2, 0.18], "p_bear": [0.1, 0.1]}),
        "intra_regime_signal": lambda *args, **kwargs: "Take partial profits",
        "sentiment_momentum": lambda *args, **kwargs: (
            type("Sentiment", (), {"trend": "improving"})(),
            pd.DataFrame({"recorded_at": ["2026-03-23"], "score": [1]}),
        ),
        "signal_from_forward_curve": lambda *args, **kwargs: type("Signal", (), {"action": "Sell"})(),
        "tax_adjusted_signals": lambda *args, **kwargs: [],
        "list_theses": lambda: [],
        "upsert_thesis": lambda ticker, thesis=None: None,
    }


def test_get_lot_details_by_scope_returns_lots() -> None:
    session = _session()
    _personal, _trust, acct_personal, _acct_trust, _nvda, _avgo = _seed_scope_data(session)
    session.add(
        PositionLot(
            account_id=acct_personal.id,
            ticker="NVDA",
            acquisition_date=dt.date.today() - dt.timedelta(days=120),
            qty=3,
            basis_total=300.0,
            adjusted_basis_total=None,
        )
    )
    session.commit()
    lots = get_lot_details_by_scope(session, tickers=["NVDA"], scope="personal", current_prices={"NVDA": 125.0})
    assert list(lots) == ["NVDA"]
    assert lots["NVDA"][0]["account_name"] == "RJ-Taxable"
    assert lots["NVDA"][0]["term"] == "ST"
    assert lots["NVDA"][0]["unrealized_gain"] == 75.0


def test_lot_details_tax_lot_precedence() -> None:
    session = _session()
    personal, _trust, acct_personal, _acct_trust, nvda, _avgo = _seed_scope_data(session)
    session.add(
        TaxLot(
            taxpayer_id=personal.id,
            account_id=acct_personal.id,
            security_id=nvda.id,
            acquired_date=dt.date.today() - dt.timedelta(days=500),
            quantity_open=2,
            basis_open=180.0,
            source="RECONSTRUCTED",
            metadata_json={},
        )
    )
    session.add(
        PositionLot(
            account_id=acct_personal.id,
            ticker="NVDA",
            acquisition_date=dt.date.today() - dt.timedelta(days=100),
            qty=4,
            basis_total=400.0,
            adjusted_basis_total=None,
        )
    )
    session.commit()
    lots = get_lot_details_by_scope(session, tickers=["NVDA"], scope="personal", current_prices={"NVDA": 125.0})
    assert len(lots["NVDA"]) == 1
    assert lots["NVDA"][0]["qty"] == 2.0
    assert lots["NVDA"][0]["term"] == "LT"


def test_lot_details_scope_filtering_and_account_id() -> None:
    session = _session()
    personal, trust, acct_personal, acct_trust, nvda, avgo = _seed_scope_data(session)
    session.add_all(
        [
            TaxLot(
                taxpayer_id=personal.id,
                account_id=acct_personal.id,
                security_id=nvda.id,
                acquired_date=dt.date.today() - dt.timedelta(days=400),
                quantity_open=1,
                basis_open=100.0,
                source="RECONSTRUCTED",
                metadata_json={},
            ),
            TaxLot(
                taxpayer_id=trust.id,
                account_id=acct_trust.id,
                security_id=avgo.id,
                acquired_date=dt.date.today() - dt.timedelta(days=40),
                quantity_open=1,
                basis_open=200.0,
                source="RECONSTRUCTED",
                metadata_json={},
            ),
        ]
    )
    session.commit()
    personal_lots = get_lot_details_by_scope(session, tickers=["NVDA", "AVGO"], scope="personal")
    trust_lots = get_lot_details_by_scope(session, tickers=["NVDA", "AVGO"], scope="trust")
    account_lots = get_lot_details_by_scope(session, tickers=["NVDA", "AVGO"], scope="household", account_id=acct_trust.id)
    assert len(personal_lots["NVDA"]) == 1 and personal_lots["AVGO"] == []
    assert trust_lots["NVDA"] == [] and len(trust_lots["AVGO"]) == 1
    assert account_lots["NVDA"] == [] and len(account_lots["AVGO"]) == 1


def test_tax_status_enriched_format() -> None:
    status, st_count, lt_count = regime_route._lot_term_status(
        [{"term": "ST"}, {"term": "ST"}, {"term": "LT"}, {"term": "LT"}, {"term": "LT"}]
    )
    assert status == "2 ST · 3 LT"
    assert st_count == 2
    assert lt_count == 3


def test_tax_status_pure_lt_and_st() -> None:
    lt_status, lt_st_count, lt_lt_count = regime_route._lot_term_status([{"term": "LT"}, {"term": "LT"}])
    st_status, st_st_count, st_lt_count = regime_route._lot_term_status([{"term": "ST"}])
    assert (lt_status, lt_st_count, lt_lt_count) == ("LT", 0, 2)
    assert (st_status, st_st_count, st_lt_count) == ("ST", 1, 0)


def test_lot_details_empty_shows_no_data_flag(monkeypatch) -> None:
    runtime = _runtime()
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    monkeypatch.setattr(regime_route, "get_current_tickers_by_scope", lambda session, scope, account_id=None: ["NVDA"])
    monkeypatch.setattr(regime_route, "get_lot_details_by_scope", lambda session, **kwargs: {"NVDA": []})
    payload = regime_route._build_regime_dashboard_payload(
        session=object(),
        benchmark="SOXX",
        period="3y",
        tickers=["NVDA"],
        portfolio_scope="household",
    )
    row = payload["rows"][0]
    assert row["lot_details"] == []
    assert row["is_portfolio_holding"] is True
