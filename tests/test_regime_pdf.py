from __future__ import annotations

from fastapi.testclient import TestClient

from src.app.main import create_app
from src.app.routes import regime as regime_route
from src.app.routes.regime_pdf import generate_regime_pdf


def _minimal_payload() -> dict:
    return {
        "benchmark": "SOXX",
        "benchmark_regime": "Bull",
        "period": "3y",
        "portfolio_scope": "trust",
        "portfolio_mode": "Filtered holdings",
        "account_id": 101,
        "selected_count": 2,
        "total_market_value": 372538.0,
        "action_items_count": 1,
        "generated_at": "2026-03-25T17:13:50-04:00",
        "portfolio_summary": {
            "bull_pct": 100.0,
            "neutral_pct": 0.0,
            "bear_pct": 0.0,
            "transition_risk_pct": 9.5,
            "diversification_score": 0.33,
        },
        "digest": {"action_items": ["MU: WATCH — Buy in Bull regime"]},
        "rows": [],
    }


def test_generate_regime_pdf_returns_valid_pdf() -> None:
    payload = _minimal_payload()
    payload["rows"] = [
        {
            "ticker": "MU",
            "regime": "Bull",
            "probability_pct": 95.1,
            "composite_signal": "Buy",
            "action": "Buy",
            "current_price": 382.09,
            "price_targets": {
                "entry_price": 361.50,
                "exit_price": 468.83,
                "stop_price": 308.13,
                "risk_reward_ratio": 2.01,
            },
            "tax_status": "1 ST · 11 LT",
            "market_value": 372538.0,
            "relative_strength": "In-line",
            "lot_details": [
                {
                    "account_name": "RJ-Taxable",
                    "acquisition_date": "2025-10-06",
                    "qty": 500.0,
                    "cost_basis": 96579.70,
                    "term": "ST",
                    "days_to_ltcg": 195,
                    "near_ltcg": False,
                }
            ],
            "account_tax_signals": [
                {
                    "account_name": "RJ-Taxable",
                    "account_type": "TAXABLE",
                    "adjusted_action": "Buy",
                    "tax_note": "No material tax adjustment.",
                }
            ],
        }
    ]
    pdf_bytes = generate_regime_pdf(payload)
    assert isinstance(pdf_bytes, bytes)
    assert len(pdf_bytes) > 100
    assert pdf_bytes[:5] == b"%PDF-"


def test_generate_regime_pdf_empty_rows() -> None:
    payload = {
        "benchmark": "SOXX",
        "benchmark_regime": "Unavailable",
        "period": "3y",
        "portfolio_scope": "household",
        "portfolio_mode": "All holdings",
        "account_id": None,
        "selected_count": 0,
        "total_market_value": 0.0,
        "action_items_count": 0,
        "generated_at": "2026-03-25T17:00:00-04:00",
        "portfolio_summary": None,
        "digest": {"action_items": []},
        "rows": [],
    }
    pdf_bytes = generate_regime_pdf(payload)
    assert pdf_bytes[:5] == b"%PDF-"


def test_generate_regime_pdf_null_price_targets() -> None:
    payload = _minimal_payload()
    payload["rows"] = [
        {
            "ticker": "KO",
            "regime": "Neutral",
            "probability_pct": 99.4,
            "composite_signal": "Hold",
            "action": "—",
            "current_price": 62.50,
            "price_targets": None,
            "tax_status": "—",
            "market_value": None,
            "relative_strength": "In-line",
            "lot_details": [],
            "account_tax_signals": [],
        }
    ]
    pdf_bytes = generate_regime_pdf(payload)
    assert pdf_bytes[:5] == b"%PDF-"


def test_export_pdf_endpoint(monkeypatch) -> None:
    monkeypatch.setattr(regime_route, "load_payload", lambda: _minimal_payload())
    client = TestClient(create_app())
    response = client.get("/regime/export-pdf")
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/pdf"
    assert "regime_report_" in response.headers["content-disposition"]
    assert response.content[:5] == b"%PDF-"


def test_export_pdf_endpoint_no_data(monkeypatch) -> None:
    monkeypatch.setattr(regime_route, "load_payload", lambda: None)
    client = TestClient(create_app())
    response = client.get("/regime/export-pdf")
    assert response.status_code == 404
