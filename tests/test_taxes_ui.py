from __future__ import annotations

import datetime as dt

from starlette.requests import Request

from src.app.main import templates
from src.core.taxes import build_tax_dashboard
from src.db.models import TaxInput, TaxProfile
from src.utils.money import format_usd


def _request(path: str = "/taxes", query: str = "") -> Request:
    scope = {
        "type": "http",
        "method": "GET",
        "path": path,
        "headers": [],
        "query_string": query.encode(),
    }
    return Request(scope)


def test_taxes_overview_missing_inputs_banner(session):
    dashboard = build_tax_dashboard(session, year=2025, as_of=dt.date(2025, 1, 15))
    html = templates.get_template("taxes_overview.html").render(
        {
            "request": _request(),
            "actor": "tester",
            "auth_banner": None,
            "year": 2025,
            "dashboard": dashboard,
        }
    )
    assert "No tax inputs detected yet" in html


def test_taxes_overview_kpis_update_with_inputs(session):
    profile = TaxProfile(
        year=2025,
        filing_status="MFJ",
        deductions_mode="standard",
        itemized_amount=None,
        household_size=3,
        dependents_count=1,
        trust_income_taxable_to_user=True,
    )
    inputs = TaxInput(
        year=2025,
        data_json={
            "yoga_net_profit_monthly": [50000.0] + [0.0] * 11,
            "daughter_w2_withholding_monthly": [1500.0] + [0.0] * 11,
            "estimated_payments": [],
            "state_tax_rate": 0.0,
            "qualified_dividend_pct": 0.0,
        },
    )
    session.add_all([profile, inputs])
    session.commit()

    dashboard = build_tax_dashboard(session, year=2025, as_of=dt.date(2025, 1, 31))
    html = templates.get_template("taxes_overview.html").render(
        {
            "request": _request(),
            "actor": "tester",
            "auth_banner": None,
            "year": 2025,
            "dashboard": dashboard,
        }
    )

    total_tax = format_usd(dashboard.summary["total_tax"])
    jan_tax = format_usd(dashboard.monthly[0]["tax_ytd"])
    assert total_tax in html
    assert jan_tax in html
