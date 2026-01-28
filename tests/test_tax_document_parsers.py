from __future__ import annotations

from src.core.tax_documents import parse_1095a, parse_1099div, parse_w2


def _field_map(fields):
    return {f["key"]: f["value"] for f in fields}


def test_parse_w2_basic():
    text = """
    Form W-2 Wage and Tax Statement
    1 Wages, tips, other compensation 12,345.67
    2 Federal income tax withheld 1,234.00
    Employer's name ACME INC
    Employer's EIN 12-3456789
    """
    fields, meta, warnings = parse_w2(text)
    fm = _field_map(fields)
    assert round(float(fm["wages"]), 2) == 12345.67
    assert round(float(fm["federal_withholding"]), 2) == 1234.00
    assert "employer_ein" in fm
    assert meta.get("employer_ein") == "12-3456789"
    assert "Missing Box 1 wages." not in warnings


def test_parse_1099div_basic():
    text = """
    FORM 1099-DIV
    1a Ordinary dividends 500.00
    1b Qualified dividends 200.00
    2a Capital gain distributions 50.00
    PAYER'S NAME BIG BROKER LLC
    """
    fields, meta, warnings = parse_1099div(text)
    fm = _field_map(fields)
    assert round(float(fm["ordinary_dividends"]), 2) == 500.00
    assert round(float(fm["qualified_dividends"]), 2) == 200.00
    assert round(float(fm["cap_gain_dist"]), 2) == 50.00
    assert meta.get("payer_name") == "BIG BROKER LLC"


def test_parse_1095a_annual_totals():
    text = """
    Form 1095-A
    Annual Totals 1200.00 1100.00 900.00
    """
    fields, meta, warnings = parse_1095a(text)
    fm = _field_map(fields)
    assert round(float(fm["aca_premium_total"]), 2) == 1200.00
    assert round(float(fm["aca_slcsp_total"]), 2) == 1100.00
    assert round(float(fm["aca_aptc_total"]), 2) == 900.00
    assert warnings == [] or "Missing ACA premium totals." not in warnings
