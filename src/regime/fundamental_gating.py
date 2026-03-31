from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, cast

import pandas as pd

from .fundamental_data import FinancialStatements, fetch_financial_statements
from .persistence import get_setting

logger = logging.getLogger(__name__)

DEFAULT_PIOTROSKI_MIN = 6
DEFAULT_ROIC_MUST_EXCEED_WACC = True
DEFAULT_ROIC_LOOKBACK_YEARS = 3
DEFAULT_PASS_ON_INSUFFICIENT_DATA = True
DEFAULT_ALTMAN_Z_DISTRESS_THRESHOLD = 1.81
DEFAULT_ALTMAN_Z_GREY_THRESHOLD = 2.99
DEFAULT_ALTMAN_Z_ENABLED = True


@dataclass
class PiotroskiResult:
    ticker: str
    score: int
    components: dict[str, int]
    details: dict[str, Any]
    data_quality: str
    years_used: int


@dataclass
class ROICResult:
    ticker: str
    roic_avg: float | None
    wacc_estimate: float
    roic_exceeds_wacc: bool
    roic_by_year: dict[str, float]
    data_quality: str


@dataclass
class AltmanZScoreResult:
    ticker: str
    z_score: float | None
    interpretation: str
    components: dict[str, float]
    details: dict[str, Any]
    data_quality: str


@dataclass
class FundamentalGateResult:
    ticker: str
    passed: bool
    piotroski: PiotroskiResult | None
    roic: ROICResult | None
    veto_reasons: list[str] = field(default_factory=list)
    altman_z: AltmanZScoreResult | None = None


def get_fundamental_gate_settings() -> dict[str, Any]:
    return {
        "piotroski_min": _int_setting("fundamental_piotroski_min", DEFAULT_PIOTROSKI_MIN, min_value=0, max_value=9),
        "require_roic_above_wacc": _bool_setting("fundamental_require_roic", DEFAULT_ROIC_MUST_EXCEED_WACC),
        "roic_lookback_years": _int_setting("fundamental_roic_lookback", DEFAULT_ROIC_LOOKBACK_YEARS, min_value=1, max_value=5),
        "pass_on_insufficient_data": _bool_setting("fundamental_pass_on_insufficient", DEFAULT_PASS_ON_INSUFFICIENT_DATA),
        "gate_enabled": _bool_setting("fundamental_gate_enabled", True),
        "altman_z_enabled": _bool_setting("fundamental_altman_z_enabled", DEFAULT_ALTMAN_Z_ENABLED),
        "altman_z_distress_threshold": _float_setting(
            "fundamental_altman_z_threshold",
            DEFAULT_ALTMAN_Z_DISTRESS_THRESHOLD,
            min_value=0.5,
            max_value=5.0,
        ),
    }


def _bool_setting(key: str, default: bool) -> bool:
    raw = get_setting(key)
    if raw in (None, ""):
        return default
    return str(raw).strip().lower() in {"true", "1", "yes", "on"}


def _int_setting(key: str, default: int, *, min_value: int | None = None, max_value: int | None = None) -> int:
    raw = get_setting(key)
    try:
        value = int(str(raw)) if raw not in (None, "") else default
    except Exception:
        value = default
    if min_value is not None:
        value = max(min_value, value)
    if max_value is not None:
        value = min(max_value, value)
    return value


def _float_setting(key: str, default: float, *, min_value: float | None = None, max_value: float | None = None) -> float:
    raw = get_setting(key)
    try:
        value = float(str(raw)) if raw not in (None, "") else default
    except Exception:
        value = default
    if min_value is not None:
        value = max(min_value, value)
    if max_value is not None:
        value = min(max_value, value)
    return value


def _get(df: pd.DataFrame, labels: list[str], col_idx: int = 0) -> float | None:
    if df.empty or col_idx >= len(df.columns):
        return None
    exact = {str(idx).strip().lower(): idx for idx in df.index}
    for label in labels:
        idx = exact.get(str(label).strip().lower())
        if idx is None:
            continue
        try:
            value = df.iloc[df.index.get_loc(idx), col_idx]
        except Exception:
            continue
        if pd.notna(value):
            try:
                return float(value)
            except Exception:
                continue
    return None


def _ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator == 0:
        return None
    numerator_value = cast(float, numerator)
    denominator_value = cast(float, denominator)
    return float(numerator_value) / float(denominator_value)


def calculate_piotroski_f_score(
    ticker: str,
    *,
    statements: FinancialStatements | None = None,
) -> PiotroskiResult:
    if statements is None:
        statements = fetch_financial_statements(ticker)
    income = statements.income_statement
    balance = statements.balance_sheet
    cashflow = statements.cashflow
    if income.empty or balance.empty or cashflow.empty:
        return PiotroskiResult(
            ticker=ticker,
            score=0,
            components={},
            details={"error": "insufficient_data"},
            data_quality="insufficient",
            years_used=0,
        )
    years = min(len(income.columns), len(balance.columns), len(cashflow.columns))
    if years < 2:
        return PiotroskiResult(
            ticker=ticker,
            score=0,
            components={},
            details={"error": "need_2_years"},
            data_quality="insufficient",
            years_used=years,
        )

    components: dict[str, int] = {}
    details: dict[str, Any] = {}

    net_income = _get(income, ["Net Income", "Net Income Common Stockholders"])
    ocf = _get(cashflow, ["Operating Cash Flow", "Total Cash From Operating Activities", "Cash Flow From Continuing Operating Activities"])
    total_assets_curr = _get(balance, ["Total Assets"], 0)
    total_assets_prev = _get(balance, ["Total Assets"], 1)
    net_income_prev = _get(income, ["Net Income", "Net Income Common Stockholders"], 1)
    roa_curr = _ratio(net_income, total_assets_curr)
    roa_prev = _ratio(net_income_prev, total_assets_prev)

    components["net_income_positive"] = 1 if net_income is not None and net_income > 0 else 0
    components["ocf_positive"] = 1 if ocf is not None and ocf > 0 else 0
    components["roa_improving"] = 1 if roa_curr is not None and roa_prev is not None and roa_curr > roa_prev else 0
    components["ocf_exceeds_net_income"] = 1 if ocf is not None and net_income is not None and ocf > net_income else 0

    ltd_curr = _get(balance, ["Total Debt", "Long Term Debt", "Long Term Debt And Capital Lease Obligation"], 0) or 0.0
    ltd_prev = _get(balance, ["Total Debt", "Long Term Debt", "Long Term Debt And Capital Lease Obligation"], 1) or 0.0
    leverage_curr = _ratio(ltd_curr, total_assets_curr)
    leverage_prev = _ratio(ltd_prev, total_assets_prev)
    components["leverage_decreasing"] = 1 if leverage_curr is not None and leverage_prev is not None and leverage_curr < leverage_prev else 0

    ca_curr = _get(balance, ["Current Assets"], 0)
    cl_curr = _get(balance, ["Current Liabilities"], 0)
    ca_prev = _get(balance, ["Current Assets"], 1)
    cl_prev = _get(balance, ["Current Liabilities"], 1)
    cr_curr = _ratio(ca_curr, cl_curr)
    cr_prev = _ratio(ca_prev, cl_prev)
    components["current_ratio_improving"] = 1 if cr_curr is not None and cr_prev is not None and cr_curr > cr_prev else 0

    shares_curr = _get(balance, ["Ordinary Shares Number", "Share Issued"], 0)
    shares_prev = _get(balance, ["Ordinary Shares Number", "Share Issued"], 1)
    components["no_dilution"] = 1 if shares_curr is not None and shares_prev is not None and shares_curr <= shares_prev else 0

    revenue_curr = _get(income, ["Total Revenue"], 0)
    revenue_prev = _get(income, ["Total Revenue"], 1)
    gross_profit_curr = _get(income, ["Gross Profit"], 0)
    gross_profit_prev = _get(income, ["Gross Profit"], 1)
    gm_curr = _ratio(gross_profit_curr, revenue_curr)
    gm_prev = _ratio(gross_profit_prev, revenue_prev)
    components["gross_margin_improving"] = 1 if gm_curr is not None and gm_prev is not None and gm_curr > gm_prev else 0

    at_curr = _ratio(revenue_curr, total_assets_curr)
    at_prev = _ratio(revenue_prev, total_assets_prev)
    components["asset_turnover_improving"] = 1 if at_curr is not None and at_prev is not None and at_curr > at_prev else 0

    details.update(
        {
            "net_income": net_income,
            "operating_cash_flow": ocf,
            "roa_current": roa_curr,
            "roa_prior": roa_prev,
            "leverage_current": leverage_curr,
            "leverage_prior": leverage_prev,
            "current_ratio_current": cr_curr,
            "current_ratio_prior": cr_prev,
            "shares_current": shares_curr,
            "shares_prior": shares_prev,
            "gross_margin_current": gm_curr,
            "gross_margin_prior": gm_prev,
            "asset_turnover_current": at_curr,
            "asset_turnover_prior": at_prev,
        }
    )
    score = sum(components.values())
    return PiotroskiResult(
        ticker=ticker,
        score=score,
        components=components,
        details=details,
        data_quality="full",
        years_used=years,
    )


def calculate_roic(
    ticker: str,
    *,
    statements: FinancialStatements | None = None,
    lookback_years: int = DEFAULT_ROIC_LOOKBACK_YEARS,
) -> ROICResult:
    if statements is None:
        statements = fetch_financial_statements(ticker)
    income = statements.income_statement
    balance = statements.balance_sheet
    info = statements.info or {}
    if income.empty or balance.empty:
        return ROICResult(ticker=ticker, roic_avg=None, wacc_estimate=0.0, roic_exceeds_wacc=False, roic_by_year={}, data_quality="insufficient")

    roic_by_year: dict[str, float] = {}
    years_to_check = min(int(lookback_years), len(income.columns), len(balance.columns))
    for index in range(years_to_check):
        operating_income = _get(income, ["Operating Income", "EBIT"], index)
        tax_provision = _get(income, ["Tax Provision", "Income Tax Expense"], index)
        pretax_income = _get(income, ["Pretax Income", "Income Before Tax"], index)
        total_assets = _get(balance, ["Total Assets"], index)
        current_liabilities = _get(balance, ["Current Liabilities"], index) or 0.0
        if operating_income is None or total_assets is None or total_assets == 0:
            continue
        total_assets_value = cast(float, total_assets)
        if tax_provision is not None and pretax_income is not None and pretax_income > 0:
            tax_rate = min(max(float(tax_provision) / float(pretax_income), 0.0), 0.50)
        else:
            tax_rate = 0.21
        nopat = operating_income * (1 - tax_rate)
        invested_capital = total_assets_value - float(current_liabilities)
        if invested_capital > 0:
            roic_by_year[str(income.columns[index])[:10]] = round((nopat / invested_capital) * 100.0, 2)
    roic_avg = (sum(roic_by_year.values()) / len(roic_by_year)) if roic_by_year else None

    beta = float(info.get("beta") or 1.0)
    risk_free = float(info.get("tenYearAverageReturn") or 4.0)
    erp = 5.5
    cost_of_equity = risk_free + beta * erp
    total_debt = _get(balance, ["Total Debt", "Long Term Debt", "Long Term Debt And Capital Lease Obligation"], 0) or 0.0
    interest_expense = abs(_get(income, ["Interest Expense"], 0) or 0.0)
    cost_of_debt = (interest_expense / total_debt * 100.0) if total_debt > 0 else 5.0
    market_cap = float(info.get("marketCap") or 0.0)
    total_capital = total_debt + market_cap if (total_debt + market_cap) > 0 else 1.0
    debt_weight = total_debt / total_capital if total_capital > 0 else 0.0
    equity_weight = 1.0 - debt_weight
    tax_provision_latest = _get(income, ["Tax Provision", "Income Tax Expense"], 0)
    pretax_latest = _get(income, ["Pretax Income", "Income Before Tax"], 0)
    if tax_provision_latest is not None and pretax_latest is not None and pretax_latest > 0:
        effective_tax = min(max(float(tax_provision_latest) / float(pretax_latest), 0.0), 0.50)
    else:
        effective_tax = 0.21
    wacc = (equity_weight * cost_of_equity) + (debt_weight * cost_of_debt * (1 - effective_tax))
    quality = "full" if len(roic_by_year) >= years_to_check and years_to_check > 0 else ("partial" if roic_by_year else "insufficient")
    return ROICResult(
        ticker=ticker,
        roic_avg=round(roic_avg, 2) if roic_avg is not None else None,
        wacc_estimate=round(wacc, 2),
        roic_exceeds_wacc=bool(roic_avg is not None and roic_avg > wacc),
        roic_by_year=roic_by_year,
        data_quality=quality,
    )


def calculate_altman_z_score(
    ticker: str,
    *,
    statements: FinancialStatements | None = None,
) -> AltmanZScoreResult:
    if statements is None:
        statements = fetch_financial_statements(ticker)
    balance = statements.balance_sheet
    income = statements.income_statement
    info = statements.info or {}
    if balance.empty or income.empty:
        return AltmanZScoreResult(
            ticker=ticker,
            z_score=None,
            interpretation="insufficient",
            components={},
            details={"error": "insufficient_data"},
            data_quality="insufficient",
        )

    total_assets = _get(balance, ["Total Assets"], 0)
    if total_assets is None or total_assets <= 0:
        return AltmanZScoreResult(
            ticker=ticker,
            z_score=None,
            interpretation="insufficient",
            components={},
            details={"error": "no_total_assets"},
            data_quality="insufficient",
        )

    current_assets = _get(balance, ["Current Assets"], 0) or 0.0
    current_liabilities = _get(balance, ["Current Liabilities"], 0) or 0.0
    working_capital = current_assets - current_liabilities

    retained_earnings = _get(
        balance,
        ["Retained Earnings", "Retained Earnings Deficit", "Accumulated Earnings"],
        0,
    ) or 0.0
    ebit = _get(income, ["Operating Income", "EBIT"], 0) or 0.0
    market_cap = float(info.get("marketCap") or 0.0)

    total_liabilities = _get(
        balance,
        ["Total Liabilities Net Minority Interest", "Total Liab", "Total Non Current Liabilities Net Minority Interest"],
        0,
    )
    if total_liabilities is None:
        stockholders_equity = _get(
            balance,
            ["Total Stockholders Equity", "Stockholders Equity", "Total Equity Gross Minority Interest"],
            0,
        )
        if stockholders_equity is not None:
            total_liabilities = float(total_assets) - float(stockholders_equity)
        else:
            total_debt = _get(balance, ["Total Debt", "Long Term Debt"], 0) or 0.0
            total_liabilities = total_debt + current_liabilities

    revenue = _get(income, ["Total Revenue"], 0) or 0.0

    x1 = working_capital / float(total_assets)
    x2 = retained_earnings / float(total_assets)
    x3 = ebit / float(total_assets)
    x4 = (market_cap / float(total_liabilities)) if total_liabilities and total_liabilities > 0 else 0.0
    x5 = revenue / float(total_assets)
    z_score = 1.2 * x1 + 1.4 * x2 + 3.3 * x3 + 0.6 * x4 + 1.0 * x5

    if z_score > DEFAULT_ALTMAN_Z_GREY_THRESHOLD:
        interpretation = "Safe"
    elif z_score >= DEFAULT_ALTMAN_Z_DISTRESS_THRESHOLD:
        interpretation = "Grey Zone"
    else:
        interpretation = "Distress"

    return AltmanZScoreResult(
        ticker=ticker,
        z_score=round(z_score, 2),
        interpretation=interpretation,
        components={
            "X1_working_capital_ta": round(x1, 4),
            "X2_retained_earnings_ta": round(x2, 4),
            "X3_ebit_ta": round(x3, 4),
            "X4_market_equity_tl": round(x4, 4),
            "X5_sales_ta": round(x5, 4),
        },
        details={
            "working_capital": working_capital,
            "retained_earnings": retained_earnings,
            "ebit": ebit,
            "market_cap": market_cap,
            "total_liabilities": total_liabilities,
            "revenue": revenue,
            "total_assets": total_assets,
        },
        data_quality="full" if market_cap > 0 else "partial",
    )


def run_fundamental_gate(
    ticker: str,
    *,
    piotroski_min: int = DEFAULT_PIOTROSKI_MIN,
    require_roic_above_wacc: bool = DEFAULT_ROIC_MUST_EXCEED_WACC,
    roic_lookback_years: int = DEFAULT_ROIC_LOOKBACK_YEARS,
    pass_on_insufficient_data: bool = DEFAULT_PASS_ON_INSUFFICIENT_DATA,
    altman_z_enabled: bool = DEFAULT_ALTMAN_Z_ENABLED,
    altman_z_distress_threshold: float = DEFAULT_ALTMAN_Z_DISTRESS_THRESHOLD,
) -> FundamentalGateResult:
    statements = fetch_financial_statements(ticker)
    veto_reasons: list[str] = []
    piotroski = calculate_piotroski_f_score(ticker, statements=statements)
    if piotroski.data_quality == "insufficient":
        if not pass_on_insufficient_data:
            veto_reasons.append(f"Piotroski: insufficient data ({piotroski.years_used} years)")
    elif piotroski.score < int(piotroski_min):
        veto_reasons.append(f"Piotroski F-Score {piotroski.score} < {int(piotroski_min)}")

    roic = calculate_roic(ticker, statements=statements, lookback_years=int(roic_lookback_years))
    if roic.data_quality == "insufficient":
        if not pass_on_insufficient_data:
            veto_reasons.append("ROIC: insufficient data")
    elif require_roic_above_wacc and roic.roic_avg is not None and not roic.roic_exceeds_wacc:
        veto_reasons.append(f"ROIC {roic.roic_avg:.1f}% <= WACC {roic.wacc_estimate:.1f}%")

    altman_z = calculate_altman_z_score(ticker, statements=statements)
    if altman_z.data_quality == "insufficient":
        if not pass_on_insufficient_data:
            veto_reasons.append("Altman Z-Score: insufficient data")
    elif altman_z_enabled and altman_z.z_score is not None and altman_z.z_score < float(altman_z_distress_threshold):
        veto_reasons.append(
            f"Altman Z-Score {altman_z.z_score:.2f} < {float(altman_z_distress_threshold):.2f} (Distress Zone)"
        )

    return FundamentalGateResult(
        ticker=str(ticker or "").upper(),
        passed=not veto_reasons,
        piotroski=piotroski,
        roic=roic,
        veto_reasons=veto_reasons,
        altman_z=altman_z,
    )
