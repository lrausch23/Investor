from __future__ import annotations

import datetime as dt
import io
import json
import os
import urllib.parse
import zipfile
from typing import Any

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import RedirectResponse, Response
from sqlalchemy.orm import Session

from src.app.auth import auth_banner_message, require_actor
from src.app.db import db_session
from src.app.utils import jsonable
from src.core.taxes import (
    TAX_TAG_CATEGORIES,
    TAX_TAG_LABELS,
    available_tax_years,
    auto_tag_tax_transactions,
    build_tax_dashboard,
    capital_gains_summary_by_account,
    dividend_summary_by_account,
    fetch_tax_tagged_transactions,
    get_or_create_tax_inputs,
    get_or_create_tax_profile,
    is_internal_transfer_like,
    list_capital_gains_details,
    list_dividend_details,
    list_estimated_payment_details,
    list_interest_details,
    list_ira_tax_details,
    list_other_withholding_details,
    list_trust_pnl_details,
    list_wash_sale_details,
    list_tax_tagged_transactions,
    list_w2_withholding_details,
    normalize_tax_inputs,
    interest_summary_by_account,
    suggest_tax_tag,
    tax_account_summaries,
)
from src.core.tax_documents import aggregate_tax_doc_overrides, build_tax_reconciliation, list_household_entities, tax_docs_summary
from src.db.audit import log_change
from src.db.models import Account, IncomeEvent, TaxDocument, TaxFact, TaxInput, TaxProfile, TaxTag, Transaction


router = APIRouter(prefix="/taxes", tags=["taxes"])


def _parse_float(value: str | None, default: float = 0.0) -> float:
    s = (value or "").strip()
    if not s:
        return float(default)
    try:
        return float(s)
    except Exception:
        return float(default)


def _parse_int(value: str | None, default: int = 0) -> int:
    s = (value or "").strip()
    if not s:
        return int(default)
    try:
        return int(s)
    except Exception:
        return int(default)


def _parse_monthly(prefix: str, form: dict[str, Any]) -> list[float]:
    values: list[float] = []
    for i in range(1, 13):
        values.append(_parse_float(form.get(f"{prefix}_{i}")))
    return values


def _safe_return_to(value: str, *, default: str) -> str:
    s = (value or "").strip()
    if not s:
        return default
    if "://" in s or not s.startswith("/"):
        return default
    return s


@router.get("")
def taxes_overview(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    year_raw = (request.query_params.get("year") or "").strip()
    year = int(year_raw) if year_raw.isdigit() else dt.date.today().year
    auto_tag_tax_transactions(session, year=year)
    dashboard = build_tax_dashboard(session, year=year)
    tax_years = available_tax_years(session, default_year=year)
    trust_pnl_details = list_trust_pnl_details(session, year=year)

    from src.app.main import templates

    return templates.TemplateResponse(
        "taxes_overview.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "year": year,
            "tax_years": tax_years,
            "dashboard": dashboard,
            "trust_pnl_details": trust_pnl_details,
        },
    )


@router.get("/inputs")
def taxes_inputs(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    year_raw = (request.query_params.get("year") or "").strip()
    year = int(year_raw) if year_raw.isdigit() else dt.date.today().year
    auto_tag_tax_transactions(session, year=year)
    profile = get_or_create_tax_profile(session, year=year)
    inputs_row = get_or_create_tax_inputs(session, year=year)
    inputs = normalize_tax_inputs(dict(inputs_row.data_json or {}))
    live_doc_overrides = aggregate_tax_doc_overrides(session, tax_year=year)
    if live_doc_overrides.get("sources"):
        inputs["tax_doc_overrides"] = live_doc_overrides
    dashboard = build_tax_dashboard(session, year=year)
    tax_years = available_tax_years(session, default_year=year)

    account_id_raw = (request.query_params.get("account_id") or "").strip()
    account_id = int(account_id_raw) if account_id_raw.isdigit() else None

    summaries = tax_account_summaries(session, year=year)
    selected_summary = None
    if account_id is not None:
        for row in summaries:
            if int(row.get("account_id") or 0) == account_id:
                selected_summary = row
                break
    tx_rows = []
    other_withholding_rows: list[dict[str, Any]] = []
    if account_id is not None:
        tx_rows = fetch_tax_tagged_transactions(session, year=year, account_id=account_id, limit=300)
        # Filter to tax-relevant rows for the detail view.
        filtered: list[tuple[Any, Any, Any]] = []
        trust_cutoff = dt.date(2025, 6, 6) if year == 2025 else None
        for tx, acct, tag in tx_rows:
            if trust_cutoff and selected_summary and str(selected_summary.get("taxpayer_type") or "").upper() == "TRUST":
                if tx.date < trust_cutoff:
                    continue
            if str(acct.account_type or "").upper() == "IRA":
                suggested = suggest_tax_tag(tx, acct, None, trust_start=trust_cutoff)
                if suggested is None:
                    continue
                if tag is None:
                    tag = type("Tag", (), {"category": suggested, "note": "auto"})()
                elif tag.category != suggested:
                    tag = type("Tag", (), {"category": suggested, "note": "auto"})()
            if tag is not None:
                filtered.append((tx, acct, tag))
                continue
            if tx.type == "TRANSFER" and float(tx.amount or 0.0) < 0 and not is_internal_transfer_like(tx.lot_links_json):
                filtered.append((tx, acct, tag))
                continue
        tx_rows = filtered
        acct_row = session.query(Account).filter(Account.id == account_id).one_or_none()
        if acct_row and str(acct_row.account_type or "").upper() != "IRA":
            tx_q = (
                session.query(Transaction)
                .filter(
                    Transaction.account_id == account_id,
                    Transaction.date >= dt.date(year, 1, 1),
                    Transaction.date <= dt.date(year, 12, 31),
                    Transaction.type == "WITHHOLDING",
                )
                .order_by(Transaction.date.desc(), Transaction.id.desc())
            )
            if trust_cutoff and selected_summary and str(selected_summary.get("taxpayer_type") or "").upper() == "TRUST":
                tx_q = tx_q.filter(Transaction.date >= trust_cutoff)
            for tx in tx_q.all():
                other_withholding_rows.append(
                    {
                        "date": tx.date,
                        "source": "Transaction",
                        "amount": float(tx.amount or 0.0),
                        "description": (tx.lot_links_json or {}).get("description") or tx.ticker or "",
                    }
                )

            ev_q = (
                session.query(IncomeEvent)
                .filter(
                    IncomeEvent.account_id == account_id,
                    IncomeEvent.date >= dt.date(year, 1, 1),
                    IncomeEvent.date <= dt.date(year, 12, 31),
                    IncomeEvent.type == "WITHHOLDING",
                )
                .order_by(IncomeEvent.date.desc(), IncomeEvent.id.desc())
            )
            if trust_cutoff and selected_summary and str(selected_summary.get("taxpayer_type") or "").upper() == "TRUST":
                ev_q = ev_q.filter(IncomeEvent.date >= trust_cutoff)
            for ev in ev_q.all():
                other_withholding_rows.append(
                    {
                        "date": ev.date,
                        "source": "IncomeEvent",
                        "amount": float(ev.amount or 0.0),
                        "description": "",
                    }
                )
            other_withholding_rows.sort(key=lambda r: (r.get("date"), r.get("source")), reverse=True)

    tax_param_overrides = inputs.get("tax_parameter_overrides") or {}
    overrides_text = json.dumps(tax_param_overrides, indent=2, sort_keys=True)

    from src.app.main import templates

    return templates.TemplateResponse(
        "taxes_inputs.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "year": year,
            "tax_years": tax_years,
            "profile": profile,
            "inputs": inputs,
            "dashboard": dashboard,
            "summaries": summaries,
            "selected_account_id": account_id,
            "selected_summary": selected_summary,
            "tx_rows": tx_rows,
            "other_withholding_rows": other_withholding_rows,
            "tag_categories": TAX_TAG_CATEGORIES,
            "tag_labels": TAX_TAG_LABELS,
            "overrides_text": overrides_text,
        },
    )


@router.post("/inputs")
def taxes_inputs_update(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    year: int = Form(...),
    filing_status: str = Form(default="MFJ"),
    state_code: str = Form(default=""),
    state_tax_rate: str = Form(default=""),
    deductions_mode: str = Form(default="standard"),
    itemized_amount: str = Form(default=""),
    household_size: str = Form(default="3"),
    dependents_count: str = Form(default="1"),
    trust_income_taxable_to_user: str = Form(default=""),
    qualified_dividend_pct: str = Form(default=""),
    niit_enabled: str = Form(default=""),
    niit_rate: str = Form(default=""),
    aca_enabled: str = Form(default=""),
    docs_primary: str = Form(default=""),
    yoga_expense_ratio: str = Form(default=""),
    last_year_total_tax: str = Form(default=""),
    safe_harbor_multiplier: str = Form(default="1.0"),
    magi_override: str = Form(default=""),
    ira_withholding_override: str = Form(default=""),
    overrides_json: str = Form(default=""),
    return_to: str = Form(default="/taxes/inputs"),
    # Monthly inputs
    yoga_net_profit_1: str = Form(default=""),
    yoga_net_profit_2: str = Form(default=""),
    yoga_net_profit_3: str = Form(default=""),
    yoga_net_profit_4: str = Form(default=""),
    yoga_net_profit_5: str = Form(default=""),
    yoga_net_profit_6: str = Form(default=""),
    yoga_net_profit_7: str = Form(default=""),
    yoga_net_profit_8: str = Form(default=""),
    yoga_net_profit_9: str = Form(default=""),
    yoga_net_profit_10: str = Form(default=""),
    yoga_net_profit_11: str = Form(default=""),
    yoga_net_profit_12: str = Form(default=""),
    daughter_w2_wages_1: str = Form(default=""),
    daughter_w2_wages_2: str = Form(default=""),
    daughter_w2_wages_3: str = Form(default=""),
    daughter_w2_wages_4: str = Form(default=""),
    daughter_w2_wages_5: str = Form(default=""),
    daughter_w2_wages_6: str = Form(default=""),
    daughter_w2_wages_7: str = Form(default=""),
    daughter_w2_wages_8: str = Form(default=""),
    daughter_w2_wages_9: str = Form(default=""),
    daughter_w2_wages_10: str = Form(default=""),
    daughter_w2_wages_11: str = Form(default=""),
    daughter_w2_wages_12: str = Form(default=""),
    daughter_w2_withholding_1: str = Form(default=""),
    daughter_w2_withholding_2: str = Form(default=""),
    daughter_w2_withholding_3: str = Form(default=""),
    daughter_w2_withholding_4: str = Form(default=""),
    daughter_w2_withholding_5: str = Form(default=""),
    daughter_w2_withholding_6: str = Form(default=""),
    daughter_w2_withholding_7: str = Form(default=""),
    daughter_w2_withholding_8: str = Form(default=""),
    daughter_w2_withholding_9: str = Form(default=""),
    daughter_w2_withholding_10: str = Form(default=""),
    daughter_w2_withholding_11: str = Form(default=""),
    daughter_w2_withholding_12: str = Form(default=""),
    trust_passthrough_1: str = Form(default=""),
    trust_passthrough_2: str = Form(default=""),
    trust_passthrough_3: str = Form(default=""),
    trust_passthrough_4: str = Form(default=""),
    trust_passthrough_5: str = Form(default=""),
    trust_passthrough_6: str = Form(default=""),
    trust_passthrough_7: str = Form(default=""),
    trust_passthrough_8: str = Form(default=""),
    trust_passthrough_9: str = Form(default=""),
    trust_passthrough_10: str = Form(default=""),
    trust_passthrough_11: str = Form(default=""),
    trust_passthrough_12: str = Form(default=""),
    trust_fees_1: str = Form(default=""),
    trust_fees_2: str = Form(default=""),
    trust_fees_3: str = Form(default=""),
    trust_fees_4: str = Form(default=""),
    trust_fees_5: str = Form(default=""),
    trust_fees_6: str = Form(default=""),
    trust_fees_7: str = Form(default=""),
    trust_fees_8: str = Form(default=""),
    trust_fees_9: str = Form(default=""),
    trust_fees_10: str = Form(default=""),
    trust_fees_11: str = Form(default=""),
    trust_fees_12: str = Form(default=""),
    aca_premium_1: str = Form(default=""),
    aca_premium_2: str = Form(default=""),
    aca_premium_3: str = Form(default=""),
    aca_premium_4: str = Form(default=""),
    aca_premium_5: str = Form(default=""),
    aca_premium_6: str = Form(default=""),
    aca_premium_7: str = Form(default=""),
    aca_premium_8: str = Form(default=""),
    aca_premium_9: str = Form(default=""),
    aca_premium_10: str = Form(default=""),
    aca_premium_11: str = Form(default=""),
    aca_premium_12: str = Form(default=""),
    aca_aptc_1: str = Form(default=""),
    aca_aptc_2: str = Form(default=""),
    aca_aptc_3: str = Form(default=""),
    aca_aptc_4: str = Form(default=""),
    aca_aptc_5: str = Form(default=""),
    aca_aptc_6: str = Form(default=""),
    aca_aptc_7: str = Form(default=""),
    aca_aptc_8: str = Form(default=""),
    aca_aptc_9: str = Form(default=""),
    aca_aptc_10: str = Form(default=""),
    aca_aptc_11: str = Form(default=""),
    aca_aptc_12: str = Form(default=""),
    est_pay_date_1: str = Form(default=""),
    est_pay_amount_1: str = Form(default=""),
    est_pay_date_2: str = Form(default=""),
    est_pay_amount_2: str = Form(default=""),
    est_pay_date_3: str = Form(default=""),
    est_pay_amount_3: str = Form(default=""),
    est_pay_date_4: str = Form(default=""),
    est_pay_amount_4: str = Form(default=""),
    est_pay_date_5: str = Form(default=""),
    est_pay_amount_5: str = Form(default=""),
    est_pay_date_6: str = Form(default=""),
    est_pay_amount_6: str = Form(default=""),
    **form: Any,
):
    profile = get_or_create_tax_profile(session, year=year)
    inputs_row = get_or_create_tax_inputs(session, year=year)

    profile_old = jsonable(profile.__dict__)
    inputs_old = jsonable(inputs_row.data_json)

    profile.filing_status = (filing_status or "MFJ").upper()
    profile.state_code = (state_code or "").strip() or None
    profile.deductions_mode = deductions_mode if deductions_mode in {"standard", "itemized"} else "standard"
    profile.itemized_amount = _parse_float(itemized_amount) if profile.deductions_mode == "itemized" else None
    profile.household_size = _parse_int(household_size, default=3)
    profile.dependents_count = _parse_int(dependents_count, default=1)
    profile.trust_income_taxable_to_user = trust_income_taxable_to_user == "on"
    profile.updated_at = dt.datetime.utcnow()

    monthly_form = {
        "yoga_net_profit": {
            1: yoga_net_profit_1,
            2: yoga_net_profit_2,
            3: yoga_net_profit_3,
            4: yoga_net_profit_4,
            5: yoga_net_profit_5,
            6: yoga_net_profit_6,
            7: yoga_net_profit_7,
            8: yoga_net_profit_8,
            9: yoga_net_profit_9,
            10: yoga_net_profit_10,
            11: yoga_net_profit_11,
            12: yoga_net_profit_12,
        },
        "daughter_w2_wages": {
            1: daughter_w2_wages_1,
            2: daughter_w2_wages_2,
            3: daughter_w2_wages_3,
            4: daughter_w2_wages_4,
            5: daughter_w2_wages_5,
            6: daughter_w2_wages_6,
            7: daughter_w2_wages_7,
            8: daughter_w2_wages_8,
            9: daughter_w2_wages_9,
            10: daughter_w2_wages_10,
            11: daughter_w2_wages_11,
            12: daughter_w2_wages_12,
        },
        "daughter_w2_withholding": {
            1: daughter_w2_withholding_1,
            2: daughter_w2_withholding_2,
            3: daughter_w2_withholding_3,
            4: daughter_w2_withholding_4,
            5: daughter_w2_withholding_5,
            6: daughter_w2_withholding_6,
            7: daughter_w2_withholding_7,
            8: daughter_w2_withholding_8,
            9: daughter_w2_withholding_9,
            10: daughter_w2_withholding_10,
            11: daughter_w2_withholding_11,
            12: daughter_w2_withholding_12,
        },
        "trust_passthrough": {
            1: trust_passthrough_1,
            2: trust_passthrough_2,
            3: trust_passthrough_3,
            4: trust_passthrough_4,
            5: trust_passthrough_5,
            6: trust_passthrough_6,
            7: trust_passthrough_7,
            8: trust_passthrough_8,
            9: trust_passthrough_9,
            10: trust_passthrough_10,
            11: trust_passthrough_11,
            12: trust_passthrough_12,
        },
        "trust_fees": {
            1: trust_fees_1,
            2: trust_fees_2,
            3: trust_fees_3,
            4: trust_fees_4,
            5: trust_fees_5,
            6: trust_fees_6,
            7: trust_fees_7,
            8: trust_fees_8,
            9: trust_fees_9,
            10: trust_fees_10,
            11: trust_fees_11,
            12: trust_fees_12,
        },
        "aca_premium": {
            1: aca_premium_1,
            2: aca_premium_2,
            3: aca_premium_3,
            4: aca_premium_4,
            5: aca_premium_5,
            6: aca_premium_6,
            7: aca_premium_7,
            8: aca_premium_8,
            9: aca_premium_9,
            10: aca_premium_10,
            11: aca_premium_11,
            12: aca_premium_12,
        },
        "aca_aptc": {
            1: aca_aptc_1,
            2: aca_aptc_2,
            3: aca_aptc_3,
            4: aca_aptc_4,
            5: aca_aptc_5,
            6: aca_aptc_6,
            7: aca_aptc_7,
            8: aca_aptc_8,
            9: aca_aptc_9,
            10: aca_aptc_10,
            11: aca_aptc_11,
            12: aca_aptc_12,
        },
    }

    yoga_monthly = [_parse_float(monthly_form["yoga_net_profit"][i]) for i in range(1, 13)]
    wages_monthly = [_parse_float(monthly_form["daughter_w2_wages"][i]) for i in range(1, 13)]
    w2_withholding_monthly = [_parse_float(monthly_form["daughter_w2_withholding"][i]) for i in range(1, 13)]
    trust_monthly = [_parse_float(monthly_form["trust_passthrough"][i]) for i in range(1, 13)]
    trust_fees_monthly = [_parse_float(monthly_form["trust_fees"][i]) for i in range(1, 13)]
    aca_premium_monthly = [_parse_float(monthly_form["aca_premium"][i]) for i in range(1, 13)]
    aca_aptc_monthly = [_parse_float(monthly_form["aca_aptc"][i]) for i in range(1, 13)]
    if aca_premium_monthly:
        aca_premium_monthly = [aca_premium_monthly[0]] * 12
    if aca_aptc_monthly:
        aca_aptc_monthly = [aca_aptc_monthly[0]] * 12

    est_payments: list[dict[str, Any]] = []
    for idx in range(1, 7):
        date_raw = locals().get(f"est_pay_date_{idx}") or ""
        amt_raw = locals().get(f"est_pay_amount_{idx}") or ""
        date_s = str(date_raw).strip()
        amt = _parse_float(str(amt_raw))
        if date_s and amt:
            est_payments.append({"date": date_s[:10], "amount": amt})

    overrides = {}
    if overrides_json.strip():
        try:
            overrides = json.loads(overrides_json)
            if not isinstance(overrides, dict):
                raise ValueError("Overrides JSON must be an object")
        except Exception as exc:
            msg = f"Invalid overrides JSON: {exc}"
            return RedirectResponse(url=f"{_safe_return_to(return_to, default='/taxes/inputs')}?error={msg}", status_code=303)

    manual_overrides: dict[str, Any] = {}
    def _manual_enabled(key: str) -> bool:
        return str(form.get(f"manual_override_enable_{key}") or "").lower() in {"on", "true", "1"}

    def _manual_value(key: str) -> float:
        return _parse_float(str(form.get(f"manual_override_{key}") or ""))

    if _manual_enabled("w2_wages_total"):
        manual_overrides["w2_wages_total"] = _manual_value("w2_wages_total")
    if _manual_enabled("w2_withholding_total"):
        manual_overrides["w2_withholding_total"] = _manual_value("w2_withholding_total")
    if _manual_enabled("ira_distributions_total"):
        manual_overrides["ira_distributions_total"] = _manual_value("ira_distributions_total")
    if _manual_enabled("ira_withholding_total"):
        manual_overrides["ira_withholding_total"] = _manual_value("ira_withholding_total")
    if _manual_enabled("interest_total"):
        manual_overrides["interest_total"] = _manual_value("interest_total")
    if _manual_enabled("dividends_ordinary_total"):
        manual_overrides["dividends_ordinary_total"] = _manual_value("dividends_ordinary_total")
    if _manual_enabled("dividends_qualified_total"):
        manual_overrides["dividends_qualified_total"] = _manual_value("dividends_qualified_total")
    if _manual_enabled("cap_gain_dist_total"):
        manual_overrides["cap_gain_dist_total"] = _manual_value("cap_gain_dist_total")
    if _manual_enabled("k1_total"):
        manual_overrides["k1_total"] = _manual_value("k1_total")
    if _manual_enabled("aca_premium_total"):
        total = _manual_value("aca_premium_total")
        manual_overrides["aca_premium_monthly"] = [total / 12.0] * 12
    if _manual_enabled("aca_aptc_total"):
        total = _manual_value("aca_aptc_total")
        manual_overrides["aca_aptc_monthly"] = [total / 12.0] * 12
    if _manual_enabled("aca_slcsp_total"):
        total = _manual_value("aca_slcsp_total")
        manual_overrides["aca_slcsp_monthly"] = [total / 12.0] * 12

    data = {
        "yoga_net_profit_monthly": yoga_monthly,
        "yoga_expense_ratio": _parse_float(yoga_expense_ratio, default=0.3),
        "daughter_w2_wages_monthly": wages_monthly,
        "daughter_w2_withholding_monthly": w2_withholding_monthly,
        "trust_passthrough_monthly": trust_monthly,
        "trust_fees_monthly": trust_fees_monthly,
        "aca_premium_monthly": aca_premium_monthly,
        "aca_aptc_monthly": aca_aptc_monthly,
        "ira_distributions_override_monthly": (inputs_old or {}).get("ira_distributions_override_monthly"),
        "state_tax_rate": _parse_float(state_tax_rate),
        "qualified_dividend_pct": _parse_float(qualified_dividend_pct),
        "niit_enabled": niit_enabled == "on",
        "niit_rate": _parse_float(niit_rate, default=0.038),
        "aca_enabled": aca_enabled == "on",
        "docs_primary": docs_primary == "on",
        "last_year_total_tax": _parse_float(last_year_total_tax),
        "safe_harbor_multiplier": _parse_float(safe_harbor_multiplier or "1.0", default=1.0),
        "magi_override": _parse_float(magi_override) if magi_override.strip() else None,
        "ira_withholding_override": _parse_float(ira_withholding_override) if ira_withholding_override.strip() else None,
        "estimated_payments": est_payments,
        "tax_doc_overrides": (inputs_old or {}).get("tax_doc_overrides", {}),
        "tax_manual_overrides": manual_overrides,
        "tax_parameter_overrides": overrides,
    }
    inputs_row.data_json = data
    inputs_row.updated_at = dt.datetime.utcnow()

    session.flush()
    log_change(
        session,
        actor=actor,
        action="UPDATE",
        entity="TaxProfile",
        entity_id=str(profile.id),
        old=profile_old,
        new=jsonable(profile.__dict__),
        note=f"Update tax profile {year}",
    )
    log_change(
        session,
        actor=actor,
        action="UPDATE",
        entity="TaxInput",
        entity_id=str(inputs_row.id),
        old=inputs_old,
        new=jsonable(inputs_row.data_json),
        note=f"Update tax inputs {year}",
    )
    session.commit()

    msg = "Saved tax inputs."
    return RedirectResponse(url=f"{_safe_return_to(return_to, default='/taxes/inputs')}?ok={msg}", status_code=303)


@router.post("/inputs/tags")
def taxes_tags_update(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    year: int = Form(...),
    return_to: str = Form(default="/taxes/inputs"),
    **form: Any,
):
    updated = 0
    deleted = 0
    for key, value in form.items():
        if not key.startswith("tag_"):
            continue
        tx_id_raw = key.replace("tag_", "")
        if not tx_id_raw.isdigit():
            continue
        tx_id = int(tx_id_raw)
        category = (value or "").strip()
        note = (form.get(f"note_{tx_id}") or "").strip()

        existing = session.query(TaxTag).filter(TaxTag.transaction_id == tx_id).one_or_none()
        if not category:
            if existing is not None:
                session.delete(existing)
                deleted += 1
            continue
        if category not in TAX_TAG_CATEGORIES:
            continue
        if existing is None:
            session.add(TaxTag(transaction_id=tx_id, category=category, note=note))
            updated += 1
        else:
            existing.category = category
            existing.note = note
            existing.updated_at = dt.datetime.utcnow()
            updated += 1
    session.commit()
    msg = f"Updated tags ({updated} updated, {deleted} removed)."
    return RedirectResponse(url=f"{_safe_return_to(return_to, default='/taxes/inputs')}?ok={msg}", status_code=303)


@router.get("/cpa-pack")
def taxes_cpa_pack(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    year_raw = (request.query_params.get("year") or "").strip()
    year = int(year_raw) if year_raw.isdigit() else dt.date.today().year
    auto_tag_tax_transactions(session, year=year)
    dashboard = build_tax_dashboard(session, year=year)
    tax_years = available_tax_years(session, default_year=year)
    dividend_details = list_dividend_details(session, year=year)
    dividend_summary = dividend_summary_by_account(session, year=year)
    interest_details = list_interest_details(session, year=year)
    interest_summary = interest_summary_by_account(session, year=year)
    trust_pnl_details = list_trust_pnl_details(session, year=year)
    capital_gains_details = list_capital_gains_details(session, year=year)
    capital_gains_summary = capital_gains_summary_by_account(session, year=year)
    wash_sale_details = list_wash_sale_details(session, year=year)
    ira_distribution_details = list_ira_tax_details(session, year=year, category="IRA_DISTRIBUTION")
    ira_withholding_details = list_ira_tax_details(session, year=year, category="IRA_WITHHOLDING")
    w2_withholding_details = list_w2_withholding_details(session, year=year, inputs=dashboard.inputs)
    other_withholding_details = list_other_withholding_details(session, year=year)
    estimated_payment_details = list_estimated_payment_details(session, year=year, inputs=dashboard.inputs)
    doc_summary = tax_docs_summary(session, tax_year=year)
    doc_overrides = aggregate_tax_doc_overrides(session, tax_year=year)
    entity_rows = list_household_entities(session, tax_year=year)
    entity_map = {int(row["id"]): row for row in entity_rows}
    doc_entity_totals = []
    for owner_id, totals in (doc_overrides.get("by_entity") or {}).items():
        owner = entity_map.get(int(owner_id))
        if not owner:
            continue
        doc_entity_totals.append(
            {
                "owner_entity_id": int(owner_id),
                "owner_label": owner.get("display_name"),
                "owner_type": owner.get("entity_type"),
                "totals": totals,
            }
        )
    reconcile = build_tax_reconciliation(session, tax_year=year)

    def _doc_relevant(row: dict[str, Any]) -> bool:
        if row.get("status") != "CONFIRMED":
            return False
        if row.get("is_corrected") and row.get("is_authoritative") is not True:
            return False
        if row.get("is_authoritative") is False:
            return False
        return True

    included_docs = [row for row in doc_summary if _doc_relevant(row)]
    lines = [
        f"Hello,",
        "",
        f"Attached is the CPA packet for tax year {year}.",
        f"- ZIP file name: taxes_cpa_packet_{year}.zip",
        "",
        "Included documents:",
    ]
    if included_docs:
        for row in included_docs:
            owner = row.get("owner_label") or ""
            owner_txt = f" ({owner})" if owner else ""
            lines.append(f"- {row.get('filename')} [{row.get('doc_type')}] {owner_txt}".strip())
    else:
        lines.append("- (No confirmed documents yet)")
    lines.extend(
        [
            "",
            "Notes:",
            "- Tax documents are used as the primary source of truth in the Investor tax estimate.",
            "- A reconciliation summary and CSV exports are available in the CPA Pack.",
            "",
            "Thanks!",
        ]
    )
    mailto_subject = f"CPA Packet - Tax Year {year}"
    mailto_body = "\n".join(lines)
    mailto_href = f"mailto:?subject={urllib.parse.quote(mailto_subject)}&body={urllib.parse.quote(mailto_body)}"

    from src.app.main import templates

    return templates.TemplateResponse(
        "taxes_cpa_pack.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "year": year,
            "tax_years": tax_years,
            "dashboard": dashboard,
            "dividend_details": dividend_details,
            "dividend_summary_by_account": dividend_summary,
            "interest_details": interest_details,
            "interest_summary_by_account": interest_summary,
            "trust_pnl_details": trust_pnl_details,
            "capital_gains_details": capital_gains_details,
            "capital_gains_summary_by_account": capital_gains_summary,
            "wash_sale_details": wash_sale_details,
            "ira_distribution_details": ira_distribution_details,
            "ira_withholding_details": ira_withholding_details,
            "w2_withholding_details": w2_withholding_details,
            "other_withholding_details": other_withholding_details,
            "estimated_payment_details": estimated_payment_details,
            "tax_doc_summary": doc_summary,
            "tax_doc_entity_totals": doc_entity_totals,
            "tax_reconciliation": reconcile,
            "mailto_href": mailto_href,
            "print_view": request.query_params.get("print") == "1",
        },
    )


@router.get("/cpa-pack/monthly.csv")
def taxes_cpa_monthly_csv(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    year_raw = (request.query_params.get("year") or "").strip()
    year = int(year_raw) if year_raw.isdigit() else dt.date.today().year
    dashboard = build_tax_dashboard(session, year=year)

    headers = [
        "month",
        "ordinary_ytd",
        "st_gains_ytd",
        "lt_gains_ytd",
        "dividends_ytd",
        "tax_ytd",
        "paid_ytd",
        "trust_dividend_tax_ytd",
        "remaining_ytd",
        "run_rate_tax",
        "flags",
    ]
    rows = []
    for row in dashboard.monthly:
        rows.append(
            [
                row.get("label"),
                row.get("ordinary_ytd"),
                row.get("st_gains_ytd"),
                row.get("lt_gains_ytd"),
                row.get("dividends_ytd"),
                row.get("tax_ytd"),
                row.get("paid_ytd"),
                row.get("trust_dividend_tax_ytd"),
                row.get("remaining_ytd"),
                row.get("run_rate_tax"),
                ", ".join(row.get("flags") or []),
            ]
        )

    csv_lines = [",".join(headers)]
    for r in rows:
        csv_lines.append(",".join([str(v) if v is not None else "" for v in r]))
    csv_text = "\n".join(csv_lines)
    fn = f"taxes_monthly_{year}.csv"
    return Response(content=csv_text, media_type="text/csv", headers={"Content-Disposition": f'attachment; filename="{fn}"'})


@router.get("/cpa-pack/transactions.csv")
def taxes_cpa_transactions_csv(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    year_raw = (request.query_params.get("year") or "").strip()
    year = int(year_raw) if year_raw.isdigit() else dt.date.today().year
    rows = list_tax_tagged_transactions(session, year=year)

    headers = ["date", "account", "amount", "category", "description", "note", "transaction_id"]
    csv_lines = [",".join(headers)]
    for r in rows:
        csv_lines.append(
            ",".join(
                [
                    str(r.get("date") or ""),
                    str(r.get("account_name") or ""),
                    str(r.get("amount") or ""),
                    str(r.get("category") or ""),
                    str(r.get("description") or ""),
                    str(r.get("note") or ""),
                    str(r.get("transaction_id") or ""),
                ]
            )
        )
    csv_text = "\n".join(csv_lines)
    fn = f"taxes_tagged_transactions_{year}.csv"
    return Response(content=csv_text, media_type="text/csv", headers={"Content-Disposition": f'attachment; filename="{fn}"'})


@router.get("/cpa-pack/tax-docs.csv")
def taxes_cpa_tax_docs_csv(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    year_raw = (request.query_params.get("year") or "").strip()
    year = int(year_raw) if year_raw.isdigit() else dt.date.today().year
    entity_rows = list_household_entities(session, tax_year=year)
    entity_map = {int(row["id"]): row for row in entity_rows}
    rows = (
        session.query(TaxFact, TaxDocument)
        .join(TaxDocument, TaxDocument.id == TaxFact.source_doc_id)
        .filter(TaxFact.tax_year == year)
        .order_by(TaxFact.fact_type.asc(), TaxFact.id.asc())
        .all()
    )
    headers = [
        "tax_year",
        "doc_id",
        "doc_type",
        "filename",
        "fact_type",
        "amount",
        "payer_name",
        "recipient_name",
        "owner_entity_id",
        "owner_label",
        "metadata",
        "confirmed",
    ]
    csv_lines = [",".join(headers)]
    for fact, doc in rows:
        owner_id = int(fact.owner_entity_id or doc.owner_entity_id or 0)
        owner = entity_map.get(owner_id) if owner_id else None
        csv_lines.append(
            ",".join(
                [
                    str(fact.tax_year),
                    str(doc.id),
                    str(doc.doc_type),
                    str(doc.filename or ""),
                    str(fact.fact_type),
                    str(float(fact.amount or 0.0)),
                    str(fact.payer_name or ""),
                    str(fact.recipient_name or ""),
                    str(owner_id or ""),
                    str(owner.get("display_name") if owner else ""),
                    json.dumps(fact.metadata_json or {}),
                    "1" if fact.user_confirmed else "0",
                ]
            )
        )
    csv_text = "\n".join(csv_lines)
    fn = f"taxes_tax_docs_{year}.csv"
    return Response(content=csv_text, media_type="text/csv", headers={"Content-Disposition": f'attachment; filename="{fn}"'})


@router.get("/cpa-pack/email-packet.zip")
def taxes_cpa_email_packet(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    year_raw = (request.query_params.get("year") or "").strip()
    year = int(year_raw) if year_raw.isdigit() else dt.date.today().year
    docs = (
        session.query(TaxDocument)
        .filter(TaxDocument.tax_year == year)
        .order_by(TaxDocument.uploaded_at.asc(), TaxDocument.id.asc())
        .all()
    )

    def _is_relevant(doc: TaxDocument) -> bool:
        if doc.status != "CONFIRMED":
            return False
        if doc.is_corrected and doc.is_authoritative is not True:
            return False
        if doc.is_authoritative is False:
            return False
        return True

    rows = []
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for doc in docs:
            if not _is_relevant(doc):
                continue
            if not doc.raw_file_path:
                rows.append([doc.id, doc.filename, doc.doc_type, "missing_file"])
                continue
            path = doc.raw_file_path
            if not os.path.exists(path):
                rows.append([doc.id, doc.filename, doc.doc_type, "missing_file"])
                continue
            safe_name = f"{doc.doc_type}_{doc.id}_{doc.filename}"
            zf.write(path, safe_name)
            rows.append([doc.id, doc.filename, doc.doc_type, "included"])
        manifest_lines = ["doc_id,filename,doc_type,status"]
        for row in rows:
            manifest_lines.append(",".join([str(v) for v in row]))
        zf.writestr("manifest.csv", "\n".join(manifest_lines))

    buffer.seek(0)
    fn = f"taxes_cpa_packet_{year}.zip"
    return Response(
        content=buffer.getvalue(),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{fn}"'},
    )
